use crate::database::Database;
use anyhow::{Result, ensure};
use axum::{
    body::{Body, HttpBody},
    http::Response,
};
use futures::TryStreamExt;
use serde_json::{Value, json};
use std::{
    future::IntoFuture,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::watch;

const LATENCY_MS: [u64; 7] = [1, 5, 20, 100, 500, 1000, 3000];
pub(crate) struct Lifecycle {
    draining: AtomicBool,
    stopped: watch::Sender<bool>,
    drain: Duration,
    started: AtomicU64,
    completed: AtomicU64,
    aborted: AtomicU64,
    client_errors: AtomicU64,
    server_errors: AtomicU64,
    bytes: AtomicU64,
    buckets: [AtomicU64; 8],
}
impl Lifecycle {
    pub(crate) fn configured() -> Result<Arc<Self>> {
        let seconds = std::env::var("MORIA_NATIVE_DRAIN_SECONDS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| anyhow::anyhow!("LIFECYCLE_CONFIG_INVALID"))?
            .unwrap_or(30);
        ensure!((1..=300).contains(&seconds), "LIFECYCLE_CONFIG_INVALID");
        Ok(Self::new(Duration::from_secs(seconds)))
    }
    fn new(drain: Duration) -> Arc<Self> {
        let (stopped, _) = watch::channel(false);
        Arc::new(Self {
            draining: AtomicBool::new(false),
            stopped,
            drain,
            started: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            aborted: AtomicU64::new(0),
            client_errors: AtomicU64::new(0),
            server_errors: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        })
    }
    pub(crate) fn accepting(&self) -> bool {
        !self.draining.load(Ordering::Acquire)
    }
    fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
        self.stopped.send_replace(true);
    }
    pub(crate) async fn shutdown_signal(&self) {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("signal handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
        self.begin_drain();
    }
    pub(crate) async fn run<F, E>(&self, server: F) -> Result<()>
    where
        F: IntoFuture<Output = std::result::Result<(), E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut stopped = self.stopped.subscribe();
        let server = server.into_future();
        tokio::pin!(server);
        tokio::select! {
            result=&mut server=>result.map_err(Into::into),
            _=stopped.wait_for(|value|*value)=>tokio::time::timeout(self.drain,&mut server).await.map_err(|_|anyhow::anyhow!("NATIVE_DRAIN_TIMEOUT"))?.map_err(Into::into)
        }
    }
    pub(crate) async fn ready(&self, db: &Database) -> bool {
        if !self.accepting() {
            return false;
        }
        let operation = async {
            let client = db.get().await?;
            client
                .query_one("SELECT moria.runtime_recovery_check()", &[])
                .await?;
            let current:bool=client.query_one("SELECT NULLIF(current_setting('moria.cell_id',true),'') IS NULL OR EXISTS(SELECT 1 FROM moria.runtime_cells c JOIN moria.indexing_instance i ON i.incarnation=c.incarnation WHERE c.enabled AND c.id::text=current_setting('moria.cell_id',true))",&[]).await?.get(0);
            Ok::<_, anyhow::Error>(current && self.accepting())
        };
        matches!(
            tokio::time::timeout(Duration::from_secs(2), operation).await,
            Ok(Ok(true))
        )
    }
    pub(crate) fn observe(self: &Arc<Self>) -> Observation {
        self.started.fetch_add(1, Ordering::Relaxed);
        Observation {
            lifecycle: self.clone(),
            started: Instant::now(),
            ended: false,
            status: 200,
        }
    }
    pub(crate) fn metrics(&self) -> Value {
        let started = self.started.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed);
        let aborted = self.aborted.load(Ordering::Relaxed);
        json!({"version":1,"accepting":self.accepting(),"drainSeconds":self.drain.as_secs(),"requestsStarted":started.to_string(),"requestsCompleted":completed.to_string(),"requestsAborted":aborted.to_string(),"requestsActive":started.saturating_sub(completed).saturating_sub(aborted).to_string(),"clientErrors":self.client_errors.load(Ordering::Relaxed).to_string(),"serverErrors":self.server_errors.load(Ordering::Relaxed).to_string(),"responseBytes":self.bytes.load(Ordering::Relaxed).to_string(),"latencyMsUpperBounds":[1,5,20,100,500,1000,3000,null],"latencyBucketCounts":self.buckets.iter().map(|value|value.load(Ordering::Relaxed).to_string()).collect::<Vec<_>>()})
    }
}
pub(crate) struct Observation {
    lifecycle: Arc<Lifecycle>,
    started: Instant,
    ended: bool,
    status: u16,
}
impl Observation {
    pub(crate) fn response(mut self, response: Response<Body>) -> Response<Body> {
        self.status = response.status().as_u16();
        let (parts, body) = response.into_parts();
        self.ended = body.is_end_stream();
        let stream = futures::stream::try_unfold(
            (body.into_data_stream(), self),
            |(mut body, mut observation)| async move {
                match body.try_next().await? {
                    Some(bytes) => {
                        observation
                            .lifecycle
                            .bytes
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        Ok::<_, axum::Error>(Some((bytes, (body, observation))))
                    }
                    None => {
                        observation.ended = true;
                        drop(observation);
                        Ok(None)
                    }
                }
            },
        );
        Response::from_parts(parts, Body::from_stream(stream))
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_millis();
        let bucket = LATENCY_MS
            .iter()
            .position(|upper| elapsed <= u128::from(*upper))
            .unwrap_or(7);
        self.lifecycle.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        if self.ended {
            self.lifecycle.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.lifecycle.aborted.fetch_add(1, Ordering::Relaxed);
        }
        if self.status >= 500 {
            self.lifecycle.server_errors.fetch_add(1, Ordering::Relaxed);
        } else if self.status >= 400 {
            self.lifecycle.client_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn completion_and_drop_are_distinct_and_streaming_bytes_are_exact() {
        let lifecycle = Lifecycle::new(Duration::from_secs(1));
        let response = lifecycle
            .observe()
            .response(Response::new(Body::from(vec![7u8; 200000])));
        assert_eq!(lifecycle.metrics()["requestsActive"], "1");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 200001)
                .await
                .unwrap()
                .len(),
            200000
        );
        let response = lifecycle
            .observe()
            .response(Response::new(Body::from_stream(
                futures::stream::pending::<Result<bytes::Bytes, std::io::Error>>(),
            )));
        drop(response);
        let metrics = lifecycle.metrics();
        assert_eq!(metrics["requestsActive"], "0");
        assert_eq!(metrics["requestsCompleted"], "1");
        assert_eq!(metrics["requestsAborted"], "1");
        assert_eq!(metrics["responseBytes"], "200000");
    }
    #[tokio::test]
    async fn drain_has_a_finite_deadline() {
        let lifecycle = Lifecycle::new(Duration::from_millis(20));
        lifecycle.begin_drain();
        assert!(!lifecycle.accepting());
        let result = lifecycle
            .run(futures::future::pending::<std::io::Result<()>>())
            .await;
        assert_eq!(result.unwrap_err().to_string(), "NATIVE_DRAIN_TIMEOUT");
    }
}
