use axum::{
    body::Body,
    extract::connect_info::Connected,
    http::Response,
    serve::{IncomingStream, Listener},
};
use bytes::Bytes;
use futures::TryStreamExt;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};

pub(crate) struct BoundedListener {
    inner: TcpListener,
    slots: Arc<Semaphore>,
}
pub(crate) struct Socket {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
    idle: Pin<Box<tokio::time::Sleep>>,
    idle_duration: Duration,
    activity: Arc<ConnectionActivity>,
    activity_generation: u64,
}
#[derive(Default)]
struct ConnectionActivity {
    handlers: AtomicUsize,
    generation: AtomicU64,
}
pub(crate) struct Processing(Arc<ConnectionActivity>);
impl Drop for Processing {
    fn drop(&mut self) {
        self.0.generation.fetch_add(1, Ordering::Release);
        self.0.handlers.fetch_sub(1, Ordering::Release);
    }
}
#[derive(Clone)]
pub(crate) struct Peer(pub SocketAddr, Arc<ConnectionActivity>);
impl Peer {
    pub(crate) fn processing(&self) -> Processing {
        self.1.handlers.fetch_add(1, Ordering::AcqRel);
        self.1.generation.fetch_add(1, Ordering::Release);
        Processing(self.1.clone())
    }
}
impl Connected<IncomingStream<'_, BoundedListener>> for Peer {
    fn connect_info(stream: IncomingStream<'_, BoundedListener>) -> Self {
        Self(*stream.remote_addr(), stream.io().activity.clone())
    }
}
impl BoundedListener {
    pub(crate) fn new(listener: TcpListener) -> Self {
        Self {
            inner: listener,
            slots: Arc::new(Semaphore::new(256)),
        }
    }
}
impl Listener for BoundedListener {
    type Io = Socket;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Socket, SocketAddr) {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("connection budget remains open");
        loop {
            match self.inner.accept().await {
                Ok((inner, address)) => {
                    return (
                        Socket {
                            inner,
                            _permit: permit,
                            idle: Box::pin(tokio::time::sleep(Duration::from_secs(60))),
                            idle_duration: Duration::from_secs(60),
                            activity: Arc::new(ConnectionActivity::default()),
                            activity_generation: 0,
                        },
                        address,
                    );
                }
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}
impl Socket {
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use std::future::Future;
        let processing = self.activity.handlers.load(Ordering::Acquire) > 0;
        let generation = self.activity.generation.load(Ordering::Acquire);
        if generation != self.activity_generation {
            self.activity_generation = generation;
            self.progress();
        }
        // Handler deadlines bound computation; TCP idle bounds waiting clients and response IO.
        if processing {
            self.progress();
            return Poll::Ready(Ok(()));
        }
        if self.idle.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle deadline",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }
    fn progress(&mut self) {
        self.idle
            .as_mut()
            .reset(tokio::time::Instant::now() + self.idle_duration);
    }
}
impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let socket = self.get_mut();
        std::task::ready!(socket.poll_idle(cx))?;
        let before = buffer.filled().len();
        let result = Pin::new(&mut socket.inner).poll_read(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            socket.progress();
        }
        result
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let socket = self.get_mut();
        std::task::ready!(socket.poll_idle(cx))?;
        let result = Pin::new(&mut socket.inner).poll_write(cx, bytes);
        if matches!(&result,Poll::Ready(Ok(amount)) if *amount>0) {
            socket.progress();
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let socket = self.get_mut();
        std::task::ready!(socket.poll_idle(cx))?;
        let result = Pin::new(&mut socket.inner).poll_write_vectored(cx, buffers);
        if matches!(&result,Poll::Ready(Ok(amount)) if *amount>0) {
            socket.progress();
        }
        result
    }
}

/// A response retains admission through streaming, including replay and client cancellation.
pub(crate) fn hold_response(
    response: Response<Body>,
    permits: Vec<OwnedSemaphorePermit>,
) -> Response<Body> {
    let (parts, body) = response.into_parts();
    let stream = futures::stream::try_unfold(
        (body.into_data_stream(), Bytes::new(), permits),
        |(mut body, mut pending, permits)| async move {
            while pending.is_empty() {
                match body.try_next().await? {
                    Some(bytes) => pending = bytes,
                    None => return Ok::<_, axum::Error>(None),
                }
            }
            let bytes = pending.split_to(pending.len().min(64 * 1024));
            Ok(Some((bytes, (body, pending, permits))))
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use futures::{StreamExt, stream};
    #[tokio::test]
    async fn idle_connections_release_real_socket_capacity() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (inner, _) = listener.accept().await.unwrap();
        let budget = Arc::new(Semaphore::new(1));
        let mut socket = Socket {
            inner,
            _permit: budget.clone().acquire_owned().await.unwrap(),
            idle: Box::pin(tokio::time::sleep(Duration::from_millis(40))),
            idle_duration: Duration::from_millis(40),
            activity: Arc::new(ConnectionActivity::default()),
            activity_generation: 0,
        };
        peer.write_all(b"x").await.unwrap();
        let mut bytes = [0u8; 1];
        socket.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, [b'x']);
        assert_eq!(
            socket.read(&mut bytes).await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        drop(socket);
        assert_eq!(budget.available_permits(), 1);
    }
    #[tokio::test]
    async fn active_handler_can_finish_after_tcp_idle_but_response_idle_still_expires() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (inner, address) = listener.accept().await.unwrap();
        let activity = Arc::new(ConnectionActivity::default());
        let peer = Peer(address, activity.clone());
        let mut socket = Socket {
            inner,
            _permit: Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap(),
            idle: Box::pin(tokio::time::sleep(Duration::from_millis(40))),
            idle_duration: Duration::from_millis(40),
            activity,
            activity_generation: 0,
        };
        let processing = peer.processing();
        tokio::time::sleep(Duration::from_millis(90)).await;
        drop(processing);
        socket.write_all(b"response").await.unwrap();
        let mut bytes = [0; 8];
        client.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"response");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            socket.write_all(b"late").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }
    #[tokio::test]
    async fn accepted_connection_budget_is_held_until_the_socket_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut listener = BoundedListener {
            inner: listener,
            slots: Arc::new(Semaphore::new(1)),
        };
        let _first = TcpStream::connect(address).await.unwrap();
        let (first, _) = listener.accept().await;
        let _second = TcpStream::connect(address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        drop(first);
        let (second, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap();
        drop(second);
        assert_eq!(listener.slots.available_permits(), 1);
    }
    #[tokio::test]
    async fn response_admission_survives_headers_and_releases_on_end_or_drop() {
        let budget = Arc::new(Semaphore::new(1));
        let permit = budget.clone().acquire_owned().await.unwrap();
        let response = hold_response(Response::new(Body::from(vec![7_u8; 200000])), vec![permit]);
        assert!(budget.clone().try_acquire_owned().is_err());
        let mut chunks = response.into_body().into_data_stream();
        let mut size = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.unwrap();
            assert!(chunk.len() <= 65536);
            size += chunk.len();
        }
        assert_eq!(size, 200000);
        assert_eq!(budget.available_permits(), 1);
        let permit = budget.clone().acquire_owned().await.unwrap();
        let response = hold_response(
            Response::new(Body::from_stream(
                stream::pending::<Result<Bytes, io::Error>>(),
            )),
            vec![permit],
        );
        assert_eq!(budget.available_permits(), 0);
        drop(response);
        assert_eq!(budget.available_permits(), 1);
        let permit = budget.clone().acquire_owned().await.unwrap();
        let response = hold_response(Response::new(Body::empty()), vec![permit]);
        assert!(to_bytes(response.into_body(), 1).await.unwrap().is_empty());
        assert_eq!(budget.available_permits(), 1);
    }
}

impl tonic::transport::server::Connected for Socket {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        tonic::transport::server::Connected::connect_info(&self.inner)
    }
}
