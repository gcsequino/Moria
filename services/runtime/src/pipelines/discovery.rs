use super::*;
use crate::connector_discovery::{MAX_PROPOSAL_BYTES, SchemaProposal};

pub(super) struct State {
    execution: Uuid,
    stream: String,
    destination: Destination,
    metadata: Value,
    calls: u32,
}

impl State {
    pub(super) fn new(
        execution: Uuid,
        stream: String,
        mut destination: Destination,
        configuration: Value,
    ) -> Self {
        destination.limit = destination.limit.min(1024 * 1024);
        let metadata = json!({"version":1,"domain":"sync","capability":"schemaDiscovery",
            "stream":stream,"configuration":configuration,"inputs":[],"outputs":[],
            "limits":{"proposalBytes":MAX_PROPOSAL_BYTES,"columns":256,"callbacks":32}});
        Self {
            execution,
            stream,
            destination,
            metadata,
            calls: 0,
        }
    }

    pub(super) async fn http(
        &mut self,
        db: &Database,
        context: &SandboxContext,
        call: &HttpCall,
        parts: &[&str],
        body: &[u8],
    ) -> Result<HttpResponse> {
        ensure!(
            self.execution == context.lease().execution,
            "PIPELINE_CONTEXT_INVALID"
        );
        self.calls = self
            .calls
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PIPELINE_FRAME_LIMIT"))?;
        ensure!(self.calls <= 32, "PIPELINE_FRAME_LIMIT");
        match (call.method.as_str(), parts) {
            ("GET", ["metadata"]) => {
                ensure!(body.is_empty(), "PIPELINE_REQUEST_INVALID");
                json_response(self.metadata.clone())
            }
            ("POST", ["connector", "request"]) => {
                let request: Value = serde_json::from_slice(body)?;
                ensure!(
                    request["method"] == "GET" && request.get("body").is_none_or(Value::is_null),
                    "PIPELINE_OPERATION_DENIED"
                );
                json_response(self.destination.request(context, body).await?)
            }
            ("POST", ["connector", "schema"]) => {
                let proposal = SchemaProposal::parse(body, &self.stream)?;
                let mut client = db.get().await?;
                let tx = client.transaction().await?;
                context.check_in(tx.client()).await?;
                check_context(tx.client(), context).await?;
                let changed=tx.execute("INSERT INTO moria.runtime_pipeline_discoveries(execution,task,owner,fence,proposal) VALUES($1,$2,$3,$4,$5) ON CONFLICT(execution) DO NOTHING",&[&self.execution,&context.lease().task,&context.lease().owner,&context.lease().fence.parse::<i64>()?,&proposal]).await?;
                if changed == 0 {
                    let existing=tx.query_one("SELECT proposal FROM moria.runtime_pipeline_discoveries WHERE execution=$1 AND task=$2 AND owner=$3 AND fence=$4",&[&self.execution,&context.lease().task,&context.lease().owner,&context.lease().fence.parse::<i64>()?]).await?;
                    ensure!(
                        existing.get::<_, Value>(0) == proposal,
                        "PIPELINE_DISCOVERY_INVALID"
                    );
                }
                context.check_in(tx.client()).await?;
                tx.commit().await?;
                json_response(json!({"staged":true}))
            }
            _ => Err(anyhow::anyhow!("PIPELINE_OPERATION_DENIED")),
        }
    }

    pub(super) async fn finish(&self, db: &Database, context: &SandboxContext) -> Result<()> {
        ensure!(
            self.execution == context.lease().execution,
            "PIPELINE_CONTEXT_INVALID"
        );
        let mut client = db.get().await?;
        let tx = client.transaction().await?;
        context.check_in(tx.client()).await?;
        let _ = sealed_result_in(tx.client(), context).await?;
        tx.query_one(
            "SELECT moria.runtime_execution_event($1,'schemaDiscovery',$2)",
            &[
                &self.execution,
                &json!({"calls":self.calls,"stream":self.stream}),
            ],
        )
        .await?;
        context.complete_current_observation_in(tx.client()).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(crate) async fn sealed_result_in(client: &Client, context: &SandboxContext) -> Result<Value> {
    let lease = context.lease();
    let row=client.query_one("SELECT r.capability,r.definition->'spec'->>'stream' AS stream,r.inputs,r.outputs,r.checkpoint,d.proposal FROM moria.runtime_pipeline_executions r JOIN moria.runtime_pipeline_discoveries d ON d.execution=r.execution WHERE r.execution=$1 AND d.task=$2 AND d.owner=$3 AND d.fence=$4",&[&lease.execution,&lease.task,&lease.owner,&lease.fence.parse::<i64>()?]).await?;
    ensure!(
        row.get::<_, String>("capability") == "schemaDiscovery"
            && row.get::<_, Value>("inputs") == json!({})
            && row.get::<_, Value>("outputs") == json!({})
            && row.get::<_, Option<Value>>("checkpoint").is_none(),
        "PIPELINE_DISCOVERY_INVALID"
    );
    let bytes = serde_json::to_vec(&row.get::<_, Value>("proposal"))?;
    let proposal = SchemaProposal::parse(&bytes, &row.get::<_, String>("stream"))?;
    Ok(json!({"schemaDiscovery":proposal}))
}

pub(super) async fn complete(db: &Database, context: &SandboxContext) -> Result<()> {
    let mut admission = Admission::begin(db, context.environment(), false, None).await?;
    let tx = admission.client.as_mut().unwrap().transaction().await?;
    context.check_in(tx.client()).await?;
    check_context(tx.client(), context).await?;
    let rows=tx.query("SELECT t.id,t.owner,t.fence,t.result,d.proposal,r.definition->'spec'->>'stream' AS stream FROM moria.runtime_execution_tasks t JOIN moria.runtime_pipeline_executions r ON r.execution=t.execution JOIN moria.runtime_pipeline_discoveries d ON d.execution=t.execution AND d.task=t.id AND d.owner=t.owner AND d.fence=t.fence WHERE t.execution=$1 AND t.status='sealed' AND r.capability='schemaDiscovery' AND r.inputs='{}'::jsonb AND r.outputs='{}'::jsonb AND r.checkpoint IS NULL LIMIT 2",&[&context.lease().execution]).await?;
    ensure!(rows.len() == 1, "PIPELINE_DISCOVERY_INVALID");
    let row = &rows[0];
    let proposal = SchemaProposal::parse(
        &serde_json::to_vec(&row.get::<_, Value>("proposal"))?,
        &row.get::<_, String>("stream"),
    )?;
    let result = json!({"schemaDiscovery":proposal});
    ensure!(
        row.get::<_, Value>("result") == result,
        "PIPELINE_DISCOVERY_INVALID"
    );
    context.check_in(tx.client()).await?;
    check_context(tx.client(), context).await?;
    tx.query_one(
        "SELECT moria.runtime_execution_complete($1,$2,$3,$4)",
        &[
            &context.lease().execution,
            &context.lease().owner,
            &context.lease().fence.parse::<i64>()?,
            &result,
        ],
    )
    .await?;
    tx.commit().await?;
    admission.release().await?;
    Ok(())
}
