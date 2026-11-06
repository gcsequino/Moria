use crate::{
    authority::{Authority, VerifiedSession},
    authorization::{SqlParameter, authorize},
    database::Database,
    models::NativeModels,
    retrieval::CHUNKER_REVISION,
    serving::{Admission, NativeServing},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use uuid::Uuid;
#[path = "search_background.rs"]
mod background;
#[path = "search_embedding.rs"]
mod embeddings;
#[path = "search_install.rs"]
mod install;
#[path = "search_projector.rs"]
mod projector;
#[path = "search_worker.rs"]
mod worker;
pub(crate) use install::SearchRuntimeProfile;

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Definition {
    source: String,
    text_property: String,
    embedding_model: String,
    chunk_size: usize,
    chunk_overlap: usize,
    metric: String,
    search_mode: String,
    #[serde(default)]
    markings: Vec<String>,
}
#[derive(Clone)]
struct Generation {
    id: Uuid,
    definition: Definition,
    collection_revision: String,
    object_binding_revision: String,
    model_fingerprint: String,
    model_revision: String,
    dimensions: usize,
    incarnation: Uuid,
    covered_revision: String,
    markings: Vec<String>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VisibilityToken {
    pub environment: String,
    pub collection: String,
    pub generation: Uuid,
    pub incarnation: Uuid,
    pub revision: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchRequest {
    pub text: String,
    pub top_k: usize,
    pub minimum_token: Option<VisibilityToken>,
}
#[derive(Clone)]
pub struct NativeSearch {
    pub db: Database,
    pub models: NativeModels,
}

impl NativeSearch {
    async fn generation(
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
    ) -> Result<Generation> {
        let authority = Authority::current(client, session).await?;
        let mut labels = Authority::resource_access(client, session, resource, "read").await?;
        // Permission precedes active-generation lookup so status cannot reveal hidden state.
        authorize(&authority.identity, resource, "query", &labels)?;
        let row=client.query_opt("SELECT g.id,c.revision,c.definition,g.object_binding_revision,g.model_fingerprint,g.model_revision,g.dimensions,g.incarnation,g.covered_revision::text,g.chunker_revision FROM moria.native_search_collections c JOIN moria.native_search_generations g ON g.id=c.active_generation AND g.environment=c.environment AND g.resource=c.resource CROSS JOIN moria.indexing_instance i WHERE c.environment=$1 AND c.resource=$2 AND c.enabled AND g.status='active' AND g.incarnation=i.incarnation AND g.collection_revision=c.revision AND g.definition=c.definition",&[&session.environment(),&resource]).await?.ok_or_else(||anyhow::anyhow!("SEARCH_NOT_READY"))?;
        let definition: Definition = serde_json::from_value(row.get(2))?;
        ensure!(
            definition.chunk_size == 1000
                && definition.chunk_overlap == 100
                && definition.metric == "cosine"
                && definition.search_mode == "exact"
                && row.get::<_, String>(9) == CHUNKER_REVISION,
            "UNSUPPORTED_CAPABILITY"
        );
        labels.extend(definition.markings.iter().cloned());
        authorize(&authority.identity, resource, "query", &labels)?;
        Ok(Generation {
            id: row.get(0),
            definition,
            collection_revision: row.get(1),
            object_binding_revision: row.get(3),
            model_fingerprint: row.get(4),
            model_revision: row.get(5),
            dimensions: row.get::<_, i32>(6) as usize,
            incarnation: row.get(7),
            covered_revision: row.get(8),
            markings: labels,
        })
    }

    pub async fn visibility(&self, session: &VerifiedSession, resource: &str) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = self
            .visibility_in(admission.client(), session, resource)
            .await;
        admission.complete(result).await
    }
    pub(crate) async fn visibility_in(
        &self,
        client: &tokio_postgres::Client,
        session: &VerifiedSession,
        resource: &str,
    ) -> Result<Value> {
        let generation = Self::generation(client, session, resource).await?;
        let candidates = NativeServing::protected_candidates(
            client,
            session,
            &generation.definition.source,
            std::slice::from_ref(&generation.definition.text_property),
            Some(&generation.covered_revision),
        )
        .await?;
        ensure!(
            candidates.binding_revision == generation.object_binding_revision,
            "SCHEMA_CHANGED"
        );
        let profile = self
            .models
            .capabilities_in(client, session, &generation.definition.embedding_model)
            .await?;
        ensure!(
            profile["modelBindingFingerprint"] == generation.model_fingerprint
                && profile["modelRevision"] == generation.model_revision
                && profile["dimensions"] == generation.dimensions
                && profile["capabilities"]
                    .as_array()
                    .is_some_and(|values| values.iter().any(|value| value == "embed")),
            "SEARCH_MODEL_CHANGED"
        );
        Authority::finish(client, session, &candidates.authority).await?;
        Ok(
            json!({"visibility":VisibilityToken{environment:session.environment().into(),collection:resource.into(),generation:generation.id,incarnation:generation.incarnation,revision:generation.covered_revision},"modelRevision":generation.model_revision,"chunkerRevision":CHUNKER_REVISION}),
        )
    }

    fn minimum(
        session: &VerifiedSession,
        resource: &str,
        generation: &Generation,
        token: Option<&VisibilityToken>,
    ) -> Result<()> {
        if let Some(token) = token {
            ensure!(
                token.environment == session.environment()
                    && token.collection == resource
                    && token.incarnation == generation.incarnation,
                "SEARCH_TOKEN_INVALID"
            );
            ensure!(
                token.generation == generation.id,
                "SEARCH_GENERATION_CHANGED"
            );
            let revision = token
                .revision
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("SEARCH_TOKEN_INVALID"))?;
            ensure!(
                revision >= 0 && revision.to_string() == token.revision,
                "SEARCH_TOKEN_INVALID"
            );
            ensure!(
                generation.covered_revision.parse::<i64>()? >= revision,
                "SEARCH_NOT_VISIBLE"
            );
        }
        Ok(())
    }

    pub async fn query(
        &self,
        session: &VerifiedSession,
        resource: &str,
        request: SearchRequest,
        key: &str,
    ) -> Result<Value> {
        ensure!(
            !request.text.is_empty()
                && request.text.len() <= 16384
                && (1..=50).contains(&request.top_k),
            "SEARCH_INPUT_INVALID"
        );
        tokio::time::timeout(
            Duration::from_secs(30),
            self.query_attempt(session, resource, request, key),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("QUERY_TIMEOUT")))
    }

    async fn query_attempt(
        &self,
        session: &VerifiedSession,
        resource: &str,
        request: SearchRequest,
        key: &str,
    ) -> Result<Value> {
        let (generation, labels) = loop {
            let admission = Admission::begin(&self.db, session, false).await?;
            let result = async {
                let generation = Self::generation(admission.client(), session, resource).await?;
                Self::minimum(
                    session,
                    resource,
                    &generation,
                    request.minimum_token.as_ref(),
                )?;
                let candidates = NativeServing::protected_candidates(
                    admission.client(),
                    session,
                    &generation.definition.source,
                    std::slice::from_ref(&generation.definition.text_property),
                    Some(&generation.covered_revision),
                )
                .await?;
                ensure!(
                    candidates.binding_revision == generation.object_binding_revision,
                    "SCHEMA_CHANGED"
                );
                let mut labels: BTreeSet<String> = candidates.markings.into_iter().collect();
                labels.extend(generation.markings.iter().cloned());
                Authority::finish(admission.client(), session, &candidates.authority).await?;
                Ok((generation, labels))
            }
            .await;
            match admission.complete(result).await {
                Err(error) if error.to_string() == "SEARCH_NOT_VISIBLE" => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                result => break result?,
            }
        };
        let profile = self
            .models
            .capabilities(session, &generation.definition.embedding_model)
            .await?;
        ensure!(
            profile["modelBindingFingerprint"] == generation.model_fingerprint
                && profile["dimensions"] == generation.dimensions
                && profile["modelRevision"] == generation.model_revision,
            "SEARCH_MODEL_CHANGED"
        );
        let embedded = self
            .models
            .embed_labeled(
                session,
                &generation.definition.embedding_model,
                key,
                &json!({"input":[request.text]}),
                labels,
            )
            .await?;
        ensure!(
            embedded["modelBindingFingerprint"] == generation.model_fingerprint
                && embedded["modelRevision"] == generation.model_revision,
            "SEARCH_MODEL_CHANGED"
        );
        let vector = vector(&embedded["vectors"][0], generation.dimensions)?;
        let admission = Admission::begin(&self.db, session, false).await?;
        let result=async {
            let current=Self::generation(admission.client(),session,resource).await?;
            ensure!(current.id==generation.id && current.collection_revision==generation.collection_revision && current.model_fingerprint==generation.model_fingerprint,"SEARCH_GENERATION_CHANGED");
            Self::minimum(session,resource,&current,request.minimum_token.as_ref())?;
            let candidates=NativeServing::protected_candidates(admission.client(),session,&current.definition.source,std::slice::from_ref(&current.definition.text_property),Some(&current.covered_revision)).await?;
            ensure!(candidates.binding_revision==current.object_binding_revision && candidates.resource==current.definition.source && candidates.revision==current.covered_revision,"SCHEMA_CHANGED");
            let profile=self.models.capabilities_in(admission.client(),session,&current.definition.embedding_model).await?;
            ensure!(profile["modelBindingFingerprint"]==current.model_fingerprint,"SEARCH_MODEL_CHANGED");
            let mut parameters=candidates.parameters;
            let bind=|parameters:&mut Vec<SqlParameter>,value:String| {parameters.push(SqlParameter::Scalar(Some(value)));format!("${}::text",parameters.len())};
            let generation_id=bind(&mut parameters,current.id.to_string());
            let query_vector=bind(&mut parameters,vector);
            let field=bind(&mut parameters,current.definition.text_property.clone());
            let model=bind(&mut parameters,current.model_fingerprint.clone());
            let dimensions=bind(&mut parameters,current.dimensions.to_string());
            let limit=bind(&mut parameters,request.top_k.to_string());
            parameters.push(SqlParameter::Array(Some(candidates.authority.identity.markings.iter().cloned().map(Some).collect())));
            let allowed_markings=format!("${}::text[]",parameters.len());
            let sql=format!("SELECT d.uid,ch.chunk_id,ch.start_offset,ch.end_offset,e.text,d.source_revision::text,e.embedding OPERATOR(public.<=>) {query_vector}::public.vector AS distance FROM ({}) c JOIN moria.native_search_heads h ON h.generation={generation_id}::uuid AND h.uid=c.uid JOIN moria.native_search_documents d ON d.id=h.document JOIN moria.native_search_chunks ch ON ch.document=d.id JOIN moria.native_search_embeddings e ON e.id=ch.embedding WHERE d.source_digest=moria.identity_key_hash(c.effective->>{field}) AND e.model_fingerprint={model} AND e.dimensions={dimensions}::integer AND e.markings<@{allowed_markings} ORDER BY distance,ch.chunk_id COLLATE \"C\" LIMIT {limit}::bigint",candidates.sql);
            let params:Vec<_>=parameters.iter().map(SqlParameter::pg).collect();
            let rows=admission.client().query(&sql,&params).await?;
            let hits:Vec<Value>=rows.into_iter().map(|row|json!({"uid":row.get::<_,Uuid>(0),"chunkId":row.get::<_,String>(1),"start":row.get::<_,i32>(2),"end":row.get::<_,i32>(3),"text":row.get::<_,String>(4),"sourceRevision":row.get::<_,String>(5),"score":1.0-row.get::<_,f64>(6)})).collect();
            Authority::finish(admission.client(),session,&candidates.authority).await?;
            let token=VisibilityToken{environment:session.environment().into(),collection:resource.into(),generation:current.id,incarnation:current.incarnation,revision:current.covered_revision};
            let value=json!({"hits":hits,"visibility":token,"modelRevision":current.model_revision,"chunkerRevision":CHUNKER_REVISION});
            ensure!(serde_json::to_vec(&value)?.len()<=8*1024*1024,"RESPONSE_TOO_LARGE");
            Ok(value)
        }.await;
        admission.complete(result).await
    }
}

pub async fn cli(db: Database, arguments: Vec<String>) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let operation = arguments.first().map(String::as_str).unwrap_or("");
    if ["once", "worker", "drain"].contains(&operation) {
        let owner: Uuid = std::env::var("MORIA_SEARCH_WORKER_OWNER")?.parse()?;
        let host: Uuid = std::env::var("MORIA_SEARCH_HOST_ID")?.parse()?;
        db.get()
            .await?
            .query_one(
                "SELECT moria.runtime_worker_check($1,$2,NULL,'search')",
                &[&owner, &host],
            )
            .await?;
        let mut worker = worker::SearchWorker {
            search: NativeSearch {
                models: NativeModels::new(db.clone())?,
                db: db.clone(),
            },
            owner,
            host,
            schedule_after: None,
        };
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        loop {
            let iteration = tokio::select! {_=tokio::signal::ctrl_c()=>break,_=terminate.recv()=>break,result=worker.once()=>result};
            let worked = match iteration {
                Ok(worked) => worked,
                Err(error)
                    if operation != "once" && error_code(&error) == "CONCURRENT_MODIFICATION" =>
                {
                    eprintln!(
                        "{}",
                        json!({"event":"search_iteration_retry","error":"CONCURRENT_MODIFICATION"})
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if operation == "once" {
                println!("{}", json!({"worked":worked}));
                break;
            }
            if !worked {
                if operation=="drain"&&db.get().await?.query_one("SELECT count(*) FROM moria.runtime_executions WHERE spec->>'domain'='search' AND status IN ('queued','running','preparing')",&[]).await?.get::<_,i64>(0)==0 {break;}
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        return Ok(());
    }
    let mut bytes = Vec::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 65536, "SEARCH_INPUT_INVALID");
    let input: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        input.as_object().is_some_and(|fields| fields
            .keys()
            .all(|key| ["token", "resource", "key", "input"].contains(&key.as_str()))),
        "SEARCH_INPUT_INVALID"
    );
    let text = |field: &str| {
        input[field]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SEARCH_INPUT_INVALID"))
    };
    let session = Authority { db: db.clone() }
        .authenticate(text("token")?)
        .await?;
    ensure!(text("resource")?.len() <= 1024, "SEARCH_INPUT_INVALID");
    let service = NativeSearch {
        models: NativeModels::new(db.clone())?,
        db,
    };
    let result = match arguments.first().map(String::as_str) {
        Some("status") => {
            service
                .inspect_rebuild(
                    &session,
                    text("resource")?,
                    input["input"]["execution"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("SEARCH_INPUT_INVALID"))?
                        .parse()?,
                )
                .await?
        }
        Some("rebuild") => {
            service
                .request_rebuild(&session, text("resource")?, text("key")?)
                .await?
        }
        Some("query") => {
            service
                .query(
                    &session,
                    text("resource")?,
                    serde_json::from_value(input["input"].clone())?,
                    text("key")?,
                )
                .await?
        }
        Some("visibility") => service.visibility(&session, text("resource")?).await?,
        _ => anyhow::bail!("UNSUPPORTED_CAPABILITY"),
    };
    println!("{result}");
    Ok(())
}

pub(crate) fn error_code(error: &anyhow::Error) -> &'static str {
    domain_error_code(error).unwrap_or_else(|| crate::model_http::error_code(error))
}

pub(crate) fn domain_error_code(error: &anyhow::Error) -> Option<&'static str> {
    const CODES: &[&str] = &[
        "SEARCH_NOT_READY",
        "SEARCH_TOKEN_INVALID",
        "SEARCH_GENERATION_CHANGED",
        "SEARCH_NOT_VISIBLE",
        "SEARCH_INPUT_INVALID",
        "SEARCH_VECTOR_INVALID",
        "SEARCH_MODEL_CHANGED",
        "SEARCH_DOCUMENT_LIMIT",
        "SEARCH_CACHE_CONFLICT",
        "SEARCH_DOCUMENT_CONFLICT",
        "SEARCH_PROJECTOR_UNAVAILABLE",
        "SEARCH_SOURCE_CHANGED",
        "SEARCH_FRAME_INCOMPLETE",
        "SEARCH_GENERATION_INCOMPLETE",
        "FULL_SOURCE_PURPOSE_REVOKED",
        "SOURCE_RANGE_INVALID",
        "UNSUPPORTED_SOURCE_FIELD",
        "UNSUPPORTED_DATASET_BINDING",
        "REVISION_INVALID",
        "IMMUTABLE_SEARCH_EXECUTION",
    ];
    for cause in error.chain() {
        let text = cause.to_string();
        if let Some(code) = CODES.iter().find(|code| **code == text) {
            return Some(code);
        }
    }
    None
}

fn vector(value: &Value, dimensions: usize) -> Result<String> {
    let values = value
        .as_array()
        .filter(|values| values.len() == dimensions)
        .ok_or_else(|| anyhow::anyhow!("SEARCH_VECTOR_INVALID"))?;
    let normalized = values
        .iter()
        .map(|value| {
            value
                .as_f64()
                .filter(|value| value.is_finite() && value.abs() <= f32::MAX as f64)
                .map(|value| value as f32)
                .ok_or_else(|| anyhow::anyhow!("SEARCH_VECTOR_INVALID"))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        normalized.iter().any(|value| *value != 0.0),
        "SEARCH_VECTOR_INVALID"
    );
    Ok(serde_json::to_string(&normalized)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cosine_input_uses_actual_float32_dimension_and_nonzero_profile() {
        assert_eq!(vector(&json!([1, 0, 0]), 3).unwrap(), "[1.0,0.0,0.0]");
        for (value, dimensions) in [
            (json!([0, 0]), 2),
            (json!([1e-100, 0]), 2),
            (json!([1e100, 0]), 2),
            (json!([1, 0]), 3),
        ] {
            assert!(vector(&value, dimensions).is_err());
        }
    }
}
