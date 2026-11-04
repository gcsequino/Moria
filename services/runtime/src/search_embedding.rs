use super::{Generation, NativeSearch, vector};
use crate::{
    authority::{Authority, VerifiedSession},
    canonical::fingerprint,
    full_object_source::{FullObjectSourceGrant, FullObjectTextRow},
    retrieval::{CHUNKER_REVISION, TextChunk, chunks},
    serving::Admission,
};
use anyhow::{Result, ensure};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

impl NativeSearch {
    async fn cached_chunks(
        &self,
        session: &VerifiedSession,
        generation: &Generation,
        chunks: &[TextChunk],
        markings: &[String],
    ) -> Result<BTreeMap<Vec<u8>, Uuid>> {
        let digests: Vec<Vec<u8>> = chunks
            .iter()
            .map(|chunk| Sha256::digest(chunk.text.as_bytes()).to_vec())
            .collect();
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = async {
            let authority = Authority::current(admission.client(), session).await?;
            ensure!(markings.iter().all(|marking| authority.identity.markings.contains(marking)), "FORBIDDEN");
            let rows = admission.client().query(
                "SELECT id,text_digest,text FROM moria.native_search_embeddings WHERE model_fingerprint=$1 AND dimensions=$2 AND markings_hash=moria.native_search_marking_hash($3) AND markings=$3 AND text_digest=ANY($4::bytea[])",
                &[&generation.model_fingerprint,&(generation.dimensions as i32),&markings,&digests],
            ).await?;
            let mut found = BTreeMap::new();
            for row in rows {
                let digest: Vec<u8> = row.get(1);
                let text: String = row.get(2);
                ensure!(chunks.iter().zip(&digests).filter(|(_, hash)| **hash == digest).all(|(chunk, _)| chunk.text == text), "SEARCH_CACHE_CONFLICT");
                found.insert(digest, row.get(0));
            }
            Authority::finish(admission.client(), session, &authority).await?;
            Ok(found)
        }.await;
        admission.complete(result).await
    }

    async fn embed_chunks(
        &self,
        grant: &Arc<FullObjectSourceGrant>,
        session: &VerifiedSession,
        generation: &Generation,
        chunks: &[TextChunk],
        markings: &[String],
    ) -> Result<Vec<Uuid>> {
        let mut embeddings = Vec::with_capacity(chunks.len());
        for batch in chunks.chunks(64) {
            grant.check().await?;
            let mut found = self
                .cached_chunks(session, generation, batch, markings)
                .await?;
            let digests: Vec<Vec<u8>> = batch
                .iter()
                .map(|chunk| Sha256::digest(chunk.text.as_bytes()).to_vec())
                .collect();
            if digests.iter().any(|digest| !found.contains_key(digest)) {
                // The full deterministic batch key survives a lost receipt or a partially warm cache.
                let input =
                    json!({"input":batch.iter().map(|chunk| &chunk.text).collect::<Vec<_>>()});
                let key = fingerprint(
                    &json!({"purpose":"search-embedding/v1","format":CHUNKER_REVISION,"model":generation.model_fingerprint,"markings":markings,"input":input}),
                )?;
                grant.check().await?;
                let result = self
                    .models
                    .embed_projector(grant.clone(), &key, &input)
                    .await?;
                grant.check().await?;
                ensure!(
                    result["modelBindingFingerprint"] == generation.model_fingerprint
                        && result["modelRevision"] == generation.model_revision,
                    "SEARCH_MODEL_CHANGED"
                );
                ensure!(
                    result["markings"]
                        .as_array()
                        .is_some_and(|labels| labels.iter().all(|label| label
                            .as_str()
                            .is_some_and(|label| markings.iter().any(|marking| marking == label)))),
                    "AUTHORIZATION_CHANGED"
                );
                let vectors = result["vectors"]
                    .as_array()
                    .filter(|values| values.len() == batch.len())
                    .ok_or_else(|| anyhow::anyhow!("SEARCH_VECTOR_INVALID"))?;
                let admission = Admission::begin(&self.db, session, false).await?;
                let stored = async {
                    let authority = Authority::current(admission.client(), session).await?;
                    ensure!(markings.iter().all(|marking| authority.identity.markings.contains(marking)), "FORBIDDEN");
                    for ((chunk, digest), embedding) in batch.iter().zip(&digests).zip(vectors) {
                        let embedding = vector(embedding, generation.dimensions)?;
                        let row = admission.client().query_one(
                            "INSERT INTO moria.native_search_embeddings(model_fingerprint,text_digest,text,dimensions,embedding,markings) VALUES($1,$2,$3,$4,$5::text::public.vector,$6) ON CONFLICT(model_fingerprint,text_digest,dimensions,markings_hash) DO UPDATE SET markings=EXCLUDED.markings WHERE native_search_embeddings.text=EXCLUDED.text AND native_search_embeddings.markings=EXCLUDED.markings RETURNING id",
                            &[&generation.model_fingerprint,digest,&chunk.text,&(generation.dimensions as i32),&embedding,&markings],
                        ).await?;
                        found.insert(digest.clone(), row.get(0));
                    }
                    Authority::finish(admission.client(), session, &authority).await?;
                    Ok(())
                }.await;
                admission.complete(stored).await?;
            }
            for digest in digests {
                embeddings.push(
                    *found
                        .get(&digest)
                        .ok_or_else(|| anyhow::anyhow!("SEARCH_CACHE_CONFLICT"))?,
                );
            }
        }
        Ok(embeddings)
    }

    pub(super) async fn prepare_documents(
        &self,
        grant: &Arc<FullObjectSourceGrant>,
        session: &VerifiedSession,
        generation: &Generation,
        revision: i64,
        rows: &[FullObjectTextRow],
    ) -> Result<Vec<(Uuid, Option<Uuid>)>> {
        ensure!(rows.len() <= 64, "RESOURCE_LIMIT");
        let mut all = Vec::new();
        let mut ranges = Vec::with_capacity(rows.len());
        let mut bytes = 0_usize;
        for row in rows {
            if row.deleted {
                ranges.push(None);
                continue;
            }
            let start = all.len();
            let chunks = chunks(row.text.as_deref().unwrap_or(""))?;
            bytes += chunks.iter().map(|chunk| chunk.text.len()).sum::<usize>();
            ensure!(bytes <= 16 * 1024 * 1024, "RESOURCE_LIMIT");
            all.extend(chunks);
            ranges.push(Some(start..all.len()));
        }
        let embeddings = self
            .embed_chunks(grant, session, generation, &all, grant.markings())
            .await?;
        let mut prepared = Vec::with_capacity(rows.len());
        for (row, range) in rows.iter().zip(ranges) {
            grant.check().await?;
            let document = if let Some(range) = range {
                Some(
                    self.store_document(
                        session,
                        generation,
                        row.uid,
                        revision,
                        row.text.as_deref().unwrap_or(""),
                        &all[range.clone()],
                        &embeddings[range],
                    )
                    .await?,
                )
            } else {
                None
            };
            prepared.push((row.uid, document));
        }
        Ok(prepared)
    }

    #[allow(clippy::too_many_arguments)]
    async fn store_document(
        &self,
        session: &VerifiedSession,
        generation: &Generation,
        uid: Uuid,
        revision: i64,
        text: &str,
        chunks: &[TextChunk],
        embeddings: &[Uuid],
    ) -> Result<Uuid> {
        let digest = Sha256::digest(text.as_bytes()).to_vec();
        let ordinals: Vec<i32> = chunks.iter().map(|chunk| chunk.ordinal as i32).collect();
        let starts: Vec<i32> = chunks.iter().map(|chunk| chunk.start as i32).collect();
        let ends: Vec<i32> = chunks.iter().map(|chunk| chunk.end as i32).collect();
        let source_digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let ids: Vec<String> = chunks.iter().map(|chunk| fingerprint(&json!({"uid":uid,"sourceRevision":revision.to_string(),"sourceDigest":source_digest,"ordinal":chunk.ordinal,"text":format!("{:x}",Sha256::digest(chunk.text.as_bytes())),"start":chunk.start,"end":chunk.end,"chunker":CHUNKER_REVISION}))).collect::<Result<_>>()?;
        let admission = Admission::begin(&self.db, session, false).await?;
        let result = async {
            let authority = Authority::current(admission.client(), session).await?;
            let document: Uuid = admission.client().query_one(
                "INSERT INTO moria.native_search_documents(generation,uid,source_revision,source_digest,chunk_count) VALUES($1,$2,$3,$4,$5) ON CONFLICT(generation,uid,source_revision,source_digest) DO UPDATE SET chunk_count=EXCLUDED.chunk_count WHERE native_search_documents.chunk_count=EXCLUDED.chunk_count RETURNING id",
                &[&generation.id,&uid,&revision,&digest,&(chunks.len() as i32)],
            ).await?.get(0);
            admission.client().execute(
                "INSERT INTO moria.native_search_chunks(document,ordinal,chunk_id,start_offset,end_offset,embedding) SELECT $1,* FROM unnest($2::integer[],$3::text[],$4::integer[],$5::integer[],$6::uuid[]) ON CONFLICT(document,ordinal) DO NOTHING",
                &[&document,&ordinals,&ids,&starts,&ends,&embeddings],
            ).await?;
            let matches: bool = admission.client().query_one(
                "SELECT NOT EXISTS(SELECT 1 FROM unnest($2::integer[],$3::text[],$4::integer[],$5::integer[],$6::uuid[]) expected(ordinal,chunk_id,start_offset,end_offset,embedding) FULL JOIN (SELECT * FROM moria.native_search_chunks WHERE document=$1) actual USING(ordinal) WHERE expected.chunk_id IS DISTINCT FROM actual.chunk_id OR expected.start_offset IS DISTINCT FROM actual.start_offset OR expected.end_offset IS DISTINCT FROM actual.end_offset OR expected.embedding IS DISTINCT FROM actual.embedding)",
                &[&document,&ordinals,&ids,&starts,&ends,&embeddings],
            ).await?.get(0);
            ensure!(matches, "SEARCH_DOCUMENT_CONFLICT");
            Authority::finish(admission.client(), session, &authority).await?;
            Ok(document)
        }.await;
        admission.complete(result).await
    }
}
