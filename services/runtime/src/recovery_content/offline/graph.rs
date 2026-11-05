//! Restore requalifies copied graph bytes; logical descriptors never carry provider versions.
use super::*;

impl RecoveredContentProof {
    pub(super) async fn qualify_graph_nodes_in(
        &self,
        client: &Client,
        restore: Uuid,
        incarnation: Uuid,
        previous: Uuid,
        values: &Value,
    ) -> Result<()> {
        client.execute(
            "INSERT INTO moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation,physical_version,producer) SELECT DISTINCT n.id,v->>'physicalFingerprint',$3,v->'physicalVersion',q.producer FROM jsonb_array_elements($1) v JOIN moria.runtime_dataset_graph_nodes n ON n.content->'object'->>'warehouseId'=v->'content'->'object'->>'warehouseId' AND n.content->'object'->>'tableId'=v->'content'->'object'->>'tableId' AND n.content->'object'->>'objectId'=v->'content'->'object'->>'objectId' AND n.content=v->'content' JOIN moria.runtime_dataset_graph_qualifications source ON source.node=n.id AND source.physical_fingerprint=v->>'sourcePhysicalFingerprint' AND source.incarnation=$4 JOIN LATERAL(SELECT q.producer FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_qualifications q ON q.root=r.key WHERE r.environment=n.environment AND r.dataset=n.dataset AND q.restore=$2 AND q.incarnation=$3 AND q.physical_fingerprint=v->>'physicalFingerprint' AND q.namespace->>'warehouseId'=n.content->'object'->>'warehouseId' AND q.namespace->>'tableId'=n.content->'object'->>'tableId' LIMIT 1) q ON true ON CONFLICT DO NOTHING",
            &[&values, &restore, &incarnation, &previous],
        ).await?;
        ensure!(!client.query_one(
            "SELECT EXISTS(SELECT 1 FROM jsonb_array_elements($1) v JOIN moria.runtime_dataset_graph_nodes n ON n.content->'object'->>'warehouseId'=v->'content'->'object'->>'warehouseId' AND n.content->'object'->>'tableId'=v->'content'->'object'->>'tableId' AND n.content->'object'->>'objectId'=v->'content'->'object'->>'objectId' AND n.content=v->'content' JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id AND q.physical_fingerprint=v->>'physicalFingerprint' AND q.incarnation=$2 WHERE q.physical_version<>v->'physicalVersion' OR NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_producers p WHERE p.scope=q.producer AND p.restore=$3 AND p.incarnation=$2 AND p.physical_fingerprint=q.physical_fingerprint))",
            &[&values, &incarnation, &restore],
        ).await?.get::<_,bool>(0), "RECOVERY_CONTENT_MISMATCH");
        Ok(())
    }

    pub(super) async fn qualify_graph_roots_in(
        &self,
        client: &Client,
        restore: Uuid,
        incarnation: Uuid,
        previous: Uuid,
    ) -> Result<()> {
        for namespace in self
            .namespaces
            .iter()
            .filter(|value| value.kind == "dataset")
        {
            ensure!(!client.query_one(
                "SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_root_qualifications q ON q.root=g.root AND q.restore=$1 AND q.incarnation=$2 AND q.physical_fingerprint=$3 AND q.sealed WHERE g.physical_fingerprint=$4 AND g.incarnation=$5 AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_qualifications n WHERE n.node=g.node AND n.physical_fingerprint=q.physical_fingerprint AND n.incarnation=q.incarnation))",
                &[&restore, &incarnation, &namespace.physical_fingerprint, &namespace.source_physical_fingerprint, &previous],
            ).await?.get::<_,bool>(0), "RECOVERY_CONTENT_MISMATCH");
            client.execute(
                "INSERT INTO moria.runtime_dataset_graph_roots(root,node,physical_fingerprint,incarnation,profile) SELECT g.root,g.node,q.physical_fingerprint,q.incarnation,g.profile FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_root_qualifications q ON q.root=g.root AND q.restore=$1 AND q.incarnation=$2 AND q.physical_fingerprint=$3 AND q.sealed WHERE g.physical_fingerprint=$4 AND g.incarnation=$5 ON CONFLICT DO NOTHING",
                &[&restore, &incarnation, &namespace.physical_fingerprint, &namespace.source_physical_fingerprint, &previous],
            ).await?;
        }
        client.execute(
            "WITH added AS(INSERT INTO moria.runtime_dataset_graph_references(kind,owner,node,physical_fingerprint,incarnation) SELECT 'history',g.root,g.node,g.physical_fingerprint,g.incarnation FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_root_qualifications q ON q.root=g.root AND q.physical_fingerprint=g.physical_fingerprint AND q.incarnation=g.incarnation AND q.restore=$1 AND q.incarnation=$2 AND q.sealed ON CONFLICT DO NOTHING RETURNING node,physical_fingerprint,incarnation) INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta) SELECT n.environment,n.dataset,$1,a.node,a.physical_fingerprint,a.incarnation,1 FROM added a JOIN moria.runtime_dataset_graph_nodes n ON n.id=a.node",
            &[&restore, &incarnation],
        ).await?;
        loop {
            self.custody.cancel.check()?;
            ensure!(
                tokio::time::Instant::now() < self.custody.deadline,
                "RECOVERY_CONTENT_DEADLINE"
            );
            let changed: i32 = client
                .query_one(
                    "SELECT moria.runtime_dataset_graph_step($1,256)",
                    &[&restore],
                )
                .await?
                .get(0);
            let pending: bool = client.query_one("SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE origin=$1 AND processed_at IS NULL)", &[&restore]).await?.get(0);
            if !pending {
                break;
            }
            ensure!(changed > 0, "RECOVERY_CONTENT_MISMATCH");
        }
        ensure!(!client.query_one(
            "SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_qualifications q JOIN moria.runtime_dataset_producers p ON p.scope=q.producer JOIN moria.runtime_dataset_graph_nodes n ON n.id=q.node JOIN moria.runtime_dataset_issued_objects o ON o.physical_fingerprint=q.physical_fingerprint AND o.warehouse_id=n.content->'object'->>'warehouseId' AND o.table_id=n.content->'object'->>'tableId' AND o.object_id=n.content->'object'->>'objectId' WHERE p.restore=$1 AND q.incarnation=$2 AND (o.producer<>q.producer OR o.expected_size<>(n.content->>'size')::bigint OR o.expected_sha256<>n.content->>'sha256'))",
            &[&restore, &incarnation],
        ).await?.get::<_,bool>(0), "RECOVERY_TARGET_NOT_FRESH");
        client.execute(
            "INSERT INTO moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id,producer,expected_size,expected_sha256) SELECT q.physical_fingerprint,n.content->'object'->>'warehouseId',n.content->'object'->>'tableId',n.content->'object'->>'objectId',q.producer,(n.content->>'size')::bigint,n.content->>'sha256' FROM moria.runtime_dataset_graph_qualifications q JOIN moria.runtime_dataset_producers p ON p.scope=q.producer JOIN moria.runtime_dataset_graph_nodes n ON n.id=q.node WHERE p.restore=$1 AND q.incarnation=$2 ON CONFLICT DO NOTHING",
            &[&restore, &incarnation],
        ).await?;
        Ok(())
    }
}
