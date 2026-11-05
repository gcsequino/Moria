//! Backup traverses immutable graph edges once per node, with durable bounded admission.
use super::*;
use serde_json::json;

impl Capture {
    async fn graph_admit(&self, nodes: &[String], physical: &str, incarnation: Uuid) -> Result<()> {
        ensure!(nodes.len() <= 256, "RECOVERY_CONTENT_LIMIT");
        let mut client = self.db.get().await?;
        let tx = client.transaction().await?;
        self.lease.check_in(tx.client()).await?;
        let inserted = tx.query(
            "INSERT INTO moria.runtime_recovery_graph_nodes(recovery_set,node,physical_fingerprint,incarnation) SELECT $1,node,$2,$3 FROM unnest($4::text[]) node ON CONFLICT DO NOTHING RETURNING node",
            &[&self.lease.set_id(), &physical, &incarnation, &nodes],
        ).await?;
        let mut bytes = 0;
        for row in inserted {
            bytes += serde_json::to_vec(&json!({"set":self.lease.set_id(),"node":row.get::<_,String>(0),"physical":physical,"incarnation":incarnation}))?.len();
        }
        if bytes != 0 {
            charge_state(tx.client(), self.lease.set_id(), bytes).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn graph(
        &self,
        snapshot: &Client,
        metadata: &str,
        reader: &dyn storage::ObjectReader,
    ) -> Result<()> {
        let physical = reader.physical_fingerprint()?;
        let reference = NativeSnapshotRef::decode(metadata)?;
        let warehouse = &reference.root.object.warehouse_id;
        let table = &reference.root.object.table_id;
        let incarnation: Uuid = snapshot
            .query_one("SELECT incarnation FROM moria.indexing_instance", &[])
            .await?
            .get(0);
        let mut after = None::<String>;
        let mut root_count = 0usize;
        loop {
            self.lease.check().await?;
            let roots = snapshot.query(
                "SELECT g.node FROM moria.runtime_dataset_graph_roots g JOIN moria.runtime_dataset_graph_nodes n ON n.id=g.node JOIN moria.runtime_dataset_roots r ON r.key=g.root JOIN moria.runtime_dataset_root_locations l ON l.root=r.key AND l.physical_fingerprint=g.physical_fingerprint AND l.incarnation=g.incarnation AND l.sealed WHERE g.root=moria.runtime_dataset_root_key($1) AND r.metadata_location=$1 AND r.root->'root'=$5 AND r.root->>'version'=$6::text AND r.sealed AND n.environment=r.environment AND n.dataset=r.dataset AND n.profile=g.profile AND n.kind='root' AND g.physical_fingerprint=$2 AND g.incarnation=$3 AND ($4::text IS NULL OR g.node>$4) AND (($6='2' AND r.reader_profile='iceberg-v2-cow-flat-bounded-v2') OR ($6='3' AND r.reader_profile='iceberg-v2-native-mor-bounded-v3' AND n.content=$5 AND g.profile->>'readerProfile'=r.reader_profile)) ORDER BY g.node LIMIT 128",
                &[&metadata,&physical,&incarnation,&after,&serde_json::to_value(&reference.root)?,&reference.version.to_string()],
            ).await?;
            if roots.is_empty() {
                break;
            }
            let nodes = roots
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>();
            root_count += nodes.len();
            self.graph_admit(&nodes, &physical, incarnation).await?;
            after = nodes.last().cloned();
        }
        ensure!(
            reference.version != 3 || root_count == 1,
            "RECOVERY_CONTENT_MISMATCH"
        );
        loop {
            self.cancel.check()?;
            self.lease.check().await?;
            let pending = self.db.get().await?.query(
                "SELECT p.node FROM moria.runtime_recovery_graph_nodes p JOIN moria.runtime_dataset_graph_nodes n ON n.id=p.node WHERE p.recovery_set=$1 AND p.physical_fingerprint=$2 AND p.incarnation=$3 AND NOT p.processed AND n.content->'object'->>'warehouseId'=$4 AND n.content->'object'->>'tableId'=$5 ORDER BY p.node LIMIT 128",
                &[&self.lease.set_id(), &physical, &incarnation, &warehouse, &table],
            ).await?;
            if pending.is_empty() {
                break;
            }
            for pending in pending {
                let node: String = pending.get(0);
                let row = snapshot.query_one(
                    "SELECT n.content,q.physical_version,n.environment,n.dataset,n.profile,n.height FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id WHERE n.id=$1 AND q.physical_fingerprint=$2 AND q.incarnation=$3",
                    &[&node, &physical, &incarnation],
                ).await?;
                let content: SealedObject = serde_json::from_value(row.get(0))?;
                let expected: storage::ObjectVersion = serde_json::from_value(row.get(1))?;
                let environment: String = row.get(2);
                let dataset: String = row.get(3);
                let profile: Value = row.get(4);
                let height: i32 = row.get(5);
                self.member(&content).await?;
                ensure!(expected.size == content.size, "RECOVERY_CONTENT_MISMATCH");
                // Existence and bytes are checked at the exact qualified version; current HEAD may be newer.
                reader
                    .range_at_version(
                        &content.object,
                        0..content.size.min(1),
                        &expected,
                        self.source.as_ref(),
                        &self.cancel,
                    )
                    .await?;
                let edges = snapshot.query(
                    "SELECT e.child,c.environment,c.dataset,c.profile,c.height,q.node IS NOT NULL FROM moria.runtime_dataset_graph_edges e JOIN moria.runtime_dataset_graph_nodes c ON c.id=e.child LEFT JOIN moria.runtime_dataset_graph_qualifications q ON q.node=c.id AND q.physical_fingerprint=$2 AND q.incarnation=$3 WHERE e.parent=$1 ORDER BY e.label LIMIT 257",
                    &[&node, &physical, &incarnation],
                ).await?;
                ensure!(edges.len() <= 256, "RECOVERY_CONTENT_LIMIT");
                let mut children = Vec::with_capacity(edges.len());
                for edge in edges {
                    ensure!(
                        edge.get::<_, String>(1) == environment
                            && edge.get::<_, String>(2) == dataset
                            && edge.get::<_, Value>(3) == profile
                            && edge.get::<_, i32>(4) < height
                            && edge.get::<_, bool>(5),
                        "RECOVERY_CONTENT_MISMATCH"
                    );
                    children.push(edge.get::<_, String>(0));
                }
                self.graph_admit(&children, &physical, incarnation).await?;
                let mut client = self.db.get().await?;
                let tx = client.transaction().await?;
                self.lease.check_in(tx.client()).await?;
                let version = serde_json::to_value(&expected)?;
                if tx.execute("UPDATE moria.runtime_recovery_members SET source_version=$5 WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4 AND source_version IS NULL", &[&self.lease.set_id(), &content.object.warehouse_id, &content.object.table_id, &content.object.object_id, &version]).await? == 1 {
                    charge_state(tx.client(), self.lease.set_id(), serde_json::to_vec(&version)?.len()).await?;
                }
                ensure!(tx.query_one("SELECT source_version=$5 FROM moria.runtime_recovery_members WHERE recovery_set=$1 AND warehouse_id=$2 AND table_id=$3 AND object_id=$4", &[&self.lease.set_id(), &content.object.warehouse_id, &content.object.table_id, &content.object.object_id, &version]).await?.get::<_,bool>(0), "STORAGE_VERSION_CHANGED");
                ensure!(tx.execute("UPDATE moria.runtime_recovery_graph_nodes SET processed=true WHERE recovery_set=$1 AND node=$2 AND physical_fingerprint=$3 AND incarnation=$4", &[&self.lease.set_id(), &node, &physical, &incarnation]).await? == 1, "RECOVERY_CONTENT_MISMATCH");
                tx.commit().await?;
            }
        }
        Ok(())
    }
}
