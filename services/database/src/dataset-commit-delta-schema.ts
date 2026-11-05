/** Immutable Dataset publication edges; sparse mutation production remains disabled. */
export const datasetCommitDeltaSchema = String.raw`
ALTER TABLE moria.runtime_snapshot_candidates ADD COLUMN delta_profile jsonb;
CREATE TABLE moria.runtime_dataset_commit_deltas(
 version integer NOT NULL DEFAULT 1 CHECK(version=1),commit uuid NOT NULL,
 environment text NOT NULL,dataset text NOT NULL,
 basis_revision bigint NOT NULL CHECK(basis_revision>=0),basis_snapshot text,basis_metadata text,
 result_revision bigint NOT NULL CHECK(result_revision=basis_revision+1),result_snapshot text NOT NULL,result_metadata text NOT NULL,
 basis_root text REFERENCES moria.runtime_dataset_roots(key),result_root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),
 binding_revision text NOT NULL CHECK(binding_revision~'^sha256:[0-9a-f]{64}$'),
 dataset_schema_hash text NOT NULL CHECK(dataset_schema_hash~'^sha256:[0-9a-f]{64}$'),dataset_schema jsonb NOT NULL,
 key_schema_hash text NOT NULL CHECK(key_schema_hash~'^sha256:[0-9a-f]{64}$'),ordered_primary_key text[] NOT NULL,
 mode text CHECK(mode IN ('replace','append','upsert','delete')),
 change_kind text NOT NULL CHECK(change_kind IN ('replace','mutation','unchanged')),
 unchanged_reason text CHECK(unchanged_reason IN ('metadataPrune','compaction','noOp')),equivalence_receipt jsonb,
 complete_rows_root text REFERENCES moria.runtime_dataset_roots(key),delete_keys_root text REFERENCES moria.runtime_dataset_roots(key),
 producer_kind text NOT NULL CHECK(producer_kind IN ('execution','maintenance')),
 execution uuid REFERENCES moria.runtime_executions(id),candidate uuid REFERENCES moria.runtime_snapshot_candidates(id),
 maintenance uuid REFERENCES moria.runtime_dataset_maintenance_runs(id),policy uuid REFERENCES moria.runtime_dataset_retention_policies(id),
 sealed_receipt jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment,dataset,result_revision),UNIQUE(commit,dataset),
 CHECK((basis_revision=0 AND basis_snapshot IS NULL AND basis_metadata IS NULL AND basis_root IS NULL)
  OR (basis_revision>0 AND basis_snapshot IS NOT NULL AND basis_metadata IS NOT NULL AND basis_root IS NOT NULL)),
 CHECK((producer_kind='execution' AND mode IS NOT NULL AND execution IS NOT NULL AND candidate IS NOT NULL AND candidate=commit AND maintenance IS NULL AND policy IS NULL)
  OR (producer_kind='maintenance' AND mode IS NULL AND change_kind='unchanged' AND execution IS NULL AND candidate IS NULL AND maintenance IS NOT NULL AND maintenance=commit AND policy IS NOT NULL)),
 CHECK((change_kind='replace' AND mode='replace' AND complete_rows_root=result_root AND delete_keys_root IS NULL AND unchanged_reason IS NULL AND equivalence_receipt IS NULL)
  OR (change_kind='unchanged' AND unchanged_reason IS NOT NULL AND equivalence_receipt IS NOT NULL AND complete_rows_root IS NULL AND delete_keys_root IS NULL)),
 CHECK(jsonb_typeof(dataset_schema)='object' AND octet_length(dataset_schema::text)<=1048576),
 CHECK(octet_length(sealed_receipt::text)<=1048576 AND jsonb_typeof(sealed_receipt)='object'),
 CHECK(equivalence_receipt IS NULL OR (jsonb_typeof(equivalence_receipt)='object' AND octet_length(equivalence_receipt::text)<=16384))
);
CREATE TRIGGER runtime_dataset_commit_delta_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_commit_deltas
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE FUNCTION moria.runtime_dataset_commit_delta_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'DATASET_DELTA_PRODUCER_REQUIRED';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_commit_delta_owner BEFORE INSERT ON moria.runtime_dataset_commit_deltas
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_commit_delta_owner();

CREATE FUNCTION moria.runtime_dataset_commit_profile_check(env text,resource_name text,profile jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE binding moria.native_dataset_bindings;
BEGIN
 SELECT * INTO binding FROM moria.native_dataset_bindings WHERE environment=env AND resource::text=resource_name AND enabled FOR SHARE;
 IF NOT FOUND OR profile IS NULL OR jsonb_typeof(profile) IS DISTINCT FROM 'object'
  OR profile-ARRAY['datasetSchemaHash','datasetSchema','keySchemaHash','orderedPrimaryKey','bindingRevision']<>'{}'::jsonb
  OR profile->>'datasetSchemaHash' IS NULL OR profile->>'datasetSchemaHash' !~ '^sha256:[0-9a-f]{64}$'
  OR jsonb_typeof(profile->'datasetSchema') IS DISTINCT FROM 'object'
  OR profile->>'bindingRevision' IS DISTINCT FROM binding.revision
  OR profile->>'keySchemaHash' IS DISTINCT FROM binding.key_schema_hash
  OR profile->'orderedPrimaryKey' IS DISTINCT FROM to_jsonb(binding.primary_key)
 THEN RAISE EXCEPTION 'DATASET_DELTA_PROFILE_CHANGED';END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_commit_root_check(env text,resource_name text,location text,snapshot text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE binding moria.native_dataset_bindings;root_key text;
BEGIN
 SELECT * INTO binding FROM moria.native_dataset_bindings WHERE environment=env AND resource::text=resource_name AND enabled FOR SHARE;
 root_key:=moria.runtime_dataset_root_key(location);
 IF binding.resource IS NULL OR NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots r
  JOIN moria.runtime_dataset_root_locations l ON l.root=r.key
  WHERE r.key=root_key AND r.environment=env AND r.dataset=resource_name AND r.metadata_location=location AND r.snapshot_id=snapshot
   AND r.sealed AND l.sealed AND l.physical_fingerprint=binding.physical_binding_fingerprint
   AND l.incarnation=(SELECT incarnation FROM moria.indexing_instance)
   AND l.namespace->>'warehouseId'=binding.warehouse_id AND l.namespace->>'tableId'=binding.table_id
   AND l.namespace->>'revision'=binding.namespace_revision)
 THEN RAISE EXCEPTION 'DATASET_DELTA_ROOT_UNQUALIFIED';END IF;
 RETURN root_key;
END $$;
CREATE FUNCTION moria.runtime_dataset_commit_record_execution(session_hash text,execution_id uuid,candidate_id uuid,publication_receipt jsonb) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE c moria.runtime_snapshot_candidates;e moria.runtime_executions;basis moria.snapshot_history;
 result_key text;basis_key text;profile jsonb;state jsonb;
BEGIN
 SELECT * INTO c FROM moria.runtime_snapshot_candidates WHERE id=candidate_id AND execution=execution_id;
 SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id;
 IF c.id IS NULL OR e.id IS NULL OR e.status<>'succeeded' OR e.result IS DISTINCT FROM publication_receipt
  OR NOT EXISTS(SELECT 1 FROM moria.publications p WHERE p.environment=e.environment AND p.execution=execution_id::text AND p.receipt=publication_receipt)
  OR NOT EXISTS(SELECT 1 FROM moria.snapshot_history h WHERE h.environment=c.environment AND h.dataset=c.resource
   AND h.revision=c.expected_revision+1 AND h.metadata_location=c.metadata_location AND h.snapshot_id=c.snapshot_id AND h.execution=execution_id::text)
 THEN RAISE EXCEPTION 'DATASET_DELTA_PUBLICATION_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.native_dataset_bindings WHERE environment=c.environment AND resource::text=c.resource) THEN RETURN;END IF;
 IF c.session_hash IS DISTINCT FROM session_hash OR c.incarnation IS DISTINCT FROM (SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'CANDIDATE_STALE';END IF;
 IF EXISTS(SELECT 1 FROM moria.runtime_job_sessions WHERE token_hash=session_hash) THEN
  PERFORM moria.native_dataset_receipt_access(session_hash,execution_id,c.owner,c.fence,c.resource,'write');
 ELSE
  state:=moria.native_serving_session(session_hash);
  IF ROW(state->>'environment',state->>'principal',state->>'application') IS DISTINCT FROM ROW(e.environment,e.principal,e.application)
   THEN RAISE EXCEPTION 'FORBIDDEN';END IF;
  PERFORM moria.runtime_dataset_labels_state(state,c.resource);
  PERFORM moria.native_resource_filesystem_access_state(state,c.resource,'write');
 END IF;
 IF c.write_mode<>'replace' THEN RAISE EXCEPTION 'DATASET_DELTA_PRODUCER_UNSUPPORTED';END IF;
 profile:=c.delta_profile;
 PERFORM moria.runtime_dataset_commit_profile_check(c.environment,c.resource,profile);
 result_key:=moria.runtime_dataset_commit_root_check(c.environment,c.resource,c.metadata_location,c.snapshot_id);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_producers p ON p.scope=r.producer
  WHERE r.key=result_key AND p.execution=execution_id AND p.task IS NULL AND p.owner=c.owner AND p.fence=c.fence AND p.incarnation=c.incarnation)
 THEN RAISE EXCEPTION 'DATASET_DELTA_PRODUCER_REQUIRED';END IF;
 IF c.expected_revision>0 THEN
  SELECT * INTO basis FROM moria.snapshot_history WHERE environment=c.environment AND dataset=c.resource AND revision=c.expected_revision;
  IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_DELTA_CHAIN_MISSING';END IF;
  basis_key:=moria.runtime_dataset_commit_root_check(c.environment,c.resource,basis.metadata_location,basis.snapshot_id);
 END IF;
 INSERT INTO moria.runtime_dataset_commit_deltas(commit,environment,dataset,basis_revision,basis_snapshot,basis_metadata,result_revision,result_snapshot,result_metadata,
  basis_root,result_root,binding_revision,dataset_schema_hash,dataset_schema,key_schema_hash,ordered_primary_key,mode,change_kind,complete_rows_root,producer_kind,execution,candidate,sealed_receipt)
 VALUES(c.id,c.environment,c.resource,c.expected_revision,basis.snapshot_id,basis.metadata_location,c.expected_revision+1,c.snapshot_id,c.metadata_location,
  basis_key,result_key,profile->>'bindingRevision',profile->>'datasetSchemaHash',profile->'datasetSchema',profile->>'keySchemaHash',ARRAY(SELECT jsonb_array_elements_text(profile->'orderedPrimaryKey')),
  c.write_mode,'replace',result_key,'execution',execution_id,c.id,publication_receipt);
END $$;
CREATE FUNCTION moria.runtime_dataset_commit_record_maintenance(run_id uuid,profile jsonb,equivalence jsonb,publication_receipt jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE run moria.runtime_dataset_maintenance_runs;policy_row moria.runtime_dataset_retention_policies;basis moria.snapshot_history;result moria.snapshot_history;
 basis_key text;result_key text;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 SELECT * INTO run FROM moria.runtime_dataset_maintenance_runs WHERE id=run_id;
 SELECT * INTO policy_row FROM moria.runtime_dataset_retention_policies WHERE id=run.policy;
 SELECT * INTO basis FROM moria.snapshot_history WHERE environment=policy_row.environment AND dataset=policy_row.dataset AND revision=run.expected_revision;
 SELECT * INTO result FROM moria.snapshot_history WHERE environment=policy_row.environment AND dataset=policy_row.dataset AND revision=run.expected_revision+1;
 IF run.id IS NULL OR run.status<>'committed' OR run.receipt IS DISTINCT FROM publication_receipt OR basis.revision IS NULL OR result.revision IS NULL
  OR basis.metadata_location IS DISTINCT FROM run.old_metadata OR result.metadata_location IS DISTINCT FROM run.new_metadata
  OR result.execution IS DISTINCT FROM 'dataset-maintenance/'||run.id::text OR result.snapshot_id IS DISTINCT FROM basis.snapshot_id
  OR NOT EXISTS(SELECT 1 FROM moria.publications p WHERE p.environment=policy_row.environment AND p.execution=result.execution AND p.receipt=publication_receipt)
 THEN RAISE EXCEPTION 'DATASET_DELTA_PUBLICATION_REQUIRED';END IF;
 PERFORM moria.runtime_dataset_commit_profile_check(policy_row.environment,policy_row.dataset,profile);
 basis_key:=moria.runtime_dataset_commit_root_check(policy_row.environment,policy_row.dataset,basis.metadata_location,basis.snapshot_id);
 result_key:=moria.runtime_dataset_commit_root_check(policy_row.environment,policy_row.dataset,result.metadata_location,result.snapshot_id);
 IF jsonb_typeof(equivalence) IS DISTINCT FROM 'object' OR equivalence-ARRAY['version','kind','basisRoot','resultRoot','snapshot','currentSnapshotHash']<>'{}'::jsonb
  OR equivalence->>'version' IS DISTINCT FROM '1' OR equivalence->>'kind' IS DISTINCT FROM 'metadataPrune'
  OR equivalence->>'basisRoot' IS DISTINCT FROM basis_key OR equivalence->>'resultRoot' IS DISTINCT FROM result_key
  OR equivalence->>'snapshot' IS DISTINCT FROM result.snapshot_id OR equivalence->>'currentSnapshotHash' IS NULL OR equivalence->>'currentSnapshotHash' !~ '^sha256:[0-9a-f]{64}$'
  OR NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_producers p ON p.scope=r.producer WHERE r.key=result_key AND p.maintenance=run_id)
 THEN RAISE EXCEPTION 'DATASET_DELTA_EQUIVALENCE_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_commit_deltas(commit,environment,dataset,basis_revision,basis_snapshot,basis_metadata,result_revision,result_snapshot,result_metadata,
  basis_root,result_root,binding_revision,dataset_schema_hash,dataset_schema,key_schema_hash,ordered_primary_key,mode,change_kind,unchanged_reason,equivalence_receipt,producer_kind,maintenance,policy,sealed_receipt)
 VALUES(run.id,policy_row.environment,policy_row.dataset,basis.revision,basis.snapshot_id,basis.metadata_location,result.revision,result.snapshot_id,result.metadata_location,
  basis_key,result_key,profile->>'bindingRevision',profile->>'datasetSchemaHash',profile->'datasetSchema',profile->>'keySchemaHash',ARRAY(SELECT jsonb_array_elements_text(profile->'orderedPrimaryKey')),
  NULL,'unchanged','metadataPrune',equivalence,'maintenance',run.id,run.policy,publication_receipt);
 RETURN (SELECT jsonb_build_object('producerKind',d.producer_kind,'changeKind',d.change_kind,'mode',d.mode,
  'unchangedReason',d.unchanged_reason,'equivalenceReceipt',d.equivalence_receipt)
  FROM moria.runtime_dataset_commit_deltas d WHERE d.commit=run_id AND d.environment=policy_row.environment AND d.dataset=policy_row.dataset);
END $$;
CREATE FUNCTION moria.runtime_dataset_commit_edge_required() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.revision>0 AND EXISTS(SELECT 1 FROM moria.native_dataset_bindings WHERE environment=NEW.environment AND resource::text=NEW.dataset)
  AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_commit_deltas d WHERE d.environment=NEW.environment AND d.dataset=NEW.dataset
   AND d.result_revision=NEW.revision AND d.result_metadata=NEW.metadata_location AND d.result_snapshot=NEW.snapshot_id)
 THEN RAISE EXCEPTION 'DATASET_DELTA_PUBLICATION_REQUIRED';END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER runtime_dataset_commit_edge_required AFTER INSERT OR UPDATE ON moria.dataset_heads
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_commit_edge_required();
REVOKE ALL ON FUNCTION moria.runtime_dataset_commit_profile_check(text,text,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_commit_root_check(text,text,text,text) FROM PUBLIC;
`;
