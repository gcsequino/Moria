/** Native bounded snapshot preparation and atomic Dataset publication authority. */
export const snapshotRuntimeSchema = String.raw`
CREATE TABLE moria.native_storage_namespace_versions (
  warehouse_id text NOT NULL,table_id text NOT NULL,revision text NOT NULL,policy jsonb NOT NULL,
  PRIMARY KEY(warehouse_id,table_id,revision)
);
CREATE FUNCTION moria.native_storage_namespace_history_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_NAMESPACE_HISTORY'; END $$;
CREATE TRIGGER native_storage_namespace_history_immutable BEFORE UPDATE OR DELETE ON moria.native_storage_namespace_versions
  FOR EACH ROW EXECUTE FUNCTION moria.native_storage_namespace_history_immutable();
CREATE FUNCTION moria.native_storage_namespace_revision() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE fresh boolean;
BEGIN
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/storage-namespace/'||NEW.warehouse_id||'/'||NEW.table_id,0));
  IF to_regclass('moria.runtime_recovery_backends') IS NOT NULL THEN
    IF EXISTS(SELECT 1 FROM moria.runtime_recovery_backends b WHERE b.namespace->>'warehouseId'=NEW.warehouse_id
      AND b.namespace->>'tableId'=NEW.table_id) THEN RAISE EXCEPTION 'RECOVERY_NAMESPACE_RESERVED'; END IF;
  END IF;
  IF NEW.warehouse_id='filesystem' AND NEW.table_id='content' THEN RAISE EXCEPTION 'RESERVED_STORAGE_NAMESPACE'; END IF;
  IF octet_length(NEW.revision) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'INVALID_NAMESPACE_REVISION'; END IF;
  fresh:=TG_OP='INSERT';
  IF TG_OP='UPDATE' THEN
    IF ROW(NEW.warehouse_id,NEW.table_id) IS DISTINCT FROM ROW(OLD.warehouse_id,OLD.table_id) THEN
      RAISE EXCEPTION 'IMMUTABLE_NAMESPACE_IDENTITY'; END IF;
    fresh:=NEW.revision<>OLD.revision;
    IF NOT fresh AND (ROW(NEW.environment,NEW.resource,NEW.purpose,NEW.iceberg_uuid,NEW.markings)
      IS DISTINCT FROM ROW(OLD.environment,OLD.resource,OLD.purpose,OLD.iceberg_uuid,OLD.markings)
      OR (NOT OLD.enabled AND NEW.enabled)) THEN RAISE EXCEPTION 'FRESH_NAMESPACE_REVISION_REQUIRED'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_storage_namespace_revision BEFORE INSERT OR UPDATE OR DELETE ON moria.native_storage_namespaces
  FOR EACH ROW EXECUTE FUNCTION moria.native_storage_namespace_revision();
CREATE FUNCTION moria.native_storage_namespace_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='UPDATE' AND NEW.revision=OLD.revision THEN RETURN NEW; END IF;
  BEGIN
    INSERT INTO moria.native_storage_namespace_versions VALUES(NEW.warehouse_id,NEW.table_id,NEW.revision,to_jsonb(NEW)-'enabled'-'revision');
  EXCEPTION WHEN unique_violation THEN RAISE EXCEPTION 'NAMESPACE_REVISION_REUSED'; END;
  RETURN NEW;
END $$;
CREATE TRIGGER native_storage_namespace_record AFTER INSERT OR UPDATE ON moria.native_storage_namespaces
  FOR EACH ROW EXECUTE FUNCTION moria.native_storage_namespace_record();
CREATE TABLE moria.runtime_snapshot_candidates (
  id uuid PRIMARY KEY, environment text NOT NULL REFERENCES moria.environments(id),
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id), resource text NOT NULL,
  expected_revision bigint NOT NULL CHECK(expected_revision>=0), namespace_state jsonb NOT NULL,
  session_hash text NOT NULL, owner uuid NOT NULL, fence bigint NOT NULL, incarnation uuid NOT NULL,
  metadata_location text NOT NULL CHECK(left(metadata_location,18) IN ('moria-snapshot-v2:','moria-snapshot-v3:')),
  snapshot_id text NOT NULL,write_mode text NOT NULL DEFAULT 'replace' CHECK(write_mode IN ('replace','append','upsert','delete')), proof jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION moria.runtime_snapshot_candidate_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_SNAPSHOT_CANDIDATE'; END $$;
CREATE TRIGGER runtime_snapshot_candidate_immutable BEFORE UPDATE ON moria.runtime_snapshot_candidates
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_snapshot_candidate_immutable();
CREATE TABLE moria.runtime_snapshot_checkpoint_bindings (
  binding uuid PRIMARY KEY REFERENCES moria.runtime_execution_bindings(id),
  connection text NOT NULL, stream text NOT NULL, config_revision text NOT NULL
);

CREATE FUNCTION moria.runtime_dataset_labels_policy_state(state jsonb,resource_name text,fs_labels jsonb) RETURNS text[] LANGUAGE plpgsql
 SET search_path=pg_catalog,moria AS $$
DECLARE labels text[];
BEGIN
  IF jsonb_typeof(fs_labels) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  labels:=ARRAY(SELECT jsonb_array_elements_text(fs_labels));
  IF NOT (state->'identity'->'grantedOperations' ? (resource_name||':read'))
    OR NOT (state->'identity'->'applicationScopes' ? (resource_name||':read'))
    OR EXISTS(SELECT 1 FROM unnest(labels) label WHERE NOT (state->'identity'->'markings' ? label)) THEN
    RAISE EXCEPTION 'FORBIDDEN';
  END IF;
  RETURN labels;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_labels_policy_state(jsonb,text,jsonb) FROM PUBLIC;
CREATE FUNCTION moria.runtime_dataset_labels_state(state jsonb, resource_name text) RETURNS text[] LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.runtime_dataset_kind(state->>'environment',resource_name);
  RETURN moria.runtime_dataset_labels_policy_state(state,resource_name,moria.native_serving_resource_access_state(state,resource_name,'read'));
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_labels_state(jsonb,text) FROM PUBLIC;
CREATE FUNCTION moria.runtime_dataset_labels(session_hash text, resource_name text) RETURNS text[]
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
  RETURN moria.runtime_dataset_labels_state(moria.native_serving_session(session_hash),resource_name);
END $$;

CREATE FUNCTION moria.runtime_dataset_binding(env text,dataset_resource text,warehouse text,table_name text,namespace_revision text)
 RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE binding record;
BEGIN
  SELECT * INTO binding FROM moria.native_dataset_bindings WHERE environment=env AND resource::text=dataset_resource FOR SHARE;
  IF NOT FOUND THEN
    IF EXISTS(SELECT 1 FROM moria.fs_resource_identities WHERE resource_id::text=dataset_resource)
      THEN RAISE EXCEPTION 'DATASET_BINDING_INVALID'; END IF;
    RETURN NULL;
  END IF;
  IF NOT binding.enabled OR ROW(binding.warehouse_id,binding.table_id,binding.namespace_revision)
    IS DISTINCT FROM ROW(warehouse,table_name,namespace_revision) THEN RAISE EXCEPTION 'DATASET_BINDING_INVALID'; END IF;
  RETURN jsonb_build_object('revision',binding.revision,'icebergSchema',binding.iceberg_schema,'primaryKey',to_jsonb(binding.primary_key),'keySchemaHash',binding.key_schema_hash,'writeModes',to_jsonb(binding.write_modes),
    'physicalBindingFingerprint',binding.physical_binding_fingerprint);
END $$;
CREATE FUNCTION moria.runtime_dataset_io_check(session_hash text,execution_id uuid,task_id uuid,worker uuid,worker_fence bigint,
  instance_incarnation uuid,warehouse text,table_name text,namespace_revision text,for_write boolean) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; e moria.runtime_executions; b moria.runtime_execution_bindings; n moria.native_storage_namespaces;
  inherited text[]:='{}'; labels text[]; input jsonb; input_bindings jsonb:='[]'; output_target boolean;
  access_requests jsonb;access_results jsonb;label_index integer:=0;
  accepted_generation bigint;
BEGIN
  -- Publication IO has no task fence to lock; retain a shared execution fence.
  IF task_id IS NULL THEN
    PERFORM 1 FROM moria.runtime_executions WHERE id=execution_id FOR SHARE;
  ELSE
    -- Take the later task fence lock before resolving the accepted job.
    PERFORM 1 FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  END IF;
  state:=moria.native_serving_session(session_hash);
  IF state->>'privateValidatedExecution' IS DISTINCT FROM execution_id::text THEN
    PERFORM moria.runtime_execution_check(execution_id);
  END IF;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id;
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=e.binding;
  inherited:=b.markings;
  IF e.environment<>state->>'environment' OR e.principal<>state->>'principal' OR e.application<>state->>'application'
    OR NOT moria.runtime_dataset_session_matches(session_hash,execution_id,task_id,worker,worker_fence,instance_incarnation)
    OR b.security_epoch::text<>state->>'securityEpoch'
    OR b.instance_epoch::text<>state->>'instanceEpoch' OR b.incarnation<>instance_incarnation
    OR NOT EXISTS(SELECT 1 FROM moria.indexing_instance WHERE incarnation=instance_incarnation) THEN
    RAISE EXCEPTION 'AUTHORIZATION_CHANGED';
  END IF;
  IF task_id IS NOT NULL THEN
    IF NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE id=task_id AND execution=execution_id)
      THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
    IF NOT for_write AND state->>'privateValidatedExecution'=execution_id::text
      AND EXISTS(SELECT 1 FROM moria.runtime_job_sessions s WHERE s.token_hash=session_hash AND s.execution=execution_id
        AND s.task=task_id AND s.owner=worker AND s.fence=worker_fence AND s.incarnation=instance_incarnation) THEN
      -- The exact private session already validated this execution under the lock above.
      -- Keep the task and worker fence locks and recheck their live expiry.
      IF e.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'EXECUTION_INACTIVE'; END IF;
      SELECT t.worker_generation INTO accepted_generation FROM moria.runtime_execution_tasks t
        WHERE t.id=task_id AND t.execution=execution_id AND t.owner=worker AND t.fence=worker_fence
          AND t.status='running' AND t.lease_until>clock_timestamp() FOR UPDATE;
      IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
      IF accepted_generation IS NOT NULL THEN
        PERFORM 1 FROM moria.runtime_workers w WHERE w.owner=worker AND w.generation=accepted_generation
          AND w.enabled AND w.expires_at>clock_timestamp() FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
      END IF;
    ELSE
      PERFORM moria.runtime_task_check(task_id,worker,worker_fence);
    END IF;
  ELSIF e.status<>'preparing' OR e.owner IS DISTINCT FROM worker OR e.fence<>worker_fence OR e.lease_until<=clock_timestamp()
    THEN RAISE EXCEPTION 'LEASE_LOST';
  END IF;
  SELECT * INTO n FROM moria.native_storage_namespaces WHERE warehouse_id=warehouse AND table_id=table_name FOR SHARE;
  IF NOT FOUND OR n.environment<>e.environment OR NOT n.enabled OR n.revision<>namespace_revision
    OR n.purpose<>'dataset-source' OR n.iceberg_uuid IS NULL THEN RAISE EXCEPTION 'STORAGE_NAMESPACE_REVOKED'; END IF;
  output_target:=e.spec->'expectedHeads' ? n.resource;
  IF NOT output_target AND (for_write OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(e.spec->'pinnedInputs') p
    WHERE p->>'resource'=n.resource)) THEN RAISE EXCEPTION 'UNDECLARED_DATASET'; END IF;
  PERFORM moria.runtime_dataset_kind(e.environment,n.resource);
  access_requests:=jsonb_build_array(jsonb_build_object('resource',n.resource,'operation','read'));
  FOR input IN SELECT value FROM jsonb_array_elements(e.spec->'pinnedInputs') LOOP
    IF NOT EXISTS(SELECT 1 FROM moria.snapshot_history h WHERE h.environment=e.environment AND h.dataset=input->>'resource'
      AND h.revision::text=input->>'revision' AND h.snapshot_id=input->>'snapshot') THEN RAISE EXCEPTION 'INPUT_PIN_CHANGED'; END IF;
    PERFORM 1 FROM moria.native_storage_namespaces x WHERE x.environment=e.environment AND x.resource=input->>'resource'
      AND x.purpose='dataset-source' FOR SHARE;
    PERFORM moria.runtime_dataset_kind(e.environment,input->>'resource');
    access_requests:=access_requests||jsonb_build_array(jsonb_build_object('resource',input->>'resource','operation','read'));
    input_bindings:=input_bindings||COALESCE((SELECT jsonb_agg(jsonb_build_object('warehouseId',x.warehouse_id,'tableId',x.table_id,
      'revision',x.revision,'icebergUuid',x.iceberg_uuid,'markings',to_jsonb(x.markings)) ORDER BY x.warehouse_id,x.table_id)
      FROM moria.native_storage_namespaces x WHERE x.environment=e.environment AND x.resource=input->>'resource'
        AND x.purpose='dataset-source' AND x.enabled),'[]');
  END LOOP;
  IF output_target THEN access_requests:=access_requests||jsonb_build_array(jsonb_build_object('resource',n.resource,'operation','write')); END IF;
  access_results:=moria.native_serving_resource_access_batch_state(state,access_requests);
  IF jsonb_array_length(access_results)<>jsonb_array_length(access_requests) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  labels:=moria.runtime_dataset_labels_policy_state(state,n.resource,access_results->0);
  FOR input IN SELECT value FROM jsonb_array_elements(e.spec->'pinnedInputs') LOOP
    label_index:=label_index+1;
    inherited:=inherited||moria.runtime_dataset_labels_policy_state(state,input->>'resource',access_results->label_index);
  END LOOP;
  IF output_target THEN
    IF NOT (state->'identity'->'grantedOperations' ? (n.resource||':write'))
      OR NOT (state->'identity'->'applicationScopes' ? (n.resource||':write'))
      OR NOT inherited<@labels OR NOT inherited<@n.markings THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  END IF;
  PERFORM moria.runtime_dataset_gc_io_check(execution_id,task_id,worker,worker_fence,instance_incarnation,warehouse,table_name,namespace_revision);
  RETURN jsonb_build_object('environment',e.environment,'resource',n.resource,'warehouseId',warehouse,'tableId',table_name,
    'revision',n.revision,'icebergUuid',n.iceberg_uuid,'markings',to_jsonb(labels),
    'inheritedMarkings',to_jsonb(ARRAY(SELECT DISTINCT label FROM unnest(inherited) label ORDER BY label)),'inputBindings',input_bindings,
    'securityEpoch',state->>'securityEpoch','instanceEpoch',state->>'instanceEpoch','expectedRevision',e.spec->'expectedHeads'->>n.resource,
    'datasetBinding',moria.runtime_dataset_binding(e.environment,n.resource,warehouse,table_name,namespace_revision));
END $$;

CREATE FUNCTION moria.runtime_snapshot_publish(session_hash text,execution_id uuid,worker uuid,worker_fence bigint,
  instance_incarnation uuid,candidate_ids uuid[],checkpoint jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE state jsonb; e moria.runtime_executions; item moria.runtime_snapshot_candidates; current_state jsonb;
  old moria.publications; receipt jsonb; outputs jsonb:='[]'; lineage jsonb; request_hash text; revision bigint; updated bigint; env_name text; delta_candidate uuid;
BEGIN
  IF cardinality(candidate_ids) NOT BETWEEN 1 AND 64 OR cardinality(candidate_ids)<>(SELECT count(DISTINCT id) FROM unnest(candidate_ids) id)
    THEN RAISE EXCEPTION 'OUTPUT_INVALID'; END IF;
  SELECT environment INTO env_name FROM moria.runtime_executions WHERE id=execution_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'EXECUTION_UNKNOWN'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/environment/'||env_name,0));
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0));
  PERFORM 1 FROM moria.environments WHERE id=env_name FOR UPDATE;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  IF (SELECT count(*) FROM moria.runtime_snapshot_candidates WHERE id=ANY(candidate_ids) AND execution=execution_id)<>cardinality(candidate_ids)
    OR (SELECT count(DISTINCT resource) FROM moria.runtime_snapshot_candidates WHERE id=ANY(candidate_ids))<>cardinality(candidate_ids)
    OR cardinality(candidate_ids)<>(SELECT count(*) FROM jsonb_object_keys(e.spec->'expectedHeads')) THEN RAISE EXCEPTION 'OUTPUT_INVALID'; END IF;
  SELECT COALESCE(jsonb_object_agg(p->>'resource',p->>'revision'),'{}') INTO lineage FROM jsonb_array_elements(e.spec->'pinnedInputs') p;
  request_hash:=encode(sha256(convert_to(jsonb_build_object('candidates',(SELECT jsonb_agg(id ORDER BY resource) FROM moria.runtime_snapshot_candidates
    WHERE id=ANY(candidate_ids)),'checkpoint',checkpoint)::text,'UTF8')),'hex');
  SELECT * INTO old FROM moria.publications WHERE environment=e.environment AND execution=execution_id::text;
  IF old.request_hash IS NOT NULL AND EXISTS(SELECT 1 FROM moria.runtime_job_sessions WHERE token_hash=session_hash) THEN
    IF old.request_hash<>request_hash THEN RAISE EXCEPTION 'PUBLICATION_CONFLICT'; END IF;
    IF e.result IS DISTINCT FROM old.receipt OR e.status<>'succeeded'
      OR EXISTS(SELECT 1 FROM moria.runtime_snapshot_candidates WHERE id=ANY(candidate_ids)
        AND (runtime_snapshot_publish.session_hash IS DISTINCT FROM runtime_snapshot_candidates.session_hash OR owner<>worker OR fence<>worker_fence OR incarnation<>instance_incarnation)) THEN
      RAISE EXCEPTION 'CANDIDATE_STALE'; END IF;
    FOR item IN SELECT * FROM moria.runtime_snapshot_candidates WHERE id=ANY(candidate_ids) ORDER BY resource LOOP
      PERFORM moria.native_dataset_receipt_access(session_hash,execution_id,worker,worker_fence,item.resource,'write');
    END LOOP;
    RETURN old.receipt;
  END IF;
  state:=moria.native_serving_session(session_hash);
  PERFORM moria.runtime_execution_check(execution_id);
  IF e.environment<>state->>'environment'
    OR NOT moria.runtime_dataset_session_matches(session_hash,execution_id,NULL,worker,worker_fence,instance_incarnation)
    THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  IF old.request_hash IS NOT NULL THEN
    IF old.request_hash<>request_hash THEN RAISE EXCEPTION 'PUBLICATION_CONFLICT'; END IF;
    PERFORM moria.runtime_execution_complete(execution_id,worker,worker_fence,old.receipt);
    RETURN old.receipt;
  END IF;
  FOR item IN SELECT * FROM moria.runtime_snapshot_candidates WHERE id=ANY(candidate_ids) ORDER BY resource FOR SHARE LOOP
    current_state:=moria.runtime_dataset_io_check(session_hash,execution_id,NULL,worker,worker_fence,instance_incarnation,
      item.namespace_state->>'warehouseId',item.namespace_state->>'tableId',item.namespace_state->>'revision',true);
    IF current_state->'datasetBinding' IS NOT NULL AND current_state->'datasetBinding'<>'null'::jsonb
      AND NOT(current_state->'datasetBinding'->'writeModes' ? item.write_mode) THEN RAISE EXCEPTION 'DATASET_WRITE_MODE_DENIED'; END IF;
    IF item.session_hash<>session_hash OR item.owner<>worker OR item.fence<>worker_fence OR item.incarnation<>instance_incarnation
      OR item.namespace_state<>current_state OR e.spec->'expectedHeads'->>item.resource IS DISTINCT FROM item.expected_revision::text THEN
      RAISE EXCEPTION 'CANDIDATE_STALE'; END IF;
    UPDATE moria.dataset_heads SET revision=moria.dataset_heads.revision+1,metadata_location=item.metadata_location,snapshot_id=item.snapshot_id
      WHERE environment=e.environment AND dataset=item.resource AND moria.dataset_heads.revision=item.expected_revision RETURNING moria.dataset_heads.revision INTO updated;
    IF NOT FOUND THEN RAISE EXCEPTION 'HEAD_CONFLICT'; END IF;
    INSERT INTO moria.snapshot_history VALUES(e.environment,item.resource,updated,item.metadata_location,item.snapshot_id,execution_id::text,lineage);
    outputs:=outputs||jsonb_build_array(jsonb_build_object('dataset',item.resource,'revision',updated::text,'snapshotId',item.snapshot_id));
  END LOOP;
  IF checkpoint IS NOT NULL AND checkpoint<>'null'::jsonb THEN
    IF jsonb_typeof(checkpoint)<>'object' OR octet_length(checkpoint::text)>65536
      OR checkpoint->>'connection' IS NULL OR checkpoint->>'stream' IS NULL OR checkpoint->>'configRevision' IS NULL
      OR checkpoint->>'expectedVersion' !~ '^(0|[1-9][0-9]*)$' OR NOT (checkpoint ? 'value') THEN RAISE EXCEPTION 'CHECKPOINT_INVALID'; END IF;
    IF e.spec->>'domain'<>'sync' OR NOT EXISTS(SELECT 1 FROM moria.runtime_snapshot_checkpoint_bindings x
      WHERE x.binding=e.binding AND x.connection=checkpoint->>'connection' AND x.stream=checkpoint->>'stream'
        AND x.config_revision=checkpoint->>'configRevision') THEN RAISE EXCEPTION 'CHECKPOINT_FORBIDDEN'; END IF;
    INSERT INTO moria.checkpoints VALUES(e.environment,checkpoint->>'connection',checkpoint->>'stream',checkpoint->>'configRevision',0,'null') ON CONFLICT DO NOTHING;
    UPDATE moria.checkpoints SET version=version+1,value=checkpoint->'value' WHERE environment=e.environment
      AND connection=checkpoint->>'connection' AND stream=checkpoint->>'stream' AND config_revision=checkpoint->>'configRevision'
      AND version=(checkpoint->>'expectedVersion')::bigint;
    IF NOT FOUND THEN RAISE EXCEPTION 'CHECKPOINT_CONFLICT'; END IF;
  END IF;
  UPDATE moria.environments SET revision=moria.environments.revision+1 WHERE id=e.environment RETURNING moria.environments.revision INTO revision;
  receipt:=jsonb_build_object('revision',revision::text,'outputs',outputs);
  FOR current_state IN SELECT value FROM jsonb_array_elements(outputs) LOOP
    PERFORM moria.indexing_enqueue(e.environment,current_state->>'dataset',(current_state->>'revision')::bigint,revision);
  END LOOP;
  INSERT INTO moria.publications VALUES(e.environment,execution_id::text,request_hash,receipt);
  PERFORM moria.runtime_execution_complete(execution_id,worker,worker_fence,receipt);
  FOREACH delta_candidate IN ARRAY candidate_ids LOOP
    PERFORM moria.runtime_dataset_commit_record_execution(session_hash,execution_id,delta_candidate,receipt);
  END LOOP;
  RETURN receipt;
END $$;

CREATE TABLE moria.runtime_snapshot_verifications (
  key text PRIMARY KEY CHECK(key ~ '^sha256:[0-9a-f]{64}$'), environment text NOT NULL REFERENCES moria.environments(id),
  resource text NOT NULL, reference jsonb NOT NULL, binding_fingerprint text NOT NULL, reader_fingerprint text NOT NULL,
  status text NOT NULL CHECK(status IN ('verifying','ready')), owner uuid NOT NULL, fence bigint NOT NULL,
  lease_until timestamptz NOT NULL, root jsonb, root_version jsonb,
  verification_count bigint NOT NULL DEFAULT 0, verified_at timestamptz,
  CHECK((status='ready')=(root IS NOT NULL AND root_version IS NOT NULL AND verified_at IS NOT NULL))
);
CREATE TABLE moria.runtime_snapshot_verified_entries (
  verification text NOT NULL REFERENCES moria.runtime_snapshot_verifications(key), fence bigint NOT NULL,
  name text COLLATE "C" NOT NULL, content jsonb NOT NULL, physical_version jsonb NOT NULL,
  PRIMARY KEY(verification,fence,name)
);
CREATE FUNCTION moria.runtime_snapshot_verification_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.status='ready' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'IMMUTABLE_SNAPSHOT_ATTESTATION'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_snapshot_verification_immutable BEFORE UPDATE ON moria.runtime_snapshot_verifications
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_snapshot_verification_immutable();

CREATE FUNCTION moria.runtime_snapshot_verifier_check(verification_key text,verifier uuid,verifier_fence bigint) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM moria.runtime_snapshot_verifications WHERE key=verification_key AND owner=verifier AND fence=verifier_fence
    AND status='verifying' AND lease_until>clock_timestamp() FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'VERIFIER_FENCE_LOST'; END IF;
END $$;
`;
