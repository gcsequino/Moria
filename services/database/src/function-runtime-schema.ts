/** Trusted release bindings and a durable single-attempt OCI supervision journal. */
export const functionRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_sandbox_bindings (
  binding uuid PRIMARY KEY REFERENCES moria.runtime_execution_bindings(id),
  domain text NOT NULL DEFAULT 'function' CHECK(domain IN ('function','agent','evaluator','transform','sync')),
  artifact_uri text NOT NULL, artifact_digest text NOT NULL,
  outputs jsonb NOT NULL CHECK(jsonb_typeof(outputs)='object'),
  effect text NOT NULL CHECK(effect IN ('readOnly','readWrite','editProposal')),
  memory_mib integer NOT NULL CHECK(memory_mib BETWEEN 32 AND 4096),
  cpu_millis integer NOT NULL CHECK(cpu_millis BETWEEN 1 AND 86400000),
  timeout_seconds integer NOT NULL CHECK(timeout_seconds BETWEEN 1 AND 86400),
  value_version integer NOT NULL DEFAULT 1 CHECK(value_version=1),
  UNIQUE(binding,domain),
  CHECK(domain<>'function' OR (cpu_millis<=30000 AND timeout_seconds<=30)),
  CHECK(domain IN ('transform','sync') OR (memory_mib<=1024 AND cpu_millis<=600000 AND timeout_seconds<=600))
);
CREATE VIEW moria.runtime_function_bindings AS SELECT binding,artifact_uri,artifact_digest,outputs,effect,
  memory_mib,cpu_millis,timeout_seconds,value_version FROM moria.runtime_sandbox_bindings WHERE domain='function' WITH CASCADED CHECK OPTION;
CREATE TABLE moria.runtime_sandbox_attempts (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  domain text NOT NULL DEFAULT 'function' CHECK(domain IN ('function','agent','evaluator','transform','sync')),
  task uuid NOT NULL UNIQUE REFERENCES moria.runtime_execution_tasks(id),
  binding uuid NOT NULL REFERENCES moria.runtime_sandbox_bindings(binding),
  owner uuid NOT NULL, fence bigint NOT NULL CHECK(fence>0), host_id uuid NOT NULL,
  attempt_id uuid NOT NULL UNIQUE, correlation_id uuid NOT NULL UNIQUE,
  container_name text NOT NULL UNIQUE,
  creation_state integer NOT NULL DEFAULT 0 CHECK(creation_state BETWEEN 0 AND 2),
  state text NOT NULL CHECK(state IN ('starting','running','sealed','failed')),
  cleaned boolean NOT NULL DEFAULT false,
  consumed_markings jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(consumed_markings)='array'),
  observation_pending text CHECK(observation_pending IS NULL OR octet_length(observation_pending) BETWEEN 1 AND 160),
  observation_blocked boolean NOT NULL DEFAULT false,
  cpu_observed_ns bigint NOT NULL DEFAULT 0 CHECK(cpu_observed_ns>=0),
  memory_observed_bytes bigint NOT NULL DEFAULT 0 CHECK(memory_observed_bytes>=0), error text,
  cleanup_checked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  FOREIGN KEY(binding,domain) REFERENCES moria.runtime_sandbox_bindings(binding,domain),
  CHECK(state<>'sealed' OR (observation_pending IS NULL AND NOT observation_blocked))
);
CREATE FUNCTION moria.runtime_sandbox_markings_union(previous jsonb,incoming jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE combined jsonb;
BEGIN
  IF jsonb_typeof(previous) IS DISTINCT FROM 'array' OR jsonb_typeof(incoming) IS DISTINCT FROM 'array'
    THEN RAISE EXCEPTION 'FUNCTION_MARKINGS_INVALID'; END IF;
  SELECT COALESCE(jsonb_agg(value ORDER BY value),'[]') INTO combined FROM
    (SELECT DISTINCT value FROM jsonb_array_elements(previous||incoming)) labels;
  IF jsonb_array_length(combined)>256 THEN RAISE EXCEPTION 'FUNCTION_MARKINGS_INVALID'; END IF;
  RETURN combined;
END $$;
CREATE VIEW moria.runtime_function_sandboxes AS SELECT execution,task,binding,owner,fence,host_id,attempt_id,correlation_id,
  container_name,creation_state,state,cleaned,consumed_markings,cpu_observed_ns,error,created_at,finished_at
  FROM moria.runtime_sandbox_attempts WHERE domain='function' WITH CASCADED CHECK OPTION;
CREATE INDEX runtime_function_cleanup ON moria.runtime_sandbox_attempts(host_id,(COALESCE(cleanup_checked_at,created_at)),execution) WHERE NOT cleaned;
CREATE FUNCTION moria.runtime_function_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME='runtime_sandbox_bindings' THEN
    RAISE EXCEPTION 'IMMUTABLE_FUNCTION_BINDING';
  END IF;
  IF ROW(NEW.execution,NEW.domain,NEW.task,NEW.binding,NEW.owner,NEW.fence,NEW.host_id,NEW.attempt_id,NEW.correlation_id,NEW.container_name)
    IS DISTINCT FROM ROW(OLD.execution,OLD.domain,OLD.task,OLD.binding,OLD.owner,OLD.fence,OLD.host_id,OLD.attempt_id,OLD.correlation_id,OLD.container_name)
    OR NEW.creation_state<OLD.creation_state OR (OLD.cleaned AND NOT NEW.cleaned)
    OR NOT OLD.consumed_markings<@NEW.consumed_markings
    OR (OLD.observation_blocked AND NOT NEW.observation_blocked)
    THEN RAISE EXCEPTION 'IMMUTABLE_FUNCTION_ATTEMPT'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_function_binding_immutable BEFORE UPDATE OR DELETE ON moria.runtime_sandbox_bindings FOR EACH ROW EXECUTE FUNCTION moria.runtime_function_immutable();
CREATE TRIGGER runtime_function_attempt_immutable BEFORE UPDATE ON moria.runtime_sandbox_attempts FOR EACH ROW EXECUTE FUNCTION moria.runtime_function_immutable();

CREATE TABLE moria.runtime_workers (
  owner uuid PRIMARY KEY, host_id uuid NOT NULL,
  incarnation uuid NOT NULL, resource_classes text[] NOT NULL CHECK(cardinality(resource_classes) BETWEEN 1 AND 64),
  domains text[] NOT NULL DEFAULT ARRAY['function'] CHECK(cardinality(domains) BETWEEN 1 AND 16 AND domains<@ARRAY['function','agent','evaluator','evaluation','search','transform','sync','indexing','delivery','maintenance']),
  enabled boolean NOT NULL DEFAULT true, expires_at timestamptz NOT NULL
  ,generation bigint NOT NULL DEFAULT 1 CHECK(generation>0)
);
CREATE VIEW moria.runtime_function_workers AS SELECT owner,host_id,incarnation,resource_classes,domains,enabled,expires_at FROM moria.runtime_workers;
CREATE FUNCTION moria.runtime_worker_check(worker uuid, host uuid, resource_class text, expected_domain text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM moria.runtime_workers w,moria.indexing_instance i WHERE w.owner=worker
    AND w.enabled AND w.expires_at>clock_timestamp() AND w.incarnation=i.incarnation AND (host IS NULL OR w.host_id=host)
    AND (resource_class IS NULL OR resource_class=ANY(w.resource_classes)) AND expected_domain=ANY(w.domains) FOR SHARE OF w;
  IF NOT FOUND THEN RAISE EXCEPTION 'FUNCTION_WORKER_NOT_AUTHORIZED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_function_worker_check(worker uuid, host uuid DEFAULT NULL, resource_class text DEFAULT NULL, expected_domain text DEFAULT 'function') RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.runtime_worker_check(worker,host,resource_class,expected_domain);
END $$;
CREATE FUNCTION moria.runtime_function_worker_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.owner,NEW.host_id,NEW.incarnation) IS DISTINCT FROM ROW(OLD.owner,OLD.host_id,OLD.incarnation)
    THEN RAISE EXCEPTION 'IMMUTABLE_FUNCTION_WORKER'; END IF;
  IF ROW(NEW.enabled,NEW.resource_classes,NEW.domains) IS DISTINCT FROM ROW(OLD.enabled,OLD.resource_classes,OLD.domains)
    OR NEW.expires_at<OLD.expires_at OR (OLD.expires_at<=clock_timestamp() AND NEW.expires_at>OLD.expires_at)
    THEN NEW.generation:=OLD.generation+1;
  ELSE NEW.generation:=OLD.generation; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_function_worker_identity BEFORE UPDATE ON moria.runtime_workers FOR EACH ROW EXECUTE FUNCTION moria.runtime_function_worker_immutable();
CREATE TABLE moria.runtime_function_invocations (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  entry_id uuid NOT NULL REFERENCES moria.fs_entries(id), release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  definition jsonb NOT NULL, parent_id uuid REFERENCES moria.runtime_executions(id),
  attempt_id uuid NOT NULL DEFAULT gen_random_uuid(), correlation_id uuid NOT NULL DEFAULT gen_random_uuid(),
  prepare_only boolean NOT NULL DEFAULT false,
  proposal_commit jsonb,
  markings jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(markings)='array')
);
CREATE INDEX runtime_function_children ON moria.runtime_function_invocations(parent_id) WHERE parent_id IS NOT NULL;
CREATE TABLE moria.runtime_function_epochs (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_function_invocations(execution),
  binding uuid NOT NULL REFERENCES moria.runtime_execution_bindings(id), task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),
  owner uuid, fence bigint, incarnation uuid NOT NULL, security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL,
  CHECK((owner IS NULL)=(fence IS NULL))
);
CREATE TABLE moria.runtime_function_epoch_events (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, execution uuid NOT NULL REFERENCES moria.runtime_function_invocations(execution),
  owner uuid, fence bigint, security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL,
  reason text NOT NULL CHECK(reason IN ('admission','publication','mutation')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE moria.runtime_function_receipts (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_function_invocations(execution),
  result jsonb NOT NULL, result_hash text NOT NULL CHECK(result_hash ~ '^[0-9a-f]{64}$'),
  markings jsonb NOT NULL, committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION moria.runtime_function_public_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME='runtime_function_receipts' OR TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_FUNCTION_RECEIPT'; END IF;
  IF ROW(NEW.execution,NEW.entry_id,NEW.release,NEW.definition,NEW.parent_id,NEW.prepare_only,NEW.attempt_id,NEW.correlation_id)
    IS DISTINCT FROM ROW(OLD.execution,OLD.entry_id,OLD.release,OLD.definition,OLD.parent_id,OLD.prepare_only,OLD.attempt_id,OLD.correlation_id)
    THEN RAISE EXCEPTION 'IMMUTABLE_FUNCTION_INVOCATION'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_function_receipt_immutable BEFORE UPDATE OR DELETE ON moria.runtime_function_receipts FOR EACH ROW EXECUTE FUNCTION moria.runtime_function_public_immutable();
CREATE TRIGGER runtime_function_invocation_immutable BEFORE UPDATE OR DELETE ON moria.runtime_function_invocations FOR EACH ROW EXECUTE FUNCTION moria.runtime_function_public_immutable();

CREATE FUNCTION moria.runtime_function_acknowledge(execution_id uuid, worker uuid, worker_fence bigint,
  previous_security bigint, previous_instance bigint, consumed jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; a moria.runtime_function_epochs; f moria.runtime_function_invocations;
  current_security bigint; current_instance bigint; state jsonb; source jsonb; combined jsonb; active_hash text; proof uuid;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  IF NOT FOUND OR e.status<>'running' OR e.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'FUNCTION_LEASE_INVALID'; END IF;
  SELECT * INTO a FROM moria.runtime_function_epochs WHERE execution=e.id FOR UPDATE;
  IF NOT FOUND OR a.owner IS DISTINCT FROM worker OR a.fence IS DISTINCT FROM worker_fence
    OR a.security_epoch<>previous_security OR a.instance_epoch<>previous_instance
    OR NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.id=a.task AND t.execution=e.id
      AND t.owner=worker AND t.fence=worker_fence AND t.attempts=1 AND t.status='running' AND t.lease_until>clock_timestamp())
    THEN RAISE EXCEPTION 'FUNCTION_LEASE_INVALID'; END IF;
  IF EXISTS(SELECT 1 FROM unnest(ARRAY['moria/environment/'||e.environment,'moria/filesystem']) locked(key)
    WHERE NOT EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=pg_backend_pid() AND l.locktype='advisory'
      AND l.granted AND l.mode='ExclusiveLock' AND l.objsubid=1
      AND l.classid=((hashtextextended(locked.key,0)>>32)&4294967295)::oid
      AND l.objid=(hashtextextended(locked.key,0)&4294967295)::oid)) THEN RAISE EXCEPTION 'FUNCTION_MUTATION_ADMISSION_REQUIRED'; END IF;
  SELECT * INTO f FROM moria.runtime_function_invocations WHERE execution=e.id;
  IF f.definition->'spec'->>'effect'<>'readWrite' OR jsonb_typeof(consumed)<>'array'
    OR jsonb_array_length(consumed)>256 OR NOT f.markings<@consumed THEN RAISE EXCEPTION 'FUNCTION_EFFECT'; END IF;
  SELECT security_epoch INTO current_security FROM moria.environments WHERE id=e.environment;
  SELECT security_epoch INTO current_instance FROM moria.instance_state;
  IF current_security<previous_security OR current_instance<previous_instance THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  PERFORM moria.runtime_function_worker_check(worker,NULL,e.resource_class);
  IF EXISTS(SELECT 1 FROM moria.runtime_job_delegations WHERE execution=e.id) THEN proof:=e.id; END IF;
  PERFORM moria.runtime_binding_authority(e.binding,current_security,current_instance,proof);
  -- The tentative acknowledgement is inside this mutation transaction. Any following
  -- source/clearance check failure rolls it back with the filesystem mutation.
  UPDATE moria.runtime_function_epochs SET security_epoch=current_security,instance_epoch=current_instance WHERE execution=e.id;
  SELECT token_hash INTO active_hash FROM moria.runtime_job_sessions WHERE execution=e.id AND task=a.task
    AND owner=worker AND fence=worker_fence AND expires_at>clock_timestamp();
  active_hash:=COALESCE(active_hash,e.context->>'sessionHash');
  state:=moria.native_serving_session(active_hash);
  IF state->>'environment'<>e.environment OR state->>'principal'<>e.principal OR state->>'application'<>e.application
    OR NOT (EXISTS(SELECT 1 FROM moria.runtime_execution_bindings b WHERE b.id=e.binding
      AND cardinality(b.operations)=1 AND b.operations[1] IN (e.resource::text||':invoke','functions:invoke')
      AND state->'identity'->'grantedOperations' ? b.operations[1]
      AND state->'identity'->'applicationScopes' ? b.operations[1]))
    OR NOT EXISTS(SELECT 1 FROM moria.fs_entries entry JOIN moria.fs_resource_bindings b ON b.entry_id=entry.id AND b.active
      WHERE entry.id=f.entry_id AND entry.resource_id=e.resource AND NOT entry.deleted)
    THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  source:=moria.native_resource_filesystem_access(active_hash,e.resource::text,'invoke');
  SELECT COALESCE(jsonb_agg(DISTINCT value),'[]') INTO combined FROM (
    SELECT value FROM jsonb_array_elements(consumed)
    UNION SELECT jsonb_build_object('environment',e.environment,'marking',value) FROM jsonb_array_elements_text(source)
  ) labels;
  IF jsonb_array_length(combined)>256 OR EXISTS(SELECT 1 FROM jsonb_array_elements(combined) label
    WHERE label->>'environment' IS DISTINCT FROM e.environment OR NOT (state->'identity'->'markings' ? (label->>'marking')))
    THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  UPDATE moria.runtime_function_invocations SET markings=combined WHERE execution=e.id;
  INSERT INTO moria.runtime_function_epoch_events(execution,owner,fence,security_epoch,instance_epoch,reason)
    VALUES(e.id,worker,worker_fence,current_security,current_instance,'mutation');
END $$;
`;
