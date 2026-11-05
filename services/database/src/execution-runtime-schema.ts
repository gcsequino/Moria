/** Durable native execution authority; domain publication invokes completion in its own transaction. */
export const executionRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_resource_classes (
  name text PRIMARY KEY, enabled boolean NOT NULL DEFAULT true,
  maximum_tasks integer NOT NULL CHECK(maximum_tasks BETWEEN 1 AND 1024),
  maximum_environment_tasks integer NOT NULL CHECK(maximum_environment_tasks BETWEEN 1 AND maximum_tasks)
);
INSERT INTO moria.runtime_resource_classes VALUES ('standard',true,32,4);
CREATE TABLE moria.runtime_execution_bindings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), environment text NOT NULL REFERENCES moria.environments(id),
  resource uuid NOT NULL, spec jsonb NOT NULL, context jsonb NOT NULL,
  operations text[] NOT NULL CHECK(cardinality(operations) BETWEEN 1 AND 256), markings text[] NOT NULL,
  security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL, incarnation uuid NOT NULL,
  enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE moria.runtime_executions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), binding uuid NOT NULL REFERENCES moria.runtime_execution_bindings(id),
  environment text NOT NULL REFERENCES moria.environments(id), resource uuid NOT NULL,
  principal text NOT NULL, application text NOT NULL, request_key text NOT NULL,
  spec jsonb NOT NULL, context jsonb NOT NULL, input jsonb NOT NULL, input_hash text NOT NULL,
  resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
  status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','preparing','succeeded','failed','cancelled')),
  owner uuid, fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,worker_generation bigint,
  deadline timestamptz NOT NULL, event_sequence bigint NOT NULL DEFAULT 0, custom_events integer NOT NULL DEFAULT 0,
  result jsonb, error text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  UNIQUE(environment,resource,principal,application,request_key)
);
CREATE INDEX runtime_executions_pending ON moria.runtime_executions(resource_class,created_at)
  WHERE status IN ('queued','running','preparing');
CREATE INDEX runtime_execution_deadlines ON moria.runtime_executions(deadline,id) WHERE status IN ('queued','running','preparing');
CREATE INDEX runtime_publication_expiry ON moria.runtime_executions(lease_until,id) WHERE status='preparing';
CREATE TABLE moria.runtime_execution_tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), execution uuid NOT NULL REFERENCES moria.runtime_executions(id),
  ordinal integer NOT NULL, plan jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','sealed','cancelled')),
  owner uuid, fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,worker_generation bigint,attempts integer NOT NULL DEFAULT 0,
  result jsonb, digest text, UNIQUE(execution,ordinal)
);
CREATE INDEX runtime_execution_tasks_pending ON moria.runtime_execution_tasks(execution,ordinal) WHERE status IN ('pending','running');
CREATE INDEX runtime_task_expiry ON moria.runtime_execution_tasks(lease_until,execution) WHERE status='running';
CREATE TABLE moria.runtime_execution_attempts (
  task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id), fence bigint NOT NULL, owner uuid NOT NULL,
  status text NOT NULL DEFAULT 'running', started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  PRIMARY KEY(task,fence)
);
CREATE TABLE moria.runtime_execution_events (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id), sequence bigint NOT NULL,
  kind text NOT NULL, payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(execution,sequence)
);
CREATE TABLE moria.runtime_execution_cancellations (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  root uuid NOT NULL REFERENCES moria.runtime_executions(id),
  requested_by jsonb NOT NULL CHECK(jsonb_typeof(requested_by)='object' AND octet_length(requested_by::text)<=16384),
  requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),expanded_at timestamptz
);
CREATE INDEX runtime_execution_cancellation_pending ON moria.runtime_execution_cancellations(requested_at,execution) WHERE expanded_at IS NULL;
CREATE FUNCTION moria.runtime_execution_cancellation_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-'expanded_at')<>(to_jsonb(OLD)-'expanded_at')
   OR (OLD.expanded_at IS NOT NULL AND NEW.expanded_at IS DISTINCT FROM OLD.expanded_at) THEN RAISE EXCEPTION 'IMMUTABLE_EXECUTION'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_execution_cancellation_identity BEFORE UPDATE OR DELETE ON moria.runtime_execution_cancellations
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_execution_cancellation_immutable();
CREATE TABLE moria.runtime_automations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), binding uuid NOT NULL REFERENCES moria.runtime_execution_bindings(id),
  enabled boolean NOT NULL DEFAULT true, interval_seconds integer NOT NULL CHECK(interval_seconds BETWEEN 60 AND 604800),
  next_run timestamptz NOT NULL, input jsonb NOT NULL, input_hash text NOT NULL, tasks jsonb NOT NULL
);
CREATE TABLE moria.runtime_automation_firings (
  automation uuid NOT NULL REFERENCES moria.runtime_automations(id), scheduled_at timestamptz NOT NULL,
  status text NOT NULL CHECK(status IN ('enqueued','overlap','denied')), execution uuid REFERENCES moria.runtime_executions(id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(automation,scheduled_at)
);
CREATE INDEX runtime_automations_due ON moria.runtime_automations(next_run,id) WHERE enabled;
CREATE INDEX runtime_automation_last_execution ON moria.runtime_automation_firings(automation,scheduled_at DESC) WHERE status='enqueued';

CREATE FUNCTION moria.runtime_execution_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME='runtime_execution_bindings' THEN
    IF (to_jsonb(NEW)-'enabled')<>(to_jsonb(OLD)-'enabled') THEN RAISE EXCEPTION 'IMMUTABLE_EXECUTION_BINDING'; END IF;
  ELSIF TG_TABLE_NAME='runtime_executions' THEN
    IF ROW(NEW.binding,NEW.environment,NEW.resource,NEW.principal,NEW.application,NEW.request_key,NEW.spec,NEW.context,NEW.input,NEW.input_hash,NEW.resource_class,NEW.deadline)
      IS DISTINCT FROM ROW(OLD.binding,OLD.environment,OLD.resource,OLD.principal,OLD.application,OLD.request_key,OLD.spec,OLD.context,OLD.input,OLD.input_hash,OLD.resource_class,OLD.deadline)
      THEN RAISE EXCEPTION 'IMMUTABLE_EXECUTION'; END IF;
  ELSE
    IF ROW(NEW.execution,NEW.ordinal,NEW.plan) IS DISTINCT FROM ROW(OLD.execution,OLD.ordinal,OLD.plan)
      THEN RAISE EXCEPTION 'IMMUTABLE_EXECUTION_TASK'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_binding_immutable BEFORE UPDATE ON moria.runtime_execution_bindings FOR EACH ROW EXECUTE FUNCTION moria.runtime_execution_immutable();
CREATE TRIGGER runtime_execution_immutable BEFORE UPDATE ON moria.runtime_executions FOR EACH ROW EXECUTE FUNCTION moria.runtime_execution_immutable();
CREATE TRIGGER runtime_task_immutable BEFORE UPDATE ON moria.runtime_execution_tasks FOR EACH ROW EXECUTE FUNCTION moria.runtime_execution_immutable();

CREATE FUNCTION moria.runtime_execution_event(execution_id uuid, event_kind text, event_payload jsonb) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE seq bigint;
BEGIN
  UPDATE moria.runtime_executions SET event_sequence=event_sequence+1 WHERE id=execution_id RETURNING event_sequence INTO seq;
  IF NOT FOUND THEN RAISE EXCEPTION 'EXECUTION_UNKNOWN'; END IF;
  INSERT INTO moria.runtime_execution_events VALUES(execution_id,seq,event_kind,event_payload,clock_timestamp());
  RETURN seq;
END $$;

CREATE FUNCTION moria.runtime_binding_authority_state(binding_id uuid, environment_epoch bigint, instance_epoch bigint, execution_proof uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE b moria.runtime_execution_bindings; p record; valid boolean; delegated jsonb; purpose_delegated boolean;
BEGIN
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=binding_id;
  IF NOT FOUND OR NOT b.enabled THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.environments e,moria.instance_state i,moria.indexing_instance c
    WHERE e.id=b.environment AND e.security_epoch=environment_epoch AND i.security_epoch=instance_epoch
      AND c.incarnation=b.incarnation) THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  SELECT a.enabled AS app_enabled,x.enabled,x.kind,x.synced_at,
    (ib.instance_id IS NULL OR (ip.enabled AND ip.kind=x.kind)) AS instance_enabled,
    x.markings,a.operations AS scopes,g.operations AS grants INTO p
    FROM moria.principals x JOIN moria.application_scopes a ON a.environment=x.environment AND a.application=b.context->>'application'
    JOIN moria.permissions g ON g.environment=x.environment AND g.application=a.application AND g.principal=x.principal
    LEFT JOIN moria.instance_principal_bindings ib ON ib.environment=x.environment AND ib.principal=x.principal
    LEFT JOIN moria.instance_principals ip ON ip.id=ib.instance_id
    WHERE x.environment=b.environment AND x.principal=b.context->>'principal';
  IF NOT FOUND OR NOT p.enabled OR NOT p.app_enabled OR NOT p.instance_enabled
    OR (p.kind='human' AND p.synced_at<=clock_timestamp()-interval '60 seconds')
    OR NOT b.operations<@p.scopes OR NOT b.operations<@p.grants OR NOT b.markings<@p.markings
    THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  purpose_delegated:=execution_proof IS NOT NULL AND EXISTS(
    SELECT 1 FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id
    WHERE e.id=execution_proof AND e.binding=b.id AND e.context=b.context
      AND d.origin_kind='service' AND d.origin_binding=b.id
      AND ROW(d.environment,d.principal,d.application)=ROW(b.environment,b.context->>'principal',b.context->>'application'));
  IF NOT purpose_delegated THEN
    IF b.context->>'automationFiring' IS NOT NULL THEN PERFORM moria.runtime_automation_binding_guard(b.id); END IF;
    IF b.context->>'evaluationCase' IS NOT NULL THEN PERFORM moria.runtime_evaluation_service_binding_guard(b.id); END IF;
    IF b.context->>'serviceParentExecution' IS NOT NULL THEN PERFORM moria.runtime_sandbox_service_child_binding_guard(b.id); END IF;
  END IF;
  IF execution_proof IS NOT NULL THEN
    BEGIN
      IF NOT EXISTS(SELECT 1 FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id
        WHERE e.id=execution_proof AND e.binding=b.id AND e.context=b.context
          AND ROW(d.environment,d.principal,d.application)=ROW(b.environment,b.context->>'principal',b.context->>'application'))
        THEN RAISE EXCEPTION 'JOB_DELEGATION_REQUIRED'; END IF;
      delegated:=moria.native_serving_job_delegation(execution_proof);
      IF ROW(delegated->>'environment',delegated->>'principal',delegated->>'application')
        IS DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application')
        OR NOT to_jsonb(b.operations)<@(delegated->'identity'->'grantedOperations')
        OR NOT to_jsonb(b.operations)<@(delegated->'identity'->'applicationScopes')
        OR NOT to_jsonb(b.markings)<@(delegated->'identity'->'markings') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
    EXCEPTION WHEN raise_exception THEN
      -- Preserve direct purpose error precedence while validating a successful delegation once.
      IF purpose_delegated THEN
        IF b.context->>'automationFiring' IS NOT NULL THEN PERFORM moria.runtime_automation_binding_guard(b.id); END IF;
        IF b.context->>'evaluationCase' IS NOT NULL THEN PERFORM moria.runtime_evaluation_service_binding_guard(b.id); END IF;
        IF b.context->>'serviceParentExecution' IS NOT NULL THEN PERFORM moria.runtime_sandbox_service_child_binding_guard(b.id); END IF;
      END IF;
      RAISE;
    END;
  ELSIF b.context->>'sessionHash' IS NOT NULL THEN
    SELECT EXISTS(SELECT 1 FROM moria.workload_sessions s WHERE s.token_hash=b.context->>'sessionHash'
      AND s.environment=b.environment AND s.principal=b.context->>'principal' AND s.application=b.context->>'application'
      AND NOT s.disabled AND s.expires_at>clock_timestamp() AND p.kind='workload'
      UNION ALL SELECT 1 FROM moria.browser_sessions s LEFT JOIN moria.auth_providers a
      ON a.environment=s.environment AND a.application=s.application AND a.provider=s.provider
      WHERE s.token_hash=b.context->>'sessionHash' AND s.environment=b.environment AND s.principal=b.context->>'principal'
      AND s.application=b.context->>'application' AND NOT s.disabled AND s.expires_at>clock_timestamp()
      AND ((s.provider IS NULL AND p.kind='workload') OR (s.provider IS NOT NULL AND p.kind='human'
      AND a.enabled AND a.revision=s.revision AND s.verified_at>clock_timestamp()-interval '60 seconds'))) INTO valid;
    IF NOT valid THEN
      -- Nested admission may originate from an actual live private parent lease.
      -- The common resolver still rejects an expired ordinary hash without that proof.
      delegated:=moria.native_serving_session(b.context->>'sessionHash');
      valid:=ROW(delegated->>'environment',delegated->>'principal',delegated->>'application')
        IS NOT DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application')
        AND to_jsonb(b.operations)<@(delegated->'identity'->'grantedOperations')
        AND to_jsonb(b.operations)<@(delegated->'identity'->'applicationScopes')
        AND to_jsonb(b.markings)<@(delegated->'identity'->'markings');
    END IF;
    IF NOT valid THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  END IF;
  IF execution_proof IS NOT NULL THEN RETURN delegated; END IF;
  RETURN NULL;
END $$;

CREATE FUNCTION moria.runtime_binding_authority(binding_id uuid, environment_epoch bigint, instance_epoch bigint, execution_proof uuid DEFAULT NULL) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.runtime_binding_authority_state(binding_id,environment_epoch,instance_epoch,execution_proof);
END $$;

CREATE FUNCTION moria.runtime_binding_check(binding_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE b moria.runtime_execution_bindings;
BEGIN
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=binding_id;
  PERFORM moria.runtime_binding_authority(binding_id,b.security_epoch,b.instance_epoch);
END $$;

CREATE FUNCTION moria.runtime_execution_check_state(execution_id uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; acknowledged record; proof uuid; delegated jsonb;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'EXECUTION_UNKNOWN'; END IF;
  PERFORM moria.runtime_environment_cell_check(e.environment);
  IF e.status='preparing' AND e.worker_generation IS NOT NULL THEN
    PERFORM 1 FROM moria.runtime_workers w WHERE w.owner=e.owner AND w.generation=e.worker_generation
      AND w.enabled AND w.expires_at>clock_timestamp() FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  END IF;
  IF EXISTS(SELECT 1 FROM moria.runtime_job_delegations WHERE execution=e.id) THEN proof:=e.id; END IF;
  IF e.spec->>'domain'='function' THEN
    SELECT a.security_epoch,a.instance_epoch INTO acknowledged FROM moria.runtime_function_epochs a
      JOIN moria.runtime_execution_tasks t ON t.id=a.task AND t.execution=a.execution
      WHERE a.execution=e.id AND a.binding=e.binding AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance)
      AND ((t.status='pending' AND t.attempts=0 AND a.owner IS NULL AND a.fence IS NULL)
        OR (t.status IN ('running','sealed') AND t.attempts=1 AND t.owner=a.owner AND t.fence=a.fence));
    IF FOUND THEN delegated:=moria.runtime_binding_authority_state(e.binding,acknowledged.security_epoch,acknowledged.instance_epoch,proof);
    ELSE delegated:=moria.runtime_binding_authority_state(e.binding,(SELECT security_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),(SELECT instance_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),proof); END IF;
  ELSIF e.spec->>'domain'='agent' THEN
    SELECT a.security_epoch,a.instance_epoch INTO acknowledged FROM moria.runtime_agent_epochs a
      JOIN moria.runtime_execution_tasks t ON t.id=a.task AND t.execution=a.execution
      WHERE a.execution=e.id AND a.binding=e.binding AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance)
      AND ((t.status='pending' AND t.attempts=0 AND a.owner IS NULL AND a.fence IS NULL)
        OR (t.status IN ('running','sealed') AND t.attempts=1 AND t.owner=a.owner AND t.fence=a.fence));
    IF FOUND THEN delegated:=moria.runtime_binding_authority_state(e.binding,acknowledged.security_epoch,acknowledged.instance_epoch,proof);
    ELSE delegated:=moria.runtime_binding_authority_state(e.binding,(SELECT security_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),(SELECT instance_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),proof); END IF;
  ELSIF e.spec->>'domain'='maintenance' AND EXISTS(SELECT 1 FROM moria.runtime_source_automation_controls c WHERE c.execution=e.id) THEN
    SELECT a.security_epoch,a.instance_epoch INTO acknowledged FROM moria.runtime_source_automation_epochs a
      JOIN moria.runtime_execution_tasks t ON t.id=a.task AND t.execution=a.execution
      WHERE a.execution=e.id AND a.binding=e.binding AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance)
      AND ((t.status='pending' AND a.owner IS NULL AND a.fence IS NULL)
        OR (t.status IN ('running','sealed') AND t.owner=a.owner AND t.fence=a.fence));
    IF FOUND THEN delegated:=moria.runtime_binding_authority_state(e.binding,acknowledged.security_epoch,acknowledged.instance_epoch,proof);
    ELSE delegated:=moria.runtime_binding_authority_state(e.binding,(SELECT security_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),(SELECT instance_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),proof); END IF;
  ELSE delegated:=moria.runtime_binding_authority_state(e.binding,(SELECT security_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),(SELECT instance_epoch FROM moria.runtime_execution_bindings WHERE id=e.binding),proof); END IF;
  IF e.status IN ('failed','cancelled') OR (e.status<>'succeeded' AND e.deadline<=clock_timestamp())
    THEN RAISE EXCEPTION 'EXECUTION_INACTIVE'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_execution_bindings b WHERE b.id=e.binding AND b.spec=e.spec AND b.context=e.context
    AND b.environment=e.environment AND b.resource=e.resource) THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  RETURN delegated;
END $$;

CREATE FUNCTION moria.runtime_execution_check(execution_id uuid) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.runtime_execution_check_state(execution_id);
END $$;

CREATE FUNCTION moria.runtime_execution_request(binding_id uuid, logical_key text, normalized_input jsonb, normalized_hash text, task_plans jsonb) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE b moria.runtime_execution_bindings; e moria.runtime_executions; item jsonb; n integer:=0;
BEGIN
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=binding_id;
  PERFORM moria.runtime_binding_check(binding_id);
  IF b.spec->>'domain'='indexing' THEN RAISE EXCEPTION 'INDEXING_TASK_AUTHORITY_REQUIRED'; END IF;
  IF octet_length(logical_key) NOT BETWEEN 1 AND 180 OR normalized_hash!~'^[0-9a-f]{64}$'
    OR octet_length(normalized_input::text)>65536 OR jsonb_typeof(task_plans)<>'array'
    OR jsonb_array_length(task_plans) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'EXECUTION_INVALID'; END IF;
  IF b.spec->>'domain' IN ('function','agent','evaluator') AND jsonb_array_length(task_plans)<>1 THEN RAISE EXCEPTION 'EXECUTION_INVALID'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/execution/request/'||jsonb_build_array(b.environment,b.resource,b.context->>'principal',b.context->>'application',logical_key)::text,0));
  SELECT * INTO e FROM moria.runtime_executions WHERE environment=b.environment AND resource=b.resource
    AND principal=b.context->>'principal' AND application=b.context->>'application' AND request_key=logical_key;
  IF FOUND THEN
    IF e.input<>normalized_input OR e.input_hash<>normalized_hash OR task_plans IS DISTINCT FROM
      (SELECT jsonb_agg(plan ORDER BY ordinal) FROM moria.runtime_execution_tasks WHERE execution=e.id)
      THEN RAISE EXCEPTION 'IDEMPOTENCY_CONFLICT'; END IF;
    PERFORM moria.runtime_binding_check(e.binding);
    RETURN e.id;
  END IF;
  INSERT INTO moria.runtime_executions(binding,environment,resource,principal,application,request_key,spec,context,input,input_hash,resource_class,deadline)
    VALUES(b.id,b.environment,b.resource,b.context->>'principal',b.context->>'application',logical_key,b.spec,b.context,normalized_input,normalized_hash,
      b.spec->>'resourceClass',LEAST(clock_timestamp()+(b.spec->>'timeoutSeconds')::integer*interval '1 second',
        COALESCE((b.context->>'parentDeadline')::timestamptz,'infinity'::timestamptz))) RETURNING * INTO e;
  FOR item IN SELECT value FROM jsonb_array_elements(task_plans) LOOP
    IF octet_length(item::text)>4096 THEN RAISE EXCEPTION 'EXECUTION_INVALID'; END IF;
    INSERT INTO moria.runtime_execution_tasks(execution,ordinal,plan) VALUES(e.id,n,item);
    n:=n+1;
  END LOOP;
  PERFORM moria.runtime_job_delegation_accept(e.id);
  PERFORM moria.runtime_execution_event(e.id,'accepted',jsonb_build_object('tasks',n));
  RETURN e.id;
END $$;

CREATE FUNCTION moria.runtime_execution_reap(target uuid DEFAULT NULL) RETURNS integer LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; t moria.runtime_execution_tasks; n integer:=0; candidates uuid[]; reaped_at timestamptz:=clock_timestamp();
BEGIN
  IF target IS NOT NULL THEN candidates:=ARRAY[target];
  ELSE
    WITH due AS (
      (SELECT id FROM moria.runtime_executions WHERE status IN ('queued','running','preparing') AND deadline<=reaped_at ORDER BY deadline,id LIMIT 64)
      UNION (SELECT id FROM moria.runtime_executions WHERE status='preparing' AND lease_until<=reaped_at ORDER BY lease_until,id LIMIT 64)
      UNION (SELECT execution FROM moria.runtime_execution_tasks WHERE status='running' AND lease_until<=reaped_at ORDER BY lease_until,execution LIMIT 64)
    ) SELECT array_agg(id) INTO candidates FROM due;
  END IF;
  FOR e IN SELECT * FROM moria.runtime_executions x WHERE x.id=ANY(candidates) AND x.status IN ('queued','running','preparing') AND
    (x.deadline<=clock_timestamp() OR (x.status='preparing' AND x.lease_until<=clock_timestamp()) OR EXISTS
      (SELECT 1 FROM moria.runtime_execution_tasks expired WHERE expired.execution=x.id AND expired.status='running' AND expired.lease_until<=clock_timestamp()))
    ORDER BY x.created_at FOR UPDATE SKIP LOCKED LIMIT 64 LOOP
    IF e.deadline<=clock_timestamp() THEN
      UPDATE moria.runtime_executions SET status='failed',error='DEADLINE_EXCEEDED',finished_at=clock_timestamp() WHERE id=e.id;
    ELSE
      FOR t IN SELECT * FROM moria.runtime_execution_tasks WHERE execution=e.id AND status='running' AND lease_until<=clock_timestamp() FOR UPDATE LOOP
        UPDATE moria.runtime_execution_attempts SET status='interrupted',finished_at=clock_timestamp() WHERE task=t.id AND fence=t.fence;
        IF e.spec->>'retryPolicy'='never' OR t.attempts>=3 THEN
          UPDATE moria.runtime_executions SET status='failed',error='WORKER_LOST',finished_at=clock_timestamp() WHERE id=e.id;
        ELSE
          UPDATE moria.runtime_execution_tasks SET status='pending',owner=NULL,lease_until=NULL WHERE id=t.id;
          PERFORM moria.runtime_execution_event(e.id,'retryScheduled',jsonb_build_object('task',t.id,'fence',t.fence::text));
        END IF;
      END LOOP;
      IF e.status='preparing' THEN
        UPDATE moria.runtime_executions SET status='running',owner=NULL,lease_until=NULL WHERE id=e.id AND status='preparing';
      END IF;
    END IF;
    IF EXISTS(SELECT 1 FROM moria.runtime_executions WHERE id=e.id AND status='failed') THEN
      UPDATE moria.runtime_execution_tasks SET status='cancelled',lease_until=NULL WHERE execution=e.id AND status IN ('pending','running');
      UPDATE moria.runtime_execution_attempts SET status='interrupted',finished_at=clock_timestamp() WHERE task IN
        (SELECT id FROM moria.runtime_execution_tasks WHERE execution=e.id) AND status='running';
      PERFORM moria.runtime_execution_event(e.id,'failed',jsonb_build_object('error',(SELECT error FROM moria.runtime_executions WHERE id=e.id)));
    END IF;
    n:=n+1;
  END LOOP;
  RETURN n;
END $$;

CREATE FUNCTION moria.runtime_task_check_state(task_id uuid, worker uuid, worker_fence bigint) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE execution_id uuid; accepted_generation bigint; delegated jsonb;
BEGIN
  SELECT execution INTO execution_id FROM moria.runtime_execution_tasks WHERE id=task_id;
  PERFORM 1 FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  delegated:=moria.runtime_execution_check_state(execution_id);
  SELECT worker_generation INTO accepted_generation FROM moria.runtime_execution_tasks WHERE id=task_id AND owner=worker AND fence=worker_fence
    AND status='running' AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  IF accepted_generation IS NOT NULL THEN
    PERFORM 1 FROM moria.runtime_workers w WHERE w.owner=worker AND w.generation=accepted_generation AND w.enabled AND w.expires_at>clock_timestamp() FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  END IF;
  RETURN delegated;
END $$;

CREATE FUNCTION moria.runtime_task_check(task_id uuid, worker uuid, worker_fence bigint) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.runtime_task_check_state(task_id,worker,worker_fence);
END $$;

CREATE FUNCTION moria.runtime_execution_complete(execution_id uuid, worker uuid, worker_fence bigint, completion jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  PERFORM moria.runtime_execution_check(execution_id);
  IF e.status='succeeded' THEN
    IF e.result IS DISTINCT FROM completion THEN RAISE EXCEPTION 'IDEMPOTENCY_CONFLICT'; END IF;
    RETURN e.result;
  END IF;
  IF e.status<>'preparing' OR e.owner IS DISTINCT FROM worker OR e.fence<>worker_fence OR e.lease_until<=clock_timestamp()
    OR EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE execution=execution_id AND status<>'sealed')
    THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  IF octet_length(completion::text)>65536 THEN RAISE EXCEPTION 'EXECUTION_INVALID'; END IF;
  UPDATE moria.runtime_executions SET status='succeeded',result=completion,finished_at=clock_timestamp(),lease_until=NULL WHERE id=execution_id;
  PERFORM moria.runtime_execution_event(execution_id,'succeeded',jsonb_build_object('resultRecorded',true));
  RETURN completion;
END $$;

CREATE FUNCTION moria.runtime_schedule_tick() RETURNS integer LANGUAGE plpgsql AS $$
DECLARE a moria.runtime_automations; execution_id uuid; outcome text; n integer:=0; due_at timestamptz:=clock_timestamp();
BEGIN
  FOR a IN SELECT * FROM moria.runtime_automations WHERE enabled AND next_run<=due_at
    ORDER BY next_run,id FOR UPDATE SKIP LOCKED LIMIT 16 LOOP
    execution_id:=NULL;
    BEGIN
      PERFORM moria.runtime_binding_check(a.binding);
      IF EXISTS(SELECT 1 FROM moria.runtime_executions e WHERE e.status IN ('queued','running','preparing') AND e.id=
        (SELECT execution FROM moria.runtime_automation_firings WHERE automation=a.id AND status='enqueued' ORDER BY scheduled_at DESC LIMIT 1)) THEN
        outcome:='overlap';
      ELSE
        execution_id:=moria.runtime_execution_request(a.binding,'automation/'||a.id::text||'/'||extract(epoch FROM a.next_run)::text,a.input,a.input_hash,a.tasks);
        outcome:='enqueued';
      END IF;
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM IN ('BINDING_CHANGED','AUTHORIZATION_CHANGED','UNAUTHENTICATED') THEN outcome:='denied';
      ELSE RAISE; END IF;
    END;
    INSERT INTO moria.runtime_automation_firings VALUES(a.id,a.next_run,outcome,execution_id,clock_timestamp());
    UPDATE moria.runtime_automations SET next_run=clock_timestamp()+a.interval_seconds*interval '1 second' WHERE id=a.id;
    n:=n+1;
  END LOOP;
  RETURN n;
END $$;
`;
