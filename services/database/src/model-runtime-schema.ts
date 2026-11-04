/** Provider configuration is trusted release/operator state, never request JSON. */
export const modelRuntimeSchema = String.raw`
INSERT INTO moria.runtime_resource_classes VALUES('models',true,8,8);
CREATE TABLE moria.native_model_bindings (
  environment text NOT NULL REFERENCES moria.environments(id), resource text NOT NULL,
  revision text NOT NULL, definition jsonb NOT NULL, enabled boolean NOT NULL DEFAULT true,
  PRIMARY KEY(environment,resource)
);
CREATE FUNCTION moria.native_model_binding_revocation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.enabled,NEW.revision,NEW.definition) IS DISTINCT FROM ROW(OLD.enabled,OLD.revision,OLD.definition) THEN
    UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_model_binding_revocation AFTER UPDATE ON moria.native_model_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.native_model_binding_revocation();
CREATE TABLE moria.native_model_calls (
  id uuid PRIMARY KEY, environment text NOT NULL, application text NOT NULL, principal text NOT NULL,
  resource text NOT NULL, call_key text NOT NULL, request_hash text NOT NULL, binding jsonb NOT NULL,
  resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
  markings text[] NOT NULL, security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL,
  state text NOT NULL CHECK(state IN ('running','succeeded','failed')), uncertain boolean NOT NULL DEFAULT false,
  owner uuid NOT NULL, deadline timestamptz NOT NULL, result jsonb, error text,
  caller_execution uuid REFERENCES moria.runtime_executions(id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  UNIQUE(environment,application,principal,resource,call_key),
  FOREIGN KEY(environment,resource) REFERENCES moria.native_model_bindings(environment,resource)
);
CREATE FUNCTION moria.native_model_origin_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.caller_execution IS DISTINCT FROM OLD.caller_execution THEN RAISE EXCEPTION 'MODEL_ORIGIN_IMMUTABLE'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_model_origin_immutable BEFORE UPDATE ON moria.native_model_calls
  FOR EACH ROW EXECUTE FUNCTION moria.native_model_origin_immutable();
CREATE INDEX native_model_calls_capacity ON moria.native_model_calls(resource_class,deadline,environment) WHERE state='running' OR uncertain;
CREATE OR REPLACE FUNCTION moria.runtime_resource_in_use(class_name text, environment_id text DEFAULT NULL) RETURNS bigint LANGUAGE sql AS $$
  WITH current_clock AS MATERIALIZED(SELECT clock_timestamp() AS value)
  SELECT (SELECT count(*) FROM moria.runtime_execution_tasks t JOIN moria.runtime_executions e ON e.id=t.execution
    WHERE e.resource_class=class_name AND (environment_id IS NULL OR e.environment=environment_id)
      AND e.status='running' AND t.status='running' AND t.lease_until>(SELECT value FROM current_clock))
    +(SELECT count(*) FROM moria.runtime_executions e WHERE e.resource_class=class_name AND (environment_id IS NULL OR e.environment=environment_id)
      AND e.status='preparing' AND e.lease_until>(SELECT value FROM current_clock))
    +(SELECT count(*) FROM moria.runtime_deliveries d WHERE d.resource_class=class_name AND (environment_id IS NULL OR d.environment=environment_id)
      AND d.status='delivering' AND d.lease_until>(SELECT value FROM current_clock))
    +(SELECT count(*) FROM moria.native_model_calls c WHERE c.resource_class=class_name AND (environment_id IS NULL OR c.environment=environment_id)
      AND (c.state='running' OR c.uncertain) AND c.deadline>(SELECT value FROM current_clock))
$$;
CREATE FUNCTION moria.native_model_admit(call_id uuid,env text,app text,actor text,model_resource text,request_key text,
  input_hash text,definition jsonb,labels text[],security_epoch bigint,instance_epoch bigint,owner_id uuid,class_name text,session_hash text)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE budget moria.runtime_resource_classes; inserted integer; call_deadline timestamptz; call_timeout integer; origin jsonb; origin_execution uuid;
BEGIN
  IF definition->>'resourceClass' IS DISTINCT FROM class_name THEN RAISE EXCEPTION 'MODEL_BINDING_INVALID'; END IF;
  call_timeout:=COALESCE((definition->>'timeoutSeconds')::integer,120);
  IF call_timeout NOT BETWEEN 1 AND 120 THEN RAISE EXCEPTION 'MODEL_BINDING_INVALID'; END IF;
  SELECT * INTO budget FROM moria.runtime_resource_classes WHERE name=class_name FOR UPDATE;
  IF NOT FOUND OR NOT budget.enabled THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  origin:=moria.runtime_job_session_origin(session_hash);
  IF origin IS NOT NULL THEN
    IF ROW(origin->>'environment',origin->>'application',origin->>'principal') IS DISTINCT FROM ROW(env,app,actor)
      THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
    origin_execution:=(origin->>'execution')::uuid;
  END IF;
  IF EXISTS(SELECT 1 FROM moria.native_model_calls c WHERE c.environment=env AND c.application=app
    AND c.principal=actor AND c.resource=model_resource AND c.call_key=request_key) THEN RETURN jsonb_build_object('accepted',false); END IF;
  IF moria.runtime_resource_in_use(class_name)>=budget.maximum_tasks
    OR moria.runtime_resource_in_use(class_name,env)>=budget.maximum_environment_tasks THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  INSERT INTO moria.native_model_calls(id,environment,application,principal,resource,call_key,request_hash,binding,
    resource_class,markings,security_epoch,instance_epoch,state,owner,deadline,caller_execution)
    VALUES(call_id,env,app,actor,model_resource,request_key,input_hash,definition,class_name,labels,
      security_epoch,instance_epoch,'running',owner_id,clock_timestamp()+call_timeout*interval '1 second',origin_execution)
    ON CONFLICT(environment,application,principal,resource,call_key) DO NOTHING RETURNING deadline INTO call_deadline;
  GET DIAGNOSTICS inserted=ROW_COUNT;
  RETURN jsonb_build_object('accepted',inserted=1,'callerExecution',origin_execution,'remainingMillis',GREATEST(0,LEAST(120000,floor(extract(epoch FROM(call_deadline-clock_timestamp()))*1000)::bigint))::text);
END $$;
CREATE TABLE moria.native_model_attempts (
  call_id uuid NOT NULL REFERENCES moria.native_model_calls(id), ordinal integer NOT NULL,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  status text NOT NULL CHECK(status IN ('started','response','connectFailure','uncertain','failed')),
  http_status integer, usage jsonb, PRIMARY KEY(call_id,ordinal)
);
CREATE FUNCTION moria.native_model_attempt_admit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE moria.native_model_calls SET uncertain=true WHERE id=NEW.call_id AND state='running' AND deadline>clock_timestamp();
  IF NOT FOUND THEN RAISE EXCEPTION 'MODEL_CALL_UNAVAILABLE'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_model_attempt_admit BEFORE INSERT ON moria.native_model_attempts
  FOR EACH ROW EXECUTE FUNCTION moria.native_model_attempt_admit();
CREATE INDEX native_model_calls_live_origin ON moria.native_model_calls(caller_execution) WHERE state='running';
CREATE FUNCTION moria.native_model_sandbox_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.state IN ('sealed','failed') THEN
    UPDATE moria.native_model_calls SET state='failed',error='MODEL_INCOMPLETE',finished_at=clock_timestamp()
      WHERE caller_execution=NEW.execution AND state='running';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_model_sandbox_terminal AFTER UPDATE OF state ON moria.runtime_sandbox_attempts
  FOR EACH ROW EXECUTE FUNCTION moria.native_model_sandbox_terminal();
CREATE TABLE moria.native_model_probes (
  environment text NOT NULL, resource text NOT NULL, definition_hash text NOT NULL,
  capability text NOT NULL CHECK(capability IN ('chat','embed','stream','structured','tools','streamTools','streamStructured')),
  call_id uuid NOT NULL REFERENCES moria.native_model_calls(id),
  tested_at timestamptz NOT NULL DEFAULT clock_timestamp(), expires_at timestamptz NOT NULL,
  PRIMARY KEY(environment,resource,definition_hash,capability),
  FOREIGN KEY(environment,resource) REFERENCES moria.native_model_bindings(environment,resource)
);
`;
