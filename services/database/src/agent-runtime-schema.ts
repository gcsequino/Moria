/** Native Agent metadata and receipts; shared sandbox tables contain only mechanism state. */
export const agentRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_agent_executions (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),
  release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  definition jsonb NOT NULL, adapter_id uuid NOT NULL, adapter_definition jsonb NOT NULL,
  application_id uuid NOT NULL, model_resource text NOT NULL, model_binding jsonb NOT NULL,
  model_secret_approval jsonb CHECK(model_secret_approval IS NULL OR jsonb_typeof(model_secret_approval)='object'),
  attempt_id uuid NOT NULL DEFAULT gen_random_uuid(), correlation_id uuid NOT NULL DEFAULT gen_random_uuid(),
  markings jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(markings)='array')
);
CREATE TABLE moria.runtime_agent_epochs (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_agent_executions(execution),
  binding uuid NOT NULL REFERENCES moria.runtime_execution_bindings(id), task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),
  owner uuid, fence bigint, incarnation uuid NOT NULL, security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL,
  CHECK((owner IS NULL)=(fence IS NULL))
);
CREATE TABLE moria.runtime_agent_receipts (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_agent_executions(execution),
  result jsonb NOT NULL, result_hash text NOT NULL CHECK(result_hash ~ '^[0-9a-f]{64}$'),
  markings jsonb NOT NULL, committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE moria.runtime_agent_model_calls (
  execution uuid NOT NULL REFERENCES moria.runtime_agent_executions(execution), logical_id text NOT NULL,
  attempt uuid NOT NULL, model_resource text NOT NULL, model_key text NOT NULL,
  request_hash text NOT NULL CHECK(request_hash ~ '^[0-9a-f]{64}$'),
  status integer NOT NULL DEFAULT 102 CHECK(status BETWEEN 100 AND 599),
  markings jsonb NOT NULL, usage jsonb, result_hash text, error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  PRIMARY KEY(execution,logical_id), UNIQUE(model_resource,model_key)
);
CREATE FUNCTION moria.runtime_agent_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME='runtime_agent_receipts' OR TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_AGENT_RECEIPT'; END IF;
  IF ROW(NEW.execution,NEW.entry_id,NEW.release,NEW.definition,NEW.adapter_id,NEW.adapter_definition,
    NEW.application_id,NEW.model_resource,NEW.model_binding,NEW.model_secret_approval,NEW.attempt_id,NEW.correlation_id)
    IS DISTINCT FROM ROW(OLD.execution,OLD.entry_id,OLD.release,OLD.definition,OLD.adapter_id,OLD.adapter_definition,
      OLD.application_id,OLD.model_resource,OLD.model_binding,OLD.model_secret_approval,OLD.attempt_id,OLD.correlation_id)
    THEN RAISE EXCEPTION 'IMMUTABLE_AGENT_EXECUTION'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_agent_execution_immutable BEFORE UPDATE OR DELETE ON moria.runtime_agent_executions FOR EACH ROW EXECUTE FUNCTION moria.runtime_agent_immutable();
CREATE TRIGGER runtime_agent_receipt_immutable BEFORE UPDATE OR DELETE ON moria.runtime_agent_receipts FOR EACH ROW EXECUTE FUNCTION moria.runtime_agent_immutable();
CREATE FUNCTION moria.runtime_agent_model_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR ROW(NEW.execution,NEW.logical_id,NEW.attempt,NEW.model_resource,NEW.model_key,NEW.request_hash,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.execution,OLD.logical_id,OLD.attempt,OLD.model_resource,OLD.model_key,OLD.request_hash,OLD.created_at)
    THEN RAISE EXCEPTION 'IMMUTABLE_AGENT_MODEL_CALL'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_agent_model_call_identity BEFORE UPDATE OR DELETE ON moria.runtime_agent_model_calls FOR EACH ROW EXECUTE FUNCTION moria.runtime_agent_model_identity();
CREATE VIEW moria.runtime_sandbox_metadata AS
  SELECT execution,entry_id,release,definition,attempt_id,correlation_id,markings,prepare_only,'function'::text AS domain
    FROM moria.runtime_function_invocations
  UNION ALL SELECT execution,entry_id,release,definition,attempt_id,correlation_id,markings,false,'agent'::text
    FROM moria.runtime_agent_executions;
`;
