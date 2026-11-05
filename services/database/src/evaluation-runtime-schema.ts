/** Immutable evaluation plans, bounded case fanout and independently fenced evaluator receipts. */
export const evaluationRuntimeSchema = String.raw`
INSERT INTO moria.runtime_resource_classes(name,maximum_tasks,maximum_environment_tasks)
  VALUES('evaluation-control',32,16) ON CONFLICT(name) DO NOTHING;
CREATE TABLE moria.runtime_evaluation_environments (
  environment text PRIMARY KEY REFERENCES moria.environments(id),application text NOT NULL,
  maximum_cases integer NOT NULL CHECK(maximum_cases BETWEEN 1 AND 1000),
  maximum_repeats integer NOT NULL CHECK(maximum_repeats BETWEEN 1 AND 16),
  maximum_parallel integer NOT NULL CHECK(maximum_parallel BETWEEN 1 AND 8),
  enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE moria.runtime_evaluation_service_profiles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment text NOT NULL REFERENCES moria.environments(id),
  resource uuid NOT NULL,authority uuid NOT NULL REFERENCES moria.runtime_service_authorities(id),
  suite_revision text NOT NULL,agent_revision text NOT NULL,evaluator_revision text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),
  UNIQUE(environment,resource,authority,suite_revision),
  CHECK(octet_length(suite_revision) BETWEEN 1 AND 128 AND octet_length(agent_revision) BETWEEN 1 AND 128 AND octet_length(evaluator_revision) BETWEEN 1 AND 128)
);
CREATE UNIQUE INDEX runtime_evaluation_service_current ON moria.runtime_evaluation_service_profiles(environment,resource) WHERE enabled;
CREATE FUNCTION moria.runtime_evaluation_service_profile_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR NEW.generation<>OLD.generation OR (to_jsonb(NEW)-ARRAY['enabled','generation'])<>(to_jsonb(OLD)-ARRAY['enabled','generation'])
    THEN RAISE EXCEPTION 'IMMUTABLE_EVALUATION'; END IF;
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN
    NEW.generation:=OLD.generation+1;
    UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_evaluation_service_profile_immutable BEFORE UPDATE OR DELETE ON moria.runtime_evaluation_service_profiles
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_service_profile_identity();
CREATE TABLE moria.runtime_evaluations (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  definition jsonb NOT NULL,agent_resource uuid NOT NULL,agent_pin jsonb NOT NULL,
  fixture_resource uuid NOT NULL,fixture_pin jsonb NOT NULL,
  maximum_cases integer NOT NULL,maximum_parallel integer NOT NULL,repeats integer NOT NULL CHECK(repeats BETWEEN 1 AND 16),
  loaded boolean NOT NULL DEFAULT false,case_count integer,markings jsonb NOT NULL,
  service_profile uuid REFERENCES moria.runtime_evaluation_service_profiles(id),service_profile_generation bigint,
  CHECK((service_profile IS NULL)=(service_profile_generation IS NULL)),
  CHECK((loaded AND case_count BETWEEN 1 AND maximum_cases) OR (NOT loaded AND case_count IS NULL))
);
CREATE TABLE moria.runtime_evaluation_cases (
  evaluation uuid NOT NULL REFERENCES moria.runtime_evaluations(execution),
  case_id text NOT NULL CHECK(octet_length(case_id) BETWEEN 1 AND 180),repeat integer NOT NULL CHECK(repeat BETWEEN 0 AND 15),
  input jsonb NOT NULL,expected jsonb NOT NULL,input_hash text NOT NULL CHECK(input_hash~'^[0-9a-f]{64}$'),
  execution uuid NOT NULL UNIQUE REFERENCES moria.runtime_executions(id),
  agent_execution uuid REFERENCES moria.runtime_agent_executions(execution),
  evaluator_execution uuid UNIQUE REFERENCES moria.runtime_executions(id),
  outcome text NOT NULL DEFAULT 'pending' CHECK(outcome IN ('pending','running','pass','fail','error')),
  score double precision,detail text,error text,duration_millis bigint,
  markings jsonb NOT NULL,finished_at timestamptz,
  PRIMARY KEY(evaluation,case_id,repeat),
  CHECK((outcome IN ('pending','running') AND finished_at IS NULL) OR (outcome IN ('pass','fail','error') AND finished_at IS NOT NULL)),
  CHECK(score IS NULL OR (score BETWEEN 0 AND 1)),CHECK(detail IS NULL OR octet_length(detail)<=4096)
);
CREATE INDEX runtime_evaluation_pending ON moria.runtime_evaluation_cases(evaluation,case_id,repeat) WHERE outcome IN ('pending','running');
CREATE TABLE moria.runtime_evaluator_executions (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),evaluation uuid NOT NULL,
  case_id text NOT NULL,repeat integer NOT NULL,
  attempt_id uuid NOT NULL DEFAULT gen_random_uuid(),correlation_id uuid NOT NULL DEFAULT gen_random_uuid(),
  markings jsonb NOT NULL,
  FOREIGN KEY(evaluation,case_id,repeat) REFERENCES moria.runtime_evaluation_cases(evaluation,case_id,repeat)
);
CREATE TABLE moria.runtime_evaluator_receipts (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_evaluator_executions(execution),
  result jsonb NOT NULL,result_hash text NOT NULL CHECK(result_hash~'^[0-9a-f]{64}$'),markings jsonb NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE moria.runtime_evaluation_receipts (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_evaluations(execution),
  summary jsonb NOT NULL,summary_hash text NOT NULL CHECK(summary_hash~'^[0-9a-f]{64}$'),markings jsonb NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION moria.runtime_evaluation_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR TG_TABLE_NAME IN ('runtime_evaluation_receipts','runtime_evaluator_receipts','runtime_evaluator_executions')
    THEN RAISE EXCEPTION 'IMMUTABLE_EVALUATION'; END IF;
  IF TG_TABLE_NAME='runtime_evaluation_environments' THEN
    IF to_jsonb(NEW)-'enabled' IS DISTINCT FROM to_jsonb(OLD)-'enabled' THEN RAISE EXCEPTION 'IMMUTABLE_EVALUATION'; END IF;
  ELSIF TG_TABLE_NAME='runtime_evaluations' THEN
    IF (to_jsonb(NEW)-ARRAY['loaded','case_count','markings']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['loaded','case_count','markings'])
      OR (OLD.loaded AND ROW(NEW.loaded,NEW.case_count) IS DISTINCT FROM ROW(OLD.loaded,OLD.case_count))
      THEN RAISE EXCEPTION 'IMMUTABLE_EVALUATION'; END IF;
  ELSE
    IF ROW(NEW.evaluation,NEW.case_id,NEW.repeat,NEW.input,NEW.expected,NEW.input_hash,NEW.execution)
      IS DISTINCT FROM ROW(OLD.evaluation,OLD.case_id,OLD.repeat,OLD.input,OLD.expected,OLD.input_hash,OLD.execution)
      OR (OLD.agent_execution IS NOT NULL AND NEW.agent_execution IS DISTINCT FROM OLD.agent_execution)
      OR (OLD.evaluator_execution IS NOT NULL AND NEW.evaluator_execution IS DISTINCT FROM OLD.evaluator_execution)
      OR (OLD.outcome IN ('pass','fail','error') AND NEW IS DISTINCT FROM OLD) THEN RAISE EXCEPTION 'IMMUTABLE_EVALUATION'; END IF;
  END IF;
  IF TG_TABLE_NAME IN ('runtime_evaluations','runtime_evaluation_cases') THEN
    IF NOT OLD.markings<@NEW.markings THEN RAISE EXCEPTION 'IMMUTABLE_EVALUATION_MARKINGS'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_evaluation_environment_identity BEFORE UPDATE OR DELETE ON moria.runtime_evaluation_environments FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_immutable();
CREATE TRIGGER runtime_evaluation_identity BEFORE UPDATE OR DELETE ON moria.runtime_evaluations FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_immutable();
CREATE TRIGGER runtime_evaluation_case_identity BEFORE UPDATE OR DELETE ON moria.runtime_evaluation_cases FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_immutable();
CREATE TRIGGER runtime_evaluator_identity BEFORE UPDATE OR DELETE ON moria.runtime_evaluator_executions FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_immutable();
CREATE TRIGGER runtime_evaluation_receipt_identity BEFORE UPDATE OR DELETE ON moria.runtime_evaluation_receipts FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_immutable();
CREATE TRIGGER runtime_evaluator_receipt_identity BEFORE UPDATE OR DELETE ON moria.runtime_evaluator_receipts FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_immutable();
CREATE FUNCTION moria.runtime_evaluation_profile_capture() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE execution moria.runtime_executions; selected moria.runtime_execution_services; profile moria.runtime_evaluation_service_profiles;
BEGIN
 SELECT * INTO execution FROM moria.runtime_executions WHERE id=NEW.execution;
 SELECT * INTO selected FROM moria.runtime_execution_services s WHERE s.binding=execution.binding;
 IF NOT FOUND THEN
   IF NEW.service_profile IS NOT NULL OR NEW.service_profile_generation IS NOT NULL THEN RAISE EXCEPTION 'EVALUATION_SERVICE_REVOKED'; END IF;
   RETURN NEW;
 END IF;
 SELECT * INTO profile FROM moria.runtime_evaluation_service_profiles p WHERE p.environment=execution.environment AND p.resource=execution.resource
   AND p.authority=selected.authority AND p.suite_revision=selected.target_revision AND p.enabled FOR SHARE;
 IF NOT FOUND OR (NEW.service_profile IS NOT NULL AND NEW.service_profile<>profile.id)
   OR (NEW.service_profile_generation IS NOT NULL AND NEW.service_profile_generation<>profile.generation) THEN RAISE EXCEPTION 'EVALUATION_SERVICE_REVOKED'; END IF;
 PERFORM moria.runtime_service_binding_guard(execution.binding);
 PERFORM moria.runtime_service_target_authority(profile.authority,profile.environment,NEW.agent_resource,'agent',profile.agent_revision,'standard');
 PERFORM moria.runtime_service_target_authority(profile.authority,profile.environment,execution.resource,'evaluator',profile.evaluator_revision,'standard');
 NEW.service_profile:=profile.id;NEW.service_profile_generation:=profile.generation;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_evaluation_service_capture BEFORE INSERT ON moria.runtime_evaluations FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_profile_capture();

-- A service evaluation may cross purpose only through its actual leased case and
-- the immutable operator-approved child mapping captured by the accepted suite.
CREATE FUNCTION moria.runtime_evaluation_service_metadata(private_hash text,target uuid,job_domain text,class_name text) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE credential moria.runtime_job_sessions; case_row moria.runtime_evaluation_cases; plan moria.runtime_evaluations;
 profile moria.runtime_evaluation_service_profiles; case_execution moria.runtime_executions; origin moria.runtime_job_delegations;
 state jsonb; selected jsonb; revision text; labels jsonb; required text;
BEGIN
 SELECT * INTO credential FROM moria.runtime_job_sessions WHERE token_hash=private_hash;
 IF NOT FOUND OR credential.task IS NULL THEN RETURN NULL; END IF;
 SELECT * INTO case_row FROM moria.runtime_evaluation_cases WHERE execution=credential.execution;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT * INTO origin FROM moria.runtime_job_delegations WHERE execution=case_row.execution;
 IF origin.origin_kind<>'service' THEN RETURN NULL; END IF;
 state:=moria.native_serving_session(private_hash);
 SELECT * INTO case_execution FROM moria.runtime_executions WHERE id=case_row.execution FOR SHARE;
 SELECT * INTO plan FROM moria.runtime_evaluations WHERE execution=case_row.evaluation;
 SELECT * INTO profile FROM moria.runtime_evaluation_service_profiles WHERE id=plan.service_profile AND enabled FOR SHARE;
 IF NOT FOUND OR profile.generation<>plan.service_profile_generation OR profile.authority<>origin.service_authority
   OR profile.environment<>case_execution.environment OR profile.resource<>case_execution.resource
   OR class_name<>'standard' OR NOT ((job_domain='agent' AND target=plan.agent_resource) OR (job_domain='evaluator' AND target=case_execution.resource))
   THEN RAISE EXCEPTION 'EVALUATION_SERVICE_REVOKED'; END IF;
 revision:=CASE job_domain WHEN 'agent' THEN profile.agent_revision ELSE profile.evaluator_revision END;
 selected:=moria.runtime_service_target_authority(profile.authority,profile.environment,target,job_domain,revision,class_name);
 IF ROW(selected->>'environment',selected->>'principal',selected->>'application') IS DISTINCT FROM ROW(state->>'environment',state->>'principal',state->>'application')
   THEN RAISE EXCEPTION 'EVALUATION_CASE_AUTHORITY'; END IF;
 required:=target::text||CASE job_domain WHEN 'agent' THEN ':invoke' ELSE ':run' END;
 IF NOT(state->'identity'->'grantedOperations' ? required) OR NOT(state->'identity'->'applicationScopes' ? required)
   THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 labels:=moria.native_resource_release_access_state(state,profile.resource::text,'run',plan.release);
 labels:=labels||moria.native_resource_filesystem_access_state(state,plan.fixture_resource::text,'read');
 IF NOT labels<@COALESCE((SELECT jsonb_agg(value->>'marking') FROM jsonb_array_elements(case_row.markings)),'[]') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
 RETURN jsonb_build_object('case',case_row.execution,'parentDeadline',case_execution.deadline,'authority',profile.authority,'revision',revision,'markings',case_row.markings);
END $$;
CREATE FUNCTION moria.runtime_evaluation_service_binding_guard(binding_id uuid) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE binding moria.runtime_execution_bindings; metadata jsonb; selected moria.runtime_execution_services;
BEGIN
 SELECT * INTO binding FROM moria.runtime_execution_bindings WHERE id=binding_id;
 IF binding.context->>'evaluationCase' IS NULL THEN RETURN; END IF;
 metadata:=moria.runtime_evaluation_service_metadata(binding.context->>'evaluationParentHash',binding.resource,binding.spec->>'domain',binding.spec->>'resourceClass');
 SELECT * INTO selected FROM moria.runtime_execution_services s WHERE s.binding=binding_id;
 IF metadata IS NULL OR metadata->>'case' IS DISTINCT FROM binding.context->>'evaluationCase'
   OR metadata->>'case' IS DISTINCT FROM binding.context->>'parentExecution'
   OR ROW(metadata->>'authority',metadata->>'revision') IS DISTINCT FROM ROW(selected.authority::text,selected.target_revision)
   OR NOT COALESCE((SELECT array_agg(value->>'marking') FROM jsonb_array_elements(metadata->'markings')),ARRAY[]::text[])<@binding.markings
   THEN RAISE EXCEPTION 'EVALUATION_SERVICE_REVOKED'; END IF;
 PERFORM moria.runtime_service_binding_guard(binding_id);
END $$;
CREATE FUNCTION moria.runtime_evaluation_service_child_state(execution_id uuid) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE execution moria.runtime_executions; state jsonb; parent_state jsonb; member text;
BEGIN
 SELECT * INTO execution FROM moria.runtime_executions WHERE id=execution_id;
 PERFORM moria.runtime_evaluation_service_binding_guard(execution.binding);
 state:=moria.runtime_service_binding_guard(execution.binding);
 parent_state:=moria.native_serving_session(execution.context->>'evaluationParentHash');
 FOREACH member IN ARRAY ARRAY['grantedOperations','applicationScopes','markings'] LOOP
   state:=jsonb_set(state,ARRAY['identity',member],COALESCE((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM(
     SELECT jsonb_array_elements_text(state->'identity'->member) AS value INTERSECT SELECT jsonb_array_elements_text(parent_state->'identity'->member)) allowed),'[]'));
 END LOOP;
 RETURN state;
END $$;
-- A controller resumes identity only; retained child data stays behind its ordinary release gate.
CREATE FUNCTION moria.runtime_evaluation_agent_resume(private_hash text,case_execution_id uuid) RETURNS uuid
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; selected record; accepted uuid;
BEGIN
 state:=moria.native_serving_session(private_hash);
 SELECT c.*,r.agent_resource,r.agent_pin INTO selected FROM moria.runtime_evaluation_cases c
   JOIN moria.runtime_evaluations r ON r.execution=c.evaluation
   JOIN moria.runtime_executions e ON e.id=c.execution
   JOIN moria.runtime_job_sessions credential ON credential.execution=e.id
     AND credential.token_hash=private_hash AND credential.task IS NOT NULL
   WHERE c.execution=case_execution_id AND e.status='running'
     AND ROW(e.environment,e.principal,e.application)=ROW(state->>'environment',state->>'principal',state->>'application');
 IF NOT FOUND THEN RAISE EXCEPTION 'EVALUATION_CASE_AUTHORITY'; END IF;
 IF selected.agent_execution IS NULL THEN RETURN NULL; END IF;
 SELECT e.id INTO accepted FROM moria.runtime_executions e
   JOIN moria.runtime_agent_executions a ON a.execution=e.id
   JOIN moria.runtime_execution_bindings b ON b.id=e.binding AND b.enabled
   JOIN moria.runtime_job_delegations d ON d.execution=e.id AND d.parent_execution=case_execution_id
   JOIN moria.indexing_instance incarnation ON incarnation.incarnation=b.incarnation
   WHERE e.id=selected.agent_execution AND e.resource=selected.agent_resource AND e.spec->>'domain'='agent'
     AND e.request_key='evaluation/'||case_execution_id::text||'/agent'
     AND e.context->>'parentExecution'=case_execution_id::text AND e.input_hash=selected.input_hash
     AND ROW(e.environment,e.principal,e.application)=ROW(state->>'environment',state->>'principal',state->>'application')
     AND ROW(d.environment,d.principal,d.application)=ROW(e.environment,e.principal,e.application)
     AND b.environment=e.environment AND b.resource=e.resource AND b.spec=e.spec AND b.context=e.context
     AND ROW(a.entry_id::text,a.release,a.adapter_id::text,a.application_id::text,a.model_resource)
       IS NOT DISTINCT FROM ROW(selected.agent_pin->>'entryId',selected.agent_pin->>'release',
         selected.agent_pin->>'adapterId',selected.agent_pin->>'applicationId',selected.agent_pin->>'modelResource')
     AND a.definition=selected.agent_pin->'definition' AND a.adapter_definition=selected.agent_pin->'adapterDefinition'
     AND a.model_binding=selected.agent_pin->'modelBinding'
     AND a.model_secret_approval IS NOT DISTINCT FROM NULLIF(selected.agent_pin->'modelSecretApproval','null'::jsonb);
 IF NOT FOUND THEN RAISE EXCEPTION 'EVALUATION_PIN_INVALID'; END IF;
 RETURN accepted;
END $$;
CREATE FUNCTION moria.runtime_evaluation_service_link_child() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE child moria.runtime_executions; changed integer;
BEGIN
 SELECT * INTO child FROM moria.runtime_executions WHERE id=NEW.execution;
 IF child.context->>'evaluationCase' IS NULL THEN RETURN NEW; END IF;
 PERFORM moria.runtime_evaluation_service_binding_guard(child.binding);
 IF child.spec->>'domain'='agent' THEN
   UPDATE moria.runtime_evaluation_cases SET agent_execution=child.id
     WHERE execution=(child.context->>'evaluationCase')::uuid AND agent_execution IS NULL;
 ELSIF child.spec->>'domain'='evaluator' THEN
   UPDATE moria.runtime_evaluation_cases SET evaluator_execution=child.id
     WHERE execution=(child.context->>'evaluationCase')::uuid AND evaluator_execution IS NULL;
 ELSE RAISE EXCEPTION 'EVALUATION_CASE_AUTHORITY'; END IF;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'EVALUATION_CHILD_ALREADY_ACCEPTED'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_evaluation_agent_service_link AFTER INSERT ON moria.runtime_agent_executions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_service_link_child();
CREATE TRIGGER runtime_evaluation_evaluator_service_link AFTER INSERT ON moria.runtime_evaluator_executions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_evaluation_service_link_child();
CREATE OR REPLACE VIEW moria.runtime_sandbox_metadata AS
  SELECT execution,entry_id,release,definition,attempt_id,correlation_id,markings,prepare_only,'function'::text AS domain FROM moria.runtime_function_invocations
  UNION ALL SELECT execution,entry_id,release,definition,attempt_id,correlation_id,markings,false,'agent'::text FROM moria.runtime_agent_executions
  UNION ALL SELECT e.execution,r.entry_id,r.release,r.definition,e.attempt_id,e.correlation_id,e.markings,false,'evaluator'::text
    FROM moria.runtime_evaluator_executions e JOIN moria.runtime_evaluations r ON r.execution=e.evaluation;
CREATE FUNCTION moria.runtime_evaluation_case_admission(execution_id uuid) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE parent uuid; case_class text; maximum integer; active integer;
BEGIN
  SELECT c.evaluation,e.resource_class INTO parent,case_class FROM moria.runtime_evaluation_cases c
    JOIN moria.runtime_executions e ON e.id=c.execution WHERE c.execution=execution_id;
  IF NOT FOUND THEN RETURN true; END IF;
  SELECT maximum_parallel INTO maximum FROM moria.runtime_evaluations WHERE execution=parent FOR UPDATE;
  SELECT count(*) INTO active FROM moria.runtime_evaluation_cases c JOIN moria.runtime_executions e ON e.id=c.execution
    WHERE c.evaluation=parent AND c.execution<>execution_id AND
      ((e.status='preparing' AND e.lease_until>clock_timestamp()) OR (e.status='running' AND EXISTS(
        SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=e.id AND t.status='running' AND t.lease_until>clock_timestamp())));
  IF active>=maximum THEN RETURN false; END IF;
  -- A case can seal between the worker's publication scan and its pending-task claim.
  -- Under the same suite lock, let a ready sibling take the freed slot first.
  IF EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=execution_id AND t.status='pending') THEN
    PERFORM 1 FROM moria.runtime_evaluation_cases c JOIN moria.runtime_executions ready ON ready.id=c.execution
      WHERE c.evaluation=parent AND c.execution<>execution_id AND c.outcome IN ('pending','running')
        AND ready.resource_class=case_class AND ready.status='running' AND ready.deadline>clock_timestamp()
        AND ready.spec->>'domain'='evaluation' AND moria.runtime_environment_cell_available(ready.environment)
        AND NOT EXISTS(SELECT 1 FROM moria.runtime_sandbox_bindings b WHERE b.binding=ready.binding AND b.domain IN ('transform','sync'))
        AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_cancellations x WHERE x.execution=ready.id)
        AND EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=ready.id AND t.status='sealed')
        AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.execution=ready.id AND t.status<>'sealed')
      LIMIT 1 FOR UPDATE OF ready SKIP LOCKED;
    IF FOUND THEN RETURN false; END IF;
  END IF;
  RETURN true;
END $$;
`;
