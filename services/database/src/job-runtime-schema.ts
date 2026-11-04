/** Accepted bounded delegation and private credentials scoped to one actual live worker lease. */
export const jobRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_service_authorities (
  id uuid PRIMARY KEY, environment text NOT NULL REFERENCES moria.environments(id),
  principal text NOT NULL, application text NOT NULL,
  operations text[] NOT NULL CHECK(cardinality(operations) BETWEEN 1 AND 4096 AND array_position(operations,NULL) IS NULL),
  markings text[] NOT NULL CHECK(cardinality(markings)<=256 AND array_position(markings,NULL) IS NULL),
  domains text[] NOT NULL CHECK(cardinality(domains) BETWEEN 1 AND 16 AND array_position(domains,NULL) IS NULL),
  resource_classes text[] NOT NULL CHECK(cardinality(resource_classes) BETWEEN 1 AND 64 AND array_position(resource_classes,NULL) IS NULL),
  incarnation uuid NOT NULL, enabled boolean NOT NULL DEFAULT true,generation bigint NOT NULL DEFAULT 1 CHECK(generation>0)
);
CREATE TABLE moria.runtime_service_targets (
  environment text NOT NULL REFERENCES moria.environments(id),resource uuid NOT NULL,
  domain text NOT NULL,binding_revision text NOT NULL CHECK(octet_length(binding_revision) BETWEEN 1 AND 128),
  resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
  authority uuid NOT NULL REFERENCES moria.runtime_service_authorities(id),enabled boolean NOT NULL DEFAULT true,generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),
  PRIMARY KEY(environment,resource,domain,binding_revision),
  UNIQUE(authority,environment,resource,domain,binding_revision)
);
CREATE TABLE moria.runtime_execution_services (
  binding uuid PRIMARY KEY REFERENCES moria.runtime_execution_bindings(id),
  authority uuid NOT NULL REFERENCES moria.runtime_service_authorities(id),target_revision text NOT NULL,
  authority_generation bigint NOT NULL CHECK(authority_generation>0),target_generation bigint NOT NULL CHECK(target_generation>0)
);
CREATE INDEX runtime_service_authority_actor ON moria.runtime_service_authorities(environment,principal) WHERE enabled;
CREATE INDEX runtime_service_authority_application ON moria.runtime_service_authorities(environment,application) WHERE enabled;
CREATE FUNCTION moria.runtime_service_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR TG_TABLE_NAME='runtime_execution_services' THEN RAISE EXCEPTION 'IMMUTABLE_SERVICE_PURPOSE'; END IF;
  IF NEW.generation<>OLD.generation
    OR (to_jsonb(NEW)-'enabled'-'generation')<>(to_jsonb(OLD)-'enabled'-'generation') THEN RAISE EXCEPTION 'IMMUTABLE_SERVICE_PURPOSE'; END IF;
  IF TG_TABLE_NAME='runtime_service_authorities' AND NEW.enabled AND NOT OLD.enabled THEN
    IF NEW.incarnation<>(SELECT incarnation FROM moria.indexing_instance) OR NOT EXISTS(
      SELECT 1 FROM moria.principals p JOIN moria.application_scopes a ON a.environment=p.environment AND a.application=NEW.application
      LEFT JOIN moria.instance_principal_bindings b ON b.environment=p.environment AND b.principal=p.principal
      LEFT JOIN moria.instance_principals i ON i.id=b.instance_id
      WHERE p.environment=NEW.environment AND p.principal=NEW.principal AND p.enabled AND p.kind='workload' AND a.enabled
        AND (b.instance_id IS NULL OR (i.enabled AND i.kind='workload'))) THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  END IF;
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN NEW.generation:=OLD.generation+1; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_service_authority_identity BEFORE UPDATE OR DELETE ON moria.runtime_service_authorities FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_immutable();
CREATE TRIGGER runtime_service_target_identity BEFORE UPDATE OR DELETE ON moria.runtime_service_targets FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_immutable();
CREATE TRIGGER runtime_execution_service_identity BEFORE UPDATE OR DELETE ON moria.runtime_execution_services FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_immutable();
CREATE FUNCTION moria.runtime_service_revocation_epoch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN
    UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_service_authority_epoch AFTER UPDATE ON moria.runtime_service_authorities
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_revocation_epoch();
CREATE TRIGGER runtime_service_target_epoch AFTER UPDATE ON moria.runtime_service_targets
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_revocation_epoch();
CREATE FUNCTION moria.runtime_service_identity_revocation() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF TG_TABLE_NAME='principals' THEN
   IF TG_OP='INSERT' THEN
     UPDATE moria.runtime_service_authorities SET enabled=false WHERE environment=NEW.environment AND principal=NEW.principal AND enabled;
   ELSIF TG_OP='DELETE' OR NEW.enabled IS DISTINCT FROM OLD.enabled OR NEW.kind IS DISTINCT FROM OLD.kind THEN
     UPDATE moria.runtime_service_authorities SET enabled=false WHERE environment=OLD.environment AND principal=OLD.principal AND enabled;
   END IF;
 ELSIF TG_TABLE_NAME='application_scopes' THEN
   IF TG_OP='INSERT' THEN
     UPDATE moria.runtime_service_authorities SET enabled=false WHERE environment=NEW.environment AND application=NEW.application AND enabled;
   ELSIF TG_OP='DELETE' OR NEW.enabled IS DISTINCT FROM OLD.enabled THEN
     UPDATE moria.runtime_service_authorities SET enabled=false WHERE environment=OLD.environment AND application=OLD.application AND enabled;
   END IF;
 ELSIF TG_TABLE_NAME='instance_principals' THEN
   IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND (NEW.enabled IS DISTINCT FROM OLD.enabled OR NEW.kind IS DISTINCT FROM OLD.kind)) THEN
     UPDATE moria.runtime_service_authorities a SET enabled=false FROM moria.instance_principal_bindings b
       WHERE b.instance_id=OLD.id AND a.environment=b.environment AND a.principal=b.principal AND a.enabled;
   END IF;
 ELSE
   IF TG_OP='INSERT' THEN
     UPDATE moria.runtime_service_authorities SET enabled=false WHERE environment=NEW.environment AND principal=NEW.principal AND enabled;
   ELSIF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN
     UPDATE moria.runtime_service_authorities SET enabled=false WHERE environment=OLD.environment AND principal=OLD.principal AND enabled;
   END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER runtime_service_actor_revocation AFTER INSERT OR UPDATE OR DELETE ON moria.principals FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_identity_revocation();
CREATE TRIGGER runtime_service_application_revocation AFTER INSERT OR UPDATE OR DELETE ON moria.application_scopes FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_identity_revocation();
CREATE TRIGGER runtime_service_instance_revocation AFTER UPDATE OR DELETE ON moria.instance_principals FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_identity_revocation();
CREATE TRIGGER runtime_service_identity_binding_revocation AFTER INSERT OR UPDATE OR DELETE ON moria.instance_principal_bindings FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_identity_revocation();

CREATE FUNCTION moria.runtime_service_target_authority(service_id uuid,env text,target uuid,job_domain text,target_revision text,class_name text)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE registered moria.runtime_service_authorities; state jsonb;
BEGIN
  SELECT a.* INTO registered FROM moria.runtime_service_authorities a JOIN moria.runtime_service_targets t
    ON t.authority=a.id AND t.enabled AND t.environment=env AND t.resource=target AND t.domain=job_domain
      AND t.binding_revision=target_revision AND t.resource_class=class_name
    WHERE a.id=service_id AND a.enabled AND a.environment=env AND job_domain=ANY(a.domains)
      AND class_name=ANY(a.resource_classes) AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance) FOR SHARE OF a,t;
  IF NOT FOUND THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  state:=moria.native_serving_service_authority(registered.id);
  IF ROW(state->>'environment',state->>'principal',state->>'application')
    IS DISTINCT FROM ROW(registered.environment,registered.principal,registered.application) THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  RETURN state;
END $$;
CREATE FUNCTION moria.runtime_service_resource_labels(service_id uuid,env text,target uuid,job_domain text,target_revision text,class_name text,
  requested_resource text,requested_operation text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; labels jsonb; permission text;
BEGIN
  state:=moria.runtime_service_target_authority(service_id,env,target,job_domain,target_revision,class_name);
  permission:=requested_resource||':'||requested_operation;
  IF NOT (state->'identity'->'grantedOperations' ? permission) OR NOT (state->'identity'->'applicationScopes' ? permission)
    THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  labels:=moria.native_serving_resource_access_state(state,requested_resource,'read');
  IF NOT labels<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  RETURN labels;
END $$;

CREATE FUNCTION moria.runtime_service_binding_guard(binding_id uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE b moria.runtime_execution_bindings; registered record; state jsonb;
BEGIN
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=binding_id;
  SELECT s.authority,s.target_revision,a.environment,a.principal,a.application,a.operations,a.markings,a.domains,a.resource_classes
    INTO registered FROM moria.runtime_execution_services s JOIN moria.runtime_service_authorities a ON a.id=s.authority AND a.enabled
    JOIN moria.runtime_service_targets t ON t.authority=a.id AND t.environment=b.environment AND t.resource=b.resource
      AND t.domain=b.spec->>'domain' AND t.resource_class=b.spec->>'resourceClass' AND t.binding_revision=s.target_revision AND t.enabled
      AND s.authority_generation=a.generation AND s.target_generation=t.generation
    WHERE s.binding=b.id AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance) FOR SHARE OF a,t;
  IF NOT FOUND OR NOT b.enabled OR b.context->>'sessionHash' IS NOT NULL
    OR ROW(registered.environment,registered.principal,registered.application)
      IS DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application')
    OR NOT (b.spec->>'domain'=ANY(registered.domains)) OR NOT (b.spec->>'resourceClass'=ANY(registered.resource_classes))
    OR NOT b.operations<@registered.operations OR NOT b.markings<@registered.markings THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  state:=moria.native_serving_service_authority(registered.authority);
  IF NOT to_jsonb(b.operations)<@(state->'identity'->'grantedOperations')
    OR NOT to_jsonb(b.operations)<@(state->'identity'->'applicationScopes')
    OR NOT to_jsonb(b.markings)<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  RETURN state;
END $$;
CREATE FUNCTION moria.runtime_execution_service_validate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE b moria.runtime_execution_bindings; state jsonb; authority_generation bigint; target_generation bigint;
BEGIN
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=NEW.binding;
  state:=moria.runtime_service_target_authority(NEW.authority,b.environment,b.resource,b.spec->>'domain',NEW.target_revision,b.spec->>'resourceClass');
  SELECT a.generation,t.generation INTO authority_generation,target_generation FROM moria.runtime_service_authorities a
    JOIN moria.runtime_service_targets t ON t.authority=a.id AND t.environment=b.environment AND t.resource=b.resource
      AND t.domain=b.spec->>'domain' AND t.binding_revision=NEW.target_revision
    WHERE a.id=NEW.authority FOR SHARE OF a,t;
  IF (NEW.authority_generation IS NOT NULL AND NEW.authority_generation<>authority_generation)
    OR (NEW.target_generation IS NOT NULL AND NEW.target_generation<>target_generation) THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  NEW.authority_generation:=authority_generation;NEW.target_generation:=target_generation;
  IF b.context->>'sessionHash' IS NOT NULL OR b.context->>'serviceAuthority' IS DISTINCT FROM NEW.authority::text
    OR b.context->>'serviceTargetRevision' IS DISTINCT FROM NEW.target_revision
    OR ROW(b.environment,b.context->>'principal',b.context->>'application')
      IS DISTINCT FROM ROW(state->>'environment',state->>'principal',state->>'application')
    OR NOT to_jsonb(b.operations)<@(state->'identity'->'grantedOperations')
    OR NOT to_jsonb(b.operations)<@(state->'identity'->'applicationScopes')
    OR NOT to_jsonb(b.markings)<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_execution_service_valid BEFORE INSERT ON moria.runtime_execution_services FOR EACH ROW EXECUTE FUNCTION moria.runtime_execution_service_validate();

CREATE TABLE moria.runtime_job_delegations (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  environment text NOT NULL, principal text NOT NULL, application text NOT NULL,
  origin_kind text NOT NULL DEFAULT 'session' CHECK(origin_kind IN ('session','service')),
  origin_session_hash text CHECK(origin_session_hash ~ '^[0-9a-f]{64}$'),
  service_authority uuid REFERENCES moria.runtime_service_authorities(id),origin_binding uuid REFERENCES moria.runtime_execution_bindings(id),
  accepted_grants text[] NOT NULL, accepted_scopes text[] NOT NULL, accepted_markings text[] NOT NULL,
  parent_execution uuid REFERENCES moria.runtime_job_delegations(execution),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK((origin_kind='session' AND origin_session_hash IS NOT NULL AND service_authority IS NULL AND origin_binding IS NULL)
    OR (origin_kind='service' AND origin_session_hash IS NULL AND service_authority IS NOT NULL AND origin_binding IS NOT NULL))
);
CREATE INDEX runtime_job_delegation_origin_session ON moria.runtime_job_delegations(origin_session_hash,execution) WHERE origin_kind='session';
CREATE INDEX runtime_job_delegation_parent ON moria.runtime_job_delegations(parent_execution,execution) WHERE parent_execution IS NOT NULL;
CREATE TABLE moria.runtime_job_sessions (
  token_hash text PRIMARY KEY CHECK(token_hash ~ '^[0-9a-f]{64}$'),
  execution uuid NOT NULL REFERENCES moria.runtime_job_delegations(execution),
  task uuid REFERENCES moria.runtime_execution_tasks(id), owner uuid NOT NULL REFERENCES moria.runtime_workers(owner),
  fence bigint NOT NULL CHECK(fence>0), host_id uuid NOT NULL, incarnation uuid NOT NULL,
  expires_at timestamptz NOT NULL,worker_generation bigint NOT NULL CHECK(worker_generation>0),
  UNIQUE NULLS NOT DISTINCT(execution,task,owner,fence,host_id,incarnation)
);
CREATE FUNCTION moria.runtime_job_acceptance_state(execution_id uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; state jsonb; parent moria.runtime_job_delegations; service uuid; origin_binding uuid;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR SHARE;
  IF NOT FOUND OR e.status<>'queued' OR EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE execution=e.id AND attempts<>0)
    THEN RAISE EXCEPTION 'JOB_ACCEPTANCE_REQUIRED'; END IF;
  IF e.context->>'sessionHash' IS NULL THEN
    SELECT authority INTO service FROM moria.runtime_execution_services WHERE binding=e.binding;
    IF NOT FOUND THEN RETURN NULL; END IF;
    state:=moria.runtime_service_binding_guard(e.binding);origin_binding:=e.binding;
    IF e.context->>'automationFiring' IS NOT NULL THEN state:=moria.runtime_automation_child_state(e.id); END IF;
    IF e.context->>'evaluationCase' IS NOT NULL THEN
      state:=moria.runtime_evaluation_service_child_state(e.id);
      SELECT d.* INTO parent FROM moria.runtime_job_sessions s JOIN moria.runtime_job_delegations d ON d.execution=s.execution
        WHERE s.token_hash=e.context->>'evaluationParentHash';
      IF NOT FOUND OR parent.execution IS DISTINCT FROM (e.context->>'evaluationCase')::uuid THEN RAISE EXCEPTION 'EVALUATION_CASE_AUTHORITY'; END IF;
    END IF;
    IF e.context->>'serviceParentExecution' IS NOT NULL THEN
      state:=moria.runtime_sandbox_service_child_state(e.id);
      SELECT d.* INTO parent FROM moria.runtime_job_sessions s JOIN moria.runtime_job_delegations d ON d.execution=s.execution
        WHERE s.token_hash=e.context->>'serviceParentHash';
      IF NOT FOUND OR parent.execution IS DISTINCT FROM (e.context->>'serviceParentExecution')::uuid THEN RAISE EXCEPTION 'JOB_PARENT_INACTIVE'; END IF;
    END IF;
  ELSE
    state:=moria.native_serving_session(e.context->>'sessionHash');
    SELECT d.* INTO parent FROM moria.runtime_job_sessions s JOIN moria.runtime_job_delegations d ON d.execution=s.execution
      WHERE s.token_hash=e.context->>'sessionHash';
    IF FOUND THEN
      service:=parent.service_authority;origin_binding:=parent.origin_binding;
      IF service IS NOT NULL AND NOT EXISTS(SELECT 1 FROM moria.runtime_execution_bindings root
        WHERE root.id=origin_binding AND ROW(root.environment,root.resource,root.spec->>'domain',root.spec->>'resourceClass')
          =ROW(e.environment,e.resource,e.spec->>'domain',e.resource_class)) THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
    ELSIF NOT EXISTS(SELECT 1 FROM moria.workload_sessions WHERE token_hash=e.context->>'sessionHash'
      UNION ALL SELECT 1 FROM moria.browser_sessions WHERE token_hash=e.context->>'sessionHash') THEN RAISE EXCEPTION 'JOB_ORIGIN_UNSUPPORTED'; END IF;
  END IF;
  IF ROW(state->>'environment',state->>'principal',state->>'application')
    IS DISTINCT FROM ROW(e.environment,e.principal,e.application) THEN RAISE EXCEPTION 'JOB_ACTOR_MISMATCH'; END IF;
  RETURN jsonb_build_object('state',state,'originKind',CASE WHEN service IS NULL THEN 'session' ELSE 'service' END,
    'originSessionHash',CASE WHEN service IS NULL THEN COALESCE(parent.origin_session_hash,e.context->>'sessionHash') ELSE NULL END,
    'parentExecution',parent.execution,'serviceAuthority',service,'originBinding',origin_binding);
END $$;
CREATE FUNCTION moria.runtime_job_acceptance_validate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; state jsonb; expected jsonb; granted text[]; scopes text[]; labels text[];
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=NEW.execution FOR SHARE;
  expected:=moria.runtime_job_acceptance_state(e.id);state:=expected->'state';
  IF expected IS NULL OR ROW(NEW.environment,NEW.principal,NEW.application) IS DISTINCT FROM ROW(e.environment,e.principal,e.application)
    THEN RAISE EXCEPTION 'JOB_ACCEPTANCE_REQUIRED'; END IF;
  granted:=ARRAY(SELECT jsonb_array_elements_text(state->'identity'->'grantedOperations'));
  scopes:=ARRAY(SELECT jsonb_array_elements_text(state->'identity'->'applicationScopes'));
  labels:=ARRAY(SELECT jsonb_array_elements_text(state->'identity'->'markings'));
  IF NOT (NEW.accepted_grants<@granted AND granted<@NEW.accepted_grants
    AND NEW.accepted_scopes<@scopes AND scopes<@NEW.accepted_scopes
    AND NEW.accepted_markings<@labels AND labels<@NEW.accepted_markings) THEN RAISE EXCEPTION 'JOB_CAPABILITY_MISMATCH'; END IF;
  IF NEW.origin_kind IS DISTINCT FROM expected->>'originKind'
    OR NEW.origin_session_hash IS DISTINCT FROM expected->>'originSessionHash'
    OR NEW.parent_execution IS DISTINCT FROM (expected->>'parentExecution')::uuid
    OR NEW.service_authority IS DISTINCT FROM (expected->>'serviceAuthority')::uuid
    OR NEW.origin_binding IS DISTINCT FROM (expected->>'originBinding')::uuid
    THEN RAISE EXCEPTION 'JOB_ORIGIN_MISMATCH'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_job_acceptance_valid BEFORE INSERT ON moria.runtime_job_delegations FOR EACH ROW EXECUTE FUNCTION moria.runtime_job_acceptance_validate();
CREATE FUNCTION moria.runtime_job_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_JOB_AUTHORITY'; END $$;
CREATE TRIGGER runtime_job_delegation_immutable BEFORE UPDATE OR DELETE ON moria.runtime_job_delegations FOR EACH ROW EXECUTE FUNCTION moria.runtime_job_immutable();
CREATE TRIGGER runtime_job_session_immutable BEFORE UPDATE OR DELETE ON moria.runtime_job_sessions FOR EACH ROW EXECUTE FUNCTION moria.runtime_job_immutable();

CREATE FUNCTION moria.runtime_job_delegation_guard(execution_id uuid, completed_receipt boolean DEFAULT false) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE d moria.runtime_job_delegations; e moria.runtime_executions; parent record; depth integer:=0;
BEGIN
  SELECT * INTO d FROM moria.runtime_job_delegations WHERE execution=execution_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'JOB_DELEGATION_REQUIRED'; END IF;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=d.execution FOR SHARE;
  IF NOT FOUND OR (completed_receipt AND (e.status<>'succeeded' OR e.finished_at IS NULL OR e.finished_at<=clock_timestamp()-interval '30 days'))
    OR (NOT completed_receipt AND (e.status IN ('failed','cancelled') OR e.deadline<=clock_timestamp()))
    OR ROW(d.environment,d.principal,d.application) IS DISTINCT FROM ROW(e.environment,e.principal,e.application)
    OR NOT EXISTS(SELECT 1 FROM moria.runtime_execution_bindings b,moria.indexing_instance i
      WHERE b.id=e.binding AND b.enabled AND b.incarnation=i.incarnation AND b.spec=e.spec AND b.context=e.context)
    THEN RAISE EXCEPTION 'JOB_DELEGATION_INACTIVE'; END IF;
  IF d.origin_kind='service' THEN
    PERFORM moria.runtime_service_binding_guard(d.origin_binding);
    IF EXISTS(SELECT 1 FROM moria.runtime_execution_bindings b WHERE b.id=d.origin_binding AND b.context->>'automationFiring' IS NOT NULL)
      THEN PERFORM moria.runtime_automation_binding_guard(d.origin_binding); END IF;
    IF EXISTS(SELECT 1 FROM moria.runtime_execution_bindings b WHERE b.id=d.origin_binding AND b.context->>'evaluationCase' IS NOT NULL)
      THEN PERFORM moria.runtime_evaluation_service_binding_guard(d.origin_binding); END IF;
    IF EXISTS(SELECT 1 FROM moria.runtime_execution_bindings b WHERE b.id=d.origin_binding AND b.context->>'serviceParentExecution' IS NOT NULL)
      THEN PERFORM moria.runtime_sandbox_service_child_binding_guard(d.origin_binding); END IF;
    IF NOT EXISTS(SELECT 1 FROM moria.runtime_execution_services s JOIN moria.runtime_execution_bindings b ON b.id=s.binding
      WHERE s.binding=d.origin_binding AND s.authority=d.service_authority
        AND ROW(b.environment,b.context->>'principal',b.context->>'application')=ROW(d.environment,d.principal,d.application))
      THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
    IF NOT EXISTS(SELECT 1 FROM moria.runtime_service_authorities a JOIN moria.runtime_execution_bindings b ON b.id=e.binding
      JOIN moria.runtime_execution_bindings root ON root.id=d.origin_binding
      WHERE a.id=d.service_authority AND e.spec->>'domain'=ANY(a.domains) AND e.resource_class=ANY(a.resource_classes)
        AND ROW(b.environment,b.resource,b.spec->>'domain',b.spec->>'resourceClass')
          =ROW(root.environment,root.resource,root.spec->>'domain',root.spec->>'resourceClass')
        AND b.operations<@a.operations AND b.markings<@a.markings) THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  END IF;
  FOR parent IN WITH RECURSIVE chain AS (
    SELECT p.execution,p.parent_execution,1 AS depth FROM moria.runtime_job_delegations p WHERE p.execution=d.parent_execution
    UNION ALL SELECT p.execution,p.parent_execution,c.depth+1 FROM chain c JOIN moria.runtime_job_delegations p ON p.execution=c.parent_execution WHERE c.depth<=8
  ) SELECT j.* FROM chain c JOIN moria.runtime_executions j ON j.id=c.execution ORDER BY j.id FOR SHARE OF j LOOP
    depth:=depth+1;
    IF depth>8 OR (NOT completed_receipt AND (parent.status IN ('failed','cancelled') OR parent.deadline<=clock_timestamp()
        OR EXISTS(SELECT 1 FROM moria.runtime_execution_cancellations c WHERE c.execution=parent.id)))
      OR (completed_receipt AND (parent.deadline<=e.finished_at
        OR (parent.status IN ('failed','cancelled') AND (parent.finished_at IS NULL OR parent.finished_at<e.finished_at))
        OR EXISTS(SELECT 1 FROM moria.runtime_execution_cancellations c WHERE c.execution=parent.id AND c.requested_at<=e.finished_at)))
      OR ROW(parent.environment,parent.principal,parent.application) IS DISTINCT FROM ROW(e.environment,e.principal,e.application)
      THEN RAISE EXCEPTION 'JOB_PARENT_INACTIVE'; END IF;
  END LOOP;
  RETURN jsonb_build_object('execution',d.execution,'originKind',d.origin_kind,'originSessionHash',d.origin_session_hash,
    'serviceAuthority',d.service_authority,'originBinding',d.origin_binding,
    'environment',d.environment,'principal',d.principal,'application',d.application,
    'acceptedGrants',to_jsonb(d.accepted_grants),'acceptedScopes',to_jsonb(d.accepted_scopes),
    'acceptedMarkings',to_jsonb(d.accepted_markings));
END $$;

-- This returns only current receipt-inspection authority. It never makes the private
-- credential valid in native_serving_session or authorizes any further object IO.
CREATE FUNCTION moria.runtime_job_receipt_authority(private_hash text,execution_id uuid,worker uuid,worker_fence bigint)
  RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE s moria.runtime_job_sessions; e moria.runtime_executions; proof jsonb;
BEGIN
  SELECT * INTO s FROM moria.runtime_job_sessions WHERE token_hash=private_hash AND execution=execution_id
    AND task IS NULL AND owner=worker AND fence=worker_fence;
  IF NOT FOUND OR s.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'JOB_RECEIPT_AUTHORITY_REQUIRED'; END IF;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=s.execution FOR SHARE;
  IF NOT FOUND OR e.status<>'succeeded' OR e.owner IS DISTINCT FROM s.owner OR e.fence<>s.fence
    OR e.result IS NULL THEN RAISE EXCEPTION 'JOB_RECEIPT_AUTHORITY_REQUIRED'; END IF;
  PERFORM moria.runtime_worker_check(s.owner,s.host_id,e.resource_class,e.spec->>'domain');
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_workers w WHERE w.owner=s.owner AND w.generation=s.worker_generation)
    THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  proof:=moria.runtime_job_delegation_guard(e.id,true);
  RETURN moria.native_serving_accepted_delegation(proof);
END $$;

CREATE FUNCTION moria.runtime_job_session_origin(private_hash text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE s moria.runtime_job_sessions; e moria.runtime_executions; proof jsonb;
BEGIN
  SELECT * INTO s FROM moria.runtime_job_sessions WHERE token_hash=private_hash;
  IF NOT FOUND THEN
    IF to_regprocedure('moria.runtime_indexing_job_session_origin(text)') IS NOT NULL THEN
      RETURN moria.runtime_indexing_job_session_origin(private_hash);
    END IF;
    RETURN NULL;
  END IF;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=s.execution FOR SHARE;
  IF NOT FOUND OR s.expires_at<=clock_timestamp() OR s.expires_at>e.deadline
    OR s.incarnation<>(SELECT incarnation FROM moria.indexing_instance)
    OR e.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  PERFORM moria.runtime_worker_check(s.owner,s.host_id,e.resource_class,e.spec->>'domain');
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_workers w WHERE w.owner=s.owner AND w.generation=s.worker_generation)
    THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  IF s.task IS NOT NULL THEN
    IF e.status<>'running' THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
    PERFORM 1 FROM moria.runtime_execution_tasks t WHERE t.id=s.task AND t.execution=e.id
      AND t.status='running' AND t.owner=s.owner AND t.fence=s.fence AND t.lease_until>clock_timestamp() FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  ELSIF e.status<>'preparing' OR e.owner IS DISTINCT FROM s.owner OR e.fence<>s.fence OR e.lease_until IS NULL OR e.lease_until<=clock_timestamp()
    THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  proof:=moria.runtime_job_delegation_guard(e.id);
  PERFORM moria.runtime_execution_check(e.id);
  RETURN proof;
END $$;

CREATE FUNCTION moria.runtime_job_delegation_accept(execution_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; state jsonb; accepted jsonb; parent_id uuid; depth integer;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  IF NOT FOUND OR e.status<>'queued' OR EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE execution=e.id AND attempts<>0)
    THEN RAISE EXCEPTION 'JOB_ACCEPTANCE_REQUIRED'; END IF;
  IF EXISTS(SELECT 1 FROM moria.runtime_job_delegations WHERE execution=e.id) THEN RETURN; END IF;
  accepted:=moria.runtime_job_acceptance_state(e.id);
  IF accepted IS NULL THEN RETURN; END IF;
  state:=accepted->'state';parent_id:=(accepted->>'parentExecution')::uuid;
  IF parent_id IS NOT NULL THEN
    WITH RECURSIVE chain AS(SELECT execution,parent_execution,1 AS depth FROM moria.runtime_job_delegations WHERE execution=parent_id
      UNION ALL SELECT p.execution,p.parent_execution,c.depth+1 FROM chain c JOIN moria.runtime_job_delegations p ON p.execution=c.parent_execution WHERE c.depth<=8)
      SELECT count(*) INTO depth FROM chain;
    IF depth>=8 THEN RAISE EXCEPTION 'JOB_DELEGATION_DEPTH'; END IF;
  END IF;
  INSERT INTO moria.runtime_job_delegations(execution,environment,principal,application,origin_kind,origin_session_hash,
    service_authority,origin_binding,accepted_grants,accepted_scopes,accepted_markings,parent_execution)
    VALUES(e.id,e.environment,e.principal,e.application,accepted->>'originKind',accepted->>'originSessionHash',
      (accepted->>'serviceAuthority')::uuid,(accepted->>'originBinding')::uuid,
      ARRAY(SELECT jsonb_array_elements_text(state->'identity'->'grantedOperations')),
      ARRAY(SELECT jsonb_array_elements_text(state->'identity'->'applicationScopes')),
      ARRAY(SELECT jsonb_array_elements_text(state->'identity'->'markings')),parent_id);
END $$;

CREATE FUNCTION moria.runtime_job_session_issue(private_hash text,execution_id uuid,task_id uuid,worker uuid,
  worker_fence bigint,host uuid,worker_incarnation uuid) RETURNS text LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; existing_hash text; accepted_generation bigint;
BEGIN
  IF EXISTS(SELECT 1 FROM moria.workload_sessions WHERE token_hash=private_hash
    UNION ALL SELECT 1 FROM moria.browser_sessions WHERE token_hash=private_hash) THEN RAISE EXCEPTION 'JOB_CREDENTIAL_COLLISION'; END IF;
  SELECT token_hash INTO existing_hash FROM moria.runtime_job_sessions WHERE execution=execution_id
    AND task IS NOT DISTINCT FROM task_id AND owner=worker AND fence=worker_fence AND host_id=host AND incarnation=worker_incarnation;
  IF FOUND THEN
    -- Exact credential reuse keeps all current guards without upgrading descendant readers.
    PERFORM moria.runtime_job_session_origin(existing_hash);
    RETURN existing_hash;
  END IF;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  IF NOT FOUND OR e.deadline<=clock_timestamp() OR worker_incarnation<>(SELECT incarnation FROM moria.indexing_instance)
    THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  PERFORM moria.runtime_worker_check(worker,host,e.resource_class,e.spec->>'domain');
  SELECT w.generation INTO accepted_generation FROM moria.runtime_workers w WHERE w.owner=worker FOR SHARE;
  PERFORM moria.runtime_job_delegation_guard(e.id);
  PERFORM moria.runtime_execution_check(e.id);
  IF task_id IS NOT NULL THEN
    IF e.status<>'running' THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
    PERFORM moria.runtime_task_check(task_id,worker,worker_fence);
    IF NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks WHERE id=task_id AND execution=e.id AND worker_generation=accepted_generation)
      THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  ELSIF e.status<>'preparing' OR e.owner IS DISTINCT FROM worker OR e.fence<>worker_fence OR e.lease_until IS NULL OR e.lease_until<=clock_timestamp() OR e.worker_generation IS DISTINCT FROM accepted_generation
    THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  SELECT token_hash INTO existing_hash FROM moria.runtime_job_sessions WHERE execution=e.id
    AND task IS NOT DISTINCT FROM task_id AND owner=worker AND fence=worker_fence AND host_id=host AND incarnation=worker_incarnation;
  IF FOUND THEN RETURN existing_hash; END IF;
  INSERT INTO moria.runtime_job_sessions(token_hash,execution,task,owner,fence,host_id,incarnation,expires_at,worker_generation)
    VALUES(private_hash,e.id,task_id,worker,worker_fence,host,worker_incarnation,e.deadline,accepted_generation);
  RETURN private_hash;
END $$;

CREATE FUNCTION moria.runtime_sandbox_claim_authority(execution_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; t moria.runtime_execution_tasks; state jsonb; metadata jsonb; task_owner uuid; task_fence bigint;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR UPDATE;
  IF e.spec->>'domain' NOT IN ('function','agent') OR NOT EXISTS(SELECT 1 FROM moria.runtime_job_delegations WHERE execution=e.id)
    THEN RETURN; END IF;
  SELECT markings INTO metadata FROM moria.runtime_sandbox_metadata WHERE execution=e.id;
  IF NOT FOUND THEN RETURN; END IF;
  state:=moria.native_serving_job_delegation(e.id);
  PERFORM moria.runtime_binding_authority(e.binding,(state->>'securityEpoch')::bigint,(state->>'instanceEpoch')::bigint,e.id);
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(metadata) label WHERE label->>'environment' IS DISTINCT FROM e.environment
    OR NOT (state->'identity'->'markings' ? (label->>'marking'))) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  SELECT * INTO t FROM moria.runtime_execution_tasks WHERE execution=e.id FOR UPDATE;
  IF NOT FOUND OR NOT ((t.status='pending' AND t.attempts=0) OR (t.status='sealed' AND t.attempts=1))
    THEN RAISE EXCEPTION 'EXECUTION_INACTIVE'; END IF;
  IF t.status='sealed' THEN task_owner:=t.owner;task_fence:=t.fence; END IF;
  IF e.spec->>'domain'='function' THEN
    INSERT INTO moria.runtime_function_epochs(execution,binding,task,owner,fence,incarnation,security_epoch,instance_epoch)
      SELECT e.id,e.binding,t.id,task_owner,task_fence,incarnation,(state->>'securityEpoch')::bigint,(state->>'instanceEpoch')::bigint FROM moria.indexing_instance
      ON CONFLICT(execution) DO UPDATE SET owner=EXCLUDED.owner,fence=EXCLUDED.fence,
        security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch;
  ELSE
    INSERT INTO moria.runtime_agent_epochs(execution,binding,task,owner,fence,incarnation,security_epoch,instance_epoch)
      SELECT e.id,e.binding,t.id,task_owner,task_fence,incarnation,(state->>'securityEpoch')::bigint,(state->>'instanceEpoch')::bigint FROM moria.indexing_instance
      ON CONFLICT(execution) DO UPDATE SET owner=EXCLUDED.owner,fence=EXCLUDED.fence,
        security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch;
  END IF;
END $$;

CREATE FUNCTION moria.runtime_sandbox_service_child_metadata(parent_hash text,target uuid) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE credential moria.runtime_job_sessions; parent moria.runtime_executions; origin moria.runtime_job_delegations;
 metadata record; attempt moria.runtime_sandbox_attempts; selected record; state jsonb; target_state jsonb; consumed jsonb; labels jsonb;
BEGIN
 SELECT * INTO credential FROM moria.runtime_job_sessions WHERE token_hash=parent_hash;
 IF NOT FOUND OR credential.task IS NULL THEN RETURN NULL; END IF;
 SELECT * INTO origin FROM moria.runtime_job_delegations WHERE execution=credential.execution;
 IF NOT FOUND OR origin.origin_kind<>'service' THEN RETURN NULL; END IF;
 state:=moria.native_serving_session(parent_hash);
 SELECT * INTO parent FROM moria.runtime_executions WHERE id=credential.execution;
 IF parent.spec->>'domain' NOT IN ('function','agent') THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
 SELECT * INTO attempt FROM moria.runtime_sandbox_attempts s WHERE s.execution=parent.id AND s.task=credential.task
   AND s.owner=credential.owner AND s.fence=credential.fence AND s.host_id=credential.host_id AND s.state='running'
   AND NOT s.cleaned AND NOT s.observation_blocked FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
 SELECT * INTO metadata FROM moria.runtime_sandbox_metadata WHERE execution=parent.id;
 IF NOT FOUND THEN RAISE EXCEPTION 'FUNCTION_INVALID'; END IF;
 IF NOT(state->'identity'->'grantedOperations' ? (target::text||':invoke'))
   OR NOT(state->'identity'->'applicationScopes' ? (target::text||':invoke')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 IF (SELECT count(*) FROM moria.runtime_service_targets t WHERE t.authority=origin.service_authority AND t.environment=parent.environment
   AND t.resource=target AND t.domain='function' AND t.resource_class='standard' AND t.enabled)<>1 THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
 SELECT * INTO selected FROM moria.runtime_service_targets t WHERE t.authority=origin.service_authority AND t.environment=parent.environment
   AND t.resource=target AND t.domain='function' AND t.resource_class='standard' AND t.enabled FOR SHARE;
 target_state:=moria.runtime_service_target_authority(selected.authority,parent.environment,target,'function',selected.binding_revision,'standard');
 IF ROW(target_state->>'environment',target_state->>'principal',target_state->>'application') IS DISTINCT FROM ROW(state->>'environment',state->>'principal',state->>'application')
   THEN RAISE EXCEPTION 'JOB_ACTOR_MISMATCH'; END IF;
 labels:=moria.native_resource_release_access_state(state,parent.resource::text,'invoke',metadata.release);
 consumed:=moria.runtime_sandbox_markings_union(metadata.markings,attempt.consumed_markings);
 consumed:=moria.runtime_sandbox_markings_union(consumed,(SELECT COALESCE(jsonb_agg(jsonb_build_object('environment',parent.environment,'marking',value)),'[]') FROM jsonb_array_elements_text(labels)));
 RETURN jsonb_build_object('parent',parent.id,'parentDeadline',parent.deadline,'task',credential.task,'owner',credential.owner,'fence',credential.fence::text,
   'incarnation',credential.incarnation,'authority',selected.authority,'revision',selected.binding_revision,'markings',consumed);
END $$;

CREATE FUNCTION moria.runtime_sandbox_service_child_binding_guard(binding_id uuid) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE binding moria.runtime_execution_bindings; child moria.runtime_executions; parent moria.runtime_executions;
 selected moria.runtime_execution_services; task moria.runtime_execution_tasks; metadata record; proof jsonb; state jsonb; labels jsonb;
BEGIN
 SELECT * INTO binding FROM moria.runtime_execution_bindings WHERE id=binding_id;
 IF binding.context->>'serviceParentExecution' IS NULL THEN RETURN; END IF;
 IF binding.spec->>'domain'<>'function' OR binding.spec->>'resourceClass'<>'standard'
   OR binding.context->>'parentExecution' IS DISTINCT FROM binding.context->>'serviceParentExecution' THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
 SELECT * INTO selected FROM moria.runtime_execution_services s WHERE s.binding=binding_id;
 PERFORM moria.runtime_service_binding_guard(binding_id);
 SELECT * INTO child FROM moria.runtime_executions WHERE binding=binding_id;
 IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM moria.runtime_job_delegations WHERE execution=child.id) THEN
   proof:=moria.runtime_sandbox_service_child_metadata(binding.context->>'serviceParentHash',binding.resource);
   IF proof IS NULL OR ROW(proof->>'parent',proof->>'task',proof->>'owner',proof->>'fence',proof->>'incarnation',proof->>'authority',proof->>'revision')
     IS DISTINCT FROM ROW(binding.context->>'serviceParentExecution',binding.context->>'serviceParentTask',binding.context->>'serviceParentOwner',binding.context->>'serviceParentFence',binding.context->>'serviceParentIncarnation',selected.authority::text,selected.target_revision)
     OR NOT COALESCE((SELECT array_agg(value->>'marking') FROM jsonb_array_elements(proof->'markings')),ARRAY[]::text[])<@binding.markings
     THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
 ELSE
   IF NOT EXISTS(SELECT 1 FROM moria.runtime_job_delegations d WHERE d.execution=child.id AND d.parent_execution=(binding.context->>'serviceParentExecution')::uuid)
     THEN RAISE EXCEPTION 'JOB_PARENT_INACTIVE'; END IF;
   state:=moria.native_serving_job_delegation((binding.context->>'serviceParentExecution')::uuid);
   SELECT * INTO parent FROM moria.runtime_executions WHERE id=(binding.context->>'serviceParentExecution')::uuid;
   SELECT * INTO task FROM moria.runtime_execution_tasks t WHERE t.id=(binding.context->>'serviceParentTask')::uuid
     AND t.execution=parent.id AND t.owner=(binding.context->>'serviceParentOwner')::uuid AND t.fence=(binding.context->>'serviceParentFence')::bigint FOR SHARE;
   IF NOT FOUND OR binding.context->>'serviceParentIncarnation'<>(SELECT incarnation::text FROM moria.indexing_instance)
     OR NOT ((task.status='running' AND task.lease_until>clock_timestamp()) OR task.status='sealed')
     OR parent.spec->>'domain' NOT IN ('function','agent')
     OR ROW(parent.environment,parent.principal,parent.application) IS DISTINCT FROM ROW(binding.environment,binding.context->>'principal',binding.context->>'application')
     OR NOT(state->'identity'->'grantedOperations' ? (binding.resource::text||':invoke'))
     OR NOT(state->'identity'->'applicationScopes' ? (binding.resource::text||':invoke'))
     THEN RAISE EXCEPTION 'JOB_PARENT_INACTIVE'; END IF;
   IF task.status='running' THEN
     PERFORM 1 FROM moria.runtime_workers w WHERE w.owner=task.owner AND w.generation=task.worker_generation AND w.enabled AND w.expires_at>clock_timestamp() FOR SHARE;
     IF NOT FOUND THEN RAISE EXCEPTION 'JOB_PARENT_INACTIVE'; END IF;
   END IF;
   IF (SELECT count(*) FROM moria.runtime_service_targets t WHERE t.authority=selected.authority AND t.environment=binding.environment
     AND t.resource=binding.resource AND t.domain='function' AND t.resource_class='standard' AND t.enabled)<>1
     THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
   SELECT * INTO metadata FROM moria.runtime_sandbox_metadata WHERE execution=parent.id;
   labels:=moria.native_resource_release_access_state(state,parent.resource::text,'invoke',metadata.release);
   IF NOT labels<@to_jsonb(binding.markings) THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
 END IF;
END $$;

CREATE FUNCTION moria.runtime_sandbox_service_child_state(execution_id uuid) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE execution moria.runtime_executions; state jsonb; parent_state jsonb; member text;
BEGIN
 SELECT * INTO execution FROM moria.runtime_executions WHERE id=execution_id;
 PERFORM moria.runtime_sandbox_service_child_binding_guard(execution.binding);
 state:=moria.runtime_service_binding_guard(execution.binding);
 parent_state:=moria.native_serving_session(execution.context->>'serviceParentHash');
 FOREACH member IN ARRAY ARRAY['grantedOperations','applicationScopes','markings'] LOOP
   state:=jsonb_set(state,ARRAY['identity',member],COALESCE((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM(
     SELECT jsonb_array_elements_text(state->'identity'->member) AS value INTERSECT SELECT jsonb_array_elements_text(parent_state->'identity'->member)) allowed),'[]'));
 END LOOP;
 RETURN state;
END $$;

CREATE FUNCTION moria.runtime_sandbox_service_child_retained(binding_id uuid,reader_hash text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE binding moria.runtime_execution_bindings; parent moria.runtime_executions; origin moria.runtime_job_delegations; metadata record; state jsonb;
BEGIN
 SELECT * INTO binding FROM moria.runtime_execution_bindings WHERE id=binding_id;
 IF binding.context->>'serviceParentExecution' IS NULL THEN RETURN; END IF;
 state:=moria.native_serving_session(reader_hash);
 IF ROW(state->>'environment',state->>'principal',state->>'application') IS DISTINCT FROM ROW(binding.environment,binding.context->>'principal',binding.context->>'application')
   THEN RAISE EXCEPTION 'JOB_ACTOR_MISMATCH'; END IF;
 PERFORM moria.runtime_service_binding_guard(binding_id);
 SELECT * INTO parent FROM moria.runtime_executions WHERE id=(binding.context->>'serviceParentExecution')::uuid;
 SELECT * INTO origin FROM moria.runtime_job_delegations WHERE execution=parent.id;
 IF origin.origin_kind<>'service' THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
 PERFORM moria.runtime_service_binding_guard(origin.origin_binding);
 SELECT * INTO metadata FROM moria.runtime_sandbox_metadata WHERE execution=parent.id;
 PERFORM moria.native_resource_release_access_state(state,parent.resource::text,'invoke',metadata.release);
END $$;
CREATE FUNCTION moria.runtime_sandbox_service_parent_identity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE binding moria.runtime_execution_bindings;
BEGIN
 SELECT b.* INTO binding FROM moria.runtime_executions e JOIN moria.runtime_execution_bindings b ON b.id=e.binding WHERE e.id=NEW.execution;
 IF binding.context->>'serviceParentExecution' IS NOT NULL THEN
   IF NEW.parent_id IS DISTINCT FROM (binding.context->>'serviceParentExecution')::uuid THEN RAISE EXCEPTION 'JOB_PARENT_INACTIVE'; END IF;
   PERFORM moria.runtime_sandbox_service_child_binding_guard(binding.id);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_sandbox_service_parent BEFORE INSERT ON moria.runtime_function_invocations FOR EACH ROW EXECUTE FUNCTION moria.runtime_sandbox_service_parent_identity();
`;
