/** Accepted indexing authority is pinned to real builds and their own fenced attempts. */
export const indexingJobRuntimeSchema = String.raw`
CREATE FUNCTION moria.native_indexing_service_admit(env text,target_id uuid,source_id uuid,service_id uuid,
  target_revision text,class_name text,output_markings text[]) RETURNS bigint LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; labels jsonb;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/environment/'||env,0));
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0));
  PERFORM 1 FROM moria.environments WHERE id=env FOR UPDATE;
  state:=moria.runtime_service_target_authority(service_id,env,target_id,'indexing',target_revision,class_name);
  IF NOT (state->'identity'->'grantedOperations' ? (source_id::text||':read'))
    OR NOT (state->'identity'->'applicationScopes' ? (source_id::text||':read'))
    OR NOT (state->'identity'->'grantedOperations' ? (target_id::text||':index'))
    OR NOT (state->'identity'->'applicationScopes' ? (target_id::text||':index')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  PERFORM moria.runtime_dataset_kind(env,source_id::text);
  labels:=moria.native_serving_resource_access_state(state,source_id::text,'read');
  IF NOT labels<@to_jsonb(output_markings) OR NOT to_jsonb(output_markings)<@(state->'identity'->'markings')
    THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  labels:=moria.native_serving_resource_access_state(state,target_id::text,'read');
  IF NOT labels<@to_jsonb(output_markings) THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  IF target_id<>source_id THEN
    INSERT INTO moria.resource_data_edges VALUES(env,target_id::text,source_id::text) ON CONFLICT DO NOTHING;
  END IF;
  PERFORM moria.native_serving_resource_access_state(state,target_id::text,'read');
  RETURN (SELECT security_epoch FROM moria.environments WHERE id=env);
END $$;
CREATE TABLE moria.native_indexing_service_definitions (
  binding uuid PRIMARY KEY REFERENCES moria.runtime_execution_bindings(id),
  environment text NOT NULL REFERENCES moria.environments(id),target text NOT NULL,target_id uuid NOT NULL,source text NOT NULL,
  definition jsonb NOT NULL,canonical_definition text NOT NULL CHECK(octet_length(canonical_definition) BETWEEN 1 AND 1048576),
  definition_hash text NOT NULL CHECK(definition_hash ~ '^[0-9a-f]{64}$')
);
CREATE FUNCTION moria.native_indexing_service_definition_validate() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE metadata moria.runtime_execution_bindings; state jsonb; payload jsonb;
BEGIN
  SELECT * INTO metadata FROM moria.runtime_execution_bindings WHERE id=NEW.binding;
  state:=moria.runtime_service_binding_guard(NEW.binding);
  payload:=jsonb_build_object('version',1,'environment',NEW.environment,'target',NEW.target,'targetId',NEW.target_id,
    'source',NEW.source,'definition',NEW.definition);
  IF NEW.canonical_definition::jsonb IS DISTINCT FROM payload
    OR encode(sha256(convert_to(NEW.canonical_definition,'UTF8')),'hex')<>NEW.definition_hash
    OR metadata.environment<>NEW.environment OR metadata.resource<>NEW.target_id OR metadata.spec->>'domain'<>'indexing'
    OR metadata.spec->>'release'<>NEW.definition_hash OR metadata.spec->'pinnedInputs'<>'[]'::jsonb OR metadata.spec->'expectedHeads'<>'{}'::jsonb
    OR metadata.markings IS DISTINCT FROM ARRAY(SELECT jsonb_array_elements_text(NEW.definition->'markings'))
    OR NOT ARRAY[NEW.source||':read',NEW.target||':index']<@metadata.operations
    OR NOT metadata.operations<@ARRAY[NEW.source||':read',NEW.target||':index','filesystem:read']
    OR NEW.definition->>'targetId' IS DISTINCT FROM NEW.target_id::text
    OR NOT (EXISTS(SELECT 1 FROM moria.indexing_bindings x WHERE x.environment=NEW.environment AND x.target=NEW.target
      AND x.target_id=NEW.target_id AND x.source=NEW.source AND x.definition=NEW.definition AND x.enabled)
      OR EXISTS(SELECT 1 FROM moria.native_serving_bindings x WHERE x.environment=NEW.environment AND x.resource=NEW.target
        AND x.object_type_id=NEW.target_id AND x.enabled AND x.definition->'schema'=NEW.definition->'schema')) THEN
    RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_indexing_service_definition_validate BEFORE INSERT ON moria.native_indexing_service_definitions
  FOR EACH ROW EXECUTE FUNCTION moria.native_indexing_service_definition_validate();
CREATE TRIGGER native_indexing_service_definition_immutable BEFORE UPDATE OR DELETE ON moria.native_indexing_service_definitions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_immutable();
CREATE FUNCTION moria.runtime_indexing_service_revision(definition_hash text,metadata uuid,security_epoch bigint,instance_epoch bigint)
  RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF definition_hash !~ '^[0-9a-f]{64}$' OR security_epoch<0 OR instance_epoch<0 THEN RAISE EXCEPTION 'INDEXING_INVALID'; END IF;
  RETURN 'sha256:'||encode(sha256(convert_to('{"definitionHash":"'||definition_hash||'","instanceEpoch":"'||instance_epoch::text
    ||'","metadataBinding":"'||metadata::text||'","securityEpoch":"'||security_epoch::text||'"}','UTF8')),'hex');
END $$;
-- Re-admit a permanent purpose for a NEW build. Existing bindings, accepted proofs and attempts stay immutable.
CREATE FUNCTION moria.runtime_indexing_service_refresh(env text,target_resource text) RETURNS moria.indexing_bindings
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE x moria.indexing_bindings; d moria.native_indexing_service_definitions; old_metadata moria.runtime_execution_bindings;
  state jsonb; service moria.runtime_execution_services; metadata uuid; new_context jsonb; epoch bigint; current_instance_epoch bigint;
  new_revision text; expected_projection text; namespace jsonb; labels jsonb;
BEGIN
  SELECT * INTO x FROM moria.indexing_bindings WHERE environment=env AND target=target_resource AND enabled FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  IF NOT (x.context ? 'executionBinding') AND NOT (x.context ? 'serviceAuthority') THEN RETURN x; END IF;
  IF x.context ? 'sessionHash' OR x.context->>'executionBinding' IS NULL OR x.context->>'serviceAuthority' IS NULL THEN
    RAISE EXCEPTION 'INDEXING_ACCEPTANCE_REQUIRED'; END IF;
  SELECT * INTO d FROM moria.native_indexing_service_definitions WHERE binding=(x.context->>'executionBinding')::uuid;
  IF NOT FOUND OR ROW(d.environment,d.target,d.target_id,d.source,d.definition)
    IS DISTINCT FROM ROW(x.environment,x.target,x.target_id,x.source,x.definition) THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  SELECT * INTO old_metadata FROM moria.runtime_execution_bindings WHERE id=d.binding FOR SHARE;
  SELECT * INTO service FROM moria.runtime_execution_services WHERE binding=d.binding;
  IF NOT FOUND OR service.authority::text IS DISTINCT FROM x.context->>'serviceAuthority'
    OR service.target_revision IS DISTINCT FROM x.context->>'serviceTargetRevision' THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  state:=moria.runtime_service_binding_guard(d.binding);
  IF ROW(state->>'environment',state->>'principal',state->>'application')
    IS DISTINCT FROM ROW(x.environment,x.context->>'principal',x.context->>'application') THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.indexing_instance i,moria.indexing_environments e
    WHERE i.incarnation::text=x.definition->>'incarnation' AND e.environment=env AND e.id::text=x.definition->>'environmentId')
    OR x.definition->'servingDefinition' IS NULL OR x.definition->>'servingBindingRevision' IS NULL
    OR NOT EXISTS(SELECT 1 FROM moria.native_serving_bindings b WHERE b.environment=env AND b.resource=x.target
      AND b.object_type_id=x.target_id AND b.enabled AND b.revision=x.definition->>'servingBindingRevision'
      AND b.authority_generation::text=x.definition->>'servingBindingGeneration'
      AND b.definition=x.definition->'servingDefinition')
    OR NOT EXISTS(SELECT 1 FROM moria.native_dataset_bindings b WHERE b.environment=env AND b.resource=x.source::uuid AND b.enabled
      AND b.columns=x.definition->'sourceColumns') THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  SELECT revision INTO expected_projection FROM moria.runtime_projection_bindings WHERE environment=env AND resource=x.target AND enabled;
  IF expected_projection IS DISTINCT FROM x.definition->>'projectionRevision' THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  IF COALESCE(x.definition->'linkConstraints','[]'::jsonb) IS DISTINCT FROM moria.runtime_link_constraints(env,x.target) THEN
    RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  namespace:=x.definition->'artifactNamespace';
  IF namespace IS NOT NULL AND NOT EXISTS(SELECT 1 FROM moria.native_storage_namespaces n WHERE n.environment=env AND n.resource=x.target
    AND n.enabled AND n.purpose='index-artifact' AND n.warehouse_id=namespace->>'warehouseId' AND n.table_id=namespace->>'tableId'
    AND n.revision=namespace->>'revision' AND to_jsonb(ARRAY(SELECT unnest(n.markings) ORDER BY 1))=x.definition->'markings') THEN
    RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  PERFORM moria.runtime_data_assert(env,x.target,x.source);
  -- A newly classified source requires a new explicit output floor, never implicit promotion.
  -- This is stale indexing metadata, distinct from a producer's own publication downgrade.
  labels:=moria.native_serving_resource_access_state(state,x.source,'read');
  IF NOT labels<@to_jsonb(old_metadata.markings) THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  labels:=moria.native_serving_resource_access_state(state,x.target,'read');
  IF NOT labels<@to_jsonb(old_metadata.markings) THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
  epoch:=moria.native_indexing_service_admit(env,x.target_id,x.source::uuid,service.authority,service.target_revision,
    old_metadata.spec->>'resourceClass',old_metadata.markings);
  state:=moria.runtime_service_binding_guard(d.binding);
  current_instance_epoch:=(state->>'instanceEpoch')::bigint;
  IF epoch=x.security_epoch AND current_instance_epoch=x.instance_epoch THEN RETURN x; END IF;
  new_context:=jsonb_build_object('environment',env,'principal',state->>'principal','application',state->>'application',
    'serviceAuthority',service.authority,'serviceTargetRevision',service.target_revision,
    'requester',jsonb_build_object('kind','registeredService','authority',service.authority));
  INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation)
    SELECT env,x.target_id,old_metadata.spec,new_context,old_metadata.operations,old_metadata.markings,epoch,current_instance_epoch,i.incarnation
      FROM moria.indexing_instance i RETURNING id INTO metadata;
  INSERT INTO moria.runtime_execution_services(binding,authority,target_revision) VALUES(metadata,service.authority,service.target_revision);
  INSERT INTO moria.native_indexing_service_definitions VALUES(metadata,d.environment,d.target,d.target_id,d.source,d.definition,d.canonical_definition,d.definition_hash);
  new_context:=jsonb_build_object('environment',env,'principal',state->>'principal','application',state->>'application',
    'executionBinding',metadata,'serviceAuthority',service.authority,'serviceTargetRevision',service.target_revision);
  new_revision:=moria.runtime_indexing_service_revision(d.definition_hash,metadata,epoch,current_instance_epoch);
  UPDATE moria.indexing_bindings SET revision=new_revision,context=new_context,security_epoch=epoch,instance_epoch=current_instance_epoch
    WHERE environment=env AND target=x.target RETURNING * INTO x;
  UPDATE moria.indexing_builds SET status='superseded' WHERE environment=env AND target=x.target AND binding_revision<>new_revision
    AND status IN ('pending','planning','running','preparing','ready');
  RETURN x;
END $$;
CREATE TABLE moria.runtime_indexing_delegations (
  build uuid PRIMARY KEY REFERENCES moria.indexing_builds(id), origin_session_hash text,
  build_state jsonb NOT NULL, proof jsonb NOT NULL,
  CHECK((origin_session_hash IS NULL)=(COALESCE(proof->>'originKind','session')='service'))
);
CREATE TABLE moria.runtime_indexing_job_sessions (
  token_hash text PRIMARY KEY CHECK(token_hash ~ '^[0-9a-f]{64}$'),
  build uuid NOT NULL REFERENCES moria.runtime_indexing_delegations(build),
  task uuid REFERENCES moria.indexing_tasks(id), owner uuid NOT NULL, fence bigint NOT NULL CHECK(fence>0),
  incarnation uuid NOT NULL, expires_at timestamptz NOT NULL,
  UNIQUE NULLS NOT DISTINCT(build,task,owner,fence,incarnation)
);
CREATE FUNCTION moria.runtime_indexing_build_state(b moria.indexing_builds) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT (to_jsonb(b)-ARRAY['status','owner','fence','lease_until','error','receipt','planned','created_at','deadline'])
    ||jsonb_build_object('created_at',extract(epoch FROM b.created_at)::text,'deadline',extract(epoch FROM b.deadline)::text)
$$;
CREATE FUNCTION moria.runtime_indexing_origin_state(b moria.indexing_builds) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE state jsonb; operations text[];
BEGIN
  IF b.context ? 'executionBinding' OR b.context ? 'serviceAuthority' THEN
    IF b.context ? 'sessionHash' OR b.context->>'executionBinding' IS NULL OR b.context->>'serviceAuthority' IS NULL THEN
      RAISE EXCEPTION 'INDEXING_ACCEPTANCE_REQUIRED'; END IF;
    state:=moria.runtime_service_binding_guard((b.context->>'executionBinding')::uuid);
    IF NOT EXISTS(SELECT 1 FROM moria.native_indexing_service_definitions d
      JOIN moria.runtime_execution_services s ON s.binding=d.binding
      WHERE d.binding=(b.context->>'executionBinding')::uuid AND s.authority::text=b.context->>'serviceAuthority'
        AND s.target_revision=b.context->>'serviceTargetRevision'
        AND d.environment=b.environment AND d.target=b.target AND d.target_id::text=b.definition->>'targetId'
        AND d.source=b.source AND d.definition=b.definition) THEN RAISE EXCEPTION 'INDEXING_AUTHORITY_CHANGED'; END IF;
    IF left(b.metadata_location,18)<>'moria-snapshot-v2:' THEN RAISE EXCEPTION 'UNSUPPORTED_DATASET_BINDING'; END IF;
  ELSE
    IF b.context->>'sessionHash' IS NULL THEN RAISE EXCEPTION 'INDEXING_ACCEPTANCE_REQUIRED'; END IF;
    state:=moria.native_serving_session_origin(b.context->>'sessionHash',false);
  END IF;
  operations:=ARRAY[b.source||':read',b.target||':index'];
  IF ROW(state->>'environment',state->>'principal',state->>'application',state->>'securityEpoch',state->>'instanceEpoch')
    IS DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application',b.security_epoch::text,b.instance_epoch::text)
    OR NOT to_jsonb(operations)<@(state->'identity'->'grantedOperations') OR NOT to_jsonb(operations)<@(state->'identity'->'applicationScopes')
    OR NOT b.definition->'markings'<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  PERFORM moria.runtime_dataset_kind(b.environment,b.source);
  IF NOT moria.runtime_data_labels(b.environment,b.source)<@ARRAY(SELECT jsonb_array_elements_text(b.definition->'markings')) THEN
    RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  RETURN state;
END $$;
CREATE FUNCTION moria.runtime_indexing_expected_proof(b moria.indexing_builds,state jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE proof jsonb; operations text[];
BEGIN
  operations:=ARRAY[b.source||':read',b.target||':index'];
  proof:=jsonb_build_object('execution',b.id,
    'purpose','indexing','allowedSources',jsonb_build_array(moria.runtime_data_resource(b.environment,b.source)),
    'indexTarget',moria.runtime_data_resource(b.environment,b.target),
    'environment',b.environment,'principal',b.context->>'principal','application',b.context->>'application',
    'acceptedGrants',to_jsonb(ARRAY(SELECT operation FROM unnest(operations||ARRAY['filesystem:read']) operation WHERE state->'identity'->'grantedOperations' ? operation ORDER BY operation)),
    'acceptedScopes',to_jsonb(ARRAY(SELECT operation FROM unnest(operations||ARRAY['filesystem:read']) operation WHERE state->'identity'->'applicationScopes' ? operation ORDER BY operation)),
    'acceptedMarkings',b.definition->'markings');
  IF b.context->>'executionBinding' IS NOT NULL THEN
    RETURN proof||jsonb_build_object('originKind','service','originBinding',b.context->>'executionBinding','serviceAuthority',b.context->>'serviceAuthority');
  END IF;
  RETURN proof||jsonb_build_object('originSessionHash',b.context->>'sessionHash');
END $$;
CREATE FUNCTION moria.runtime_indexing_accept() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE state jsonb; proof jsonb; operations text[];
BEGIN
  IF NEW.context->>'sessionHash' IS NULL AND NOT (NEW.context ? 'executionBinding') AND NOT (NEW.context ? 'serviceAuthority') THEN RETURN NEW; END IF;
  state:=moria.runtime_indexing_origin_state(NEW);
  IF ROW(state->>'environment',state->>'principal',state->>'application')
    IS DISTINCT FROM ROW(NEW.environment,NEW.context->>'principal',NEW.context->>'application')
    OR state->>'securityEpoch' IS DISTINCT FROM NEW.security_epoch::text
    OR state->>'instanceEpoch' IS DISTINCT FROM NEW.instance_epoch::text THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  operations:=ARRAY[NEW.source||':read',NEW.target||':index'];
  PERFORM moria.runtime_dataset_kind(NEW.environment,NEW.source);
  IF NOT moria.runtime_data_labels(NEW.environment,NEW.source)<@ARRAY(SELECT jsonb_array_elements_text(NEW.definition->'markings')) THEN
    RAISE EXCEPTION 'AUTHORIZATION_CHANGED';
  END IF;
  IF EXISTS(SELECT 1 FROM unnest(operations) operation WHERE NOT (state->'identity'->'grantedOperations' ? operation)
    OR NOT (state->'identity'->'applicationScopes' ? operation))
    OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(NEW.definition->'markings') label
      WHERE NOT (state->'identity'->'markings' ? label)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  proof:=moria.runtime_indexing_expected_proof(NEW,state);
  INSERT INTO moria.runtime_indexing_delegations VALUES(NEW.id,NEW.context->>'sessionHash',moria.runtime_indexing_build_state(NEW),proof);
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_indexing_accept AFTER INSERT ON moria.indexing_builds
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_accept();
CREATE FUNCTION moria.runtime_indexing_acceptance_validate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE b moria.indexing_builds; state jsonb; expected jsonb; operations text[];
BEGIN
  SELECT * INTO b FROM moria.indexing_builds WHERE id=NEW.build;
  IF NOT FOUND OR b.status<>'pending' OR b.fence<>0 OR b.owner IS NOT NULL
    OR NEW.origin_session_hash IS DISTINCT FROM b.context->>'sessionHash'
    OR NEW.build_state IS DISTINCT FROM moria.runtime_indexing_build_state(b) THEN RAISE EXCEPTION 'INDEXING_ACCEPTANCE_REQUIRED'; END IF;
  state:=moria.runtime_indexing_origin_state(b);
  IF ROW(state->>'environment',state->>'principal',state->>'application')
    IS DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  operations:=ARRAY[b.source||':read',b.target||':index'];
  expected:=moria.runtime_indexing_expected_proof(b,state);
  IF NEW.proof IS DISTINCT FROM expected THEN RAISE EXCEPTION 'JOB_CAPABILITY_MISMATCH'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_indexing_acceptance_valid BEFORE INSERT ON moria.runtime_indexing_delegations
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_acceptance_validate();
CREATE TRIGGER runtime_indexing_delegation_immutable BEFORE UPDATE OR DELETE ON moria.runtime_indexing_delegations
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_immutable();
CREATE TRIGGER runtime_indexing_session_immutable BEFORE UPDATE OR DELETE ON moria.runtime_indexing_job_sessions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_immutable();
CREATE TRIGGER indexing_blocked_intent_immutable BEFORE UPDATE OR DELETE ON moria.indexing_blocked_intents
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_immutable();
CREATE FUNCTION moria.runtime_indexing_delegation_guard(build_id uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE d moria.runtime_indexing_delegations; b moria.indexing_builds; state jsonb;
BEGIN
  SELECT * INTO d FROM moria.runtime_indexing_delegations WHERE build=build_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'INDEXING_ACCEPTANCE_REQUIRED'; END IF;
  SELECT * INTO b FROM moria.indexing_builds WHERE id=build_id;
  IF NOT FOUND OR d.build_state IS DISTINCT FROM moria.runtime_indexing_build_state(b)
    OR b.status IN ('failed','cancelled','superseded','conflicted') OR (b.status<>'active' AND b.deadline<=clock_timestamp())
    OR NOT EXISTS(SELECT 1 FROM moria.indexing_bindings x,moria.indexing_instance i,moria.indexing_environments e
      WHERE x.environment=b.environment AND x.target=b.target AND x.enabled AND x.revision=b.binding_revision
        AND x.definition=b.definition AND x.context=b.context AND i.incarnation::text=b.definition->>'incarnation'
        AND e.environment=b.environment AND e.id::text=b.definition->>'environmentId') THEN RAISE EXCEPTION 'INDEXING_DELEGATION_INACTIVE'; END IF;
  state:=moria.native_serving_accepted_delegation(d.proof);
  IF state->>'securityEpoch' IS DISTINCT FROM b.security_epoch::text
    OR state->>'instanceEpoch' IS DISTINCT FROM b.instance_epoch::text
    OR NOT (state->'identity'->'grantedOperations' ? (b.source||':read'))
    OR NOT (state->'identity'->'applicationScopes' ? (b.source||':read'))
    OR NOT (state->'identity'->'grantedOperations' ? (b.target||':index'))
    OR NOT (state->'identity'->'applicationScopes' ? (b.target||':index'))
    OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(b.definition->'markings') label WHERE NOT (state->'identity'->'markings' ? label))
    THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  RETURN d.proof;
END $$;
CREATE FUNCTION moria.runtime_indexing_job_session_origin(private_hash text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE s moria.runtime_indexing_job_sessions; b moria.indexing_builds;
BEGIN
  SELECT * INTO s FROM moria.runtime_indexing_job_sessions WHERE token_hash=private_hash;
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT * INTO b FROM moria.indexing_builds WHERE id=s.build;
  IF NOT FOUND OR s.expires_at<=clock_timestamp() OR s.expires_at>b.deadline
    OR s.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  IF s.task IS NULL THEN
    IF b.owner IS DISTINCT FROM s.owner OR b.fence<>s.fence OR b.lease_until IS NULL OR b.lease_until<=clock_timestamp()
      OR b.status NOT IN ('planning','running','preparing','ready') THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  ELSIF NOT EXISTS(SELECT 1 FROM moria.indexing_tasks t WHERE t.id=s.task AND t.build=s.build AND t.status='running'
    AND t.owner=s.owner AND t.fence=s.fence AND t.lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.indexing_attempts a WHERE a.build=s.build AND a.unit=COALESCE(s.task,s.build)
    AND a.kind=CASE WHEN s.task IS NULL THEN 'coordinator' ELSE 'task' END AND a.owner=s.owner AND a.fence=s.fence
    AND a.outcome='running' AND a.finished_at IS NULL) THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  RETURN moria.runtime_indexing_delegation_guard(s.build);
END $$;
CREATE FUNCTION moria.runtime_indexing_job_session_issue() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM moria.workload_sessions WHERE token_hash=NEW.token_hash)
    OR EXISTS(SELECT 1 FROM moria.browser_sessions WHERE token_hash=NEW.token_hash)
    OR EXISTS(SELECT 1 FROM moria.runtime_job_sessions WHERE token_hash=NEW.token_hash) THEN RAISE EXCEPTION 'JOB_TOKEN_COLLISION'; END IF;
  -- The inserted row is visible to AFTER validation; no unvalidated private hash can resolve.
  PERFORM moria.runtime_indexing_job_session_origin(NEW.token_hash);
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_indexing_job_session_issue AFTER INSERT ON moria.runtime_indexing_job_sessions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_job_session_issue();
CREATE FUNCTION moria.runtime_indexing_session_issue(build_id uuid,task_id uuid,worker uuid,worker_fence bigint,new_hash text)
  RETURNS text LANGUAGE plpgsql AS $$
DECLARE result text;
BEGIN
  PERFORM moria.indexing_claim_check(build_id,task_id,worker,worker_fence);
  INSERT INTO moria.runtime_indexing_job_sessions(token_hash,build,task,owner,fence,incarnation,expires_at)
    SELECT new_hash,b.id,task_id,worker,worker_fence,i.incarnation,b.deadline FROM moria.indexing_builds b,moria.indexing_instance i
      WHERE b.id=build_id ON CONFLICT(build,task,owner,fence,incarnation) DO NOTHING;
  SELECT token_hash INTO result FROM moria.runtime_indexing_job_sessions
    WHERE build=build_id AND task IS NOT DISTINCT FROM task_id AND owner=worker AND fence=worker_fence
      AND incarnation=(SELECT incarnation FROM moria.indexing_instance);
  IF result IS NULL THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  PERFORM moria.runtime_indexing_job_session_origin(result);
  RETURN result;
END $$;
CREATE FUNCTION moria.runtime_indexing_retry_authority(build_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE b moria.indexing_builds; d moria.runtime_indexing_delegations; state jsonb; operations text[];
BEGIN
  SELECT * INTO b FROM moria.indexing_builds WHERE id=build_id FOR UPDATE;
  IF NOT FOUND OR b.status<>'failed' OR b.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'BUILD_INACTIVE'; END IF;
  SELECT * INTO d FROM moria.runtime_indexing_delegations WHERE build=build_id;
  IF NOT FOUND OR d.build_state IS DISTINCT FROM moria.runtime_indexing_build_state(b) THEN RAISE EXCEPTION 'INDEXING_ACCEPTANCE_REQUIRED'; END IF;
  state:=moria.runtime_indexing_origin_state(b);
  operations:=ARRAY[b.source||':read',b.target||':index'];
  IF ROW(state->>'environment',state->>'principal',state->>'application',state->>'securityEpoch',state->>'instanceEpoch')
    IS DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application',b.security_epoch::text,b.instance_epoch::text)
    OR NOT to_jsonb(operations)<@(state->'identity'->'grantedOperations')
    OR NOT to_jsonb(operations)<@(state->'identity'->'applicationScopes')
    OR NOT (b.definition->'markings')<@(state->'identity'->'markings')
    OR NOT to_jsonb(operations)<@(d.proof->'acceptedGrants') OR NOT to_jsonb(operations)<@(d.proof->'acceptedScopes')
    OR NOT EXISTS(SELECT 1 FROM moria.indexing_bindings x,moria.indexing_instance i,moria.indexing_environments env
      WHERE x.environment=b.environment AND x.target=b.target AND x.enabled AND x.revision=b.binding_revision
        AND x.definition=b.definition AND x.context=b.context AND i.incarnation::text=b.definition->>'incarnation'
        AND env.environment=b.environment AND env.id::text=b.definition->>'environmentId') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  PERFORM moria.runtime_dataset_kind(b.environment,b.source);
  IF NOT moria.runtime_data_labels(b.environment,b.source)<@ARRAY(SELECT jsonb_array_elements_text(b.definition->'markings')) THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
END $$;
`;
