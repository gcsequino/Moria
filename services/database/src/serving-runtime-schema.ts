/** Trusted serving definitions and the current transactional security authority. */
export const servingRuntimeSchema = String.raw`
CREATE TABLE moria.native_serving_bindings (
  environment text NOT NULL REFERENCES moria.environments(id), resource text NOT NULL,
  object_type_id uuid, revision text NOT NULL, definition jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT true,authority_generation bigint NOT NULL DEFAULT 0 CHECK(authority_generation>=0),
  PRIMARY KEY(environment,resource), UNIQUE(environment,object_type_id)
);
CREATE TABLE moria.native_serving_actions (
  environment text NOT NULL, resource text NOT NULL, object_resource text NOT NULL,
  revision text NOT NULL, definition jsonb NOT NULL, enabled boolean NOT NULL DEFAULT true,authority_generation bigint NOT NULL DEFAULT 0 CHECK(authority_generation>=0),
  PRIMARY KEY(environment,resource),
  FOREIGN KEY(environment,object_resource) REFERENCES moria.native_serving_bindings(environment,resource)
);
CREATE TABLE moria.native_serving_audit (
  environment text NOT NULL REFERENCES moria.environments(id), revision bigint NOT NULL,
  application text NOT NULL, principal text NOT NULL, action text NOT NULL, key text NOT NULL,
  targets uuid[] NOT NULL CHECK(cardinality(targets) BETWEEN 1 AND 100),
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(environment,revision)
);
CREATE TABLE moria.native_serving_receipt_inputs (
  environment text NOT NULL, application text NOT NULL, principal text NOT NULL, action text NOT NULL, key text NOT NULL,
  input_types jsonb NOT NULL, object_resource text NOT NULL,
  object_resources text[] NOT NULL DEFAULT '{}',
  PRIMARY KEY(environment,application,principal,action,key),
  FOREIGN KEY(environment,application,principal,action,key) REFERENCES moria.action_keys(environment,application,principal,action,key)
);
CREATE TABLE moria.native_serving_links (
  environment text NOT NULL, resource text NOT NULL, revision text NOT NULL,
  source_resource text NOT NULL, target_resource text NOT NULL, definition jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT true,authority_generation bigint NOT NULL DEFAULT 0 CHECK(authority_generation>=0), PRIMARY KEY(environment,resource),
  FOREIGN KEY(environment,source_resource) REFERENCES moria.native_serving_bindings(environment,resource),
  FOREIGN KEY(environment,target_resource) REFERENCES moria.native_serving_bindings(environment,resource)
);
CREATE FUNCTION moria.native_serving_binding_generation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_BINDING_GENERATION'; END IF;
  IF NEW.authority_generation<>OLD.authority_generation THEN RAISE EXCEPTION 'IMMUTABLE_BINDING_GENERATION'; END IF;
  IF ROW(NEW.definition,NEW.enabled,NEW.revision) IS DISTINCT FROM ROW(OLD.definition,OLD.enabled,OLD.revision) THEN
    NEW.authority_generation:=OLD.authority_generation+1;
    UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_serving_binding_generation BEFORE UPDATE OR DELETE ON moria.native_serving_bindings FOR EACH ROW EXECUTE FUNCTION moria.native_serving_binding_generation();
CREATE TRIGGER native_serving_action_generation BEFORE UPDATE OR DELETE ON moria.native_serving_actions FOR EACH ROW EXECUTE FUNCTION moria.native_serving_binding_generation();
CREATE TRIGGER native_serving_link_generation BEFORE UPDATE OR DELETE ON moria.native_serving_links FOR EACH ROW EXECUTE FUNCTION moria.native_serving_binding_generation();
CREATE FUNCTION moria.native_serving_check_budget(observed bigint, allowed bigint) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  IF allowed<1 OR observed>allowed THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  RETURN true;
END $$;

CREATE TABLE moria.native_function_reads (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id), attempt uuid,
  call_key text NOT NULL CHECK(length(call_key) BETWEEN 1 AND 512),
  evidence jsonb NOT NULL CHECK(jsonb_typeof(evidence)='array'),
  markings jsonb NOT NULL CHECK(jsonb_typeof(markings)='array'), fingerprint text NOT NULL,
  PRIMARY KEY(execution,call_key)
);
CREATE TABLE moria.native_function_observation_blocks (
 execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE moria.native_function_failed_actions (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id),attempt uuid,
  call_key text NOT NULL CHECK(length(call_key) BETWEEN 1 AND 512),
  evidence jsonb NOT NULL CHECK(jsonb_typeof(evidence)='array' AND octet_length(evidence::text)<=1048576),
  markings jsonb NOT NULL CHECK(jsonb_typeof(markings)='array'),fingerprint text NOT NULL,
  PRIMARY KEY(execution,call_key)
);
CREATE TABLE moria.native_function_proposals (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id), attempt uuid NOT NULL,
  ordinal bigint GENERATED ALWAYS AS IDENTITY, key text NOT NULL,
  action text NOT NULL, input jsonb NOT NULL, binding_hash text NOT NULL, input_hash text NOT NULL,
  prepared_execution uuid REFERENCES moria.runtime_executions(id),prepared_hash text,
  CHECK((prepared_execution IS NULL)=(prepared_hash IS NULL)),CHECK(prepared_hash IS NULL OR prepared_hash ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY(execution,key), UNIQUE(execution,ordinal)
);
CREATE TABLE moria.native_function_dependencies (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id),
  child uuid NOT NULL REFERENCES moria.runtime_executions(id),
  PRIMARY KEY(execution,child), CHECK(execution<>child)
);
CREATE TABLE moria.native_serving_prepared_actions (
  environment text NOT NULL, application text NOT NULL, principal text NOT NULL, action text NOT NULL, key text NOT NULL,
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id),
  PRIMARY KEY(environment,application,principal,action,key),
  FOREIGN KEY(environment,application,principal,action,key) REFERENCES moria.action_keys(environment,application,principal,action,key)
);
CREATE TABLE moria.native_function_calls (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id), call_id text NOT NULL CHECK(length(call_id) BETWEEN 1 AND 128),
  attempt uuid NOT NULL, method text NOT NULL CHECK(method IN ('GET','POST')), path text NOT NULL CHECK(length(path)<=4096),
  request_hash text NOT NULL, action text, action_key text,
  status integer NOT NULL CHECK(status BETWEEN 100 AND 599), error text, response_hash text NOT NULL,
  markings jsonb NOT NULL CHECK(jsonb_typeof(markings)='array'), created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(execution,call_id)
);
CREATE INDEX native_function_action_trace ON moria.native_function_calls(action,action_key) WHERE action_key IS NOT NULL;

CREATE FUNCTION moria.native_serving_session_origin(token_hash text, ignore_expiry boolean DEFAULT false) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE s record; p record; result jsonb;
BEGIN
  PERFORM moria.runtime_cli_session_check(token_hash);
  IF token_hash !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  SELECT environment,principal,application,false AS browser,NULL::text AS provider,
    NULL::text AS provider_revision,NULL::timestamptz AS verified_at INTO s
    FROM moria.workload_sessions w WHERE w.token_hash=native_serving_session_origin.token_hash
      AND NOT disabled AND (ignore_expiry OR expires_at>clock_timestamp());
  IF NOT FOUND THEN
    SELECT environment,principal,application,true AS browser,provider,revision AS provider_revision,verified_at INTO s
      FROM moria.browser_sessions b WHERE b.token_hash=native_serving_session_origin.token_hash
        AND NOT disabled AND (ignore_expiry OR expires_at>clock_timestamp());
    IF NOT FOUND THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  END IF;
  PERFORM moria.runtime_environment_cell_check(s.environment);
  SELECT e.security_epoch,x.enabled,x.kind,x.synced_at,x.attributes,x.markings,
    a.enabled AS app_enabled,a.operations AS scopes,COALESCE(g.operations,'{}') AS grants,
    b.instance_id,(b.instance_id IS NULL OR (i.enabled AND i.kind=x.kind)) AS instance_enabled INTO p
    FROM moria.environments e JOIN moria.principals x ON x.environment=e.id AND x.principal=s.principal
    JOIN moria.application_scopes a ON a.environment=e.id AND a.application=s.application
    LEFT JOIN moria.permissions g ON g.environment=e.id AND g.principal=x.principal AND g.application=a.application
    LEFT JOIN moria.instance_principal_bindings b ON b.environment=x.environment AND b.principal=x.principal
    LEFT JOIN moria.instance_principals i ON i.id=b.instance_id
    WHERE e.id=s.environment;
  IF NOT FOUND OR NOT p.enabled OR NOT p.app_enabled OR NOT p.instance_enabled THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  IF p.kind='human' AND p.synced_at<=clock_timestamp()-interval '60 seconds' THEN RAISE EXCEPTION 'IDENTITY_STALE'; END IF;
  IF NOT s.browser AND p.kind<>'workload' THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  IF s.browser THEN
    IF s.provider IS NULL THEN
      IF p.kind<>'workload' THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
    ELSIF p.kind<>'human' OR s.verified_at<=clock_timestamp()-interval '60 seconds'
      OR NOT EXISTS(SELECT 1 FROM moria.auth_providers a WHERE a.environment=s.environment
        AND a.application=s.application AND a.provider=s.provider AND a.enabled AND a.revision=s.provider_revision) THEN
      RAISE EXCEPTION 'UNAUTHENTICATED';
    END IF;
  END IF;
  result:=jsonb_build_object('environment',s.environment,'principal',s.principal,'application',s.application,
    'securityEpoch',p.security_epoch::text,'instanceEpoch',(SELECT security_epoch::text FROM moria.instance_state),
    'instanceId',p.instance_id,'attributes',p.attributes,
    'identity',jsonb_build_object('environment',s.environment,'principal',s.principal,'application',s.application,
      'grantedOperations',to_jsonb(p.grants),'applicationScopes',to_jsonb(p.scopes),'markings',to_jsonb(p.markings)));
  RETURN result;
END $$;

-- Ordinary credentials and private accepted-job credentials share current identity checks.
-- Only the accepted immutable delegation path may ask the origin helper to ignore TTL.
CREATE FUNCTION moria.native_serving_service_authority(authority_id uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE registered record; p record; grants text[]; scopes text[]; labels text[];
BEGIN
  SELECT * INTO registered FROM moria.runtime_service_authorities a
    WHERE a.id=authority_id AND a.enabled AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance) FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  PERFORM moria.runtime_environment_cell_check(registered.environment);
  SELECT e.security_epoch,x.enabled,x.kind,x.attributes,x.markings,
    a.enabled AS app_enabled,a.operations AS scopes,COALESCE(g.operations,'{}') AS grants,
    b.instance_id,(b.instance_id IS NULL OR (i.enabled AND i.kind=x.kind)) AS instance_enabled INTO p
    FROM moria.environments e JOIN moria.principals x ON x.environment=e.id AND x.principal=registered.principal
    JOIN moria.application_scopes a ON a.environment=e.id AND a.application=registered.application
    LEFT JOIN moria.permissions g ON g.environment=e.id AND g.principal=x.principal AND g.application=a.application
    LEFT JOIN moria.instance_principal_bindings b ON b.environment=x.environment AND b.principal=x.principal
    LEFT JOIN moria.instance_principals i ON i.id=b.instance_id WHERE e.id=registered.environment;
  IF NOT FOUND OR NOT p.enabled OR p.kind<>'workload' OR NOT p.app_enabled OR NOT p.instance_enabled THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  grants:=ARRAY(SELECT unnest(p.grants) INTERSECT SELECT unnest(registered.operations));
  scopes:=ARRAY(SELECT unnest(p.scopes) INTERSECT SELECT unnest(registered.operations));
  labels:=ARRAY(SELECT unnest(p.markings) INTERSECT SELECT unnest(registered.markings));
  RETURN jsonb_build_object('environment',registered.environment,'principal',registered.principal,'application',registered.application,
    'securityEpoch',p.security_epoch::text,'instanceEpoch',(SELECT security_epoch::text FROM moria.instance_state),
    'instanceId',p.instance_id,'attributes',p.attributes,
    'identity',jsonb_build_object('environment',registered.environment,'principal',registered.principal,'application',registered.application,
      'grantedOperations',to_jsonb(grants),'applicationScopes',to_jsonb(scopes),'markings',to_jsonb(labels)));
END $$;
CREATE FUNCTION moria.native_serving_accepted_delegation(origin jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE current_state jsonb; key text;
BEGIN
  IF jsonb_typeof(origin) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  FOREACH key IN ARRAY ARRAY['acceptedGrants','acceptedScopes','acceptedMarkings'] LOOP
    IF jsonb_typeof(origin->key) IS DISTINCT FROM 'array' OR EXISTS(SELECT 1 FROM jsonb_array_elements(origin->key) value WHERE jsonb_typeof(value)<>'string') THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  END LOOP;
  IF origin->>'originKind'='service' THEN
    IF origin->>'originSessionHash' IS NOT NULL OR origin->>'serviceAuthority' IS NULL OR origin->>'originBinding' IS NULL
      OR NOT EXISTS(SELECT 1 FROM moria.runtime_execution_services s WHERE s.binding=(origin->>'originBinding')::uuid
        AND s.authority=(origin->>'serviceAuthority')::uuid) THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
    current_state:=moria.runtime_service_binding_guard((origin->>'originBinding')::uuid);
  ELSIF COALESCE(origin->>'originKind','session')='session' THEN
    IF origin->>'originSessionHash' IS NULL OR origin->>'originSessionHash' !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
    current_state:=moria.native_serving_session_origin(origin->>'originSessionHash',true);
  ELSE RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  IF current_state->>'environment' IS DISTINCT FROM origin->>'environment'
    OR current_state->>'principal' IS DISTINCT FROM origin->>'principal'
    OR current_state->>'application' IS DISTINCT FROM origin->>'application' THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  current_state:=jsonb_set(current_state,'{identity,grantedOperations}',COALESCE((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM (SELECT jsonb_array_elements_text(current_state->'identity'->'grantedOperations') AS value INTERSECT SELECT jsonb_array_elements_text(origin->'acceptedGrants')) caps),'[]'::jsonb));
  current_state:=jsonb_set(current_state,'{identity,applicationScopes}',COALESCE((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM (SELECT jsonb_array_elements_text(current_state->'identity'->'applicationScopes') AS value INTERSECT SELECT jsonb_array_elements_text(origin->'acceptedScopes')) caps),'[]'::jsonb));
  current_state:=jsonb_set(current_state,'{identity,markings}',COALESCE((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM (SELECT jsonb_array_elements_text(current_state->'identity'->'markings') AS value INTERSECT SELECT jsonb_array_elements_text(origin->'acceptedMarkings')) caps),'[]'::jsonb));
  IF origin ? 'purpose' THEN
    IF origin->>'purpose'<>'indexing' OR jsonb_typeof(origin->'allowedSources') IS DISTINCT FROM 'array'
      OR jsonb_array_length(origin->'allowedSources')<>1 OR jsonb_typeof(origin->'allowedSources'->0) IS DISTINCT FROM 'string' OR origin->'allowedSources'->>0=''
      OR jsonb_typeof(origin->'indexTarget') IS DISTINCT FROM 'string' OR origin->>'indexTarget'='' THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
    IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(origin->'acceptedGrants') caps(value) WHERE value NOT IN ((origin->'allowedSources'->>0)||':read',(origin->>'indexTarget')||':index','filesystem:read'))
      OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(origin->'acceptedScopes') caps(value) WHERE value NOT IN ((origin->'allowedSources'->>0)||':read',(origin->>'indexTarget')||':index','filesystem:read')) THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
    current_state:=current_state||jsonb_build_object('privatePurpose','indexing','privateAllowedSources',origin->'allowedSources','privateIndexTarget',origin->>'indexTarget');
  END IF;
  RETURN current_state;
END $$;
CREATE FUNCTION moria.native_serving_job_delegation(execution uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
BEGIN
  RETURN moria.native_serving_accepted_delegation(moria.runtime_job_delegation_guard(execution));
END $$;
CREATE FUNCTION moria.native_serving_session(token_hash text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE origin jsonb; current_state jsonb;
BEGIN
  BEGIN
    RETURN moria.native_serving_session_origin(token_hash,false);
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM<>'UNAUTHENTICATED' THEN RAISE; END IF;
  END;
  IF to_regprocedure('moria.runtime_job_session_origin(text)') IS NULL THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  origin:=moria.runtime_job_session_origin(token_hash);
  IF origin IS NULL THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  current_state:=moria.native_serving_accepted_delegation(origin);
  IF current_state->>'environment' IS DISTINCT FROM origin->>'environment'
    OR current_state->>'principal' IS DISTINCT FROM origin->>'principal'
    OR current_state->>'application' IS DISTINCT FROM origin->>'application' THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  -- This local result attests the execution already checked by the private credential path.
  IF EXISTS(SELECT 1 FROM moria.runtime_job_sessions s WHERE s.token_hash=native_serving_session.token_hash
    AND s.execution::text=origin->>'execution') THEN
    current_state:=current_state||jsonb_build_object('privateValidatedExecution',origin->>'execution');
  END IF;
  RETURN current_state;
END $$;

CREATE INDEX browser_sessions_provider_refresh ON moria.browser_sessions(environment,application,verified_at,token_hash) WHERE NOT disabled AND provider IS NOT NULL;
CREATE TABLE moria.fs_source_resource_files (
  digest text NOT NULL REFERENCES moria.fs_source_releases(digest),reference text NOT NULL,
  entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),PRIMARY KEY(digest,reference,entry_id)
);
CREATE FUNCTION moria.native_source_file_proof_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_SOURCE_FILE_PROOF'; END $$;
CREATE TRIGGER fs_source_resource_files_immutable BEFORE UPDATE OR DELETE ON moria.fs_source_resource_files
  FOR EACH ROW EXECUTE FUNCTION moria.native_source_file_proof_immutable();
-- Extract immutable release geometry once; live policy and provenance still resolve per statement.
CREATE FUNCTION moria.native_source_release_file_entries(snapshot jsonb) RETURNS uuid[]
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
  SELECT ARRAY(SELECT (file->'entry'->>'id')::uuid FROM jsonb_array_elements($1->'files') file)
$$;
CREATE FUNCTION moria.native_source_release_dependencies(snapshot jsonb) RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
  SELECT ARRAY(SELECT dependency->>'digest' FROM jsonb_array_elements($1->'descriptor'->'dependencies') dependency)
$$;
ALTER TABLE moria.fs_source_releases
  ADD COLUMN snapshot_root_entry uuid GENERATED ALWAYS AS ((snapshot->>'rootId')::uuid) STORED,
  ADD COLUMN snapshot_file_entries uuid[] GENERATED ALWAYS AS (moria.native_source_release_file_entries(snapshot)) STORED,
  ADD COLUMN snapshot_dependencies text[] GENERATED ALWAYS AS (moria.native_source_release_dependencies(snapshot)) STORED;

-- Recursive closure costs depend on the actual targets and release inventory.
CREATE FUNCTION moria.native_filesystem_acl_targets_operations_state(state jsonb, target_ids uuid[], operations text[], include_inactive boolean DEFAULT false) RETURNS jsonb LANGUAGE plpgsql STABLE SET plan_cache_mode=force_custom_plan AS $$
DECLARE node record; administrator boolean;
  labels jsonb; label_names text[]; required_ids uuid[]; missing_proof boolean;
  operation text; failure text; errors jsonb:='{}';
BEGIN
  IF operations IS NULL OR cardinality(operations) NOT BETWEEN 1 AND 10
    OR array_position(operations,NULL) IS NOT NULL
    OR NOT operations<@ARRAY['read','list','create','write','move','delete','export','publish','register','administer']::text[]
    THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  operations:=ARRAY(SELECT DISTINCT value FROM unnest(operations) value);
  IF state->>'instanceId' IS NULL THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  IF target_ids IS NULL OR cardinality(target_ids)=0 OR cardinality(target_ids)>4096
    OR array_position(target_ids,NULL) IS NOT NULL THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  target_ids:=ARRAY(SELECT DISTINCT value FROM unnest(target_ids) value);
  IF (SELECT count(*) FROM moria.fs_entries e WHERE e.id=ANY(target_ids))<>cardinality(target_ids)
    OR EXISTS(SELECT 1 FROM moria.fs_entries e
      LEFT JOIN moria.fs_resource_bindings b ON b.entry_id=e.id
      LEFT JOIN moria.fs_roles r ON r.entry_id=b.root_id
      WHERE e.id=ANY(target_ids) AND ((b.active=false AND NOT include_inactive) OR r.retired))
    THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
  administrator:=(state->'identity'->'grantedOperations' ? 'instance:manage') AND (state->'identity'->'applicationScopes' ? 'instance:manage');
WITH RECURSIVE origins AS (
    SELECT unnest(target_ids) AS id UNION SELECT edge.source_id FROM origins o CROSS JOIN LATERAL (
      SELECT parent_id AS source_id FROM moria.fs_entries WHERE id=o.id AND parent_id IS NOT NULL
      UNION ALL SELECT source_id FROM moria.fs_provenance WHERE entry_id=o.id
      UNION ALL SELECT b.entry_id FROM moria.fs_resource_bindings a
        JOIN moria.fs_resource_dependencies d ON d.environment=a.environment AND d.project_id=a.project_id AND d.reference=a.reference
        JOIN moria.fs_resource_bindings b ON b.environment=d.environment AND b.project_id=d.source_project_id AND b.reference=d.source_reference
        WHERE a.entry_id=o.id
    ) edge
  ), releases AS (
    SELECT o.id AS entry_id,r.digest FROM origins o CROSS JOIN LATERAL (
      SELECT digest FROM moria.fs_resource_bindings WHERE entry_id=o.id
      UNION SELECT digest FROM moria.fs_resource_release_history WHERE entry_id=o.id
    ) binding JOIN moria.fs_source_releases r ON r.digest=binding.digest
    UNION SELECT r.entry_id,d.digest FROM releases r
      JOIN moria.fs_source_releases parent_release ON parent_release.digest=r.digest
      CROSS JOIN LATERAL unnest(parent_release.snapshot_dependencies) dependency(digest)
      JOIN moria.fs_source_releases d ON d.digest=dependency.digest
  ), closure AS (
    SELECT unnest(target_ids) AS id UNION SELECT edge.source_id FROM closure c CROSS JOIN LATERAL (
      SELECT parent_id AS source_id FROM moria.fs_entries WHERE id=c.id AND parent_id IS NOT NULL
      UNION ALL SELECT source_id FROM moria.fs_provenance WHERE entry_id=c.id
      UNION ALL SELECT b.entry_id FROM moria.fs_resource_bindings a
        JOIN moria.fs_resource_dependencies d ON d.environment=a.environment AND d.project_id=a.project_id AND d.reference=a.reference
        JOIN moria.fs_resource_bindings b ON b.environment=d.environment AND b.project_id=d.source_project_id AND b.reference=d.source_reference
        WHERE a.entry_id=c.id
      UNION ALL SELECT source_release.snapshot_root_entry FROM releases r
        JOIN moria.fs_source_releases source_release ON source_release.digest=r.digest WHERE r.entry_id=c.id
      UNION ALL SELECT file.entry_id FROM releases r
        JOIN moria.fs_source_releases source_release ON source_release.digest=r.digest
        CROSS JOIN LATERAL unnest(source_release.snapshot_file_entries) file(entry_id) WHERE r.entry_id=c.id
    ) edge
  ), required AS (
    SELECT unnest(target_ids) AS id UNION SELECT edge.source_id FROM required c CROSS JOIN LATERAL (
      WITH RECURSIVE ancestry AS (
        SELECT id,parent_id FROM moria.fs_entries WHERE id=c.id
        UNION ALL SELECT e.id,e.parent_id FROM ancestry a JOIN moria.fs_entries e ON e.id=a.parent_id
      ) SELECT p.source_id FROM ancestry a JOIN moria.fs_provenance p ON p.entry_id=a.id
      UNION ALL SELECT b.entry_id FROM ancestry origin JOIN moria.fs_resource_bindings a ON a.entry_id=origin.id
        JOIN moria.fs_resource_dependencies d ON d.environment=a.environment AND d.project_id=a.project_id AND d.reference=a.reference
        JOIN moria.fs_resource_bindings b ON b.environment=d.environment AND b.project_id=d.source_project_id AND b.reference=d.source_reference
      UNION ALL SELECT proof.entry_id FROM moria.fs_resource_bindings b
        JOIN moria.fs_source_resource_files proof ON proof.digest=b.digest AND proof.reference=b.reference
        WHERE b.entry_id=c.id
    ) edge
  ), inherited AS (
    SELECT COALESCE(p.markings,'[]')||COALESCE(p.retained_markings,'[]') AS markings
      FROM closure c LEFT JOIN moria.fs_policies p ON p.entry_id=c.id
    UNION ALL SELECT source_release.markings FROM releases r
      JOIN moria.fs_source_releases source_release ON source_release.digest=r.digest WHERE r.entry_id IN(SELECT id FROM closure)
  ) SELECT COALESCE(jsonb_agg(DISTINCT marking),'[]'),
      (SELECT array_agg(id) FROM (SELECT id FROM required LIMIT 4097) bounded),
      EXISTS(SELECT 1 FROM origins o JOIN moria.fs_resource_bindings b ON b.entry_id=o.id
        WHERE NOT EXISTS(SELECT 1 FROM moria.fs_source_resource_files proof WHERE proof.digest=b.digest AND proof.reference=b.reference))
    INTO labels,required_ids,missing_proof FROM inherited CROSS JOIN LATERAL jsonb_array_elements(markings) marking;
  IF cardinality(required_ids)>4096 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  IF EXISTS(SELECT 1 FROM unnest(required_ids) AS required(entry_id) LEFT JOIN moria.fs_entries e ON e.id=required.entry_id WHERE e.id IS NULL OR e.deleted)
    THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
  IF EXISTS(SELECT 1 FROM unnest(required_ids) origin JOIN moria.fs_resource_bindings b ON b.entry_id=origin
    JOIN moria.fs_roles role ON role.entry_id=b.root_id
    WHERE (NOT(origin=ANY(target_ids)) OR NOT include_inactive) AND (NOT b.active OR role.retired)) THEN failure:='NOT_FOUND'; END IF;
  label_names:=ARRAY(SELECT value->>'marking' FROM jsonb_array_elements(labels));
  IF failure IS NULL AND (EXISTS(SELECT 1 FROM jsonb_array_elements(labels) label
      WHERE label->>'environment' IS DISTINCT FROM state->>'environment')
    OR EXISTS(SELECT 1 FROM unnest(label_names) name WHERE NOT (state->'identity'->'markings' ? name))) THEN
    failure:='FORBIDDEN';
  END IF;
  FOREACH operation IN ARRAY operations LOOP
    IF missing_proof AND operation<>'publish' THEN
      errors:=jsonb_set(errors,ARRAY[operation],to_jsonb('SOURCE_FILE_PROOF_REQUIRED'::text));
    ELSIF failure IS NOT NULL THEN errors:=jsonb_set(errors,ARRAY[operation],to_jsonb(failure)); END IF;
  END LOOP;
  FOR node IN WITH RECURSIVE ancestry AS (
    SELECT e.id AS root,e.id,e.parent_id,e.environment_id FROM moria.fs_entries e WHERE e.id=ANY(required_ids)
    UNION ALL SELECT a.root,e.id,e.parent_id,e.environment_id FROM ancestry a JOIN moria.fs_entries e ON e.id=a.parent_id
  ), applicable AS (
    SELECT a.root,a.environment_id,op.operation,
      grant_value->>'effect' AS effect,
      CASE grant_value->'subject'->>'kind'
        WHEN 'administrators' THEN administrator
        WHEN 'principal' THEN grant_value->'subject'->>'id'=state->>'instanceId'
        WHEN 'group' THEN grant_value->'subject'->>'environment'=state->>'environment'
          AND jsonb_typeof(state->'attributes'->'groups')='array' AND (state->'attributes'->'groups' ? (grant_value->'subject'->>'id'))
        ELSE false END
      AND (a.id=a.root OR (grant_value->>'inherit')::boolean)
      AND (grant_value->'operations' ? CASE WHEN a.root=ANY(target_ids) THEN op.operation ELSE 'read' END) AS matches
    FROM ancestry a CROSS JOIN unnest(operations) op(operation)
      LEFT JOIN moria.fs_policies p ON p.entry_id=a.id
      LEFT JOIN LATERAL jsonb_array_elements(COALESCE(p.grants,'[]')) grant_value ON true
    WHERE NOT (errors ? op.operation)
  ) SELECT root,applicable.operation,COALESCE(bool_or(matches AND effect='allow'),false) AS allowed,
      COALESCE(bool_or(matches AND effect='deny'),false) AS denied,
      COALESCE(bool_or(environment_id IS NOT NULL AND environment_id<>state->>'environment'),false) AS wrong_environment
    FROM applicable GROUP BY root,applicable.operation LOOP
    IF NOT node.allowed OR node.denied OR node.wrong_environment THEN
      errors:=jsonb_set(errors,ARRAY[node.operation],to_jsonb('FORBIDDEN'::text));
    END IF;
  END LOOP;
  RETURN jsonb_build_object('labels',to_jsonb(ARRAY(SELECT DISTINCT unnest(label_names) ORDER BY 1)),'errors',errors);
END $$;
REVOKE ALL ON FUNCTION moria.native_filesystem_acl_targets_operations_state(jsonb,uuid[],text[],boolean) FROM PUBLIC;
CREATE FUNCTION moria.native_filesystem_acl_targets_state(state jsonb, target_ids uuid[], operation text, include_inactive boolean DEFAULT false)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE result jsonb;
BEGIN
  result:=moria.native_filesystem_acl_targets_operations_state(state,target_ids,ARRAY[operation],include_inactive);
  IF result->'errors' ? operation THEN RAISE EXCEPTION '%',result->'errors'->>operation; END IF;
  RETURN result->'labels';
END $$;
REVOKE ALL ON FUNCTION moria.native_filesystem_acl_targets_state(jsonb,uuid[],text,boolean) FROM PUBLIC;
CREATE FUNCTION moria.native_filesystem_acl_state(state jsonb,target_id uuid,operation text,include_inactive boolean DEFAULT false)
RETURNS jsonb LANGUAGE plpgsql AS $$
BEGIN
 RETURN moria.native_filesystem_acl_targets_state(state,ARRAY[target_id],operation,include_inactive);
END $$;
REVOKE ALL ON FUNCTION moria.native_filesystem_acl_state(jsonb,uuid,text,boolean) FROM PUBLIC;
CREATE FUNCTION moria.native_serving_filesystem_resource_entry_state(state jsonb, resource text) RETURNS uuid LANGUAGE plpgsql STABLE AS $$
DECLARE target record; targets integer;
BEGIN
  SELECT count(*) INTO targets FROM moria.fs_entries e LEFT JOIN moria.fs_resource_bindings b ON b.entry_id=e.id
    WHERE e.environment_id=state->>'environment' AND e.kind='resource' AND NOT e.deleted
      AND (e.resource_id::text=resource OR b.reference=resource);
  IF targets=0 THEN
    IF EXISTS(SELECT 1 FROM moria.fs_entries e
      LEFT JOIN moria.fs_resource_bindings b ON b.entry_id=e.id
      LEFT JOIN moria.fs_resource_identities i ON i.resource_id=e.resource_id
      WHERE e.environment_id=state->>'environment' AND e.kind='resource'
        AND (e.resource_id::text=resource OR b.reference=resource OR i.reference=resource)) THEN
      RAISE EXCEPTION 'NOT_FOUND';
    END IF;
    RETURN NULL;
  END IF;
  IF targets<>1 OR state->>'instanceId' IS NULL THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  SELECT e.id,b.active,r.retired INTO target FROM moria.fs_entries e
    LEFT JOIN moria.fs_resource_bindings b ON b.entry_id=e.id LEFT JOIN moria.fs_roles r ON r.entry_id=b.root_id
    WHERE e.environment_id=state->>'environment' AND e.kind='resource' AND NOT e.deleted
      AND (e.resource_id::text=resource OR b.reference=resource);
  IF target.active IS DISTINCT FROM true OR target.retired THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
  RETURN target.id;
END $$;
REVOKE ALL ON FUNCTION moria.native_serving_filesystem_resource_entry_state(jsonb,text) FROM PUBLIC;
CREATE FUNCTION moria.native_serving_filesystem_resource_access_operations_state(state jsonb, resource text, operations text[]) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE target uuid;
BEGIN
  IF operations IS NULL OR cardinality(operations) NOT BETWEEN 1 AND 3
    OR array_position(operations,NULL) IS NOT NULL OR NOT operations<@ARRAY['read','write','publish']::text[]
    THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  target:=moria.native_serving_filesystem_resource_entry_state(state,resource);
  IF target IS NULL THEN RETURN jsonb_build_object('labels','[]'::jsonb,'errors','{}'::jsonb); END IF;
  RETURN moria.native_filesystem_acl_targets_operations_state(state,ARRAY[target],operations,false);
END $$;
REVOKE ALL ON FUNCTION moria.native_serving_filesystem_resource_access_operations_state(jsonb,text,text[]) FROM PUBLIC;
CREATE FUNCTION moria.native_serving_filesystem_resource_access_state(state jsonb, resource text, operation text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE result jsonb;
BEGIN
  result:=moria.native_serving_filesystem_resource_access_operations_state(state,resource,ARRAY[operation]);
  IF result->'errors' ? operation THEN RAISE EXCEPTION '%',result->'errors'->>operation; END IF;
  RETURN result->'labels';
END $$;
REVOKE ALL ON FUNCTION moria.native_serving_filesystem_resource_access_state(jsonb,text,text) FROM PUBLIC;
CREATE FUNCTION moria.native_serving_resource_access(token_hash text,resource text,operation text) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
  RETURN moria.native_serving_filesystem_resource_access_state(moria.native_serving_session(token_hash),resource,operation);
END $$;
`;

/** Function-only references use immutable invocation identities rather than mutable execution fences. */
export const servingFunctionReferenceSchema = String.raw`
ALTER TABLE moria.native_function_dependencies
  DROP CONSTRAINT native_function_dependencies_child_fkey,
  ADD CONSTRAINT native_function_dependencies_child_fkey
    FOREIGN KEY(child) REFERENCES moria.runtime_function_invocations(execution);
ALTER TABLE moria.native_function_proposals
  DROP CONSTRAINT native_function_proposals_prepared_execution_fkey,
  ADD CONSTRAINT native_function_proposals_prepared_execution_fkey
    FOREIGN KEY(prepared_execution) REFERENCES moria.runtime_function_invocations(execution);
ALTER TABLE moria.native_serving_prepared_actions
  DROP CONSTRAINT native_serving_prepared_actions_execution_fkey,
  ADD CONSTRAINT native_serving_prepared_actions_execution_fkey
    FOREIGN KEY(execution) REFERENCES moria.runtime_function_invocations(execution);
`;
