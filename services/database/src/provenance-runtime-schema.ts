/** Retained resource classifications and immutable, pre-admitted data dependencies. */
export const provenanceRuntimeSchema = String.raw`
CREATE TABLE moria.resource_data_edges (
  environment text NOT NULL REFERENCES moria.environments(id), derived text NOT NULL, source text NOT NULL,
  PRIMARY KEY(environment,derived,source)
);
CREATE INDEX resource_data_sources ON moria.resource_data_edges(environment,source,derived);
CREATE TABLE moria.resource_data_labels (
  environment text NOT NULL REFERENCES moria.environments(id), resource text NOT NULL, marking text NOT NULL,
  PRIMARY KEY(environment,resource,marking)
);
CREATE FUNCTION moria.runtime_data_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_DATA_PROVENANCE'; END $$;
CREATE FUNCTION moria.runtime_dataset_session_matches(session_hash text,execution_id uuid,task_id uuid,worker uuid,worker_fence bigint,instance_incarnation uuid)
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS(SELECT 1 FROM moria.runtime_executions e WHERE e.id=execution_id AND e.context->>'sessionHash'=session_hash)
    OR EXISTS(SELECT 1 FROM moria.runtime_job_sessions s WHERE s.token_hash=session_hash AND s.execution=execution_id
      AND s.task IS NOT DISTINCT FROM task_id AND s.owner=worker AND s.fence=worker_fence AND s.incarnation=instance_incarnation)
$$;
CREATE TRIGGER resource_data_edges_immutable BEFORE UPDATE OR DELETE ON moria.resource_data_edges
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_immutable();
CREATE TRIGGER resource_data_labels_immutable BEFORE UPDATE OR DELETE ON moria.resource_data_labels
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_immutable();

CREATE FUNCTION moria.runtime_data_resource(env text, resource_name text) RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE ids text[];
BEGIN
  IF octet_length(resource_name) NOT BETWEEN 1 AND 1024 THEN RAISE EXCEPTION 'RESOURCE_INVALID'; END IF;
  IF resource_name ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RETURN (resource_name::uuid)::text;
  END IF;
  SELECT array_agg(DISTINCT e.resource_id::text) INTO ids FROM moria.fs_entries e
    LEFT JOIN moria.fs_resource_bindings b ON b.entry_id=e.id
    WHERE e.environment_id=env AND e.kind='resource' AND NOT e.deleted
      AND (e.resource_id::text=resource_name OR b.reference=resource_name);
  IF cardinality(ids)>1 THEN RAISE EXCEPTION 'AMBIGUOUS_RESOURCE'; END IF;
  IF cardinality(ids)=1 THEN RETURN ids[1]; END IF;
  SELECT array_agg(DISTINCT i.resource_id::text) INTO ids FROM moria.fs_resource_identities i
    JOIN moria.fs_entries root ON root.id=i.root_id
    WHERE root.environment_id=env AND (i.resource_id::text=resource_name OR i.reference=resource_name);
  IF cardinality(ids)>1 THEN RAISE EXCEPTION 'AMBIGUOUS_RESOURCE'; END IF;
  RETURN COALESCE(ids[1],resource_name);
END $$;
CREATE FUNCTION moria.runtime_data_origins(env text, resource_name text) RETURNS text[] LANGUAGE plpgsql STABLE AS $$
DECLARE resources text[];
BEGIN
  WITH RECURSIVE origins(resource) AS (
    SELECT moria.runtime_data_resource(env,resource_name)
    UNION SELECT e.source FROM origins o JOIN moria.resource_data_edges e
      ON e.environment=env AND e.derived=o.resource
  ) SELECT array_agg(resource) INTO resources FROM (SELECT resource FROM origins LIMIT 4097) bounded;
  IF cardinality(resources)>4096 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  RETURN resources;
END $$;
CREATE FUNCTION moria.runtime_data_labels(env text, resource_name text) RETURNS text[] LANGUAGE sql STABLE AS $$
  SELECT ARRAY(SELECT DISTINCT l.marking FROM unnest(moria.runtime_data_origins(env,resource_name)) origin
    JOIN moria.resource_data_labels l ON l.environment=env AND l.resource=origin ORDER BY 1)
$$;
CREATE FUNCTION moria.runtime_dataset_kind(env text,resource_name text) RETURNS void LANGUAGE plpgsql STABLE AS $$
DECLARE binding record; active record; published jsonb;
BEGIN
  IF NOT EXISTS(SELECT 1 FROM moria.dataset_heads WHERE environment=env AND dataset=resource_name) THEN RAISE EXCEPTION 'DATASET_UNKNOWN'; END IF;
  SELECT b.*,e.resource_id INTO binding FROM moria.fs_entries e LEFT JOIN moria.fs_resource_bindings b ON b.entry_id=e.id
    WHERE e.environment_id=env AND NOT e.deleted AND e.kind='resource'
      AND (e.resource_id::text=resource_name OR b.reference=resource_name);
  IF FOUND THEN
    IF binding.active IS DISTINCT FROM true THEN RAISE EXCEPTION 'DATASET_UNKNOWN'; END IF;
    SELECT * INTO active FROM moria.fs_active_releases WHERE root_id=binding.root_id AND environment=env;
    IF NOT FOUND OR active.digest IS DISTINCT FROM binding.digest THEN RAISE EXCEPTION 'DATASET_BINDING_CHANGED'; END IF;
    SELECT item INTO published FROM jsonb_array_elements(active.resources) item
      WHERE concat_ws('/',item->'metadata'->>'namespace',item->>'kind',item->'metadata'->>'name')=binding.reference;
    IF NOT FOUND OR published->>'kind'<>'Dataset' THEN RAISE EXCEPTION 'WHOLE_DATASET_REQUIRED'; END IF;
  ELSIF EXISTS(SELECT 1 FROM moria.fs_entries e LEFT JOIN moria.fs_resource_identities i ON i.resource_id=e.resource_id
    WHERE e.environment_id=env AND (e.resource_id::text=resource_name OR i.reference=resource_name)) THEN
    RAISE EXCEPTION 'DATASET_UNKNOWN';
  END IF;
END $$;
CREATE FUNCTION moria.runtime_data_epoch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  RETURN NEW;
END $$;
CREATE TRIGGER resource_data_edge_epoch AFTER INSERT ON moria.resource_data_edges
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_epoch();
CREATE TRIGGER resource_data_label_epoch AFTER INSERT ON moria.resource_data_labels
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_data_epoch();
CREATE FUNCTION moria.runtime_namespace_classification() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.enabled AND NEW.purpose='dataset-source' THEN
    INSERT INTO moria.resource_data_labels(environment,resource,marking)
      SELECT NEW.environment,moria.runtime_data_resource(NEW.environment,NEW.resource),label
        FROM unnest(NEW.markings) label ON CONFLICT DO NOTHING;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_storage_namespace_classification AFTER INSERT OR UPDATE ON moria.native_storage_namespaces
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_namespace_classification();

ALTER FUNCTION moria.native_serving_resource_access(text,text,text) RENAME TO native_serving_filesystem_resource_access;
REVOKE ALL ON FUNCTION moria.native_serving_filesystem_resource_access(text,text,text) FROM PUBLIC;
CREATE FUNCTION moria.runtime_data_operational(env text,resource_name text,operation text) RETURNS void LANGUAGE plpgsql STABLE AS $$
DECLARE resource_id_value uuid;
BEGIN
  IF operation='publish' THEN RETURN; END IF;
  IF resource_name !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN RETURN; END IF;
  resource_id_value:=resource_name::uuid;
  IF EXISTS(SELECT 1 FROM moria.native_dataset_bindings d WHERE d.environment=env AND d.resource=resource_id_value AND NOT d.enabled)
    THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
  IF EXISTS(SELECT 1 FROM moria.fs_resource_identities WHERE resource_id=resource_id_value)
    AND NOT EXISTS(SELECT 1 FROM moria.fs_entries e
      JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.environment=env AND b.active
      JOIN moria.fs_resource_identities i ON i.resource_id=e.resource_id AND i.root_id=b.root_id AND i.reference=b.reference
      JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired
      JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=env AND a.digest=b.digest
      WHERE e.environment_id=env AND e.resource_id=resource_id_value AND e.kind='resource' AND NOT e.deleted
        AND EXISTS(SELECT 1 FROM jsonb_array_elements(a.resources) item
          WHERE concat_ws('/',item->'metadata'->>'namespace',item->>'kind',item->'metadata'->>'name')=b.reference)) THEN
    RAISE EXCEPTION 'NOT_FOUND';
  END IF;
END $$;
CREATE FUNCTION moria.native_serving_resource_access_state(state jsonb,resource text,operation text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE origin text; labels text[]; fs_labels jsonb; self_resource text; origins text[];
BEGIN
  IF operation NOT IN ('read','write','publish') THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  self_resource:=moria.runtime_data_resource(state->>'environment',resource);
  IF state->>'privatePurpose'='indexing' AND (operation<>'read' OR NOT (state->'privateAllowedSources' ? self_resource)) THEN
    RAISE EXCEPTION 'FORBIDDEN';
  END IF;
  origins:=moria.runtime_data_origins(state->>'environment',resource);
  labels:=ARRAY(SELECT DISTINCT l.marking FROM unnest(origins) o JOIN moria.resource_data_labels l
    ON l.environment=state->>'environment' AND l.resource=o);
  FOREACH origin IN ARRAY origins LOOP
    PERFORM moria.runtime_data_operational(state->>'environment',origin,operation);
    fs_labels:=moria.native_serving_filesystem_resource_access_state(state,origin,
      CASE WHEN origin=self_resource THEN operation ELSE 'read' END);
    labels:=labels||ARRAY(SELECT jsonb_array_elements_text(fs_labels));
  END LOOP;
  IF EXISTS(SELECT 1 FROM unnest(labels) label WHERE NOT (state->'identity'->'markings' ? label)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  RETURN to_jsonb(ARRAY(SELECT DISTINCT label FROM unnest(labels) label ORDER BY label));
END $$;
REVOKE ALL ON FUNCTION moria.native_serving_resource_access_state(jsonb,text,text) FROM PUBLIC;
CREATE FUNCTION moria.native_serving_resource_access_batch_state(state jsonb,requests jsonb) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE request jsonb; origin text; operation text; actual_operation text; self_resource text; origins text[];
  labels text[]; fs_labels jsonb; fs_outcomes jsonb; fs_cache jsonb:='{}'; results jsonb:='[]';
BEGIN
  IF jsonb_typeof(requests) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  IF jsonb_array_length(requests)>256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  FOR request IN SELECT value FROM jsonb_array_elements(requests) LOOP
    IF jsonb_typeof(request) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    IF request-ARRAY['resource','operation']<>'{}'::jsonb
      OR jsonb_typeof(request->'resource') IS DISTINCT FROM 'string'
      OR octet_length(request->>'resource') NOT BETWEEN 1 AND 4096
      OR jsonb_typeof(request->'operation') IS DISTINCT FROM 'string'
      OR request->>'operation' NOT IN ('read','write','publish') THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    operation:=request->>'operation';
    self_resource:=moria.runtime_data_resource(state->>'environment',request->>'resource');
    IF state->>'privatePurpose'='indexing' AND (operation<>'read' OR NOT (state->'privateAllowedSources' ? self_resource)) THEN
      RAISE EXCEPTION 'FORBIDDEN';
    END IF;
    origins:=moria.runtime_data_origins(state->>'environment',request->>'resource');
    labels:=ARRAY(SELECT DISTINCT l.marking FROM unnest(origins) o JOIN moria.resource_data_labels l
      ON l.environment=state->>'environment' AND l.resource=o);
    FOREACH origin IN ARRAY origins LOOP
      PERFORM moria.runtime_data_operational(state->>'environment',origin,operation);
      actual_operation:=CASE WHEN origin=self_resource THEN operation ELSE 'read' END;
      -- Outcomes share this statement snapshot; each denial is raised in request order.
      IF fs_cache ? origin THEN
        fs_outcomes:=fs_cache->origin;
      ELSE
        fs_outcomes:=moria.native_serving_filesystem_resource_access_operations_state(state,origin,ARRAY['read','write','publish']);
        fs_cache:=jsonb_set(fs_cache,ARRAY[origin],fs_outcomes);
        IF pg_column_size(fs_cache)>8388608 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
      END IF;
      IF fs_outcomes->'errors' ? actual_operation THEN RAISE EXCEPTION '%',fs_outcomes->'errors'->>actual_operation; END IF;
      fs_labels:=fs_outcomes->'labels';
      labels:=labels||ARRAY(SELECT jsonb_array_elements_text(fs_labels));
    END LOOP;
    IF EXISTS(SELECT 1 FROM unnest(labels) label WHERE NOT (state->'identity'->'markings' ? label)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    results:=results||jsonb_build_array(to_jsonb(ARRAY(SELECT DISTINCT label FROM unnest(labels) label ORDER BY label)));
  END LOOP;
  RETURN results;
END $$;
REVOKE ALL ON FUNCTION moria.native_serving_resource_access_batch_state(jsonb,jsonb) FROM PUBLIC;
CREATE FUNCTION moria.native_serving_resource_access(token_hash text,resource text,operation text) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
  RETURN moria.native_serving_resource_access_state(moria.native_serving_session(token_hash),resource,operation);
END $$;
CREATE FUNCTION moria.native_resource_filesystem_access_state(state jsonb,resource text,operation text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE canonical text; permission text; legacy text; kind text;
BEGIN
  IF operation NOT IN ('read','write','invoke','run','build','sync','index','project','query','publish','use') THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  canonical:=moria.runtime_data_resource(state->>'environment',resource);
  permission:=canonical||':'||operation;
  SELECT item->>'kind' INTO kind FROM moria.fs_entries e
    JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active AND b.environment=state->>'environment'
    JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest
    CROSS JOIN LATERAL jsonb_array_elements(a.resources) item
    WHERE e.environment_id=state->>'environment' AND e.resource_id::text=canonical AND NOT e.deleted
      AND concat_ws('/',item->'metadata'->>'namespace',item->>'kind',item->'metadata'->>'name')=b.reference;
  IF operation='use' AND kind IS DISTINCT FROM 'SecretReference' THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  legacy:=CASE WHEN kind='Function' AND operation='invoke' THEN 'functions:invoke'
    WHEN kind='Agent' AND operation='invoke' THEN 'agents:invoke'
    WHEN kind='EvaluationSuite' AND operation='run' THEN 'evaluations:run' ELSE NULL END;
  IF NOT ((state->'identity'->'grantedOperations' ? permission) AND (state->'identity'->'applicationScopes' ? permission))
    AND NOT (legacy IS NOT NULL AND (state->'identity'->'grantedOperations' ? legacy)
      AND (state->'identity'->'applicationScopes' ? legacy)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  RETURN moria.native_serving_resource_access_state(state,canonical,CASE WHEN operation IN('write','publish') THEN operation ELSE 'read' END);
END $$;
REVOKE ALL ON FUNCTION moria.native_resource_filesystem_access_state(jsonb,text,text) FROM PUBLIC;
CREATE FUNCTION moria.native_resource_filesystem_access(token_hash text,resource text,operation text) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
  RETURN moria.native_resource_filesystem_access_state(moria.native_serving_session(token_hash),resource,operation);
END $$;
CREATE FUNCTION moria.native_resource_release_access_state(state jsonb,resource text,operation text,release_digest text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE canonical text; target record; file_id uuid; labels text[]; marks jsonb;
BEGIN
  labels:=ARRAY(SELECT jsonb_array_elements_text(moria.native_resource_filesystem_access_state(state,resource,operation)));
  canonical:=moria.runtime_data_resource(state->>'environment',resource);
  SELECT r.root_id,r.markings,i.reference INTO target FROM moria.fs_source_releases r
    JOIN moria.fs_resource_identities i ON i.root_id=r.root_id AND i.resource_id::text=canonical
    JOIN moria.fs_roles role ON role.entry_id=r.root_id AND NOT role.retired
    WHERE r.digest=release_digest AND r.owned ? i.reference;
  IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM moria.fs_source_resource_files p WHERE p.digest=release_digest AND p.reference=target.reference)
    THEN RAISE EXCEPTION 'SOURCE_FILE_PROOF_REQUIRED'; END IF;
  IF (SELECT count(*) FROM moria.fs_source_resource_files p WHERE p.digest=release_digest AND p.reference=target.reference)>4096
    THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  FOR file_id IN SELECT entry_id FROM moria.fs_source_resource_files p WHERE p.digest=release_digest AND p.reference=target.reference LOOP
    IF NOT EXISTS(SELECT 1 FROM moria.fs_entries e WHERE e.id=file_id AND e.kind='file' AND NOT e.deleted)
      THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
    labels:=labels||ARRAY(SELECT jsonb_array_elements_text(moria.native_filesystem_acl_state(state,file_id,'read',false)));
  END LOOP;
  FOR marks IN SELECT value FROM jsonb_array_elements(target.markings) LOOP
    IF marks->>'environment' IS DISTINCT FROM state->>'environment' THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    labels:=array_append(labels,marks->>'marking');
  END LOOP;
  IF EXISTS(SELECT 1 FROM unnest(labels) label WHERE NOT (state->'identity'->'markings' ? label)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  RETURN to_jsonb(ARRAY(SELECT DISTINCT label FROM unnest(labels) label ORDER BY label));
END $$;
REVOKE ALL ON FUNCTION moria.native_resource_release_access_state(jsonb,text,text,text) FROM PUBLIC;
CREATE FUNCTION moria.native_resource_release_access(token_hash text,resource text,operation text,release_digest text) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
  RETURN moria.native_resource_release_access_state(moria.native_serving_session(token_hash),resource,operation,release_digest);
END $$;
CREATE FUNCTION moria.native_dataset_receipt_access(private_hash text,execution_id uuid,worker uuid,worker_fence bigint,resource text,operation text)
  RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; e moria.runtime_executions; labels jsonb;
BEGIN
  IF operation NOT IN ('read','write') THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id;
  IF NOT FOUND OR NOT (e.spec->'expectedHeads' ? resource) OR e.status<>'succeeded'
    OR NOT EXISTS(SELECT 1 FROM moria.publications p WHERE p.environment=e.environment AND p.execution=e.id::text
      AND p.receipt=e.result AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.receipt->'outputs') item WHERE item->>'dataset'=resource)) THEN
    RAISE EXCEPTION 'JOB_RECEIPT_AUTHORITY_REQUIRED';
  END IF;
  state:=moria.runtime_job_receipt_authority(private_hash,execution_id,worker,worker_fence);
  IF ROW(state->>'environment',state->>'principal',state->>'application') IS DISTINCT FROM ROW(e.environment,e.principal,e.application)
    OR NOT (state->'identity'->'grantedOperations' ? (resource||':read')) OR NOT (state->'identity'->'applicationScopes' ? (resource||':read'))
    OR NOT (state->'identity'->'grantedOperations' ? (resource||':write')) OR NOT (state->'identity'->'applicationScopes' ? (resource||':write')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  PERFORM moria.runtime_dataset_kind(e.environment,resource);
  labels:=moria.native_serving_resource_access_state(state,resource,operation);
  RETURN labels;
END $$;

CREATE FUNCTION moria.runtime_data_admit(session_hash text,derived_resources text[],source_resources text[],target_operation text)
  RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE state jsonb; derived_resource text; source_resource text; labels jsonb;
BEGIN
  IF cardinality(derived_resources)>64 OR cardinality(source_resources)>64
    OR target_operation NOT IN ('write','index') THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  state:=moria.native_serving_session_origin(session_hash,false);
  PERFORM 1 FROM moria.environments WHERE id=state->>'environment' FOR UPDATE;
  FOREACH source_resource IN ARRAY source_resources LOOP
    IF NOT (state->'identity'->'grantedOperations' ? (source_resource||':read'))
      OR NOT (state->'identity'->'applicationScopes' ? (source_resource||':read')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    PERFORM moria.native_serving_resource_access(session_hash,source_resource,'read');
  END LOOP;
  FOREACH derived_resource IN ARRAY derived_resources LOOP
    IF NOT (state->'identity'->'grantedOperations' ? (derived_resource||':'||target_operation))
      OR NOT (state->'identity'->'applicationScopes' ? (derived_resource||':'||target_operation)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    PERFORM moria.native_serving_resource_access(session_hash,derived_resource,'write');
    FOREACH source_resource IN ARRAY source_resources LOOP
      IF moria.runtime_data_resource(state->>'environment',derived_resource)<>moria.runtime_data_resource(state->>'environment',source_resource) THEN
        INSERT INTO moria.resource_data_edges VALUES(state->>'environment',
          moria.runtime_data_resource(state->>'environment',derived_resource),moria.runtime_data_resource(state->>'environment',source_resource))
          ON CONFLICT DO NOTHING;
      END IF;
    END LOOP;
    -- Also bounds the complete newly admitted graph and checks inherited ACLs/labels.
    PERFORM moria.native_serving_resource_access(session_hash,derived_resource,'write');
  END LOOP;
  RETURN (SELECT security_epoch FROM moria.environments WHERE id=state->>'environment');
END $$;
CREATE FUNCTION moria.runtime_data_assert(env text,derived_resource text,source_resource text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF moria.runtime_data_resource(env,derived_resource)<>moria.runtime_data_resource(env,source_resource)
    AND NOT EXISTS(SELECT 1 FROM moria.resource_data_edges WHERE environment=env
      AND derived=moria.runtime_data_resource(env,derived_resource) AND source=moria.runtime_data_resource(env,source_resource)) THEN
    RAISE EXCEPTION 'UNDECLARED_DATA_PROVENANCE';
  END IF;
END $$;
CREATE FUNCTION moria.runtime_service_dataset_edges(authority uuid,env text,target uuid,domain text,binding_revision text,class_name text,
  inputs uuid[],outputs uuid[],markings text[]) RETURNS bigint LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; source_id uuid; target_id uuid; inherited text[]:='{}'; labels jsonb;
BEGIN
  IF inputs IS NULL OR outputs IS NULL OR markings IS NULL OR cardinality(inputs) NOT BETWEEN 1 AND 64
    OR cardinality(outputs) NOT BETWEEN 1 AND 64 OR cardinality(markings)>256
    OR array_position(inputs,NULL) IS NOT NULL OR array_position(outputs,NULL) IS NOT NULL OR array_position(markings,NULL) IS NOT NULL
    THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/environment/'||env,0));
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0));
  PERFORM 1 FROM moria.environments WHERE id=env FOR UPDATE;
  state:=moria.runtime_service_target_authority(authority,env,target,domain,binding_revision,class_name);
  IF NOT to_jsonb(markings)<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  FOREACH source_id IN ARRAY inputs LOOP
    IF NOT (state->'identity'->'grantedOperations' ? (source_id::text||':read'))
      OR NOT (state->'identity'->'applicationScopes' ? (source_id::text||':read')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    PERFORM moria.runtime_dataset_kind(env,source_id::text);
    labels:=moria.native_serving_resource_access_state(state,source_id::text,'read');
    inherited:=inherited||ARRAY(SELECT jsonb_array_elements_text(labels));
  END LOOP;
  FOREACH target_id IN ARRAY outputs LOOP
    IF NOT (state->'identity'->'grantedOperations' ? (target_id::text||':read')) OR NOT (state->'identity'->'applicationScopes' ? (target_id::text||':read'))
      OR NOT (state->'identity'->'grantedOperations' ? (target_id::text||':write')) OR NOT (state->'identity'->'applicationScopes' ? (target_id::text||':write')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    PERFORM moria.runtime_dataset_kind(env,target_id::text);
    labels:=moria.native_serving_resource_access_state(state,target_id::text,'write');
    IF NOT inherited<@moria.runtime_data_labels(env,target_id::text) OR NOT to_jsonb(inherited)<@to_jsonb(markings)
      OR NOT labels<@to_jsonb(markings) THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  END LOOP;
  FOREACH target_id IN ARRAY outputs LOOP
    FOREACH source_id IN ARRAY inputs LOOP
      IF target_id<>source_id THEN INSERT INTO moria.resource_data_edges VALUES(env,target_id::text,source_id::text) ON CONFLICT DO NOTHING; END IF;
    END LOOP;
    PERFORM moria.native_serving_resource_access_state(state,target_id::text,'write');
  END LOOP;
  RETURN (SELECT security_epoch FROM moria.environments WHERE id=env);
END $$;
CREATE FUNCTION moria.runtime_binding_data_admit() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE sources text[]; targets text[]; epoch bigint; state jsonb; target_name text;
BEGIN
  sources:=ARRAY(SELECT DISTINCT p->>'resource' FROM jsonb_array_elements(NEW.spec->'pinnedInputs') p);
  targets:=ARRAY(SELECT jsonb_object_keys(NEW.spec->'expectedHeads'));
  IF cardinality(sources)=0 OR cardinality(targets)=0 THEN RETURN NEW; END IF;
  SELECT security_epoch INTO epoch FROM moria.environments WHERE id=NEW.environment FOR UPDATE;
  IF NEW.security_epoch<>epoch OR NEW.instance_epoch<>(SELECT security_epoch FROM moria.instance_state)
    OR NEW.environment IS DISTINCT FROM NEW.context->>'environment' THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  IF NEW.context ? 'serviceAuthority' THEN
    IF NEW.context ? 'sessionHash' OR NEW.context->>'serviceTargetRevision' IS NULL THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
    state:=moria.runtime_service_target_authority((NEW.context->>'serviceAuthority')::uuid,NEW.environment,NEW.resource,
      NEW.spec->>'domain',NEW.context->>'serviceTargetRevision',NEW.spec->>'resourceClass');
    IF ROW(state->>'environment',state->>'principal',state->>'application')
      IS DISTINCT FROM ROW(NEW.environment,NEW.context->>'principal',NEW.context->>'application')
      OR state->>'securityEpoch' IS DISTINCT FROM NEW.security_epoch::text
      OR state->>'instanceEpoch' IS DISTINCT FROM NEW.instance_epoch::text
      OR NOT to_jsonb(NEW.markings)<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
    NEW.security_epoch:=moria.runtime_service_dataset_edges((NEW.context->>'serviceAuthority')::uuid,NEW.environment,NEW.resource,
      NEW.spec->>'domain',NEW.context->>'serviceTargetRevision',NEW.spec->>'resourceClass',sources::uuid[],targets::uuid[],NEW.markings);
    RETURN NEW;
  END IF;
  state:=moria.native_serving_session_origin(NEW.context->>'sessionHash',false);
  IF state->>'environment' IS DISTINCT FROM NEW.environment OR state->>'principal' IS DISTINCT FROM NEW.context->>'principal'
    OR state->>'application' IS DISTINCT FROM NEW.context->>'application' THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  NEW.security_epoch:=moria.runtime_data_admit(NEW.context->>'sessionHash',targets,sources,'write');
  FOREACH target_name IN ARRAY targets LOOP
    IF NOT moria.runtime_data_labels(NEW.environment,target_name)<@NEW.markings THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  END LOOP;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_binding_data_admit BEFORE INSERT ON moria.runtime_execution_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_binding_data_admit();
CREATE FUNCTION moria.runtime_snapshot_data_assert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source text;
BEGIN
  FOR source IN SELECT jsonb_object_keys(NEW.lineage) LOOP
    PERFORM moria.runtime_data_assert(NEW.environment,NEW.dataset,source);
  END LOOP;
  RETURN NEW;
END $$;
CREATE TRIGGER snapshot_history_data_assert BEFORE INSERT ON moria.snapshot_history
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_snapshot_data_assert();
CREATE FUNCTION moria.runtime_index_data_assert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_name text;
BEGIN
  SELECT object_type INTO target_name FROM moria.source_stages WHERE id=NEW.stage;
  PERFORM moria.runtime_data_assert(NEW.environment,target_name,NEW.source_dataset);
  RETURN NEW;
END $$;
CREATE TRIGGER index_provenance_data_assert BEFORE INSERT ON moria.index_provenance
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_index_data_assert();
CREATE FUNCTION moria.runtime_search_projector_admit(projector_id uuid,service_id uuid) RETURNS bigint
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE p moria.native_search_projector_bindings; state jsonb; model jsonb; collection_definition jsonb;
  labels text[]:='{}'; incoming jsonb; source_name text; class_name text; env_name text;
BEGIN
  SELECT environment INTO env_name FROM moria.native_search_projector_bindings WHERE id=projector_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'FULL_SOURCE_PURPOSE_REVOKED'; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/environment/'||env_name,0));
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/filesystem',0));
  PERFORM 1 FROM moria.environments WHERE id=env_name FOR UPDATE;
  SELECT binding.* INTO p FROM moria.native_search_projector_bindings binding
    JOIN moria.native_search_collections c ON c.environment=binding.environment AND c.resource=binding.collection
      AND c.id=binding.collection_id AND c.projector_binding=binding.id AND c.enabled
    WHERE binding.id=projector_id AND binding.enabled AND binding.service_id=runtime_search_projector_admit.service_id
      AND binding.domain='search' AND binding.purpose='full-object-projection';
  IF NOT FOUND THEN RAISE EXCEPTION 'FULL_SOURCE_PURPOSE_REVOKED'; END IF;
  SELECT resource_class INTO class_name FROM moria.runtime_service_targets WHERE authority=p.service_id AND environment=p.environment
    AND resource=p.collection_id AND domain='search' AND binding_revision=p.service_binding_revision AND enabled;
  IF NOT FOUND THEN RAISE EXCEPTION 'SERVICE_PURPOSE_REVOKED'; END IF;
  state:=moria.runtime_service_target_authority(p.service_id,p.environment,p.collection_id,'search',p.service_binding_revision,class_name);
  incoming:=moria.runtime_service_resource_labels(p.service_id,p.environment,p.collection_id,'search',p.service_binding_revision,class_name,p.collection,'project');
  labels:=labels||ARRAY(SELECT jsonb_array_elements_text(incoming));
  FOREACH source_name IN ARRAY ARRAY[p.source_resource,p.source_dataset,p.model_resource] LOOP
    incoming:=moria.runtime_service_resource_labels(p.service_id,p.environment,p.collection_id,'search',p.service_binding_revision,class_name,source_name,
      CASE WHEN source_name=p.model_resource THEN 'invoke' ELSE 'read' END);
    labels:=labels||ARRAY(SELECT jsonb_array_elements_text(incoming));
  END LOOP;
  PERFORM moria.runtime_dataset_kind(p.environment,p.source_dataset);
  SELECT definition INTO collection_definition FROM moria.native_search_collections WHERE environment=p.environment AND resource=p.collection;
  IF ROW(collection_definition->>'source',collection_definition->>'textProperty',collection_definition->>'embeddingModel')
    IS DISTINCT FROM ROW(p.source_resource,p.text_property,p.model_resource) THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  labels:=labels||ARRAY(SELECT jsonb_array_elements_text(COALESCE(collection_definition->'markings','[]')));
  SELECT definition INTO model FROM moria.native_model_bindings WHERE environment=p.environment AND resource=p.model_resource AND enabled;
  IF NOT FOUND OR model->>'destination' IS DISTINCT FROM p.model_destination THEN RAISE EXCEPTION 'DESTINATION_FORBIDDEN'; END IF;
  labels:=labels||ARRAY(SELECT jsonb_array_elements_text(COALESCE(model->'markings','[]')));
  labels:=labels||COALESCE((SELECT required_markings FROM moria.source_generations WHERE environment=p.environment AND object_type=p.source_resource ORDER BY revision DESC LIMIT 1),'{}');
  labels:=labels||COALESCE((SELECT ARRAY(SELECT jsonb_array_elements_text(COALESCE(definition->'policies'->'markings','[]'))) FROM moria.native_serving_bindings WHERE environment=p.environment AND resource=p.source_resource AND enabled),'{}');
  IF NOT labels<@p.markings OR NOT to_jsonb(p.markings)<@(state->'identity'->'markings') THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  IF (model->'destinationPolicy'->'allowedDestinations' ? p.model_destination) IS DISTINCT FROM true
    OR NOT to_jsonb(p.markings)<@COALESCE(model->'destinationPolicy'->'allowedMarkings','[]') THEN RAISE EXCEPTION 'DESTINATION_FORBIDDEN'; END IF;
  FOREACH source_name IN ARRAY ARRAY[p.source_resource,p.source_dataset,p.model_resource] LOOP
    IF moria.runtime_data_resource(p.environment,p.collection)<>moria.runtime_data_resource(p.environment,source_name) THEN
      INSERT INTO moria.resource_data_edges VALUES(p.environment,moria.runtime_data_resource(p.environment,p.collection),moria.runtime_data_resource(p.environment,source_name)) ON CONFLICT DO NOTHING;
    END IF;
  END LOOP;
  RETURN (SELECT security_epoch FROM moria.environments WHERE id=p.environment);
END $$;
`;
