/** Shared publication and distributed-indexing authority. Runtime roles never own these functions. */
export const indexingSchema = String.raw`
CREATE TABLE moria.indexing_instance (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), incarnation uuid NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO moria.indexing_instance DEFAULT VALUES;
CREATE TABLE moria.indexing_environments (
  environment text PRIMARY KEY REFERENCES moria.environments(id), id uuid NOT NULL UNIQUE DEFAULT gen_random_uuid()
);
CREATE TABLE moria.indexing_bindings (
  environment text NOT NULL REFERENCES moria.environments(id), target text NOT NULL, target_id uuid NOT NULL,
  source text NOT NULL, revision text NOT NULL, definition jsonb NOT NULL, context jsonb NOT NULL,
  security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL, enabled boolean NOT NULL DEFAULT true,
  PRIMARY KEY(environment,target), UNIQUE(environment,target_id)
);
CREATE TABLE moria.indexing_builds (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), environment text NOT NULL, target text NOT NULL,
  binding_revision text NOT NULL, definition jsonb NOT NULL, context jsonb NOT NULL,
  security_epoch bigint NOT NULL, instance_epoch bigint NOT NULL,
  source text NOT NULL, source_revision bigint NOT NULL, metadata_location text NOT NULL, snapshot_id text NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','planning','running','preparing','ready','active','failed','cancelled','superseded','conflicted')),
  owner uuid, fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), deadline timestamptz NOT NULL,
  error text, receipt jsonb, planned boolean NOT NULL DEFAULT false,
  FOREIGN KEY(environment,target) REFERENCES moria.indexing_bindings(environment,target),
  FOREIGN KEY(environment,source,source_revision) REFERENCES moria.snapshot_history(environment,dataset,revision),
  UNIQUE(environment,target,binding_revision,source_revision)
);
CREATE INDEX indexing_pending_builds ON moria.indexing_builds(created_at) WHERE status IN ('pending','planning','running','preparing','ready');
CREATE TABLE moria.indexing_tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), build uuid NOT NULL REFERENCES moria.indexing_builds(id),
  ordinal integer NOT NULL, plan jsonb NOT NULL, wire bytea, status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','sealed')),
  owner uuid, fence bigint NOT NULL DEFAULT 0, lease_until timestamptz,
  row_count bigint, digest text, UNIQUE(build,ordinal)
);
CREATE INDEX indexing_pending_tasks ON moria.indexing_tasks(build,ordinal) WHERE status<>'sealed';
CREATE TABLE moria.indexing_attempts (
  build uuid NOT NULL REFERENCES moria.indexing_builds(id), kind text NOT NULL CHECK(kind IN ('coordinator','task')),
  unit uuid NOT NULL, fence bigint NOT NULL, owner uuid NOT NULL,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  outcome text NOT NULL DEFAULT 'running', PRIMARY KEY(build,kind,unit,fence)
);
CREATE TABLE moria.indexing_rows (
  task uuid NOT NULL REFERENCES moria.indexing_tasks(id), fence bigint NOT NULL,
  uid uuid NOT NULL, partition integer NOT NULL CHECK(partition>=0), value jsonb NOT NULL,
  PRIMARY KEY(task,fence,uid)
);
CREATE INDEX indexing_rows_partition ON moria.indexing_rows(partition,uid,task,fence);
CREATE TABLE moria.indexing_changes (
  environment text NOT NULL REFERENCES moria.environments(id), revision bigint NOT NULL,
  ordinal integer NOT NULL, target text NOT NULL, generation uuid NOT NULL,
  transaction_end boolean NOT NULL, PRIMARY KEY(environment,revision,ordinal)
);

CREATE FUNCTION moria.indexing_check(build_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE b moria.indexing_builds; ctx jsonb; p record; valid boolean;
BEGIN
  SELECT * INTO b FROM moria.indexing_builds WHERE id=build_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'BUILD_UNKNOWN'; END IF;
  IF b.status IN ('failed','cancelled','superseded','conflicted') OR (b.status<>'active' AND b.deadline<=clock_timestamp()) THEN
    RAISE EXCEPTION 'BUILD_INACTIVE';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.environments e,moria.instance_state i
    WHERE e.id=b.environment AND e.security_epoch=b.security_epoch AND i.security_epoch=b.instance_epoch) THEN
    RAISE EXCEPTION 'AUTHORIZATION_CHANGED';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.indexing_instance i,moria.indexing_environments e
    WHERE i.incarnation::text=b.definition->>'incarnation' AND e.environment=b.environment
      AND e.id::text=b.definition->>'environmentId') THEN RAISE EXCEPTION 'INCARNATION_CHANGED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.indexing_bindings x WHERE x.environment=b.environment AND x.target=b.target
    AND x.enabled AND x.revision=b.binding_revision AND x.definition=b.definition AND x.context=b.context) THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  ctx:=b.context;
  SELECT a.enabled AS app_enabled,x.enabled,x.kind,x.synced_at,
    (ib.instance_id IS NULL OR (ip.enabled AND ip.kind=x.kind)) AS instance_enabled,
    x.markings,a.operations AS scopes,g.operations AS grants INTO p
    FROM moria.principals x JOIN moria.application_scopes a ON a.environment=x.environment AND a.application=ctx->>'application'
    JOIN moria.permissions g ON g.environment=x.environment AND g.application=a.application AND g.principal=x.principal
    LEFT JOIN moria.instance_principal_bindings ib ON ib.environment=x.environment AND ib.principal=x.principal
    LEFT JOIN moria.instance_principals ip ON ip.id=ib.instance_id
    WHERE x.environment=b.environment AND x.principal=ctx->>'principal';
  IF NOT FOUND OR NOT p.enabled OR NOT p.app_enabled OR NOT p.instance_enabled
    OR (p.kind='human' AND p.synced_at<=clock_timestamp()-interval '60 seconds') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  IF NOT ARRAY[b.source||':read',b.target||':index'] <@ p.scopes
    OR NOT ARRAY[b.source||':read',b.target||':index'] <@ p.grants
    OR NOT ARRAY(SELECT jsonb_array_elements_text(b.definition->'markings')) <@ p.markings THEN
    RAISE EXCEPTION 'AUTHORIZATION_CHANGED';
  END IF;
  IF ctx->>'sessionHash' IS NOT NULL THEN
    SELECT EXISTS(SELECT 1 FROM moria.workload_sessions s WHERE s.token_hash=ctx->>'sessionHash'
      AND s.environment=b.environment AND s.principal=ctx->>'principal' AND s.application=ctx->>'application'
      AND NOT s.disabled AND s.expires_at>clock_timestamp() AND p.kind='workload'
      UNION ALL SELECT 1 FROM moria.browser_sessions s LEFT JOIN moria.auth_providers a
      ON a.environment=s.environment AND a.application=s.application AND a.provider=s.provider
      WHERE s.token_hash=ctx->>'sessionHash' AND s.environment=b.environment AND s.principal=ctx->>'principal'
      AND s.application=ctx->>'application' AND NOT s.disabled AND s.expires_at>clock_timestamp()
      AND ((s.provider IS NULL AND p.kind='workload') OR (s.provider IS NOT NULL AND p.kind='human'
      AND a.enabled AND a.revision=s.revision AND s.verified_at>clock_timestamp()-interval '60 seconds'))) INTO valid;
    IF NOT valid THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  END IF;
END $$;

CREATE FUNCTION moria.indexing_enqueue(env text, dataset text, source_rev bigint, expected_rev bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE x moria.indexing_bindings; h moria.snapshot_history; build_id uuid; base bigint;
BEGIN
  SELECT * INTO h FROM moria.snapshot_history WHERE environment=env AND snapshot_history.dataset=indexing_enqueue.dataset AND revision=source_rev;
  IF NOT FOUND THEN RAISE EXCEPTION 'SOURCE_UNPUBLISHED'; END IF;
  FOR x IN SELECT * FROM moria.indexing_bindings WHERE environment=env AND source=dataset AND enabled LOOP
    SELECT id INTO build_id FROM moria.indexing_builds WHERE environment=env AND target=x.target AND binding_revision=x.revision AND source_revision=source_rev;
    IF FOUND THEN CONTINUE; END IF;
    SELECT max(revision) INTO base FROM moria.source_generations WHERE environment=env AND object_type=x.target;
    INSERT INTO moria.indexing_builds(environment,target,binding_revision,definition,context,security_epoch,instance_epoch,
      source,source_revision,metadata_location,snapshot_id,deadline)
      VALUES(env,x.target,x.revision,x.definition,x.context,x.security_epoch,x.instance_epoch,dataset,source_rev,h.metadata_location,h.snapshot_id,
      clock_timestamp()+(x.definition->>'timeoutSeconds')::integer*interval '1 second') RETURNING id INTO build_id;
    INSERT INTO moria.source_stages(id,environment,object_type,expected_revision,security_epoch,status,object_schema,base_source_revision,required_markings)
      VALUES(build_id,env,x.target,expected_rev,x.security_epoch,'loading',x.definition->'schema',base,ARRAY(SELECT jsonb_array_elements_text(x.definition->'markings')));
    INSERT INTO moria.index_provenance VALUES(build_id,env,x.context->>'principal',x.context->>'application',x.revision,dataset,source_rev,h.metadata_location,h.snapshot_id);
    UPDATE moria.indexing_builds SET status='superseded' WHERE environment=env AND target=x.target AND id<>build_id
      AND status IN ('pending','planning','running','preparing','ready') AND source_revision<source_rev;
  END LOOP;
END $$;

CREATE FUNCTION moria.activate_source_stage(stage_id uuid, admission_revision bigint DEFAULT NULL) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE s moria.source_stages; e moria.environments; h moria.source_generations; next_revision bigint;
BEGIN
  SELECT * INTO s FROM moria.source_stages WHERE id=stage_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'STAGE_INVALID'; END IF;
  SELECT * INTO e FROM moria.environments WHERE id=s.environment FOR UPDATE;
  IF NOT FOUND OR e.security_epoch<>s.security_epoch THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  IF s.status='published' THEN RETURN s.published_revision; END IF;
  IF s.status<>'ready' THEN RAISE EXCEPTION 'STAGE_INVALID'; END IF;
  IF s.lease_binding IS NOT NULL THEN
    PERFORM 1 FROM moria.leases WHERE environment=s.environment AND execution=s.lease_binding->>'execution'
      AND worker=s.lease_binding->>'worker' AND fence=(s.lease_binding->>'fence')::bigint AND expires_at>clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  END IF;
  SELECT * INTO h FROM moria.source_generations WHERE environment=s.environment AND object_type=s.object_type ORDER BY revision DESC LIMIT 1;
  IF h.object_schema IS NOT NULL AND s.object_schema IS NOT NULL AND (
    h.object_schema->>'primaryKey' IS DISTINCT FROM s.object_schema->>'primaryKey'
    OR EXISTS(SELECT 1 FROM jsonb_each(h.object_schema->'properties') p WHERE p.value IS DISTINCT FROM s.object_schema->'properties'->p.key)
    OR EXISTS(SELECT 1 FROM jsonb_each(s.object_schema->'properties') p WHERE NOT (h.object_schema->'properties' ? p.key) AND p.value->'nullable' IS DISTINCT FROM 'true'::jsonb)
  ) THEN RAISE EXCEPTION 'MIGRATION_REQUIRED'; END IF;
  IF NOT COALESCE(h.required_markings,'{}') <@ COALESCE(s.required_markings,'{}') THEN RAISE EXCEPTION 'MARKING_DOWNGRADE'; END IF;
  IF COALESCE(admission_revision,e.revision)<>s.expected_revision AND
    (h.object_schema IS NULL OR s.object_schema IS NULL OR h.revision IS DISTINCT FROM s.base_source_revision OR h.object_schema<>s.object_schema) THEN
    RAISE EXCEPTION 'HEAD_CONFLICT';
  END IF;
  UPDATE moria.environments SET revision=revision+1 WHERE id=s.environment RETURNING revision INTO next_revision;
  INSERT INTO moria.source_generations(environment,object_type,revision,stage,object_schema,required_markings)
    VALUES(s.environment,s.object_type,next_revision,s.id,s.object_schema,s.required_markings);
  UPDATE moria.source_stages SET status='published',published_revision=next_revision WHERE id=s.id;
  RETURN next_revision;
END $$;

CREATE FUNCTION moria.indexing_activate(build_id uuid, worker_id uuid, worker_fence bigint) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE b moria.indexing_builds; r bigint; result jsonb;
BEGIN
  SELECT * INTO b FROM moria.indexing_builds WHERE id=build_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'BUILD_UNKNOWN'; END IF;
  PERFORM moria.indexing_check(build_id);
  IF b.status='active' THEN RETURN b.receipt; END IF;
  IF b.status<>'ready' OR b.owner IS DISTINCT FROM worker_id OR b.fence<>worker_fence OR b.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  IF EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE build=build_id AND status<>'sealed') THEN RAISE EXCEPTION 'STAGE_INVALID'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.dataset_heads WHERE environment=b.environment AND dataset=b.source AND revision=b.source_revision) THEN RAISE EXCEPTION 'HEAD_CONFLICT'; END IF;
  r:=moria.activate_source_stage(build_id);
  result:=jsonb_build_object('stage',build_id,'revision',r::text,'sourceRevision',b.source_revision::text);
  INSERT INTO moria.indexing_changes VALUES(b.environment,r,0,b.target,build_id,true);
  UPDATE moria.indexing_builds SET status='active',receipt=result,lease_until=NULL WHERE id=build_id;
  RETURN result;
END $$;
`;
