/** Operator-owned compiler type contexts and immutable verified source releases. */
export const sourceRuntimeSchema = String.raw`
CREATE TABLE moria.native_dataset_bindings (
  environment text NOT NULL REFERENCES moria.environments(id), resource uuid NOT NULL,
  reference text NOT NULL, revision text NOT NULL, columns jsonb NOT NULL,
  primary_key text[] NOT NULL DEFAULT '{}',key_schema_hash text NOT NULL DEFAULT 'sha256:5f09c244fefaefc0a81cf6d5fe37368d7119dd621906a94e2b1f7e2918d13758',write_modes text[] NOT NULL DEFAULT ARRAY['replace']::text[],
  iceberg_schema jsonb NOT NULL, warehouse_id text NOT NULL, table_id text NOT NULL,
  namespace_revision text NOT NULL, physical_binding_fingerprint text NOT NULL,
  source_digest text NOT NULL REFERENCES moria.fs_source_releases(digest),
  enabled boolean NOT NULL DEFAULT true, PRIMARY KEY(environment,resource)
);
CREATE FUNCTION moria.native_dataset_binding_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_DATASET_BINDING'; END IF;
  IF NEW.revision !~ '^sha256:[0-9a-f]{64}$' OR NEW.physical_binding_fingerprint !~ '^sha256:[0-9a-f]{64}$'
    OR jsonb_typeof(NEW.columns)<>'object' OR jsonb_typeof(NEW.iceberg_schema)<>'object'
    OR octet_length(NEW.columns::text)>1048576 OR octet_length(NEW.iceberg_schema::text)>1048576
    OR NEW.key_schema_hash !~ '^sha256:[0-9a-f]{64}$' OR cardinality(NEW.primary_key)>256 OR cardinality(NEW.primary_key)<>(SELECT count(DISTINCT key) FROM unnest(NEW.primary_key) key)
    OR EXISTS(SELECT 1 FROM unnest(NEW.primary_key) key WHERE key IS NULL OR key='' OR NOT(NEW.columns ? key) OR NEW.columns->key->'nullable' IS DISTINCT FROM 'false'::jsonb)
    OR (cardinality(NEW.primary_key)=0 AND NEW.write_modes && ARRAY['upsert','delete']::text[])
    OR cardinality(NEW.write_modes) NOT BETWEEN 1 AND 4 OR NOT(NEW.write_modes<@ARRAY['replace','append','upsert','delete']::text[])
    OR cardinality(NEW.write_modes)<>(SELECT count(DISTINCT mode) FROM unnest(NEW.write_modes) mode) THEN
    RAISE EXCEPTION 'DATASET_BINDING_INVALID';
  END IF;
  IF TG_OP='UPDATE' THEN
    IF ROW(NEW.environment,NEW.resource,NEW.reference,NEW.columns,NEW.primary_key,NEW.key_schema_hash,NEW.iceberg_schema,
      NEW.warehouse_id,NEW.table_id)
      IS DISTINCT FROM ROW(OLD.environment,OLD.resource,OLD.reference,OLD.columns,OLD.primary_key,OLD.key_schema_hash,OLD.iceberg_schema,
      OLD.warehouse_id,OLD.table_id) THEN RAISE EXCEPTION 'DATASET_SCHEMA_EVOLUTION_REQUIRED'; END IF;
    IF NEW.physical_binding_fingerprint IS DISTINCT FROM OLD.physical_binding_fingerprint THEN
      IF NEW.namespace_revision IS DISTINCT FROM OLD.namespace_revision THEN
        RAISE EXCEPTION 'IMMUTABLE_DATASET_BINDING';
      END IF;
      PERFORM moria.runtime_restore_dataset_rebind_check(OLD.environment,OLD.resource,OLD.warehouse_id,OLD.table_id,
        OLD.namespace_revision,OLD.physical_binding_fingerprint,NEW.physical_binding_fingerprint);
    END IF;
    IF NEW.revision=OLD.revision AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'FRESH_DATASET_REVISION_REQUIRED'; END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
  END IF;
  UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  RETURN NEW;
END $$;
CREATE TRIGGER native_dataset_binding_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_dataset_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.native_dataset_binding_guard();
ALTER TABLE moria.fs_release_plans ADD COLUMN runtime_plan jsonb;
CREATE TABLE moria.native_source_runtime_configurations (
  root uuid NOT NULL REFERENCES moria.fs_roles(entry_id),environment text NOT NULL REFERENCES moria.environments(id),
  release text NOT NULL REFERENCES moria.fs_source_releases(digest),profile_fingerprint text NOT NULL,
  binding_fingerprint text NOT NULL,PRIMARY KEY(root,environment)
);
CREATE TABLE moria.native_action_identity_domains (
  domain text PRIMARY KEY REFERENCES moria.runtime_identity_domains(domain),target uuid NOT NULL UNIQUE
);
CREATE TABLE moria.native_link_edge_versions (
  environment text NOT NULL REFERENCES moria.environments(id),link text NOT NULL,
  source_uid uuid NOT NULL REFERENCES moria.object_homes(uid),target_uid uuid NOT NULL REFERENCES moria.object_homes(uid),
  revision bigint NOT NULL,linked boolean NOT NULL,PRIMARY KEY(environment,link,source_uid,target_uid,revision),
  FOREIGN KEY(environment,link) REFERENCES moria.native_serving_links(environment,resource)
);
CREATE INDEX native_link_edge_source ON moria.native_link_edge_versions(environment,link,source_uid,target_uid,revision DESC);
CREATE INDEX native_link_edge_target ON moria.native_link_edge_versions(environment,link,target_uid,source_uid,revision DESC);
CREATE FUNCTION moria.runtime_link_edges_at(env text,resource text,at_revision bigint)
 RETURNS TABLE(source_uid uuid,target_uid uuid) LANGUAGE sql STABLE AS $$
 SELECT source_uid,target_uid FROM (SELECT DISTINCT ON(source_uid,target_uid) source_uid,target_uid,linked
   FROM moria.native_link_edge_versions WHERE environment=env AND link=resource AND revision<=at_revision ORDER BY source_uid,target_uid,revision DESC) e WHERE linked
$$;
CREATE TABLE moria.native_object_creations (
  environment text NOT NULL REFERENCES moria.environments(id),object_type text NOT NULL,
  uid uuid NOT NULL REFERENCES moria.object_homes(uid),created_revision bigint NOT NULL,
  source_value jsonb NOT NULL CHECK(jsonb_typeof(source_value)='object'),
  mapped_base jsonb NOT NULL CHECK(jsonb_typeof(mapped_base)='object'),
  action_scope_hash text NOT NULL CHECK(action_scope_hash ~ '^[0-9a-f]{64}$'),
  markings text[] NOT NULL,PRIMARY KEY(environment,object_type,uid)
);
CREATE TABLE moria.native_object_source_attachments (
  environment text NOT NULL,object_type text NOT NULL,uid uuid NOT NULL,source_dataset text NOT NULL,
  revision bigint NOT NULL,application text NOT NULL,principal text NOT NULL,
  PRIMARY KEY(environment,object_type,uid),
  FOREIGN KEY(environment,object_type,uid) REFERENCES moria.native_object_creations(environment,object_type,uid)
);
CREATE TABLE moria.native_object_attachment_receipts (
 environment text NOT NULL,application text NOT NULL,principal text NOT NULL,key text NOT NULL,
 fingerprint text NOT NULL,result jsonb NOT NULL,expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '30 days',
 PRIMARY KEY(environment,application,principal,key)
);
CREATE FUNCTION moria.native_object_creation_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_OBJECT_CREATION'; END $$;
CREATE TRIGGER native_action_identity_domain_immutable BEFORE UPDATE OR DELETE ON moria.native_action_identity_domains
  FOR EACH ROW EXECUTE FUNCTION moria.native_object_creation_immutable();
CREATE TRIGGER native_object_creation_immutable BEFORE UPDATE OR DELETE ON moria.native_object_creations
  FOR EACH ROW EXECUTE FUNCTION moria.native_object_creation_immutable();
CREATE TRIGGER native_object_attachment_immutable BEFORE UPDATE OR DELETE ON moria.native_object_source_attachments
  FOR EACH ROW EXECUTE FUNCTION moria.native_object_creation_immutable();
CREATE FUNCTION moria.runtime_object_creations_at(env text,target text,at_revision bigint)
  RETURNS TABLE(uid uuid,value jsonb,source_value jsonb,action_created boolean) LANGUAGE sql STABLE AS $$
  SELECT c.uid,c.mapped_base,c.source_value,true FROM moria.native_object_creations c
    WHERE c.environment=env AND c.object_type=target AND c.created_revision<=at_revision
$$;
CREATE FUNCTION moria.runtime_object_base_stage(stage_id uuid,env text,target text,at_revision bigint)
  RETURNS TABLE(uid uuid,value jsonb,source_value jsonb,action_created boolean) LANGUAGE sql STABLE AS $$
  SELECT r.uid,r.value,COALESCE(r.source_value,r.value),false FROM moria.staged_source_rows r WHERE r.stage=stage_id
  UNION ALL SELECT c.uid,c.value,c.source_value,c.action_created FROM moria.runtime_object_creations_at(env,target,at_revision) c
    WHERE NOT EXISTS(SELECT 1 FROM moria.staged_source_rows r WHERE r.stage=stage_id AND r.uid=c.uid)
$$;
CREATE FUNCTION moria.runtime_object_base(env text,target text,at_revision bigint)
  RETURNS TABLE(uid uuid,value jsonb,source_value jsonb,action_created boolean) LANGUAGE sql STABLE AS $$
  WITH generation AS NOT MATERIALIZED(SELECT g.revision,g.stage FROM moria.source_generations g
    WHERE g.environment=env AND g.object_type=target AND g.revision<=at_revision ORDER BY g.revision DESC LIMIT 1),
  actual AS NOT MATERIALIZED(
    SELECT r.uid,r.value,COALESCE(r.source_value,r.value) AS source_value FROM generation g
      JOIN moria.staged_source_rows r ON r.stage=g.stage
    UNION ALL SELECT r.uid,r.value,r.value FROM generation g JOIN moria.source_rows r
      ON r.environment=env AND r.object_type=target AND r.revision=g.revision WHERE g.stage IS NULL)
  SELECT a.uid,a.value,a.source_value,false FROM actual a
  UNION ALL SELECT c.uid,c.value,c.source_value,c.action_created FROM moria.runtime_object_creations_at(env,target,at_revision) c
    WHERE NOT EXISTS(SELECT 1 FROM actual a WHERE a.uid=c.uid)
$$;
CREATE FUNCTION moria.runtime_object_import_check(env text,target text,ids uuid[],source_dataset text)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE canonical_target text;canonical_source text;
BEGIN
  IF cardinality(ids) IS NULL OR cardinality(ids)>10000 OR array_position(ids,NULL) IS NOT NULL THEN RAISE EXCEPTION 'IDENTITY_REQUEST_INVALID'; END IF;
  canonical_target:=moria.runtime_data_resource(env,target);
  canonical_source:=moria.runtime_data_resource(env,source_dataset);
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/object-population/'||env||'/'||canonical_target,0));
  IF EXISTS(SELECT 1 FROM moria.native_object_creations c WHERE c.environment=env AND c.object_type=canonical_target AND c.uid=ANY(ids)
    AND NOT EXISTS(SELECT 1 FROM moria.native_object_source_attachments a WHERE a.environment=c.environment
      AND a.object_type=c.object_type AND a.uid=c.uid AND a.source_dataset=canonical_source)) THEN RAISE EXCEPTION 'ACTION_SOURCE_ATTACHMENT_REQUIRED'; END IF;
END $$;
ALTER TABLE moria.native_filesystem_uploads ADD COLUMN compiler_source jsonb;
CREATE TABLE moria.native_compiler_artifacts (
  artifact uuid PRIMARY KEY REFERENCES moria.fs_entries(id),root uuid NOT NULL REFERENCES moria.fs_roles(entry_id),source_digest text NOT NULL
);
CREATE TABLE moria.native_compiler_artifact_sources (
  artifact uuid NOT NULL REFERENCES moria.fs_entries(id), source uuid NOT NULL REFERENCES moria.fs_entries(id),
  root uuid NOT NULL REFERENCES moria.fs_roles(entry_id), operation text NOT NULL CHECK(operation IN ('read','export')),
  PRIMARY KEY(artifact,source,root,operation),CHECK(artifact<>source)
);
ALTER FUNCTION moria.native_filesystem_access(text,uuid,text,boolean) RENAME TO native_filesystem_access_base;
REVOKE ALL ON FUNCTION moria.native_filesystem_access_base(text,uuid,text,boolean) FROM PUBLIC;
CREATE FUNCTION moria.native_compiler_proof_access(token_hash text, proof jsonb) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE item jsonb; labels jsonb:='[]'; targets jsonb;
BEGIN
  IF jsonb_typeof(proof->'sources') IS DISTINCT FROM 'array' OR jsonb_array_length(proof->'sources') NOT BETWEEN 1 AND 10000 THEN RAISE EXCEPTION 'SOURCE_INVALID'; END IF;
  WITH RECURSIVE nodes AS (
    SELECT (value->>'id')::uuid AS id FROM jsonb_array_elements(proof->'sources')
    UNION SELECT p.source_id FROM nodes n JOIN moria.fs_provenance p ON p.entry_id=n.id
  ), entries AS (
    SELECT value FROM jsonb_array_elements(proof->'sources')
    UNION SELECT jsonb_build_object('id',s.source,'rootId',s.root,'operation',s.operation) FROM nodes n JOIN moria.native_compiler_artifact_sources s ON s.artifact=n.id
  ) SELECT jsonb_agg(value) INTO targets FROM entries;
  IF jsonb_array_length(targets)>10000 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  FOR item IN SELECT value FROM jsonb_array_elements(targets) LOOP
    IF NOT EXISTS(SELECT 1 FROM moria.fs_roles WHERE entry_id=(item->>'rootId')::uuid AND kind IN ('ontology','repository') AND NOT retired) THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
    IF item->>'operation' NOT IN ('read','export') THEN RAISE EXCEPTION 'SOURCE_INVALID'; END IF;
    labels:=labels||moria.native_filesystem_access_base(token_hash,(item->>'id')::uuid,item->>'operation',false);
    labels:=COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(labels)),'[]');
    IF jsonb_array_length(labels)>256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  END LOOP;
  RETURN COALESCE((SELECT jsonb_agg(DISTINCT value) FROM jsonb_array_elements(labels)),'[]');
END $$;
REVOKE ALL ON FUNCTION moria.native_compiler_proof_access(text,jsonb) FROM PUBLIC;
CREATE FUNCTION moria.native_compiler_artifact_access(token_hash text, target uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE proof jsonb;
BEGIN
  WITH RECURSIVE nodes AS (
    SELECT target AS id UNION SELECT p.source_id FROM nodes n JOIN moria.fs_provenance p ON p.entry_id=n.id
  ) SELECT jsonb_build_object('sources',COALESCE(jsonb_agg(DISTINCT jsonb_build_object('id',s.source,'rootId',s.root,'operation',s.operation)),'[]')) INTO proof
    FROM nodes n JOIN moria.native_compiler_artifact_sources s ON s.artifact=n.id;
  IF jsonb_array_length(proof->'sources')>0 THEN PERFORM moria.native_compiler_proof_access(token_hash,proof); END IF;
END $$;
CREATE FUNCTION moria.native_compiler_upload_access(token_hash text, upload uuid, worker uuid, worker_fence bigint) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE candidate moria.native_filesystem_uploads; state jsonb;
BEGIN
  state:=moria.native_serving_session(token_hash);
  SELECT u.* INTO candidate FROM moria.native_filesystem_uploads u WHERE u.id=upload AND u.owner=worker AND u.fence=worker_fence AND u.lease_until>clock_timestamp() AND u.state IN ('writing','sealed') AND u.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND NOT EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones t WHERE t.upload=u.id);
  IF NOT FOUND OR candidate.compiler_source IS NULL OR ROW(candidate.environment,candidate.principal,candidate.application) IS DISTINCT FROM ROW(state->>'environment',state->>'principal',state->>'application') THEN RAISE EXCEPTION 'FILESYSTEM_LEASE_INVALID'; END IF;
  RETURN moria.native_compiler_proof_access(token_hash,candidate.compiler_source);
END $$;
CREATE FUNCTION moria.native_filesystem_access(token_hash text,target_id uuid,operation text,include_inactive boolean DEFAULT false) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE labels jsonb;
BEGIN
  labels:=moria.native_filesystem_access_base(token_hash,target_id,operation,include_inactive);
  PERFORM moria.native_compiler_artifact_access(token_hash,target_id);
  RETURN labels;
END $$;
CREATE TABLE moria.native_source_compiler_contexts (
  environment text NOT NULL REFERENCES moria.environments(id), fingerprint text NOT NULL,
  principal_attributes jsonb NOT NULL CHECK(jsonb_typeof(principal_attributes)='object'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(environment,fingerprint)
);
CREATE TABLE moria.native_source_compiler_current (
  environment text PRIMARY KEY REFERENCES moria.environments(id), fingerprint text NOT NULL,
  FOREIGN KEY(environment,fingerprint) REFERENCES moria.native_source_compiler_contexts(environment,fingerprint)
);
CREATE TABLE moria.native_source_compiler_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, environment text NOT NULL,
  principal text NOT NULL, application text NOT NULL, prior_fingerprint text, fingerprint text NOT NULL,
  epoch bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION moria.native_source_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ARTIFACT'; END $$;
CREATE TRIGGER native_source_compiler_context_immutable BEFORE UPDATE OR DELETE ON moria.native_source_compiler_contexts
  FOR EACH ROW EXECUTE FUNCTION moria.native_source_immutable();
CREATE TRIGGER native_source_release_immutable BEFORE UPDATE OR DELETE ON moria.fs_source_releases
  FOR EACH ROW EXECUTE FUNCTION moria.native_source_immutable();
CREATE TRIGGER native_compiler_artifact_sources_immutable BEFORE UPDATE OR DELETE ON moria.native_compiler_artifact_sources
  FOR EACH ROW EXECUTE FUNCTION moria.native_source_immutable();
CREATE TRIGGER native_compiler_artifacts_immutable BEFORE UPDATE OR DELETE ON moria.native_compiler_artifacts
  FOR EACH ROW EXECUTE FUNCTION moria.native_source_immutable();
CREATE FUNCTION moria.native_compiler_upload_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.compiler_source IS DISTINCT FROM OLD.compiler_source THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ARTIFACT'; END IF;RETURN NEW;END $$;
CREATE TRIGGER native_compiler_upload_immutable BEFORE UPDATE ON moria.native_filesystem_uploads
  FOR EACH ROW EXECUTE FUNCTION moria.native_compiler_upload_immutable();
`;
