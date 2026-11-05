/** Source delta windows remain disabled until the qualified publisher and complete writer order are installed. */
export const datasetDeltaSchema = String.raw`
CREATE TABLE moria.native_source_delta_windows(
 id uuid PRIMARY KEY,kind text NOT NULL CHECK(kind IN ('bootstrap','delta')),
 store uuid NOT NULL REFERENCES moria.native_source_row_stores(id) DEFERRABLE INITIALLY DEFERRED,
 source_version bigint NOT NULL CHECK(source_version>0),
 stage uuid NOT NULL REFERENCES moria.indexing_builds(id),owner uuid NOT NULL,fence bigint NOT NULL CHECK(fence>0),incarnation uuid NOT NULL,
 environment text NOT NULL,object_type text NOT NULL,
 profile jsonb NOT NULL,definition jsonb NOT NULL,basis_revision bigint NOT NULL CHECK(basis_revision>=0),basis_source_version bigint NOT NULL CHECK(basis_source_version>=0),
 basis_dataset_revision bigint NOT NULL CHECK(basis_dataset_revision>=0),target_dataset_revision bigint NOT NULL CHECK(target_dataset_revision>0),
 prepared_delta jsonb,preparation_revision bigint CHECK(preparation_revision>=0),publication_revision bigint,
 status text NOT NULL DEFAULT 'preparing' CHECK(status IN ('preparing','ready','publishing','published','failed','abandoned')),
 mutation_transaction xid8,require_alias boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,
 UNIQUE(store,source_version),CHECK((kind='bootstrap')=(source_version=1)),
 CHECK((status='published')=(publication_revision IS NOT NULL AND finished_at IS NOT NULL))
);
CREATE UNIQUE INDEX native_source_one_preparation ON moria.native_source_delta_windows(store) WHERE status IN ('preparing','ready','publishing');
CREATE INDEX native_source_delta_build ON moria.native_source_delta_windows(stage,id);
CREATE TABLE moria.native_source_key_map(
 store uuid NOT NULL REFERENCES moria.native_source_row_stores(id),key_hash bytea NOT NULL CHECK(octet_length(key_hash)=32),
 collision integer NOT NULL CHECK(collision BETWEEN 0 AND 63),encoded_key bytea NOT NULL CHECK(octet_length(encoded_key) BETWEEN 1 AND 4194304),
 uid uuid NOT NULL REFERENCES moria.object_homes(uid),source_version bigint NOT NULL CHECK(source_version>0),
 PRIMARY KEY(store,key_hash,collision),UNIQUE(store,uid)
);
CREATE FUNCTION moria.runtime_source_window_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER native_source_window_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_delta_windows FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_window_owner();
CREATE FUNCTION moria.runtime_source_window_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE live_build moria.indexing_builds;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_WINDOW';END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.status<>'preparing' OR NEW.mutation_transaction IS NOT NULL OR NEW.require_alias OR NEW.prepared_delta IS NOT NULL OR NEW.preparation_revision IS NOT NULL OR NEW.publication_revision IS NOT NULL OR NEW.finished_at IS NOT NULL THEN RAISE EXCEPTION 'SOURCE_DELTA_NOT_READY';END IF;
  RETURN NEW;
 END IF;
 IF ROW(NEW.id,NEW.kind,NEW.store,NEW.source_version,NEW.stage,NEW.incarnation,NEW.environment,NEW.object_type,NEW.profile,NEW.definition,NEW.basis_revision,NEW.basis_source_version,NEW.basis_dataset_revision,NEW.target_dataset_revision,NEW.created_at)
  IS DISTINCT FROM ROW(OLD.id,OLD.kind,OLD.store,OLD.source_version,OLD.stage,OLD.incarnation,OLD.environment,OLD.object_type,OLD.profile,OLD.definition,OLD.basis_revision,OLD.basis_source_version,OLD.basis_dataset_revision,OLD.target_dataset_revision,OLD.created_at)
  OR OLD.status IN ('published','failed','abandoned') AND NEW IS DISTINCT FROM OLD
  OR OLD.require_alias AND NOT NEW.require_alias
  OR OLD.mutation_transaction IS NOT NULL AND NEW.mutation_transaction IS DISTINCT FROM OLD.mutation_transaction
  OR NEW.mutation_transaction IS DISTINCT FROM OLD.mutation_transaction AND NEW.mutation_transaction IS DISTINCT FROM pg_current_xact_id()
  OR OLD.preparation_revision IS NOT NULL AND NEW.preparation_revision IS DISTINCT FROM OLD.preparation_revision
  OR OLD.prepared_delta IS NOT NULL AND NEW.prepared_delta IS DISTINCT FROM OLD.prepared_delta
  OR OLD.publication_revision IS NOT NULL AND NEW.publication_revision IS DISTINCT FROM OLD.publication_revision
  OR OLD.finished_at IS NOT NULL AND NEW.finished_at IS DISTINCT FROM OLD.finished_at
  THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_WINDOW';END IF;
 IF NEW.status IS DISTINCT FROM OLD.status AND NOT (
  OLD.status='preparing' AND NEW.status IN ('ready','failed','abandoned') OR
  OLD.status='ready' AND NEW.status IN ('publishing','failed','abandoned') OR
  OLD.status='publishing' AND NEW.status='published') THEN RAISE EXCEPTION 'SOURCE_DELTA_NOT_READY';END IF;
 IF ROW(NEW.owner,NEW.fence) IS DISTINCT FROM ROW(OLD.owner,OLD.fence) THEN
  SELECT * INTO live_build FROM moria.indexing_builds WHERE id=NEW.stage;
  IF NEW.fence<=OLD.fence OR live_build.owner IS DISTINCT FROM NEW.owner OR live_build.fence<>NEW.fence OR live_build.lease_until IS NULL OR live_build.lease_until<=clock_timestamp() OR live_build.deadline<=clock_timestamp() OR OLD.require_alias THEN RAISE EXCEPTION 'LEASE_LOST';END IF;
 END IF;
 IF NEW.require_alias AND (NEW.mutation_transaction IS DISTINCT FROM pg_current_xact_id() OR NEW.status NOT IN ('preparing','ready','publishing','published')) THEN RAISE EXCEPTION 'SOURCE_DELTA_PUBLICATION_REQUIRED';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_window_immutable BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_delta_windows FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_window_immutable();

CREATE TRIGGER native_source_keys_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_key_map FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_window_owner();
CREATE FUNCTION moria.runtime_source_delta_profile(build_id uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE b moria.indexing_builds;d moria.native_dataset_bindings;s moria.native_serving_bindings;p moria.runtime_projection_bindings;
BEGIN
 SELECT * INTO b FROM moria.indexing_builds WHERE id=build_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'BUILD_UNKNOWN';END IF;
 SELECT * INTO d FROM moria.native_dataset_bindings WHERE environment=b.environment AND resource::text=b.source AND enabled;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_BINDING_INVALID';END IF;
 SELECT * INTO s FROM moria.native_serving_bindings WHERE environment=b.environment AND resource=b.target AND enabled;
 IF NOT FOUND OR s.revision IS DISTINCT FROM b.definition->>'servingBindingRevision' OR s.authority_generation::text IS DISTINCT FROM b.definition->>'servingBindingGeneration' OR s.definition IS DISTINCT FROM b.definition->'servingDefinition' THEN RAISE EXCEPTION 'BINDING_CHANGED';END IF;
 SELECT * INTO p FROM moria.runtime_projection_bindings WHERE environment=b.environment AND resource=b.target AND enabled;
 IF NOT FOUND OR p.revision IS DISTINCT FROM b.definition->>'projectionRevision' THEN RAISE EXCEPTION 'BINDING_CHANGED';END IF;
 RETURN jsonb_build_object('environment',b.environment,'objectType',b.target,'sourceDataset',b.source,'targetDatasetRevision',b.source_revision::text,'targetSnapshot',b.snapshot_id,'targetRoot',moria.runtime_dataset_root_key(b.metadata_location),
  'definitionHash',encode(sha256(convert_to(b.definition::text,'UTF8')),'hex'),'keySchemaHash',d.key_schema_hash,
  'schemaHash',encode(sha256(convert_to(jsonb_build_object('dataset',d.iceberg_schema,'sourceColumns',b.definition->'sourceColumns','objectSchema',b.definition->'schema')::text,'UTF8')),'hex'),
  'servingGeneration',s.authority_generation::text,'layoutHash',p.revision,
  'constraintHash',encode(sha256(convert_to(jsonb_build_object('links',b.definition->'linkConstraints','schema',b.definition->'schema','serving',b.definition->'servingDefinition')::text,'UTF8')),'hex'),
  'incarnation',b.definition->>'incarnation');
END $$;
REVOKE ALL ON FUNCTION moria.runtime_source_delta_profile(uuid) FROM PUBLIC;
CREATE FUNCTION moria.runtime_source_window_open(build_id uuid,worker uuid,worker_fence bigint,window_id uuid,store_id uuid,window_kind text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE b moria.indexing_builds;s moria.source_stages;a moria.native_source_generation_aliases;metadata moria.native_source_row_stores;profile jsonb;version bigint;basis bigint;basis_s bigint;basis_dataset bigint;previous moria.native_source_delta_windows;
BEGIN
 -- Run on a short connection before the private mutation TX; full authority may retain control locks.
 PERFORM moria.indexing_claim_check(build_id,NULL,worker,worker_fence);
 SELECT * INTO b FROM moria.indexing_builds WHERE id=build_id;
 SELECT * INTO s FROM moria.source_stages WHERE id=build_id;
 IF NOT FOUND OR s.environment<>b.environment OR s.object_type<>b.target OR window_kind NOT IN ('bootstrap','delta') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 profile:=moria.runtime_source_delta_profile(build_id);
 SELECT * INTO previous FROM moria.native_source_delta_windows WHERE id=window_id;
 IF FOUND THEN
  IF ROW(previous.kind,previous.store,previous.stage,previous.owner,previous.fence,previous.profile,previous.definition)
   IS DISTINCT FROM ROW(window_kind,store_id,build_id,worker,worker_fence,profile,b.definition) OR previous.status IN ('failed','abandoned') THEN RAISE EXCEPTION 'SOURCE_DELTA_BASIS_CHANGED';END IF;
  RETURN jsonb_build_object('window',window_id,'store',store_id,'sourceVersion',previous.source_version::text,'profile',previous.profile);
 END IF;
 IF window_kind='bootstrap' THEN
  IF EXISTS(SELECT 1 FROM moria.source_generations WHERE stage=build_id) OR EXISTS(SELECT 1 FROM moria.native_source_row_stores WHERE id=store_id) THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
  version:=1;basis:=s.expected_revision;basis_s:=0;basis_dataset:=0;
 ELSE
  SELECT alias.* INTO a FROM moria.native_source_generation_aliases alias WHERE alias.environment=b.environment AND alias.object_type=b.target ORDER BY alias.revision DESC LIMIT 1;
  IF NOT FOUND OR a.store<>store_id OR a.revision<>s.expected_revision OR a.indexed_dataset_revision>=b.source_revision THEN RAISE EXCEPTION 'SOURCE_DELTA_BASIS_CHANGED';END IF;
  SELECT * INTO metadata FROM moria.native_source_row_stores WHERE id=store_id;
  IF ROW(metadata.definition_hash,metadata.key_schema_hash,metadata.schema_hash,metadata.serving_generation,metadata.layout_hash,metadata.constraint_hash)
   IS DISTINCT FROM ROW(profile->>'definitionHash',profile->>'keySchemaHash',profile->>'schemaHash',(profile->>'servingGeneration')::bigint,profile->>'layoutHash',profile->>'constraintHash') THEN RAISE EXCEPTION 'SOURCE_DELTA_REBUILD_REQUIRED';END IF;
  IF EXISTS(SELECT 1 FROM moria.native_source_delta_windows active WHERE active.store=store_id AND active.status IN ('preparing','ready','publishing')) THEN RAISE EXCEPTION 'SOURCE_DELTA_BUSY';END IF;
  -- Failed private transactions have rolled back all upper bounds before terminal cleanup can lock their window.
  SELECT GREATEST(a.source_version+1,COALESCE(max(source_version)+1,1)) INTO version FROM moria.native_source_delta_windows WHERE store=store_id;
  basis:=a.revision;basis_s:=a.source_version;basis_dataset:=a.indexed_dataset_revision;
 END IF;
 INSERT INTO moria.native_source_delta_windows(id,kind,store,source_version,stage,owner,fence,incarnation,environment,object_type,profile,definition,basis_revision,basis_source_version,basis_dataset_revision,target_dataset_revision)
 VALUES(window_id,window_kind,store_id,version,build_id,worker,worker_fence,(profile->>'incarnation')::uuid,b.environment,b.target,profile,b.definition,basis,basis_s,basis_dataset,b.source_revision) ON CONFLICT DO NOTHING;
 IF window_kind='bootstrap' THEN
  INSERT INTO moria.native_source_row_stores(id,environment,object_type,stage,definition_hash,key_schema_hash,schema_hash,serving_generation,layout_hash,constraint_hash)
  VALUES(store_id,b.environment,b.target,build_id,profile->>'definitionHash',profile->>'keySchemaHash',profile->>'schemaHash',(profile->>'servingGeneration')::bigint,profile->>'layoutHash',profile->>'constraintHash');
 END IF;
 SELECT * INTO previous FROM moria.native_source_delta_windows WHERE id=window_id;
 IF ROW(previous.kind,previous.store,previous.source_version,previous.stage,previous.owner,previous.fence,previous.profile,previous.definition,previous.basis_revision)
  IS DISTINCT FROM ROW(window_kind,store_id,version,build_id,worker,worker_fence,profile,b.definition,basis) THEN RAISE EXCEPTION 'SOURCE_DELTA_BASIS_CHANGED';END IF;
 RETURN jsonb_build_object('window',window_id,'store',store_id,'sourceVersion',version::text,'profile',profile);
END $$;
CREATE FUNCTION moria.runtime_source_store_mutation_check(store_id uuid,env text,target text,source_s bigint,origin_stage uuid,operation text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE w moria.native_source_delta_windows;s moria.native_source_row_stores;b moria.indexing_builds;active_revision bigint;declared_keys text[];
BEGIN
 -- State guard only. Protected entry helpers validate exact task/coordinator claims separately;
 -- no full authority/control lock is retained during off-admission MVCC preparation.
 IF NOT has_schema_privilege(current_user,'moria','CREATE') OR operation NOT IN ('bootstrap','insert','close','freeze','alias') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 SELECT * INTO w FROM moria.native_source_delta_windows WHERE store=store_id AND source_version=source_s;
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=store_id;
 IF w.id IS NULL OR s.id IS NULL OR w.environment<>env OR w.object_type<>target OR s.environment<>env OR s.object_type<>target OR s.stage<>origin_stage OR w.status NOT IN ('preparing','ready','publishing')
  OR ROW(s.definition_hash,s.key_schema_hash,s.schema_hash,s.serving_generation,s.layout_hash,s.constraint_hash)
   IS DISTINCT FROM ROW(w.profile->>'definitionHash',w.profile->>'keySchemaHash',w.profile->>'schemaHash',(w.profile->>'servingGeneration')::bigint,w.profile->>'layoutHash',w.profile->>'constraintHash') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 SELECT * INTO b FROM moria.indexing_builds WHERE id=w.stage;
 IF NOT FOUND OR b.environment<>env OR b.target<>target OR b.definition<>w.definition OR b.source_revision<>w.target_dataset_revision OR b.deadline<=clock_timestamp() OR b.status NOT IN ('planning','running','preparing','ready')
  OR b.definition->>'incarnation' IS DISTINCT FROM w.incarnation::text OR w.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'LEASE_LOST';END IF;
 IF w.kind='delta' AND (b.owner IS DISTINCT FROM w.owner OR b.fence<>w.fence OR b.lease_until IS NULL OR b.lease_until<=clock_timestamp()) THEN RAISE EXCEPTION 'LEASE_LOST';END IF;
 IF w.kind='bootstrap' AND (source_s<>1 OR w.stage<>origin_stage OR operation='close') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 IF w.kind='bootstrap' AND operation IN ('bootstrap','insert','freeze') AND w.status<>'preparing' THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ROW';END IF;
 IF operation='freeze' THEN
  IF w.kind<>'bootstrap' OR source_s<>1 OR b.owner IS DISTINCT FROM w.owner OR b.fence<>w.fence OR b.lease_until IS NULL OR b.lease_until<=clock_timestamp() OR NOT b.planned OR NOT EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE build=b.id) OR EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE build=b.id AND status<>'sealed') THEN RAISE EXCEPTION 'SOURCE_STORE_INCOMPLETE';END IF;
  IF EXISTS(WITH actual AS MATERIALIZED(SELECT uid,value,COALESCE(source_value,value) raw FROM moria.staged_source_rows WHERE stage=w.stage), prospective AS MATERIALIZED(SELECT uid,mapped,COALESCE(raw,mapped) raw FROM moria.native_source_row_versions WHERE store=store_id AND from_source_version=1 AND to_source_version IS NULL)
   SELECT 1 FROM actual a FULL JOIN prospective p USING(uid) WHERE a.uid IS NULL OR p.uid IS NULL OR a.value IS DISTINCT FROM p.mapped OR a.raw IS DISTINCT FROM p.raw) THEN RAISE EXCEPTION 'SOURCE_STORE_INCOMPLETE';END IF;
  SELECT primary_key INTO declared_keys FROM moria.native_dataset_bindings WHERE environment=env AND resource::text=b.source AND enabled;
  IF declared_keys IS NULL THEN RAISE EXCEPTION 'DATASET_BINDING_INVALID';END IF;
  IF cardinality(declared_keys)=0 THEN
   IF EXISTS(SELECT 1 FROM moria.native_source_key_map WHERE store=store_id) THEN RAISE EXCEPTION 'SOURCE_KEY_MAP_INCOMPLETE';END IF;
  ELSE
   IF EXISTS(WITH actual AS MATERIALIZED(SELECT uid,COALESCE(raw,mapped) raw FROM moria.native_source_row_versions WHERE store=store_id AND from_source_version=1 AND to_source_version IS NULL), keys AS MATERIALIZED(SELECT uid,encoded_key FROM moria.native_source_key_map WHERE store=store_id AND source_version=1)
    SELECT 1 FROM actual a FULL JOIN keys k USING(uid) WHERE a.uid IS NULL OR k.uid IS NULL OR convert_from(k.encoded_key,'UTF8')::jsonb IS DISTINCT FROM (SELECT jsonb_agg(a.raw->name ORDER BY ordinal) FROM unnest(declared_keys) WITH ORDINALITY field(name,ordinal))) THEN RAISE EXCEPTION 'SOURCE_KEY_MAP_INCOMPLETE';END IF;
  END IF;
 END IF;

 IF w.kind='delta' THEN
  SELECT max(revision) INTO active_revision FROM moria.source_generations WHERE environment=env AND object_type=target;
  IF operation='alias' THEN
   IF active_revision IS DISTINCT FROM w.publication_revision OR (SELECT max(revision) FROM moria.source_generations WHERE environment=env AND object_type=target AND revision<w.publication_revision) IS DISTINCT FROM w.basis_revision THEN RAISE EXCEPTION 'SOURCE_DELTA_BASIS_CHANGED';END IF;
  ELSIF active_revision IS DISTINCT FROM w.basis_revision THEN RAISE EXCEPTION 'SOURCE_DELTA_BASIS_CHANGED';END IF;
 END IF;
 IF operation='alias' AND (w.status<>'publishing' OR w.publication_revision IS NULL) THEN RAISE EXCEPTION 'SOURCE_DELTA_NOT_READY';END IF;
 -- A single window-row lock precedes Source/projection mutations. Lease renewal uses the
 -- actual indexing build/task tuple on another connection, never this row.
 IF w.kind='delta' OR operation='alias' THEN
  UPDATE moria.native_source_delta_windows SET mutation_transaction=pg_current_xact_id(),require_alias=true,preparation_revision=COALESCE(preparation_revision,(SELECT revision FROM moria.environments WHERE id=env)) WHERE id=w.id;
 END IF;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_source_store_mutation_check(uuid,text,text,bigint,uuid,text) FROM PUBLIC;
CREATE FUNCTION moria.runtime_source_window_commit_check() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE current_window moria.native_source_delta_windows;a moria.native_source_generation_aliases;
BEGIN
 SELECT * INTO current_window FROM moria.native_source_delta_windows WHERE id=NEW.id;
 IF NOT current_window.require_alias OR current_window.mutation_transaction IS DISTINCT FROM pg_current_xact_id() THEN RETURN NEW;END IF;
 SELECT * INTO a FROM moria.native_source_generation_aliases WHERE store=current_window.store AND source_version=current_window.source_version;
 IF NOT FOUND OR current_window.status<>'published' OR current_window.publication_revision IS DISTINCT FROM a.revision OR a.environment<>current_window.environment OR a.object_type<>current_window.object_type
  OR NOT EXISTS(SELECT 1 FROM moria.indexing_builds b WHERE b.id=current_window.stage AND b.status='active' AND b.source_revision=current_window.target_dataset_revision AND b.receipt->>'revision'=a.revision::text AND b.receipt->>'sourceRevision'=current_window.target_dataset_revision::text)
  OR NOT EXISTS(SELECT 1 FROM moria.indexing_changes c WHERE c.environment=a.environment AND c.revision=a.revision AND c.target=a.object_type AND c.generation=current_window.stage)
  OR a.indexed_dataset_revision<>current_window.target_dataset_revision OR a.indexed_snapshot IS DISTINCT FROM current_window.profile->>'targetSnapshot' OR a.indexed_root IS DISTINCT FROM current_window.profile->>'targetRoot' THEN RAISE EXCEPTION 'SOURCE_DELTA_PUBLICATION_REQUIRED';END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER native_source_window_atomic AFTER INSERT OR UPDATE ON moria.native_source_delta_windows DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_window_commit_check();
`;
