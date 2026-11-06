/** Full-build Source stores reuse the existing immutable staged population. */
export const sourceStoreSchema = String.raw`
CREATE TABLE moria.native_source_row_stores(
 id uuid PRIMARY KEY REFERENCES moria.source_stages(id),environment text NOT NULL,object_type text NOT NULL,
 definition_hash text NOT NULL,key_schema_hash text NOT NULL,schema_hash text NOT NULL,
 serving_generation bigint NOT NULL,layout_hash text NOT NULL,constraint_hash text NOT NULL,
 profile jsonb NOT NULL,ordered_keys text[] NOT NULL,source_columns jsonb NOT NULL,
 status text NOT NULL DEFAULT 'loading' CHECK(status IN ('loading','sealed','published','retired')),
 row_count bigint CHECK(row_count>=0),key_count bigint CHECK(key_count>=0),
 owner uuid NOT NULL,fence bigint NOT NULL CHECK(fence>0),incarnation uuid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Keep the current canonical UID index and archive only tuples closed by a Source version.
-- Published versions beyond one require the separately qualified sparse writer.
ALTER TABLE moria.staged_source_rows ADD COLUMN source_from bigint NOT NULL DEFAULT 1 CHECK(source_from=1);
CREATE TABLE moria.native_source_canonical_history(
 store uuid NOT NULL REFERENCES moria.native_source_row_stores(id),
 uid uuid NOT NULL REFERENCES moria.object_homes(uid),
 source_from bigint NOT NULL CHECK(source_from>0),
 source_to bigint NOT NULL CHECK(source_to>source_from),
 value jsonb NOT NULL,source_value jsonb CHECK(source_value IS NULL OR jsonb_typeof(source_value)='object'),
 PRIMARY KEY(store,uid,source_from)
);
CREATE TABLE moria.native_source_key_map(
 store uuid NOT NULL REFERENCES moria.native_source_row_stores(id),key_hash bytea NOT NULL CHECK(octet_length(key_hash)=32),
 collision integer NOT NULL CHECK(collision BETWEEN 0 AND 63),encoded_key bytea NOT NULL CHECK(octet_length(encoded_key) BETWEEN 1 AND 4194304),
 uid uuid NOT NULL REFERENCES moria.object_homes(uid),source_version bigint NOT NULL DEFAULT 1 CHECK(source_version=1),
 PRIMARY KEY(store,key_hash,collision),UNIQUE(store,uid)
);
CREATE TABLE moria.native_source_generation_aliases(
 environment text NOT NULL,object_type text NOT NULL,revision bigint NOT NULL,
 store uuid NOT NULL REFERENCES moria.native_source_row_stores(id),source_version bigint NOT NULL CHECK(source_version=1),
 indexed_build uuid NOT NULL UNIQUE REFERENCES moria.indexing_builds(id),
 indexed_dataset_revision bigint NOT NULL,indexed_snapshot text,indexed_root text NOT NULL,
 PRIMARY KEY(environment,object_type,revision),
 FOREIGN KEY(environment,object_type,revision) REFERENCES moria.source_generations(environment,object_type,revision) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX native_source_alias_store ON moria.native_source_generation_aliases(store,revision DESC);
CREATE FUNCTION moria.runtime_source_store_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER native_source_store_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_row_stores FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_owner();
CREATE TRIGGER native_source_canonical_history_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_canonical_history
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_owner();
CREATE FUNCTION moria.runtime_source_canonical_history_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_SOURCE_HISTORY';END $$;
CREATE TRIGGER native_source_canonical_history_immutable BEFORE UPDATE ON moria.native_source_canonical_history
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_canonical_history_immutable();
CREATE TRIGGER native_source_keys_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_key_map FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_owner();
CREATE TRIGGER native_source_alias_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_generation_aliases FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_owner();
CREATE FUNCTION moria.runtime_source_alias_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ALIAS';END $$;
CREATE TRIGGER native_source_alias_immutable BEFORE UPDATE OR DELETE ON moria.native_source_generation_aliases FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_alias_immutable();
CREATE FUNCTION moria.runtime_source_store_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['status','row_count','key_count','owner','fence']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['status','row_count','key_count','owner','fence'])
  OR OLD.status IN ('published','retired') AND (to_jsonb(NEW)-'status') IS DISTINCT FROM (to_jsonb(OLD)-'status')
  OR NEW.fence<OLD.fence
  OR OLD.status='retired' AND NEW.status<>'retired'
  OR OLD.status='published' AND NEW.status NOT IN ('published','retired')
  OR OLD.status='sealed' AND NEW.status='loading' AND NOT EXISTS(SELECT 1 FROM moria.indexing_builds WHERE id=OLD.id AND status='failed')
  OR OLD.status='loading' AND NEW.status NOT IN ('loading','sealed','retired')
  OR NEW.status='retired' AND OLD.status IN ('loading','sealed') AND (
   EXISTS(SELECT 1 FROM moria.native_source_generation_aliases WHERE store=OLD.id)
   OR NOT EXISTS(SELECT 1 FROM moria.indexing_builds b WHERE b.id=OLD.id
    AND b.status IN ('failed','cancelled','superseded','conflicted') AND b.deadline<=clock_timestamp())) THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_STORE';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_store_immutable BEFORE UPDATE OR DELETE ON moria.native_source_row_stores FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_immutable();
CREATE FUNCTION moria.runtime_source_store_profile(input_build uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE b moria.indexing_builds;d moria.native_dataset_bindings;s moria.native_serving_bindings;p moria.runtime_projection_bindings;
BEGIN
 SELECT * INTO b FROM moria.indexing_builds WHERE id=input_build;
 IF NOT FOUND THEN RAISE EXCEPTION 'BUILD_UNKNOWN';END IF;
 -- Retained full-build contracts without the managed serving/projection profile remain unchanged.
 IF NOT(b.definition ? 'servingBindingGeneration') OR NOT(b.definition ? 'projectionRevision') THEN RETURN NULL;END IF;
 SELECT * INTO d FROM moria.native_dataset_bindings WHERE environment=b.environment AND resource::text=b.source AND enabled;
 IF NOT FOUND THEN RETURN NULL;END IF;
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
REVOKE ALL ON FUNCTION moria.runtime_source_store_profile(uuid) FROM PUBLIC;
CREATE FUNCTION moria.runtime_source_store_open(input_build uuid,input_worker uuid,input_fence bigint) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE b moria.indexing_builds;d moria.native_dataset_bindings;s moria.native_source_row_stores;profile jsonb;
BEGIN
 PERFORM moria.runtime_indexing_staging_shared(input_build);
 PERFORM moria.indexing_claim_check(input_build,NULL,input_worker,input_fence);
 SELECT * INTO b FROM moria.indexing_builds WHERE id=input_build;
 profile:=moria.runtime_source_store_profile(input_build);
 IF profile IS NULL THEN RETURN NULL;END IF;
 SELECT * INTO d FROM moria.native_dataset_bindings WHERE environment=b.environment AND resource::text=b.source AND enabled;
 INSERT INTO moria.native_source_row_stores(id,environment,object_type,definition_hash,key_schema_hash,schema_hash,serving_generation,layout_hash,constraint_hash,profile,ordered_keys,source_columns,owner,fence,incarnation)
 VALUES(input_build,b.environment,b.target,profile->>'definitionHash',profile->>'keySchemaHash',profile->>'schemaHash',(profile->>'servingGeneration')::bigint,profile->>'layoutHash',profile->>'constraintHash',profile,d.primary_key,d.columns,input_worker,input_fence,(profile->>'incarnation')::uuid) ON CONFLICT DO NOTHING;
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=input_build FOR UPDATE;
 IF s.profile<>profile OR s.ordered_keys<>d.primary_key OR s.source_columns<>d.columns OR s.status NOT IN ('loading','sealed') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 UPDATE moria.native_source_row_stores SET owner=input_worker,fence=input_fence WHERE id=input_build;
 RETURN jsonb_build_object('store',input_build,'sourceVersion','1','keys',d.primary_key,'columns',d.columns);
END $$;
CREATE FUNCTION moria.runtime_source_store_task(input_build uuid,input_task uuid,input_worker uuid,input_fence bigint) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE s moria.native_source_row_stores;
BEGIN
 PERFORM moria.runtime_indexing_staging_shared(input_build);
 PERFORM moria.indexing_claim_check(input_build,input_task,input_worker,input_fence);
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=input_build;
 IF NOT FOUND THEN RETURN NULL;END IF;
 IF s.status<>'loading' OR NOT EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE id=input_task AND build=input_build AND plan->>'kind'='reduce') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 RETURN jsonb_build_object('store',input_build,'sourceVersion','1','keys',s.ordered_keys,'columns',s.source_columns);
END $$;
CREATE FUNCTION moria.runtime_source_store_keys(input_build uuid,input_task uuid,input_worker uuid,input_fence bigint,input_ids uuid[],input_keys bytea[]) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE s moria.native_source_row_stores;item record;existing moria.native_source_key_map;next_collision integer;
BEGIN
 PERFORM moria.runtime_source_store_task(input_build,input_task,input_worker,input_fence);
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=input_build;
 IF NOT FOUND OR cardinality(input_ids) IS NULL OR cardinality(input_keys) IS NULL OR cardinality(input_ids) NOT BETWEEN 1 AND 1000 OR cardinality(input_ids) IS DISTINCT FROM cardinality(input_keys)
  OR array_position(input_ids,NULL) IS NOT NULL OR array_position(input_keys,NULL) IS NOT NULL
  OR EXISTS(SELECT 1 FROM unnest(input_keys) key WHERE octet_length(key) NOT BETWEEN 1 AND 4194304)
  OR (SELECT COALESCE(sum(octet_length(key)),0) FROM unnest(input_keys) key)>16777216 THEN RAISE EXCEPTION 'RESOURCE_LIMIT';END IF;
 IF cardinality(s.ordered_keys)=0 THEN RAISE EXCEPTION 'SOURCE_KEY_MAP_NOT_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM unnest(input_ids,input_keys) i(uid,key) LEFT JOIN moria.staged_source_rows r ON r.stage=input_build AND r.uid=i.uid
  WHERE r.uid IS NULL OR convert_from(i.key,'UTF8')::jsonb IS DISTINCT FROM (SELECT jsonb_agg(COALESCE(r.source_value,r.value)->name ORDER BY ordinal) FROM unnest(s.ordered_keys) WITH ORDINALITY field(name,ordinal))) THEN RAISE EXCEPTION 'SOURCE_KEY_MAP_INVALID';END IF;
 IF (SELECT count(DISTINCT key) FROM unnest(input_keys) key)<>cardinality(input_keys) THEN RAISE EXCEPTION 'DATASET_PRIMARY_KEY_DUPLICATE';END IF;
 INSERT INTO moria.native_source_key_map(store,key_hash,collision,encoded_key,uid)
  SELECT DISTINCT ON(sha256(key)) input_build,sha256(key),0,key,uid FROM unnest(input_ids,input_keys) i(uid,key) ORDER BY sha256(key),key ON CONFLICT DO NOTHING;
 FOR item IN SELECT i.uid,i.key,sha256(i.key) hash FROM unnest(input_ids,input_keys) i(uid,key)
  WHERE NOT EXISTS(SELECT 1 FROM moria.native_source_key_map m WHERE m.store=input_build AND m.key_hash=sha256(i.key) AND m.encoded_key=i.key AND m.uid=i.uid) ORDER BY sha256(i.key),i.key LOOP
  PERFORM 1 FROM moria.native_source_key_map WHERE store=input_build AND key_hash=item.hash AND collision=0 FOR UPDATE;
  SELECT * INTO existing FROM moria.native_source_key_map WHERE store=input_build AND key_hash=item.hash AND encoded_key=item.key;
  IF FOUND THEN RAISE EXCEPTION 'DATASET_PRIMARY_KEY_DUPLICATE';END IF;
  SELECT COALESCE(max(collision),-1)+1 INTO next_collision FROM moria.native_source_key_map WHERE store=input_build AND key_hash=item.hash;
  IF next_collision>63 THEN RAISE EXCEPTION 'RESOURCE_LIMIT';END IF;
  INSERT INTO moria.native_source_key_map(store,key_hash,collision,encoded_key,uid) VALUES(input_build,item.hash,next_collision,item.key,item.uid);
 END LOOP;
END $$;
CREATE FUNCTION moria.runtime_source_store_seal(input_build uuid,input_worker uuid,input_fence bigint) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE s moria.native_source_row_stores;profile jsonb;rows bigint;input_keys bigint;expected bigint;
BEGIN
 PERFORM moria.runtime_indexing_staging_shared(input_build);
 PERFORM moria.indexing_claim_check(input_build,NULL,input_worker,input_fence);
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=input_build FOR UPDATE;
 IF NOT FOUND THEN RETURN;END IF;
 profile:=moria.runtime_source_store_profile(input_build);
 IF s.profile IS DISTINCT FROM profile OR s.status NOT IN ('loading','sealed') OR NOT EXISTS(SELECT 1 FROM moria.indexing_builds WHERE id=input_build AND planned)
  OR NOT EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE build=input_build) OR EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE build=input_build AND status<>'sealed') THEN RAISE EXCEPTION 'SOURCE_STORE_INCOMPLETE';END IF;
 SELECT count(*) INTO rows FROM moria.staged_source_rows WHERE stage=input_build;
 SELECT count(*) INTO input_keys FROM moria.native_source_key_map WHERE store=input_build;
 SELECT COALESCE(sum(row_count),0) INTO expected FROM moria.indexing_tasks WHERE build=input_build AND plan->>'kind'='scan';
 IF rows<>expected OR cardinality(s.ordered_keys)>0 AND input_keys<>rows OR cardinality(s.ordered_keys)=0 AND input_keys<>0 THEN RAISE EXCEPTION 'SOURCE_KEY_MAP_INCOMPLETE';END IF;
 UPDATE moria.native_source_row_stores SET status='sealed',row_count=rows,key_count=input_keys,owner=input_worker,fence=input_fence WHERE id=input_build;
END $$;
CREATE FUNCTION moria.runtime_source_store_publish(input_build uuid,input_worker uuid,input_fence bigint,input_revision bigint) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE s moria.native_source_row_stores;b moria.indexing_builds;
BEGIN
 PERFORM moria.runtime_indexing_staging_shared(input_build);
 PERFORM moria.indexing_claim_check(input_build,NULL,input_worker,input_fence);
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=input_build FOR UPDATE;
 IF NOT FOUND THEN RETURN;END IF;
 SELECT * INTO b FROM moria.indexing_builds WHERE id=input_build;
 IF s.status<>'sealed' OR s.owner<>input_worker OR s.fence<>input_fence OR s.profile IS DISTINCT FROM moria.runtime_source_store_profile(input_build)
  OR NOT EXISTS(SELECT 1 FROM moria.source_generations WHERE environment=b.environment AND object_type=b.target AND revision=input_revision AND stage=input_build) THEN RAISE EXCEPTION 'SOURCE_STORE_INCOMPLETE';END IF;
 INSERT INTO moria.native_source_generation_aliases VALUES(b.environment,b.target,input_revision,input_build,1,input_build,b.source_revision,b.snapshot_id,moria.runtime_dataset_root_key(b.metadata_location));
 UPDATE moria.native_source_row_stores SET status='published' WHERE id=input_build;
END $$;
CREATE FUNCTION moria.runtime_source_alias_atomic() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE s moria.native_source_row_stores;b moria.indexing_builds;a moria.native_source_generation_aliases;
BEGIN
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=NEW.stage;
 IF NOT FOUND THEN RETURN NEW;END IF;
 SELECT * INTO a FROM moria.native_source_generation_aliases alias WHERE alias.environment=NEW.environment AND alias.object_type=NEW.object_type AND alias.revision=NEW.revision;
 SELECT * INTO b FROM moria.indexing_builds WHERE id=a.indexed_build;
 IF s.status<>'published' OR a.store IS DISTINCT FROM NEW.stage OR a.source_version IS DISTINCT FROM 1
  OR b.status IS DISTINCT FROM 'active' OR ROW(b.environment,b.target) IS DISTINCT FROM ROW(NEW.environment,NEW.object_type)
  OR b.receipt->>'revision' IS DISTINCT FROM NEW.revision::text
  OR b.receipt->>'stage' IS DISTINCT FROM NEW.stage::text
  OR ROW(a.indexed_dataset_revision,a.indexed_snapshot,a.indexed_root) IS DISTINCT FROM ROW(b.source_revision,b.snapshot_id,moria.runtime_dataset_root_key(b.metadata_location))
  OR (a.indexed_build<>a.store AND NOT EXISTS(
   SELECT 1 FROM moria.native_source_delta_windows w WHERE w.build=a.indexed_build AND w.status='qualified' AND NOT w.has_mutation
    AND w.closed_at IS NOT NULL AND w.basis_store=a.store AND w.basis_source_version=a.source_version
    AND w.target_dataset_revision=a.indexed_dataset_revision AND w.cursor_revision=w.target_dataset_revision))
 THEN RAISE EXCEPTION 'SOURCE_ALIAS_REQUIRED';END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER native_source_alias_atomic AFTER INSERT ON moria.source_generations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_alias_atomic();
CREATE FUNCTION moria.runtime_source_store_retry(input_build uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/indexing-staging/v1/'||input_build::text,0)) THEN
  RAISE EXCEPTION 'INDEXING_STAGING_BUSY';
 END IF;
 PERFORM moria.runtime_indexing_retry_authority(input_build);
 IF EXISTS(SELECT 1 FROM moria.native_source_generation_aliases WHERE store=input_build) THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_STORE';END IF;
 DELETE FROM moria.native_source_key_map WHERE store=input_build;
 UPDATE moria.native_source_row_stores SET status='loading',row_count=NULL,key_count=NULL WHERE id=input_build;
END $$;

-- Public and compatibility readers bind an exact logical alias; an unpublished
-- projection stage still reads its own version-one rows before it has an alias.
CREATE FUNCTION moria.runtime_source_stage_selection(input_stage uuid,input_environment text,input_target text,
 input_generation_revision bigint DEFAULT NULL,input_at_revision bigint DEFAULT NULL)
 RETURNS TABLE(stage uuid,source_version bigint,head_version bigint)
 LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE store moria.native_source_row_stores;selected_version bigint;latest_version bigint;
BEGIN
 IF input_stage IS NULL THEN RETURN QUERY SELECT NULL::uuid,NULL::bigint,NULL::bigint;RETURN;END IF;
 SELECT * INTO store FROM moria.native_source_row_stores WHERE id=input_stage;
 IF NOT FOUND THEN RETURN QUERY SELECT input_stage,1::bigint,1::bigint;RETURN;END IF;
 IF store.environment<>input_environment OR store.object_type<>input_target OR store.status='retired' THEN RAISE EXCEPTION 'SOURCE_ALIAS_REQUIRED';END IF;
 IF store.status IN ('loading','sealed') THEN
  IF input_generation_revision IS NOT NULL OR NOT EXISTS(SELECT 1 FROM moria.indexing_builds b WHERE b.id=input_stage AND b.environment=input_environment AND b.target=input_target AND b.status IN ('planning','running','preparing','ready')) THEN RAISE EXCEPTION 'SOURCE_ALIAS_REQUIRED';END IF;
  RETURN QUERY SELECT input_stage,1::bigint,1::bigint;RETURN;
 END IF;
 SELECT a.source_version INTO selected_version FROM moria.native_source_generation_aliases a JOIN moria.source_generations g
  ON g.environment=a.environment AND g.object_type=a.object_type AND g.revision=a.revision AND g.stage=input_stage
  WHERE a.environment=input_environment AND a.object_type=input_target AND a.store=input_stage
  AND (input_generation_revision IS NULL OR a.revision=input_generation_revision)
  AND (input_at_revision IS NULL OR a.revision<=input_at_revision)
  ORDER BY a.revision DESC LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'SOURCE_ALIAS_REQUIRED';END IF;
 IF input_generation_revision IS NULL AND input_at_revision IS NULL THEN latest_version:=selected_version;
 ELSE SELECT a.source_version INTO latest_version FROM moria.native_source_generation_aliases a
  WHERE a.store=input_stage ORDER BY a.revision DESC LIMIT 1;END IF;
 RETURN QUERY SELECT input_stage,selected_version,latest_version;
END $$;
CREATE FUNCTION moria.runtime_source_stage(input_stage uuid,input_environment text,input_target text,input_generation_revision bigint DEFAULT NULL) RETURNS uuid
 LANGUAGE sql STABLE AS $$
 SELECT selection.stage FROM moria.runtime_source_stage_selection(input_stage,input_environment,input_target,input_generation_revision) selection
$$;
CREATE FUNCTION moria.runtime_source_canonical_rows(input_stage uuid,input_version bigint,input_head_version bigint)
 RETURNS TABLE(uid uuid,value jsonb,source_value jsonb) LANGUAGE sql STABLE AS $$
 SELECT current_row.uid,current_row.value,COALESCE(current_row.source_value,current_row.value)
  FROM moria.staged_source_rows current_row WHERE current_row.stage=input_stage AND current_row.source_from<=input_version
 UNION ALL
 SELECT old.uid,old.value,COALESCE(old.source_value,old.value)
  FROM moria.native_source_canonical_history old WHERE input_version<input_head_version AND old.store=input_stage
   AND old.source_from<=input_version AND old.source_to>input_version
$$;
CREATE OR REPLACE FUNCTION moria.runtime_object_base_stage(stage_id uuid,env text,target text,at_revision bigint)
 RETURNS TABLE(uid uuid,value jsonb,source_value jsonb,action_created boolean) LANGUAGE sql STABLE AS $$
 WITH selection AS MATERIALIZED(SELECT * FROM moria.runtime_source_stage_selection(stage_id,env,target,NULL,at_revision)),
 actual AS NOT MATERIALIZED(SELECT r.* FROM selection s CROSS JOIN LATERAL moria.runtime_source_canonical_rows(s.stage,s.source_version,s.head_version) r)
 SELECT r.uid,r.value,r.source_value,false FROM actual r
 UNION ALL SELECT c.uid,c.value,c.source_value,c.action_created FROM moria.runtime_object_creations_at(env,target,at_revision) c
 WHERE NOT EXISTS(SELECT 1 FROM actual r WHERE r.uid=c.uid)
$$;
CREATE OR REPLACE FUNCTION moria.runtime_object_base(env text,target text,at_revision bigint)
 RETURNS TABLE(uid uuid,value jsonb,source_value jsonb,action_created boolean) LANGUAGE sql STABLE AS $$
 WITH generation AS MATERIALIZED(
  SELECT g.revision,s.* FROM (SELECT g.revision,g.stage FROM moria.source_generations g
   WHERE g.environment=env AND g.object_type=target AND g.revision<=at_revision ORDER BY g.revision DESC LIMIT 1) g
  CROSS JOIN LATERAL moria.runtime_source_stage_selection(g.stage,env,target,g.revision) s),
 actual AS NOT MATERIALIZED(
  SELECT r.* FROM generation g CROSS JOIN LATERAL moria.runtime_source_canonical_rows(g.stage,g.source_version,g.head_version) r
  UNION ALL SELECT r.uid,r.value,r.value FROM generation g JOIN moria.source_rows r
  ON r.environment=env AND r.object_type=target AND r.revision=g.revision WHERE g.stage IS NULL)
 SELECT a.uid,a.value,a.source_value,false FROM actual a
 UNION ALL SELECT c.uid,c.value,c.source_value,c.action_created FROM moria.runtime_object_creations_at(env,target,at_revision) c
 WHERE NOT EXISTS(SELECT 1 FROM actual a WHERE a.uid=c.uid)
$$;
`;
