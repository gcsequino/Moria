/** Versioned canonical Source rows. Installed only with the guarded delta publisher. */
export const sourceRowsSchema = String.raw`
CREATE TABLE moria.native_source_row_stores(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 environment text NOT NULL REFERENCES moria.environments(id),
 object_type text NOT NULL,
 stage uuid NOT NULL UNIQUE REFERENCES moria.source_stages(id),
 definition_hash text NOT NULL CHECK(octet_length(definition_hash) BETWEEN 1 AND 256),
 key_schema_hash text NOT NULL CHECK(octet_length(key_schema_hash) BETWEEN 1 AND 256),
 schema_hash text NOT NULL CHECK(octet_length(schema_hash) BETWEEN 1 AND 256),
 serving_generation bigint NOT NULL CHECK(serving_generation>=0),
 layout_hash text NOT NULL CHECK(octet_length(layout_hash) BETWEEN 1 AND 256),
 constraint_hash text NOT NULL CHECK(octet_length(constraint_hash) BETWEEN 1 AND 256),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(id,environment,object_type,stage)
);
CREATE TABLE moria.native_source_row_versions(
 store uuid NOT NULL REFERENCES moria.native_source_row_stores(id),
 uid uuid NOT NULL REFERENCES moria.object_homes(uid),
 from_source_version bigint NOT NULL CHECK(from_source_version>0),
 to_source_version bigint CHECK(to_source_version>from_source_version),
 raw jsonb CHECK(raw IS NULL OR jsonb_typeof(raw)='object'),
 mapped jsonb NOT NULL CHECK(jsonb_typeof(mapped)='object'),
 CHECK(octet_length(mapped::text)::bigint+COALESCE(octet_length(raw::text)::bigint,0)<=16777216),
 PRIMARY KEY(store,uid,from_source_version)
);
CREATE UNIQUE INDEX native_source_rows_current ON moria.native_source_row_versions(store,uid)
 WHERE to_source_version IS NULL;
CREATE INDEX native_source_rows_closed ON moria.native_source_row_versions(store,to_source_version,uid,from_source_version)
 WHERE to_source_version IS NOT NULL;
CREATE TABLE moria.native_source_generation_aliases(
 environment text NOT NULL,
 object_type text NOT NULL,
 revision bigint NOT NULL,
 store uuid NOT NULL,
 source_version bigint NOT NULL CHECK(source_version>0),
 stage uuid NOT NULL,
 projection_id uuid NOT NULL REFERENCES moria.runtime_projections(id),
 source_dataset text NOT NULL,
 indexed_dataset_revision bigint NOT NULL CHECK(indexed_dataset_revision>=0),
 indexed_snapshot text NOT NULL CHECK(octet_length(indexed_snapshot) BETWEEN 1 AND 256),
 indexed_root text NOT NULL CHECK(indexed_root~'^[0-9a-f]{64}$'),
 definition_hash text NOT NULL CHECK(octet_length(definition_hash) BETWEEN 1 AND 256),
 key_schema_hash text NOT NULL CHECK(octet_length(key_schema_hash) BETWEEN 1 AND 256),
 schema_hash text NOT NULL CHECK(octet_length(schema_hash) BETWEEN 1 AND 256),
 serving_generation bigint NOT NULL CHECK(serving_generation>=0),
 layout_hash text NOT NULL CHECK(octet_length(layout_hash) BETWEEN 1 AND 256),
 constraint_hash text NOT NULL CHECK(octet_length(constraint_hash) BETWEEN 1 AND 256),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment,object_type,revision),
 FOREIGN KEY(environment,object_type,revision) REFERENCES moria.source_generations(environment,object_type,revision),
 FOREIGN KEY(store,environment,object_type,stage) REFERENCES moria.native_source_row_stores(id,environment,object_type,stage),
 FOREIGN KEY(environment,source_dataset,indexed_dataset_revision) REFERENCES moria.snapshot_history(environment,dataset,revision),
 UNIQUE(store,source_version)
);
CREATE FUNCTION moria.runtime_source_store_owner_check() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED'; END IF;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_source_store_owner_check() FROM PUBLIC;
CREATE FUNCTION moria.runtime_source_store_row_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM moria.runtime_source_store_owner_check();
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_STORE'; END IF;
 PERFORM moria.runtime_source_store_mutation_check(NEW.id,NEW.environment,NEW.object_type,1,NEW.stage,'bootstrap');
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_store_insert AFTER INSERT ON moria.native_source_row_stores
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_row_guard();
CREATE TRIGGER native_source_store_immutable BEFORE UPDATE OR DELETE ON moria.native_source_row_stores
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_store_row_guard();
CREATE FUNCTION moria.runtime_source_version_row_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ROW'; END IF;
 IF TG_OP='UPDATE' AND (
   ROW(NEW.store,NEW.uid,NEW.from_source_version,NEW.raw,NEW.mapped)
    IS DISTINCT FROM ROW(OLD.store,OLD.uid,OLD.from_source_version,OLD.raw,OLD.mapped)
   OR OLD.to_source_version IS NOT NULL OR NEW.to_source_version IS NULL
   OR NEW.to_source_version<=OLD.from_source_version)
  THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ROW'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_version_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_row_versions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_version_row_guard();
CREATE FUNCTION moria.runtime_source_version_statement_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item record;
BEGIN
 PERFORM moria.runtime_source_store_owner_check();
 IF (SELECT count(*) FROM new_rows)>10000 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
 IF (SELECT count(*) FROM (SELECT DISTINCT store FROM new_rows LIMIT 257) bounded)>256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM new_rows n JOIN moria.native_source_row_stores s ON s.id=n.store
    JOIN moria.object_homes h ON h.uid=n.uid WHERE h.object_type<>s.object_type)
   THEN RAISE EXCEPTION 'UID_TYPE_CONFLICT'; END IF;
  FOR item IN SELECT DISTINCT n.store,s.environment,s.object_type,s.stage,n.from_source_version AS version
   FROM new_rows n JOIN moria.native_source_row_stores s ON s.id=n.store LOOP
    PERFORM moria.runtime_source_store_mutation_check(item.store,item.environment,item.object_type,item.version,item.stage,'insert');
  END LOOP;
 ELSE
  FOR item IN SELECT DISTINCT n.store,s.environment,s.object_type,s.stage,n.to_source_version AS version
   FROM new_rows n JOIN moria.native_source_row_stores s ON s.id=n.store LOOP
    PERFORM moria.runtime_source_store_mutation_check(item.store,item.environment,item.object_type,item.version,item.stage,'close');
  END LOOP;
 END IF;
 RETURN NULL;
END $$;
CREATE TRIGGER native_source_version_insert AFTER INSERT ON moria.native_source_row_versions
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION moria.runtime_source_version_statement_guard();
CREATE TRIGGER native_source_version_close AFTER UPDATE ON moria.native_source_row_versions
 REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION moria.runtime_source_version_statement_guard();
CREATE FUNCTION moria.runtime_source_alias_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE accepted moria.native_source_row_stores; current_stage uuid; history record;
BEGIN
 PERFORM moria.runtime_source_store_owner_check();
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_ALIAS'; END IF;
 SELECT * INTO accepted FROM moria.native_source_row_stores WHERE id=NEW.store;
 SELECT stage INTO current_stage FROM moria.source_generations
  WHERE environment=NEW.environment AND object_type=NEW.object_type AND revision=NEW.revision;
 IF NOT FOUND OR current_stage IS DISTINCT FROM NEW.stage
  OR ROW(NEW.definition_hash,NEW.key_schema_hash,NEW.schema_hash,NEW.serving_generation,NEW.layout_hash,NEW.constraint_hash)
   IS DISTINCT FROM ROW(accepted.definition_hash,accepted.key_schema_hash,accepted.schema_hash,accepted.serving_generation,accepted.layout_hash,accepted.constraint_hash)
  OR NOT EXISTS(SELECT 1 FROM moria.runtime_projections p WHERE p.id=NEW.projection_id AND p.stage=NEW.stage AND p.environment=NEW.environment AND p.resource=NEW.object_type)
  THEN RAISE EXCEPTION 'SOURCE_ALIAS_INVALID'; END IF;
 SELECT metadata_location,snapshot_id INTO history FROM moria.snapshot_history
  WHERE environment=NEW.environment AND dataset=NEW.source_dataset AND revision=NEW.indexed_dataset_revision;
 IF NOT FOUND OR NEW.indexed_snapshot IS DISTINCT FROM history.snapshot_id
  OR NEW.indexed_root IS DISTINCT FROM moria.runtime_dataset_root_key(history.metadata_location)
  THEN RAISE EXCEPTION 'SOURCE_ALIAS_INVALID'; END IF;
 PERFORM moria.runtime_source_store_mutation_check(NEW.store,NEW.environment,NEW.object_type,NEW.source_version,NEW.stage,'alias');
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_alias_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_generation_aliases
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_alias_guard();
CREATE FUNCTION moria.runtime_source_rows_stage(stage_id uuid,env text,target text,at_revision bigint)
 RETURNS TABLE(uid uuid,value jsonb,source_value jsonb) LANGUAGE sql STABLE AS $$
 WITH alias AS MATERIALIZED(
  SELECT a.store,a.source_version FROM moria.source_generations g
  JOIN moria.native_source_generation_aliases a USING(environment,object_type,revision)
  WHERE g.environment=env AND g.object_type=target AND g.stage=stage_id AND g.revision<=at_revision
  ORDER BY g.revision DESC LIMIT 1),
 selected AS MATERIALIZED(
  SELECT a.store,a.source_version FROM alias a
  UNION ALL SELECT s.id,1::bigint FROM moria.native_source_row_stores s
   WHERE s.stage=stage_id AND s.environment=env AND s.object_type=target AND NOT EXISTS(SELECT 1 FROM alias)
    AND NOT EXISTS(SELECT 1 FROM moria.source_generations published WHERE published.stage=s.stage))
 SELECT r.uid,r.mapped,COALESCE(r.raw,r.mapped) FROM selected a
  JOIN moria.native_source_row_versions r ON r.store=a.store AND r.from_source_version<=a.source_version
   AND (r.to_source_version IS NULL OR r.to_source_version>a.source_version)
 UNION ALL SELECT r.uid,r.value,COALESCE(r.source_value,r.value) FROM moria.staged_source_rows r
  WHERE r.stage=stage_id AND NOT EXISTS(SELECT 1 FROM selected)
$$;
CREATE FUNCTION moria.runtime_source_rows_at(env text,target text,at_revision bigint)
 RETURNS TABLE(uid uuid,value jsonb,source_value jsonb) LANGUAGE sql STABLE AS $$
 WITH generation AS MATERIALIZED(
  SELECT g.stage,g.revision FROM moria.source_generations g
   WHERE g.environment=env AND g.object_type=target AND g.revision<=at_revision ORDER BY g.revision DESC LIMIT 1)
 SELECT r.* FROM generation g CROSS JOIN LATERAL moria.runtime_source_rows_stage(g.stage,env,target,at_revision) r WHERE g.stage IS NOT NULL
 UNION ALL SELECT r.uid,r.value,r.value FROM generation g JOIN moria.source_rows r
  ON r.environment=env AND r.object_type=target AND r.revision=g.revision WHERE g.stage IS NULL
$$;
`;
