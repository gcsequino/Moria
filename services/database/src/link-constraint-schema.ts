/** Global mapped-link key proofs belong to the same versioned projection as object reads. */
export const linkConstraintSchema = String.raw`
CREATE FUNCTION moria.runtime_link_constraints(env text,target text) RETURNS jsonb LANGUAGE sql STABLE
 SET search_path=pg_catalog,moria AS $$
 WITH requested AS (
   SELECT l.resource,l.authority_generation,l.definition,'target'::text AS side,l.definition->>'targetProperty' AS property
   FROM moria.native_serving_links l WHERE l.environment=env AND l.enabled AND l.target_resource=target
     AND l.definition->>'mode'='mapped' AND l.definition->>'cardinality' IN ('manyToOne','oneToOne')
   UNION ALL
   SELECT l.resource,l.authority_generation,l.definition,'source',l.definition->>'sourceProperty'
   FROM moria.native_serving_links l WHERE l.environment=env AND l.enabled AND l.source_resource=target
     AND l.definition->>'mode'='mapped' AND l.definition->>'cardinality'='oneToOne'
 ), accepted AS (
   SELECT r.*,b.definition->'schema'->'properties'->r.property AS descriptor
   FROM requested r JOIN moria.native_serving_bindings b ON b.environment=env AND b.resource=target AND b.enabled
   WHERE r.property IS DISTINCT FROM b.definition->'schema'->>'primaryKey'
 ) SELECT COALESCE(jsonb_agg(jsonb_build_object('link',resource,'generation',authority_generation::text,
   'definition',definition,'side',side,'property',property,'descriptor',descriptor) ORDER BY resource,side),'[]'::jsonb) FROM accepted
$$;
CREATE TABLE moria.native_link_projection_proofs (
 projection uuid PRIMARY KEY REFERENCES moria.runtime_projections(id),constraints jsonb NOT NULL,
 CHECK(jsonb_typeof(constraints)='array' AND jsonb_array_length(constraints)<=128)
);
CREATE TABLE moria.native_link_key_owners (
 projection uuid NOT NULL REFERENCES moria.native_link_projection_proofs(projection),property text NOT NULL,
 key_hash bytea NOT NULL CHECK(octet_length(key_hash)=32),uid uuid NOT NULL,key_bytes bytea NOT NULL,
 PRIMARY KEY(projection,property,key_hash,uid)
);
-- Keys use physical scalar equality, including normalized float zero and decimal scale.
-- The bounded hash index finds candidates; full bytes decide equality after every match.
CREATE FUNCTION moria.runtime_link_key(value jsonb,descriptor jsonb) RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT
 SET search_path=pg_catalog,moria AS $$
DECLARE text_value text;n double precision;
BEGIN
 IF value='null'::jsonb THEN RETURN NULL; END IF;
 text_value:=value#>>'{}';
 CASE descriptor->>'type'
  WHEN 'string' THEN RETURN convert_to(text_value,'UTF8');
  WHEN 'boolean' THEN RETURN boolsend(text_value::boolean);
  WHEN 'uid' THEN RETURN uuid_send(text_value::uuid);
  WHEN 'int64' THEN RETURN int8send(text_value::bigint);
  WHEN 'float64' THEN n:=text_value::double precision;RETURN float8send(CASE WHEN n=0 THEN 0::double precision ELSE n END);
  WHEN 'date' THEN RETURN date_send(moria.runtime_projection_date(text_value));
  WHEN 'timestamp' THEN RETURN timestamptz_send(moria.runtime_projection_timestamp(text_value));
  WHEN 'decimal' THEN RETURN numeric_send(trim_scale(text_value::numeric));
  ELSE RAISE EXCEPTION 'LINK_CONSTRAINT_INVALID';
 END CASE;
END $$;
CREATE FUNCTION moria.runtime_link_projection_row() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
 SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;proof jsonb;item jsonb;field text;descriptor jsonb;key bytea;digest bytea;bytes bigint:=0;
BEGIN
 SELECT * INTO p FROM moria.runtime_projections WHERE id=TG_ARGV[0]::uuid;
 IF p.id IS NULL OR TG_TABLE_SCHEMA<>'moria' OR TG_TABLE_NAME<>'runtime_projection_'||replace(p.id::text,'-','')
   OR p.status NOT IN ('building','replaying','active') THEN RAISE EXCEPTION 'LINK_CONSTRAINT_INVALID'; END IF;
 SELECT constraints INTO proof FROM moria.native_link_projection_proofs WHERE projection=p.id;
 IF proof IS DISTINCT FROM moria.runtime_link_constraints(p.environment,p.resource) THEN RAISE EXCEPTION 'LINK_CONSTRAINT_CHANGED'; END IF;
 DELETE FROM moria.native_link_key_owners WHERE projection=p.id AND uid=NEW.uid;
 IF NEW.to_revision IS NOT NULL OR NEW.deleted THEN RETURN NEW; END IF;
 FOR field,descriptor IN SELECT DISTINCT value->>'property',value->'descriptor' FROM jsonb_array_elements(proof) LOOP
   key:=moria.runtime_link_key(NEW.effective->field,descriptor);
   IF key IS NULL THEN CONTINUE; END IF;
   bytes:=bytes+octet_length(key);
   IF bytes>(p.definition->'budgets'->>'rowBytes')::bigint THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
   digest:=sha256(key);
   IF EXISTS(SELECT 1 FROM moria.native_link_key_owners k WHERE k.projection=p.id AND k.property=field
      AND k.key_hash=digest AND k.uid<>NEW.uid AND k.key_bytes=key) THEN RAISE EXCEPTION 'LINK_CARDINALITY'; END IF;
   INSERT INTO moria.native_link_key_owners VALUES(p.id,field,digest,NEW.uid,key);
 END LOOP;
 RETURN NEW;
END $$;
CREATE FUNCTION moria.runtime_link_projection_begin(projection_id uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
 SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;b moria.indexing_builds;constraints jsonb;item jsonb;
BEGIN
 SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id;
 SELECT * INTO b FROM moria.indexing_builds WHERE id=p.stage;
 PERFORM moria.indexing_claim_check(p.stage,NULL,p.owner,p.fence);
 constraints:=moria.runtime_link_constraints(p.environment,p.resource);
 IF jsonb_array_length(constraints)>128 OR b.definition->'linkConstraints' IS DISTINCT FROM constraints
   AND NOT (constraints='[]'::jsonb AND b.definition->'linkConstraints' IS NULL) THEN RAISE EXCEPTION 'LINK_CONSTRAINT_CHANGED'; END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(constraints) LOOP
   IF item->'descriptor' IS NULL OR item->'descriptor'->>'type' NOT IN ('string','boolean','uid','int64','float64','date','timestamp','decimal')
      OR p.object_schema->'properties'->(item->>'property') IS DISTINCT FROM item->'descriptor' THEN RAISE EXCEPTION 'LINK_CONSTRAINT_INVALID'; END IF;
 END LOOP;
 INSERT INTO moria.native_link_projection_proofs VALUES(p.id,constraints);
 EXECUTE format('CREATE TRIGGER native_link_projection_row AFTER INSERT OR UPDATE OF to_revision,deleted ON %s FOR EACH ROW EXECUTE FUNCTION moria.runtime_link_projection_row(%L)',moria.runtime_projection_relation(p.id),p.id::text);
END $$;
CREATE FUNCTION moria.runtime_link_projection_activate(projection_id uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
 SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;proof jsonb;
BEGIN
 SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id;
 PERFORM moria.indexing_claim_check(p.stage,NULL,p.owner,p.fence);
 SELECT constraints INTO proof FROM moria.native_link_projection_proofs WHERE projection=p.id;
 IF proof IS DISTINCT FROM moria.runtime_link_constraints(p.environment,p.resource) THEN RAISE EXCEPTION 'LINK_CONSTRAINT_CHANGED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_link_constraint_ready(env text,target text,at_revision bigint) RETURNS boolean LANGUAGE sql STABLE
 SET search_path=pg_catalog,moria AS $$
 SELECT moria.runtime_link_constraints(env,target)='[]'::jsonb OR EXISTS(
  SELECT 1 FROM moria.runtime_projections p JOIN moria.native_link_projection_proofs proof ON proof.projection=p.id
  WHERE p.environment=env AND p.resource=target AND p.activation_revision<=at_revision
   AND (p.closed_revision IS NULL OR p.closed_revision>at_revision)
   AND proof.constraints=moria.runtime_link_constraints(env,target))
$$;
CREATE FUNCTION moria.runtime_link_constraint_overlay() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT moria.runtime_link_constraint_ready(NEW.environment,NEW.object_type,NEW.revision) THEN RAISE EXCEPTION 'LINK_CONSTRAINT_NOT_READY'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER zz_runtime_link_constraint_overlay AFTER INSERT ON moria.overlay_versions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_link_constraint_overlay();
REVOKE ALL ON FUNCTION moria.runtime_link_projection_row() FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_link_projection_begin(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_link_projection_activate(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_link_constraint_overlay() FROM PUBLIC;
`;
