/** Logged effective serving projections; provisioners accept only trusted bindings and UUID identities. */
export const projectionRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_projection_bindings (
  environment text NOT NULL REFERENCES moria.environments(id),resource text NOT NULL,
  revision text NOT NULL,definition jsonb NOT NULL,enabled boolean NOT NULL DEFAULT true,
  PRIMARY KEY(environment,resource)
);
CREATE TABLE moria.runtime_projection_binding_versions (
  environment text NOT NULL,resource text NOT NULL,revision text NOT NULL,definition jsonb NOT NULL,
  PRIMARY KEY(environment,resource,revision)
);
CREATE FUNCTION moria.runtime_projection_binding_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  IF octet_length(NEW.revision) NOT BETWEEN 1 AND 128 OR octet_length(NEW.definition::text)>65536 THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  IF TG_OP='UPDATE' THEN
    IF ROW(NEW.environment,NEW.resource) IS DISTINCT FROM ROW(OLD.environment,OLD.resource) THEN RAISE EXCEPTION 'IMMUTABLE_PROJECTION_IDENTITY'; END IF;
    IF NEW.revision=OLD.revision THEN
      IF NEW.definition<>OLD.definition OR (NOT OLD.enabled AND NEW.enabled) THEN RAISE EXCEPTION 'FRESH_PROJECTION_REVISION_REQUIRED'; END IF;
      RETURN NEW;
    END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_projection_binding_revision BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_projection_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_projection_binding_revision();
CREATE FUNCTION moria.runtime_projection_binding_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='UPDATE' AND NEW.revision=OLD.revision THEN RETURN NEW; END IF;
  BEGIN
    INSERT INTO moria.runtime_projection_binding_versions VALUES(NEW.environment,NEW.resource,NEW.revision,NEW.definition);
  EXCEPTION WHEN unique_violation THEN RAISE EXCEPTION 'PROJECTION_REVISION_REUSED'; END;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_projection_binding_record AFTER INSERT OR UPDATE ON moria.runtime_projection_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_projection_binding_record();
CREATE FUNCTION moria.runtime_projection_history_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'IMMUTABLE_PROJECTION_HISTORY'; END $$;
CREATE TRIGGER runtime_projection_binding_history_immutable BEFORE UPDATE OR DELETE ON moria.runtime_projection_binding_versions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_projection_history_immutable();

CREATE TABLE moria.runtime_projections (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),stage uuid NOT NULL UNIQUE REFERENCES moria.source_stages(id),
  environment text NOT NULL REFERENCES moria.environments(id),resource text NOT NULL,binding_revision text NOT NULL,
  definition jsonb NOT NULL,object_schema jsonb NOT NULL,source_schema jsonb NOT NULL,columns jsonb NOT NULL,
  owner uuid NOT NULL,fence bigint NOT NULL,status text NOT NULL CHECK(status IN ('building','replaying','active','retired')),
  base_revision bigint NOT NULL,build_after uuid,build_complete boolean NOT NULL DEFAULT false,
  replay_revision bigint NOT NULL,replay_uid uuid,applied_through bigint NOT NULL,
  indexes_complete boolean NOT NULL DEFAULT false,row_count bigint NOT NULL DEFAULT 0,
  activation_revision bigint,closed_revision bigint,created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX runtime_projection_one_active ON moria.runtime_projections(environment,resource) WHERE status='active';
CREATE INDEX runtime_projection_history ON moria.runtime_projections(environment,resource,activation_revision DESC) WHERE activation_revision IS NOT NULL;
CREATE TABLE moria.runtime_projection_layouts (
  projection uuid NOT NULL REFERENCES moria.runtime_projections(id),name text NOT NULL,ordinal integer NOT NULL,
  definition jsonb NOT NULL,created boolean NOT NULL DEFAULT false,PRIMARY KEY(projection,name),UNIQUE(projection,ordinal)
);
CREATE INDEX runtime_projection_overlay_replay ON moria.overlay_versions(environment,object_type,revision,uid);

CREATE FUNCTION moria.runtime_projection_relation(projection_id uuid) RETURNS text LANGUAGE sql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$ SELECT 'moria.'||quote_ident('runtime_projection_'||replace(projection_id::text,'-','')) $$;
CREATE FUNCTION moria.runtime_projection_date(value text) RETURNS date LANGUAGE plpgsql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$
BEGIN
  IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' OR left(value,4)='0000' THEN RAISE EXCEPTION 'VALUE_INVALID'; END IF;
  RETURN value::date;
END $$;
CREATE FUNCTION moria.runtime_projection_timestamp(value text) RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$
DECLARE result timestamptz;canonical text;
BEGIN
  IF value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$' OR left(value,4)='0000' THEN RAISE EXCEPTION 'VALUE_INVALID'; END IF;
  result:=value::timestamptz;
  canonical:=left(value,19)||'.'||rpad(split_part(split_part(value,'.',2),'Z',1),6,'0')||'Z';
  IF to_char(result AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')<>canonical THEN RAISE EXCEPTION 'VALUE_INVALID'; END IF;
  RETURN result;
END $$;
CREATE FUNCTION moria.runtime_projection_array(value jsonb) RETURNS text[] LANGUAGE plpgsql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$
DECLARE result text[];
BEGIN
  IF value='null'::jsonb THEN RETURN NULL; END IF;
  IF jsonb_typeof(value)<>'array' OR jsonb_array_length(value)>100000
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(value) x WHERE x='null'::jsonb OR jsonb_typeof(x) NOT IN ('string','number','boolean')) THEN RAISE EXCEPTION 'VALUE_INVALID'; END IF;
  SELECT ARRAY(SELECT jsonb_array_elements_text(value)) INTO result;RETURN result;
END $$;
CREATE FUNCTION moria.runtime_projection_dates(value text[]) RETURNS date[] LANGUAGE sql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$ SELECT ARRAY(SELECT moria.runtime_projection_date(x) FROM unnest(value) WITH ORDINALITY AS v(x,n) ORDER BY n) $$;
CREATE FUNCTION moria.runtime_projection_timestamps(value text[]) RETURNS timestamptz[] LANGUAGE sql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$ SELECT ARRAY(SELECT moria.runtime_projection_timestamp(x) FROM unnest(value) WITH ORDINALITY AS v(x,n) ORDER BY n) $$;

CREATE FUNCTION moria.runtime_projection_type(descriptor jsonb) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$
DECLARE result text;p integer;s integer;
BEGIN
  CASE descriptor->>'type'
    WHEN 'string' THEN result:='text';WHEN 'boolean' THEN result:='boolean';WHEN 'uid' THEN result:='uuid';
    WHEN 'int64' THEN result:='bigint';WHEN 'float64' THEN result:='double precision';
    WHEN 'date' THEN result:='date';WHEN 'timestamp' THEN result:='timestamp with time zone';
    WHEN 'decimal' THEN
      p:=(descriptor->>'precision')::integer;s:=(descriptor->>'scale')::integer;
      IF p NOT BETWEEN 1 AND 38 OR s NOT BETWEEN 0 AND p THEN RAISE EXCEPTION 'TYPE_INVALID'; END IF;
      result:=format('numeric(%s,%s)',p,s);
    WHEN 'array' THEN
      IF descriptor->'items'->>'type'='array' OR descriptor->'items'->>'nullable' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'TYPE_INVALID'; END IF;
      result:=moria.runtime_projection_type(descriptor->'items')||'[]';
    ELSE RAISE EXCEPTION 'TYPE_INVALID';
  END CASE;
  RETURN result;
END $$;
CREATE FUNCTION moria.runtime_projection_expression(descriptor jsonb,side text,field_name text) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT
  SET search_path=pg_catalog,moria AS $$
DECLARE value_sql text;array_sql text;
BEGIN
  IF side NOT IN ('source','effective') THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  value_sql:=format('(%I->>%L)',side,field_name);
  CASE descriptor->>'type'
    WHEN 'date' THEN RETURN format('moria.runtime_projection_date(%s)',value_sql);
    WHEN 'timestamp' THEN RETURN format('moria.runtime_projection_timestamp(%s)',value_sql);
    WHEN 'array' THEN
      array_sql:=format('moria.runtime_projection_array(%I->%L)',side,field_name);
      IF descriptor->'items'->>'type'='date' THEN RETURN format('moria.runtime_projection_dates(%s)',array_sql); END IF;
      IF descriptor->'items'->>'type'='timestamp' THEN RETURN format('moria.runtime_projection_timestamps(%s)',array_sql); END IF;
      RETURN format('(%s)::%s',array_sql,moria.runtime_projection_type(descriptor));
    ELSE RETURN format('(%s)::%s',value_sql,moria.runtime_projection_type(descriptor));
  END CASE;
END $$;
-- Native creation admission and retained-overlay validation own C03 value codecs.
-- A later source removal must also preserve full required-field shape, including unmaterialized properties.
CREATE FUNCTION moria.runtime_projection_required_fields(row_value jsonb,properties jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT jsonb_typeof(row_value)='object' AND jsonb_typeof(properties)='object'
    AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(row_value) AS keys(name) WHERE NOT (properties ? keys.name))
    AND NOT EXISTS(SELECT 1 FROM jsonb_each(properties) p WHERE p.value->'nullable' IS DISTINCT FROM 'true'::jsonb
      AND (row_value->p.key IS NULL OR row_value->p.key='null'::jsonb))
$$;

CREATE FUNCTION moria.runtime_projection_check(projection_id uuid,worker uuid,worker_fence bigint) RETURNS moria.runtime_projections LANGUAGE plpgsql
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;
BEGIN
  SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id;
  IF NOT FOUND OR p.owner<>worker OR p.fence<>worker_fence OR p.status NOT IN ('building','replaying') THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  PERFORM moria.runtime_indexing_staging_shared(p.stage);
  PERFORM moria.indexing_claim_check(p.stage,NULL,worker,worker_fence);
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_projection_bindings b WHERE b.environment=p.environment AND b.resource=p.resource
    AND b.enabled AND b.revision=p.binding_revision AND b.definition=p.definition) THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  RETURN p;
END $$;

CREATE FUNCTION moria.runtime_projection_begin(stage_id uuid,worker uuid,worker_fence bigint) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE b moria.indexing_builds;s moria.source_stages;binding moria.runtime_projection_bindings;p moria.runtime_projections;
  cols jsonb:='{}';field_name text;descriptor jsonb;column_name text;side text;n integer;ddl text;kind text;source_schema jsonb;properties jsonb;
  layout jsonb;sort_key jsonb;safe_sql text;key_width integer;role_name text;existing_role oid;base bigint;
BEGIN
  PERFORM moria.runtime_indexing_staging_shared(stage_id);
  PERFORM moria.indexing_claim_check(stage_id,NULL,worker,worker_fence);
  SELECT * INTO b FROM moria.indexing_builds WHERE id=stage_id;
  SELECT * INTO s FROM moria.source_stages WHERE id=stage_id;
  source_schema:=COALESCE(b.definition->'sourceColumns',s.object_schema->'properties');
  SELECT * INTO binding FROM moria.runtime_projection_bindings WHERE environment=b.environment AND resource=b.target AND enabled;
  IF NOT FOUND OR b.definition->>'projectionRevision' IS DISTINCT FROM binding.revision OR s.object_schema IS DISTINCT FROM b.definition->'schema'
    OR s.environment IS DISTINCT FROM b.environment OR s.object_type IS DISTINCT FROM b.target
    OR EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE build=stage_id AND status<>'sealed') THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  SELECT * INTO p FROM moria.runtime_projections WHERE stage=stage_id FOR UPDATE;
  IF FOUND THEN
    IF p.binding_revision<>binding.revision OR p.definition<>binding.definition OR p.object_schema<>s.object_schema OR p.source_schema<>source_schema OR p.status NOT IN ('building','replaying')
      THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
    UPDATE moria.runtime_projections SET owner=worker,fence=worker_fence WHERE id=p.id;RETURN p.id;
  END IF;
  IF binding.definition->>'version'<>'1' OR jsonb_typeof(binding.definition->'sourceFields')<>'array'
    OR jsonb_typeof(binding.definition->'effectiveFields')<>'array' OR jsonb_typeof(binding.definition->'layouts')<>'array'
    OR jsonb_array_length(binding.definition->'sourceFields')+jsonb_array_length(binding.definition->'effectiveFields')>128
    OR jsonb_array_length(binding.definition->'layouts') NOT BETWEEN 1 AND 16
    OR jsonb_typeof(binding.definition->'readerRoles')<>'array' OR jsonb_array_length(binding.definition->'readerRoles') NOT BETWEEN 1 AND 32
    THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  FOREACH side IN ARRAY ARRAY['source','effective'] LOOP
    n:=0;
    properties:=CASE WHEN side='source' THEN source_schema ELSE s.object_schema->'properties' END;
    FOR field_name IN SELECT jsonb_array_elements_text(binding.definition->(side||'Fields')) LOOP
      descriptor:=properties->field_name;
      IF descriptor IS NULL OR octet_length(field_name)>256 OR cols->field_name ? side THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
      PERFORM moria.runtime_projection_type(descriptor);
      column_name:=left(side,1)||n::text;n:=n+1;
      cols:=jsonb_set(cols,ARRAY[field_name],COALESCE(cols->field_name,'{}')||jsonb_build_object(side,column_name,'descriptor',descriptor));
      IF side='source' THEN cols:=jsonb_set(cols,ARRAY[field_name,'sourceDescriptor'],descriptor); END IF;
    END LOOP;
  END LOOP;
  SELECT revision INTO base FROM moria.environments WHERE id=b.environment;
  INSERT INTO moria.runtime_projections(stage,environment,resource,binding_revision,definition,object_schema,source_schema,columns,owner,fence,status,base_revision,replay_revision,applied_through)
    VALUES(stage_id,b.environment,b.target,binding.revision,binding.definition,s.object_schema,source_schema,cols,worker,worker_fence,'building',base,base,base) RETURNING * INTO p;
  ddl:='CREATE TABLE '||moria.runtime_projection_relation(p.id)||'(uid uuid NOT NULL,from_revision bigint NOT NULL,to_revision bigint,deleted boolean NOT NULL,source jsonb NOT NULL,effective jsonb NOT NULL';
  FOREACH side IN ARRAY ARRAY['source','effective'] LOOP
    properties:=CASE WHEN side='source' THEN source_schema ELSE s.object_schema->'properties' END;
    FOR field_name,descriptor IN SELECT key,value FROM jsonb_each(properties) LOOP
      column_name:=cols->field_name->>side;IF column_name IS NULL THEN CONTINUE; END IF;
      kind:=moria.runtime_projection_type(descriptor);
      ddl:=ddl||format(',%I %s%s GENERATED ALWAYS AS (%s) STORED',column_name,kind,
        CASE WHEN kind IN ('text','text[]') THEN ' COLLATE "C"' ELSE '' END,
        moria.runtime_projection_expression(descriptor,side,field_name));
    END LOOP;
  END LOOP;
  n:=0;
  FOR layout IN SELECT value FROM jsonb_array_elements(binding.definition->'layouts') LOOP
    IF octet_length(layout->>'name') NOT BETWEEN 1 AND 64 OR layout->>'name'!~'^[A-Za-z0-9_-]+$'
      OR jsonb_typeof(layout->'sort')<>'array' OR jsonb_array_length(layout->'sort')>8 OR (layout->>'keyBytes')::integer NOT BETWEEN 64 AND 1800 THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
    safe_sql:='64';
    FOR sort_key IN SELECT value FROM jsonb_array_elements(layout->'sort') LOOP
      field_name:=sort_key->>'property';descriptor:=s.object_schema->'properties'->field_name;
      IF cols->field_name->>'effective' IS NULL OR sort_key->>'direction' NOT IN ('asc','desc') OR descriptor->>'type'='array' THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
      IF descriptor->>'type'='string' THEN safe_sql:=safe_sql||format('+8+COALESCE(octet_length(effective->>%L),0)',field_name);
      ELSE safe_sql:=safe_sql||'+72';END IF;
    END LOOP;
    ddl:=ddl||format(',%I boolean GENERATED ALWAYS AS ((%s)<= %s) STORED','k'||n,safe_sql,(layout->>'keyBytes')::integer);
    INSERT INTO moria.runtime_projection_layouts VALUES(p.id,layout->>'name',n,layout,false);n:=n+1;
  END LOOP;
  ddl:=ddl||format(',PRIMARY KEY(uid,from_revision),CHECK(to_revision IS NULL OR to_revision>from_revision),CHECK((effective->>%L)::uuid IS NOT DISTINCT FROM uid))',s.object_schema->>'primaryKey');
  EXECUTE ddl;
  EXECUTE format('CREATE UNIQUE INDEX %I ON %s(uid) WHERE to_revision IS NULL','runtime_pi_'||replace(p.id::text,'-','')||'_uid',moria.runtime_projection_relation(p.id));
  EXECUTE format('CREATE INDEX %I ON %s(to_revision,uid,from_revision) WHERE to_revision IS NOT NULL','runtime_pi_'||replace(p.id::text,'-','')||'_history',moria.runtime_projection_relation(p.id));
  PERFORM moria.runtime_link_projection_begin(p.id);
  FOR role_name IN SELECT DISTINCT value FROM (SELECT jsonb_array_elements_text(binding.definition->'readerRoles') value UNION ALL SELECT session_user::text) roles LOOP
    SELECT oid INTO existing_role FROM pg_roles WHERE rolname=role_name;
    IF existing_role IS NULL OR octet_length(role_name)>63 OR NOT has_table_privilege(role_name,'moria.staged_source_rows','SELECT') THEN RAISE EXCEPTION 'PROJECTION_ROLE_INVALID'; END IF;
    EXECUTE format('GRANT SELECT ON %s TO %I',moria.runtime_projection_relation(p.id),role_name);
  END LOOP;
  PERFORM moria.indexing_claim_check(stage_id,NULL,worker,worker_fence);
  RETURN p.id;
END $$;

CREATE FUNCTION moria.runtime_projection_base_keys(stage_id uuid,env text,target text,at_revision bigint,after_uid uuid,row_limit integer)
 RETURNS TABLE(uid uuid) LANGUAGE sql STABLE AS $$
  SELECT candidates.uid FROM (
    (SELECT r.uid FROM moria.staged_source_rows r WHERE r.stage=stage_id
      AND r.uid>=COALESCE(after_uid,'00000000-0000-0000-0000-000000000000'::uuid)
      AND (after_uid IS NULL OR r.uid>after_uid) ORDER BY r.uid LIMIT 1000)
    UNION ALL
    (SELECT c.uid FROM moria.native_object_creations c WHERE c.environment=env AND c.object_type=target
      AND c.created_revision<=at_revision AND c.uid>=COALESCE(after_uid,'00000000-0000-0000-0000-000000000000'::uuid)
      AND (after_uid IS NULL OR c.uid>after_uid)
      AND NOT EXISTS(SELECT 1 FROM moria.staged_source_rows r WHERE r.stage=stage_id AND r.uid=c.uid)
      ORDER BY c.uid LIMIT 1000)
  ) candidates ORDER BY candidates.uid LIMIT row_limit
$$;
REVOKE ALL ON FUNCTION moria.runtime_projection_base_keys(uuid,text,text,bigint,uuid,integer) FROM PUBLIC;
CREATE FUNCTION moria.runtime_projection_build_page(projection_id uuid,worker uuid,worker_fence bigint) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;item record;through_uid uuid;amount integer:=0;bytes bigint:=0;added bigint;complete boolean;
  max_rows integer;max_bytes bigint;row_bytes bigint;relation text;
BEGIN
  SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id FOR UPDATE;
  p:=moria.runtime_projection_check(projection_id,worker,worker_fence);
  IF p.build_complete THEN RETURN true; END IF;
  max_rows:=(p.definition->'budgets'->>'batchRows')::integer;max_bytes:=(p.definition->'budgets'->>'batchBytes')::bigint;row_bytes:=(p.definition->'budgets'->>'rowBytes')::bigint;
  IF max_rows NOT BETWEEN 1 AND 1000 OR max_bytes NOT BETWEEN 1 AND 67108864 OR row_bytes NOT BETWEEN 1 AND 16777216 OR row_bytes>max_bytes THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  FOR item IN WITH keys AS MATERIALIZED(SELECT uid FROM moria.runtime_projection_base_keys(p.stage,p.environment,p.resource,p.base_revision,p.build_after,max_rows))
    SELECT r.uid,octet_length(COALESCE(r.source_value,r.value)::text)+octet_length((r.value||COALESCE(o.overrides,'{}'))::text) AS bytes,
    CASE WHEN r.action_created THEN moria.runtime_projection_required_fields(r.source_value,p.source_schema)
      AND moria.runtime_projection_required_fields(r.value||COALESCE(o.overrides,'{}'),p.object_schema->'properties') ELSE true END AS shape_valid
    FROM keys k CROSS JOIN LATERAL(SELECT * FROM moria.runtime_object_base_stage(p.stage,p.environment,p.resource,p.base_revision) r WHERE r.uid=k.uid OFFSET 0) r
    LEFT JOIN LATERAL (SELECT overrides FROM moria.overlay_versions o WHERE o.environment=p.environment AND o.object_type=p.resource AND o.uid=r.uid AND o.revision<=p.base_revision ORDER BY revision DESC LIMIT 1) o ON TRUE
    ORDER BY k.uid LOOP
    IF item.shape_valid IS NOT TRUE THEN RAISE EXCEPTION 'VALUE_INVALID'; END IF;
    IF item.bytes>row_bytes THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
    IF bytes+item.bytes>max_bytes THEN EXIT; END IF;
    through_uid:=item.uid;bytes:=bytes+item.bytes;amount:=amount+1;
  END LOOP;
  relation:=moria.runtime_projection_relation(p.id);
  IF through_uid IS NOT NULL THEN
    EXECUTE format('INSERT INTO %s(uid,from_revision,to_revision,deleted,source,effective)
      SELECT r.uid,$2,NULL,COALESCE(o.tombstone,false),COALESCE(r.source_value,r.value),r.value||COALESCE(o.overrides,''{}'')
      FROM moria.runtime_object_base_stage($1,$3,$4,$2) r LEFT JOIN LATERAL (SELECT overrides,tombstone FROM moria.overlay_versions WHERE environment=$3 AND object_type=$4 AND uid=r.uid AND revision<=$2 ORDER BY revision DESC LIMIT 1) o ON TRUE
      WHERE ($5::uuid IS NULL OR r.uid>$5) AND r.uid<=$6',relation)
      USING p.stage,p.base_revision,p.environment,p.resource,p.build_after,through_uid;
    GET DIAGNOSTICS added=ROW_COUNT;IF added<>amount THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
  END IF;
  complete:=NOT EXISTS(SELECT 1 FROM moria.runtime_object_base_stage(p.stage,p.environment,p.resource,p.base_revision) WHERE (COALESCE(through_uid,p.build_after) IS NULL OR uid>COALESCE(through_uid,p.build_after)));
  UPDATE moria.runtime_projections SET build_after=COALESCE(through_uid,build_after),row_count=row_count+amount,build_complete=complete,
    status=CASE WHEN complete THEN 'replaying' ELSE 'building' END WHERE id=p.id;
  PERFORM moria.runtime_projection_check(projection_id,worker,worker_fence);
  RETURN complete;
END $$;

CREATE FUNCTION moria.runtime_projection_build_index(projection_id uuid,worker uuid,worker_fence bigint,layout_ordinal integer) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;layout moria.runtime_projection_layouts;key jsonb;columns_sql text:='';column_name text;
  relation text;prefix text;safe_name text;counted bigint;missing boolean;
BEGIN
  SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id FOR UPDATE;
  p:=moria.runtime_projection_check(projection_id,worker,worker_fence);
  IF NOT p.build_complete THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
  SELECT * INTO layout FROM moria.runtime_projection_layouts WHERE projection=p.id AND ordinal=layout_ordinal;
  IF NOT FOUND THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  IF layout.created THEN RETURN; END IF;
  IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/projection/ddl/1',0)) THEN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/projection/ddl/2',0)) THEN RAISE EXCEPTION 'PROJECTION_DDL_BUSY'; END IF;
  END IF;
  relation:=moria.runtime_projection_relation(p.id);prefix:='runtime_pi_'||replace(p.id::text,'-','')||'_'||layout_ordinal;
  safe_name:='k'||layout_ordinal;
  FOR key IN SELECT value FROM jsonb_array_elements(layout.definition->'sort') LOOP
    column_name:=p.columns->(key->>'property')->>'effective';
    IF column_name IS NULL OR key->>'direction' NOT IN ('asc','desc') THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
    columns_sql:=columns_sql||format('%I %s NULLS LAST,',column_name,CASE WHEN key->>'direction'='asc' THEN 'ASC' ELSE 'DESC' END);
  END LOOP;
  columns_sql:=columns_sql||'uid ASC';
  EXECUTE format('CREATE INDEX %I ON %s(%s) WHERE to_revision IS NULL AND NOT deleted AND %I',prefix||'_c',relation,columns_sql,safe_name);
  EXECUTE format('CREATE INDEX %I ON %s(%s,from_revision) INCLUDE(to_revision) WHERE NOT deleted AND %I',prefix||'_h',relation,columns_sql,safe_name);
  EXECUTE format('CREATE INDEX %I ON %s(uid,from_revision) INCLUDE(to_revision) WHERE NOT deleted AND NOT %I',prefix||'_o',relation,safe_name);
  UPDATE moria.runtime_projection_layouts SET created=true WHERE projection=p.id AND ordinal=layout_ordinal;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_projection_layouts WHERE projection=p.id AND NOT created) THEN
    EXECUTE format('SELECT count(*) FROM %s WHERE to_revision IS NULL',relation) INTO counted;
    IF counted<>p.row_count OR counted<>(SELECT count(*) FROM moria.runtime_object_base_stage(p.stage,p.environment,p.resource,p.base_revision)) THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM moria.runtime_object_base_stage($1,$2,$3,$4) s WHERE NOT EXISTS(SELECT 1 FROM %s x WHERE x.uid=s.uid AND x.to_revision IS NULL))',relation) INTO missing USING p.stage,p.environment,p.resource,p.base_revision;
    IF missing THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
    UPDATE moria.runtime_projections SET indexes_complete=true WHERE id=p.id;
    EXECUTE format('ANALYZE %s',relation);
  END IF;
  PERFORM moria.runtime_projection_check(projection_id,worker,worker_fence);
END $$;

CREATE FUNCTION moria.runtime_projection_apply(projection_id uuid,target_uid uuid,event_revision bigint,overrides jsonb,tombstone boolean) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;canonical jsonb;mapped jsonb;current_revision bigint;effective_value jsonb;bytes bigint;relation text;changed bigint;created boolean;
BEGIN
  SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  SELECT source_value,value,action_created INTO canonical,mapped,created FROM moria.runtime_object_base_stage(p.stage,p.environment,p.resource,event_revision) WHERE uid=target_uid;
  IF NOT FOUND THEN RETURN 0; END IF;
  relation:=moria.runtime_projection_relation(p.id);
  EXECUTE format('SELECT from_revision FROM %s WHERE uid=$1 AND to_revision IS NULL FOR UPDATE',relation) INTO current_revision USING target_uid;
  IF current_revision IS NULL THEN
    IF NOT created OR NOT EXISTS(SELECT 1 FROM moria.native_object_creations c WHERE c.environment=p.environment AND c.object_type=p.resource AND c.uid=target_uid AND c.created_revision=event_revision)
      THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
  ELSIF current_revision>=event_revision THEN RAISE EXCEPTION 'PROJECTION_REPLAY_ORDER'; END IF;
  effective_value:=mapped||overrides;bytes:=octet_length(canonical::text)+octet_length(effective_value::text);
  IF created AND (moria.runtime_projection_required_fields(canonical,p.source_schema) IS NOT TRUE
    OR moria.runtime_projection_required_fields(effective_value,p.object_schema->'properties') IS NOT TRUE) THEN RAISE EXCEPTION 'VALUE_INVALID'; END IF;
  IF bytes>(p.definition->'budgets'->>'rowBytes')::bigint THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
  IF current_revision IS NOT NULL THEN
    EXECUTE format('UPDATE %s SET to_revision=$2 WHERE uid=$1 AND to_revision IS NULL',relation) USING target_uid,event_revision;
    GET DIAGNOSTICS changed=ROW_COUNT;IF changed<>1 THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
  ELSE UPDATE moria.runtime_projections SET row_count=row_count+1 WHERE id=p.id; END IF;
  EXECUTE format('INSERT INTO %s(uid,from_revision,to_revision,deleted,source,effective) VALUES($1,$2,NULL,$3,$4,$5)',relation)
    USING target_uid,event_revision,tombstone,canonical,effective_value;
  RETURN bytes;
END $$;

CREATE FUNCTION moria.runtime_projection_replay(projection_id uuid,worker uuid,worker_fence bigint,barrier bigint DEFAULT NULL) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;item moria.overlay_versions;through_revision bigint;through_uid uuid;target bigint;
  bytes bigint:=0;row_bytes bigint;amount integer:=0;max_rows integer;max_bytes bigint;complete boolean;
BEGIN
  SELECT * INTO p FROM moria.runtime_projections WHERE id=projection_id FOR UPDATE;
  p:=moria.runtime_projection_check(projection_id,worker,worker_fence);
  IF NOT p.build_complete OR NOT p.indexes_complete THEN RAISE EXCEPTION 'PROJECTION_INCOMPLETE'; END IF;
  target:=COALESCE(barrier,(SELECT revision FROM moria.environments WHERE id=p.environment));
  IF target<p.applied_through THEN RAISE EXCEPTION 'PROJECTION_REPLAY_ORDER'; END IF;
  max_rows:=(p.definition->'budgets'->>CASE WHEN barrier IS NULL THEN 'batchRows' ELSE 'commitReplayRows' END)::integer;
  max_bytes:=(p.definition->'budgets'->>CASE WHEN barrier IS NULL THEN 'batchBytes' ELSE 'commitReplayBytes' END)::bigint;
  IF max_rows NOT BETWEEN 1 AND 1000 OR max_bytes NOT BETWEEN 1 AND 67108864
    OR (barrier IS NOT NULL AND (max_rows>256 OR max_bytes>16777216)) THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  through_revision:=p.replay_revision;through_uid:=p.replay_uid;
  FOR item IN SELECT * FROM moria.overlay_versions WHERE environment=p.environment AND object_type=p.resource
    AND revision<=target AND (revision>p.replay_revision OR (revision=p.replay_revision AND p.replay_uid IS NOT NULL AND uid>p.replay_uid))
    ORDER BY revision,uid LIMIT max_rows LOOP
    SELECT octet_length(source_value::text)+octet_length((value||item.overrides)::text) INTO row_bytes FROM moria.runtime_object_base_stage(p.stage,p.environment,p.resource,item.revision) WHERE uid=item.uid;
    row_bytes:=COALESCE(row_bytes,0);
    IF bytes+row_bytes>max_bytes THEN
      IF barrier IS NOT NULL THEN RAISE EXCEPTION 'PROJECTION_CATCHUP_REQUIRED'; END IF;
      EXIT;
    END IF;
    PERFORM moria.runtime_projection_apply(p.id,item.uid,item.revision,item.overrides,item.tombstone);
    bytes:=bytes+row_bytes;
    through_revision:=item.revision;through_uid:=item.uid;amount:=amount+1;
  END LOOP;
  complete:=NOT EXISTS(SELECT 1 FROM moria.overlay_versions WHERE environment=p.environment AND object_type=p.resource
    AND revision<=target AND (revision>through_revision OR (revision=through_revision AND through_uid IS NOT NULL AND uid>through_uid)));
  IF barrier IS NOT NULL AND NOT complete THEN RAISE EXCEPTION 'PROJECTION_CATCHUP_REQUIRED'; END IF;
  UPDATE moria.runtime_projections SET replay_revision=CASE WHEN complete THEN target ELSE through_revision END,
    replay_uid=CASE WHEN complete THEN NULL ELSE through_uid END,applied_through=CASE WHEN complete THEN target ELSE applied_through END
    WHERE id=p.id;
  PERFORM moria.runtime_projection_check(projection_id,worker,worker_fence);
  RETURN complete;
END $$;

CREATE FUNCTION moria.runtime_projection_overlay() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;current_revision bigint;
BEGIN
  IF TG_RELID<>'moria.overlay_versions'::regclass THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  SELECT * INTO p FROM moria.runtime_projections WHERE environment=NEW.environment AND resource=NEW.object_type AND status='active' FOR UPDATE;
  IF NOT FOUND THEN RETURN NEW; END IF;
  SELECT revision INTO current_revision FROM moria.environments WHERE id=NEW.environment;
  IF NEW.revision<>current_revision OR NEW.revision<p.applied_through THEN RAISE EXCEPTION 'PROJECTION_REPLAY_ORDER'; END IF;
  PERFORM moria.runtime_projection_apply(p.id,NEW.uid,NEW.revision,NEW.overrides,NEW.tombstone);
  UPDATE moria.runtime_projections SET applied_through=NEW.revision WHERE id=p.id;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_projection_overlay AFTER INSERT ON moria.overlay_versions FOR EACH ROW EXECUTE FUNCTION moria.runtime_projection_overlay();

CREATE FUNCTION moria.runtime_projection_activate() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
DECLARE p moria.runtime_projections;b moria.indexing_builds;binding moria.runtime_projection_bindings;reused boolean;
BEGIN
  IF TG_RELID<>'moria.source_generations'::regclass THEN RAISE EXCEPTION 'PROJECTION_INVALID'; END IF;
  reused:=moria.runtime_source_generation_reused(NEW.environment,NEW.object_type,NEW.revision,NEW.stage);
  SELECT * INTO binding FROM moria.runtime_projection_bindings WHERE environment=NEW.environment AND resource=NEW.object_type AND enabled;
  IF reused THEN
    IF binding.resource IS NULL OR NOT EXISTS(SELECT 1 FROM moria.runtime_projections active
     WHERE active.stage=NEW.stage AND active.environment=NEW.environment AND active.resource=NEW.object_type
      AND active.status='active' AND active.activation_revision<NEW.revision AND active.closed_revision IS NULL
      AND active.object_schema=NEW.object_schema AND active.definition=binding.definition AND active.binding_revision=binding.revision
      AND active.applied_through>=COALESCE((SELECT max(o.revision) FROM moria.overlay_versions o
       WHERE o.environment=NEW.environment AND o.object_type=NEW.object_type AND o.revision<NEW.revision),0))
    THEN RAISE EXCEPTION 'PROJECTION_NOT_READY';END IF;
    RETURN NEW;
  END IF;
  IF FOUND THEN
    SELECT * INTO p FROM moria.runtime_projections WHERE stage=NEW.stage FOR UPDATE;
    SELECT * INTO b FROM moria.indexing_builds WHERE id=NEW.stage;
    IF p.id IS NULL OR b.id IS NULL OR p.object_schema<>NEW.object_schema OR p.definition<>binding.definition
      OR p.binding_revision<>binding.revision OR b.definition->>'projectionRevision' IS DISTINCT FROM binding.revision
      THEN RAISE EXCEPTION 'PROJECTION_NOT_READY'; END IF;
    PERFORM moria.runtime_projection_replay(p.id,b.owner,b.fence,NEW.revision-1);
    PERFORM moria.runtime_link_projection_activate(p.id);
  END IF;
  UPDATE moria.runtime_projections SET status='retired',closed_revision=NEW.revision
    WHERE environment=NEW.environment AND resource=NEW.object_type AND status='active';
  IF p.id IS NOT NULL THEN
    UPDATE moria.runtime_projections SET status='active',activation_revision=NEW.revision,applied_through=NEW.revision WHERE id=p.id;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_projection_activate AFTER INSERT ON moria.source_generations FOR EACH ROW EXECUTE FUNCTION moria.runtime_projection_activate();
REVOKE ALL ON FUNCTION moria.runtime_projection_apply(uuid,uuid,bigint,jsonb,boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_projection_overlay() FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_projection_activate() FROM PUBLIC;
`;
