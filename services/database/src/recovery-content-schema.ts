/** Recovery content is exact retained membership; private spill never becomes a durable reference. */
export const recoveryContentSchema = String.raw`
CREATE TABLE moria.runtime_recovery_namespaces (
  recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),warehouse_id text COLLATE "C" NOT NULL,table_id text COLLATE "C" NOT NULL,
  namespace jsonb NOT NULL,kind text NOT NULL CHECK(kind IN ('dataset','filesystem')),
  captured_state jsonb NOT NULL,physical_fingerprint text NOT NULL,
  PRIMARY KEY(recovery_set,warehouse_id,table_id)
);
CREATE TABLE moria.runtime_recovery_roots (
  recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),id uuid NOT NULL DEFAULT gen_random_uuid(),
  kind text NOT NULL CHECK(kind IN ('dataset','filesystem')),root_hash text NOT NULL CHECK(root_hash ~ '^[0-9a-f]{64}$'),
  descriptor jsonb NOT NULL,complete boolean NOT NULL DEFAULT false,
  PRIMARY KEY(recovery_set,id),UNIQUE(recovery_set,root_hash)
);
CREATE TABLE moria.runtime_recovery_members (
  recovery_set uuid NOT NULL,warehouse_id text COLLATE "C" NOT NULL,table_id text COLLATE "C" NOT NULL,object_id text COLLATE "C" NOT NULL,
  content jsonb NOT NULL,backup_object uuid NOT NULL DEFAULT gen_random_uuid(),source_version jsonb,backup jsonb,backup_version jsonb,
  PRIMARY KEY(recovery_set,warehouse_id,table_id,object_id),
  UNIQUE(recovery_set,backup_object),
  FOREIGN KEY(recovery_set,warehouse_id,table_id) REFERENCES moria.runtime_recovery_namespaces(recovery_set,warehouse_id,table_id)
);
CREATE TABLE moria.runtime_recovery_closure_members (
  recovery_set uuid NOT NULL,root_id uuid NOT NULL,name text COLLATE "C" NOT NULL CHECK(octet_length(name) BETWEEN 1 AND 512),
  warehouse_id text COLLATE "C" NOT NULL,table_id text COLLATE "C" NOT NULL,object_id text COLLATE "C" NOT NULL,
  PRIMARY KEY(recovery_set,root_id,name),
  FOREIGN KEY(recovery_set,root_id) REFERENCES moria.runtime_recovery_roots(recovery_set,id),
  FOREIGN KEY(recovery_set,warehouse_id,table_id,object_id) REFERENCES moria.runtime_recovery_members(recovery_set,warehouse_id,table_id,object_id)
);
CREATE TABLE moria.runtime_recovery_backends (
  recovery_set uuid PRIMARY KEY REFERENCES moria.runtime_recovery_sets(id),namespace jsonb NOT NULL,physical_fingerprint text NOT NULL
);
CREATE INDEX runtime_recovery_backend_namespace ON moria.runtime_recovery_backends((namespace->>'warehouseId'),(namespace->>'tableId'));
CREATE TABLE moria.runtime_recovery_content_counters (
 recovery_set uuid PRIMARY KEY REFERENCES moria.runtime_recovery_sets(id),max_roots bigint NOT NULL,max_members bigint NOT NULL,max_bytes bigint NOT NULL,max_dump_bytes bigint NOT NULL,max_state_bytes bigint NOT NULL,
 roots bigint NOT NULL DEFAULT 0,members bigint NOT NULL DEFAULT 0,bytes bigint NOT NULL DEFAULT 0,state_bytes bigint NOT NULL DEFAULT 0,
 CHECK(roots BETWEEN 0 AND max_roots AND members BETWEEN 0 AND max_members AND bytes BETWEEN 0 AND max_bytes AND state_bytes BETWEEN 0 AND max_state_bytes)
);
CREATE TABLE moria.runtime_recovery_outputs (
 recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),warehouse_id text COLLATE "C" NOT NULL,
 table_id text COLLATE "C" NOT NULL,object_id text COLLATE "C" NOT NULL,kind text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(recovery_set,warehouse_id,table_id,object_id)
);
CREATE FUNCTION moria.runtime_recovery_namespace_state(warehouse text,table_name text,source_kind text) RETURNS jsonb
 LANGUAGE plpgsql AS $$
DECLARE result jsonb;
BEGIN
  IF source_kind='dataset' THEN
    SELECT to_jsonb(n)||jsonb_build_object('classification',to_jsonb(moria.runtime_data_labels(n.environment,n.resource)),
      'managed',(SELECT jsonb_build_object('revision',b.revision,'enabled',b.enabled,
      'physicalBindingFingerprint',b.physical_binding_fingerprint) FROM moria.native_dataset_bindings b
      WHERE b.environment=n.environment AND b.resource::text=n.resource)) INTO result
      FROM moria.native_storage_namespaces n WHERE n.warehouse_id=warehouse AND n.table_id=table_name AND n.purpose='dataset-source';
  ELSIF source_kind='filesystem' THEN
    SELECT jsonb_build_object('warehouseId',warehouse_id,'tableId',table_id,'physicalBindingFingerprint',binding_hash) INTO result
      FROM moria.fs_storage_bindings WHERE warehouse_id=warehouse AND table_id=table_name;
  END IF;
  IF result IS NULL THEN RAISE EXCEPTION 'RECOVERY_NAMESPACE_INVALID'; END IF;
  RETURN result;
END $$;
CREATE FUNCTION moria.runtime_recovery_member_check(set_id uuid,lease_owner uuid,lease_fence bigint,instance uuid,
 warehouse text,table_name text,object_name text) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE member moria.runtime_recovery_members;namespace moria.runtime_recovery_namespaces;
BEGIN
  PERFORM moria.runtime_recovery_lease_check(set_id,lease_owner,lease_fence,instance);
  SELECT * INTO member FROM moria.runtime_recovery_members WHERE recovery_set=set_id AND warehouse_id=warehouse
    AND table_id=table_name AND object_id=object_name;
  IF NOT FOUND THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
  SELECT * INTO namespace FROM moria.runtime_recovery_namespaces WHERE recovery_set=set_id AND warehouse_id=warehouse AND table_id=table_name;
  IF namespace.captured_state IS DISTINCT FROM moria.runtime_recovery_namespace_state(warehouse,table_name,namespace.kind)
    THEN RAISE EXCEPTION 'RECOVERY_NAMESPACE_CHANGED'; END IF;
  RETURN member.content;
END $$;
CREATE FUNCTION moria.runtime_recovery_content_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT'; END IF;
  IF TG_TABLE_NAME='runtime_recovery_members' THEN
    IF (to_jsonb(NEW)-ARRAY['source_version','backup','backup_version'])<>(to_jsonb(OLD)-ARRAY['source_version','backup','backup_version'])
      OR (OLD.source_version IS NOT NULL AND NEW.source_version IS DISTINCT FROM OLD.source_version)
      OR (OLD.backup IS NOT NULL AND NEW.backup IS DISTINCT FROM OLD.backup)
      OR (OLD.backup_version IS NOT NULL AND NEW.backup_version IS DISTINCT FROM OLD.backup_version) THEN RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT'; END IF;
  ELSIF TG_TABLE_NAME='runtime_recovery_roots' THEN
    IF (to_jsonb(NEW)-'complete')<>(to_jsonb(OLD)-'complete') OR (OLD.complete AND NOT NEW.complete) THEN RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT'; END IF;
  ELSIF TG_TABLE_NAME='runtime_recovery_content_counters' THEN
    IF ROW(NEW.recovery_set,NEW.max_roots,NEW.max_members,NEW.max_bytes,NEW.max_dump_bytes,NEW.max_state_bytes) IS DISTINCT FROM ROW(OLD.recovery_set,OLD.max_roots,OLD.max_members,OLD.max_bytes,OLD.max_dump_bytes,OLD.max_state_bytes)
      OR NEW.roots<OLD.roots OR NEW.members<OLD.members OR NEW.bytes<OLD.bytes OR NEW.state_bytes<OLD.state_bytes THEN RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT'; END IF;
  ELSE RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_recovery_namespace_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_namespaces FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_recovery_backend_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_backends FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_recovery_root_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_roots FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_recovery_member_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_members FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_recovery_closure_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_closure_members FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_recovery_counter_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_content_counters FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_recovery_output_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_outputs FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_namespaces
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_roots
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_members
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_closure_members
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();




-- These three tables also accept writes from narrowly validated leased backup workers.
CREATE FUNCTION moria.runtime_recovery_backup_content_mutation() RETURNS trigger LANGUAGE plpgsql
 SET search_path=pg_catalog,moria AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'RECOVERY_OPERATOR_REQUIRED'; END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_recovery_backup_content_mutation() FROM PUBLIC;
CREATE FUNCTION moria.runtime_recovery_backup_bind(set_id uuid,lease_owner uuid,lease_fence bigint,instance uuid,logical_namespace jsonb,physical text)
 RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE warehouse text:=logical_namespace->>'warehouseId';table_name text:=logical_namespace->>'tableId';
BEGIN
 PERFORM moria.runtime_recovery_lease_check(set_id,lease_owner,lease_fence,instance);
 IF jsonb_typeof(logical_namespace) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(logical_namespace))<>3
   OR jsonb_typeof(logical_namespace->'warehouseId') IS DISTINCT FROM 'string' OR jsonb_typeof(logical_namespace->'tableId') IS DISTINCT FROM 'string' OR jsonb_typeof(logical_namespace->'revision') IS DISTINCT FROM 'string' OR warehouse IS NULL OR table_name IS NULL OR warehouse!~'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$' OR table_name!~'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
   OR COALESCE(length(logical_namespace->>'revision'),0) NOT BETWEEN 1 AND 128 OR physical!~'^sha256:[0-9a-f]{64}$'
   OR (warehouse='filesystem' AND table_name='content') THEN RAISE EXCEPTION 'RECOVERY_NAMESPACE_INVALID'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('moria/storage-namespace/'||warehouse||'/'||table_name,0));
 IF EXISTS(SELECT 1 FROM moria.native_storage_namespaces WHERE warehouse_id=warehouse AND table_id=table_name)
   OR EXISTS(SELECT 1 FROM moria.fs_storage_bindings WHERE warehouse_id=warehouse AND table_id=table_name)
   THEN RAISE EXCEPTION 'RECOVERY_NAMESPACE_INVALID'; END IF;
 INSERT INTO moria.runtime_recovery_backends VALUES(set_id,logical_namespace,physical) ON CONFLICT(recovery_set) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_backends WHERE recovery_set=set_id AND namespace=logical_namespace AND physical_fingerprint=physical)
   THEN RAISE EXCEPTION 'RECOVERY_MANIFEST_CONFLICT'; END IF;
END $$;
CREATE FUNCTION moria.runtime_recovery_backup_object(set_id uuid,lease_owner uuid,lease_fence bigint,instance uuid,logical_namespace jsonb,physical text,logical_object jsonb,writing boolean)
 RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE warehouse text:=logical_object->>'warehouseId';table_name text:=logical_object->>'tableId';object_name text:=logical_object->>'objectId';
 prefix text:='recovery_'||replace(set_id::text,'-','')||'_';suffix text;amount bigint;
BEGIN
 PERFORM moria.runtime_recovery_lease_check(set_id,lease_owner,lease_fence,instance);
 IF jsonb_typeof(logical_object) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(logical_object))<>4
  OR jsonb_typeof(logical_object->'warehouseId') IS DISTINCT FROM 'string' OR jsonb_typeof(logical_object->'tableId') IS DISTINCT FROM 'string' OR jsonb_typeof(logical_object->'objectId') IS DISTINCT FROM 'string' OR logical_object->'version' IS DISTINCT FROM '1'::jsonb OR warehouse IS DISTINCT FROM logical_namespace->>'warehouseId'
  OR table_name IS DISTINCT FROM logical_namespace->>'tableId' OR object_name IS NULL OR length(object_name)>128
  OR object_name!~'^[A-Za-z0-9][A-Za-z0-9_-]*$' OR left(object_name,length(prefix))<>prefix
  THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_backends WHERE recovery_set=set_id AND namespace=logical_namespace AND physical_fingerprint=physical)
  THEN RAISE EXCEPTION 'RECOVERY_NAMESPACE_CHANGED'; END IF;
 suffix:=substr(object_name,length(prefix)+1);
 IF suffix NOT IN ('metadata','inventory','manifest') THEN
  IF suffix~'^data_[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$' THEN
   IF NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_members WHERE recovery_set=set_id AND backup_object=substr(suffix,6)::uuid)
    THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
  ELSIF suffix~'^key_[0-9]+$' THEN
   IF substr(suffix,5)::numeric>=64 THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
  ELSIF suffix~'^namespace_page_[0-9]+$' THEN
   IF substr(suffix,16)::numeric>=2 THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
  ELSIF suffix~'^(page|metadata_part|metadata_page)_[0-9]+$' THEN
   IF regexp_replace(suffix,'^.*_','')::numeric>18446744073709551615 THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
  ELSE RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
 END IF;
 IF writing THEN
  INSERT INTO moria.runtime_recovery_outputs(recovery_set,warehouse_id,table_id,object_id,kind)
   VALUES(set_id,warehouse,table_name,object_name,split_part(suffix,'_',1)) ON CONFLICT DO NOTHING;
  IF FOUND THEN
   amount:=octet_length(convert_to(logical_object::text,'UTF8'))+1024;
   UPDATE moria.runtime_recovery_content_counters SET state_bytes=state_bytes+amount WHERE recovery_set=set_id AND state_bytes+amount<=max_state_bytes;
   IF NOT FOUND THEN RAISE EXCEPTION 'RECOVERY_CONTENT_LIMIT'; END IF;
  END IF;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_outputs WHERE recovery_set=set_id AND warehouse_id=warehouse AND table_id=table_name AND object_id=object_name)
  THEN RAISE EXCEPTION 'RECOVERY_OBJECT_DENIED'; END IF;
END $$;
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_backends
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_backup_content_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_content_counters
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_backup_content_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_outputs
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_backup_content_mutation();
`;
