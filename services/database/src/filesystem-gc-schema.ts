/** Exact uncommitted filesystem upload retirement; retained file history is never a deletion candidate. */
export const filesystemGcSchema = String.raw`
ALTER TABLE moria.native_filesystem_uploads ADD CONSTRAINT native_filesystem_upload_object CHECK (
 location=jsonb_build_object('version',1,'warehouseId','filesystem','tableId','content','objectId',location->>'objectId')
 AND location->>'objectId' IS NOT NULL AND location->>'objectId'=(location->>'objectId')::uuid::text
);
CREATE UNIQUE INDEX native_filesystem_upload_object ON moria.native_filesystem_uploads((location->>'objectId'));
CREATE TABLE moria.native_filesystem_gc_policies (
 id uuid PRIMARY KEY,request_key text NOT NULL UNIQUE,revision text NOT NULL,namespace jsonb NOT NULL,
 physical_hash text NOT NULL,minimum_age_seconds bigint NOT NULL CHECK(minimum_age_seconds>=86400),
 recheck_seconds integer NOT NULL CHECK(recheck_seconds BETWEEN 30 AND 604800),
 incarnation uuid NOT NULL,approved_role name NOT NULL,enabled boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE moria.native_filesystem_upload_tombstones (
 upload uuid PRIMARY KEY REFERENCES moria.native_filesystem_uploads(id),
 policy uuid NOT NULL REFERENCES moria.native_filesystem_gc_policies(id),
 retired_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE moria.native_filesystem_gc_candidates (
 upload uuid PRIMARY KEY REFERENCES moria.native_filesystem_upload_tombstones(upload),
 policy uuid NOT NULL REFERENCES moria.native_filesystem_gc_policies(id),first_unreferenced timestamptz,
 status text NOT NULL CHECK(status IN ('pending','referenced','running','watching','unsupported')),
 owner uuid,fence bigint NOT NULL DEFAULT 0,incarnation uuid,lease_until timestamptz,
 admission_pid integer,admission_started timestamptz,expected_version jsonb,
 next_check timestamptz NOT NULL DEFAULT clock_timestamp(),attempts bigint NOT NULL DEFAULT 0,
 last_result text,last_removed_version jsonb
);
CREATE TABLE moria.native_filesystem_multipart_intents (
 upload uuid NOT NULL REFERENCES moria.native_filesystem_uploads(id),producer_fence bigint NOT NULL,
 owner uuid NOT NULL,physical_hash text NOT NULL,location jsonb NOT NULL,upload_id text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(upload,producer_fence),
 CHECK(upload_id IS NULL OR octet_length(upload_id) BETWEEN 1 AND 1024)
);
CREATE TABLE moria.native_filesystem_gc_multipart (
 upload uuid NOT NULL REFERENCES moria.native_filesystem_upload_tombstones(upload),upload_id text NOT NULL CHECK(octet_length(upload_id) BETWEEN 1 AND 1024),
 discovered_at timestamptz NOT NULL DEFAULT clock_timestamp(),aborted_at timestamptz,
 PRIMARY KEY(upload,upload_id)
);
ALTER TABLE moria.native_filesystem_gc_candidates ADD COLUMN multipart_after text CHECK(multipart_after IS NULL OR octet_length(multipart_after) BETWEEN 1 AND 1024);
CREATE FUNCTION moria.native_filesystem_multipart_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE u moria.native_filesystem_uploads;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_MULTIPART_INTENT'; END IF;
 SELECT * INTO u FROM moria.native_filesystem_uploads WHERE id=NEW.upload FOR SHARE;
 IF NOT FOUND OR u.owner IS DISTINCT FROM NEW.owner OR u.fence IS DISTINCT FROM NEW.producer_fence OR u.physical_hash IS DISTINCT FROM NEW.physical_hash
  OR u.location IS DISTINCT FROM NEW.location OR u.state NOT IN ('writing','sealed') OR u.lease_until<=clock_timestamp()
  OR EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones WHERE upload=u.id) THEN RAISE EXCEPTION 'FILESYSTEM_LEASE_INVALID'; END IF;
 IF TG_OP='UPDATE' AND ((to_jsonb(NEW)-'upload_id') IS DISTINCT FROM (to_jsonb(OLD)-'upload_id')
  OR (OLD.upload_id IS NOT NULL AND NEW.upload_id IS DISTINCT FROM OLD.upload_id)) THEN RAISE EXCEPTION 'IMMUTABLE_MULTIPART_INTENT'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_filesystem_multipart_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_filesystem_multipart_intents FOR EACH ROW EXECUTE FUNCTION moria.native_filesystem_multipart_guard();
CREATE INDEX native_filesystem_gc_due ON moria.native_filesystem_gc_candidates(policy,next_check,upload);
CREATE INDEX fs_current_content_location ON moria.fs_entries((content->'location')) WHERE kind='file';
CREATE FUNCTION moria.native_filesystem_gc_operator() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 IF NOT has_schema_privilege(session_user,'moria','CREATE') THEN RAISE EXCEPTION 'GC_OPERATOR_REQUIRED'; END IF;
END $$;
CREATE FUNCTION moria.native_filesystem_gc_metadata_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE u moria.native_filesystem_uploads;p moria.native_filesystem_gc_policies;
BEGIN
 PERFORM moria.native_filesystem_gc_operator();
 IF TG_TABLE_NAME='native_filesystem_gc_policies' THEN
  IF TG_OP='INSERT' THEN
   IF NEW.approved_role IS DISTINCT FROM session_user OR NEW.incarnation IS DISTINCT FROM (SELECT incarnation FROM moria.indexing_instance)
      OR NEW.namespace->>'warehouseId' IS DISTINCT FROM 'filesystem' OR NEW.namespace->>'tableId' IS DISTINCT FROM 'content'
      OR NEW.namespace->>'revision' IS NULL OR NEW.physical_hash IS DISTINCT FROM (SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id='filesystem' AND table_id='content')
      THEN RAISE EXCEPTION 'GC_POLICY_INVALID'; END IF;
  ELSIF TG_OP='DELETE' OR to_jsonb(NEW)-'enabled' IS DISTINCT FROM to_jsonb(OLD)-'enabled' OR (NOT OLD.enabled AND NEW.enabled)
    THEN RAISE EXCEPTION 'IMMUTABLE_RETENTION_POLICY'; END IF;
 ELSIF TG_TABLE_NAME='native_filesystem_upload_tombstones' THEN
  IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'IMMUTABLE_UPLOAD_TOMBSTONE'; END IF;
  SELECT * INTO u FROM moria.native_filesystem_uploads WHERE id=NEW.upload FOR UPDATE;
  SELECT * INTO p FROM moria.native_filesystem_gc_policies WHERE id=NEW.policy;
  IF NOT FOUND OR u.id IS NULL OR u.state='committed' OR u.lease_until>clock_timestamp() OR NOT p.enabled
    OR p.approved_role IS DISTINCT FROM session_user OR p.incarnation IS DISTINCT FROM (SELECT incarnation FROM moria.indexing_instance)
    OR p.physical_hash IS DISTINCT FROM u.physical_hash OR u.incarnation IS DISTINCT FROM p.incarnation THEN RAISE EXCEPTION 'GC_PRODUCER_ACTIVE'; END IF;
  IF moria.native_filesystem_gc_has_reference(u.location) THEN RAISE EXCEPTION 'GC_REFERENCE_PRESENT'; END IF;
  IF NOT moria.runtime_gc_admission_held(pg_backend_pid(),(SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()))
    OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND mode='ExclusiveLock' AND granted AND objsubid=1
      AND classid=((hashtextextended('moria/filesystem',0)>>32)&4294967295)::oid AND objid=(hashtextextended('moria/filesystem',0)&4294967295)::oid)
    THEN RAISE EXCEPTION 'GC_ADMISSION_LOST'; END IF;
 ELSIF TG_TABLE_NAME='native_filesystem_gc_multipart' THEN
  IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND ((to_jsonb(NEW)-'aborted_at') IS DISTINCT FROM (to_jsonb(OLD)-'aborted_at') OR (OLD.aborted_at IS NOT NULL AND NEW.aborted_at IS DISTINCT FROM OLD.aborted_at))) THEN RAISE EXCEPTION 'IMMUTABLE_MULTIPART_INTENT'; END IF;
 ELSE
  IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND (ROW(NEW.upload,NEW.policy) IS DISTINCT FROM ROW(OLD.upload,OLD.policy)
      OR NEW.fence<OLD.fence OR NEW.attempts<OLD.attempts)) THEN RAISE EXCEPTION 'IMMUTABLE_GC_CANDIDATE'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_filesystem_gc_policy_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_filesystem_gc_policies FOR EACH ROW EXECUTE FUNCTION moria.native_filesystem_gc_metadata_guard();
CREATE TRIGGER native_filesystem_gc_tombstone_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_filesystem_upload_tombstones FOR EACH ROW EXECUTE FUNCTION moria.native_filesystem_gc_metadata_guard();
CREATE TRIGGER native_filesystem_gc_candidate_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_filesystem_gc_candidates FOR EACH ROW EXECUTE FUNCTION moria.native_filesystem_gc_metadata_guard();
CREATE TRIGGER native_filesystem_gc_multipart_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_filesystem_gc_multipart FOR EACH ROW EXECUTE FUNCTION moria.native_filesystem_gc_metadata_guard();
CREATE FUNCTION moria.native_filesystem_gc_has_reference(logical_object jsonb) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM moria.fs_entries WHERE kind='file' AND content->'location'=logical_object)
 OR EXISTS(SELECT 1 FROM moria.fs_entry_versions WHERE snapshot->'content'->'location'=logical_object)
 OR EXISTS(SELECT 1 FROM moria.runtime_recovery_members m JOIN moria.runtime_recovery_sets s ON s.id=m.recovery_set
   WHERE m.warehouse_id=logical_object->>'warehouseId' AND m.table_id=logical_object->>'tableId' AND m.object_id=logical_object->>'objectId'
     AND (s.status<>'retired' OR s.retained_until>clock_timestamp()));
$$;
REVOKE ALL ON FUNCTION moria.native_filesystem_gc_has_reference(jsonb) FROM PUBLIC;
CREATE FUNCTION moria.native_filesystem_gc_reference_check(policy_id uuid,upload_id uuid) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE p moria.native_filesystem_gc_policies;u moria.native_filesystem_uploads;
BEGIN
 PERFORM moria.native_filesystem_gc_operator();PERFORM moria.runtime_recovery_check();
 SELECT * INTO p FROM moria.native_filesystem_gc_policies WHERE id=policy_id FOR SHARE;
 SELECT * INTO u FROM moria.native_filesystem_uploads WHERE id=upload_id FOR SHARE;
 IF p.id IS NULL OR u.id IS NULL OR NOT p.enabled OR p.approved_role IS DISTINCT FROM session_user
   OR p.incarnation IS DISTINCT FROM (SELECT incarnation FROM moria.indexing_instance)
   OR u.incarnation IS DISTINCT FROM p.incarnation OR p.physical_hash IS DISTINCT FROM u.physical_hash OR p.physical_hash IS DISTINCT FROM (SELECT binding_hash FROM moria.fs_storage_bindings WHERE warehouse_id='filesystem' AND table_id='content')
   OR u.location->>'warehouseId' IS DISTINCT FROM 'filesystem' OR u.location->>'tableId' IS DISTINCT FROM 'content'
   THEN RAISE EXCEPTION 'GC_POLICY_CHANGED'; END IF;
 IF u.state='committed' OR u.lease_until>clock_timestamp() THEN RAISE EXCEPTION 'GC_PRODUCER_ACTIVE'; END IF;
 RETURN moria.native_filesystem_gc_has_reference(u.location);
END $$;
CREATE FUNCTION moria.native_filesystem_gc_check(upload_id uuid,worker uuid,worker_fence bigint,instance uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE c moria.native_filesystem_gc_candidates;p moria.native_filesystem_gc_policies;u moria.native_filesystem_uploads;
BEGIN
 PERFORM moria.native_filesystem_gc_operator();
 SELECT * INTO c FROM moria.native_filesystem_gc_candidates WHERE upload=upload_id FOR SHARE;
 IF NOT FOUND OR c.status<>'running' OR worker IS NULL OR worker_fence IS NULL OR instance IS NULL
  OR c.owner IS DISTINCT FROM worker OR c.fence IS DISTINCT FROM worker_fence OR c.incarnation IS DISTINCT FROM instance
  OR c.lease_until IS NULL OR c.lease_until<=clock_timestamp() OR instance IS DISTINCT FROM (SELECT incarnation FROM moria.indexing_instance)
  OR NOT moria.runtime_gc_admission_held(c.admission_pid,c.admission_started)
  OR NOT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.pid=c.admission_pid AND a.backend_start=c.admission_started AND a.datname=current_database()
    AND l.locktype='advisory' AND l.mode='ExclusiveLock' AND l.granted AND l.objsubid=1
    AND l.classid=((hashtextextended('moria/filesystem',0)>>32)&4294967295)::oid AND l.objid=(hashtextextended('moria/filesystem',0)&4294967295)::oid)
   THEN RAISE EXCEPTION 'GC_LEASE_LOST'; END IF;
 SELECT * INTO p FROM moria.native_filesystem_gc_policies WHERE id=c.policy;
 SELECT * INTO u FROM moria.native_filesystem_uploads WHERE id=upload_id;
 IF c.first_unreferenced IS NULL OR c.first_unreferenced+make_interval(secs=>p.minimum_age_seconds::double precision)>clock_timestamp()
   OR NOT EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones WHERE upload=upload_id AND policy=c.policy)
   THEN RAISE EXCEPTION 'GC_RETENTION_PINNED'; END IF;
 IF moria.native_filesystem_gc_reference_check(c.policy,upload_id) THEN RAISE EXCEPTION 'GC_REFERENCE_PRESENT'; END IF;
 RETURN jsonb_build_object('object',u.location,'namespace',p.namespace,'physicalFingerprint',p.physical_hash,'expectedVersion',c.expected_version);
END $$;
`;
