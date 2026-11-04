/** Durable qualified filesystem transfer ownership; provider paths remain operator configuration. */
export const filesystemContentSchema = String.raw`
CREATE TABLE moria.native_filesystem_uploads (
  id uuid PRIMARY KEY, scope_hash text NOT NULL, request_key text NOT NULL,
  request_hash text NOT NULL, environment text NOT NULL REFERENCES moria.environments(id),
  principal text NOT NULL, application text NOT NULL, session_hash text NOT NULL,
  request jsonb NOT NULL, location jsonb NOT NULL, physical_hash text NOT NULL,
  source_markings text[] NOT NULL CHECK(cardinality(source_markings)<=256),
  owner uuid NOT NULL, fence bigint NOT NULL CHECK(fence>0), lease_until timestamptz NOT NULL,incarnation uuid NOT NULL,
  state text NOT NULL CHECK(state IN ('writing','sealed','committed')),
  receipt jsonb, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(scope_hash,request_key), CHECK((state='committed')=(receipt IS NOT NULL))
);
CREATE INDEX native_filesystem_upload_expiry ON moria.native_filesystem_uploads(lease_until,id) WHERE state<>'committed';
CREATE FUNCTION moria.native_filesystem_upload_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='INSERT' THEN NEW.incarnation:=(SELECT incarnation FROM moria.indexing_instance);RETURN NEW; END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_UPLOAD'; END IF;
  IF EXISTS(SELECT 1 FROM moria.native_filesystem_upload_tombstones WHERE upload=OLD.id) THEN RAISE EXCEPTION 'UPLOAD_RETIRED'; END IF;
  IF ROW(NEW.id,NEW.scope_hash,NEW.request_key,NEW.request_hash,NEW.environment,NEW.principal,NEW.application,NEW.request,NEW.location,NEW.physical_hash,NEW.incarnation)
    IS DISTINCT FROM ROW(OLD.id,OLD.scope_hash,OLD.request_key,OLD.request_hash,OLD.environment,OLD.principal,OLD.application,OLD.request,OLD.location,OLD.physical_hash,OLD.incarnation)
    OR NOT OLD.source_markings<@NEW.source_markings OR OLD.state='committed' THEN RAISE EXCEPTION 'FILESYSTEM_CONFLICT'; END IF;
  IF ROW(NEW.owner,NEW.fence) IS DISTINCT FROM ROW(OLD.owner,OLD.fence)
    AND (OLD.lease_until>clock_timestamp() OR NEW.fence<>OLD.fence+1) THEN RAISE EXCEPTION 'FILESYSTEM_LEASE_INVALID'; END IF;
  IF NEW.state='committed' AND OLD.lease_until<=clock_timestamp() THEN RAISE EXCEPTION 'FILESYSTEM_LEASE_INVALID'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_filesystem_upload_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_filesystem_uploads
  FOR EACH ROW EXECUTE FUNCTION moria.native_filesystem_upload_guard();
`;
