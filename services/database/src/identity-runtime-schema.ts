/** Installation-wide identity ownership; domain moves never copy/reassign UID rows. */
export const identityRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_cells (
  id uuid PRIMARY KEY, endpoint_ref text NOT NULL CHECK(endpoint_ref ~ '^[A-Z][A-Z0-9_]{0,127}$'),
  incarnation uuid NOT NULL, enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE moria.runtime_identity_domains (
  domain text COLLATE "C" PRIMARY KEY CHECK(octet_length(domain) BETWEEN 1 AND 1024),
  cell uuid NOT NULL REFERENCES moria.runtime_cells(id), fence bigint NOT NULL DEFAULT 1 CHECK(fence>0),
  incarnation uuid NOT NULL
);
CREATE TABLE moria.runtime_placements (
  environment text PRIMARY KEY REFERENCES moria.environments(id),
  cell uuid NOT NULL REFERENCES moria.runtime_cells(id), fence bigint NOT NULL DEFAULT 1 CHECK(fence>0),
  incarnation uuid NOT NULL, serving boolean NOT NULL DEFAULT false
);
CREATE TABLE moria.runtime_ownership_events (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind text NOT NULL CHECK(kind IN ('identity','environment')), resource text NOT NULL,
  previous_cell uuid, next_cell uuid NOT NULL, fence bigint NOT NULL,
  incarnation uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION moria.runtime_identity_resolve(request_domain text, keys text[], owner_cell uuid, owner_fence bigint, cell_incarnation uuid)
RETURNS TABLE(canonical_key text,uid uuid) LANGUAGE plpgsql AS $$
DECLARE ownership moria.runtime_identity_domains; collision text;
BEGIN
  IF cardinality(keys) IS NULL OR cardinality(keys)<1 OR cardinality(keys)>1000
    OR EXISTS(SELECT 1 FROM unnest(keys) k WHERE k IS NULL OR octet_length(k)>65536)
    OR (SELECT COALESCE(sum(octet_length(k)),0) FROM unnest(keys) k)>1048576 THEN
    RAISE EXCEPTION 'IDENTITY_REQUEST_INVALID';
  END IF;
  SELECT * INTO ownership FROM moria.runtime_identity_domains WHERE domain=request_domain FOR SHARE;
  IF NOT FOUND OR ownership.cell<>owner_cell OR ownership.fence<>owner_fence OR ownership.incarnation<>cell_incarnation
    OR NOT EXISTS(SELECT 1 FROM moria.indexing_instance WHERE incarnation=cell_incarnation)
    OR NOT EXISTS(SELECT 1 FROM moria.runtime_cells WHERE id=owner_cell AND enabled AND incarnation=cell_incarnation) THEN
    RAISE EXCEPTION 'OWNERSHIP_LOST';
  END IF;
  FOR attempt IN 1..5 LOOP
    BEGIN
      INSERT INTO moria.identities(domain,canonical_key,uid)
        SELECT request_domain,k,gen_random_uuid() FROM (SELECT DISTINCT unnest(keys) AS k) wanted
        ON CONFLICT ON CONSTRAINT identities_pkey DO NOTHING;
      IF EXISTS(SELECT 1 FROM unnest(keys) k JOIN moria.identities i ON i.domain=request_domain
        AND i.key_hash=moria.identity_key_hash(k) WHERE i.canonical_key<>k COLLATE "C") THEN
        RAISE EXCEPTION 'IDENTITY_KEY_COLLISION';
      END IF;
      RETURN QUERY SELECT k.key,i.uid FROM unnest(keys) WITH ORDINALITY k(key,ordinal)
        JOIN moria.identities i ON i.domain=request_domain AND i.key_hash=moria.identity_key_hash(k.key)
          AND i.canonical_key=k.key COLLATE "C" ORDER BY k.ordinal;
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      GET STACKED DIAGNOSTICS collision=CONSTRAINT_NAME;
      IF collision<>'identities_uid_key' THEN RAISE; END IF;
    END;
  END LOOP;
  RAISE EXCEPTION 'IDENTITY_ALLOCATION_FAILED';
END $$;

CREATE FUNCTION moria.runtime_identity_resolve_legacy(request_domain text,request_key text) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE result uuid; collision text; stored_key text;
BEGIN
  PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/identity-domain/'||request_domain,0));
  IF EXISTS(SELECT 1 FROM moria.runtime_identity_domains WHERE domain=request_domain) THEN RAISE EXCEPTION 'OWNERSHIP_REQUIRED'; END IF;
  FOR attempt IN 1..5 LOOP
    BEGIN
      INSERT INTO moria.identities(domain,canonical_key,uid) VALUES(request_domain,request_key,gen_random_uuid())
        ON CONFLICT ON CONSTRAINT identities_pkey DO NOTHING;
      SELECT i.uid,i.canonical_key INTO result,stored_key FROM moria.identities i
        WHERE i.domain=request_domain AND i.key_hash=moria.identity_key_hash(request_key);
      IF NOT FOUND OR stored_key<>request_key COLLATE "C" THEN RAISE EXCEPTION 'IDENTITY_KEY_COLLISION'; END IF;
      RETURN result;
    EXCEPTION WHEN unique_violation THEN
      GET STACKED DIAGNOSTICS collision=CONSTRAINT_NAME;
      IF collision<>'identities_uid_key' THEN RAISE; END IF;
    END;
  END LOOP;
  RAISE EXCEPTION 'IDENTITY_ALLOCATION_FAILED';
END $$;

CREATE FUNCTION moria.runtime_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IDENTITY_RESERVED'; END IF;
  IF ROW(NEW.domain,NEW.canonical_key,NEW.uid,NEW.created_at) IS DISTINCT FROM ROW(OLD.domain,OLD.canonical_key,OLD.uid,OLD.created_at) THEN
    RAISE EXCEPTION 'IDENTITY_RESERVED';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_identity_permanent BEFORE UPDATE OR DELETE ON moria.identities
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_identity_immutable();
`;
