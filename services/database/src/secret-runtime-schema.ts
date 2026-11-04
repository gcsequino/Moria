/** Operator-approved, incarnation-bound use of external secret material. Values never enter this schema. */
export const secretRuntimeSchema = String.raw`
CREATE TABLE moria.native_secret_approvals (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),request_key text NOT NULL UNIQUE,
 environment text NOT NULL REFERENCES moria.environments(id),secret uuid NOT NULL,consumer uuid NOT NULL,
 source_release text NOT NULL CHECK(source_release ~ '^sha256:[0-9a-f]{64}$'),secret_definition_hash text NOT NULL,
 owner_kind text NOT NULL CHECK(owner_kind IN ('model','connector')),owner_profile uuid NOT NULL,owner_revision text NOT NULL,
 destination_fingerprint text NOT NULL,material_ref text NOT NULL CHECK(material_ref ~ '^[A-Za-z_][A-Za-z0-9_]{0,127}$'),
 material_revision text NOT NULL CHECK(octet_length(material_revision) BETWEEN 1 AND 1024),
 markings text[] NOT NULL CHECK(cardinality(markings)<=256),incarnation uuid NOT NULL,
 generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),enabled boolean NOT NULL DEFAULT true,
 approved_role text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(secret_definition_hash ~ '^sha256:[0-9a-f]{64}$'),CHECK(destination_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
 CHECK(octet_length(request_key) BETWEEN 1 AND 256),CHECK(octet_length(owner_revision) BETWEEN 1 AND 256)
);
CREATE UNIQUE INDEX native_secret_current_approval ON moria.native_secret_approvals(environment,secret,consumer,owner_kind,owner_profile) WHERE enabled;
CREATE FUNCTION moria.native_secret_approval_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM moria.runtime_recovery_operator();
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'SECRET_APPROVAL_IMMUTABLE'; END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.approved_role<>session_user OR NEW.incarnation<>(SELECT incarnation FROM moria.indexing_instance) OR NEW.generation<>1
   THEN RAISE EXCEPTION 'SECRET_APPROVAL_INVALID'; END IF;
 ELSE
  IF (to_jsonb(NEW)-'enabled'-'generation')<>(to_jsonb(OLD)-'enabled'-'generation') OR NEW.generation<>OLD.generation
   THEN RAISE EXCEPTION 'SECRET_APPROVAL_IMMUTABLE'; END IF;
  IF NEW.enabled AND NEW.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'SECRET_APPROVAL_INVALID'; END IF;
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN NEW.generation:=OLD.generation+1; END IF;
 END IF;
 IF TG_OP='INSERT' OR NEW.enabled IS DISTINCT FROM OLD.enabled THEN
  UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  IF NEW.owner_kind='model' THEN
   INSERT INTO moria.native_search_binding_signals(environment,resource,nonce) VALUES(NEW.environment,NEW.owner_profile::text,gen_random_uuid())
    ON CONFLICT(environment,resource) DO UPDATE SET nonce=EXCLUDED.nonce;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_secret_approval_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_secret_approvals
 FOR EACH ROW EXECUTE FUNCTION moria.native_secret_approval_guard();
`;
