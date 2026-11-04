/** Full source projection is a registered service purpose, never an ordinary query capability. */
export const searchProjectorSchema = String.raw`
ALTER TABLE moria.native_search_collections ADD UNIQUE(environment,resource,id);
CREATE TABLE moria.native_search_projector_bindings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), revision text NOT NULL,
  environment text NOT NULL, collection text NOT NULL, collection_id uuid NOT NULL,
  service_id uuid NOT NULL REFERENCES moria.runtime_service_authorities(id), service_binding_revision text NOT NULL,
  domain text NOT NULL DEFAULT 'search' CHECK(domain='search'),
  purpose text NOT NULL DEFAULT 'full-object-projection' CHECK(purpose='full-object-projection'),
  source_resource text NOT NULL, text_property text NOT NULL, source_dataset text NOT NULL,
  model_resource text NOT NULL, model_destination text NOT NULL, markings text[] NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  UNIQUE(environment,collection,revision),
  FOREIGN KEY(environment,collection,collection_id) REFERENCES moria.native_search_collections(environment,resource,id),
  FOREIGN KEY(service_id,environment,collection_id,domain,service_binding_revision)
    REFERENCES moria.runtime_service_targets(authority,environment,resource,domain,binding_revision),
  FOREIGN KEY(environment,source_resource) REFERENCES moria.native_serving_bindings(environment,resource),
  FOREIGN KEY(environment,model_resource) REFERENCES moria.native_model_bindings(environment,resource)
);
CREATE FUNCTION moria.native_search_configuration_revocation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (TG_TABLE_NAME='native_search_projector_bindings' AND NEW.enabled IS DISTINCT FROM OLD.enabled)
    OR (TG_TABLE_NAME='native_search_collections' AND (to_jsonb(NEW)-'active_generation') IS DISTINCT FROM (to_jsonb(OLD)-'active_generation')) THEN
    UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_search_projector_revocation AFTER UPDATE ON moria.native_search_projector_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_configuration_revocation();
CREATE TRIGGER native_search_collection_revocation AFTER UPDATE ON moria.native_search_collections
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_configuration_revocation();
CREATE TABLE moria.native_search_binding_signals (
  environment text NOT NULL REFERENCES moria.environments(id),resource text NOT NULL,nonce uuid NOT NULL,
  PRIMARY KEY(environment,resource)
);
CREATE FUNCTION moria.native_search_binding_signal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME<>'native_model_probes' AND TG_OP='UPDATE' THEN
    IF (NEW.revision,NEW.definition,NEW.enabled) IS NOT DISTINCT FROM (OLD.revision,OLD.definition,OLD.enabled) THEN RETURN NEW; END IF;
  END IF;
  INSERT INTO moria.native_search_binding_signals VALUES(NEW.environment,NEW.resource,gen_random_uuid())
    ON CONFLICT(environment,resource) DO UPDATE SET nonce=EXCLUDED.nonce;
  RETURN NEW;
END $$;
CREATE TRIGGER native_search_source_signal AFTER INSERT OR UPDATE ON moria.native_serving_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_binding_signal();
CREATE TRIGGER native_search_model_signal AFTER INSERT OR UPDATE ON moria.native_model_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_binding_signal();
CREATE TRIGGER native_search_probe_signal AFTER INSERT ON moria.native_model_probes
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_binding_signal();
ALTER TABLE moria.native_search_generations ADD COLUMN source_signal uuid,ADD COLUMN model_signal uuid;
ALTER TABLE moria.native_search_collections ADD COLUMN projector_binding uuid REFERENCES moria.native_search_projector_bindings(id);
ALTER TABLE moria.native_search_rebuilds ADD COLUMN projector_binding uuid NOT NULL REFERENCES moria.native_search_projector_bindings(id);
CREATE FUNCTION moria.native_search_projector_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR (to_jsonb(NEW)-'enabled') IS DISTINCT FROM (to_jsonb(OLD)-'enabled')
    THEN RAISE EXCEPTION 'IMMUTABLE_PROJECTOR_BINDING'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_search_projector_immutable BEFORE UPDATE OR DELETE ON moria.native_search_projector_bindings
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_projector_immutable();
`;
