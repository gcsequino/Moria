/** Security disable pulses permanently revoke issued credentials and pending browser flows. */
export const browserSessionRevocationSchema = String.raw`
CREATE TABLE moria.native_browser_auth_generations (
 environment text PRIMARY KEY REFERENCES moria.environments(id),generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0)
);
CREATE FUNCTION moria.native_browser_auth_generation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='moria' AND c.relname='native_browser_auth_generations' AND pg_has_role(current_user,c.relowner,'MEMBER')) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 IF TG_OP='INSERT' THEN IF NEW.generation<>0 THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 ELSIF NEW.environment IS DISTINCT FROM OLD.environment OR NEW.generation<>OLD.generation+1 THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_browser_auth_generation_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_browser_auth_generations FOR EACH ROW EXECUTE FUNCTION moria.native_browser_auth_generation_guard();
CREATE FUNCTION moria.native_browser_auth_bump(env text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 INSERT INTO moria.native_browser_auth_generations(environment) VALUES(env) ON CONFLICT DO NOTHING;
 UPDATE moria.native_browser_auth_generations SET generation=generation+1 WHERE environment=env;
END $$;
REVOKE ALL ON FUNCTION moria.native_browser_auth_bump(text) FROM PUBLIC;
ALTER TABLE moria.auth_flows ADD COLUMN auth_generation bigint NOT NULL DEFAULT 0;
CREATE FUNCTION moria.native_browser_auth_flow_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 INSERT INTO moria.native_browser_auth_generations(environment) VALUES(NEW.environment) ON CONFLICT DO NOTHING;
 SELECT generation INTO STRICT NEW.auth_generation FROM moria.native_browser_auth_generations WHERE environment=NEW.environment FOR SHARE;
 IF NOT EXISTS(SELECT 1 FROM moria.application_scopes WHERE environment=NEW.environment AND application=NEW.application AND enabled)
  OR (NEW.provider IS NOT NULL AND NOT EXISTS(SELECT 1 FROM moria.auth_providers WHERE environment=NEW.environment AND application=NEW.application AND provider=NEW.provider AND revision=NEW.revision AND enabled)) THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_browser_auth_flow_guard BEFORE INSERT ON moria.auth_flows FOR EACH ROW EXECUTE FUNCTION moria.native_browser_auth_flow_guard();
CREATE FUNCTION moria.native_browser_auth_flow_current(env text,expected bigint) RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,moria AS $$
DECLARE current_generation bigint;
BEGIN
 SELECT generation INTO current_generation FROM moria.native_browser_auth_generations WHERE environment=env FOR SHARE;
 RETURN current_generation IS NOT NULL AND current_generation=expected;
END $$;
CREATE INDEX browser_sessions_live_actor ON moria.browser_sessions(environment,principal) WHERE NOT disabled;
CREATE INDEX browser_sessions_live_application ON moria.browser_sessions(environment,application) WHERE NOT disabled;
CREATE INDEX browser_sessions_live_provider ON moria.browser_sessions(environment,application,provider) WHERE NOT disabled;
CREATE INDEX workload_sessions_live_actor ON moria.workload_sessions(environment,principal) WHERE NOT disabled;
CREATE INDEX workload_sessions_live_application ON moria.workload_sessions(environment,application) WHERE NOT disabled;
CREATE INDEX instance_principal_bindings_identity ON moria.instance_principal_bindings(instance_id,environment,principal);
CREATE FUNCTION moria.native_session_revocation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.disabled AND NOT NEW.disabled THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
 IF TG_TABLE_NAME='browser_sessions' AND NEW.disabled THEN NEW.access_token:=NULL; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_browser_session_revocation_guard BEFORE UPDATE ON moria.browser_sessions FOR EACH ROW EXECUTE FUNCTION moria.native_session_revocation_guard();
CREATE TRIGGER native_workload_session_revocation_guard BEFORE UPDATE ON moria.workload_sessions FOR EACH ROW EXECUTE FUNCTION moria.native_session_revocation_guard();
CREATE FUNCTION moria.native_browser_auth_revoke_actor(env text,actor text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE environment=env AND principal=actor AND NOT disabled;
 UPDATE moria.workload_sessions SET disabled=true WHERE environment=env AND principal=actor AND NOT disabled;
 PERFORM moria.native_browser_auth_bump(env);
END $$;
REVOKE ALL ON FUNCTION moria.native_browser_auth_revoke_actor(text,text) FROM PUBLIC;
CREATE FUNCTION moria.native_browser_security_revocation() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE actor record;
BEGIN
 IF TG_TABLE_NAME='auth_providers' THEN
  IF TG_OP='DELETE' OR (OLD.enabled AND NOT NEW.enabled) OR OLD.revision IS DISTINCT FROM NEW.revision THEN
   UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE environment=OLD.environment AND application=OLD.application AND provider=OLD.provider AND NOT disabled;
   DELETE FROM moria.auth_flows WHERE environment=OLD.environment AND application=OLD.application AND provider=OLD.provider;
   PERFORM moria.native_browser_auth_bump(OLD.environment);
  END IF;
 ELSIF TG_TABLE_NAME='application_scopes' THEN
  IF TG_OP='DELETE' OR (OLD.enabled AND NOT NEW.enabled) THEN
   UPDATE moria.browser_sessions SET disabled=true,access_token=NULL WHERE environment=OLD.environment AND application=OLD.application AND NOT disabled;
   UPDATE moria.workload_sessions SET disabled=true WHERE environment=OLD.environment AND application=OLD.application AND NOT disabled;
   DELETE FROM moria.auth_flows WHERE environment=OLD.environment AND application=OLD.application;
   PERFORM moria.native_browser_auth_bump(OLD.environment);
  END IF;
 ELSIF TG_TABLE_NAME='principals' THEN
  IF TG_OP='DELETE' OR (OLD.enabled AND NOT NEW.enabled) OR OLD.kind IS DISTINCT FROM NEW.kind THEN PERFORM moria.native_browser_auth_revoke_actor(OLD.environment,OLD.principal); END IF;
 ELSIF TG_TABLE_NAME='instance_principals' THEN
  IF TG_OP='DELETE' OR (OLD.enabled AND NOT NEW.enabled) OR OLD.kind IS DISTINCT FROM NEW.kind THEN
   FOR actor IN SELECT environment,principal FROM moria.instance_principal_bindings WHERE instance_id=OLD.id LOOP PERFORM moria.native_browser_auth_revoke_actor(actor.environment,actor.principal); END LOOP;
  END IF;
 ELSIF TG_TABLE_NAME='instance_principal_bindings' THEN
  IF TG_OP='DELETE' OR OLD.instance_id IS DISTINCT FROM NEW.instance_id OR OLD.environment IS DISTINCT FROM NEW.environment OR OLD.principal IS DISTINCT FROM NEW.principal THEN PERFORM moria.native_browser_auth_revoke_actor(OLD.environment,OLD.principal); END IF;
 ELSIF TG_TABLE_NAME='external_identities' THEN
  IF TG_OP='DELETE' OR ROW(OLD.environment,OLD.issuer,OLD.subject,OLD.principal) IS DISTINCT FROM ROW(NEW.environment,NEW.issuer,NEW.subject,NEW.principal) THEN PERFORM moria.native_browser_auth_revoke_actor(OLD.environment,OLD.principal); END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
END $$;
CREATE TRIGGER native_provider_session_revoke BEFORE UPDATE OR DELETE ON moria.auth_providers FOR EACH ROW EXECUTE FUNCTION moria.native_browser_security_revocation();
CREATE TRIGGER native_application_session_revoke BEFORE UPDATE OR DELETE ON moria.application_scopes FOR EACH ROW EXECUTE FUNCTION moria.native_browser_security_revocation();
CREATE TRIGGER native_principal_session_revoke BEFORE UPDATE OR DELETE ON moria.principals FOR EACH ROW EXECUTE FUNCTION moria.native_browser_security_revocation();
CREATE TRIGGER native_instance_principal_session_revoke BEFORE UPDATE OR DELETE ON moria.instance_principals FOR EACH ROW EXECUTE FUNCTION moria.native_browser_security_revocation();
CREATE TRIGGER native_principal_binding_session_revoke BEFORE UPDATE OR DELETE ON moria.instance_principal_bindings FOR EACH ROW EXECUTE FUNCTION moria.native_browser_security_revocation();
CREATE TRIGGER native_external_identity_session_revoke BEFORE UPDATE OR DELETE ON moria.external_identities FOR EACH ROW EXECUTE FUNCTION moria.native_browser_security_revocation();
`;
