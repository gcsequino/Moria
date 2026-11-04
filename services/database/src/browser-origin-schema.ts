/** Browser code shares ambient authority within one origin; ownership never changes across releases or restores. */
export const browserOriginSchema = String.raw`
CREATE FUNCTION moria.native_browser_origin_key(value text) RETURNS text LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,moria AS $$
DECLARE parts text[];scheme text;hostname text;port_value integer;
BEGIN
 IF value IS NULL OR octet_length(value)>1024 THEN RAISE EXCEPTION 'BROWSER_ORIGIN_INVALID'; END IF;
 parts:=regexp_match(value,'^(https?)://(\[[0-9a-f:.]+\]|[a-z0-9]([a-z0-9.-]*[a-z0-9])?)(:([0-9]{1,5}))?/?$','i');
 IF parts IS NULL THEN RAISE EXCEPTION 'BROWSER_ORIGIN_INVALID'; END IF;
 scheme:=lower(parts[1]);hostname:=lower(parts[2]);
 IF hostname LIKE '[%]' THEN hostname:='['||host(substring(hostname FROM 2 FOR length(hostname)-2)::inet)||']';
 ELSIF hostname ~ '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' THEN hostname:=host(hostname::inet);
 ELSIF hostname LIKE '%..%' THEN RAISE EXCEPTION 'BROWSER_ORIGIN_INVALID'; END IF;
 IF parts[5] IS NOT NULL THEN
  port_value:=parts[5]::integer;
  IF port_value NOT BETWEEN 1 AND 65535 THEN RAISE EXCEPTION 'BROWSER_ORIGIN_INVALID'; END IF;
 END IF;
 RETURN scheme||'://'||hostname||CASE WHEN port_value IS NULL OR (scheme='https' AND port_value=443) OR (scheme='http' AND port_value=80) THEN '' ELSE ':'||port_value::text END;
END $$;
CREATE TABLE moria.native_browser_origins (
 origin text PRIMARY KEY,environment text NOT NULL,application text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),CHECK(octet_length(environment) BETWEEN 1 AND 1024),CHECK(octet_length(application) BETWEEN 1 AND 1024)
);
CREATE FUNCTION moria.native_browser_origin_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'BROWSER_ORIGIN_RESERVED'; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='moria' AND c.relname='native_browser_origins' AND pg_has_role(current_user,c.relowner,'MEMBER')) THEN RAISE EXCEPTION 'BROWSER_ORIGIN_RESERVED'; END IF;
 NEW.origin:=moria.native_browser_origin_key(NEW.origin);
 RETURN NEW;
END $$;
CREATE TRIGGER native_browser_origin_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.native_browser_origins FOR EACH ROW EXECUTE FUNCTION moria.native_browser_origin_guard();
CREATE FUNCTION moria.native_browser_origin_admit(value text,env text,app text) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE canonical text:=moria.native_browser_origin_key(value);prior moria.native_browser_origins;
BEGIN
 IF env IS NULL OR app IS NULL OR octet_length(env) NOT BETWEEN 1 AND 1024 OR octet_length(app) NOT BETWEEN 1 AND 1024 THEN RAISE EXCEPTION 'BROWSER_ORIGIN_INVALID'; END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.application_scopes WHERE environment=env AND application=app) THEN RAISE EXCEPTION 'BROWSER_ORIGIN_INVALID'; END IF;
 INSERT INTO moria.native_browser_origins(origin,environment,application) VALUES(canonical,env,app) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT prior FROM moria.native_browser_origins WHERE origin=canonical FOR SHARE;
 IF ROW(prior.environment,prior.application) IS DISTINCT FROM ROW(env,app) THEN RAISE EXCEPTION 'BROWSER_ORIGIN_RESERVED'; END IF;
 RETURN canonical;
END $$;
`;
