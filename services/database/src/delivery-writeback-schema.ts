/** Explicit Action-origin connector effects use the existing durable delivery queue. */
export const deliveryWritebackSchema = String.raw`
CREATE TABLE moria.runtime_delivery_writebacks (
  destination uuid PRIMARY KEY REFERENCES moria.runtime_delivery_destinations(id),
  environment text NOT NULL REFERENCES moria.environments(id), action uuid NOT NULL, action_revision text NOT NULL,
  action_generation bigint NOT NULL, action_release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  connection uuid NOT NULL, connection_release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  implementation uuid NOT NULL, implementation_release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  connector_destination uuid NOT NULL REFERENCES moria.runtime_pipeline_destinations(id),
  destination_fingerprint text NOT NULL CHECK(destination_fingerprint~'^sha256:[0-9a-f]{64}$'),
  secret_ids uuid[] NOT NULL CHECK(cardinality(secret_ids)<=8 AND array_position(secret_ids,NULL) IS NULL),
  writer_role name NOT NULL, effect_key text NOT NULL CHECK(octet_length(effect_key) BETWEEN 1 AND 128),
  UNIQUE(environment,action,action_revision)
);
ALTER TABLE moria.action_keys ADD COLUMN writeback jsonb
  CHECK(writeback IS NULL OR (jsonb_typeof(writeback)='object' AND octet_length(writeback::text)<=16384));
ALTER TABLE moria.runtime_deliveries ADD COLUMN writeback jsonb
  CHECK(writeback IS NULL OR (jsonb_typeof(writeback)='object' AND octet_length(writeback::text)<=16384));

CREATE TRIGGER runtime_delivery_writeback_immutable_delete BEFORE DELETE ON moria.runtime_deliveries
  FOR EACH ROW WHEN (OLD.writeback IS NOT NULL) EXECUTE FUNCTION moria.runtime_delivery_immutable();

-- A legacy destination also resolves a host secret reference. Its endpoint and
-- secret must remain operator metadata when a worker hosts Connector material.
CREATE TRIGGER runtime_delivery_destination_operator BEFORE INSERT OR UPDATE OR DELETE
  ON moria.runtime_delivery_destinations FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();

CREATE FUNCTION moria.runtime_delivery_writeback_registration() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria,pg_temp AS $$
BEGIN
  PERFORM moria.runtime_recovery_operator();
  IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'IMMUTABLE_DELIVERY_WRITEBACK'; END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname=NEW.writer_role AND r.rolcanlogin
    AND NOT(r.rolsuper OR r.rolbypassrls OR r.rolcreatedb OR r.rolcreaterole)
    AND NOT has_schema_privilege(r.rolname,'moria','CREATE'))
    OR NOT EXISTS(SELECT 1 FROM moria.runtime_delivery_destinations d JOIN moria.runtime_execution_bindings b ON b.id=d.binding
      JOIN moria.runtime_pipeline_destinations p ON p.id=NEW.connector_destination
      WHERE d.id=NEW.destination AND NOT d.idempotent AND d.secret_ref IS NULL AND d.enabled
        AND b.environment=NEW.environment AND b.spec->>'domain'='delivery' AND p.enabled
        AND p.environment=NEW.environment AND p.resource=NEW.connection AND 'POST'=ANY(p.methods)
        AND NOT EXISTS(SELECT 1 FROM moria.runtime_deliveries q
          WHERE q.destination_resource=b.resource AND q.destination=d.id))
    THEN RAISE EXCEPTION 'DELIVERY_WRITEBACK_INVALID'; END IF;
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_delivery_writeback_registration() FROM PUBLIC;
CREATE TRIGGER runtime_delivery_writeback_registration BEFORE INSERT OR UPDATE OR DELETE
  ON moria.runtime_delivery_writebacks FOR EACH ROW EXECUTE FUNCTION moria.runtime_delivery_writeback_registration();

CREATE INDEX runtime_delivery_destination_binding ON moria.runtime_delivery_destinations(binding);
CREATE FUNCTION moria.runtime_delivery_writeback_binding_epoch() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria,pg_temp AS $$
BEGIN
  IF EXISTS(SELECT 1 FROM moria.runtime_delivery_destinations d JOIN moria.runtime_delivery_writebacks w
    ON w.destination=d.id WHERE d.binding=NEW.id) THEN
    PERFORM moria.runtime_recovery_operator();
    UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
  END IF;
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_delivery_writeback_binding_epoch() FROM PUBLIC;
CREATE TRIGGER runtime_delivery_writeback_binding_epoch AFTER UPDATE OF enabled ON moria.runtime_execution_bindings
  FOR EACH ROW WHEN (OLD.enabled IS DISTINCT FROM NEW.enabled) EXECUTE FUNCTION moria.runtime_delivery_writeback_binding_epoch();

CREATE FUNCTION moria.runtime_delivery_writeback_current(delivery_id uuid, dispatch boolean) RETURNS void LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria,pg_temp AS $$
DECLARE w moria.runtime_delivery_writebacks; p moria.runtime_pipeline_destinations; state jsonb; labels text[];
  d moria.runtime_deliveries; x moria.runtime_delivery_destinations; b moria.runtime_execution_bindings;
  scope text[]; pin record; approval moria.native_secret_approvals; current_labels text[]; pinned_secrets uuid[]:='{}';
BEGIN
  SELECT * INTO d FROM moria.runtime_deliveries WHERE id=delivery_id AND writeback IS NOT NULL;
  IF NOT FOUND THEN RAISE EXCEPTION 'DELIVERY_UNKNOWN'; END IF;
  SELECT * INTO STRICT w FROM moria.runtime_delivery_writebacks WHERE destination=d.destination;
  SELECT * INTO x FROM moria.runtime_delivery_destinations WHERE id=d.destination FOR SHARE;
  IF NOT FOUND OR (dispatch AND NOT x.enabled) THEN RAISE EXCEPTION 'DESTINATION_DISABLED'; END IF;
  SELECT * INTO STRICT b FROM moria.runtime_execution_bindings WHERE id=x.binding FOR SHARE;
  IF ROW(b.environment,b.resource,b.context->>'principal',b.context->>'application')
    IS DISTINCT FROM ROW(w.environment,d.destination_resource,d.origin->>'principal',d.origin->>'application')
    OR b.spec->>'domain' IS DISTINCT FROM 'delivery' OR b.spec->>'version' IS DISTINCT FROM '1'
    OR NOT ((b.resource::text||':write')=ANY(b.operations))
    THEN RAISE EXCEPTION 'DELIVERY_FORBIDDEN'; END IF;
  SELECT * INTO p FROM moria.runtime_pipeline_destinations WHERE id=w.connector_destination AND enabled FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'PIPELINE_DESTINATION_REVOKED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.native_serving_actions a WHERE a.environment=w.environment AND a.resource=w.action::text
    AND a.enabled AND a.revision=w.action_revision AND a.authority_generation=w.action_generation)
    OR EXISTS(SELECT 1 FROM (VALUES(w.action,w.action_release),(w.connection,w.connection_release),(w.implementation,w.implementation_release)) target(resource,digest)
      WHERE NOT EXISTS(SELECT 1 FROM moria.fs_entries e JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active
        JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest
        WHERE e.resource_id=target.resource AND e.environment_id=w.environment AND NOT e.deleted AND b.digest=target.digest))
    THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  state:=moria.native_serving_session(d.writeback->>'sessionHash');
  IF state->>'privatePurpose' IS NOT NULL OR ROW(state->>'environment',state->>'principal',state->>'application')
    IS DISTINCT FROM ROW(w.environment,d.origin->>'principal',d.origin->>'application')
    OR ROW(state->>'securityEpoch',state->>'instanceEpoch') IS DISTINCT FROM ROW(d.writeback->>'securityEpoch',d.writeback->>'instanceEpoch')
    THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  PERFORM moria.runtime_binding_authority(b.id,(d.writeback->>'securityEpoch')::bigint,(d.writeback->>'instanceEpoch')::bigint);
  scope:=ARRAY[w.connection::text||':writeback',d.destination_resource::text||':write',w.action::text||':invoke'];
  labels:=p.markings||ARRAY(SELECT jsonb_array_elements_text(moria.native_source_declaration_labels_batch_state(state,jsonb_build_array(
    jsonb_build_object('resource',w.action,'release',w.action_release),
    jsonb_build_object('resource',w.connection,'release',w.connection_release),
    jsonb_build_object('resource',w.implementation,'release',w.implementation_release)))));
  FOR pin IN SELECT * FROM jsonb_to_recordset(d.writeback->'approvals')
    AS x(id uuid,generation bigint,incarnation uuid,secret uuid,"materialRevision" text) LOOP
    SELECT a.* INTO approval FROM moria.native_secret_approvals a JOIN moria.indexing_instance i ON i.incarnation=a.incarnation
      WHERE a.id=pin.id AND a.enabled AND a.generation=pin.generation AND a.incarnation=pin.incarnation
        AND a.material_revision=pin."materialRevision" AND a.environment=w.environment AND a.secret=pin.secret
        AND a.consumer=w.connection AND a.owner_kind='connector' AND a.owner_profile=p.id AND a.owner_revision=p.revision
        AND a.source_release=w.connection_release AND a.destination_fingerprint=w.destination_fingerprint FOR SHARE OF a;
    IF NOT FOUND THEN RAISE EXCEPTION 'SECRET_APPROVAL_REVOKED'; END IF;
    pinned_secrets:=array_append(pinned_secrets,approval.secret);
    scope:=array_append(scope,approval.secret::text||':use');
    current_labels:=ARRAY(SELECT jsonb_array_elements_text(moria.native_serving_filesystem_resource_access_state(state,approval.secret::text,'use')));
    labels:=labels||approval.markings||current_labels;
  END LOOP;
  IF cardinality(pinned_secrets)<>cardinality(w.secret_ids) OR NOT w.secret_ids<@pinned_secrets
    OR NOT (state->'identity'->'grantedOperations' ?& scope) OR NOT (state->'identity'->'applicationScopes' ?& scope)
    OR NOT (state->'identity'->'markings' ?& d.source_markings) OR NOT labels<@d.source_markings
    OR NOT d.source_markings<@p.allowed_markings OR NOT d.source_markings<@x.allowed_markings
    OR NOT b.markings<@d.source_markings THEN RAISE EXCEPTION 'DELIVERY_FORBIDDEN'; END IF;
END $$;

CREATE FUNCTION moria.runtime_delivery_writeback_action_guard() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria,pg_temp AS $$
DECLARE w moria.runtime_delivery_writebacks;
BEGIN
  IF TG_OP<>'INSERT' THEN
    IF OLD.writeback IS NOT NULL THEN RAISE EXCEPTION 'IMMUTABLE_DELIVERY_WRITEBACK'; END IF;
    IF TG_OP='DELETE' THEN RETURN OLD; END IF;
    IF NEW.writeback IS NOT NULL THEN RAISE EXCEPTION 'DELIVERY_WRITEBACK_ORIGIN_INVALID'; END IF;
  END IF;
  SELECT * INTO w FROM moria.runtime_delivery_writebacks WHERE environment=NEW.environment
    AND action=CASE WHEN NEW.action~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NEW.action::uuid END AND action_revision=NEW.action_revision;
  IF NOT FOUND THEN
    IF NEW.writeback IS NOT NULL THEN RAISE EXCEPTION 'DELIVERY_WRITEBACK_ORIGIN_INVALID'; END IF;
    RETURN NEW;
  END IF;
  IF TG_OP<>'INSERT' OR session_user<>w.writer_role OR NEW.result IS NULL OR NEW.writeback IS NULL
    OR NEW.writeback->>'sessionHash' IS NULL OR NEW.writeback->>'securityEpoch' IS NULL
    OR NEW.writeback->>'instanceEpoch' IS NULL OR jsonb_typeof(NEW.writeback->'approvals') IS DISTINCT FROM 'array'
    OR jsonb_array_length(NEW.writeback->'approvals')>8
    OR jsonb_typeof(NEW.writeback->'markings') IS DISTINCT FROM 'array'
    THEN RAISE EXCEPTION 'DELIVERY_WRITEBACK_ORIGIN_INVALID'; END IF;
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_delivery_writeback_action_guard() FROM PUBLIC;
CREATE TRIGGER runtime_delivery_writeback_action_guard BEFORE INSERT OR UPDATE OR DELETE ON moria.action_keys
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_delivery_writeback_action_guard();

CREATE FUNCTION moria.runtime_delivery_writeback_intent_guard() RETURNS trigger LANGUAGE plpgsql
  SET search_path=pg_catalog,moria,pg_temp AS $$
DECLARE w moria.runtime_delivery_writebacks;
BEGIN
  SELECT * INTO w FROM moria.runtime_delivery_writebacks WHERE destination=NEW.destination;
  IF NOT FOUND AND NEW.writeback IS NULL THEN RETURN NEW; END IF;
  IF NOT FOUND OR session_user<>w.writer_role OR NEW.writeback IS NULL
    OR current_user<>pg_get_userbyid((SELECT proowner FROM pg_proc
      WHERE oid='moria.runtime_delivery_writeback_from_action()'::regprocedure))
    OR NEW.status<>'pending' OR NEW.fence<>0 OR NEW.attempts<>0 OR NEW.sent_at IS NOT NULL
    OR NOT EXISTS(SELECT 1 FROM moria.action_keys k
      JOIN moria.runtime_delivery_destinations d ON d.id=w.destination
      JOIN moria.runtime_execution_bindings b ON b.id=d.binding
      WHERE NEW.environment=w.environment AND NEW.destination_resource=b.resource AND NEW.resource_class=b.spec->>'resourceClass'
      AND NEW.source_markings=ARRAY(SELECT DISTINCT value FROM unnest(b.markings||ARRAY(SELECT jsonb_array_elements_text(k.writeback->'markings'))) value ORDER BY value)
      AND k.environment=w.environment AND k.action=w.action::text
      AND k.action_revision=w.action_revision AND k.principal=NEW.origin->>'principal' AND k.application=NEW.origin->>'application'
      AND k.key=NEW.origin->>'key' AND k.writeback=NEW.writeback
      AND NEW.origin=jsonb_build_object('kind','action','environment',k.environment,'application',k.application,'principal',k.principal,'action',k.action,'key',k.key)
      AND NEW.logical_key='action/'||k.action||'/'||k.key||'/'||w.effect_key
      AND NEW.payload=jsonb_build_object('version',1,'action',k.action,'key',k.key,'result',k.result)
      AND NEW.body=NEW.payload::text AND NEW.payload_hash=encode(sha256(convert_to(NEW.body,'UTF8')),'hex')
      AND NEW.origin_hash=encode(sha256(convert_to(NEW.origin::text,'UTF8')),'hex'))
    THEN RAISE EXCEPTION 'DELIVERY_WRITEBACK_ORIGIN_INVALID'; END IF;
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_delivery_writeback_intent_guard() FROM PUBLIC;
CREATE TRIGGER runtime_delivery_writeback_intent_guard BEFORE INSERT ON moria.runtime_deliveries
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_delivery_writeback_intent_guard();

CREATE FUNCTION moria.runtime_delivery_writeback_from_action() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path=pg_catalog,moria,pg_temp AS $$
DECLARE w moria.runtime_delivery_writebacks; b moria.runtime_execution_bindings;
  payload jsonb; origin jsonb; body text; labels text[]; intent uuid;
BEGIN
  SELECT * INTO STRICT w FROM moria.runtime_delivery_writebacks WHERE environment=NEW.environment
    AND action=CASE WHEN NEW.action~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NEW.action::uuid END AND action_revision=NEW.action_revision;
  SELECT binding.* INTO STRICT b FROM moria.runtime_delivery_destinations d JOIN moria.runtime_execution_bindings binding
    ON binding.id=d.binding WHERE d.id=w.destination;
  IF ROW(b.environment,b.context->>'principal',b.context->>'application')
    IS DISTINCT FROM ROW(NEW.environment,NEW.principal,NEW.application) THEN RAISE EXCEPTION 'DELIVERY_WRITEBACK_ORIGIN_INVALID'; END IF;
  payload:=jsonb_build_object('version',1,'action',NEW.action,'key',NEW.key,'result',NEW.result);
  body:=payload::text;
  origin:=jsonb_build_object('kind','action','environment',NEW.environment,'application',NEW.application,
    'principal',NEW.principal,'action',NEW.action,'key',NEW.key);
  labels:=ARRAY(SELECT DISTINCT value FROM unnest(b.markings||ARRAY(SELECT jsonb_array_elements_text(NEW.writeback->'markings'))) value ORDER BY value);
  IF octet_length(body)>65536 OR octet_length(origin::text)>4096
    OR octet_length('action/'||NEW.action||'/'||NEW.key||'/'||w.effect_key)>512 THEN RAISE EXCEPTION 'DELIVERY_INVALID'; END IF;
  INSERT INTO moria.runtime_deliveries(destination,destination_resource,environment,resource_class,logical_key,origin_hash,
    payload,body,payload_hash,source_markings,origin,writeback)
  VALUES(w.destination,b.resource,NEW.environment,b.spec->>'resourceClass','action/'||NEW.action||'/'||NEW.key||'/'||w.effect_key,
    encode(sha256(convert_to(origin::text,'UTF8')),'hex'),payload,body,encode(sha256(convert_to(body,'UTF8')),'hex'),labels,origin,NEW.writeback)
  RETURNING id INTO intent;
  PERFORM moria.runtime_delivery_check(intent);
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_delivery_writeback_from_action() FROM PUBLIC;
CREATE TRIGGER runtime_delivery_writeback_from_action AFTER INSERT ON moria.action_keys
  FOR EACH ROW WHEN (NEW.writeback IS NOT NULL) EXECUTE FUNCTION moria.runtime_delivery_writeback_from_action();
`;
