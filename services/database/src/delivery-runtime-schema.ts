/** External delivery is independent of the action/publication transaction that records intent. */
export const deliveryRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_delivery_destinations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), binding uuid NOT NULL REFERENCES moria.runtime_execution_bindings(id),
  endpoint text NOT NULL, secret_ref text, idempotent boolean NOT NULL,
  allowed_markings text[] NOT NULL, maximum_attempts integer NOT NULL DEFAULT 3 CHECK(maximum_attempts BETWEEN 1 AND 10),
  retry_seconds integer NOT NULL DEFAULT 30 CHECK(retry_seconds BETWEEN 1 AND 300),
  enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE moria.runtime_deliveries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), destination uuid NOT NULL REFERENCES moria.runtime_delivery_destinations(id),
  destination_resource uuid NOT NULL, environment text NOT NULL REFERENCES moria.environments(id), resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
  logical_key text NOT NULL, origin_hash text NOT NULL, payload jsonb NOT NULL, body text NOT NULL, payload_hash text NOT NULL,
  source_markings text[] NOT NULL, origin jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','delivering','retryScheduled','delivered','failed','uncertain','cancelled')),
  owner uuid, fence bigint NOT NULL DEFAULT 0, lease_until timestamptz, sent_at timestamptz,
  attempts integer NOT NULL DEFAULT 0, cycle_attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT clock_timestamp(), deadline timestamptz NOT NULL DEFAULT clock_timestamp()+interval '30 minutes',
  receipt jsonb, error text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  UNIQUE(destination_resource,origin_hash,logical_key)
);
CREATE INDEX runtime_deliveries_pending ON moria.runtime_deliveries(resource_class,available_at,created_at)
  WHERE status IN ('pending','retryScheduled','delivering');
CREATE INDEX runtime_delivery_deadlines ON moria.runtime_deliveries(deadline,id) WHERE status IN ('pending','retryScheduled','delivering');
CREATE INDEX runtime_delivery_expiry ON moria.runtime_deliveries(lease_until,id) WHERE status='delivering';
CREATE TABLE moria.runtime_delivery_attempts (
  delivery uuid NOT NULL REFERENCES moria.runtime_deliveries(id), fence bigint NOT NULL, owner uuid NOT NULL,
  status text NOT NULL DEFAULT 'claimed', sent_at timestamptz, error text, receipt jsonb,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  PRIMARY KEY(delivery,fence)
);
CREATE TABLE moria.runtime_delivery_reconciliations (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, delivery uuid NOT NULL REFERENCES moria.runtime_deliveries(id),
  expected_fence bigint NOT NULL, resolution text NOT NULL CHECK(resolution IN ('delivered','notDelivered')),
  evidence text NOT NULL CHECK(octet_length(evidence) BETWEEN 1 AND 1024), created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION moria.runtime_delivery_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME='runtime_delivery_destinations' THEN
    IF (to_jsonb(NEW)-'enabled')<>(to_jsonb(OLD)-'enabled') THEN RAISE EXCEPTION 'IMMUTABLE_DESTINATION'; END IF;
  ELSE
    IF ROW(NEW.destination,NEW.destination_resource,NEW.environment,NEW.resource_class,NEW.logical_key,NEW.origin_hash,NEW.payload,NEW.body,NEW.payload_hash,NEW.source_markings,NEW.origin)
      IS DISTINCT FROM ROW(OLD.destination,OLD.destination_resource,OLD.environment,OLD.resource_class,OLD.logical_key,OLD.origin_hash,OLD.payload,OLD.body,OLD.payload_hash,OLD.source_markings,OLD.origin)
      THEN RAISE EXCEPTION 'IMMUTABLE_DELIVERY'; END IF;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_destination_immutable BEFORE UPDATE ON moria.runtime_delivery_destinations FOR EACH ROW EXECUTE FUNCTION moria.runtime_delivery_immutable();
CREATE FUNCTION moria.runtime_delivery_destination_epoch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN
    UPDATE moria.environments SET security_epoch=security_epoch+1
      WHERE id=(SELECT environment FROM moria.runtime_execution_bindings WHERE id=NEW.binding);
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_delivery_destination_epoch AFTER UPDATE ON moria.runtime_delivery_destinations
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_delivery_destination_epoch();
CREATE TRIGGER runtime_delivery_immutable BEFORE UPDATE ON moria.runtime_deliveries FOR EACH ROW EXECUTE FUNCTION moria.runtime_delivery_immutable();

CREATE FUNCTION moria.runtime_resource_in_use(class_name text, environment_id text DEFAULT NULL) RETURNS bigint LANGUAGE sql AS $$
  SELECT (SELECT count(*) FROM moria.runtime_execution_tasks t JOIN moria.runtime_executions e ON e.id=t.execution
    WHERE e.resource_class=class_name AND (environment_id IS NULL OR e.environment=environment_id)
      AND e.status='running' AND t.status='running' AND t.lease_until>clock_timestamp())
    +(SELECT count(*) FROM moria.runtime_executions e WHERE e.resource_class=class_name AND (environment_id IS NULL OR e.environment=environment_id)
      AND e.status='preparing' AND e.lease_until>clock_timestamp())
    +(SELECT count(*) FROM moria.runtime_deliveries d WHERE d.resource_class=class_name AND (environment_id IS NULL OR d.environment=environment_id)
      AND d.status='delivering' AND d.lease_until>clock_timestamp())
$$;

CREATE FUNCTION moria.runtime_destination_check(destination_id uuid, labels text[], dispatch boolean DEFAULT true) RETURNS void LANGUAGE plpgsql AS $$
DECLARE d moria.runtime_delivery_destinations; b moria.runtime_execution_bindings; clearance text[];
BEGIN
  IF labels IS NULL OR array_position(labels,NULL) IS NOT NULL
    THEN RAISE EXCEPTION 'DELIVERY_INVALID'; END IF;
  SELECT * INTO d FROM moria.runtime_delivery_destinations WHERE id=destination_id;
  IF NOT FOUND OR (dispatch AND NOT d.enabled) THEN RAISE EXCEPTION 'DESTINATION_DISABLED'; END IF;
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=d.binding;
  PERFORM moria.runtime_binding_check(d.binding);
  SELECT markings INTO clearance FROM moria.principals WHERE environment=b.environment AND principal=b.context->>'principal';
  IF b.spec->>'version' IS DISTINCT FROM '1' OR b.spec->>'domain' IS DISTINCT FROM 'delivery' OR NOT ARRAY[b.resource::text||':write']<@b.operations
    OR NOT labels<@d.allowed_markings OR NOT labels<@clearance OR NOT b.markings<@d.allowed_markings
    THEN RAISE EXCEPTION 'DELIVERY_FORBIDDEN'; END IF;
END $$;

CREATE FUNCTION moria.runtime_delivery_check(delivery_id uuid, dispatch boolean DEFAULT true) RETURNS void LANGUAGE plpgsql AS $$
DECLARE d moria.runtime_deliveries;
BEGIN
  SELECT * INTO d FROM moria.runtime_deliveries WHERE id=delivery_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'DELIVERY_UNKNOWN'; END IF;
  PERFORM moria.runtime_destination_check(d.destination,d.source_markings,dispatch);
END $$;

CREATE FUNCTION moria.runtime_delivery_enqueue(destination_id uuid, logical_key text, payload jsonb, source_markings text[], origin jsonb) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE d moria.runtime_delivery_destinations; b moria.runtime_execution_bindings; old moria.runtime_deliveries; labels text[]; wire text; scope_hash text; result uuid;
BEGIN
  IF payload IS NULL OR origin IS NULL OR source_markings IS NULL OR array_position(source_markings,NULL) IS NOT NULL
    THEN RAISE EXCEPTION 'DELIVERY_INVALID'; END IF;
  SELECT * INTO d FROM moria.runtime_delivery_destinations WHERE id=destination_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'DESTINATION_DISABLED'; END IF;
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=d.binding;
  SELECT ARRAY(SELECT DISTINCT value FROM unnest(b.markings||source_markings) value ORDER BY value) INTO labels;
  PERFORM moria.runtime_destination_check(destination_id,labels);
  wire:=payload::text;
  IF logical_key IS NULL OR octet_length(logical_key) NOT BETWEEN 1 AND 512 OR octet_length(wire)>65536 OR jsonb_typeof(origin)<>'object'
    OR octet_length(origin::text)>4096 OR origin->>'environment' IS DISTINCT FROM b.environment
    THEN RAISE EXCEPTION 'DELIVERY_INVALID'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_each(origin) field WHERE jsonb_typeof(field.value)<>'string') THEN RAISE EXCEPTION 'DELIVERY_ORIGIN_INVALID'; END IF;
  IF origin->>'kind'='action' THEN
    IF (SELECT count(*) FROM jsonb_object_keys(origin))<>6 OR origin->>'principal' IS DISTINCT FROM b.context->>'principal' OR origin->>'application' IS DISTINCT FROM b.context->>'application'
      OR NOT EXISTS(SELECT 1 FROM moria.action_keys a WHERE a.environment=b.environment AND a.principal=origin->>'principal'
        AND a.application=origin->>'application' AND a.action=origin->>'action' AND a.key=origin->>'key' AND a.result IS NOT NULL)
      THEN RAISE EXCEPTION 'DELIVERY_ORIGIN_INVALID'; END IF;
  ELSIF origin->>'kind'='publication' THEN
    IF (SELECT count(*) FROM jsonb_object_keys(origin))<>3 OR NOT EXISTS(SELECT 1 FROM moria.publications p WHERE p.environment=b.environment AND p.execution=origin->>'execution')
      THEN RAISE EXCEPTION 'DELIVERY_ORIGIN_INVALID'; END IF;
  ELSE RAISE EXCEPTION 'DELIVERY_ORIGIN_INVALID'; END IF;
  scope_hash:=encode(sha256(convert_to(origin::text,'UTF8')),'hex');
  PERFORM pg_advisory_xact_lock(hashtextextended('moria/delivery/'||jsonb_build_array(b.resource,scope_hash,logical_key)::text,0));
  SELECT * INTO old FROM moria.runtime_deliveries x WHERE x.destination_resource=b.resource AND x.origin_hash=scope_hash AND x.logical_key=runtime_delivery_enqueue.logical_key;
  IF FOUND THEN
    IF old.payload IS DISTINCT FROM payload OR old.source_markings IS DISTINCT FROM labels OR old.origin IS DISTINCT FROM origin
      THEN RAISE EXCEPTION 'IDEMPOTENCY_CONFLICT'; END IF;
    RETURN old.id;
  END IF;
  INSERT INTO moria.runtime_deliveries(destination,destination_resource,environment,resource_class,logical_key,origin_hash,payload,body,payload_hash,source_markings,origin)
    VALUES(d.id,b.resource,b.environment,b.spec->>'resourceClass',logical_key,scope_hash,payload,wire,encode(sha256(convert_to(wire,'UTF8')),'hex'),labels,origin) RETURNING id INTO result;
  RETURN result;
END $$;

CREATE FUNCTION moria.runtime_delivery_reap() RETURNS integer LANGUAGE plpgsql AS $$
DECLARE item moria.runtime_deliveries; d moria.runtime_delivery_destinations; next_state text; n integer:=0;
  candidates uuid[]; reaped_at timestamptz:=clock_timestamp();
BEGIN
  WITH due AS (
    (SELECT id FROM moria.runtime_deliveries WHERE status IN ('pending','retryScheduled','delivering') AND deadline<=reaped_at ORDER BY deadline,id LIMIT 64)
    UNION (SELECT id FROM moria.runtime_deliveries WHERE status='delivering' AND lease_until<=reaped_at ORDER BY lease_until,id LIMIT 64)
  ) SELECT array_agg(id) INTO candidates FROM due;
  FOR item IN SELECT * FROM moria.runtime_deliveries x WHERE x.id=ANY(candidates) AND x.status IN ('pending','retryScheduled','delivering')
    AND (x.deadline<=clock_timestamp() OR (x.status='delivering' AND x.lease_until<=clock_timestamp()))
    ORDER BY x.created_at FOR UPDATE SKIP LOCKED LIMIT 64 LOOP
    SELECT * INTO d FROM moria.runtime_delivery_destinations WHERE id=item.destination;
    IF item.sent_at IS NOT NULL AND NOT d.idempotent THEN next_state:='uncertain';
    ELSIF item.deadline<=clock_timestamp() OR item.cycle_attempts>=d.maximum_attempts THEN next_state:='failed';
    ELSE next_state:='retryScheduled'; END IF;
    UPDATE moria.runtime_deliveries SET status=next_state,error=CASE WHEN next_state='uncertain' THEN 'DELIVERY_UNCERTAIN'
      WHEN item.deadline<=clock_timestamp() THEN 'DEADLINE_EXCEEDED' ELSE 'OWNER_LOST' END,
      owner=NULL,lease_until=NULL,available_at=clock_timestamp()+d.retry_seconds*interval '1 second',
      finished_at=CASE WHEN next_state IN ('failed','uncertain') THEN clock_timestamp() ELSE NULL END WHERE id=item.id;
    UPDATE moria.runtime_delivery_attempts SET status='interrupted',error='OWNER_LOST',finished_at=clock_timestamp()
      WHERE delivery=item.id AND fence=item.fence AND status IN ('claimed','sent');
    n:=n+1;
  END LOOP;
  RETURN n;
END $$;
`;
