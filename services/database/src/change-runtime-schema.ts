/** Object invalidations share the originating authority transaction and commit order. */
export const changeRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_change_transactions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  environment text NOT NULL REFERENCES moria.environments(id),
  incarnation uuid NOT NULL, transaction_id xid8 NOT NULL,
  commit_revision bigint, item_count integer NOT NULL DEFAULT 0 CHECK(item_count BETWEEN 0 AND 10000),
  complete boolean NOT NULL DEFAULT false,
  UNIQUE(environment,transaction_id), UNIQUE(environment,commit_revision)
);
CREATE TABLE moria.runtime_change_items (
  transaction_id uuid NOT NULL REFERENCES moria.runtime_change_transactions(id),
  ordinal integer NOT NULL CHECK(ordinal>=0),
  revision bigint NOT NULL, object_type text NOT NULL CHECK(octet_length(object_type)<=1024),
  kind text NOT NULL CHECK(kind IN ('object','rebuild')),
  uid uuid, generation uuid,
  PRIMARY KEY(transaction_id,ordinal),
  CHECK((kind='object' AND uid IS NOT NULL AND generation IS NULL)
    OR (kind='rebuild' AND uid IS NULL))
);
CREATE TABLE moria.runtime_change_pins (
  id uuid PRIMARY KEY, environment text NOT NULL REFERENCES moria.environments(id),
  incarnation uuid NOT NULL, owner uuid NOT NULL, after_revision bigint NOT NULL,
  expires_at timestamptz NOT NULL
);
CREATE INDEX runtime_change_pin_retention ON moria.runtime_change_pins(environment,after_revision,expires_at);

CREATE FUNCTION moria.runtime_capture_change_item(env text,item_revision bigint,type_name text,item_kind text,item_uid uuid,item_generation uuid)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE frame uuid;ordinal_value integer;current_revision bigint;
BEGIN
  SELECT revision INTO current_revision FROM moria.environments WHERE id=env FOR UPDATE;
  IF item_revision>current_revision THEN RAISE EXCEPTION 'CHANGE_REVISION_INVALID'; END IF;
  INSERT INTO moria.runtime_change_transactions(environment,incarnation,transaction_id)
    VALUES(env,(SELECT incarnation FROM moria.indexing_instance),pg_current_xact_id())
    ON CONFLICT(environment,transaction_id) DO NOTHING;
  UPDATE moria.runtime_change_transactions SET item_count=item_count+1
    WHERE environment=env AND transaction_id=pg_current_xact_id() AND NOT complete
    RETURNING id,item_count-1 INTO frame,ordinal_value;
  IF NOT FOUND THEN RAISE EXCEPTION 'CHANGE_FRAME_SEALED'; END IF;
  INSERT INTO moria.runtime_change_items VALUES(frame,ordinal_value,item_revision,type_name,item_kind,item_uid,item_generation);
END $$;
CREATE FUNCTION moria.runtime_capture_object_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.runtime_capture_change_item(NEW.environment,NEW.revision,NEW.object_type,'object',NEW.uid,NULL);
  RETURN NEW;
END $$;
CREATE FUNCTION moria.runtime_capture_source_generation() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
  IF TG_RELID<>'moria.source_generations'::regclass THEN RAISE EXCEPTION 'CHANGE_INVALID';END IF;
  IF NOT moria.runtime_source_generation_reused(NEW.environment,NEW.object_type,NEW.revision,NEW.stage) THEN
    PERFORM moria.runtime_capture_change_item(NEW.environment,NEW.revision,NEW.object_type,'rebuild',NULL,NEW.stage);
  END IF;
  RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_capture_source_generation() FROM PUBLIC;
CREATE FUNCTION moria.runtime_capture_link_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE endpoint record;
BEGIN
  FOR endpoint IN
    SELECT DISTINCT type_name,uid FROM moria.native_serving_links l
      CROSS JOIN LATERAL(VALUES(l.source_resource,NEW.source_uid),(l.target_resource,NEW.target_uid)) ends(type_name,uid)
      WHERE l.environment=NEW.environment AND l.resource=NEW.link
  LOOP
    PERFORM moria.runtime_capture_change_item(NEW.environment,NEW.revision,endpoint.type_name,'object',endpoint.uid,NULL);
  END LOOP;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_capture_link AFTER INSERT ON moria.native_link_edge_versions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_capture_link_change();
CREATE TRIGGER runtime_capture_overlay AFTER INSERT ON moria.overlay_versions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_capture_object_change();
CREATE TRIGGER runtime_capture_generation AFTER INSERT ON moria.source_generations
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_capture_source_generation();

CREATE FUNCTION moria.runtime_seal_change_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE moria.runtime_change_transactions t SET complete=true,
    commit_revision=(SELECT revision FROM moria.environments WHERE id=t.environment)
    WHERE t.id=NEW.id AND NOT t.complete;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER runtime_change_transaction_end AFTER INSERT ON moria.runtime_change_transactions
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_seal_change_transaction();
`;
