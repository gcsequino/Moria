/** Normalized operational read pins; expiration never comes from caller-supplied evidence. */
export const historyRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_history_floors(
 environment text PRIMARY KEY REFERENCES moria.environments(id),revision bigint NOT NULL CHECK(revision>=0),
 collection uuid NOT NULL,advanced_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION moria.runtime_history_floor_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR NEW.environment<>OLD.environment OR NEW.revision<OLD.revision THEN
  RAISE EXCEPTION 'HISTORY_FLOOR_RESERVED'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_floor_guard BEFORE UPDATE OR DELETE ON moria.runtime_history_floors
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_floor_guard();
CREATE FUNCTION moria.runtime_history_assert(env text,at_revision bigint) RETURNS void
 LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF at_revision IS NULL OR at_revision<0 OR EXISTS(SELECT 1 FROM moria.runtime_history_floors f WHERE f.environment=env AND f.revision>at_revision)
 THEN RAISE EXCEPTION 'HISTORY_EXPIRED'; END IF;
END $$;
CREATE TABLE moria.runtime_history_read_pins(
 execution uuid NOT NULL REFERENCES moria.runtime_executions(id),call_key text NOT NULL,
 environment text NOT NULL REFERENCES moria.environments(id),revision bigint NOT NULL CHECK(revision>=0),
 expires_at timestamptz NOT NULL,PRIMARY KEY(execution,call_key,revision)
);
CREATE INDEX runtime_history_read_pin_floor ON moria.runtime_history_read_pins(environment,revision,expires_at);
CREATE INDEX runtime_history_read_pin_expiry ON moria.runtime_history_read_pins(environment,expires_at,execution,call_key);
CREATE FUNCTION moria.runtime_history_record_reads() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE env text;expiry timestamptz;item jsonb;position text;
BEGIN
 IF TG_OP='UPDATE' AND ROW(NEW.execution,NEW.call_key) IS DISTINCT FROM ROW(OLD.execution,OLD.call_key) THEN RAISE EXCEPTION 'HISTORY_PIN_INVALID'; END IF;
 IF TG_OP='DELETE' THEN
  DELETE FROM moria.runtime_history_read_pins WHERE execution=OLD.execution AND call_key=OLD.call_key;
  RETURN OLD;
 END IF;
 SELECT e.environment,CASE WHEN e.status IN ('queued','running','preparing') OR e.finished_at IS NULL
   THEN 'infinity'::timestamptz ELSE e.finished_at+interval '30 days' END INTO STRICT env,expiry
   FROM moria.runtime_executions e WHERE e.id=NEW.execution;
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/environment/'||env,0));
 IF jsonb_typeof(NEW.evidence)<>'array' OR jsonb_array_length(NEW.evidence)>256 THEN RAISE EXCEPTION 'HISTORY_PIN_INVALID'; END IF;
 DELETE FROM moria.runtime_history_read_pins WHERE execution=NEW.execution AND call_key=NEW.call_key;
 FOR item IN SELECT value FROM jsonb_array_elements(NEW.evidence) LOOP
  position:=item->>'revision';
  IF position IS NULL OR position !~ '^(0|[1-9][0-9]{0,18})$' OR position::numeric>9223372036854775807 THEN RAISE EXCEPTION 'HISTORY_PIN_INVALID'; END IF;
  PERFORM moria.runtime_history_assert(env,position::bigint);
  INSERT INTO moria.runtime_history_read_pins VALUES(NEW.execution,NEW.call_key,env,position::bigint,expiry) ON CONFLICT DO NOTHING;
 END LOOP;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_record_reads BEFORE INSERT OR UPDATE OR DELETE ON moria.native_function_reads
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_record_reads();
CREATE FUNCTION moria.runtime_history_execution_expiry() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF ROW(NEW.status,NEW.finished_at) IS DISTINCT FROM ROW(OLD.status,OLD.finished_at) THEN
  UPDATE moria.runtime_history_read_pins SET expires_at=CASE WHEN NEW.status IN ('queued','running','preparing') OR NEW.finished_at IS NULL
    THEN 'infinity'::timestamptz ELSE NEW.finished_at+interval '30 days' END WHERE execution=NEW.id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_execution_expiry AFTER UPDATE ON moria.runtime_executions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_execution_expiry();
CREATE FUNCTION moria.runtime_history_cursor_guard() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/environment/'||NEW.environment,0));
 PERFORM moria.runtime_history_assert(NEW.environment,NEW.revision);
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_cursor_guard BEFORE INSERT OR UPDATE ON moria.read_cursors
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_cursor_guard();
CREATE TABLE moria.runtime_history_revisions(
 environment text NOT NULL REFERENCES moria.environments(id),revision bigint NOT NULL CHECK(revision>=0),
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(environment,revision)
);
CREATE INDEX runtime_history_revision_age ON moria.runtime_history_revisions(environment,observed_at,revision);
CREATE FUNCTION moria.runtime_history_revision_record() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE position bigint;
BEGIN
 IF TG_TABLE_NAME='native_object_creations' THEN position:=NEW.created_revision;ELSE position:=NEW.revision;END IF;
 INSERT INTO moria.runtime_history_revisions(environment,revision) VALUES(NEW.environment,position)
 ON CONFLICT(environment,revision) DO NOTHING;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_source_revision AFTER INSERT ON moria.source_generations FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_revision_record();
CREATE TRIGGER runtime_history_overlay_revision AFTER INSERT ON moria.overlay_versions FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_revision_record();
CREATE TRIGGER runtime_history_link_revision AFTER INSERT ON moria.native_link_edge_versions FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_revision_record();
CREATE TRIGGER runtime_history_creation_revision AFTER INSERT ON moria.native_object_creations FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_revision_record();

CREATE TABLE moria.runtime_history_policies(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),request_key text NOT NULL UNIQUE CHECK(octet_length(request_key) BETWEEN 1 AND 256),
 environment text NOT NULL REFERENCES moria.environments(id),retain_seconds bigint NOT NULL CHECK(retain_seconds BETWEEN 86400 AND 315360000),
 keep_generations integer NOT NULL CHECK(keep_generations BETWEEN 2 AND 100),incarnation uuid NOT NULL,
 approved_role name NOT NULL DEFAULT session_user,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),enabled boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX runtime_history_policy_active ON moria.runtime_history_policies(environment) WHERE enabled;
CREATE TABLE moria.runtime_history_collections(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),policy uuid NOT NULL REFERENCES moria.runtime_history_policies(id),
 incarnation uuid NOT NULL,floor bigint NOT NULL CHECK(floor>=0),observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 not_before timestamptz NOT NULL DEFAULT clock_timestamp()+interval '24 hours',
 status text NOT NULL DEFAULT 'ready' CHECK(status IN ('ready','collecting','blocked','complete')),
 phase text NOT NULL DEFAULT 'overlays' CHECK(phase IN ('overlays','links','sources','projections','changes','pins','complete')),
 position jsonb NOT NULL DEFAULT '{}'::jsonb,removed_rows bigint NOT NULL DEFAULT 0 CHECK(removed_rows>=0),
 completed_at timestamptz,dropped_tables bigint NOT NULL DEFAULT 0 CHECK(dropped_tables>=0),CHECK(not_before>=observed_at+interval '24 hours')
);
CREATE UNIQUE INDEX runtime_history_collection_live ON moria.runtime_history_collections(policy) WHERE status IN ('ready','collecting');
CREATE INDEX runtime_history_overlay_scan ON moria.overlay_versions(environment,revision,object_type,uid);
CREATE INDEX runtime_history_link_scan ON moria.native_link_edge_versions(environment,revision,link,source_uid,target_uid);
CREATE INDEX runtime_history_active_search ON moria.native_search_generations(environment,status,covered_revision,base_revision);
CREATE FUNCTION moria.runtime_history_operator() RETURNS void LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname=session_user AND r.rolsuper)
 AND NOT EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname='moria' AND pg_has_role(session_user,n.nspowner,'MEMBER'))
 THEN RAISE EXCEPTION 'HISTORY_OPERATOR_REQUIRED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_history_safe_floor(env text,retention_seconds bigint) RETURNS bigint
 LANGUAGE sql STABLE SET search_path=pg_catalog,moria AS $$
 WITH boundary AS(SELECT LEAST(e.revision,COALESCE((SELECT max(a.revision) FROM moria.runtime_history_revisions a WHERE a.environment=env AND a.observed_at<=statement_timestamp()-retention_seconds*interval '1 second'),0),
 COALESCE((SELECT min(p.revision) FROM moria.runtime_history_read_pins p WHERE p.environment=env AND p.expires_at>statement_timestamp()),e.revision),
 COALESCE((SELECT min(c.revision) FROM moria.read_cursors c WHERE c.environment=env AND c.expires_at>statement_timestamp()),e.revision),
 COALESCE((SELECT min(c.after_revision) FROM moria.runtime_change_pins c JOIN moria.indexing_instance i ON i.incarnation=c.incarnation WHERE c.environment=env AND c.expires_at>statement_timestamp()),e.revision),
 COALESCE((SELECT min(s.expected_revision) FROM moria.source_stages s WHERE s.environment=env AND s.status IN ('loading','ready')),e.revision),
 COALESCE((SELECT min(p.base_revision) FROM moria.runtime_projections p WHERE p.environment=env AND p.status IN ('building','replaying')),e.revision),
 COALESCE((SELECT min(CASE WHEN g.status='active' THEN g.covered_revision ELSE g.base_revision END) FROM moria.native_search_generations g JOIN moria.indexing_instance i ON i.incarnation=g.incarnation WHERE g.environment=env AND g.status IN ('building','ready','active')),e.revision))
 AS revision FROM moria.environments e WHERE e.id=env)
 SELECT COALESCE((SELECT max(t.commit_revision) FROM moria.runtime_change_transactions t JOIN moria.indexing_instance i ON i.incarnation=t.incarnation WHERE t.environment=env AND t.complete AND t.commit_revision<=b.revision),0) FROM boundary b
$$;
CREATE FUNCTION moria.runtime_history_policy_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.id,NEW.request_key,NEW.environment,NEW.retain_seconds,NEW.keep_generations,NEW.incarnation,NEW.approved_role,NEW.created_at)
 IS DISTINCT FROM ROW(OLD.id,OLD.request_key,OLD.environment,OLD.retain_seconds,OLD.keep_generations,OLD.incarnation,OLD.approved_role,OLD.created_at)
 OR (NOT OLD.enabled AND NEW.enabled) THEN RAISE EXCEPTION 'HISTORY_POLICY_RESERVED'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_policy_guard BEFORE UPDATE OR DELETE ON moria.runtime_history_policies FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_policy_guard();
CREATE FUNCTION moria.runtime_history_control_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM moria.runtime_history_operator();
 IF TG_OP='INSERT' AND TG_TABLE_NAME='runtime_history_policies' THEN
  IF NEW.approved_role IS DISTINCT FROM session_user THEN RAISE EXCEPTION 'HISTORY_OPERATOR_REQUIRED'; END IF;
 END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER runtime_history_policy_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_history_policies FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_control_guard();
CREATE TRIGGER runtime_history_collection_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_history_collections FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_control_guard();
CREATE TRIGGER runtime_history_floor_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_history_floors FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_control_guard();
CREATE FUNCTION moria.runtime_history_approve(env text,logical_key text,retention_seconds bigint,generations integer) RETURNS uuid
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE prior moria.runtime_history_policies;identity uuid;
BEGIN
 PERFORM moria.runtime_history_operator();
 SELECT incarnation INTO STRICT identity FROM moria.indexing_instance;
 PERFORM pg_advisory_xact_lock(hashtextextended('moria/history-policy/'||env,0));
 SELECT * INTO prior FROM moria.runtime_history_policies WHERE request_key=logical_key;
 IF FOUND THEN
  IF ROW(prior.environment,prior.retain_seconds,prior.keep_generations,prior.incarnation) IS DISTINCT FROM ROW(env,retention_seconds,generations,identity)
  THEN RAISE EXCEPTION 'HISTORY_POLICY_CONFLICT'; END IF;
  RETURN prior.id;
 END IF;
 INSERT INTO moria.runtime_history_policies(environment,request_key,retain_seconds,keep_generations,incarnation)
 VALUES(env,logical_key,retention_seconds,generations,identity) RETURNING id INTO identity;
 RETURN identity;
END $$;
CREATE FUNCTION moria.runtime_history_plan(policy_id uuid) RETURNS uuid
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE policy moria.runtime_history_policies;identity uuid;boundary bigint;
BEGIN
 PERFORM moria.runtime_history_operator();
 SELECT * INTO STRICT policy FROM moria.runtime_history_policies WHERE id=policy_id FOR UPDATE;
 IF NOT policy.enabled OR policy.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'HISTORY_POLICY_REVOKED'; END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/environment/'||policy.environment,0)) THEN RAISE EXCEPTION 'HISTORY_BUSY'; END IF;
 SELECT id INTO identity FROM moria.runtime_history_collections c WHERE c.policy=policy_id AND c.status IN ('ready','collecting');
 IF FOUND THEN RETURN identity; END IF;
 boundary:=moria.runtime_history_safe_floor(policy.environment,policy.retain_seconds);
 INSERT INTO moria.runtime_history_collections(policy,incarnation,floor) VALUES(policy.id,policy.incarnation,boundary) RETURNING id INTO identity;
 RETURN identity;
END $$;

CREATE FUNCTION moria.runtime_history_step(policy_id uuid) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE policy moria.runtime_history_policies;work moria.runtime_history_collections;boundary bigint;scanned bigint;removed bigint:=0;last_position jsonb;source_head record;projection record;relation text;
BEGIN
 PERFORM moria.runtime_history_operator();
 SELECT * INTO STRICT policy FROM moria.runtime_history_policies WHERE id=policy_id FOR SHARE;
 IF NOT policy.enabled OR policy.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'HISTORY_POLICY_REVOKED'; END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/environment/'||policy.environment,0)) THEN
  RETURN jsonb_build_object('state','busy'); END IF;
 SELECT * INTO work FROM moria.runtime_history_collections c WHERE c.policy=policy_id AND c.status IN ('ready','collecting') FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('state','planRequired'); END IF;
 IF work.incarnation<>policy.incarnation THEN RAISE EXCEPTION 'HISTORY_POLICY_REVOKED'; END IF;
 IF work.not_before>clock_timestamp() THEN RETURN jsonb_build_object('state','grace','collection',work.id,'eligibleAt',work.not_before); END IF;
 boundary:=moria.runtime_history_safe_floor(policy.environment,policy.retain_seconds);
 IF boundary<work.floor THEN
  IF work.status='collecting' THEN RAISE EXCEPTION 'HISTORY_PIN_BELOW_FLOOR'; END IF;
  UPDATE moria.runtime_history_collections SET status='blocked' WHERE id=work.id;
  RETURN jsonb_build_object('state','pinned','collection',work.id);
 END IF;
 IF work.status='ready' THEN
  INSERT INTO moria.runtime_history_floors(environment,revision,collection) VALUES(policy.environment,work.floor,work.id)
   ON CONFLICT(environment) DO UPDATE SET revision=GREATEST(runtime_history_floors.revision,EXCLUDED.revision),collection=EXCLUDED.collection,advanced_at=clock_timestamp();
  UPDATE moria.runtime_history_collections SET status='collecting' WHERE id=work.id;
 END IF;
 IF work.phase='overlays' THEN
  WITH batch AS MATERIALIZED(SELECT revision,object_type,uid FROM moria.overlay_versions
   WHERE environment=policy.environment AND revision<work.floor
   AND (work.position='{}'::jsonb OR (revision,object_type,uid)>((work.position->>'revision')::bigint,work.position->>'resource',(work.position->>'uid')::uuid))
   ORDER BY revision,object_type,uid LIMIT 1000),obsolete AS MATERIALIZED(SELECT b.* FROM batch b WHERE EXISTS(
    SELECT 1 FROM moria.overlay_versions newer WHERE newer.environment=policy.environment AND newer.object_type=b.object_type AND newer.uid=b.uid AND newer.revision>b.revision AND newer.revision<=work.floor)),
   deleted AS(DELETE FROM moria.overlay_versions old USING obsolete b WHERE old.environment=policy.environment AND old.object_type=b.object_type AND old.uid=b.uid AND old.revision=b.revision RETURNING 1)
  SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM deleted),(SELECT jsonb_build_object('revision',revision::text,'resource',object_type,'uid',uid) FROM batch ORDER BY revision DESC,object_type DESC,uid DESC LIMIT 1)
  INTO scanned,removed,last_position;
  UPDATE moria.runtime_history_collections SET phase=CASE WHEN scanned=0 THEN 'links' ELSE phase END,position=COALESCE(last_position,'{}'::jsonb),removed_rows=removed_rows+removed WHERE id=work.id;
 ELSIF work.phase='links' THEN
  WITH batch AS MATERIALIZED(SELECT revision,link,source_uid,target_uid FROM moria.native_link_edge_versions
   WHERE environment=policy.environment AND revision<work.floor
   AND (work.position='{}'::jsonb OR (revision,link,source_uid,target_uid)>((work.position->>'revision')::bigint,work.position->>'resource',(work.position->>'source')::uuid,(work.position->>'target')::uuid))
   ORDER BY revision,link,source_uid,target_uid LIMIT 1000),obsolete AS MATERIALIZED(SELECT b.* FROM batch b WHERE EXISTS(
    SELECT 1 FROM moria.native_link_edge_versions newer WHERE newer.environment=policy.environment AND newer.link=b.link AND newer.source_uid=b.source_uid AND newer.target_uid=b.target_uid AND newer.revision>b.revision AND newer.revision<=work.floor)),
   deleted AS(DELETE FROM moria.native_link_edge_versions old USING obsolete b WHERE old.environment=policy.environment AND old.link=b.link AND old.source_uid=b.source_uid AND old.target_uid=b.target_uid AND old.revision=b.revision RETURNING 1)
  SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM deleted),(SELECT jsonb_build_object('revision',revision::text,'resource',link,'source',source_uid,'target',target_uid) FROM batch ORDER BY revision DESC,link DESC,source_uid DESC,target_uid DESC LIMIT 1)
  INTO scanned,removed,last_position;
  UPDATE moria.runtime_history_collections SET phase=CASE WHEN scanned=0 THEN 'sources' ELSE phase END,position=COALESCE(last_position,'{}'::jsonb),removed_rows=removed_rows+removed WHERE id=work.id;
 ELSIF work.phase='sources' THEN
  SELECT g.* INTO source_head FROM moria.source_generations g WHERE g.environment=policy.environment AND g.revision<work.floor
   AND (work.position='{}'::jsonb OR (g.object_type,g.revision)>=(work.position->>'resource',(work.position->>'revision')::bigint))
   AND NOT EXISTS(SELECT 1 FROM (SELECT r.revision FROM moria.source_generations r WHERE r.environment=g.environment AND r.object_type=g.object_type ORDER BY r.revision DESC LIMIT policy.keep_generations) retained WHERE retained.revision=g.revision)
   AND g.revision<>(SELECT max(r.revision) FROM moria.source_generations r WHERE r.environment=g.environment AND r.object_type=g.object_type AND r.revision<=work.floor)
   AND NOT EXISTS(SELECT 1 FROM moria.native_search_rebuilds r JOIN moria.native_search_generations h ON h.id=r.generation WHERE r.source_stage=g.stage AND h.status IN ('building','ready','active'))
   AND (g.stage IS NULL OR NOT EXISTS(SELECT 1 FROM moria.source_generations shared WHERE shared.stage=g.stage AND ROW(shared.environment,shared.object_type,shared.revision)<>ROW(g.environment,g.object_type,g.revision)))
   ORDER BY g.object_type,g.revision LIMIT 1;
  IF NOT FOUND THEN UPDATE moria.runtime_history_collections SET phase='projections',position='{}'::jsonb WHERE id=work.id;
  ELSE
   IF source_head.stage IS NOT NULL THEN
    WITH batch AS MATERIALIZED(SELECT uid FROM moria.staged_source_rows WHERE stage=source_head.stage AND (work.position->>'uid' IS NULL OR uid>(work.position->>'uid')::uuid) ORDER BY uid LIMIT 1000),
     keys_deleted AS(DELETE FROM moria.native_source_key_map m USING batch b WHERE m.store=source_head.stage AND m.uid=b.uid RETURNING 1),
     deleted AS(DELETE FROM moria.staged_source_rows r USING batch b WHERE r.stage=source_head.stage AND r.uid=b.uid RETURNING 1)
    SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM deleted),(SELECT to_jsonb(uid) FROM batch ORDER BY uid DESC LIMIT 1) INTO scanned,removed,last_position;
   ELSE
    WITH batch AS MATERIALIZED(SELECT uid FROM moria.source_rows WHERE environment=policy.environment AND object_type=source_head.object_type AND revision=source_head.revision AND (work.position->>'uid' IS NULL OR uid>(work.position->>'uid')::uuid) ORDER BY uid LIMIT 1000),
     deleted AS(DELETE FROM moria.source_rows r USING batch b WHERE r.environment=policy.environment AND r.object_type=source_head.object_type AND r.revision=source_head.revision AND r.uid=b.uid RETURNING 1)
    SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM deleted),(SELECT to_jsonb(uid) FROM batch ORDER BY uid DESC LIMIT 1) INTO scanned,removed,last_position;
   END IF;
   IF source_head.stage IS NOT NULL AND scanned=0 THEN
    UPDATE moria.native_source_row_stores SET status='retired' WHERE id=source_head.stage AND status='published';
   END IF;
   UPDATE moria.runtime_history_collections SET position=jsonb_build_object('resource',source_head.object_type,'revision',CASE WHEN scanned=0 THEN (source_head.revision+1)::text ELSE source_head.revision::text END)||CASE WHEN scanned=0 THEN '{}'::jsonb ELSE jsonb_build_object('uid',last_position) END,removed_rows=removed_rows+removed WHERE id=work.id;
  END IF;
 ELSIF work.phase='projections' THEN
  SELECT p.* INTO projection FROM moria.runtime_projections p WHERE p.environment=policy.environment AND p.activation_revision IS NOT NULL
   AND p.status IN ('active','retired') AND (work.position->>'projection' IS NULL OR p.id>=(work.position->>'projection')::uuid)
   ORDER BY p.id LIMIT 1;
  IF NOT FOUND THEN UPDATE moria.runtime_history_collections SET phase='changes',position='{}'::jsonb WHERE id=work.id;
  ELSE
   relation:=moria.runtime_projection_relation(projection.id);
   IF to_regclass(relation) IS NULL THEN scanned:=0;
   ELSIF projection.status='retired' AND projection.closed_revision<work.floor
    AND NOT EXISTS(SELECT 1 FROM (SELECT g.stage FROM moria.source_generations g WHERE g.environment=policy.environment AND g.object_type=projection.resource ORDER BY g.revision DESC LIMIT policy.keep_generations) retained WHERE retained.stage=projection.stage)
    AND projection.stage IS DISTINCT FROM (SELECT g.stage FROM moria.source_generations g WHERE g.environment=policy.environment AND g.object_type=projection.resource AND g.revision<=work.floor ORDER BY g.revision DESC LIMIT 1)
    AND NOT EXISTS(SELECT 1 FROM moria.native_search_rebuilds r JOIN moria.native_search_generations g ON g.id=r.generation WHERE r.source_stage=projection.stage AND g.status IN ('building','ready','active')) THEN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/recovery-retention/v1',0)) THEN
     RETURN jsonb_build_object('state','backupBusy','collection',work.id); END IF;
    EXECUTE 'DROP TABLE '||relation;scanned:=0;
    UPDATE moria.runtime_history_collections SET dropped_tables=dropped_tables+1 WHERE id=work.id;
   ELSE
    EXECUTE format('WITH batch AS MATERIALIZED(SELECT uid,from_revision,to_revision FROM %s WHERE to_revision<=$1 AND ($2::bigint IS NULL OR (to_revision,uid,from_revision)>($2,$3,$4)) ORDER BY to_revision,uid,from_revision LIMIT 1000),deleted AS(DELETE FROM %s r USING batch b WHERE r.uid=b.uid AND r.from_revision=b.from_revision RETURNING 1) SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM deleted),(SELECT jsonb_build_object(''to'',to_revision::text,''uid'',uid,''from'',from_revision::text) FROM batch ORDER BY to_revision DESC,uid DESC,from_revision DESC LIMIT 1)',relation,relation)
     INTO scanned,removed,last_position USING work.floor,(work.position->>'to')::bigint,(work.position->>'uid')::uuid,(work.position->>'from')::bigint;
   END IF;
   UPDATE moria.runtime_history_collections SET position=CASE WHEN scanned=0 THEN jsonb_build_object('projection',projection.id) ELSE jsonb_build_object('projection',projection.id)||last_position END,removed_rows=removed_rows+removed WHERE id=work.id;
   IF scanned=0 THEN
    SELECT p.id INTO projection FROM moria.runtime_projections p WHERE p.environment=policy.environment AND p.activation_revision IS NOT NULL AND p.status IN ('active','retired') AND p.id>projection.id ORDER BY p.id LIMIT 1;
    IF FOUND THEN UPDATE moria.runtime_history_collections SET position=jsonb_build_object('projection',projection.id) WHERE id=work.id;
    ELSE UPDATE moria.runtime_history_collections SET phase='changes',position='{}'::jsonb WHERE id=work.id;END IF;
   END IF;
  END IF;
 ELSIF work.phase='changes' THEN
  WITH batch AS MATERIALIZED(SELECT id,commit_revision FROM moria.runtime_change_transactions WHERE environment=policy.environment AND complete AND commit_revision<work.floor ORDER BY commit_revision LIMIT 100),
   keys AS MATERIALIZED(SELECT i.transaction_id,i.ordinal FROM moria.runtime_change_items i JOIN batch b ON b.id=i.transaction_id ORDER BY b.commit_revision,i.ordinal LIMIT 1000),
   items AS(DELETE FROM moria.runtime_change_items i USING keys k WHERE i.transaction_id=k.transaction_id AND i.ordinal=k.ordinal RETURNING i.transaction_id),
   frames AS(DELETE FROM moria.runtime_change_transactions t USING batch b WHERE t.id=b.id AND
    (SELECT count(*) FROM moria.runtime_change_items i WHERE i.transaction_id=t.id)=(SELECT count(*) FROM items i WHERE i.transaction_id=t.id) RETURNING 1)
  SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM items)+(SELECT count(*) FROM frames) INTO scanned,removed;
  UPDATE moria.runtime_history_collections SET phase=CASE WHEN scanned=0 THEN 'pins' ELSE phase END,removed_rows=removed_rows+removed WHERE id=work.id;
 ELSIF work.phase='pins' THEN
  WITH batch AS MATERIALIZED(SELECT execution,call_key,revision FROM moria.runtime_history_read_pins WHERE environment=policy.environment AND expires_at<=clock_timestamp() ORDER BY expires_at,execution,call_key LIMIT 1000),
   deleted AS(DELETE FROM moria.runtime_history_read_pins p USING batch b WHERE p.execution=b.execution AND p.call_key=b.call_key AND p.revision=b.revision RETURNING 1)
  SELECT (SELECT count(*) FROM batch),(SELECT count(*) FROM deleted) INTO scanned,removed;
  UPDATE moria.runtime_history_collections SET phase=CASE WHEN scanned=0 THEN 'complete' ELSE phase END,status=CASE WHEN scanned=0 THEN 'complete' ELSE status END,completed_at=CASE WHEN scanned=0 THEN clock_timestamp() ELSE completed_at END,removed_rows=removed_rows+removed WHERE id=work.id;
 END IF;
 SELECT * INTO work FROM moria.runtime_history_collections WHERE id=work.id;
 RETURN jsonb_build_object('collection',work.id,'state',work.status,'phase',work.phase,'floor',work.floor::text,'removedRows',work.removed_rows::text,'droppedTables',work.dropped_tables::text);
END $$;
REVOKE ALL ON FUNCTION moria.runtime_history_operator() FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_history_approve(text,text,bigint,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_history_plan(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_history_step(uuid) FROM PUBLIC;

CREATE FUNCTION moria.runtime_history_change_pin_guard() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('moria/environment/'||NEW.environment,0));
 PERFORM moria.runtime_history_assert(NEW.environment,NEW.after_revision);
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_history_change_pin_guard BEFORE INSERT OR UPDATE ON moria.runtime_change_pins
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_history_change_pin_guard();
`;
