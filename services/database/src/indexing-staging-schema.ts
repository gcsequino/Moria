/** Closed indexing staging is collected independently from published Source history. */
export const indexingStagingSchema = String.raw`
CREATE FUNCTION moria.runtime_indexing_staging_shared(input_build uuid)
RETURNS void LANGUAGE sql AS $$
  SELECT pg_advisory_xact_lock_shared(hashtextextended('moria/indexing-staging/v1/'||input_build::text,0))
$$;

CREATE TABLE moria.runtime_indexing_stage_closures (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  build uuid NOT NULL REFERENCES moria.indexing_builds(id),
  environment text NOT NULL REFERENCES moria.environments(id),
  status text NOT NULL CHECK(status IN ('active','failed','cancelled','superseded','conflicted')),
  owner uuid, fence bigint NOT NULL,
  scope_hash text NOT NULL CHECK(scope_hash ~ '^[0-9a-f]{64}$'),
  kind text NOT NULL CHECK(kind IN ('transition','observed')),
  observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX runtime_indexing_closure_latest
ON moria.runtime_indexing_stage_closures(build,sequence DESC);
CREATE INDEX indexing_closed_builds
ON moria.indexing_builds(environment,created_at,id)
WHERE status IN ('active','failed','cancelled','superseded','conflicted');

CREATE FUNCTION moria.runtime_indexing_staging_owner()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN
    RAISE EXCEPTION 'INDEXING_STAGING_AUTHORITY_REQUIRED';
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER runtime_indexing_closure_owner
BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_indexing_stage_closures
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_staging_owner();
CREATE FUNCTION moria.runtime_indexing_closure_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'IMMUTABLE_INDEXING_CLOSURE';
END $$;
CREATE TRIGGER runtime_indexing_closure_immutable
BEFORE UPDATE OR DELETE ON moria.runtime_indexing_stage_closures
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_closure_immutable();

CREATE FUNCTION moria.runtime_indexing_staging_scope(input_build moria.indexing_builds)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT encode(sha256(convert_to(moria.runtime_indexing_build_state(input_build)::text,'UTF8')),'hex')
$$;
-- Ready age and deferred retry are separate indexed ranges; policy changes never require a full reschedule.
CREATE TABLE moria.runtime_indexing_staging_pending (
  build uuid PRIMARY KEY REFERENCES moria.indexing_builds(id),
  environment text NOT NULL REFERENCES moria.environments(id),
  closure uuid REFERENCES moria.runtime_indexing_stage_closures(id),
  observed_at timestamptz,
  state text NOT NULL DEFAULT 'ready' CHECK(state IN ('ready','deferred')),
  retry_at timestamptz,
  CHECK((closure IS NULL)=(observed_at IS NULL)),
  CHECK((state='deferred')=(retry_at IS NOT NULL))
);
CREATE INDEX runtime_indexing_staging_ready_age
ON moria.runtime_indexing_staging_pending(environment,observed_at,build)
WHERE state='ready' AND observed_at IS NOT NULL;
CREATE INDEX runtime_indexing_staging_ready_unknown
ON moria.runtime_indexing_staging_pending(environment,build)
WHERE state='ready' AND observed_at IS NULL;
CREATE INDEX runtime_indexing_staging_due
ON moria.runtime_indexing_staging_pending(environment,retry_at,build)
WHERE state='deferred';
CREATE TRIGGER runtime_indexing_staging_pending_owner
BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_indexing_staging_pending
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_staging_owner();

CREATE TABLE moria.runtime_indexing_staging_discovery (
  environment text NOT NULL REFERENCES moria.environments(id),
  incarnation uuid NOT NULL,
  after_created_at timestamptz NOT NULL DEFAULT '-infinity',
  after_build uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
  complete boolean NOT NULL DEFAULT false,
  PRIMARY KEY(environment,incarnation)
);
CREATE TRIGGER runtime_indexing_staging_discovery_owner
BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_indexing_staging_discovery
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_staging_owner();

CREATE FUNCTION moria.runtime_indexing_staging_ready(input_environment text,input_cutoff timestamptz,input_now timestamptz)
RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,moria AS $$
  SELECT EXISTS(SELECT 1 FROM moria.runtime_indexing_staging_pending q
    WHERE q.environment=input_environment AND q.state='ready' AND q.observed_at IS NULL LIMIT 1)
  OR EXISTS(SELECT 1 FROM moria.runtime_indexing_staging_pending q
    WHERE q.environment=input_environment AND q.state='ready'
      AND q.observed_at IS NOT NULL AND q.observed_at<=input_cutoff LIMIT 1)
  OR EXISTS(SELECT 1 FROM moria.runtime_indexing_staging_pending q
    WHERE q.environment=input_environment AND q.state='deferred' AND q.retry_at<=input_now LIMIT 1)
$$;

CREATE FUNCTION moria.runtime_indexing_capture_closure()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE previous moria.runtime_indexing_stage_closures;
BEGIN
  IF NEW.status NOT IN ('active','failed','cancelled','superseded','conflicted') THEN
    DELETE FROM moria.runtime_indexing_staging_pending WHERE build=NEW.id;
    RETURN NEW;
  END IF;
  SELECT * INTO previous FROM moria.runtime_indexing_stage_closures c
  WHERE c.build=NEW.id ORDER BY c.sequence DESC LIMIT 1;
  IF NOT FOUND OR previous.status<>NEW.status
    OR previous.owner IS DISTINCT FROM NEW.owner OR previous.fence<>NEW.fence
    OR previous.scope_hash<>moria.runtime_indexing_staging_scope(NEW) THEN
    INSERT INTO moria.runtime_indexing_stage_closures(build,environment,status,owner,fence,scope_hash,kind)
    VALUES(NEW.id,NEW.environment,NEW.status,NEW.owner,NEW.fence,
      moria.runtime_indexing_staging_scope(NEW),'transition') RETURNING * INTO previous;
    INSERT INTO moria.runtime_indexing_staging_pending(build,environment,closure,observed_at)
    VALUES(NEW.id,NEW.environment,previous.id,previous.observed_at)
    ON CONFLICT(build) DO UPDATE SET environment=EXCLUDED.environment,
      closure=EXCLUDED.closure,observed_at=EXCLUDED.observed_at,state='ready',retry_at=NULL;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_indexing_capture_closure
AFTER INSERT OR UPDATE OF status,owner,fence ON moria.indexing_builds
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_capture_closure();

CREATE TABLE moria.runtime_indexing_staging_work (
  closure uuid PRIMARY KEY REFERENCES moria.runtime_indexing_stage_closures(id),
  build uuid NOT NULL REFERENCES moria.indexing_builds(id),
  environment text NOT NULL REFERENCES moria.environments(id),
  origin_policy uuid NOT NULL REFERENCES moria.runtime_history_policies(id),
  mode text NOT NULL CHECK(mode IN ('publishedSpills','unpublished')),
  phase text NOT NULL DEFAULT 'spills' CHECK(phase IN ('spills','maps','rows','projection','complete')),
  position jsonb NOT NULL DEFAULT '{}'::jsonb,
  removed_spills bigint NOT NULL DEFAULT 0 CHECK(removed_spills>=0),
  removed_keys bigint NOT NULL DEFAULT 0 CHECK(removed_keys>=0),
  removed_rows bigint NOT NULL DEFAULT 0 CHECK(removed_rows>=0),
  dropped_tables bigint NOT NULL DEFAULT 0 CHECK(dropped_tables>=0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz
);
CREATE TRIGGER runtime_indexing_staging_work_owner
BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_indexing_staging_work
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_staging_owner();
CREATE FUNCTION moria.runtime_indexing_staging_work_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR ROW(NEW.closure,NEW.build,NEW.environment,NEW.origin_policy,NEW.mode,NEW.created_at)
    IS DISTINCT FROM ROW(OLD.closure,OLD.build,OLD.environment,OLD.origin_policy,OLD.mode,OLD.created_at)
    OR OLD.phase='complete' THEN
    RAISE EXCEPTION 'IMMUTABLE_INDEXING_STAGING_WORK';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_indexing_staging_work_identity
BEFORE UPDATE OR DELETE ON moria.runtime_indexing_staging_work
FOR EACH ROW EXECUTE FUNCTION moria.runtime_indexing_staging_work_identity();

-- Negative ownership/reference lookups must not scan unrelated historical generations.
CREATE INDEX runtime_indexing_staging_published_stage ON moria.source_generations(stage) WHERE stage IS NOT NULL;
CREATE INDEX runtime_indexing_staging_search_stage ON moria.native_search_rebuilds(source_stage,generation)
WHERE source_stage IS NOT NULL;

CREATE FUNCTION moria.runtime_indexing_staging_published(input_build uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS(SELECT 1 FROM moria.source_generations g WHERE g.stage=input_build)
    OR EXISTS(SELECT 1 FROM moria.native_source_generation_aliases a WHERE a.store=input_build)
    OR EXISTS(SELECT 1 FROM moria.native_source_generation_aliases a WHERE a.indexed_build=input_build)
    OR EXISTS(SELECT 1 FROM moria.native_source_row_stores s
      WHERE s.id=input_build AND s.status IN ('published','retired'))
    OR EXISTS(SELECT 1 FROM moria.runtime_projections p
      WHERE p.stage=input_build AND p.activation_revision IS NOT NULL)
$$;
CREATE FUNCTION moria.runtime_indexing_staging_pinned(input_build uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
  -- Every valid revision cursor/read pin resolves through a published generation/alias.
  -- Search additionally keeps its actual physical Source stage structurally pinned.
  SELECT EXISTS(SELECT 1 FROM moria.native_search_rebuilds r
    JOIN moria.native_search_generations g ON g.id=r.generation
    WHERE r.source_stage=input_build AND g.status IN ('building','ready','active'))
$$;

CREATE FUNCTION moria.runtime_indexing_staging_once(input_policy uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE policy moria.runtime_history_policies;
  candidate record; discovered_build record; build_row moria.indexing_builds;
  queued moria.runtime_indexing_staging_pending;
  discovery moria.runtime_indexing_staging_discovery;
  discovered bigint:=0; promoted bigint:=0; discovery_pending boolean:=false;
  scope_changed boolean:=false; step_time timestamptz; eligible_cutoff timestamptz;
  closed moria.runtime_indexing_stage_closures;
  work moria.runtime_indexing_staging_work;
  projection moria.runtime_projections;
  relation text; published boolean; busy boolean:=false;
  scanned bigint; removed bigint; stored_bytes bigint; next_position jsonb;
BEGIN
  PERFORM moria.runtime_history_operator();
  SELECT * INTO STRICT policy FROM moria.runtime_history_policies p
  WHERE p.id=input_policy FOR SHARE;
  IF NOT policy.enabled OR policy.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN
    RAISE EXCEPTION 'HISTORY_POLICY_REVOKED';
  END IF;
  step_time:=clock_timestamp();
  eligible_cutoff:=step_time-policy.retain_seconds*interval '1 second';
  INSERT INTO moria.runtime_indexing_staging_discovery(environment,incarnation)
  VALUES(policy.environment,policy.incarnation) ON CONFLICT DO NOTHING;
  SELECT * INTO discovery FROM moria.runtime_indexing_staging_discovery d
  WHERE d.environment=policy.environment AND d.incarnation=policy.incarnation;
  IF NOT discovery.complete THEN
    SELECT * INTO discovery FROM moria.runtime_indexing_staging_discovery d
    WHERE d.environment=policy.environment AND d.incarnation=policy.incarnation FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      RETURN jsonb_build_object('state','busy','worked',false,'pending',true);
    END IF;
    FOR discovered_build IN
      SELECT b.id,b.created_at FROM moria.indexing_builds b
      WHERE b.environment=policy.environment
        AND b.status IN ('active','failed','cancelled','superseded','conflicted')
        AND (b.created_at,b.id)>(discovery.after_created_at,discovery.after_build)
      ORDER BY b.created_at,b.id LIMIT 8
    LOOP
      discovered:=discovered+1;
      SELECT * INTO closed FROM moria.runtime_indexing_stage_closures c
      WHERE c.build=discovered_build.id ORDER BY c.sequence DESC LIMIT 1;
      IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM moria.runtime_indexing_staging_work w
        WHERE w.closure=closed.id AND w.phase='complete') THEN
        INSERT INTO moria.runtime_indexing_staging_pending(build,environment,closure,observed_at)
        VALUES(discovered_build.id,policy.environment,closed.id,closed.observed_at)
        ON CONFLICT DO NOTHING;
      END IF;
      discovery.after_created_at:=discovered_build.created_at;
      discovery.after_build:=discovered_build.id;
    END LOOP;
    UPDATE moria.runtime_indexing_staging_discovery SET
      after_created_at=discovery.after_created_at,after_build=discovery.after_build,complete=discovered<8
    WHERE environment=policy.environment AND incarnation=policy.incarnation;
    discovery_pending:=discovered=8;
  END IF;
  WITH due AS MATERIALIZED(
    SELECT q.build FROM moria.runtime_indexing_staging_pending q
    WHERE q.environment=policy.environment AND q.state='deferred' AND q.retry_at<=step_time
    ORDER BY q.retry_at,q.build LIMIT 8 FOR UPDATE SKIP LOCKED)
  UPDATE moria.runtime_indexing_staging_pending q SET state='ready',retry_at=NULL
  FROM due WHERE q.build=due.build;
  GET DIAGNOSTICS promoted=ROW_COUNT;
  FOR candidate IN
    WITH unknown AS MATERIALIZED(
      SELECT q.build,q.observed_at FROM moria.runtime_indexing_staging_pending q
      WHERE q.environment=policy.environment AND q.state='ready' AND q.observed_at IS NULL
      ORDER BY q.build LIMIT 8),
    aged AS MATERIALIZED(
      SELECT q.build,q.observed_at FROM moria.runtime_indexing_staging_pending q
      WHERE q.environment=policy.environment AND q.state='ready' AND q.observed_at IS NOT NULL
        AND q.observed_at<=eligible_cutoff
      ORDER BY q.observed_at,q.build LIMIT 8)
    SELECT build AS id FROM(SELECT * FROM unknown UNION ALL SELECT * FROM aged) ready
    ORDER BY observed_at NULLS FIRST,build LIMIT 8
  LOOP
    SELECT * INTO queued FROM moria.runtime_indexing_staging_pending q
    WHERE q.build=candidate.id FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      busy:=true;
      CONTINUE;
    END IF;
    IF queued.environment<>policy.environment OR queued.state<>'ready'
      OR queued.observed_at>clock_timestamp()-policy.retain_seconds*interval '1 second' THEN
      CONTINUE;
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/indexing-staging/v1/'||candidate.id::text,0)) THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
      WHERE build=candidate.id;
      busy:=true;
      CONTINUE;
    END IF;
    SELECT * INTO build_row FROM moria.indexing_builds b
    WHERE b.id=candidate.id FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
      WHERE build=candidate.id;
      busy:=true;
      CONTINUE;
    END IF;
    IF build_row.environment<>policy.environment
      OR build_row.status NOT IN ('active','failed','cancelled','superseded','conflicted') THEN
      DELETE FROM moria.runtime_indexing_staging_pending WHERE build=candidate.id;
      CONTINUE;
    END IF;
    SELECT * INTO closed FROM moria.runtime_indexing_stage_closures c
    WHERE c.build=build_row.id ORDER BY c.sequence DESC LIMIT 1;
    IF NOT FOUND THEN
      -- Older migrations/restores have no authentic close time: observe now, never backdate.
      INSERT INTO moria.runtime_indexing_stage_closures(build,environment,status,owner,fence,scope_hash,kind)
      VALUES(build_row.id,build_row.environment,build_row.status,build_row.owner,build_row.fence,
        moria.runtime_indexing_staging_scope(build_row),'observed') RETURNING * INTO closed;
      UPDATE moria.runtime_indexing_staging_pending SET closure=closed.id,observed_at=closed.observed_at,
        state='ready',retry_at=NULL WHERE build=build_row.id;
      RETURN jsonb_build_object('state','observed','worked',true,'pending',discovery_pending
        OR moria.runtime_indexing_staging_ready(policy.environment,eligible_cutoff,step_time),
        'build',build_row.id,'closure',closed.id,
        'eligibleAt',closed.observed_at+policy.retain_seconds*interval '1 second');
    END IF;
    IF closed.status<>build_row.status OR closed.owner IS DISTINCT FROM build_row.owner
      OR closed.fence<>build_row.fence OR closed.scope_hash<>moria.runtime_indexing_staging_scope(build_row) THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 minute'
      WHERE build=build_row.id;
      scope_changed:=true;
      CONTINUE;
    END IF;
    IF queued.closure IS DISTINCT FROM closed.id OR queued.observed_at IS DISTINCT FROM closed.observed_at THEN
      UPDATE moria.runtime_indexing_staging_pending SET closure=closed.id,observed_at=closed.observed_at
      WHERE build=build_row.id;
    END IF;
    IF closed.observed_at>clock_timestamp()-policy.retain_seconds*interval '1 second' THEN
      CONTINUE;
    END IF;
    published:=moria.runtime_indexing_staging_published(build_row.id);
    IF NOT published AND (build_row.deadline>clock_timestamp()
      OR moria.runtime_indexing_staging_pinned(build_row.id)) THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',
        retry_at=GREATEST(clock_timestamp()+interval '1 second',build_row.deadline)
      WHERE build=build_row.id;
      CONTINUE;
    END IF;
    IF EXISTS(SELECT 1 FROM moria.source_stages s WHERE s.id=build_row.id
      AND ROW(s.environment,s.object_type) IS DISTINCT FROM ROW(build_row.environment,build_row.target))
      OR EXISTS(SELECT 1 FROM moria.native_source_row_stores s WHERE s.id=build_row.id
      AND ROW(s.environment,s.object_type) IS DISTINCT FROM ROW(build_row.environment,build_row.target)) THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 minute'
      WHERE build=build_row.id;
      scope_changed:=true;
      CONTINUE;
    END IF;
    INSERT INTO moria.runtime_indexing_staging_work(closure,build,environment,origin_policy,mode)
    VALUES(closed.id,build_row.id,build_row.environment,input_policy,
      CASE WHEN published THEN 'publishedSpills' ELSE 'unpublished' END)
    ON CONFLICT DO NOTHING;
    SELECT * INTO work FROM moria.runtime_indexing_staging_work w
    WHERE w.closure=closed.id FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
      WHERE build=build_row.id;
      busy:=true;
      CONTINUE;
    END IF;
    IF work.phase='complete' THEN
      DELETE FROM moria.runtime_indexing_staging_pending WHERE build=build_row.id;
      CONTINUE;
    END IF;
    IF work.mode='unpublished' AND (published OR moria.runtime_indexing_staging_pinned(build_row.id)) THEN
      UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
      WHERE build=build_row.id;
      CONTINUE;
    END IF;
    IF work.phase='spills' THEN
      WITH candidates AS MATERIALIZED(
        SELECT r.partition,r.uid,r.task,r.fence,
          pg_column_size(r.value)::bigint+COALESCE(pg_column_size(r.source_value),0) AS bytes
        FROM moria.indexing_rows r WHERE r.build=build_row.id
          AND (r.partition,r.uid,r.task,r.fence)>=
            (COALESCE((work.position->>'partition')::integer,-1),
             COALESCE((work.position->>'uid')::uuid,'00000000-0000-0000-0000-000000000000'::uuid),
             COALESCE((work.position->>'task')::uuid,'00000000-0000-0000-0000-000000000000'::uuid),
             COALESCE((work.position->>'fence')::bigint,'-9223372036854775808'::bigint))
          AND (work.position='{}'::jsonb OR (r.partition,r.uid,r.task,r.fence)>
            ((work.position->>'partition')::integer,(work.position->>'uid')::uuid,
             (work.position->>'task')::uuid,(work.position->>'fence')::bigint))
        ORDER BY r.partition,r.uid,r.task,r.fence LIMIT 1000),
      sized AS MATERIALIZED(SELECT *,sum(bytes) OVER(ORDER BY partition,uid,task,fence) AS total FROM candidates),
      chosen AS MATERIALIZED(SELECT * FROM sized WHERE total<=16777216),
      deleted AS(DELETE FROM moria.indexing_rows r USING chosen k
        WHERE r.build=build_row.id AND r.partition=k.partition AND r.uid=k.uid
          AND r.task=k.task AND r.fence=k.fence RETURNING 1)
      SELECT (SELECT count(*) FROM candidates),(SELECT count(*) FROM deleted),
        COALESCE((SELECT sum(bytes) FROM chosen),0),
        (SELECT jsonb_build_object('partition',partition,'uid',uid,'task',task,'fence',fence::text)
          FROM chosen ORDER BY partition DESC,uid DESC,task DESC,fence DESC LIMIT 1)
      INTO scanned,removed,stored_bytes,next_position;
      IF scanned>0 AND removed=0 THEN
        RAISE EXCEPTION 'RESOURCE_LIMIT';
      END IF;
      IF scanned=0 AND EXISTS(SELECT 1 FROM moria.indexing_rows r WHERE r.build=build_row.id) THEN
        next_position:='{}'::jsonb;
      ELSIF scanned=0 THEN
        UPDATE moria.runtime_indexing_staging_work SET
          phase=CASE WHEN mode='publishedSpills' THEN 'complete' ELSE 'maps' END,
          position='{}'::jsonb,
          finished_at=CASE WHEN mode='publishedSpills' THEN clock_timestamp() END
        WHERE closure=closed.id;
      END IF;
      IF scanned>0 OR next_position='{}'::jsonb THEN
        UPDATE moria.runtime_indexing_staging_work SET position=COALESCE(next_position,'{}'::jsonb),
          removed_spills=removed_spills+removed WHERE closure=closed.id;
      END IF;
    ELSIF work.phase='maps' THEN
      WITH candidates AS MATERIALIZED(
        SELECT m.uid,pg_column_size(m.encoded_key)::bigint AS bytes
        FROM moria.native_source_key_map m WHERE m.store=build_row.id
          AND m.uid>=COALESCE((work.position->>'uid')::uuid,'00000000-0000-0000-0000-000000000000'::uuid)
          AND (work.position='{}'::jsonb OR m.uid>(work.position->>'uid')::uuid)
        ORDER BY m.uid LIMIT 1000),
      sized AS MATERIALIZED(SELECT *,sum(bytes) OVER(ORDER BY uid) AS total FROM candidates),
      chosen AS MATERIALIZED(SELECT * FROM sized WHERE total<=16777216),
      deleted AS(DELETE FROM moria.native_source_key_map m USING chosen k
        WHERE m.store=build_row.id AND m.uid=k.uid RETURNING 1)
      SELECT (SELECT count(*) FROM candidates),(SELECT count(*) FROM deleted),
        COALESCE((SELECT sum(bytes) FROM chosen),0),
        (SELECT jsonb_build_object('uid',uid) FROM chosen ORDER BY uid DESC LIMIT 1)
      INTO scanned,removed,stored_bytes,next_position;
      IF scanned>0 AND removed=0 THEN
        RAISE EXCEPTION 'RESOURCE_LIMIT';
      END IF;
      IF scanned=0 AND EXISTS(SELECT 1 FROM moria.native_source_key_map m WHERE m.store=build_row.id) THEN
        next_position:='{}'::jsonb;
      ELSIF scanned=0 THEN
        UPDATE moria.runtime_indexing_staging_work SET phase='rows',position='{}'::jsonb WHERE closure=closed.id;
      END IF;
      IF scanned>0 OR next_position='{}'::jsonb THEN
        UPDATE moria.runtime_indexing_staging_work SET position=COALESCE(next_position,'{}'::jsonb),
          removed_keys=removed_keys+removed WHERE closure=closed.id;
      END IF;
    ELSIF work.phase='rows' THEN
      WITH candidates AS MATERIALIZED(
        SELECT r.uid,pg_column_size(r.value)::bigint+COALESCE(pg_column_size(r.source_value),0) AS bytes
        FROM moria.staged_source_rows r WHERE r.stage=build_row.id
          AND r.uid>=COALESCE((work.position->>'uid')::uuid,'00000000-0000-0000-0000-000000000000'::uuid)
          AND (work.position='{}'::jsonb OR r.uid>(work.position->>'uid')::uuid)
        ORDER BY r.uid LIMIT 1000),
      sized AS MATERIALIZED(SELECT *,sum(bytes) OVER(ORDER BY uid) AS total FROM candidates),
      chosen AS MATERIALIZED(SELECT * FROM sized WHERE total<=16777216),
      deleted AS(DELETE FROM moria.staged_source_rows r USING chosen k
        WHERE r.stage=build_row.id AND r.uid=k.uid RETURNING 1)
      SELECT (SELECT count(*) FROM candidates),(SELECT count(*) FROM deleted),
        COALESCE((SELECT sum(bytes) FROM chosen),0),
        (SELECT jsonb_build_object('uid',uid) FROM chosen ORDER BY uid DESC LIMIT 1)
      INTO scanned,removed,stored_bytes,next_position;
      IF scanned>0 AND removed=0 THEN
        RAISE EXCEPTION 'RESOURCE_LIMIT';
      END IF;
      IF scanned=0 AND EXISTS(SELECT 1 FROM moria.staged_source_rows r WHERE r.stage=build_row.id) THEN
        next_position:='{}'::jsonb;
      ELSIF scanned=0 THEN
        UPDATE moria.runtime_indexing_staging_work SET phase='projection',position='{}'::jsonb WHERE closure=closed.id;
      END IF;
      IF scanned>0 OR next_position='{}'::jsonb THEN
        UPDATE moria.runtime_indexing_staging_work SET position=COALESCE(next_position,'{}'::jsonb),
          removed_rows=removed_rows+removed WHERE closure=closed.id;
      END IF;
    ELSIF work.phase='projection' THEN
      SELECT * INTO projection FROM moria.runtime_projections p
      WHERE p.stage=build_row.id FOR UPDATE SKIP LOCKED;
      IF NOT FOUND AND EXISTS(SELECT 1 FROM moria.runtime_projections p WHERE p.stage=build_row.id) THEN
        UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
        WHERE build=build_row.id;
        RETURN jsonb_build_object('state','busy','worked',false,'pending',true,'build',build_row.id);
      END IF;
      IF FOUND THEN
        IF ROW(projection.environment,projection.resource) IS DISTINCT FROM ROW(build_row.environment,build_row.target)
          OR projection.activation_revision IS NOT NULL OR projection.status='active' THEN
          UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 minute'
          WHERE build=build_row.id;
          scope_changed:=true;
          CONTINUE;
        END IF;
        relation:=moria.runtime_projection_relation(projection.id);
        IF to_regclass(relation) IS NOT NULL THEN
          IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/recovery-retention/v1',0)) THEN
            UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
            WHERE build=build_row.id;
            RETURN jsonb_build_object('state','backupBusy','worked',false,'pending',true,'build',build_row.id);
          END IF;
          BEGIN
            EXECUTE 'LOCK TABLE '||relation||' IN ACCESS EXCLUSIVE MODE NOWAIT';
          EXCEPTION WHEN lock_not_available THEN
            UPDATE moria.runtime_indexing_staging_pending SET state='deferred',retry_at=clock_timestamp()+interval '1 second'
            WHERE build=build_row.id;
            RETURN jsonb_build_object('state','busy','worked',false,'pending',true,'build',build_row.id);
          END;
          EXECUTE 'DROP TABLE '||relation;
          UPDATE moria.runtime_indexing_staging_work SET dropped_tables=dropped_tables+1 WHERE closure=closed.id;
        END IF;
        UPDATE moria.runtime_projections SET status='retired' WHERE id=projection.id;
      END IF;
      IF EXISTS(SELECT 1 FROM moria.indexing_rows r WHERE r.build=build_row.id)
        OR EXISTS(SELECT 1 FROM moria.native_source_key_map m WHERE m.store=build_row.id)
        OR EXISTS(SELECT 1 FROM moria.staged_source_rows r WHERE r.stage=build_row.id) THEN
        RAISE EXCEPTION 'INDEXING_STAGING_INCOMPLETE';
      END IF;
      UPDATE moria.source_stages SET status='failed' WHERE id=build_row.id AND status<>'published';
      UPDATE moria.native_source_row_stores SET status='retired' WHERE id=build_row.id AND status IN ('loading','sealed');
      UPDATE moria.runtime_indexing_staging_work SET phase='complete',finished_at=clock_timestamp()
      WHERE closure=closed.id;
      scanned:=0;
      removed:=0;
      stored_bytes:=0;
    ELSE
      CONTINUE;
    END IF;
    SELECT * INTO work FROM moria.runtime_indexing_staging_work w WHERE w.closure=closed.id;
    IF work.phase='complete' THEN
      DELETE FROM moria.runtime_indexing_staging_pending WHERE build=build_row.id;
    END IF;
    RETURN jsonb_build_object('state','progress','worked',true,'pending',discovery_pending OR busy
      OR moria.runtime_indexing_staging_ready(policy.environment,eligible_cutoff,step_time),
      'build',build_row.id,'closure',closed.id,'mode',work.mode,'phase',work.phase,
      'discovered',discovered,'promoted',promoted,
      'removedRows',COALESCE(removed,0)::text,'storageBytes',COALESCE(stored_bytes,0)::text,
      'removedSpills',work.removed_spills::text,'removedKeys',work.removed_keys::text,
      'removedSourceRows',work.removed_rows::text,'droppedTables',work.dropped_tables::text);
  END LOOP;
  RETURN jsonb_build_object('state',CASE WHEN busy THEN 'busy' WHEN discovery_pending THEN 'discovering'
      WHEN scope_changed THEN 'scopeChanged' ELSE 'idle' END,
    'worked',discovered>0 OR promoted>0,'pending',busy OR discovery_pending
      OR moria.runtime_indexing_staging_ready(policy.environment,eligible_cutoff,step_time),
    'discovered',discovered,'promoted',promoted,'nextRetryAt',(SELECT q.retry_at
      FROM moria.runtime_indexing_staging_pending q WHERE q.environment=policy.environment AND q.state='deferred'
      ORDER BY q.retry_at,q.build LIMIT 1));
END $$;
`;
