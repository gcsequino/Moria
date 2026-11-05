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
CREATE FUNCTION moria.runtime_indexing_capture_closure()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE previous moria.runtime_indexing_stage_closures;
BEGIN
  IF NEW.status NOT IN ('active','failed','cancelled','superseded','conflicted') THEN
    RETURN NEW;
  END IF;
  SELECT * INTO previous FROM moria.runtime_indexing_stage_closures c
  WHERE c.build=NEW.id ORDER BY c.sequence DESC LIMIT 1;
  IF NOT FOUND OR previous.status<>NEW.status
    OR previous.owner IS DISTINCT FROM NEW.owner OR previous.fence<>NEW.fence
    OR previous.scope_hash<>moria.runtime_indexing_staging_scope(NEW) THEN
    INSERT INTO moria.runtime_indexing_stage_closures(build,environment,status,owner,fence,scope_hash,kind)
    VALUES(NEW.id,NEW.environment,NEW.status,NEW.owner,NEW.fence,
      moria.runtime_indexing_staging_scope(NEW),'transition');
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

CREATE FUNCTION moria.runtime_indexing_staging_published(input_build uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS(SELECT 1 FROM moria.source_generations g WHERE g.stage=input_build)
    OR EXISTS(SELECT 1 FROM moria.native_source_generation_aliases a WHERE a.store=input_build)
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
  candidate record; build_row moria.indexing_builds;
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
  FOR candidate IN
    SELECT b.id FROM moria.indexing_builds b
    LEFT JOIN LATERAL(SELECT c.* FROM moria.runtime_indexing_stage_closures c
      WHERE c.build=b.id ORDER BY c.sequence DESC LIMIT 1)c ON true
    LEFT JOIN moria.runtime_indexing_staging_work w ON w.closure=c.id
    WHERE b.environment=policy.environment
      AND b.status IN ('active','failed','cancelled','superseded','conflicted')
      AND (c.id IS NULL OR c.observed_at<=clock_timestamp()-policy.retain_seconds*interval '1 second')
      AND (w.closure IS NULL OR w.phase<>'complete')
    ORDER BY b.created_at,b.id LIMIT 8
  LOOP
    IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/indexing-staging/v1/'||candidate.id::text,0)) THEN
      busy:=true;
      CONTINUE;
    END IF;
    SELECT * INTO build_row FROM moria.indexing_builds b
    WHERE b.id=candidate.id FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      busy:=true;
      CONTINUE;
    END IF;
    IF build_row.environment<>policy.environment
      OR build_row.status NOT IN ('active','failed','cancelled','superseded','conflicted') THEN
      CONTINUE;
    END IF;
    SELECT * INTO closed FROM moria.runtime_indexing_stage_closures c
    WHERE c.build=build_row.id ORDER BY c.sequence DESC LIMIT 1;
    IF NOT FOUND THEN
      -- Older migrations/restores have no authentic close time: observe now, never backdate.
      INSERT INTO moria.runtime_indexing_stage_closures(build,environment,status,owner,fence,scope_hash,kind)
      VALUES(build_row.id,build_row.environment,build_row.status,build_row.owner,build_row.fence,
        moria.runtime_indexing_staging_scope(build_row),'observed') RETURNING * INTO closed;
      RETURN jsonb_build_object('state','observed','worked',true,'pending',false,
        'build',build_row.id,'closure',closed.id,
        'eligibleAt',closed.observed_at+policy.retain_seconds*interval '1 second');
    END IF;
    IF closed.status<>build_row.status OR closed.owner IS DISTINCT FROM build_row.owner
      OR closed.fence<>build_row.fence OR closed.scope_hash<>moria.runtime_indexing_staging_scope(build_row) THEN
      RETURN jsonb_build_object('state','scopeChanged','worked',false,'pending',false,'build',build_row.id);
    END IF;
    IF closed.observed_at>clock_timestamp()-policy.retain_seconds*interval '1 second' THEN
      CONTINUE;
    END IF;
    published:=moria.runtime_indexing_staging_published(build_row.id);
    IF NOT published AND (build_row.deadline>clock_timestamp()
      OR moria.runtime_indexing_staging_pinned(build_row.id)) THEN
      CONTINUE;
    END IF;
    IF EXISTS(SELECT 1 FROM moria.source_stages s WHERE s.id=build_row.id
      AND ROW(s.environment,s.object_type) IS DISTINCT FROM ROW(build_row.environment,build_row.target))
      OR EXISTS(SELECT 1 FROM moria.native_source_row_stores s WHERE s.id=build_row.id
      AND ROW(s.environment,s.object_type) IS DISTINCT FROM ROW(build_row.environment,build_row.target)) THEN
      RETURN jsonb_build_object('state','scopeChanged','worked',false,'pending',false,'build',build_row.id);
    END IF;
    INSERT INTO moria.runtime_indexing_staging_work(closure,build,environment,origin_policy,mode)
    VALUES(closed.id,build_row.id,build_row.environment,input_policy,
      CASE WHEN published THEN 'publishedSpills' ELSE 'unpublished' END)
    ON CONFLICT DO NOTHING;
    SELECT * INTO work FROM moria.runtime_indexing_staging_work w
    WHERE w.closure=closed.id FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
      busy:=true;
      CONTINUE;
    END IF;
    IF work.mode='unpublished' AND (published OR moria.runtime_indexing_staging_pinned(build_row.id)) THEN
      RETURN jsonb_build_object('state','pinned','worked',false,'pending',false,'build',build_row.id);
    END IF;
    IF work.phase='spills' THEN
      WITH candidates AS MATERIALIZED(
        SELECT r.partition,r.uid,r.task,r.fence,
          pg_column_size(r.value)::bigint+COALESCE(pg_column_size(r.source_value),0) AS bytes
        FROM moria.indexing_rows r WHERE r.build=build_row.id
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
        RETURN jsonb_build_object('state','busy','worked',false,'pending',true,'build',build_row.id);
      END IF;
      IF FOUND THEN
        IF ROW(projection.environment,projection.resource) IS DISTINCT FROM ROW(build_row.environment,build_row.target)
          OR projection.activation_revision IS NOT NULL OR projection.status='active' THEN
          RETURN jsonb_build_object('state','pinned','worked',false,'pending',false,'build',build_row.id);
        END IF;
        relation:=moria.runtime_projection_relation(projection.id);
        IF to_regclass(relation) IS NOT NULL THEN
          IF NOT pg_try_advisory_xact_lock(hashtextextended('moria/recovery-retention/v1',0)) THEN
            RETURN jsonb_build_object('state','backupBusy','worked',false,'pending',true,'build',build_row.id);
          END IF;
          BEGIN
            EXECUTE 'LOCK TABLE '||relation||' IN ACCESS EXCLUSIVE MODE NOWAIT';
          EXCEPTION WHEN lock_not_available THEN
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
    RETURN jsonb_build_object('state','progress','worked',true,'pending',work.phase<>'complete' OR EXISTS(
      SELECT 1 FROM moria.indexing_builds other
      JOIN LATERAL(SELECT c.* FROM moria.runtime_indexing_stage_closures c
        WHERE c.build=other.id ORDER BY c.sequence DESC LIMIT 1)c ON true
      LEFT JOIN moria.runtime_indexing_staging_work w ON w.closure=c.id
      WHERE other.environment=policy.environment
        AND other.status IN ('active','failed','cancelled','superseded','conflicted')
        AND other.status=c.status AND other.owner IS NOT DISTINCT FROM c.owner AND other.fence=c.fence
        AND c.observed_at<=clock_timestamp()-policy.retain_seconds*interval '1 second'
        AND (w.closure IS NULL OR w.phase<>'complete')
        AND (other.deadline<=clock_timestamp() OR moria.runtime_indexing_staging_published(other.id))),
      'build',build_row.id,'closure',closed.id,'mode',work.mode,'phase',work.phase,
      'removedRows',COALESCE(removed,0)::text,'storageBytes',COALESCE(stored_bytes,0)::text,
      'removedSpills',work.removed_spills::text,'removedKeys',work.removed_keys::text,
      'removedSourceRows',work.removed_rows::text,'droppedTables',work.dropped_tables::text);
  END LOOP;
  RETURN jsonb_build_object('state',CASE WHEN busy THEN 'busy' ELSE 'idle' END,
    'worked',false,'pending',busy);
END $$;
`;
