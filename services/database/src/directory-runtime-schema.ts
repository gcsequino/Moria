/** Managed environments admit only their live current cell; directory state is never cached authority. */
export const directoryRuntimeSchema = String.raw`
ALTER TABLE moria.runtime_placements ADD COLUMN leases_fenced boolean NOT NULL DEFAULT false;
ALTER TABLE moria.runtime_placements ADD COLUMN transfer_from_cell uuid;
ALTER TABLE moria.runtime_placements ADD COLUMN transfer_from_fence bigint;
CREATE FUNCTION moria.runtime_environment_cell_check(env text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE p record;
BEGIN
  PERFORM moria.runtime_recovery_check();
  SELECT placement.*,c.enabled,c.incarnation AS cell_incarnation INTO p
    FROM moria.runtime_placements placement JOIN moria.runtime_cells c ON c.id=placement.cell
    WHERE placement.environment=env FOR SHARE OF placement,c;
  IF NOT FOUND THEN RETURN; END IF;
  IF NOT p.serving OR NOT p.leases_fenced OR NOT p.enabled
    OR p.incarnation<>(SELECT incarnation FROM moria.indexing_instance) OR p.cell_incarnation<>p.incarnation
    OR p.cell::text IS DISTINCT FROM NULLIF(current_setting('moria.cell_id',true),'') THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
END $$;
CREATE FUNCTION moria.runtime_environment_cell_available(env text) RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT (SELECT dispatch_enabled FROM moria.runtime_recovery_control WHERE singleton) AND (NOT EXISTS(SELECT 1 FROM moria.runtime_placements WHERE environment=env)
    OR EXISTS(SELECT 1 FROM moria.runtime_placements p JOIN moria.runtime_cells c ON c.id=p.cell
      JOIN moria.indexing_instance i ON i.incarnation=p.incarnation AND i.incarnation=c.incarnation
      WHERE p.environment=env AND p.serving AND p.leases_fenced AND c.enabled
        AND p.cell::text=NULLIF(current_setting('moria.cell_id',true),'')))
$$;
CREATE FUNCTION moria.runtime_environment_managed_check(env text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_placements WHERE environment=env) THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  PERFORM moria.runtime_environment_cell_check(env);
END $$;
CREATE FUNCTION moria.runtime_environment_freeze(env text,expected_cell uuid,expected_fence bigint,instance uuid,next_cell uuid)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE p moria.runtime_placements;
BEGIN
  SELECT * INTO p FROM moria.runtime_placements WHERE environment=env FOR UPDATE;
  IF NOT FOUND OR instance<>(SELECT incarnation FROM moria.indexing_instance) OR p.incarnation<>instance
    OR NOT EXISTS(SELECT 1 FROM moria.runtime_cells WHERE id=next_cell AND enabled AND incarnation=instance)
    THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  IF NOT p.serving AND p.cell=next_cell AND p.fence=expected_fence+1
    AND p.transfer_from_cell=expected_cell AND p.transfer_from_fence=expected_fence THEN RETURN; END IF;
  IF NOT p.serving OR p.cell<>expected_cell OR p.fence<>expected_fence OR expected_cell=next_cell
    THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  UPDATE moria.runtime_placements SET serving=false,leases_fenced=false,cell=next_cell,fence=fence+1,
    transfer_from_cell=expected_cell,transfer_from_fence=expected_fence WHERE environment=env;
  INSERT INTO moria.runtime_ownership_events(kind,resource,previous_cell,next_cell,fence,incarnation)
    VALUES('environment',env,expected_cell,next_cell,expected_fence+1,instance);
END $$;
CREATE FUNCTION moria.runtime_environment_fence_leases(env text,expected_cell uuid,expected_fence bigint,instance uuid)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE p moria.runtime_placements;
BEGIN
  -- Freeze has committed first; avoid holding a placement lock while waiting on attempt rows.
  SELECT * INTO p FROM moria.runtime_placements WHERE environment=env;
  IF NOT FOUND OR p.serving OR ROW(p.cell,p.fence,p.incarnation) IS DISTINCT FROM ROW(expected_cell,expected_fence,instance)
    OR instance<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  IF p.leases_fenced THEN RETURN; END IF;
  UPDATE moria.runtime_execution_tasks t SET lease_until=clock_timestamp()
    FROM moria.runtime_executions e WHERE t.execution=e.id AND e.environment=env AND t.status='running';
  UPDATE moria.runtime_executions SET lease_until=clock_timestamp() WHERE environment=env AND status='preparing';
  UPDATE moria.indexing_tasks t SET lease_until=clock_timestamp() FROM moria.indexing_builds b
    WHERE t.build=b.id AND b.environment=env AND t.status='running';
  UPDATE moria.indexing_builds SET lease_until=clock_timestamp() WHERE environment=env AND status IN ('planning','running','preparing','ready');
  UPDATE moria.runtime_deliveries SET lease_until=clock_timestamp() WHERE environment=env AND status='delivering';
  UPDATE moria.native_model_calls SET state='failed',uncertain=true,error='CELL_OWNERSHIP_LOST',finished_at=clock_timestamp()
    WHERE environment=env AND state='running';
  UPDATE moria.native_model_attempts a SET status='uncertain',finished_at=clock_timestamp() FROM moria.native_model_calls c
    WHERE a.call_id=c.id AND c.environment=env AND c.error='CELL_OWNERSHIP_LOST' AND a.status='started';
  UPDATE moria.runtime_placements SET leases_fenced=true WHERE environment=env AND NOT serving
    AND ROW(cell,fence,incarnation)=ROW(expected_cell,expected_fence,instance);
  IF NOT FOUND THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
END $$;
CREATE FUNCTION moria.runtime_environment_activate(env text,expected_cell uuid,expected_fence bigint,instance uuid)
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  UPDATE moria.runtime_placements p SET serving=true WHERE p.environment=env AND p.leases_fenced
    AND ROW(p.cell,p.fence,p.incarnation)=ROW(expected_cell,expected_fence,instance)
    AND instance=(SELECT incarnation FROM moria.indexing_instance)
    AND EXISTS(SELECT 1 FROM moria.runtime_cells c WHERE c.id=p.cell AND c.enabled AND c.incarnation=instance);
  IF NOT FOUND THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
END $$;
`;
