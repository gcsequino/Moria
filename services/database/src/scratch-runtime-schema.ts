/** Dedicated hard-bounded host filesystems stay occupied until proven terminal cleanup. */
export const scratchRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_engine_scratch_slots (
  id uuid PRIMARY KEY,host_id uuid NOT NULL,mount_root text NOT NULL UNIQUE,
  filesystem_uuid uuid NOT NULL UNIQUE,capacity_bytes bigint NOT NULL CHECK(capacity_bytes BETWEEN 67108864 AND 1099511627776),
  resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
  incarnation uuid NOT NULL,enabled boolean NOT NULL DEFAULT true,
  CHECK(mount_root LIKE '/%' AND octet_length(mount_root)<=1024)
);
CREATE TABLE moria.runtime_engine_scratch_reservations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),slot uuid NOT NULL REFERENCES moria.runtime_engine_scratch_slots(id),
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id),task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),
  owner uuid NOT NULL REFERENCES moria.runtime_workers(owner),fence bigint NOT NULL CHECK(fence>0),
  host_id uuid NOT NULL,incarnation uuid NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  container_id text,creation_started boolean NOT NULL DEFAULT false,creation_acknowledged boolean NOT NULL DEFAULT false,cleanup_nonce uuid,cleaned_at timestamptz,
  UNIQUE(task,owner,fence,incarnation),CHECK(container_id IS NULL OR container_id ~ '^[0-9a-f]{64}$')
);
CREATE UNIQUE INDEX runtime_engine_scratch_occupied ON moria.runtime_engine_scratch_reservations(slot) WHERE cleaned_at IS NULL;
CREATE FUNCTION moria.runtime_engine_scratch_slot_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR (to_jsonb(OLD)-'enabled') IS DISTINCT FROM (to_jsonb(NEW)-'enabled') THEN RAISE EXCEPTION 'IMMUTABLE_SCRATCH_SLOT'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_engine_scratch_slot_identity BEFORE UPDATE OR DELETE ON moria.runtime_engine_scratch_slots
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_engine_scratch_slot_immutable();
CREATE FUNCTION moria.runtime_engine_scratch_reserve(exec uuid,task_id uuid,worker uuid,worker_fence bigint,host uuid,instance uuid,class_name text)
  RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE e moria.runtime_executions; selected uuid; reservation uuid;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=exec FOR UPDATE;
  IF NOT FOUND OR e.resource_class<>class_name OR e.spec->>'domain' NOT IN ('transform','sync')
    OR instance<>(SELECT incarnation FROM moria.indexing_instance)
    OR NOT EXISTS(SELECT 1 FROM moria.runtime_execution_tasks t WHERE t.id=task_id AND t.execution=exec
      AND t.status='running' AND t.owner=worker AND t.fence=worker_fence AND t.lease_until>clock_timestamp())
    THEN RAISE EXCEPTION 'JOB_LEASE_LOST'; END IF;
  PERFORM moria.runtime_worker_check(worker,host,class_name,e.spec->>'domain');
  SELECT id INTO reservation FROM moria.runtime_engine_scratch_reservations
    WHERE task=task_id AND owner=worker AND fence=worker_fence AND incarnation=instance AND cleaned_at IS NULL;
  IF FOUND THEN RETURN reservation; END IF;
  SELECT s.id INTO selected FROM moria.runtime_engine_scratch_slots s WHERE s.host_id=host AND s.incarnation=instance
    AND s.resource_class=class_name AND s.enabled AND NOT EXISTS(SELECT 1 FROM moria.runtime_engine_scratch_reservations r
      WHERE r.slot=s.id AND r.cleaned_at IS NULL) ORDER BY s.id FOR UPDATE OF s SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN NULL; END IF;
  INSERT INTO moria.runtime_engine_scratch_reservations(slot,execution,task,owner,fence,host_id,incarnation)
    VALUES(selected,exec,task_id,worker,worker_fence,host,instance) RETURNING id INTO reservation;
  RETURN reservation;
END $$;
`;
