/** Physical Dataset roots retained by accepted execution specifications. */
export const executionRootPinsSchema = String.raw`
CREATE FUNCTION moria.runtime_dataset_execution_pin_live(owner_id text) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE execution_id uuid;
BEGIN
  BEGIN
    execution_id:=owner_id::uuid;
  EXCEPTION WHEN invalid_text_representation THEN
    RETURN false;
  END;
  RETURN EXISTS(
    SELECT 1 FROM moria.runtime_executions e WHERE e.id=execution_id
      AND (e.status IN ('queued','running','preparing') OR e.finished_at IS NULL
        OR e.finished_at>=statement_timestamp()-interval '30 days')
  );
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_execution_pin_live(text) FROM PUBLIC;

CREATE FUNCTION moria.runtime_execution_dataset_pin_admit() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE input jsonb; expected record; actual_snapshot text;
BEGIN
  IF jsonb_typeof(NEW.spec->'pinnedInputs') IS DISTINCT FROM 'array'
    OR jsonb_typeof(NEW.spec->'expectedHeads') IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'INPUT_PIN_CHANGED';
  END IF;
  IF jsonb_array_length(NEW.spec->'pinnedInputs')>64
    OR (SELECT count(*) FROM jsonb_object_keys(NEW.spec->'expectedHeads'))>64 THEN
    RAISE EXCEPTION 'RESOURCE_LIMIT';
  END IF;
  FOR input IN SELECT value FROM jsonb_array_elements(NEW.spec->'pinnedInputs') LOOP
    SELECT snapshot_id INTO actual_snapshot FROM moria.snapshot_history
      WHERE environment=NEW.environment AND dataset=input->>'resource'
        AND revision=(input->>'revision')::bigint;
    IF NOT FOUND OR actual_snapshot IS DISTINCT FROM input->>'snapshot' THEN
      RAISE EXCEPTION 'INPUT_PIN_CHANGED';
    END IF;
    PERFORM moria.runtime_dataset_root_pin(NEW.environment,input->>'resource',
      (input->>'revision')::bigint,'execution',NEW.id::text);
  END LOOP;
  FOR expected IN SELECT key,value FROM jsonb_each_text(NEW.spec->'expectedHeads') LOOP
    IF expected.value::bigint>0 THEN
      PERFORM moria.runtime_dataset_root_pin(NEW.environment,expected.key,
        expected.value::bigint,'execution',NEW.id::text);
    END IF;
  END LOOP;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_execution_dataset_root_pins AFTER INSERT ON moria.runtime_executions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_execution_dataset_pin_admit();
`;
