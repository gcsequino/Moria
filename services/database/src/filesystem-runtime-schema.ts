/** Current entry authority shared by native filesystem reads and mutations. */
export const filesystemRuntimeSchema = String.raw`
CREATE FUNCTION moria.native_filesystem_access(token_hash text, target_id uuid, operation text, include_inactive boolean DEFAULT false) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;
BEGIN
  state:=moria.native_serving_session(token_hash);
  IF state->>'privatePurpose'='indexing'
    OR NOT (state->'identity'->'grantedOperations' ? ('filesystem:'||operation))
    OR NOT (state->'identity'->'applicationScopes' ? ('filesystem:'||operation)) THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  RETURN moria.native_filesystem_acl_state(state,target_id,operation,include_inactive);
END $$;
CREATE FUNCTION moria.native_app_frontend_access(token_hash text,resource_id_value uuid) RETURNS jsonb
  LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb; binding record;
BEGIN
  state:=moria.native_serving_session(token_hash);
  IF state->>'privatePurpose' IS NOT NULL OR state->>'application' IS DISTINCT FROM resource_id_value::text
    THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
  SELECT b.reference,a.resources INTO binding FROM moria.fs_entries e
    JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active AND b.environment=state->>'environment'
    JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired
    JOIN moria.fs_active_releases a ON a.root_id=b.root_id AND a.environment=b.environment AND a.digest=b.digest
    WHERE e.resource_id=resource_id_value AND e.environment_id=state->>'environment' AND NOT e.deleted AND e.kind='resource';
  IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(binding.resources) item
    WHERE item->>'kind'='Application' AND item->'spec'->>'mode'='browser'
      AND concat_ws('/',item->'metadata'->>'namespace',item->>'kind',item->'metadata'->>'name')=binding.reference)
    THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
  RETURN moria.native_serving_resource_access_state(state,resource_id_value::text,'read');
END $$;
CREATE FUNCTION moria.native_filesystem_visible(token_hash text,entry_id uuid) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.native_filesystem_access(token_hash,entry_id,'read'); RETURN true;
EXCEPTION WHEN raise_exception THEN
  IF SQLERRM IN ('FORBIDDEN','NOT_FOUND') THEN RETURN false; END IF; RAISE;
END $$;
CREATE TABLE moria.native_function_filesystem_reads (
  execution uuid NOT NULL REFERENCES moria.runtime_executions(id), attempt uuid,
  call_key text NOT NULL CHECK(length(call_key) BETWEEN 1 AND 512),
  evidence jsonb NOT NULL CHECK(jsonb_typeof(evidence)='array'),
  markings jsonb NOT NULL CHECK(jsonb_typeof(markings)='array'), fingerprint text NOT NULL,
  PRIMARY KEY(execution,call_key)
);
`;
