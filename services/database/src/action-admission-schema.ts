/** One current Action proof derives its demands from immutable server bindings, never caller state. */
export const actionAdmissionSchema = String.raw`
CREATE FUNCTION moria.native_action_resource_access(token_hash text,action_resource text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE
 state jsonb;
 action moria.native_serving_actions;
 binding moria.native_serving_bindings;
 target text;
 targets text[];
 required jsonb;
 permission text;
 request jsonb;
 requests jsonb:='[]'::jsonb;
 labels jsonb;
 results jsonb:='[]'::jsonb;
 position integer:=0;
BEGIN
 state:=moria.native_serving_session(token_hash);
 IF state->>'privatePurpose'='indexing' THEN
  RAISE EXCEPTION 'FORBIDDEN';
 END IF;
 action_resource:=moria.runtime_data_resource(state->>'environment',action_resource);
 SELECT * INTO action FROM moria.native_serving_actions a
 WHERE a.environment=state->>'environment' AND a.resource=action_resource AND a.enabled;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'NOT_FOUND';
 END IF;
 IF action.definition->>'resource' IS DISTINCT FROM action.resource
  OR action.definition->>'revision' IS DISTINCT FROM action.revision
  OR NULLIF(action.definition->>'function','') IS NOT NULL
  OR (action.definition->'typedEdits' IS NOT NULL AND action.definition->'typedEdits'<>'null'::jsonb
   AND jsonb_typeof(action.definition->'typedEdits') IS DISTINCT FROM 'array') THEN
  RAISE EXCEPTION 'BINDING_INVALID';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(action.definition->'typedEdits')='array' THEN action.definition->'typedEdits' ELSE '[]'::jsonb END) edit
  WHERE edit->>'op' NOT IN ('set','clearOverride','delete','restore')) THEN
  RAISE EXCEPTION 'BINDING_INVALID';
 END IF;
 permission:=action.resource||':invoke';
 IF NOT (state->'identity'->'grantedOperations' ? permission)
  OR NOT (state->'identity'->'applicationScopes' ? permission)
  OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(action.definition->'markings') label
   WHERE NOT (state->'identity'->'markings' ? label)) THEN
  RAISE EXCEPTION 'FORBIDDEN';
 END IF;
 SELECT array_agg(resource ORDER BY resource) INTO targets FROM (
  SELECT action.object_resource AS resource
  UNION
  SELECT moria.runtime_data_resource(state->>'environment',edit->>'targetType')
  FROM jsonb_array_elements(CASE WHEN jsonb_typeof(action.definition->'typedEdits')='array' THEN action.definition->'typedEdits' ELSE '[]'::jsonb END) edit
 ) declared;
 IF cardinality(targets) NOT BETWEEN 1 AND 101 THEN
  RAISE EXCEPTION 'ACTION_LIMIT';
 END IF;
 IF action.resource=ANY(targets) THEN
  RAISE EXCEPTION 'BINDING_INVALID';
 END IF;
 -- All operation/clearance checks precede filesystem/provenance evaluation.
 FOREACH target IN ARRAY targets LOOP
  SELECT * INTO binding FROM moria.native_serving_bindings b
  WHERE b.environment=state->>'environment' AND b.resource=target AND b.enabled;
  IF NOT FOUND THEN
   RAISE EXCEPTION 'NOT_FOUND';
  END IF;
  IF binding.definition->'policies'->>'resource' IS DISTINCT FROM target THEN
   RAISE EXCEPTION 'BINDING_INVALID';
  END IF;
  permission:=target||':read';
  IF NOT (state->'identity'->'grantedOperations' ? permission)
   OR NOT (state->'identity'->'applicationScopes' ? permission)
   OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(binding.definition->'policies'->'markings') label
    WHERE NOT (state->'identity'->'markings' ? label)) THEN
   RAISE EXCEPTION 'FORBIDDEN';
  END IF;
  FOR required IN SELECT value FROM jsonb_array_elements(COALESCE(binding.definition->'requiredOperations','[]'::jsonb)) LOOP
   permission:=(required->>'resource')||':'||(required->>'operation');
   IF permission IS NULL OR NOT (state->'identity'->'grantedOperations' ? permission)
    OR NOT (state->'identity'->'applicationScopes' ? permission) THEN
    RAISE EXCEPTION 'FORBIDDEN';
   END IF;
  END LOOP;
  requests:=requests||jsonb_build_array(jsonb_build_object('resource',target,'operation','read'),jsonb_build_object('resource',target,'operation','write'));
 END LOOP;
 requests:=requests||jsonb_build_array(jsonb_build_object('resource',action.resource,'operation','read'),jsonb_build_object('resource',action.resource,'operation','write'));
 labels:=moria.native_serving_resource_access_batch_state(state,requests);
 IF jsonb_typeof(labels) IS DISTINCT FROM 'array' OR jsonb_array_length(labels)<>jsonb_array_length(requests) THEN
  RAISE EXCEPTION 'BINDING_INVALID';
 END IF;
 FOR request IN SELECT value FROM jsonb_array_elements(requests) LOOP
  IF jsonb_typeof(labels->position) IS DISTINCT FROM 'array' THEN
   RAISE EXCEPTION 'BINDING_INVALID';
  END IF;
  results:=results||jsonb_build_array(request||jsonb_build_object('markings',labels->position));
  position:=position+1;
 END LOOP;
 IF pg_column_size(results)>8388608 THEN
  RAISE EXCEPTION 'RESOURCE_LIMIT';
 END IF;
 RETURN jsonb_build_object('authority',state,'resources',results);
END $$;
`;
