/** Exact current and pinned declaration-file metadata; no storage or secret-material authority. */
export const sourceDeclarationSchema = String.raw`
CREATE FUNCTION moria.native_source_declaration_labels_batch_state(state jsonb,requests jsonb)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE request record; target record; released record; descriptor uuid; file_id uuid;
 files uuid[]; targets uuid[]:='{}'; labels text[]:='{}'; source_mark jsonb;
BEGIN
 IF state->>'privatePurpose'='indexing' OR state->>'instanceId' IS NULL THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 IF jsonb_typeof(requests) IS DISTINCT FROM 'array' OR jsonb_array_length(requests) NOT BETWEEN 1 AND 32
  OR octet_length(requests::text)>16384 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(requests) item WHERE jsonb_typeof(item) IS DISTINCT FROM 'object'
  OR (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(item) key) IS DISTINCT FROM ARRAY['release','resource']
  OR jsonb_typeof(item->'resource') IS DISTINCT FROM 'string' OR jsonb_typeof(item->'release') IS DISTINCT FROM 'string'
  OR item->>'resource'!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  OR octet_length(item->>'release') NOT BETWEEN 1 AND 256)
  THEN RAISE EXCEPTION 'SOURCE_FILE_PROOF_REQUIRED'; END IF;
 FOR request IN SELECT DISTINCT (item->>'resource')::uuid AS resource,item->>'release' AS digest
  FROM jsonb_array_elements(requests) item ORDER BY resource,digest LOOP
  SELECT e.id,b.root_id,b.digest,b.reference INTO target FROM moria.fs_entries e
   JOIN moria.fs_resource_bindings b ON b.entry_id=e.id AND b.active AND b.environment=state->>'environment'
   JOIN moria.fs_roles role ON role.entry_id=b.root_id AND NOT role.retired
   JOIN moria.fs_active_releases active ON active.root_id=b.root_id AND active.environment=b.environment AND active.digest=b.digest
   JOIN moria.fs_resource_identities owned_id ON owned_id.root_id=b.root_id AND owned_id.reference=b.reference AND owned_id.resource_id=e.resource_id
   WHERE e.resource_id=request.resource AND e.environment_id=state->>'environment' AND e.kind='resource' AND NOT e.deleted
    AND EXISTS(SELECT 1 FROM jsonb_array_elements(active.resources) item
     WHERE concat_ws('/',item->'metadata'->>'namespace',item->>'kind',item->'metadata'->>'name')=b.reference);
  IF NOT FOUND THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.fs_source_releases r WHERE r.digest=target.digest AND r.root_id=target.root_id AND r.owned ? target.reference)
   THEN RAISE EXCEPTION 'SOURCE_FILE_PROOF_REQUIRED'; END IF;
  PERFORM moria.runtime_data_operational(state->>'environment',request.resource::text,'read');
  targets:=array_append(targets,target.id);
  FOR released IN SELECT r.* FROM moria.fs_source_releases r
   WHERE r.digest=ANY(ARRAY[target.digest,request.digest]) AND r.root_id=target.root_id AND r.owned ? target.reference LOOP
   IF NOT EXISTS(SELECT 1 FROM moria.fs_source_resource_files p WHERE p.digest=released.digest AND p.reference=target.reference)
    THEN RAISE EXCEPTION 'SOURCE_FILE_PROOF_REQUIRED'; END IF;
   SELECT (file->'entry'->>'id')::uuid INTO descriptor FROM jsonb_array_elements(released.snapshot->'files') file
    WHERE file->>'path'='.'||(released.snapshot->'role'->>'kind')||'.json';
   IF descriptor IS NULL OR NOT EXISTS(SELECT 1 FROM moria.fs_source_resource_files p
    WHERE p.digest=released.digest AND p.reference=target.reference AND p.entry_id=descriptor)
    THEN RAISE EXCEPTION 'SOURCE_FILE_PROOF_REQUIRED'; END IF;
   SELECT array_agg(entry_id) INTO files FROM (SELECT p.entry_id FROM moria.fs_source_resource_files p
    WHERE p.digest=released.digest AND p.reference=target.reference ORDER BY p.entry_id LIMIT 4097) bounded;
   IF cardinality(files)>4096 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
   IF EXISTS(SELECT 1 FROM unnest(files) AS candidate(entry_id) LEFT JOIN moria.fs_entries e ON e.id=candidate.entry_id
    WHERE e.id IS NULL OR e.kind<>'file' OR e.deleted) THEN RAISE EXCEPTION 'NOT_FOUND'; END IF;
   targets:=targets||files;
   targets:=ARRAY(SELECT DISTINCT value FROM unnest(targets) value);
   IF cardinality(targets)>4096 THEN RAISE EXCEPTION 'RESOURCE_LIMIT'; END IF;
   FOR source_mark IN SELECT value FROM jsonb_array_elements(released.markings) LOOP
    IF source_mark->>'environment' IS DISTINCT FROM state->>'environment' THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
    labels:=array_append(labels,source_mark->>'marking');
   END LOOP;
  END LOOP;
  IF NOT EXISTS(SELECT 1 FROM moria.fs_source_releases r WHERE r.digest=request.digest AND r.root_id=target.root_id AND r.owned ? target.reference)
   THEN RAISE EXCEPTION 'SOURCE_FILE_PROOF_REQUIRED'; END IF;
 END LOOP;
 labels:=labels||ARRAY(SELECT jsonb_array_elements_text(moria.native_filesystem_acl_targets_state(state,targets,'read',false)));
 IF EXISTS(SELECT 1 FROM unnest(labels) AS all_labels(value) WHERE value IS NULL OR NOT(state->'identity'->'markings' ? value))
  THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 RETURN to_jsonb(ARRAY(SELECT DISTINCT value FROM unnest(labels) AS all_labels(value) ORDER BY value));
END $$;
REVOKE ALL ON FUNCTION moria.native_source_declaration_labels_batch_state(jsonb,jsonb) FROM PUBLIC;
CREATE FUNCTION moria.native_source_declaration_labels_state(state jsonb,resource_id_value uuid,release_digest text)
RETURNS jsonb LANGUAGE plpgsql AS $$
BEGIN
 RETURN moria.native_source_declaration_labels_batch_state(state,jsonb_build_array(jsonb_build_object('resource',resource_id_value,'release',release_digest)));
END $$;
REVOKE ALL ON FUNCTION moria.native_source_declaration_labels_state(jsonb,uuid,text) FROM PUBLIC;
CREATE FUNCTION moria.native_source_declaration_labels_batch(token_hash text,requests jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 RETURN moria.native_source_declaration_labels_batch_state(moria.native_serving_session(token_hash),requests);
END $$;
CREATE FUNCTION moria.native_source_declaration_labels(token_hash text,resource_id_value uuid,release_digest text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 RETURN moria.native_source_declaration_labels_batch_state(moria.native_serving_session(token_hash),jsonb_build_array(jsonb_build_object('resource',resource_id_value,'release',release_digest)));
END $$;
CREATE FUNCTION moria.native_registered_declaration_labels_batch(service_id uuid,env text,target uuid,job_domain text,target_revision text,class_name text,requests jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 RETURN moria.native_source_declaration_labels_batch_state(moria.runtime_service_target_authority(service_id,env,target,job_domain,target_revision,class_name),requests);
END $$;
CREATE FUNCTION moria.native_registered_declaration_labels(service_id uuid,env text,target uuid,job_domain text,target_revision text,class_name text,resource_id_value uuid,release_digest text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 RETURN moria.native_source_declaration_labels_batch_state(moria.runtime_service_target_authority(service_id,env,target,job_domain,target_revision,class_name),jsonb_build_array(jsonb_build_object('resource',resource_id_value,'release',release_digest)));
END $$;
`;
