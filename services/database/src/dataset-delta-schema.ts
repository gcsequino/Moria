/** Accepted Source consumers capture real journal pages before incremental activation. */
export const datasetDeltaSchema = String.raw`
CREATE TABLE moria.native_source_delta_windows(
 build uuid PRIMARY KEY REFERENCES moria.indexing_builds(id),
 environment text NOT NULL,dataset text NOT NULL,target text NOT NULL,
 basis_generation bigint NOT NULL,basis_store uuid NOT NULL REFERENCES moria.native_source_row_stores(id),
 basis_source_version bigint NOT NULL CHECK(basis_source_version>0),
 basis_dataset_revision bigint NOT NULL CHECK(basis_dataset_revision>0),
 target_dataset_revision bigint NOT NULL CHECK(target_dataset_revision>basis_dataset_revision),
 definition_hash text NOT NULL,binding_revision text NOT NULL,key_schema_hash text NOT NULL,
 ordered_primary_key text[] NOT NULL,dataset_schema_hash text NOT NULL,
 physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,
 cursor_revision bigint NOT NULL,cursor_snapshot text NOT NULL,cursor_metadata text NOT NULL,
 status text NOT NULL DEFAULT 'capturing' CHECK(status IN ('capturing','qualified','bootstrap')),
 reason text,has_mutation boolean NOT NULL DEFAULT false,
 coverage int8range GENERATED ALWAYS AS (int8range(basis_dataset_revision+1,CASE WHEN target_dataset_revision=9223372036854775807 THEN NULL ELSE target_dataset_revision+1 END,'[)')) STORED,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),closed_at timestamptz,
 CHECK(cursor_revision BETWEEN basis_dataset_revision AND target_dataset_revision),
 CHECK((status='bootstrap')=(reason IS NOT NULL)),
 CHECK(status<>'qualified' OR cursor_revision=target_dataset_revision)
);
CREATE INDEX native_source_delta_window_scope ON moria.native_source_delta_windows(environment,dataset,target_dataset_revision,basis_dataset_revision)
 WHERE closed_at IS NULL AND status IN ('capturing','qualified');
CREATE INDEX native_source_delta_window_coverage ON moria.native_source_delta_windows USING gist(coverage)
 WHERE closed_at IS NULL AND status IN ('capturing','qualified');
CREATE INDEX runtime_dataset_commit_result_root ON moria.runtime_dataset_commit_deltas(result_root,result_revision);
CREATE INDEX runtime_dataset_commit_replace_boundary ON moria.runtime_dataset_commit_deltas(environment,dataset,result_revision)
 WHERE change_kind='replace';
CREATE TABLE moria.native_source_delta_members(
 build uuid NOT NULL REFERENCES moria.native_source_delta_windows(build),
 result_revision bigint NOT NULL,commit uuid NOT NULL,dataset text NOT NULL,
 PRIMARY KEY(build,result_revision),
 FOREIGN KEY(commit,dataset) REFERENCES moria.runtime_dataset_commit_deltas(commit,dataset)
);
CREATE FUNCTION moria.runtime_source_delta_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'SOURCE_STORE_AUTHORITY_REQUIRED';END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER native_source_delta_window_00_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_delta_windows
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_delta_owner();
CREATE TRIGGER native_source_delta_member_owner BEFORE INSERT OR UPDATE OR DELETE ON moria.native_source_delta_members
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_delta_owner();
CREATE TRIGGER native_source_delta_member_immutable BEFORE UPDATE OR DELETE ON moria.native_source_delta_members
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE FUNCTION moria.runtime_source_delta_window_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- PostgreSQL computes the derived range after BEFORE triggers. The immutable endpoints are checked here.
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['coverage','closed_at','cursor_revision','cursor_snapshot','cursor_metadata','status','reason','has_mutation'])
  IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['coverage','closed_at','cursor_revision','cursor_snapshot','cursor_metadata','status','reason','has_mutation'])
  OR NEW.cursor_revision<OLD.cursor_revision OR OLD.has_mutation AND NOT NEW.has_mutation
  OR OLD.status<>'capturing' AND (to_jsonb(NEW)-ARRAY['coverage','closed_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['coverage','closed_at'])
  OR OLD.closed_at IS NOT NULL AND (to_jsonb(NEW)-'coverage') IS DISTINCT FROM (to_jsonb(OLD)-'coverage')
  OR NEW.closed_at IS DISTINCT FROM OLD.closed_at AND (NEW.closed_at IS NULL OR NOT EXISTS(
    SELECT 1 FROM moria.indexing_builds b WHERE b.id=NEW.build AND b.status IN ('active','failed','cancelled','superseded','conflicted'))) THEN RAISE EXCEPTION 'IMMUTABLE_SOURCE_WINDOW';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_delta_window_identity BEFORE UPDATE OR DELETE ON moria.native_source_delta_windows
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_delta_window_identity();

-- The range is derived only from an actual accepted build and its existing materialized alias.
-- It holds custody while bounded pages are qualified, and never grants object IO.
CREATE FUNCTION moria.runtime_source_delta_capture(input_build uuid) RETURNS void LANGUAGE plpgsql
 SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE b moria.indexing_builds;a moria.native_source_generation_aliases;s moria.native_source_row_stores;
 d moria.native_dataset_bindings;basis moria.runtime_dataset_commit_deltas;profile jsonb;replacement boolean;
BEGIN
 SELECT * INTO b FROM moria.indexing_builds WHERE id=input_build;
 IF NOT FOUND OR b.status NOT IN ('pending','planning','running','preparing','ready')
  OR NOT(b.definition ? 'servingBindingGeneration') OR NOT(b.definition ? 'projectionRevision')
  OR NOT EXISTS(SELECT 1 FROM moria.native_dataset_bindings current_dataset WHERE current_dataset.environment=b.environment
    AND current_dataset.resource::text=b.source AND current_dataset.enabled)
  OR EXISTS(SELECT 1 FROM moria.native_source_delta_windows WHERE build=input_build) THEN RETURN;END IF;
 PERFORM moria.indexing_check(input_build);
 profile:=moria.runtime_source_store_profile(input_build);
 IF profile IS NULL THEN RETURN;END IF;
 SELECT alias.* INTO a FROM moria.source_generations g JOIN moria.native_source_generation_aliases alias
  ON alias.environment=g.environment AND alias.object_type=g.object_type AND alias.revision=g.revision
  WHERE g.environment=b.environment AND g.object_type=b.target ORDER BY g.revision DESC LIMIT 1;
 IF NOT FOUND OR a.indexed_dataset_revision>=b.source_revision THEN RETURN;END IF;
 SELECT * INTO s FROM moria.native_source_row_stores WHERE id=a.store AND status='published';
 SELECT * INTO d FROM moria.native_dataset_bindings WHERE environment=b.environment AND resource::text=b.source AND enabled;
 SELECT * INTO basis FROM moria.runtime_dataset_commit_deltas WHERE environment=b.environment AND dataset=b.source AND result_revision=a.indexed_dataset_revision;
 IF s.id IS NULL OR d.resource IS NULL OR basis.commit IS NULL
  OR ROW(s.definition_hash,s.key_schema_hash,s.schema_hash,s.serving_generation,s.layout_hash,s.constraint_hash,s.incarnation)
   IS DISTINCT FROM ROW(profile->>'definitionHash',profile->>'keySchemaHash',profile->>'schemaHash',(profile->>'servingGeneration')::bigint,profile->>'layoutHash',profile->>'constraintHash',(profile->>'incarnation')::uuid)
  OR basis.binding_revision<>d.revision OR basis.key_schema_hash<>d.key_schema_hash OR basis.ordered_primary_key<>d.primary_key
  OR basis.result_root<>a.indexed_root OR basis.result_snapshot IS DISTINCT FROM a.indexed_snapshot THEN RETURN;END IF;
 PERFORM moria.runtime_dataset_root_assert(b.metadata_location);
 SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_commit_deltas edge WHERE edge.environment=b.environment AND edge.dataset=b.source
  AND edge.change_kind='replace' AND edge.result_revision>a.indexed_dataset_revision AND edge.result_revision<=b.source_revision) INTO replacement;
 INSERT INTO moria.native_source_delta_windows(build,environment,dataset,target,basis_generation,basis_store,basis_source_version,
  basis_dataset_revision,target_dataset_revision,definition_hash,binding_revision,key_schema_hash,ordered_primary_key,dataset_schema_hash,
  physical_fingerprint,incarnation,cursor_revision,cursor_snapshot,cursor_metadata,status,reason)
 VALUES(input_build,b.environment,b.source,b.target,a.revision,a.store,a.source_version,a.indexed_dataset_revision,b.source_revision,
  s.definition_hash,d.revision,d.key_schema_hash,d.primary_key,basis.dataset_schema_hash,d.physical_binding_fingerprint,s.incarnation,
  a.indexed_dataset_revision,basis.result_snapshot,basis.result_metadata,
  CASE WHEN replacement THEN 'bootstrap' ELSE 'capturing' END,CASE WHEN replacement THEN 'replacementBoundary' END) ON CONFLICT DO NOTHING;
END $$;
CREATE FUNCTION moria.runtime_source_delta_build_capture() RETURNS trigger LANGUAGE plpgsql
 SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM moria.runtime_source_delta_capture(NEW.id);RETURN NEW;
END $$;
-- C04 enqueues before appending its edge; deferred capture sees the real immutable publication.
CREATE CONSTRAINT TRIGGER native_source_delta_build_capture AFTER INSERT ON moria.indexing_builds
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_delta_build_capture();
-- Remove terminal work from the live interval indexes without scanning any journal members.
CREATE FUNCTION moria.runtime_source_delta_build_close() RETURNS trigger LANGUAGE plpgsql
 SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 UPDATE moria.native_source_delta_windows SET closed_at=clock_timestamp() WHERE build=NEW.id AND closed_at IS NULL;
 RETURN NEW;
END $$;
CREATE TRIGGER native_source_delta_build_close AFTER UPDATE OF status ON moria.indexing_builds
 FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status AND NEW.status IN ('active','failed','cancelled','superseded','conflicted'))
 EXECUTE FUNCTION moria.runtime_source_delta_build_close();
CREATE FUNCTION moria.runtime_source_delta_root_pinned(input_root text) RETURNS boolean LANGUAGE sql STABLE
 SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
 SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_commit_deltas edge
  JOIN moria.native_source_delta_windows w ON w.environment=edge.environment AND w.dataset=edge.dataset
   AND w.closed_at IS NULL AND w.status IN ('capturing','qualified') AND w.coverage @> edge.result_revision
  JOIN moria.indexing_builds build ON build.id=w.build AND build.environment=w.environment AND build.source=w.dataset
   AND build.target=w.target AND build.source_revision=w.target_dataset_revision
   AND build.status IN ('pending','planning','running','preparing','ready')
  JOIN moria.indexing_bindings current_binding ON current_binding.environment=build.environment AND current_binding.target=build.target
   AND current_binding.enabled AND current_binding.revision=build.binding_revision AND current_binding.definition=build.definition
  JOIN moria.native_dataset_bindings dataset_binding ON dataset_binding.environment=w.environment AND dataset_binding.resource::text=w.dataset
   AND dataset_binding.enabled AND dataset_binding.revision=w.binding_revision AND dataset_binding.physical_binding_fingerprint=w.physical_fingerprint
  JOIN moria.indexing_instance instance ON instance.incarnation=w.incarnation
  WHERE edge.result_root=input_root)
$$;
CREATE FUNCTION moria.runtime_source_delta_page(input_build uuid,input_worker uuid,input_fence bigint,maximum integer DEFAULT 256)
 RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE w moria.native_source_delta_windows;b moria.indexing_builds;binding moria.native_dataset_bindings;
 edge moria.runtime_dataset_commit_deltas;descriptor jsonb;bytes bigint:=0;captured integer:=0;denial text;found_edge boolean:=false;
BEGIN
 IF maximum IS NULL OR maximum NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT';END IF;
 PERFORM moria.runtime_indexing_staging_shared(input_build);
 SELECT * INTO b FROM moria.indexing_builds WHERE id=input_build AND owner=input_worker AND fence=input_fence
  AND status IN ('planning','running','preparing','ready') AND lease_until>clock_timestamp() AND deadline>clock_timestamp() FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'LEASE_LOST';END IF;
 PERFORM moria.indexing_claim_check(input_build,NULL,input_worker,input_fence);
 SELECT * INTO w FROM moria.native_source_delta_windows WHERE build=input_build FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('kind','bootstrap','reason','noCompatibleMaterializedBasis');END IF;
 IF w.closed_at IS NOT NULL THEN RETURN jsonb_build_object('kind','bootstrap','reason','closedWindowCustody');END IF;
 SELECT * INTO binding FROM moria.native_dataset_bindings WHERE environment=w.environment AND resource::text=w.dataset AND enabled;
 IF binding.resource IS NULL OR binding.revision<>w.binding_revision OR binding.physical_binding_fingerprint<>w.physical_fingerprint
  OR binding.key_schema_hash<>w.key_schema_hash OR binding.primary_key<>w.ordered_primary_key
  OR w.incarnation<>(SELECT incarnation FROM moria.indexing_instance)
  OR w.definition_hash<>encode(sha256(convert_to(b.definition::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'BINDING_CHANGED';END IF;
 IF w.status='capturing' THEN
  IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('moria/recovery-retention/v1',0)) THEN
   RETURN jsonb_build_object('state','busy','pending',true);
  END IF;
  FOR edge IN SELECT * FROM moria.runtime_dataset_commit_deltas
   WHERE environment=w.environment AND dataset=w.dataset AND result_revision>w.cursor_revision AND result_revision<=w.target_dataset_revision
   ORDER BY result_revision LIMIT maximum LOOP
   found_edge:=true;
   descriptor:=jsonb_build_object('commit',edge.commit,'basisRevision',edge.basis_revision::text,'resultRevision',edge.result_revision::text,
    'basisRoot',edge.basis_root,'resultRoot',edge.result_root,'changeKind',edge.change_kind);
   IF bytes+octet_length(descriptor::text)>1048576 THEN EXIT;END IF;
   IF edge.basis_revision<>w.cursor_revision OR edge.result_revision<>w.cursor_revision+1
    OR edge.basis_snapshot IS DISTINCT FROM w.cursor_snapshot OR edge.basis_metadata IS DISTINCT FROM w.cursor_metadata
    OR edge.basis_root IS DISTINCT FROM moria.runtime_dataset_root_key(w.cursor_metadata) THEN denial:='journalGap';EXIT;END IF;
   IF edge.binding_revision<>w.binding_revision OR edge.dataset_schema_hash<>w.dataset_schema_hash
    OR edge.key_schema_hash<>w.key_schema_hash OR edge.ordered_primary_key<>w.ordered_primary_key THEN denial:='journalProfileChanged';EXIT;END IF;
   IF edge.change_kind='replace' THEN denial:='replacementBoundary';EXIT;END IF;
   IF NOT EXISTS(SELECT 1 FROM moria.snapshot_history h WHERE h.environment=edge.environment AND h.dataset=edge.dataset
    AND h.revision=edge.result_revision AND h.metadata_location=edge.result_metadata AND h.snapshot_id=edge.result_snapshot
    AND h.execution=CASE edge.producer_kind WHEN 'execution' THEN edge.execution::text ELSE 'dataset-maintenance/'||edge.maintenance::text END)
    OR NOT EXISTS(SELECT 1 FROM moria.publications p WHERE p.environment=edge.environment
     AND p.execution=CASE edge.producer_kind WHEN 'execution' THEN edge.execution::text ELSE 'dataset-maintenance/'||edge.maintenance::text END
     AND p.receipt=edge.sealed_receipt)
    OR (edge.producer_kind='execution' AND NOT EXISTS(SELECT 1 FROM moria.runtime_executions execution JOIN moria.runtime_snapshot_candidates candidate ON candidate.execution=execution.id
     WHERE execution.id=edge.execution AND execution.status='succeeded' AND execution.result=edge.sealed_receipt AND candidate.id=edge.candidate
      AND candidate.id=edge.commit AND candidate.resource=edge.dataset AND candidate.metadata_location=edge.result_metadata
      AND candidate.snapshot_id=edge.result_snapshot AND candidate.expected_revision=edge.basis_revision))
    OR (edge.producer_kind='maintenance' AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_maintenance_runs maintenance
     WHERE maintenance.id=edge.maintenance AND maintenance.id=edge.commit AND maintenance.policy=edge.policy AND maintenance.status='committed'
      AND maintenance.receipt=edge.sealed_receipt AND maintenance.expected_revision=edge.basis_revision
      AND maintenance.old_metadata=edge.basis_metadata AND maintenance.new_metadata=edge.result_metadata)) THEN denial:='journalReceiptMissing';EXIT;END IF;
   IF edge.change_kind='unchanged' AND (edge.unchanged_reason IS DISTINCT FROM 'metadataPrune'
    OR edge.result_snapshot IS DISTINCT FROM edge.basis_snapshot OR edge.equivalence_receipt->>'version' IS DISTINCT FROM '1'
    OR edge.equivalence_receipt->>'kind' IS DISTINCT FROM 'metadataPrune'
    OR edge.equivalence_receipt->>'basisRoot' IS DISTINCT FROM edge.basis_root OR edge.equivalence_receipt->>'resultRoot' IS DISTINCT FROM edge.result_root
    OR edge.equivalence_receipt->>'snapshot' IS DISTINCT FROM edge.result_snapshot
    OR edge.equivalence_receipt->>'currentSnapshotHash' IS NULL OR edge.equivalence_receipt->>'currentSnapshotHash' !~ '^sha256:[0-9a-f]{64}$')
    THEN denial:='unsupportedEquivalence';EXIT;END IF;
   BEGIN
    PERFORM moria.runtime_dataset_commit_root_check(edge.environment,edge.dataset,edge.result_metadata,edge.result_snapshot,edge.result_revision);
   EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT IN ('DATASET_DELTA_ROOT_UNQUALIFIED','DATASET_SNAPSHOT_EXPIRED') THEN RAISE;END IF;
    denial:='journalRootUnavailable';
   END;
   IF denial IS NOT NULL THEN EXIT;END IF;
   INSERT INTO moria.native_source_delta_members(build,result_revision,commit,dataset) VALUES(input_build,edge.result_revision,edge.commit,edge.dataset);
   w.cursor_revision:=edge.result_revision;w.cursor_snapshot:=edge.result_snapshot;w.cursor_metadata:=edge.result_metadata;
   w.has_mutation:=w.has_mutation OR edge.change_kind='mutation';bytes:=bytes+octet_length(descriptor::text);captured:=captured+1;
  END LOOP;
  IF NOT found_edge AND w.cursor_revision<w.target_dataset_revision THEN denial:='journalGap';END IF;
  IF denial IS NOT NULL THEN w.status:='bootstrap';w.reason:=denial;
  ELSIF w.cursor_revision=w.target_dataset_revision THEN
   IF w.cursor_snapshot IS DISTINCT FROM b.snapshot_id OR w.cursor_metadata IS DISTINCT FROM b.metadata_location THEN
    w.status:='bootstrap';w.reason:='targetPinChanged';
   ELSE w.status:='qualified';END IF;
  END IF;
  IF b.lease_until<=clock_timestamp() OR b.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'LEASE_LOST';END IF;
  UPDATE moria.native_source_delta_windows SET cursor_revision=w.cursor_revision,cursor_snapshot=w.cursor_snapshot,cursor_metadata=w.cursor_metadata,
   has_mutation=w.has_mutation,status=w.status,reason=w.reason WHERE build=input_build;
 END IF;
 RETURN jsonb_build_object('build',input_build,'state',w.status,'kind',CASE WHEN w.status='bootstrap' THEN 'bootstrap' WHEN w.has_mutation THEN 'mutation' ELSE 'unchanged' END,
  'reason',w.reason,'basisGeneration',w.basis_generation::text,'store',w.basis_store,'basisSourceVersion',w.basis_source_version::text,
  'basisDatasetRevision',w.basis_dataset_revision::text,'capturedThrough',w.cursor_revision::text,'targetDatasetRevision',w.target_dataset_revision::text,
  'captured',captured,'descriptorBytes',bytes::text,'pending',w.status='capturing');
END $$;
REVOKE ALL ON FUNCTION moria.runtime_source_delta_capture(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_source_delta_build_capture() FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_source_delta_build_close() FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_source_delta_root_pinned(text) FROM PUBLIC;
`;
