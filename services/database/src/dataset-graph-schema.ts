/** Native v3 graph custody. Install only with the v3 receiver, publication, recovery and collector paths. */
export const datasetGraphSchema = String.raw`
CREATE TABLE moria.runtime_dataset_graph_byte_admissions(
 producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),
 physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,content jsonb NOT NULL,physical_version jsonb NOT NULL,
 PRIMARY KEY(producer,physical_fingerprint,incarnation,content)
);
CREATE TABLE moria.runtime_dataset_graph_nodes(
 id text PRIMARY KEY CHECK(id~'^[0-9a-f]{64}$'),environment text NOT NULL,dataset text NOT NULL,
 profile jsonb NOT NULL,content jsonb NOT NULL,kind text NOT NULL CHECK(kind IN ('root','directory','content','certificate','keyRun','partset')),
 height integer NOT NULL CHECK(height BETWEEN 0 AND 128),entries bigint NOT NULL CHECK(entries BETWEEN 0 AND 9007199254740991),
 logical_bytes bigint NOT NULL CHECK(logical_bytes BETWEEN 0 AND 9007199254740991),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),UNIQUE(environment,dataset,profile,content)
);
CREATE INDEX runtime_dataset_graph_object ON moria.runtime_dataset_graph_nodes(environment,dataset,(content->'object'->>'warehouseId'),(content->'object'->>'tableId'),(content->'object'->>'objectId'));
CREATE TABLE moria.runtime_dataset_graph_edges(
 parent text NOT NULL REFERENCES moria.runtime_dataset_graph_nodes(id),label text NOT NULL CHECK(octet_length(label) BETWEEN 1 AND 96 AND label~'^[a-zA-Z0-9_./:-]+$'),
 child text NOT NULL REFERENCES moria.runtime_dataset_graph_nodes(id),PRIMARY KEY(parent,label)
);
CREATE INDEX runtime_dataset_graph_child ON moria.runtime_dataset_graph_edges(child,parent);
CREATE TABLE moria.runtime_dataset_graph_qualifications(
 node text NOT NULL REFERENCES moria.runtime_dataset_graph_nodes(id),physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,
 physical_version jsonb NOT NULL,producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),qualified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(node,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_graph_admissions(
 producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,
 origin text NOT NULL,source_root text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(producer,node,physical_fingerprint,incarnation),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_graph_counts(
 node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,live_references bigint NOT NULL DEFAULT 0 CHECK(live_references>=0),
 PRIMARY KEY(node,physical_fingerprint,incarnation),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_graph_stages(
 id uuid PRIMARY KEY,producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),environment text NOT NULL,dataset text NOT NULL,
 node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,status text NOT NULL DEFAULT 'preparing' CHECK(status IN ('preparing','ready','candidate','published','abandoned')),
 candidate uuid UNIQUE,root text REFERENCES moria.runtime_dataset_roots(key),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_graph_references(
 kind text NOT NULL CHECK(kind IN ('stage','history','recovery')),owner text NOT NULL,node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,
 PRIMARY KEY(kind,owner,node,physical_fingerprint,incarnation),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_graph_queue(
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment text NOT NULL,dataset text NOT NULL,origin uuid NOT NULL,
 node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,delta smallint NOT NULL CHECK(delta IN (-1,1)),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),processed_at timestamptz,
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE INDEX runtime_dataset_graph_pending_stage ON moria.runtime_dataset_graph_queue(origin,sequence) WHERE processed_at IS NULL;
CREATE INDEX runtime_dataset_graph_pending_dataset ON moria.runtime_dataset_graph_queue(environment,dataset,physical_fingerprint,incarnation,sequence) WHERE processed_at IS NULL;
CREATE TABLE moria.runtime_dataset_graph_roots(
 root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,profile jsonb NOT NULL,
 PRIMARY KEY(root,physical_fingerprint,incarnation,profile),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_graph_retirements(
 root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),maintenance uuid NOT NULL REFERENCES moria.runtime_dataset_maintenance_runs(id),
 environment text NOT NULL,dataset text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,completed boolean NOT NULL DEFAULT false,
 PRIMARY KEY(root,physical_fingerprint,incarnation)
);
CREATE INDEX runtime_dataset_graph_retirement_pending ON moria.runtime_dataset_graph_retirements(environment,dataset,physical_fingerprint,incarnation,maintenance,root) WHERE NOT completed;
CREATE INDEX runtime_dataset_graph_retirement_work ON moria.runtime_dataset_graph_retirements(maintenance,root) WHERE NOT completed;
CREATE INDEX runtime_dataset_graph_history_root ON moria.snapshot_history(moria.runtime_dataset_root_key(metadata_location));
CREATE TABLE moria.runtime_dataset_graph_task_roots(
 task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),owner uuid NOT NULL,fence bigint NOT NULL CHECK(fence>0),resource text NOT NULL,
 node text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,
 PRIMARY KEY(task,owner,fence,resource),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
DO $$DECLARE target text;BEGIN
 FOREACH target IN ARRAY ARRAY['nodes','edges','qualifications','admissions','byte_admissions','roots','task_roots'] LOOP
  EXECUTE format('CREATE TRIGGER dataset_graph_immutable BEFORE UPDATE OR DELETE ON moria.%I FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable()','runtime_dataset_graph_'||target);
 END LOOP;
 FOREACH target IN ARRAY ARRAY['nodes','edges','qualifications','admissions','byte_admissions','counts','stages','references','queue','retirements','roots','task_roots'] LOOP
  EXECUTE format('CREATE TRIGGER dataset_graph_admission BEFORE INSERT OR UPDATE OR DELETE ON moria.%I FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_multipart_mutation()','runtime_dataset_graph_'||target);
 END LOOP;
END $$;
CREATE INDEX runtime_dataset_graph_unpublished ON moria.runtime_dataset_graph_stages(environment,dataset,physical_fingerprint,incarnation,created_at,id) WHERE status IN ('preparing','ready','candidate');
CREATE INDEX runtime_dataset_graph_member_object ON moria.runtime_dataset_graph_nodes((content->'object'->>'warehouseId'),(content->'object'->>'tableId'),(content->'object'->>'objectId'),id);
CREATE TABLE moria.runtime_recovery_graph_nodes(
 recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),node text NOT NULL,
 physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,processed boolean NOT NULL DEFAULT false,
 PRIMARY KEY(recovery_set,node,physical_fingerprint,incarnation),
 FOREIGN KEY(node,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation)
);
CREATE INDEX runtime_recovery_graph_pending ON moria.runtime_recovery_graph_nodes(recovery_set,node,physical_fingerprint,incarnation) WHERE NOT processed;
CREATE INDEX runtime_dataset_graph_root_nodes ON moria.runtime_dataset_graph_roots(root,physical_fingerprint,incarnation,node);
CREATE FUNCTION moria.runtime_recovery_graph_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.recovery_set,NEW.node,NEW.physical_fingerprint,NEW.incarnation) IS DISTINCT FROM ROW(OLD.recovery_set,OLD.node,OLD.physical_fingerprint,OLD.incarnation)
  OR (OLD.processed AND NOT NEW.processed) THEN RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_recovery_graph_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_graph_nodes FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_graph_immutable();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_graph_nodes FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE FUNCTION moria.runtime_dataset_graph_scope(claims jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;binding jsonb;producer text;namespace_state jsonb;profile jsonb;
BEGIN
 IF claims->>'kind'='maintenance' THEN
  state:=moria.runtime_dataset_maintenance_check((claims->>'run')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid);
  namespace_state:=state->'namespace';
  binding:=moria.runtime_dataset_binding(state->>'environment',state->>'dataset',namespace_state->>'warehouseId',namespace_state->>'tableId',namespace_state->>'revision');
  producer:='dataset-maintenance/'||(claims->>'run')||'/'||(claims->>'owner')||'/'||(claims->>'fence')||'/'||(claims->>'incarnation');
  state:=state||jsonb_build_object('resource',state->>'dataset','datasetBinding',binding,'icebergUuid',state->>'tableUuid');
 ELSIF claims->>'kind'='execution' THEN
  state:=moria.runtime_dataset_io_check(claims->>'session',(claims->>'execution')::uuid,(claims->>'task')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid,claims->>'warehouse',claims->>'table',claims->>'revision',true);
  namespace_state:=jsonb_build_object('warehouseId',state->>'warehouseId','tableId',state->>'tableId','revision',state->>'revision');
  producer:=moria.runtime_dataset_producer_scope((claims->>'execution')::uuid,(claims->>'task')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid,claims->>'warehouse',claims->>'table',claims->>'revision');
 ELSE RAISE EXCEPTION 'DATASET_PUBLICATION_CONTRACT_REQUIRED';END IF;
 binding:=state->'datasetBinding';
 IF binding IS NULL OR binding='null'::jsonb THEN RAISE EXCEPTION 'DATASET_WRITE_CAPABILITY_REQUIRED';END IF;
 -- Reserve only the already accepted real producer attempt; no lease or issued object is invented.
 IF claims->>'kind'='execution' THEN
  INSERT INTO moria.runtime_dataset_producers(scope,execution,task,owner,fence,incarnation,namespace,physical_fingerprint)
   VALUES(producer,(claims->>'execution')::uuid,(claims->>'task')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid,namespace_state,binding->>'physicalBindingFingerprint') ON CONFLICT DO NOTHING;
 ELSE
  INSERT INTO moria.runtime_dataset_producers(scope,maintenance,owner,fence,incarnation,namespace,physical_fingerprint)
   VALUES(producer,(claims->>'run')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid,namespace_state,binding->>'physicalBindingFingerprint') ON CONFLICT DO NOTHING;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_producers p WHERE p.scope=producer AND p.tombstoned_at IS NULL AND p.incarnation=(claims->>'incarnation')::uuid AND p.namespace=namespace_state AND p.physical_fingerprint=binding->>'physicalBindingFingerprint') THEN RAISE EXCEPTION 'GC_PRODUCER_TOMBSTONED';END IF;
 profile:=jsonb_build_object('version',3,'readerProfile','iceberg-v2-native-mor-bounded-v3','directoryProfile','sha256-radix16-v1','chunkProfiles',jsonb_build_array('moria-sha256-chunks-1m-v1','moria-sha256-chunks-64k-v1'),'tableUuid',state->>'icebergUuid','schemaIdentity',encode(sha256(convert_to((binding->'icebergSchema')::text,'UTF8')),'hex'),'keySchemaHash',binding->>'keySchemaHash');
 RETURN state||jsonb_build_object('producer',producer,'namespace',namespace_state,'physicalFingerprint',binding->>'physicalBindingFingerprint','incarnation',claims->>'incarnation','graphProfile',profile);
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_register(claims jsonb,input jsonb) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
#variable_conflict use_variable
DECLARE state jsonb;content jsonb;node_id text;edge jsonb;child moria.runtime_dataset_graph_nodes;maximum_height integer:=-1;previous moria.runtime_dataset_graph_nodes;physical text;inc uuid;producer_key text;labels text[]:='{}';
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);physical:=state->>'physicalFingerprint';inc:=(state->>'incarnation')::uuid;producer_key:=state->>'producer';content:=input->'content';
 IF octet_length(input::text)>131072 OR jsonb_typeof(input->'edges')<>'array' OR jsonb_array_length(input->'edges')>256 OR input->>'kind' NOT IN ('root','directory','content','certificate','keyRun','partset')
  OR (content->>'size')::bigint NOT BETWEEN 1 AND 16777216 OR content->'object'->>'warehouseId' IS DISTINCT FROM state->'namespace'->>'warehouseId' OR content->'object'->>'tableId' IS DISTINCT FROM state->'namespace'->>'tableId'
  OR (input->'physicalVersion'->>'size')::bigint IS DISTINCT FROM (content->>'size')::bigint OR input->'physicalVersion'->>'token' IS NULL THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_issued_objects i WHERE i.producer=producer_key AND i.physical_fingerprint=physical AND i.warehouse_id=content->'object'->>'warehouseId' AND i.table_id=content->'object'->>'tableId' AND i.object_id=content->'object'->>'objectId' AND i.expected_size=(content->>'size')::bigint AND i.expected_sha256=content->>'sha256') AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_byte_admissions a WHERE a.producer=producer_key AND a.physical_fingerprint=physical AND a.incarnation=inc AND a.content=content AND a.physical_version=input->'physicalVersion') THEN RAISE EXCEPTION 'GC_OBJECT_OWNERSHIP_CHANGED';END IF;
 FOR edge IN SELECT value FROM jsonb_array_elements(input->'edges') LOOP
  IF edge->>'label' IS NULL OR edge->>'label'=ANY(labels) OR octet_length(edge->>'label') NOT BETWEEN 1 AND 96 OR edge->>'label'!~'^[a-zA-Z0-9_./:-]+$' THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
  labels:=array_append(labels,edge->>'label');
  SELECT n.* INTO child FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id AND q.physical_fingerprint=physical AND q.incarnation=inc
   WHERE n.id=edge->>'node' AND n.environment=state->>'environment' AND n.dataset=state->>'resource' AND n.profile=state->'graphProfile'
   AND (q.producer=producer_key OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_admissions a WHERE a.producer=producer_key AND a.node=n.id AND a.physical_fingerprint=physical AND a.incarnation=inc));
  IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
  maximum_height:=greatest(maximum_height,child.height);
 END LOOP;
 IF (input->>'height')::integer IS DISTINCT FROM maximum_height+1 THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 node_id:=encode(sha256(convert_to(jsonb_build_object('environment',state->>'environment','dataset',state->>'resource','profile',state->'graphProfile','content',content)::text,'UTF8')),'hex');
 INSERT INTO moria.runtime_dataset_graph_nodes(id,environment,dataset,profile,content,kind,height,entries,logical_bytes) VALUES(node_id,state->>'environment',state->>'resource',state->'graphProfile',content,input->>'kind',(input->>'height')::integer,(input->>'entries')::bigint,(input->>'logicalBytes')::bigint) ON CONFLICT DO NOTHING;
 SELECT * INTO previous FROM moria.runtime_dataset_graph_nodes WHERE id=node_id;
 IF ROW(previous.environment,previous.dataset,previous.profile,previous.content,previous.kind,previous.height,previous.entries,previous.logical_bytes) IS DISTINCT FROM ROW(state->>'environment',state->>'resource',state->'graphProfile',content,input->>'kind',(input->>'height')::integer,(input->>'entries')::bigint,(input->>'logicalBytes')::bigint) THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 INSERT INTO moria.runtime_dataset_graph_edges(parent,label,child) SELECT node_id,value->>'label',value->>'node' FROM jsonb_array_elements(input->'edges') ON CONFLICT DO NOTHING;
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_edges e WHERE e.parent=node_id AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(input->'edges') v WHERE v->>'label'=e.label AND v->>'node'=e.child)) THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 INSERT INTO moria.runtime_dataset_graph_qualifications(node,physical_fingerprint,incarnation,physical_version,producer) VALUES(node_id,physical,inc,input->'physicalVersion',producer_key) ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_qualifications q WHERE q.node=node_id AND q.physical_fingerprint=physical AND q.incarnation=inc AND q.physical_version=input->'physicalVersion') THEN RAISE EXCEPTION 'GC_OBJECT_OWNERSHIP_CHANGED';END IF;
 INSERT INTO moria.runtime_dataset_graph_admissions(producer,node,physical_fingerprint,incarnation,origin,source_root)
  SELECT producer_key,node_id,physical,inc,'expectedHeadBytes',a.root FROM moria.runtime_dataset_graph_byte_admissions a
   WHERE a.producer=producer_key AND a.physical_fingerprint=physical AND a.incarnation=inc AND a.content=content AND a.physical_version=input->'physicalVersion' ON CONFLICT DO NOTHING;
 RETURN node_id;
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_walk(start_node text,path text[],physical text,inc uuid) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE current_node text:=start_node;walk_label text;
BEGIN
 IF path IS NULL OR cardinality(path)>128 THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 FOREACH walk_label IN ARRAY path LOOP
  SELECT child INTO current_node FROM moria.runtime_dataset_graph_edges WHERE parent=current_node AND runtime_dataset_graph_edges.label=walk_label;
  IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_qualifications WHERE node=current_node AND physical_fingerprint=physical AND incarnation=inc) THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 END LOOP;
 RETURN current_node;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_graph_walk(text,text[],text,uuid) FROM PUBLIC;
CREATE FUNCTION moria.runtime_dataset_graph_reuse_head(claims jsonb,path text[],wanted jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;head moria.dataset_heads;origin moria.runtime_dataset_graph_roots;selected text;q moria.runtime_dataset_graph_qualifications;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 SELECT * INTO head FROM moria.dataset_heads WHERE environment=state->>'environment' AND dataset=state->>'resource' FOR SHARE;
 IF NOT FOUND OR head.revision::text IS DISTINCT FROM state->>'expectedRevision' THEN RAISE EXCEPTION 'HEAD_CONFLICT';END IF;
 PERFORM moria.runtime_dataset_root_assert(head.metadata_location);
 SELECT * INTO origin FROM moria.runtime_dataset_graph_roots WHERE root=moria.runtime_dataset_root_key(head.metadata_location) AND physical_fingerprint=state->>'physicalFingerprint' AND incarnation=(state->>'incarnation')::uuid AND profile=state->'graphProfile';
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_WRITE_CAPABILITY_REQUIRED';END IF;
 selected:=moria.runtime_dataset_graph_walk(origin.node,path,origin.physical_fingerprint,origin.incarnation);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_nodes n WHERE n.id=selected AND n.content=wanted AND n.profile=state->'graphProfile') THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_graph_admissions(producer,node,physical_fingerprint,incarnation,origin,source_root) VALUES(state->>'producer',selected,origin.physical_fingerprint,origin.incarnation,'expectedHead',origin.root) ON CONFLICT DO NOTHING;
 SELECT * INTO q FROM moria.runtime_dataset_graph_qualifications WHERE node=selected AND physical_fingerprint=origin.physical_fingerprint AND incarnation=origin.incarnation;
 RETURN (SELECT jsonb_build_object('node',selected,'physicalVersion',q.physical_version,'profile',state->'graphProfile','content',n.content,'kind',n.kind,'height',n.height,'entries',n.entries,'logicalBytes',n.logical_bytes) FROM moria.runtime_dataset_graph_nodes n WHERE n.id=selected);
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_stage(claims jsonb,stage_id uuid,root_node text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;s moria.runtime_dataset_graph_stages;added integer;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id WHERE n.id=root_node AND n.kind='root' AND n.environment=state->>'environment' AND n.dataset=state->>'resource' AND n.profile=state->'graphProfile' AND q.physical_fingerprint=state->>'physicalFingerprint' AND q.incarnation=(state->>'incarnation')::uuid AND q.producer=state->>'producer') THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_graph_stages(id,producer,environment,dataset,node,physical_fingerprint,incarnation) VALUES(stage_id,state->>'producer',state->>'environment',state->>'resource',root_node,state->>'physicalFingerprint',(state->>'incarnation')::uuid) ON CONFLICT DO NOTHING;
 SELECT * INTO s FROM moria.runtime_dataset_graph_stages WHERE id=stage_id FOR UPDATE;
 IF ROW(s.producer,s.node,s.physical_fingerprint,s.incarnation) IS DISTINCT FROM ROW(state->>'producer',root_node,state->>'physicalFingerprint',(state->>'incarnation')::uuid) OR s.status NOT IN ('preparing','ready') THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 INSERT INTO moria.runtime_dataset_graph_references(kind,owner,node,physical_fingerprint,incarnation) VALUES('stage',stage_id::text,root_node,s.physical_fingerprint,s.incarnation) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS added=ROW_COUNT;
 IF added=1 THEN INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta) VALUES(s.environment,s.dataset,stage_id,root_node,s.physical_fingerprint,s.incarnation,1);END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_step(origin_id uuid,maximum integer) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE event moria.runtime_dataset_graph_queue;previous bigint;next_count bigint;done integer:=0;
BEGIN
 IF maximum NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT';END IF;
 FOR event IN SELECT * FROM moria.runtime_dataset_graph_queue WHERE origin=origin_id AND processed_at IS NULL ORDER BY sequence LIMIT maximum FOR UPDATE SKIP LOCKED LOOP
  INSERT INTO moria.runtime_dataset_graph_counts(node,physical_fingerprint,incarnation) VALUES(event.node,event.physical_fingerprint,event.incarnation) ON CONFLICT DO NOTHING;
  SELECT live_references INTO previous FROM moria.runtime_dataset_graph_counts WHERE node=event.node AND physical_fingerprint=event.physical_fingerprint AND incarnation=event.incarnation FOR UPDATE;
  next_count:=previous+event.delta;IF next_count<0 THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
  UPDATE moria.runtime_dataset_graph_counts SET live_references=next_count WHERE node=event.node AND physical_fingerprint=event.physical_fingerprint AND incarnation=event.incarnation;
  IF (previous=0 AND next_count=1) OR (previous=1 AND next_count=0) THEN
   INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta)
    SELECT event.environment,event.dataset,event.origin,e.child,event.physical_fingerprint,event.incarnation,event.delta FROM moria.runtime_dataset_graph_edges e WHERE e.parent=event.node ORDER BY e.label;
  END IF;
  UPDATE moria.runtime_dataset_graph_queue SET processed_at=clock_timestamp() WHERE sequence=event.sequence;
  done:=done+1;
 END LOOP;
 RETURN done;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_graph_step(uuid,integer) FROM PUBLIC;
CREATE FUNCTION moria.runtime_dataset_graph_advance(claims jsonb,stage_id uuid,maximum integer) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;s moria.runtime_dataset_graph_stages;changed integer;remaining boolean;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 SELECT * INTO s FROM moria.runtime_dataset_graph_stages WHERE id=stage_id FOR UPDATE;
 IF NOT FOUND OR s.producer<>state->>'producer' OR s.incarnation<>(state->>'incarnation')::uuid OR s.status NOT IN ('preparing','ready','abandoned','published') THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 changed:=moria.runtime_dataset_graph_step(stage_id,maximum);
 remaining:=EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE origin=stage_id AND processed_at IS NULL);
 IF NOT remaining AND s.status IN ('preparing','ready') THEN UPDATE moria.runtime_dataset_graph_stages SET status='ready' WHERE id=stage_id;END IF;
 RETURN jsonb_build_object('changed',changed,'ready',NOT remaining);
END $$;

CREATE FUNCTION moria.runtime_dataset_graph_task_root(claims jsonb,root_node text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;q moria.runtime_dataset_graph_qualifications;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 IF claims->>'kind'<>'execution' OR claims->>'task' IS NULL THEN RAISE EXCEPTION 'TASK_REQUIRED';END IF;
 SELECT q0.* INTO q FROM moria.runtime_dataset_graph_qualifications q0 JOIN moria.runtime_dataset_graph_nodes n ON n.id=q0.node
  WHERE q0.node=root_node AND q0.physical_fingerprint=state->>'physicalFingerprint' AND q0.incarnation=(state->>'incarnation')::uuid AND q0.producer=state->>'producer' AND n.kind='partset' AND n.profile=state->'graphProfile';
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_graph_task_roots(task,owner,fence,resource,node,physical_fingerprint,incarnation)
  VALUES((claims->>'task')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,state->>'resource',root_node,q.physical_fingerprint,q.incarnation) ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_task_roots WHERE task=(claims->>'task')::uuid AND owner=(claims->>'owner')::uuid AND fence=(claims->>'fence')::bigint AND resource=state->>'resource' AND node=root_node AND physical_fingerprint=q.physical_fingerprint AND incarnation=q.incarnation) THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_reuse_task(claims jsonb,source_task uuid,path text[],wanted jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;task_row moria.runtime_execution_tasks;origin moria.runtime_dataset_graph_task_roots;selected text;q moria.runtime_dataset_graph_qualifications;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 IF claims->>'kind'<>'execution' OR claims->>'task' IS NOT NULL THEN RAISE EXCEPTION 'COORDINATOR_REQUIRED';END IF;
 SELECT * INTO task_row FROM moria.runtime_execution_tasks WHERE id=source_task AND execution=(claims->>'execution')::uuid AND status='sealed' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'OUTPUT_ATTEMPT_CHANGED';END IF;
 SELECT r.* INTO origin FROM moria.runtime_dataset_graph_task_roots r JOIN moria.runtime_dataset_graph_nodes n ON n.id=r.node
  WHERE r.task=source_task AND r.owner=task_row.owner AND r.fence=task_row.fence AND r.resource=state->>'resource' AND r.physical_fingerprint=state->>'physicalFingerprint' AND r.incarnation=(state->>'incarnation')::uuid AND n.content=task_row.result->'datasetOutputs'->(state->>'resource') AND n.profile=state->'graphProfile';
 IF NOT FOUND THEN RAISE EXCEPTION 'OUTPUT_ATTEMPT_CHANGED';END IF;
 selected:=moria.runtime_dataset_graph_walk(origin.node,path,origin.physical_fingerprint,origin.incarnation);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_nodes n WHERE n.id=selected AND n.content=wanted AND n.profile=state->'graphProfile') THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_graph_admissions(producer,node,physical_fingerprint,incarnation,origin,source_root) VALUES(state->>'producer',selected,origin.physical_fingerprint,origin.incarnation,'sealedTask',origin.node) ON CONFLICT DO NOTHING;
 SELECT * INTO q FROM moria.runtime_dataset_graph_qualifications WHERE node=selected AND physical_fingerprint=origin.physical_fingerprint AND incarnation=origin.incarnation;
 RETURN (SELECT jsonb_build_object('node',selected,'physicalVersion',q.physical_version,'profile',state->'graphProfile','content',n.content,'kind',n.kind,'height',n.height,'entries',n.entries,'logicalBytes',n.logical_bytes) FROM moria.runtime_dataset_graph_nodes n WHERE n.id=selected);
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_attach(claims jsonb,stage_id uuid,candidate_id uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;s moria.runtime_dataset_graph_stages;c moria.runtime_snapshot_candidates;n moria.runtime_dataset_graph_nodes;reference jsonb;root_key text;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 IF claims->>'kind'<>'execution' OR claims->>'task' IS NOT NULL THEN RAISE EXCEPTION 'COORDINATOR_REQUIRED';END IF;
 SELECT * INTO s FROM moria.runtime_dataset_graph_stages WHERE id=stage_id FOR UPDATE;
 IF NOT FOUND OR s.status NOT IN ('ready','candidate') OR s.producer<>state->>'producer' OR s.incarnation<>(state->>'incarnation')::uuid OR s.physical_fingerprint<>state->>'physicalFingerprint'
  OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE origin=stage_id AND processed_at IS NULL) THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
 SELECT * INTO c FROM moria.runtime_snapshot_candidates WHERE id=candidate_id AND execution=(claims->>'execution')::uuid AND owner=(claims->>'owner')::uuid AND fence=(claims->>'fence')::bigint AND incarnation=s.incarnation AND resource=state->>'resource';
 IF NOT FOUND OR c.metadata_location NOT LIKE 'moria-snapshot-v3:%' THEN RAISE EXCEPTION 'DATASET_PUBLICATION_CONTRACT_REQUIRED';END IF;
 reference:=substring(c.metadata_location FROM length('moria-snapshot-v3:')+1)::jsonb;
 SELECT * INTO n FROM moria.runtime_dataset_graph_nodes WHERE id=s.node;
 IF n.content IS DISTINCT FROM reference->'root' OR reference->>'version'<>'3' THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 root_key:=moria.runtime_dataset_root_key(c.metadata_location);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots WHERE key=root_key AND sealed AND producer=s.producer) THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
 INSERT INTO moria.runtime_dataset_graph_roots(root,node,physical_fingerprint,incarnation,profile) VALUES(root_key,s.node,s.physical_fingerprint,s.incarnation,state->'graphProfile') ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_roots WHERE root=root_key AND node=s.node AND physical_fingerprint=s.physical_fingerprint AND incarnation=s.incarnation) THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 IF s.candidate IS NOT NULL AND (s.candidate<>candidate_id OR s.root<>root_key) THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 UPDATE moria.runtime_dataset_graph_stages SET candidate=candidate_id,root=root_key,status='candidate' WHERE id=stage_id;
END $$;
CREATE FUNCTION moria.runtime_dataset_graph_publish() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE s moria.runtime_dataset_graph_stages;source moria.runtime_dataset_graph_references;added integer;
BEGIN
 IF NEW.metadata_location IS NULL OR NEW.metadata_location NOT LIKE 'moria-snapshot-v3:%' THEN RETURN NEW;END IF;
 SELECT stage.* INTO s FROM moria.runtime_dataset_graph_stages stage JOIN moria.runtime_snapshot_candidates c ON c.id=stage.candidate
 WHERE stage.root=moria.runtime_dataset_root_key(NEW.metadata_location) AND stage.environment=NEW.environment AND stage.dataset=NEW.dataset AND stage.status IN ('candidate','published')
  AND c.expected_revision=NEW.revision-1 AND c.environment=NEW.environment AND c.resource=NEW.dataset AND c.metadata_location=NEW.metadata_location
 ORDER BY stage.created_at DESC LIMIT 1 FOR UPDATE OF stage;
 IF NOT FOUND OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE origin=s.id AND processed_at IS NULL) THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
 IF s.status='published' THEN RETURN NEW;END IF;
 SELECT * INTO source FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=s.id::text AND node=s.node AND physical_fingerprint=s.physical_fingerprint AND incarnation=s.incarnation FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
 INSERT INTO moria.runtime_dataset_graph_references(kind,owner,node,physical_fingerprint,incarnation) VALUES('history',s.root,s.node,s.physical_fingerprint,s.incarnation) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS added=ROW_COUNT;
 DELETE FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=s.id::text AND physical_fingerprint=s.physical_fingerprint AND incarnation=s.incarnation;
 IF added=0 THEN INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta) VALUES(s.environment,s.dataset,s.id,s.node,s.physical_fingerprint,s.incarnation,-1);END IF;
 UPDATE moria.runtime_dataset_graph_stages SET status='published' WHERE id=s.id;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER runtime_dataset_graph_publish AFTER INSERT OR UPDATE ON moria.dataset_heads DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_graph_publish();
CREATE FUNCTION moria.runtime_dataset_graph_gc_busy(env text,resource text,physical text,inc uuid) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
 SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE environment=env AND dataset=resource AND physical_fingerprint=physical AND incarnation=inc AND processed_at IS NULL) OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_retirements WHERE environment=env AND dataset=resource AND physical_fingerprint=physical AND incarnation=inc AND NOT completed)
$$;
CREATE FUNCTION moria.runtime_dataset_graph_expire() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE r moria.runtime_dataset_roots;policy moria.runtime_dataset_retention_policies;
BEGIN
 SELECT * INTO policy FROM moria.runtime_dataset_retention_policies WHERE id=NEW.policy;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_roots WHERE root=NEW.root AND physical_fingerprint=policy.physical_fingerprint AND incarnation=policy.incarnation) THEN RETURN NEW;END IF;
 SELECT * INTO r FROM moria.runtime_dataset_roots WHERE key=NEW.root;
 IF moria.runtime_dataset_root_pinned_except(NEW.root,NEW.maintenance) THEN RAISE EXCEPTION 'GC_REFERENCE_PRESENT';END IF;
 -- Closed unpublished stages retire through their own queue, not a history maintenance run.
 IF NOT EXISTS(SELECT 1 FROM moria.snapshot_history h WHERE moria.runtime_dataset_root_key(h.metadata_location)=r.key AND h.metadata_location=r.metadata_location)
  AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_references WHERE kind='history' AND owner=r.key AND physical_fingerprint=policy.physical_fingerprint AND incarnation=policy.incarnation) THEN
  RETURN NEW;
 END IF;
 INSERT INTO moria.runtime_dataset_graph_retirements(root,maintenance,environment,dataset,physical_fingerprint,incarnation)
 VALUES(NEW.root,NEW.maintenance,r.environment,r.dataset,policy.physical_fingerprint,policy.incarnation) ON CONFLICT DO NOTHING;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_graph_expire AFTER INSERT ON moria.runtime_dataset_expired_roots FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_graph_expire();
CREATE FUNCTION moria.runtime_dataset_graph_retirement_advance(policy_id uuid,instance uuid,origin_id uuid,maximum integer) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;retirement moria.runtime_dataset_graph_retirements;reference moria.runtime_dataset_graph_references;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 IF maximum NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT';END IF;
 IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('moria/recovery-retention/v1',0)) THEN RAISE EXCEPTION 'GC_ADMISSION_BUSY';END IF;
 PERFORM 1 FROM moria.indexing_instance WHERE incarnation=instance FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'GC_LEASE_LOST';END IF;
 state:=moria.runtime_dataset_policy_check(policy_id);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_maintenance_runs m JOIN moria.runtime_dataset_retention_policies p ON p.id=m.policy
   WHERE m.id=origin_id AND m.incarnation=instance AND m.status='committed' AND p.environment=state->>'environment' AND p.dataset=state->>'dataset' AND p.physical_fingerprint=state->>'physicalFingerprint' AND p.incarnation=instance)
  OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_retirements WHERE maintenance=origin_id AND (environment<>state->>'environment' OR dataset<>state->>'dataset' OR physical_fingerprint<>state->>'physicalFingerprint' OR incarnation<>instance))
  OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE origin=origin_id AND processed_at IS NULL AND (environment<>state->>'environment' OR dataset<>state->>'dataset' OR physical_fingerprint<>state->>'physicalFingerprint' OR incarnation<>instance)) THEN RAISE EXCEPTION 'GC_POLICY_CHANGED';END IF;
 FOR retirement IN SELECT * FROM moria.runtime_dataset_graph_retirements WHERE maintenance=origin_id AND NOT completed ORDER BY root LIMIT 1 FOR UPDATE LOOP
  FOR reference IN SELECT * FROM moria.runtime_dataset_graph_references WHERE kind='history' AND owner=retirement.root AND physical_fingerprint=retirement.physical_fingerprint AND incarnation=retirement.incarnation ORDER BY node LIMIT 64 FOR UPDATE LOOP
   DELETE FROM moria.runtime_dataset_graph_references WHERE kind=reference.kind AND owner=reference.owner AND node=reference.node AND physical_fingerprint=reference.physical_fingerprint AND incarnation=reference.incarnation;
   INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta) VALUES(retirement.environment,retirement.dataset,origin_id,reference.node,reference.physical_fingerprint,reference.incarnation,-1);
  END LOOP;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_references WHERE kind='history' AND owner=retirement.root AND physical_fingerprint=retirement.physical_fingerprint AND incarnation=retirement.incarnation) THEN UPDATE moria.runtime_dataset_graph_retirements SET completed=true WHERE root=retirement.root AND physical_fingerprint=retirement.physical_fingerprint AND incarnation=retirement.incarnation;END IF;
 END LOOP;
 RETURN moria.runtime_dataset_graph_step(origin_id,maximum);
END $$;

-- Read authority is resolved from an actual accepted input/expected head or indexing build.
-- It never reserves a producer or adds qualifications/admissions.
CREATE FUNCTION moria.runtime_dataset_graph_read(claims jsonb,path text[]) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;binding jsonb;graph_profile jsonb;execution_state moria.runtime_executions;build moria.indexing_builds;history moria.snapshot_history;origin moria.runtime_dataset_graph_roots;node moria.runtime_dataset_graph_nodes;qualification moria.runtime_dataset_graph_qualifications;resource_id text;env text;instance_inc uuid;physical text;chosen text;table_uuid text;retained moria.runtime_dataset_roots;catalog_version integer;stage moria.runtime_dataset_graph_stages;stage_state jsonb;root_node text;
BEGIN
 IF claims->>'kind'='execution' THEN
  state:=moria.runtime_dataset_io_check(claims->>'session',(claims->>'execution')::uuid,(claims->>'task')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid,claims->>'warehouse',claims->>'table',claims->>'revision',false);
  SELECT * INTO execution_state FROM moria.runtime_executions WHERE id=(claims->>'execution')::uuid;
  resource_id:=state->>'resource';env:=state->>'environment';instance_inc:=(claims->>'incarnation')::uuid;
  IF claims->>'basis'='stage' THEN
   IF claims->>'task' IS NOT NULL THEN RAISE EXCEPTION 'COORDINATOR_REQUIRED';END IF;
   stage_state:=moria.runtime_dataset_graph_scope(claims);
   SELECT s.* INTO stage FROM moria.runtime_dataset_graph_stages s WHERE s.id=(claims->>'stage')::uuid
    AND s.producer=stage_state->>'producer' AND s.environment=env AND s.dataset=resource_id
    AND s.physical_fingerprint=stage_state->>'physicalFingerprint' AND s.incarnation=instance_inc AND s.status IN ('ready','candidate');
   IF NOT FOUND OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue q WHERE q.origin=stage.id AND q.processed_at IS NULL) THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
   IF NOT EXISTS(SELECT 1 FROM moria.dataset_heads h WHERE h.environment=env AND h.dataset=resource_id AND h.revision::text=stage_state->>'expectedRevision') THEN RAISE EXCEPTION 'HEAD_CONFLICT';END IF;
   SELECT r.* INTO retained FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_graph_nodes n ON n.content=r.root->'root'
    WHERE n.id=stage.node AND n.kind='root' AND n.profile=stage_state->'graphProfile' AND r.producer=stage.producer AND r.environment=env AND r.dataset=resource_id AND r.sealed
     AND r.root->>'version'='3' AND r.reader_profile='iceberg-v2-native-mor-bounded-v3';
   IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
   history.environment:=env;history.dataset:=resource_id;history.snapshot_id:=retained.snapshot_id;history.metadata_location:=retained.metadata_location;
  ELSIF claims->>'basis'='input' THEN
   SELECT h.* INTO history FROM moria.snapshot_history h JOIN LATERAL jsonb_array_elements(execution_state.spec->'pinnedInputs') pin ON pin->>'resource'=h.dataset AND pin->>'revision'=h.revision::text AND pin->>'snapshot'=h.snapshot_id
    WHERE h.environment=env AND h.dataset=resource_id;
  ELSIF claims->>'basis'='expectedHead' THEN
   SELECT h.* INTO history FROM moria.snapshot_history h JOIN moria.dataset_heads current_head USING(environment,dataset,revision)
    WHERE h.environment=env AND h.dataset=resource_id AND h.revision::text=execution_state.spec->'expectedHeads'->>resource_id;
  ELSE RAISE EXCEPTION 'UNDECLARED_DATASET';END IF;
  IF history.dataset IS NULL THEN RAISE EXCEPTION 'INPUT_PIN_CHANGED';END IF;
  binding:=state->'datasetBinding';table_uuid:=state->>'icebergUuid';
 ELSIF claims->>'kind'='maintenance' THEN
  IF claims->>'basis' IS DISTINCT FROM 'expectedHead' THEN RAISE EXCEPTION 'UNDECLARED_DATASET';END IF;
  state:=moria.runtime_dataset_maintenance_check((claims->>'run')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,(claims->>'incarnation')::uuid);
  env:=state->>'environment';resource_id:=state->>'dataset';instance_inc:=(claims->>'incarnation')::uuid;
  SELECT h.* INTO history FROM moria.snapshot_history h JOIN moria.dataset_heads current_head USING(environment,dataset,revision)
   WHERE h.environment=env AND h.dataset=resource_id AND h.revision::text=state->>'expectedRevision' AND h.metadata_location=state->>'oldMetadata';
  IF NOT FOUND THEN RAISE EXCEPTION 'HEAD_CONFLICT';END IF;
  binding:=moria.runtime_dataset_binding(env,resource_id,state->'namespace'->>'warehouseId',state->'namespace'->>'tableId',state->'namespace'->>'revision');
  table_uuid:=state->>'tableUuid';
 ELSIF claims->>'kind'='indexing' THEN
  PERFORM moria.native_storage_check((claims->>'build')::uuid,(claims->>'task')::uuid,(claims->>'owner')::uuid,(claims->>'fence')::bigint,claims->>'warehouse',claims->>'table',claims->>'revision',false);
  SELECT * INTO build FROM moria.indexing_builds WHERE id=(claims->>'build')::uuid;
  resource_id:=build.source;env:=build.environment;instance_inc:=(build.definition->>'incarnation')::uuid;
  SELECT * INTO history FROM moria.snapshot_history h WHERE h.environment=env AND h.dataset=resource_id AND h.revision=build.source_revision AND h.snapshot_id=build.snapshot_id AND h.metadata_location=build.metadata_location;
  IF NOT FOUND THEN RAISE EXCEPTION 'INPUT_PIN_CHANGED';END IF;
  binding:=moria.runtime_dataset_binding(env,resource_id,claims->>'warehouse',claims->>'table',claims->>'revision');
  SELECT iceberg_uuid::text INTO table_uuid FROM moria.native_storage_namespaces WHERE warehouse_id=claims->>'warehouse' AND table_id=claims->>'table' AND revision=claims->>'revision' AND environment=env AND resource=resource_id AND enabled;
 ELSE RAISE EXCEPTION 'UNDECLARED_DATASET';END IF;
 IF binding IS NULL OR binding='null'::jsonb THEN RAISE EXCEPTION 'DATASET_WRITE_CAPABILITY_REQUIRED';END IF;
 PERFORM moria.runtime_dataset_root_assert(history.metadata_location);
 physical:=binding->>'physicalBindingFingerprint';
 catalog_version:=CASE WHEN history.metadata_location LIKE 'moria-snapshot-v3:%' THEN 3 WHEN history.metadata_location LIKE 'moria-snapshot-v2:%' THEN 2 ELSE NULL END;
 SELECT r.* INTO retained FROM moria.runtime_dataset_roots r
  WHERE r.key=moria.runtime_dataset_root_key(history.metadata_location) AND r.environment=env AND r.dataset=resource_id
   AND r.metadata_location=history.metadata_location AND r.snapshot_id=history.snapshot_id AND r.sealed
   AND r.root->>'version'=catalog_version::text
   AND r.reader_profile=CASE catalog_version WHEN 2 THEN 'iceberg-v2-cow-flat-bounded-v2' WHEN 3 THEN 'iceberg-v2-native-mor-bounded-v3' ELSE NULL END
   AND EXISTS(SELECT 1 FROM moria.runtime_dataset_root_locations l WHERE l.root=r.key AND l.sealed AND l.physical_fingerprint=physical AND l.incarnation=instance_inc);
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 graph_profile:=jsonb_build_object('version',3,'readerProfile','iceberg-v2-native-mor-bounded-v3','directoryProfile','sha256-radix16-v1','chunkProfiles',jsonb_build_array('moria-sha256-chunks-1m-v1','moria-sha256-chunks-64k-v1'),'tableUuid',table_uuid,'schemaIdentity',encode(sha256(convert_to((binding->'icebergSchema')::text,'UTF8')),'hex'),'keySchemaHash',binding->>'keySchemaHash');
 IF stage.id IS NOT NULL THEN
  IF stage_state->'graphProfile' IS DISTINCT FROM graph_profile THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
  root_node:=stage.node;
 ELSE
  SELECT * INTO origin FROM moria.runtime_dataset_graph_roots WHERE root=moria.runtime_dataset_root_key(history.metadata_location) AND physical_fingerprint=physical AND incarnation=instance_inc AND profile=graph_profile;
  IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_WRITE_CAPABILITY_REQUIRED';END IF;
  root_node:=origin.node;
 END IF;
 chosen:=moria.runtime_dataset_graph_walk(root_node,path,physical,instance_inc);
 SELECT * INTO node FROM moria.runtime_dataset_graph_nodes WHERE id=chosen AND environment=env AND dataset=resource_id AND runtime_dataset_graph_nodes.profile=graph_profile;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 SELECT * INTO qualification FROM moria.runtime_dataset_graph_qualifications WHERE runtime_dataset_graph_qualifications.node=chosen AND physical_fingerprint=physical AND incarnation=instance_inc;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 PERFORM moria.runtime_dataset_object_read(node.content->'object'->>'warehouseId',node.content->'object'->>'tableId',node.content->'object'->>'objectId');
 RETURN jsonb_build_object('identity',jsonb_build_object('environment',env,'dataset',resource_id,'revision',history.revision::text,'snapshot',history.snapshot_id,'metadataLocation',history.metadata_location,'catalogVersion',catalog_version,'readerProfile',retained.reader_profile,'root',root_node,'stage',stage.id,'basisRevision',stage_state->>'expectedRevision','physicalFingerprint',physical,'incarnation',instance_inc,'profile',graph_profile),
  'node',jsonb_build_object('node',chosen,'content',node.content,'physicalVersion',qualification.physical_version,'kind',node.kind,'height',node.height,'entries',node.entries,'logicalBytes',node.logical_bytes,'profile',graph_profile));
END $$;

CREATE FUNCTION moria.runtime_dataset_graph_known(claims jsonb,wanted jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;selected moria.runtime_dataset_graph_nodes;q moria.runtime_dataset_graph_qualifications;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 SELECT n.* INTO selected FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications qualification ON qualification.node=n.id
  WHERE n.environment=state->>'environment' AND n.dataset=state->>'resource' AND n.profile=state->'graphProfile' AND n.content=wanted
   AND qualification.physical_fingerprint=state->>'physicalFingerprint' AND qualification.incarnation=(state->>'incarnation')::uuid
   AND (qualification.producer=state->>'producer' OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_admissions admission WHERE admission.producer=state->>'producer' AND admission.node=n.id AND admission.physical_fingerprint=qualification.physical_fingerprint AND admission.incarnation=qualification.incarnation));
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT * INTO q FROM moria.runtime_dataset_graph_qualifications WHERE node=selected.id AND physical_fingerprint=state->>'physicalFingerprint' AND incarnation=(state->>'incarnation')::uuid;
 RETURN jsonb_build_object('node',selected.id,'physicalVersion',q.physical_version,'profile',state->'graphProfile','content',selected.content,'kind',selected.kind,'height',selected.height,'entries',selected.entries,'logicalBytes',selected.logical_bytes);
END $$;

CREATE FUNCTION moria.runtime_dataset_graph_child(claims jsonb,parent_id text,label_name text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;parent moria.runtime_dataset_graph_nodes;selected moria.runtime_dataset_graph_nodes;q moria.runtime_dataset_graph_qualifications;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 SELECT n.* INTO parent FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications qualification ON qualification.node=n.id
 WHERE n.id=parent_id AND n.environment=state->>'environment' AND n.dataset=state->>'resource' AND n.profile=state->'graphProfile'
  AND qualification.physical_fingerprint=state->>'physicalFingerprint' AND qualification.incarnation=(state->>'incarnation')::uuid
  AND (qualification.producer=state->>'producer' OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_admissions a WHERE a.producer=state->>'producer' AND a.node=n.id AND a.physical_fingerprint=qualification.physical_fingerprint AND a.incarnation=qualification.incarnation));
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 SELECT n.* INTO selected FROM moria.runtime_dataset_graph_edges edge JOIN moria.runtime_dataset_graph_nodes n ON n.id=edge.child
 WHERE edge.parent=parent_id AND edge.label=label_name AND n.environment=parent.environment AND n.dataset=parent.dataset AND n.profile=parent.profile;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 SELECT qualification.* INTO q FROM moria.runtime_dataset_graph_qualifications qualification WHERE qualification.node=selected.id AND qualification.physical_fingerprint=state->>'physicalFingerprint' AND qualification.incarnation=(state->>'incarnation')::uuid;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_graph_admissions(producer,node,physical_fingerprint,incarnation,origin,source_root) VALUES(state->>'producer',selected.id,q.physical_fingerprint,q.incarnation,'qualifiedChild',parent_id) ON CONFLICT DO NOTHING;
 RETURN jsonb_build_object('node',selected.id,'physicalVersion',q.physical_version,'profile',state->'graphProfile','content',selected.content,'kind',selected.kind,'height',selected.height,'entries',selected.entries,'logicalBytes',selected.logical_bytes);
END $$;

CREATE FUNCTION moria.runtime_dataset_graph_bootstrap_member(claims jsonb,wanted jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;head moria.dataset_heads;member record;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 SELECT * INTO head FROM moria.dataset_heads WHERE environment=state->>'environment' AND dataset=state->>'resource' FOR SHARE;
 IF NOT FOUND OR head.revision::text IS DISTINCT FROM state->>'expectedRevision' THEN RAISE EXCEPTION 'HEAD_CONFLICT';END IF;
 PERFORM moria.runtime_dataset_root_assert(head.metadata_location);
 SELECT m.content,m.physical_version INTO member FROM moria.runtime_dataset_member_locations m JOIN moria.runtime_dataset_root_locations root ON root.root=m.root AND root.physical_fingerprint=m.physical_fingerprint AND root.incarnation=m.incarnation
  JOIN moria.runtime_dataset_roots retained ON retained.key=m.root
  WHERE m.root=moria.runtime_dataset_root_key(head.metadata_location) AND m.physical_fingerprint=state->>'physicalFingerprint' AND m.incarnation=(state->>'incarnation')::uuid AND root.sealed
  AND retained.sealed AND retained.environment=state->>'environment' AND retained.dataset=state->>'resource' AND retained.snapshot_id=head.snapshot_id AND retained.reader_profile='iceberg-v2-cow-flat-bounded-v2'
  AND m.warehouse_id=state->'namespace'->>'warehouseId' AND m.table_id=state->'namespace'->>'tableId' AND m.object_id=wanted->'object'->>'objectId' AND m.content=wanted LIMIT 1;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_ANCESTRY_REQUIRED';END IF;
 INSERT INTO moria.runtime_dataset_graph_byte_admissions(producer,root,physical_fingerprint,incarnation,content,physical_version)
 VALUES(state->>'producer',moria.runtime_dataset_root_key(head.metadata_location),state->>'physicalFingerprint',(state->>'incarnation')::uuid,member.content,member.physical_version) ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_byte_admissions a WHERE a.producer=state->>'producer' AND a.root=moria.runtime_dataset_root_key(head.metadata_location) AND a.physical_fingerprint=state->>'physicalFingerprint' AND a.incarnation=(state->>'incarnation')::uuid AND a.content=member.content AND a.physical_version=member.physical_version) THEN RAISE EXCEPTION 'GC_OBJECT_OWNERSHIP_CHANGED';END IF;
 RETURN jsonb_build_object('root',moria.runtime_dataset_root_key(head.metadata_location),'physicalFingerprint',state->>'physicalFingerprint','physicalVersion',member.physical_version,'content',member.content,'incarnation',state->>'incarnation');
END $$;

CREATE FUNCTION moria.runtime_dataset_graph_bootstrap_finish(claims jsonb,stage_id uuid) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;stage moria.runtime_dataset_graph_stages;head moria.dataset_heads;key text;chosen text;added integer;
BEGIN
 state:=moria.runtime_dataset_graph_scope(claims);
 SELECT * INTO head FROM moria.dataset_heads WHERE environment=state->>'environment' AND dataset=state->>'resource' FOR SHARE;
 IF NOT FOUND OR head.revision::text IS DISTINCT FROM state->>'expectedRevision' THEN RAISE EXCEPTION 'HEAD_CONFLICT';END IF;
 PERFORM moria.runtime_dataset_root_assert(head.metadata_location);key:=moria.runtime_dataset_root_key(head.metadata_location);
 SELECT * INTO stage FROM moria.runtime_dataset_graph_stages WHERE id=stage_id FOR UPDATE;
 IF NOT FOUND OR stage.producer<>state->>'producer' OR stage.physical_fingerprint<>state->>'physicalFingerprint' OR stage.incarnation<>(state->>'incarnation')::uuid OR stage.status NOT IN ('ready','published') OR stage.candidate IS NOT NULL OR EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_queue WHERE origin=stage_id AND processed_at IS NULL) THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_nodes WHERE id=stage.node AND profile=state->'graphProfile' AND kind='root') THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;
 INSERT INTO moria.runtime_dataset_graph_roots(root,node,physical_fingerprint,incarnation,profile) VALUES(key,stage.node,stage.physical_fingerprint,stage.incarnation,state->'graphProfile') ON CONFLICT DO NOTHING;
 SELECT node INTO chosen FROM moria.runtime_dataset_graph_roots WHERE root=key AND physical_fingerprint=stage.physical_fingerprint AND incarnation=stage.incarnation AND profile=state->'graphProfile';
 IF stage.status='published' THEN IF chosen<>stage.node OR stage.root<>key THEN RAISE EXCEPTION 'DATASET_GRAPH_INVALID';END IF;RETURN chosen;END IF;
 INSERT INTO moria.runtime_dataset_graph_references(kind,owner,node,physical_fingerprint,incarnation) SELECT 'history',key,stage.node,stage.physical_fingerprint,stage.incarnation WHERE chosen=stage.node ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS added=ROW_COUNT;
 DELETE FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=stage_id::text AND node=stage.node AND physical_fingerprint=stage.physical_fingerprint AND incarnation=stage.incarnation;
 IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_GRAPH_NOT_READY';END IF;
 IF added=0 THEN INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta) VALUES(stage.environment,stage.dataset,stage_id,stage.node,stage.physical_fingerprint,stage.incarnation,-1);END IF;
 UPDATE moria.runtime_dataset_graph_stages SET root=key,status=CASE WHEN chosen=stage.node THEN 'published' ELSE 'abandoned' END WHERE id=stage_id;
 RETURN chosen;
END $$;

CREATE FUNCTION moria.runtime_dataset_graph_object_live(env text,resource text,physical text,inc uuid,warehouse text,table_name text,object_name text) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
 SELECT moria.runtime_dataset_graph_gc_busy(env,resource,physical,inc) OR EXISTS(
 SELECT 1 FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_counts c ON c.node=n.id
 WHERE n.environment=env AND n.dataset=resource AND n.content->'object'->>'warehouseId'=warehouse AND n.content->'object'->>'tableId'=table_name AND n.content->'object'->>'objectId'=object_name AND c.physical_fingerprint=physical AND c.incarnation=inc AND c.live_references>0)
$$;
CREATE FUNCTION moria.runtime_dataset_graph_retire(policy_id uuid,instance uuid,maximum integer) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE state jsonb;origin uuid;stage moria.runtime_dataset_graph_stages;reference moria.runtime_dataset_graph_references;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 IF maximum NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'RESOURCE_LIMIT';END IF;
 state:=moria.runtime_dataset_policy_check(policy_id);
 PERFORM 1 FROM moria.indexing_instance WHERE incarnation=instance FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'GC_LEASE_LOST';END IF;
 SELECT s.* INTO stage FROM moria.runtime_dataset_graph_stages s WHERE s.environment=state->>'environment' AND s.dataset=state->>'dataset'
  AND s.physical_fingerprint=state->>'physicalFingerprint' AND s.incarnation=instance AND s.status IN ('preparing','ready','candidate') AND moria.runtime_dataset_producer_closed(s.producer)
  ORDER BY s.created_at,s.id LIMIT 1 FOR UPDATE;
 IF FOUND THEN
  DELETE FROM moria.runtime_dataset_graph_references WHERE kind='stage' AND owner=stage.id::text AND node=stage.node AND physical_fingerprint=stage.physical_fingerprint AND incarnation=stage.incarnation RETURNING * INTO reference;
  IF FOUND THEN INSERT INTO moria.runtime_dataset_graph_queue(environment,dataset,origin,node,physical_fingerprint,incarnation,delta) VALUES(stage.environment,stage.dataset,stage.id,stage.node,stage.physical_fingerprint,stage.incarnation,-1);END IF;
  UPDATE moria.runtime_dataset_graph_stages SET status='abandoned' WHERE id=stage.id;
  RETURN moria.runtime_dataset_graph_step(stage.id,maximum);
 END IF;
 SELECT q.origin INTO origin FROM moria.runtime_dataset_graph_queue q JOIN moria.runtime_dataset_graph_stages s ON s.id=q.origin
  WHERE q.environment=state->>'environment' AND q.dataset=state->>'dataset' AND q.physical_fingerprint=state->>'physicalFingerprint' AND q.incarnation=instance
   AND q.processed_at IS NULL AND s.status IN ('published','abandoned') ORDER BY q.sequence LIMIT 1;
 IF origin IS NOT NULL THEN RETURN moria.runtime_dataset_graph_step(origin,maximum);END IF;
 SELECT maintenance INTO origin FROM moria.runtime_dataset_graph_retirements WHERE environment=state->>'environment' AND dataset=state->>'dataset'
  AND physical_fingerprint=state->>'physicalFingerprint' AND incarnation=instance AND NOT completed ORDER BY maintenance LIMIT 1;
 IF origin IS NULL THEN
  SELECT q.origin INTO origin FROM moria.runtime_dataset_graph_queue q JOIN moria.runtime_dataset_maintenance_runs m ON m.id=q.origin JOIN moria.runtime_dataset_retention_policies p ON p.id=m.policy
   WHERE q.environment=state->>'environment' AND q.dataset=state->>'dataset' AND q.physical_fingerprint=state->>'physicalFingerprint' AND q.incarnation=instance
    AND q.processed_at IS NULL AND m.incarnation=instance AND m.status='committed' AND p.environment=q.environment AND p.dataset=q.dataset AND p.physical_fingerprint=q.physical_fingerprint AND p.incarnation=instance ORDER BY q.sequence LIMIT 1;
 END IF;
 IF origin IS NULL THEN RETURN 0;END IF;
 RETURN moria.runtime_dataset_graph_retirement_advance(policy_id,instance,origin,maximum);
END $$;
`;
