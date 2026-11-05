/** Registered engine profiles and private attempt outputs; Dataset publication remains atomic. */
export const pipelineRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_pipeline_destinations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment text NOT NULL REFERENCES moria.environments(id),resource uuid NOT NULL,
  revision text NOT NULL,endpoint_ref text NOT NULL CHECK(endpoint_ref~'^[A-Za-z_][A-Za-z0-9_]{0,127}$'),
  secret_ref text NOT NULL CHECK(secret_ref~'^[A-Za-z_][A-Za-z0-9_]{0,127}$'),
  credentials jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(credentials)='array' AND jsonb_array_length(credentials)<=8),
  methods text[] NOT NULL CHECK(cardinality(methods) BETWEEN 1 AND 2 AND methods<@ARRAY['GET','POST']),
  paths text[] NOT NULL CHECK(cardinality(paths) BETWEEN 1 AND 32),
  markings text[] NOT NULL CHECK(cardinality(markings)<=256),allowed_markings text[] NOT NULL CHECK(cardinality(allowed_markings)<=256),enabled boolean NOT NULL DEFAULT true,
  UNIQUE(environment,resource,revision)
);
CREATE TABLE moria.runtime_pipeline_profiles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment text NOT NULL REFERENCES moria.environments(id),
  resource uuid NOT NULL,revision text NOT NULL CHECK(octet_length(revision) BETWEEN 1 AND 128),
  domain text NOT NULL CHECK(domain IN ('transform','sync')),service_authority uuid NOT NULL,
  service_revision text NOT NULL,resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
  profile jsonb NOT NULL CHECK(jsonb_typeof(profile)='object' AND octet_length(profile::text)<=1048576),
  enabled boolean NOT NULL DEFAULT true,UNIQUE(environment,resource,revision),
  FOREIGN KEY(service_authority,environment,resource,domain,service_revision)
    REFERENCES moria.runtime_service_targets(authority,environment,resource,domain,binding_revision)
);
CREATE UNIQUE INDEX runtime_pipeline_current_profile ON moria.runtime_pipeline_profiles(environment,resource) WHERE enabled;
CREATE TABLE moria.runtime_pipeline_executions (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),profile uuid NOT NULL REFERENCES moria.runtime_pipeline_profiles(id),
  entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),release text NOT NULL REFERENCES moria.fs_source_releases(digest),
  definition jsonb NOT NULL,code_resource uuid,inputs jsonb NOT NULL,outputs jsonb NOT NULL,configuration jsonb NOT NULL,
  checkpoint jsonb,destination_fingerprint text,requester jsonb NOT NULL,markings jsonb NOT NULL,
  secret_approvals jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(secret_approvals)='array' AND jsonb_array_length(secret_approvals)<=8),
  attempt_id uuid NOT NULL DEFAULT gen_random_uuid(),correlation_id uuid NOT NULL DEFAULT gen_random_uuid()
);
CREATE TABLE moria.runtime_pipeline_outputs (
  execution uuid NOT NULL REFERENCES moria.runtime_pipeline_executions(execution),
  task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),owner uuid NOT NULL,fence bigint NOT NULL,
  resource uuid NOT NULL,root jsonb NOT NULL CHECK(jsonb_typeof(root)='object'),
  PRIMARY KEY(execution,resource)
);
CREATE TABLE moria.runtime_pipeline_checkpoints (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_pipeline_executions(execution),
  task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),owner uuid NOT NULL,fence bigint NOT NULL,
  value jsonb NOT NULL CHECK(octet_length(value::text)<=65536)
);
CREATE FUNCTION moria.runtime_pipeline_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' OR TG_TABLE_NAME NOT IN ('runtime_pipeline_profiles','runtime_pipeline_destinations')
    OR (to_jsonb(NEW)-'enabled')<>(to_jsonb(OLD)-'enabled') THEN RAISE EXCEPTION 'IMMUTABLE_PIPELINE_BINDING'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_pipeline_profile_identity BEFORE UPDATE OR DELETE ON moria.runtime_pipeline_profiles
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_pipeline_immutable();
CREATE TRIGGER runtime_pipeline_destination_identity BEFORE UPDATE OR DELETE ON moria.runtime_pipeline_destinations
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_pipeline_immutable();
CREATE TRIGGER runtime_pipeline_profile_epoch AFTER UPDATE ON moria.runtime_pipeline_profiles
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_revocation_epoch();
CREATE TRIGGER runtime_pipeline_destination_epoch AFTER UPDATE ON moria.runtime_pipeline_destinations
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_service_revocation_epoch();
CREATE TRIGGER runtime_pipeline_execution_identity BEFORE UPDATE OR DELETE ON moria.runtime_pipeline_executions
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_pipeline_immutable();
CREATE TRIGGER runtime_pipeline_output_identity BEFORE UPDATE OR DELETE ON moria.runtime_pipeline_outputs
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_pipeline_immutable();
CREATE FUNCTION moria.runtime_pipeline_current(execution_id uuid) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE e moria.runtime_executions;p moria.runtime_pipeline_profiles;r moria.runtime_pipeline_executions;state jsonb;item jsonb;requests jsonb;
BEGIN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=execution_id FOR SHARE;
  SELECT p0.* INTO p FROM moria.runtime_pipeline_executions pe JOIN moria.runtime_pipeline_profiles p0 ON p0.id=pe.profile
    WHERE pe.execution=execution_id AND p0.enabled AND p0.environment=e.environment AND p0.resource=e.resource
      AND p0.domain=e.spec->>'domain' AND p0.resource_class=e.resource_class FOR SHARE OF p0;
  IF NOT FOUND THEN RAISE EXCEPTION 'PIPELINE_PROFILE_REVOKED'; END IF;
  SELECT * INTO r FROM moria.runtime_pipeline_executions WHERE execution=execution_id;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_execution_services s WHERE s.binding=e.binding AND s.authority=p.service_authority AND s.target_revision=p.service_revision)
    THEN RAISE EXCEPTION 'PIPELINE_PROFILE_REVOKED'; END IF;
  IF p.profile->>'connector' IS NOT NULL AND NOT EXISTS(SELECT 1 FROM moria.runtime_pipeline_destinations d
    WHERE d.id=(p.profile->>'connector')::uuid AND d.environment=e.environment AND d.resource=e.resource AND d.enabled)
    THEN RAISE EXCEPTION 'PIPELINE_DESTINATION_REVOKED'; END IF;
  state:=moria.runtime_service_binding_guard(e.binding);
  requests:=jsonb_build_array(jsonb_build_object('resource',e.resource::text,'operation','read'));
  IF r.code_resource IS NOT NULL THEN requests:=requests||jsonb_build_array(jsonb_build_object('resource',r.code_resource::text,'operation','read')); END IF;
  FOR item IN SELECT value FROM jsonb_each(r.inputs) LOOP
    PERFORM moria.runtime_dataset_kind(e.environment,item->>'resource');
    requests:=requests||jsonb_build_array(jsonb_build_object('resource',item->>'resource','operation','read'));
    IF NOT EXISTS(SELECT 1 FROM moria.native_storage_namespaces n WHERE n.environment=e.environment AND n.resource=item->>'resource'
      AND n.warehouse_id=item->'pin'->'namespace'->>'warehouseId' AND n.table_id=item->'pin'->'namespace'->>'tableId'
      AND n.revision=item->'pin'->'namespace'->>'revision' AND n.enabled) THEN RAISE EXCEPTION 'STORAGE_NAMESPACE_REVOKED'; END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_each(r.outputs) LOOP
    PERFORM moria.runtime_dataset_kind(e.environment,item->>'resource');
    requests:=requests||jsonb_build_array(jsonb_build_object('resource',item->>'resource','operation','read'),jsonb_build_object('resource',item->>'resource','operation','write'));
    IF NOT EXISTS(SELECT 1 FROM moria.native_storage_namespaces n WHERE n.environment=e.environment AND n.resource=item->>'resource'
      AND n.warehouse_id=item->'namespace'->>'warehouseId' AND n.table_id=item->'namespace'->>'tableId'
      AND n.revision=item->'namespace'->>'revision' AND n.enabled) THEN RAISE EXCEPTION 'STORAGE_NAMESPACE_REVOKED'; END IF;
  END LOOP;
  FOR item IN SELECT value FROM jsonb_array_elements(r.secret_approvals) LOOP
    IF NOT EXISTS(SELECT 1 FROM moria.native_secret_approvals a
      WHERE a.id::text=item->'approval'->>'id' AND a.generation::text=item->'approval'->>'generation'
        AND a.incarnation::text=item->'approval'->>'incarnation' AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance)
        AND a.environment=e.environment AND a.consumer=e.resource AND a.secret::text=item->'request'->>'secret'
        AND a.owner_kind='connector' AND a.owner_profile::text=item->'request'->>'ownerProfile'
        AND a.owner_revision=item->'request'->>'ownerRevision' AND a.destination_fingerprint=r.destination_fingerprint
        AND a.material_ref=item->'request'->>'materialRef' AND a.material_revision=item->>'materialRevision' AND a.enabled)
      THEN RAISE EXCEPTION 'SECRET_APPROVAL_REVOKED'; END IF;
    requests:=requests||jsonb_build_array(jsonb_build_object('resource',item->'request'->>'secret','operation','use'));
  END LOOP;
  PERFORM moria.native_serving_resource_access_batch_state(state,requests);
END $$;
CREATE OR REPLACE VIEW moria.runtime_sandbox_metadata AS
 SELECT execution,entry_id,release,definition,attempt_id,correlation_id,markings,prepare_only,'function'::text AS domain FROM moria.runtime_function_invocations
 UNION ALL SELECT execution,entry_id,release,definition,attempt_id,correlation_id,markings,false,'agent'::text FROM moria.runtime_agent_executions
 UNION ALL SELECT e.execution,r.entry_id,r.release,r.definition,e.attempt_id,e.correlation_id,e.markings,false,'evaluator'::text
  FROM moria.runtime_evaluator_executions e JOIN moria.runtime_evaluations r ON r.execution=e.evaluation
 UNION ALL SELECT r.execution,r.entry_id,r.release,r.definition,r.attempt_id,r.correlation_id,r.markings,false,e.spec->>'domain'
  FROM moria.runtime_pipeline_executions r JOIN moria.runtime_executions e ON e.id=r.execution;
`;
