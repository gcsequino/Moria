/** Recovery sets pin exact retained content; restored dispatch stays disabled until verified reconciliation. */
export const recoveryRuntimeSchema = String.raw`
CREATE FUNCTION moria.runtime_recovery_operator() RETURNS void LANGUAGE plpgsql
 SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF NOT has_schema_privilege(session_user,'moria','CREATE') THEN RAISE EXCEPTION 'RECOVERY_OPERATOR_REQUIRED'; END IF;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_recovery_operator() FROM PUBLIC;
CREATE FUNCTION moria.runtime_recovery_operator_mutation() RETURNS trigger LANGUAGE plpgsql
 SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM moria.runtime_recovery_operator();
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_recovery_operator_mutation() FROM PUBLIC;
CREATE TABLE public.moria_restore_bootstrap (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),dispatch_approved boolean NOT NULL DEFAULT true,
  restore_id uuid,dump_sha256 text,manifest_hash text,status text NOT NULL DEFAULT 'normal'
);
INSERT INTO public.moria_restore_bootstrap DEFAULT VALUES;
CREATE FUNCTION moria.runtime_dispatch_bootstrap_approved() RETURNS boolean LANGUAGE sql SECURITY DEFINER
  SET search_path=pg_catalog,moria AS $$
  SELECT COALESCE((SELECT dispatch_approved FROM public.moria_restore_bootstrap WHERE singleton FOR SHARE),false)
$$;
CREATE TABLE moria.native_cli_profile_approvals (
  key_hash text NOT NULL CHECK(key_hash ~ '^[0-9a-f]{64}$'),profile_hash text NOT NULL CHECK(profile_hash ~ '^[0-9a-f]{64}$'),
  incarnation uuid NOT NULL,profiles jsonb NOT NULL,enabled boolean NOT NULL DEFAULT true,generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),
  approved_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(key_hash,profile_hash,incarnation)
);
CREATE TABLE moria.native_cli_sessions (
  token_hash text PRIMARY KEY REFERENCES moria.workload_sessions(token_hash),key_hash text NOT NULL,profile_hash text NOT NULL,
  incarnation uuid NOT NULL,profile_name text NOT NULL,approval_generation bigint NOT NULL,
  FOREIGN KEY(key_hash,profile_hash,incarnation) REFERENCES moria.native_cli_profile_approvals(key_hash,profile_hash,incarnation)
);
CREATE FUNCTION moria.native_cli_approval_generation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'CLI_APPROVAL_INVALID'; END IF;
  IF ROW(NEW.key_hash,NEW.profile_hash,NEW.incarnation,NEW.profiles,NEW.generation,NEW.approved_at)
    IS DISTINCT FROM ROW(OLD.key_hash,OLD.profile_hash,OLD.incarnation,OLD.profiles,OLD.generation,OLD.approved_at)
    THEN RAISE EXCEPTION 'CLI_APPROVAL_INVALID'; END IF;
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN NEW.generation:=OLD.generation+1; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_cli_approval_generation BEFORE UPDATE OR DELETE ON moria.native_cli_profile_approvals
  FOR EACH ROW EXECUTE FUNCTION moria.native_cli_approval_generation();
CREATE FUNCTION moria.native_cli_session_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'CLI_APPROVAL_INVALID'; END $$;
CREATE TRIGGER native_cli_session_immutable BEFORE UPDATE OR DELETE ON moria.native_cli_sessions
  FOR EACH ROW EXECUTE FUNCTION moria.native_cli_session_immutable();
CREATE FUNCTION moria.runtime_cli_session_check(session_hash text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE issued moria.native_cli_sessions;
BEGIN
  SELECT * INTO issued FROM moria.native_cli_sessions WHERE token_hash=session_hash;
  IF NOT FOUND THEN RETURN; END IF;
  PERFORM 1 FROM moria.native_cli_profile_approvals a JOIN moria.indexing_instance i ON i.incarnation=a.incarnation
    JOIN moria.workload_sessions s ON s.token_hash=issued.token_hash
    WHERE a.key_hash=issued.key_hash AND a.profile_hash=issued.profile_hash AND a.incarnation=issued.incarnation
      AND a.enabled AND a.generation=issued.approval_generation
      AND ROW(a.profiles->issued.profile_name->>'environment',a.profiles->issued.profile_name->>'principal',a.profiles->issued.profile_name->>'application')
        IS NOT DISTINCT FROM ROW(s.environment,s.principal,s.application) FOR SHARE OF a;
  IF NOT FOUND THEN RAISE EXCEPTION 'CLI_APPROVAL_REQUIRED'; END IF;
END $$;
CREATE TABLE moria.runtime_recovery_sets (
  id uuid PRIMARY KEY,request_key text NOT NULL UNIQUE CHECK(octet_length(request_key) BETWEEN 1 AND 256),
  owner uuid NOT NULL,fence bigint NOT NULL CHECK(fence>0),incarnation uuid NOT NULL,
  lease_until timestamptz NOT NULL,status text NOT NULL CHECK(status IN ('preparing','ready','failed','retired')),
  schema_checksum text NOT NULL,key_manifest_hash text NOT NULL CHECK(key_manifest_hash ~ '^[0-9a-f]{64}$'),
  exported_snapshot text,metadata_dump jsonb,content_inventory jsonb,manifest_hash text,sealed_manifest jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),retained_until timestamptz NOT NULL,
  retention_seconds bigint NOT NULL DEFAULT 86400 CHECK(retention_seconds BETWEEN 86400 AND 315360000),
  error text
);
CREATE FUNCTION moria.runtime_recovery_set_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'RECOVERY_MANIFEST_IMMUTABLE'; END IF;
  IF ROW(NEW.id,NEW.request_key,NEW.owner,NEW.fence,NEW.incarnation,NEW.schema_checksum,NEW.key_manifest_hash,NEW.created_at,NEW.retention_seconds)
    IS DISTINCT FROM ROW(OLD.id,OLD.request_key,OLD.owner,OLD.fence,OLD.incarnation,OLD.schema_checksum,OLD.key_manifest_hash,OLD.created_at,OLD.retention_seconds)
    OR NEW.retained_until<OLD.retained_until
    OR (OLD.metadata_dump IS NOT NULL AND NEW.metadata_dump IS DISTINCT FROM OLD.metadata_dump)
    OR (OLD.content_inventory IS NOT NULL AND NEW.content_inventory IS DISTINCT FROM OLD.content_inventory)
    OR (OLD.exported_snapshot IS NOT NULL AND NEW.exported_snapshot IS DISTINCT FROM OLD.exported_snapshot)
    OR (OLD.manifest_hash IS NOT NULL AND NEW.manifest_hash IS DISTINCT FROM OLD.manifest_hash)
    OR (OLD.sealed_manifest IS NOT NULL AND NEW.sealed_manifest IS DISTINCT FROM OLD.sealed_manifest)
    OR (OLD.status='retired' AND NEW.status<>OLD.status)
    OR (OLD.status IN ('ready','failed') AND NEW.status NOT IN (OLD.status,'retired'))
    OR (NEW.status='retired' AND NEW.retained_until>clock_timestamp())
    THEN RAISE EXCEPTION 'RECOVERY_MANIFEST_IMMUTABLE'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER runtime_recovery_set_immutable BEFORE UPDATE OR DELETE ON moria.runtime_recovery_sets
  FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_set_immutable();
CREATE TABLE moria.runtime_recovery_control (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  dispatch_enabled boolean NOT NULL DEFAULT true,restore_id uuid
);
INSERT INTO moria.runtime_recovery_control DEFAULT VALUES;
CREATE TABLE moria.runtime_restores (
  id uuid PRIMARY KEY,recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),
  manifest_hash text NOT NULL CHECK(manifest_hash ~ '^[0-9a-f]{64}$'),
  previous_incarnation uuid NOT NULL,new_incarnation uuid NOT NULL UNIQUE,
  status text NOT NULL CHECK(status IN ('disabled','verified','enabled')),leases_fenced boolean NOT NULL DEFAULT false,
  verification jsonb,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),enabled_at timestamptz
);
CREATE TABLE moria.runtime_restore_namespace_rebindings (
  restore_id uuid NOT NULL REFERENCES moria.runtime_restores(id),warehouse_id text NOT NULL,table_id text NOT NULL,
  namespace_revision text NOT NULL,source_fingerprint text NOT NULL,target_fingerprint text NOT NULL,
  kind text NOT NULL CHECK(kind IN ('dataset','filesystem')),PRIMARY KEY(restore_id,warehouse_id,table_id,namespace_revision),
  CHECK(source_fingerprint<>target_fingerprint)
);
CREATE FUNCTION moria.runtime_restore_dataset_rebind_check(env text,target uuid,warehouse text,table_name text,
  namespace_version text,previous_fingerprint text,next_fingerprint text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM moria.runtime_restore_namespace_rebindings b
    JOIN moria.runtime_restores r ON r.id=b.restore_id
    JOIN moria.runtime_recovery_control c ON c.restore_id=r.id
    JOIN moria.indexing_instance i ON i.incarnation=r.new_incarnation
    JOIN moria.native_storage_namespaces n ON n.environment=env AND n.resource=target::text
      AND n.warehouse_id=b.warehouse_id AND n.table_id=b.table_id AND n.revision=b.namespace_revision
    WHERE c.singleton AND NOT c.dispatch_enabled AND r.status='disabled' AND r.leases_fenced
      AND b.kind='dataset' AND b.warehouse_id=warehouse AND b.table_id=table_name AND b.namespace_revision=namespace_version
      AND b.source_fingerprint=previous_fingerprint AND b.target_fingerprint=next_fingerprint
    FOR SHARE OF b,r,c;
  IF NOT FOUND THEN RAISE EXCEPTION 'DATASET_SCHEMA_EVOLUTION_REQUIRED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_recovery_check() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT moria.runtime_dispatch_bootstrap_approved() THEN RAISE EXCEPTION 'RESTORE_DISPATCH_DISABLED'; END IF;
  PERFORM 1 FROM moria.runtime_recovery_control WHERE singleton AND dispatch_enabled FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'RESTORE_DISPATCH_DISABLED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_recovery_lease_check(set_id uuid,lease_owner uuid,lease_fence bigint,instance uuid)
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM moria.runtime_recovery_sets s JOIN moria.indexing_instance i ON i.incarnation=s.incarnation
    WHERE s.id=set_id AND s.owner=lease_owner AND s.fence=lease_fence AND s.incarnation=instance
      AND s.status='preparing' AND s.lease_until>clock_timestamp() FOR SHARE OF s;
  IF NOT FOUND THEN RAISE EXCEPTION 'RECOVERY_LEASE_LOST'; END IF;
END $$;

CREATE FUNCTION moria.runtime_restore_pause(restore_id uuid,set_id uuid,manifest text,expected_instance uuid)
  RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE control moria.runtime_recovery_control;previous moria.runtime_restores;current_instance uuid;next_instance uuid;
BEGIN
  IF manifest !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'RECOVERY_INVALID'; END IF;
  SELECT * INTO control FROM moria.runtime_recovery_control WHERE singleton FOR UPDATE;
  SELECT * INTO previous FROM moria.runtime_restores WHERE id=restore_id;
  IF FOUND THEN
    IF ROW(previous.recovery_set,previous.manifest_hash,previous.previous_incarnation)
      IS DISTINCT FROM ROW(set_id,manifest,expected_instance) OR control.restore_id IS DISTINCT FROM restore_id
      OR previous.new_incarnation<>(SELECT incarnation FROM moria.indexing_instance)
      THEN RAISE EXCEPTION 'RECOVERY_MANIFEST_CONFLICT'; END IF;
    RETURN previous.new_incarnation;
  END IF;
  IF NOT control.dispatch_enabled THEN RAISE EXCEPTION 'RESTORE_DISPATCH_DISABLED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_sets WHERE id=set_id AND incarnation=expected_instance
    AND status IN ('preparing','ready')) THEN RAISE EXCEPTION 'RECOVERY_INVALID'; END IF;
  SELECT incarnation INTO current_instance FROM moria.indexing_instance FOR UPDATE;
  IF current_instance<>expected_instance THEN RAISE EXCEPTION 'INCARNATION_CHANGED'; END IF;
  next_instance:=gen_random_uuid();
  INSERT INTO moria.runtime_restores(id,recovery_set,manifest_hash,previous_incarnation,new_incarnation,status)
    VALUES(restore_id,set_id,manifest,current_instance,next_instance,'disabled');
  UPDATE moria.runtime_recovery_control SET dispatch_enabled=false,restore_id=runtime_restore_pause.restore_id WHERE singleton;
  UPDATE moria.indexing_instance SET incarnation=next_instance;
  RETURN next_instance;
END $$;
CREATE FUNCTION moria.runtime_restore_fence(restore_id uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE restore moria.runtime_restores;
BEGIN
  -- Pausing and incarnation rotation committed first; no control lock while waiting on old attempts.
  SELECT * INTO restore FROM moria.runtime_restores WHERE id=restore_id FOR UPDATE;
  IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_control c,moria.indexing_instance i
    WHERE c.singleton AND NOT c.dispatch_enabled AND c.restore_id=restore.id AND i.incarnation=restore.new_incarnation)
    THEN RAISE EXCEPTION 'RECOVERY_INVALID'; END IF;
  IF restore.leases_fenced THEN RETURN; END IF;
  UPDATE moria.workload_sessions SET disabled=true;
  UPDATE moria.browser_sessions SET disabled=true;
  UPDATE moria.native_cli_profile_approvals SET enabled=false;
  UPDATE moria.auth_providers SET enabled=false;
  UPDATE moria.principals SET enabled=false;
  UPDATE moria.instance_principals SET enabled=false;
  UPDATE moria.application_scopes SET enabled=false;
  UPDATE moria.instance_state SET security_epoch=security_epoch+1;
  UPDATE moria.environments SET security_epoch=security_epoch+1;
  UPDATE moria.runtime_cells SET enabled=false;
  UPDATE moria.runtime_workers SET enabled=false;
  UPDATE moria.native_service_peers SET enabled=false;
  UPDATE moria.runtime_service_authorities SET enabled=false;
  UPDATE moria.runtime_execution_bindings SET enabled=false;
  UPDATE moria.runtime_automations SET enabled=false;
  UPDATE moria.indexing_bindings SET enabled=false;
  UPDATE moria.native_model_bindings SET enabled=false;
  UPDATE moria.native_secret_approvals SET enabled=false;
  UPDATE moria.native_search_projector_bindings SET enabled=false;
  UPDATE moria.runtime_delivery_destinations SET enabled=false;
  UPDATE moria.runtime_identity_domains SET incarnation=restore.new_incarnation,fence=fence+1;
  UPDATE moria.runtime_placements SET incarnation=restore.new_incarnation,fence=fence+1,serving=false,leases_fenced=false,
    transfer_from_cell=NULL,transfer_from_fence=NULL;
  UPDATE moria.runtime_execution_attempts SET status='uncertain',finished_at=clock_timestamp() WHERE status='running';
  UPDATE moria.runtime_execution_tasks SET status='cancelled',fence=fence+1,lease_until=clock_timestamp()
    WHERE status IN ('pending','running');
  UPDATE moria.runtime_executions SET status='failed',fence=fence+1,lease_until=clock_timestamp(),
    error='RESTORE_RECONCILIATION_REQUIRED',finished_at=clock_timestamp() WHERE status IN ('queued','running','preparing');
  UPDATE moria.indexing_tasks SET fence=fence+1,lease_until=clock_timestamp() WHERE status IN ('pending','running');
  UPDATE moria.indexing_builds SET status='failed',fence=fence+1,lease_until=clock_timestamp(),error='RESTORE_RECONCILIATION_REQUIRED'
    WHERE status IN ('pending','planning','running','preparing','ready');
  UPDATE moria.indexing_attempts SET outcome='cancelled',finished_at=clock_timestamp() WHERE outcome='running';
  UPDATE moria.runtime_delivery_attempts SET status='uncertain',
    error='RESTORE_RECONCILIATION_REQUIRED',finished_at=clock_timestamp() WHERE status IN ('claimed','sent');
  UPDATE moria.runtime_deliveries SET status='uncertain',
    fence=fence+1,lease_until=clock_timestamp(),error='RESTORE_RECONCILIATION_REQUIRED',finished_at=clock_timestamp()
    WHERE status IN ('pending','delivering','retryScheduled');
  DELETE FROM moria.native_model_probes;
  UPDATE moria.native_model_calls SET state='failed',uncertain=true,error='RESTORE_RECONCILIATION_REQUIRED',finished_at=clock_timestamp()
    WHERE state='running';
  UPDATE moria.native_model_attempts SET status='uncertain',finished_at=clock_timestamp() WHERE status='started';
  UPDATE moria.runtime_restores SET leases_fenced=true WHERE id=restore.id;
END $$;
CREATE FUNCTION moria.runtime_restore_assign_cell(restore_id uuid,env text,expected_cell uuid,expected_fence bigint,instance uuid,next_cell uuid)
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE p moria.runtime_placements;
BEGIN
  PERFORM 1 FROM moria.runtime_restores r JOIN moria.indexing_instance i ON i.incarnation=r.new_incarnation
    WHERE r.id=restore_id AND r.new_incarnation=instance AND r.leases_fenced AND r.status='enabled' FOR SHARE OF r;
  IF NOT FOUND THEN RAISE EXCEPTION 'RECOVERY_INVALID'; END IF;
  PERFORM 1 FROM moria.runtime_cells WHERE id=next_cell AND enabled AND incarnation=instance FOR SHARE;
  IF NOT FOUND OR next_cell=expected_cell OR expected_fence<=0 THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  SELECT * INTO p FROM moria.runtime_placements WHERE environment=env FOR UPDATE;
  IF NOT FOUND OR p.serving OR p.incarnation<>instance THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  IF p.cell=next_cell AND p.fence=expected_fence+1 AND p.leases_fenced
    AND p.transfer_from_cell=expected_cell AND p.transfer_from_fence=expected_fence THEN RETURN; END IF;
  IF ROW(p.cell,p.fence) IS DISTINCT FROM ROW(expected_cell,expected_fence) OR p.leases_fenced
    THEN RAISE EXCEPTION 'CELL_OWNERSHIP_LOST'; END IF;
  UPDATE moria.runtime_placements SET cell=next_cell,fence=fence+1,leases_fenced=true,
    transfer_from_cell=expected_cell,transfer_from_fence=expected_fence WHERE environment=env;
  INSERT INTO moria.runtime_ownership_events(kind,resource,previous_cell,next_cell,fence,incarnation)
    VALUES('environment',env,expected_cell,next_cell,expected_fence+1,instance);
END $$;
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON public.moria_restore_bootstrap
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_sets
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_recovery_control
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_restores
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
CREATE TRIGGER recovery_operator_mutation BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_restore_namespace_rebindings
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_operator_mutation();
`;
