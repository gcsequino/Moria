/** Exact-issued reclamation; Dataset/history expiration remains a separate admission gate. */
export const retentionRuntimeSchema = String.raw`
CREATE TABLE moria.runtime_retention_policies(
 id uuid PRIMARY KEY,request_key text NOT NULL UNIQUE,revision text NOT NULL CHECK(revision ~ '^sha256:[0-9a-f]{64}$'),namespace jsonb NOT NULL CHECK(jsonb_typeof(namespace)='object' AND octet_length(namespace::text)<=1024),
 physical_fingerprint text NOT NULL,minimum_age_seconds bigint NOT NULL CHECK(minimum_age_seconds>=86400),
 recheck_seconds integer NOT NULL CHECK(recheck_seconds BETWEEN 30 AND 604800),
 incarnation uuid NOT NULL,approved_role name NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 enabled boolean NOT NULL DEFAULT true
);
CREATE TABLE moria.runtime_gc_retirements(
 recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),policy uuid NOT NULL REFERENCES moria.runtime_retention_policies(id),
 approved_role name NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(recovery_set,policy)
);
CREATE TABLE moria.runtime_gc_candidates(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),policy uuid NOT NULL REFERENCES moria.runtime_retention_policies(id),
 recovery_set uuid NOT NULL REFERENCES moria.runtime_recovery_sets(id),object jsonb NOT NULL CHECK(jsonb_typeof(object)='object' AND octet_length(object::text)<=2048),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','watching','unsupported')),
 owner uuid,fence bigint NOT NULL DEFAULT 0,incarnation uuid,lease_until timestamptz,
 admission_pid integer,admission_started timestamptz,expected_version jsonb,
 next_check timestamptz NOT NULL DEFAULT clock_timestamp(),attempts bigint NOT NULL DEFAULT 0,
 last_result text,last_removed_version jsonb,UNIQUE(policy,recovery_set,object)
);
CREATE INDEX runtime_gc_due ON moria.runtime_gc_candidates(policy,next_check,id);
CREATE FUNCTION moria.runtime_gc_candidate_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.id,NEW.policy,NEW.recovery_set,NEW.object) IS DISTINCT FROM ROW(OLD.id,OLD.policy,OLD.recovery_set,OLD.object)
   OR NEW.fence<OLD.fence OR NEW.attempts<OLD.attempts THEN RAISE EXCEPTION 'IMMUTABLE_GC_CANDIDATE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_gc_candidate_immutable BEFORE UPDATE OR DELETE ON moria.runtime_gc_candidates
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_gc_candidate_immutable();
CREATE INDEX runtime_recovery_member_source_object ON moria.runtime_recovery_members(warehouse_id,table_id,object_id,recovery_set);
CREATE INDEX runtime_recovery_issued_object ON moria.runtime_recovery_outputs(warehouse_id,table_id,object_id,recovery_set);
CREATE INDEX runtime_recovery_member_backup_object ON moria.runtime_recovery_members((backup->'object'->>'warehouseId'),(backup->'object'->>'tableId'),(backup->'object'->>'objectId'),recovery_set);
CREATE FUNCTION moria.runtime_retention_policy_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-'enabled') IS DISTINCT FROM (to_jsonb(OLD)-'enabled') OR (NOT OLD.enabled AND NEW.enabled)
   THEN RAISE EXCEPTION 'IMMUTABLE_RETENTION_POLICY'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_retention_policy_immutable BEFORE UPDATE OR DELETE ON moria.runtime_retention_policies
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_policy_immutable();
CREATE TRIGGER runtime_gc_retirement_immutable BEFORE UPDATE OR DELETE ON moria.runtime_gc_retirements
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();

CREATE FUNCTION moria.runtime_gc_reference_check(set_id uuid,logical_object jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE warehouse text:=logical_object->>'warehouseId';table_name text:=logical_object->>'tableId';object_name text:=logical_object->>'objectId';
BEGIN
 IF jsonb_typeof(logical_object) IS DISTINCT FROM 'object' OR logical_object-ARRAY['version','warehouseId','tableId','objectId']<>'{}'::jsonb
   OR logical_object->>'version' IS DISTINCT FROM '1' OR warehouse IS NULL OR table_name IS NULL OR object_name IS NULL
   OR warehouse !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$' OR table_name !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
   OR object_name='moria_restore_target_custody_v1' OR object_name !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
   OR left(object_name,length('recovery_'||replace(set_id::text,'-','')||'_'))<>'recovery_'||replace(set_id::text,'-','')||'_'
   THEN RAISE EXCEPTION 'GC_OBJECT_DENIED'; END IF;
 IF EXISTS(SELECT 1 FROM moria.native_storage_namespaces WHERE warehouse_id=warehouse AND table_id=table_name)
   OR EXISTS(SELECT 1 FROM moria.fs_storage_bindings WHERE warehouse_id=warehouse AND table_id=table_name)
   THEN RAISE EXCEPTION 'GC_REFERENCE_PRESENT'; END IF;
 -- Dedicated backup namespaces cannot be valid Dataset, filesystem, execution, cursor, index or search input namespaces.
 -- Other retained recovery sets are checked independently rather than inferred from an empty issued ledger.
 IF EXISTS(SELECT 1 FROM moria.runtime_recovery_outputs o JOIN moria.runtime_recovery_sets s ON s.id=o.recovery_set
   WHERE s.id<>set_id AND (s.status<>'retired' OR s.retained_until>clock_timestamp()) AND o.warehouse_id=warehouse AND o.table_id=table_name AND o.object_id=object_name)
   OR EXISTS(SELECT 1 FROM moria.runtime_recovery_members m JOIN moria.runtime_recovery_sets s ON s.id=m.recovery_set
   WHERE s.id<>set_id AND (s.status<>'retired' OR s.retained_until>clock_timestamp())
    AND ((m.warehouse_id=warehouse AND m.table_id=table_name AND m.object_id=object_name)
      OR (m.backup->'object'->>'warehouseId'=warehouse AND m.backup->'object'->>'tableId'=table_name AND m.backup->'object'->>'objectId'=object_name)))
   OR EXISTS(SELECT 1 FROM moria.runtime_recovery_sets s WHERE s.id<>set_id AND (s.status<>'retired' OR s.retained_until>clock_timestamp())
      AND (s.metadata_dump->'object'=logical_object OR s.content_inventory->'root'->'object'=logical_object OR s.sealed_manifest->'object'=logical_object))
   THEN RAISE EXCEPTION 'GC_REFERENCE_PRESENT'; END IF;
END $$;

CREATE FUNCTION moria.runtime_gc_admission_held(pid_value integer,started timestamptz) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
  WHERE a.pid=pid_value AND a.backend_start=started AND a.datname=current_database()
    AND l.locktype='advisory' AND l.mode='ExclusiveLock' AND l.granted AND l.objsubid=1
    AND l.classid=((hashtextextended('moria/recovery-retention/v1',0)>>32)&4294967295)::oid
    AND l.objid=(hashtextextended('moria/recovery-retention/v1',0)&4294967295)::oid)
$$;
CREATE FUNCTION moria.runtime_gc_claim_check(claim_id uuid,worker uuid,worker_fence bigint,instance uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE c moria.runtime_gc_candidates;p moria.runtime_retention_policies;s moria.runtime_recovery_sets;b moria.runtime_recovery_backends;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 PERFORM moria.runtime_recovery_check();
 SELECT * INTO c FROM moria.runtime_gc_candidates WHERE id=claim_id FOR SHARE;
 IF NOT FOUND OR worker IS NULL OR worker_fence IS NULL OR worker_fence<=0 OR instance IS NULL
   OR c.status<>'running' OR c.owner IS DISTINCT FROM worker OR c.fence<>worker_fence OR c.incarnation IS DISTINCT FROM instance
   OR c.lease_until IS NULL OR c.lease_until<=clock_timestamp() OR instance<>(SELECT incarnation FROM moria.indexing_instance)
   OR NOT moria.runtime_gc_admission_held(c.admission_pid,c.admission_started) THEN RAISE EXCEPTION 'GC_LEASE_LOST'; END IF;
 SELECT * INTO p FROM moria.runtime_retention_policies WHERE id=c.policy;
 SELECT * INTO s FROM moria.runtime_recovery_sets WHERE id=c.recovery_set;
 SELECT * INTO b FROM moria.runtime_recovery_backends WHERE recovery_set=s.id;
 IF NOT p.enabled OR p.incarnation<>instance OR s.incarnation<>instance OR s.status<>'retired' OR s.retained_until>clock_timestamp()
   OR s.created_at+make_interval(secs=>p.minimum_age_seconds::double precision)>clock_timestamp()
   OR b.namespace IS DISTINCT FROM p.namespace OR b.physical_fingerprint IS DISTINCT FROM p.physical_fingerprint
   OR NOT EXISTS(SELECT 1 FROM moria.runtime_gc_retirements WHERE recovery_set=s.id AND policy=p.id)
   OR NOT EXISTS(SELECT 1 FROM moria.runtime_recovery_outputs o WHERE o.recovery_set=s.id AND o.warehouse_id=c.object->>'warehouseId'
     AND o.table_id=c.object->>'tableId' AND o.object_id=c.object->>'objectId') THEN RAISE EXCEPTION 'GC_POLICY_CHANGED'; END IF;
 PERFORM moria.runtime_gc_reference_check(s.id,c.object);
 RETURN jsonb_build_object('object',c.object,'namespace',p.namespace,'physicalFingerprint',p.physical_fingerprint,
   'expectedVersion',c.expected_version,'recheckSeconds',p.recheck_seconds);
END $$;
REVOKE ALL ON FUNCTION moria.runtime_gc_reference_check(uuid,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_gc_admission_held(integer,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_gc_claim_check(uuid,uuid,bigint,uuid) FROM PUBLIC;

-- Managed native Dataset roots and exact issued objects. Unknown/legacy roots remain ineligible.
CREATE TABLE moria.runtime_dataset_producers(
 scope text PRIMARY KEY,execution uuid REFERENCES moria.runtime_executions(id),maintenance uuid,restore uuid REFERENCES moria.runtime_restores(id),task uuid,
 owner uuid,fence bigint CHECK(fence>0),incarnation uuid NOT NULL,
 namespace jsonb NOT NULL,physical_fingerprint text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),tombstoned_at timestamptz,
 CHECK(num_nonnulls(execution,maintenance,restore)=1),
 CHECK((restore IS NOT NULL AND owner IS NULL AND fence IS NULL AND task IS NULL) OR (restore IS NULL AND owner IS NOT NULL AND fence IS NOT NULL))
);
CREATE TABLE moria.runtime_dataset_issued_objects(
 physical_fingerprint text NOT NULL,warehouse_id text NOT NULL,table_id text NOT NULL,object_id text NOT NULL CHECK(object_id<>'moria_restore_target_custody_v1'),
 producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),expected_size bigint NOT NULL CHECK(expected_size>=0),
 expected_sha256 text NOT NULL CHECK(expected_sha256 ~ '^sha256:[0-9a-f]{64}$'),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(physical_fingerprint,warehouse_id,table_id,object_id)
);
CREATE INDEX runtime_dataset_issued_producer ON moria.runtime_dataset_issued_objects(producer,object_id);
CREATE TABLE moria.runtime_dataset_roots(
 key text PRIMARY KEY CHECK(key ~ '^[0-9a-f]{64}$'),environment text NOT NULL,dataset text NOT NULL,
 namespace jsonb NOT NULL,physical_fingerprint text NOT NULL,metadata_location text NOT NULL UNIQUE,
 root jsonb NOT NULL,snapshot_id text NOT NULL,reader_profile text NOT NULL,producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),retention_origin timestamptz NOT NULL DEFAULT clock_timestamp(),sealed boolean NOT NULL DEFAULT false
);
CREATE INDEX runtime_dataset_roots_dataset ON moria.runtime_dataset_roots(environment,dataset,key);
CREATE INDEX runtime_dataset_roots_object ON moria.runtime_dataset_roots((root->'root'->'object'->>'warehouseId'),(root->'root'->'object'->>'tableId'),(root->'root'->'object'->>'objectId'));
CREATE TABLE moria.runtime_dataset_root_members(
 root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),warehouse_id text NOT NULL,table_id text NOT NULL,object_id text NOT NULL,
 content jsonb NOT NULL,physical_version jsonb NOT NULL,PRIMARY KEY(root,warehouse_id,table_id,object_id)
);
CREATE INDEX runtime_dataset_member_object ON moria.runtime_dataset_root_members(warehouse_id,table_id,object_id,root);
CREATE TABLE moria.runtime_dataset_key_certificates(
 root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),key_schema_hash text NOT NULL CHECK(key_schema_hash~'^sha256:[0-9a-f]{64}$'),
 snapshot_id text NOT NULL,key_rows bigint NOT NULL CHECK(key_rows>=0),verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(root,key_schema_hash)
);
CREATE TRIGGER runtime_dataset_key_certificate_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_key_certificates
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE INDEX runtime_snapshot_history_selection ON moria.snapshot_history(environment,dataset,snapshot_id,revision DESC);
CREATE INDEX runtime_snapshot_candidate_output ON moria.runtime_snapshot_candidates(environment,resource,metadata_location,snapshot_id,execution);
CREATE FUNCTION moria.runtime_dataset_head_contract() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE binding record;history record;valid boolean;
BEGIN
 SELECT * INTO binding FROM moria.native_dataset_bindings WHERE environment=NEW.environment AND resource::text=NEW.dataset FOR SHARE;
 IF NOT FOUND OR NEW.revision=0 THEN RETURN NEW;END IF;
 IF NOT binding.enabled OR NEW.metadata_location IS NULL OR NEW.snapshot_id IS NULL THEN RAISE EXCEPTION 'DATASET_BINDING_INVALID';END IF;
 SELECT * INTO history FROM moria.snapshot_history WHERE environment=NEW.environment AND dataset=NEW.dataset AND revision=NEW.revision;
 IF NOT FOUND OR history.metadata_location IS DISTINCT FROM NEW.metadata_location OR history.snapshot_id IS DISTINCT FROM NEW.snapshot_id
  THEN RAISE EXCEPTION 'DATASET_PUBLICATION_CONTRACT_REQUIRED';END IF;
 SELECT EXISTS(SELECT 1 FROM moria.runtime_snapshot_candidates c WHERE c.environment=NEW.environment AND c.resource=NEW.dataset
  AND c.metadata_location=NEW.metadata_location AND c.snapshot_id=NEW.snapshot_id AND c.execution::text=history.execution
  AND c.expected_revision=NEW.revision-1 AND c.write_mode=ANY(binding.write_modes)) INTO valid;
 IF NOT valid THEN
  SELECT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots root JOIN moria.runtime_dataset_producers producer ON producer.scope=root.producer
   JOIN moria.runtime_dataset_maintenance_runs run ON run.id=producer.maintenance JOIN moria.runtime_dataset_retention_policies policy ON policy.id=run.policy
   WHERE root.key=moria.runtime_dataset_root_key(NEW.metadata_location) AND root.sealed AND root.environment=NEW.environment AND root.dataset=NEW.dataset
    AND policy.environment=NEW.environment AND policy.dataset=NEW.dataset AND run.status='committed' AND run.expected_revision=NEW.revision-1
    AND run.new_metadata=NEW.metadata_location AND history.execution='dataset-maintenance/'||run.id::text) INTO valid;
 END IF;
 IF NOT valid THEN RAISE EXCEPTION 'DATASET_PUBLICATION_CONTRACT_REQUIRED';END IF;
 IF cardinality(binding.primary_key)>0 AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_key_certificates c
   WHERE c.root=moria.runtime_dataset_root_key(NEW.metadata_location) AND c.key_schema_hash=binding.key_schema_hash AND c.snapshot_id=NEW.snapshot_id)
  THEN RAISE EXCEPTION 'DATASET_PRIMARY_KEY_PROOF_REQUIRED';END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER runtime_dataset_head_contract AFTER INSERT OR UPDATE ON moria.dataset_heads
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_head_contract();

CREATE TABLE moria.runtime_dataset_root_qualifications(
 root text NOT NULL REFERENCES moria.runtime_dataset_roots(key),physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,
 restore uuid NOT NULL REFERENCES moria.runtime_restores(id),producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),namespace jsonb NOT NULL,inventory jsonb NOT NULL,target_mapping_hash text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),sealed boolean NOT NULL DEFAULT false,
 PRIMARY KEY(root,physical_fingerprint,incarnation)
);
CREATE TABLE moria.runtime_dataset_qualified_members(
 root text NOT NULL,physical_fingerprint text NOT NULL,incarnation uuid NOT NULL,warehouse_id text NOT NULL,table_id text NOT NULL,object_id text NOT NULL,
 content jsonb NOT NULL,physical_version jsonb NOT NULL,
 PRIMARY KEY(root,physical_fingerprint,incarnation,warehouse_id,table_id,object_id),
 FOREIGN KEY(root,physical_fingerprint,incarnation) REFERENCES moria.runtime_dataset_root_qualifications(root,physical_fingerprint,incarnation)
);
CREATE INDEX runtime_dataset_qualified_object ON moria.runtime_dataset_qualified_members(physical_fingerprint,incarnation,warehouse_id,table_id,object_id,root);
CREATE VIEW moria.runtime_dataset_root_locations AS
 SELECT r.key AS root,r.physical_fingerprint,p.incarnation,r.namespace,r.sealed,p.scope AS producer FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_producers p ON p.scope=r.producer
 UNION ALL SELECT q.root,q.physical_fingerprint,q.incarnation,q.namespace,q.sealed,q.producer FROM moria.runtime_dataset_root_qualifications q;
CREATE VIEW moria.runtime_dataset_member_locations AS
 SELECT m.root,r.physical_fingerprint,p.incarnation,m.warehouse_id,m.table_id,m.object_id,m.content,m.physical_version
 FROM moria.runtime_dataset_root_members m JOIN moria.runtime_dataset_roots r ON r.key=m.root JOIN moria.runtime_dataset_producers p ON p.scope=r.producer WHERE r.sealed
 UNION ALL SELECT m.root,m.physical_fingerprint,m.incarnation,m.warehouse_id,m.table_id,m.object_id,m.content,m.physical_version
 FROM moria.runtime_dataset_qualified_members m JOIN moria.runtime_dataset_root_qualifications q USING(root,physical_fingerprint,incarnation) WHERE q.sealed;
CREATE TABLE moria.runtime_dataset_root_pins(
 root_key text NOT NULL CHECK(root_key ~ '^[0-9a-f]{64}$'),owner_kind text NOT NULL,owner_id text NOT NULL,
 environment text NOT NULL,dataset text NOT NULL,revision bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(root_key,owner_kind,owner_id)
);
CREATE INDEX runtime_dataset_pin_owner ON moria.runtime_dataset_root_pins(owner_kind,owner_id,root_key);
CREATE TABLE moria.runtime_dataset_retention_policies(
 id uuid PRIMARY KEY,request_key text NOT NULL UNIQUE,environment text NOT NULL,dataset text NOT NULL,
 namespace jsonb NOT NULL,physical_fingerprint text NOT NULL,revision text NOT NULL,incarnation uuid NOT NULL,
 binding_revision text NOT NULL,source_digest text NOT NULL,
 retain_seconds bigint NOT NULL CHECK(retain_seconds>=86400),recheck_seconds integer NOT NULL CHECK(recheck_seconds BETWEEN 30 AND 604800),
 approved_role name NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),enabled boolean NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX runtime_dataset_policy_dataset ON moria.runtime_dataset_retention_policies(environment,dataset,incarnation) WHERE enabled;
CREATE TABLE moria.runtime_dataset_expired_roots(
 root text PRIMARY KEY REFERENCES moria.runtime_dataset_roots(key),policy uuid NOT NULL REFERENCES moria.runtime_dataset_retention_policies(id),
 expired_at timestamptz NOT NULL DEFAULT clock_timestamp(),maintenance uuid NOT NULL
);
CREATE TABLE moria.runtime_dataset_gc_candidates(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),policy uuid NOT NULL REFERENCES moria.runtime_dataset_retention_policies(id),
 physical_fingerprint text NOT NULL,warehouse_id text NOT NULL,table_id text NOT NULL,object_id text NOT NULL,
 producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),
 orphaned_at timestamptz NOT NULL DEFAULT clock_timestamp(),not_before timestamptz NOT NULL DEFAULT clock_timestamp()+interval '24 hours',
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','watching','unsupported')),
 owner uuid,fence bigint NOT NULL DEFAULT 0,incarnation uuid,lease_until timestamptz,admission_pid integer,admission_started timestamptz,
 expected_version jsonb,next_check timestamptz NOT NULL DEFAULT clock_timestamp(),attempts bigint NOT NULL DEFAULT 0,last_result text,last_removed_version jsonb,
 UNIQUE(policy,physical_fingerprint,warehouse_id,table_id,object_id),
 FOREIGN KEY(physical_fingerprint,warehouse_id,table_id,object_id) REFERENCES moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id),
 CHECK(not_before>=orphaned_at+interval '24 hours')
);
CREATE INDEX runtime_dataset_gc_due ON moria.runtime_dataset_gc_candidates(policy,next_check,id);
CREATE FUNCTION moria.runtime_dataset_root_key(location text) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT encode(sha256(convert_to(location,'UTF8')),'hex')
$$;
CREATE FUNCTION moria.runtime_dataset_root_assert(location text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('moria/recovery-retention/v1',0)) THEN RAISE EXCEPTION 'GC_ADMISSION_BUSY'; END IF;
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_expired_roots WHERE root=moria.runtime_dataset_root_key(location)) THEN
   RAISE EXCEPTION 'DATASET_SNAPSHOT_EXPIRED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_root_pin(env text,resource text,at_revision bigint,kind text,owner text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE location text;
BEGIN
 IF kind IS NULL OR owner IS NULL OR length(kind) NOT BETWEEN 1 AND 64 OR length(owner) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'GC_PIN_INVALID'; END IF;
 SELECT metadata_location INTO location FROM moria.snapshot_history WHERE environment=env AND dataset=resource AND revision=at_revision;
 IF NOT FOUND THEN RAISE EXCEPTION 'INPUT_PIN_CHANGED'; END IF;
 PERFORM moria.runtime_dataset_root_assert(location);
 INSERT INTO moria.runtime_dataset_root_pins(root_key,owner_kind,owner_id,environment,dataset,revision)
 VALUES(moria.runtime_dataset_root_key(location),kind,owner,env,resource,at_revision) ON CONFLICT DO NOTHING;
END $$;
CREATE FUNCTION moria.runtime_dataset_producer_scope(execution uuid,task uuid,owner uuid,fence bigint,incarnation uuid,warehouse text,table_name text,revision text)
 RETURNS text LANGUAGE sql IMMUTABLE AS $$
 SELECT 'dataset/'||execution::text||'/'||COALESCE(task::text,'')||'/'||owner::text||'/'||fence::text||'/'||incarnation::text||'/'||warehouse||'/'||table_name||'/'||revision
$$;
CREATE FUNCTION moria.runtime_dataset_gc_io_check(execution uuid,task uuid,owner uuid,fence bigint,incarnation uuid,warehouse text,table_name text,revision text)
 RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
#variable_conflict use_variable
BEGIN
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_producers WHERE scope=moria.runtime_dataset_producer_scope(execution,task,owner,fence,incarnation,warehouse,table_name,revision)
   AND tombstoned_at IS NOT NULL) THEN RAISE EXCEPTION 'GC_PRODUCER_TOMBSTONED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_issue(session_hash text,execution uuid,task uuid,owner uuid,fence bigint,incarnation uuid,
 warehouse text,table_name text,revision text,physical text,object_name text,size bigint,digest text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
#variable_conflict use_variable
DECLARE state jsonb;scope_key text;previous moria.runtime_dataset_issued_objects;producer_state moria.runtime_dataset_producers;
BEGIN
 IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('moria/recovery-retention/v1',0)) THEN RAISE EXCEPTION 'GC_ADMISSION_BUSY'; END IF;
 state:=moria.runtime_dataset_io_check(session_hash,execution,task,owner,fence,incarnation,warehouse,table_name,revision,true);
 -- Only an admitted managed Dataset binding establishes ownership eligible for physical reclamation.
 IF state->'datasetBinding' IS NULL OR state->'datasetBinding'='null'::jsonb THEN RETURN; END IF;
 IF state->'datasetBinding'->>'physicalBindingFingerprint' IS DISTINCT FROM physical OR size<0 OR digest !~ '^sha256:[0-9a-f]{64}$'
   OR object_name='moria_restore_target_custody_v1' OR object_name !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$' THEN RAISE EXCEPTION 'GC_OBJECT_DENIED'; END IF;
 scope_key:=moria.runtime_dataset_producer_scope(execution,task,owner,fence,incarnation,warehouse,table_name,revision);
 INSERT INTO moria.runtime_dataset_producers(scope,execution,task,owner,fence,incarnation,namespace,physical_fingerprint)
 VALUES(scope_key,execution,task,owner,fence,incarnation,jsonb_build_object('warehouseId',warehouse,'tableId',table_name,'revision',revision),physical) ON CONFLICT DO NOTHING;
 SELECT * INTO producer_state FROM moria.runtime_dataset_producers WHERE scope=scope_key FOR SHARE;
 IF producer_state.tombstoned_at IS NOT NULL OR producer_state.physical_fingerprint<>physical THEN RAISE EXCEPTION 'GC_PRODUCER_TOMBSTONED'; END IF;
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_gc_candidates WHERE physical_fingerprint=physical AND warehouse_id=warehouse AND table_id=table_name AND object_id=object_name)
   THEN RAISE EXCEPTION 'GC_OBJECT_RETIRED'; END IF;
 INSERT INTO moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id,producer,expected_size,expected_sha256)
 VALUES(physical,warehouse,table_name,object_name,scope_key,size,digest) ON CONFLICT DO NOTHING;
 SELECT * INTO previous FROM moria.runtime_dataset_issued_objects WHERE physical_fingerprint=physical AND warehouse_id=warehouse AND table_id=table_name AND object_id=object_name;
 IF previous.producer<>scope_key OR previous.expected_size<>size OR previous.expected_sha256<>digest THEN RAISE EXCEPTION 'GC_OBJECT_OWNERSHIP_CHANGED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_object_read(warehouse text,table_name text,object_name text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_expired_roots e ON e.root=r.key
  WHERE r.root->'root'->'object'->>'warehouseId'=warehouse AND r.root->'root'->'object'->>'tableId'=table_name AND r.root->'root'->'object'->>'objectId'=object_name)
  THEN RAISE EXCEPTION 'DATASET_SNAPSHOT_EXPIRED'; END IF;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_gc_io_check(uuid,uuid,uuid,bigint,uuid,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_issue(text,uuid,uuid,uuid,bigint,uuid,text,text,text,text,text,bigint,text) FROM PUBLIC;
-- The public entry validates the actual current native lease before invoking the private issuer.
CREATE FUNCTION moria.runtime_dataset_admit_write(session_hash text,execution uuid,task uuid,owner uuid,fence bigint,incarnation uuid,
 warehouse text,table_name text,revision text,physical text,object_name text,size bigint,digest text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM moria.runtime_dataset_issue(session_hash,execution,task,owner,fence,incarnation,warehouse,table_name,revision,physical,object_name,size,digest);
END $$;

CREATE FUNCTION moria.runtime_dataset_producer_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-'tombstoned_at') IS DISTINCT FROM (to_jsonb(OLD)-'tombstoned_at')
   OR OLD.tombstoned_at IS NOT NULL OR NEW.tombstoned_at IS NULL OR NEW.tombstoned_at>clock_timestamp()
 THEN RAISE EXCEPTION 'IMMUTABLE_DATASET_PRODUCER'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_producer_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_producers FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_producer_immutable();
CREATE FUNCTION moria.runtime_dataset_root_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-'sealed') IS DISTINCT FROM (to_jsonb(OLD)-'sealed') OR OLD.sealed OR NOT NEW.sealed
 THEN RAISE EXCEPTION 'IMMUTABLE_DATASET_ROOT'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_root_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_roots FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_root_immutable();
CREATE TRIGGER runtime_dataset_issued_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_issued_objects FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_member_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_root_members FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_pin_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_root_pins FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_expired_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_expired_roots FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_policy_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_retention_policies FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_policy_immutable();
CREATE FUNCTION moria.runtime_dataset_gc_candidate_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.id,NEW.policy,NEW.physical_fingerprint,NEW.warehouse_id,NEW.table_id,NEW.object_id,NEW.producer,NEW.orphaned_at,NEW.not_before)
 IS DISTINCT FROM ROW(OLD.id,OLD.policy,OLD.physical_fingerprint,OLD.warehouse_id,OLD.table_id,OLD.object_id,OLD.producer,OLD.orphaned_at,OLD.not_before)
 OR NEW.fence<OLD.fence OR NEW.attempts<OLD.attempts THEN RAISE EXCEPTION 'IMMUTABLE_GC_CANDIDATE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_gc_candidate_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_gc_candidates FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_gc_candidate_immutable();

CREATE TABLE moria.runtime_dataset_maintenance_runs(
 id uuid PRIMARY KEY,request_key text NOT NULL UNIQUE,request_hash text NOT NULL,policy uuid NOT NULL REFERENCES moria.runtime_dataset_retention_policies(id),
 owner uuid NOT NULL,fence bigint NOT NULL CHECK(fence>0),incarnation uuid NOT NULL,lease_until timestamptz NOT NULL,deadline timestamptz NOT NULL,
 expected_revision bigint NOT NULL,old_metadata text NOT NULL,status text NOT NULL CHECK(status IN ('preparing','committed','failed')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,new_metadata text,receipt jsonb,error text
);
ALTER TABLE moria.runtime_dataset_producers ADD FOREIGN KEY(maintenance) REFERENCES moria.runtime_dataset_maintenance_runs(id);
CREATE FUNCTION moria.runtime_dataset_policy_check(policy_id uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE p moria.runtime_dataset_retention_policies;b moria.native_dataset_bindings;n moria.native_storage_namespaces;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 PERFORM moria.runtime_recovery_check();
 SELECT * INTO p FROM moria.runtime_dataset_retention_policies WHERE id=policy_id;
 IF NOT FOUND OR NOT p.enabled OR p.incarnation<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'GC_POLICY_CHANGED'; END IF;
 PERFORM moria.runtime_dataset_kind(p.environment,p.dataset);
 SELECT * INTO b FROM moria.native_dataset_bindings WHERE environment=p.environment AND resource=p.dataset::uuid;
 SELECT * INTO n FROM moria.native_storage_namespaces WHERE environment=p.environment AND resource=p.dataset
   AND warehouse_id=p.namespace->>'warehouseId' AND table_id=p.namespace->>'tableId';
 IF NOT FOUND OR NOT n.enabled OR n.purpose<>'dataset-source' OR n.revision IS DISTINCT FROM p.namespace->>'revision'
   OR NOT b.enabled OR b.revision IS DISTINCT FROM p.binding_revision OR b.source_digest IS DISTINCT FROM p.source_digest
   OR b.warehouse_id<>n.warehouse_id OR b.table_id<>n.table_id OR b.namespace_revision<>n.revision
   OR b.physical_binding_fingerprint<>p.physical_fingerprint THEN RAISE EXCEPTION 'GC_POLICY_CHANGED'; END IF;
 RETURN jsonb_build_object('environment',p.environment,'dataset',p.dataset,'namespace',p.namespace,'physicalFingerprint',p.physical_fingerprint,
  'retainSeconds',p.retain_seconds::text,'recheckSeconds',p.recheck_seconds,'icebergSchema',b.iceberg_schema,'tableUuid',n.iceberg_uuid::text,'keySchemaHash',b.key_schema_hash,'primaryKey',to_jsonb(b.primary_key));
END $$;
CREATE FUNCTION moria.runtime_dataset_maintenance_check(run_id uuid,worker uuid,worker_fence bigint,instance uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE r moria.runtime_dataset_maintenance_runs;state jsonb;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 SELECT * INTO r FROM moria.runtime_dataset_maintenance_runs WHERE id=run_id FOR SHARE;
 IF NOT FOUND OR r.status<>'preparing' OR r.owner IS DISTINCT FROM worker OR r.fence<>worker_fence OR r.incarnation IS DISTINCT FROM instance
   OR r.lease_until<=clock_timestamp() OR instance<>(SELECT incarnation FROM moria.indexing_instance) THEN RAISE EXCEPTION 'GC_LEASE_LOST'; END IF;
 state:=moria.runtime_dataset_policy_check(r.policy);
 RETURN state||jsonb_build_object('expectedRevision',r.expected_revision::text,'oldMetadata',r.old_metadata);
END $$;
CREATE FUNCTION moria.runtime_dataset_maintenance_issue(run_id uuid,worker uuid,worker_fence bigint,instance uuid,physical text,
 object_name text,size bigint,digest text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE state jsonb;scope_key text;previous moria.runtime_dataset_issued_objects;
BEGIN
 state:=moria.runtime_dataset_maintenance_check(run_id,worker,worker_fence,instance);
 IF state->>'physicalFingerprint' IS DISTINCT FROM physical OR object_name='moria_restore_target_custody_v1' OR object_name !~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'
  OR size<0 OR digest !~ '^sha256:[0-9a-f]{64}$' THEN RAISE EXCEPTION 'GC_OBJECT_DENIED'; END IF;
 scope_key:='dataset-maintenance/'||run_id::text||'/'||worker::text||'/'||worker_fence::text||'/'||instance::text;
 INSERT INTO moria.runtime_dataset_producers(scope,maintenance,owner,fence,incarnation,namespace,physical_fingerprint)
 VALUES(scope_key,run_id,worker,worker_fence,instance,state->'namespace',physical) ON CONFLICT DO NOTHING;
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_producers WHERE scope=scope_key AND tombstoned_at IS NOT NULL) THEN RAISE EXCEPTION 'GC_PRODUCER_TOMBSTONED'; END IF;
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_gc_candidates WHERE physical_fingerprint=physical AND warehouse_id=state->'namespace'->>'warehouseId'
   AND table_id=state->'namespace'->>'tableId' AND object_id=object_name) THEN RAISE EXCEPTION 'GC_OBJECT_RETIRED'; END IF;
 INSERT INTO moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id,producer,expected_size,expected_sha256)
 VALUES(physical,state->'namespace'->>'warehouseId',state->'namespace'->>'tableId',object_name,scope_key,size,digest) ON CONFLICT DO NOTHING;
 SELECT * INTO previous FROM moria.runtime_dataset_issued_objects WHERE physical_fingerprint=physical AND warehouse_id=state->'namespace'->>'warehouseId'
   AND table_id=state->'namespace'->>'tableId' AND object_id=object_name;
 IF previous.producer<>scope_key OR previous.expected_size<>size OR previous.expected_sha256<>digest THEN RAISE EXCEPTION 'GC_OBJECT_OWNERSHIP_CHANGED'; END IF;
END $$;
CREATE FUNCTION moria.runtime_dataset_root_publication_check() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.metadata_location IS NOT NULL THEN
  PERFORM moria.runtime_dataset_root_assert(NEW.metadata_location);
  IF EXISTS(SELECT 1 FROM moria.runtime_dataset_retention_policies p JOIN moria.indexing_instance i ON i.incarnation=p.incarnation
    WHERE p.environment=NEW.environment AND p.dataset=NEW.dataset AND p.enabled)
   AND NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_roots r JOIN moria.runtime_dataset_root_locations l ON l.root=r.key
     JOIN moria.runtime_dataset_retention_policies p ON p.environment=NEW.environment AND p.dataset=NEW.dataset AND p.enabled
     JOIN moria.indexing_instance i ON i.incarnation=p.incarnation AND i.incarnation=l.incarnation
     WHERE r.key=moria.runtime_dataset_root_key(NEW.metadata_location) AND r.environment=NEW.environment AND r.dataset=NEW.dataset
       AND r.metadata_location=NEW.metadata_location AND r.sealed AND l.sealed AND l.physical_fingerprint=p.physical_fingerprint)
   THEN RAISE EXCEPTION 'GC_UNTRACKED_ROOT'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_head_gc_admission BEFORE INSERT OR UPDATE ON moria.dataset_heads FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_root_publication_check();
CREATE TRIGGER runtime_dataset_history_gc_admission BEFORE INSERT ON moria.snapshot_history FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_root_publication_check();
CREATE FUNCTION moria.runtime_dataset_indexing_pin_admit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM moria.runtime_dataset_root_pin(NEW.environment,NEW.source,NEW.source_revision,'indexing',NEW.id::text);
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_indexing_dataset_root_pins AFTER INSERT ON moria.indexing_builds FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_indexing_pin_admit();
CREATE FUNCTION moria.runtime_dataset_root_pinned_except(root_key text,own_maintenance uuid) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE p moria.runtime_dataset_root_pins;
BEGIN
 FOR p IN SELECT * FROM moria.runtime_dataset_root_pins WHERE runtime_dataset_root_pins.root_key=runtime_dataset_root_pinned_except.root_key LOOP
  IF p.owner_kind='execution' THEN
    IF moria.runtime_dataset_execution_pin_live(p.owner_id) THEN RETURN true; END IF;
  ELSIF p.owner_kind='indexing' THEN
    IF EXISTS(SELECT 1 FROM moria.indexing_builds WHERE id=p.owner_id::uuid AND status IN ('pending','planning','running','preparing','ready')) THEN RETURN true; END IF;
  ELSIF p.owner_kind='maintenance' THEN
    IF p.owner_id=own_maintenance::text THEN CONTINUE; END IF;
    IF EXISTS(SELECT 1 FROM moria.runtime_dataset_maintenance_runs WHERE id=p.owner_id::uuid AND status='preparing' AND lease_until>clock_timestamp()) THEN RETURN true; END IF;
  ELSE RETURN true;
  END IF;
 END LOOP;
 RETURN false;
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_policy_check(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_maintenance_check(uuid,uuid,bigint,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_maintenance_issue(uuid,uuid,bigint,uuid,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION moria.runtime_dataset_root_pinned(root_key text) RETURNS boolean LANGUAGE sql AS $$
 SELECT moria.runtime_dataset_root_pinned_except(root_key,NULL)
$$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_root_pinned_except(text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_root_pinned(text) FROM PUBLIC;

CREATE TABLE moria.runtime_dataset_gc_plans(
 id uuid PRIMARY KEY,request_key text NOT NULL UNIQUE,request_hash text NOT NULL,policy uuid NOT NULL REFERENCES moria.runtime_dataset_retention_policies(id),
 approved_role name NOT NULL,incarnation uuid NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),receipt jsonb NOT NULL
);
CREATE TABLE moria.runtime_dataset_gc_versions(
 candidate uuid NOT NULL REFERENCES moria.runtime_dataset_gc_candidates(id),version_key text NOT NULL CHECK(version_key ~ '^[0-9a-f]{64}$'),
 version jsonb NOT NULL,result text NOT NULL CHECK(result IN ('removed','absent')),observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(candidate,version_key)
);
CREATE TRIGGER runtime_dataset_gc_plan_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_gc_plans FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_gc_version_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_gc_versions FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE FUNCTION moria.runtime_dataset_producer_closed(scope_key text) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE p moria.runtime_dataset_producers;e moria.runtime_executions;r moria.runtime_dataset_maintenance_runs;
BEGIN
 SELECT * INTO p FROM moria.runtime_dataset_producers WHERE scope=scope_key;
 IF NOT FOUND THEN RETURN false; END IF;
 IF p.execution IS NOT NULL THEN
  SELECT * INTO e FROM moria.runtime_executions WHERE id=p.execution;
  IF NOT FOUND THEN RETURN false; END IF;
  IF e.status NOT IN ('queued','running','preparing') THEN RETURN true; END IF;
  -- Sealed task partsets remain coordinator inputs until their actual parent finishes.
  IF p.task IS NOT NULL THEN RETURN false; END IF;
  RETURN e.owner IS DISTINCT FROM p.owner OR e.fence<>p.fence OR (SELECT incarnation FROM moria.runtime_execution_bindings WHERE id=e.binding)<>p.incarnation OR e.deadline<=clock_timestamp() OR e.lease_until IS NULL OR e.lease_until<=clock_timestamp();
 END IF;
 IF p.restore IS NOT NULL THEN
  RETURN EXISTS(SELECT 1 FROM moria.runtime_restores r WHERE r.id=p.restore AND r.new_incarnation=p.incarnation AND r.status='enabled');
 END IF;
 SELECT * INTO r FROM moria.runtime_dataset_maintenance_runs WHERE id=p.maintenance;
 IF NOT FOUND THEN RETURN false; END IF;
 RETURN r.status<>'preparing' OR r.owner IS DISTINCT FROM p.owner OR r.fence<>p.fence OR r.incarnation<>p.incarnation OR r.lease_until<=clock_timestamp();
END $$;
CREATE FUNCTION moria.runtime_dataset_object_referenced(physical text,warehouse text,table_name text,object_name text) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_graph_nodes n JOIN moria.runtime_dataset_graph_qualifications q ON q.node=n.id
  LEFT JOIN moria.runtime_dataset_graph_counts c ON c.node=q.node AND c.physical_fingerprint=q.physical_fingerprint AND c.incarnation=q.incarnation
  WHERE n.content->'object'->>'warehouseId'=warehouse AND n.content->'object'->>'tableId'=table_name AND n.content->'object'->>'objectId'=object_name
   AND q.physical_fingerprint=physical AND q.incarnation=(SELECT incarnation FROM moria.indexing_instance)
   AND (COALESCE(c.live_references,0)>0 OR moria.runtime_dataset_graph_gc_busy(n.environment,n.dataset,physical,q.incarnation))) THEN RETURN true;END IF;
 IF EXISTS(SELECT 1 FROM moria.runtime_dataset_member_locations m
  LEFT JOIN moria.runtime_dataset_expired_roots e ON e.root=m.root WHERE m.warehouse_id=warehouse AND m.table_id=table_name AND m.object_id=object_name
    AND m.physical_fingerprint=physical AND m.incarnation=(SELECT incarnation FROM moria.indexing_instance) AND e.root IS NULL) THEN RETURN true; END IF;
 IF EXISTS(SELECT 1 FROM moria.runtime_recovery_members m JOIN moria.runtime_recovery_sets s ON s.id=m.recovery_set
  WHERE (s.status<>'retired' OR s.retained_until>clock_timestamp()) AND m.warehouse_id=warehouse AND m.table_id=table_name AND m.object_id=object_name
   AND NOT(s.status='ready' AND s.sealed_manifest IS NOT NULL AND s.metadata_dump IS NOT NULL AND s.content_inventory IS NOT NULL
     AND m.backup IS NOT NULL AND m.backup_version IS NOT NULL AND m.backup->>'sha256'=m.content->>'sha256' AND m.backup->>'size'=m.content->>'size'))
 OR EXISTS(SELECT 1 FROM moria.runtime_recovery_outputs o JOIN moria.runtime_recovery_sets s ON s.id=o.recovery_set
  WHERE (s.status<>'retired' OR s.retained_until>clock_timestamp()) AND o.warehouse_id=warehouse AND o.table_id=table_name AND o.object_id=object_name)
 OR EXISTS(SELECT 1 FROM moria.runtime_recovery_members m JOIN moria.runtime_recovery_sets s ON s.id=m.recovery_set
  WHERE (s.status<>'retired' OR s.retained_until>clock_timestamp()) AND m.backup->'object'->>'warehouseId'=warehouse AND m.backup->'object'->>'tableId'=table_name AND m.backup->'object'->>'objectId'=object_name)
 THEN RETURN true; END IF;
 -- A restored missing issued ledger is never evidence of abandonment on a shared external backend.
 IF EXISTS(SELECT 1 FROM moria.runtime_recovery_backends b JOIN moria.runtime_recovery_sets s ON s.id=b.recovery_set
   WHERE s.incarnation<>(SELECT incarnation FROM moria.indexing_instance) AND b.physical_fingerprint=physical
     AND (s.status<>'retired' OR s.retained_until>clock_timestamp())) THEN RAISE EXCEPTION 'GC_FOREIGN_CUSTODY'; END IF;
 RETURN false;
END $$;
CREATE FUNCTION moria.runtime_dataset_gc_admission_held(c moria.runtime_dataset_gc_candidates) RETURNS boolean LANGUAGE sql STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
  WHERE a.pid=c.admission_pid AND a.backend_start=c.admission_started AND a.datname=current_database()
   AND l.locktype='advisory' AND l.mode='ExclusiveLock' AND l.granted AND l.objsubid=1
   AND l.classid=((hashtextextended('moria/dataset-gc/'||c.id::text||'/'||c.owner::text||'/'||c.fence::text||'/'||c.incarnation::text,0)>>32)&4294967295)::oid
   AND l.objid=(hashtextextended('moria/dataset-gc/'||c.id::text||'/'||c.owner::text||'/'||c.fence::text||'/'||c.incarnation::text,0)&4294967295)::oid)
$$;
CREATE FUNCTION moria.runtime_dataset_gc_claim_check(claim_id uuid,worker uuid,worker_fence bigint,instance uuid) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE c moria.runtime_dataset_gc_candidates;p moria.runtime_dataset_producers;o moria.runtime_dataset_issued_objects;state jsonb;
BEGIN
 PERFORM moria.runtime_recovery_operator();
 SELECT * INTO c FROM moria.runtime_dataset_gc_candidates WHERE id=claim_id FOR SHARE;
 IF NOT FOUND OR c.status<>'running' OR c.owner IS DISTINCT FROM worker OR c.fence IS DISTINCT FROM worker_fence
   OR c.incarnation IS DISTINCT FROM instance OR instance<>(SELECT incarnation FROM moria.indexing_instance)
   OR c.lease_until IS NULL OR c.lease_until<=clock_timestamp() OR c.not_before>clock_timestamp()
   OR NOT moria.runtime_dataset_gc_admission_held(c) THEN RAISE EXCEPTION 'GC_LEASE_LOST'; END IF;
 state:=moria.runtime_dataset_policy_check(c.policy);
 SELECT * INTO p FROM moria.runtime_dataset_producers WHERE scope=c.producer;
 SELECT * INTO o FROM moria.runtime_dataset_issued_objects WHERE physical_fingerprint=c.physical_fingerprint AND warehouse_id=c.warehouse_id AND table_id=c.table_id AND object_id=c.object_id;
 IF p.tombstoned_at IS NULL OR p.incarnation<>instance OR o.producer IS DISTINCT FROM p.scope
  OR c.physical_fingerprint IS DISTINCT FROM state->>'physicalFingerprint' OR c.warehouse_id IS DISTINCT FROM state->'namespace'->>'warehouseId'
  OR c.table_id IS DISTINCT FROM state->'namespace'->>'tableId' THEN RAISE EXCEPTION 'GC_POLICY_CHANGED'; END IF;
 IF moria.runtime_dataset_object_referenced(c.physical_fingerprint,c.warehouse_id,c.table_id,c.object_id) THEN RAISE EXCEPTION 'GC_REFERENCE_PRESENT'; END IF;
 RETURN jsonb_build_object('object',jsonb_build_object('version',1,'warehouseId',c.warehouse_id,'tableId',c.table_id,'objectId',c.object_id),
  'namespace',state->'namespace','physicalFingerprint',c.physical_fingerprint,'expectedVersion',c.expected_version,
  'expectedSize',o.expected_size::text,'expectedSha256',o.expected_sha256,'recheckSeconds',state->'recheckSeconds');
END $$;
REVOKE ALL ON FUNCTION moria.runtime_dataset_producer_closed(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_object_referenced(text,text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_gc_admission_held(moria.runtime_dataset_gc_candidates) FROM PUBLIC;
REVOKE ALL ON FUNCTION moria.runtime_dataset_gc_claim_check(uuid,uuid,bigint,uuid) FROM PUBLIC;

-- Control-plane credentials cannot self-approve physical reclamation through direct table DML.
CREATE FUNCTION moria.runtime_retention_control_guard() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 PERFORM moria.runtime_recovery_operator();
 IF TG_OP='INSERT' AND TG_TABLE_NAME IN ('runtime_retention_policies','runtime_gc_retirements','runtime_dataset_retention_policies','runtime_dataset_gc_plans') THEN
   IF NEW.approved_role IS DISTINCT FROM session_user THEN RAISE EXCEPTION 'GC_OPERATOR_REQUIRED'; END IF;
 END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER runtime_retention_policies_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_retention_policies
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_gc_retirements_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_gc_retirements
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_gc_candidates_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_gc_candidates
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_retention_policies_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_retention_policies
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_maintenance_runs_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_maintenance_runs
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_expired_roots_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_expired_roots
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_gc_candidates_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_gc_candidates
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_gc_plans_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_gc_plans
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_gc_versions_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_gc_versions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_producer_retirement_operator BEFORE UPDATE OF tombstoned_at ON moria.runtime_dataset_producers
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE FUNCTION moria.runtime_dataset_qualification_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR to_jsonb(NEW)-'sealed' IS DISTINCT FROM to_jsonb(OLD)-'sealed' OR (OLD.sealed AND NOT NEW.sealed)
 THEN RAISE EXCEPTION 'IMMUTABLE_RECOVERY_CONTENT'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_qualification_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_root_qualifications
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_qualification_immutable();
CREATE TRIGGER runtime_dataset_qualified_member_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_qualified_members
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_root_qualifications_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_root_qualifications
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TRIGGER runtime_dataset_qualified_members_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_qualified_members
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE FUNCTION moria.runtime_dataset_restore_issue_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
BEGIN
 IF TG_TABLE_NAME='runtime_dataset_producers' THEN
  IF NEW.restore IS NOT NULL THEN PERFORM moria.runtime_recovery_operator();END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM moria.runtime_dataset_producers WHERE scope=NEW.producer AND restore IS NOT NULL)
   THEN PERFORM moria.runtime_recovery_operator(); END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_dataset_restored_producer_operator BEFORE INSERT ON moria.runtime_dataset_producers
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_restore_issue_guard();
CREATE TRIGGER runtime_dataset_restored_issued_operator BEFORE INSERT ON moria.runtime_dataset_issued_objects
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_restore_issue_guard();
CREATE TABLE moria.runtime_dataset_gc_multipart(
 candidate uuid NOT NULL REFERENCES moria.runtime_dataset_gc_candidates(id),upload_id text NOT NULL CHECK(octet_length(upload_id) BETWEEN 1 AND 2048),
 result text NOT NULL CHECK(result IN ('aborted','absent')),owner uuid NOT NULL,fence bigint NOT NULL,incarnation uuid NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(candidate,upload_id)
);
CREATE TRIGGER runtime_dataset_gc_multipart_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_gc_multipart
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE TRIGGER runtime_dataset_gc_multipart_operator BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_gc_multipart
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_retention_control_guard();
CREATE TABLE moria.runtime_dataset_multipart_uploads(
 producer text NOT NULL REFERENCES moria.runtime_dataset_producers(scope),physical_fingerprint text NOT NULL,
 warehouse_id text NOT NULL,table_id text NOT NULL,object_id text NOT NULL,upload_id text NOT NULL CHECK(octet_length(upload_id) BETWEEN 1 AND 2048 AND upload_id~'^[!-~]+$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(producer,object_id,upload_id),
 FOREIGN KEY(physical_fingerprint,warehouse_id,table_id,object_id) REFERENCES moria.runtime_dataset_issued_objects(physical_fingerprint,warehouse_id,table_id,object_id)
);
CREATE INDEX runtime_dataset_multipart_object ON moria.runtime_dataset_multipart_uploads(physical_fingerprint,warehouse_id,table_id,object_id,upload_id);
CREATE TRIGGER runtime_dataset_multipart_immutable BEFORE UPDATE OR DELETE ON moria.runtime_dataset_multipart_uploads
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_recovery_content_immutable();
CREATE FUNCTION moria.runtime_dataset_multipart_mutation() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,moria AS $$
BEGIN
 IF NOT has_schema_privilege(current_user,'moria','CREATE') THEN RAISE EXCEPTION 'GC_OBJECT_DENIED';END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$;
CREATE TRIGGER runtime_dataset_producer_issue_admission BEFORE INSERT ON moria.runtime_dataset_producers
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_multipart_mutation();
CREATE TRIGGER runtime_dataset_object_issue_admission BEFORE INSERT ON moria.runtime_dataset_issued_objects
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_multipart_mutation();
CREATE TRIGGER runtime_dataset_multipart_admission BEFORE INSERT OR UPDATE OR DELETE ON moria.runtime_dataset_multipart_uploads
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_dataset_multipart_mutation();
CREATE FUNCTION moria.runtime_dataset_record_multipart(session_hash text,execution uuid,task uuid,owner uuid,fence bigint,incarnation uuid,
 warehouse text,table_name text,revision text,physical text,object_name text,provider_upload text) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
#variable_conflict use_variable
DECLARE state jsonb;scope_key text;
BEGIN
 IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('moria/recovery-retention/v1',0)) THEN RAISE EXCEPTION 'GC_ADMISSION_BUSY'; END IF;
 state:=moria.runtime_dataset_io_check(session_hash,execution,task,owner,fence,incarnation,warehouse,table_name,revision,true);
 IF state->'datasetBinding' IS NULL OR state->'datasetBinding'='null'::jsonb THEN RETURN;END IF;
 IF state->'datasetBinding'->>'physicalBindingFingerprint' IS DISTINCT FROM physical OR provider_upload IS NULL OR octet_length(provider_upload) NOT BETWEEN 1 AND 2048 OR provider_upload!~'^[!-~]+$'
  THEN RAISE EXCEPTION 'GC_OBJECT_DENIED'; END IF;
 scope_key:=moria.runtime_dataset_producer_scope(execution,task,owner,fence,incarnation,warehouse,table_name,revision);
 IF NOT EXISTS(SELECT 1 FROM moria.runtime_dataset_issued_objects o JOIN moria.runtime_dataset_producers p ON p.scope=o.producer
  WHERE o.physical_fingerprint=physical AND o.warehouse_id=warehouse AND o.table_id=table_name AND o.object_id=object_name
    AND o.producer=scope_key AND p.tombstoned_at IS NULL) THEN RAISE EXCEPTION 'GC_OBJECT_OWNERSHIP_CHANGED'; END IF;
 INSERT INTO moria.runtime_dataset_multipart_uploads(producer,physical_fingerprint,warehouse_id,table_id,object_id,upload_id)
 VALUES(scope_key,physical,warehouse,table_name,object_name,provider_upload) ON CONFLICT DO NOTHING;
END $$;
`;
