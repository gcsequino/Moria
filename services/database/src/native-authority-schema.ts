/** Logical storage namespaces are trusted bindings; readers cannot manufacture write grants. */
export const nativeAuthoritySchema = String.raw`
CREATE FUNCTION moria.runtime_schema_admit(expected_version integer, expected_checksum text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT pg_try_advisory_lock_shared(731804135219) THEN RAISE EXCEPTION 'MIGRATION_IN_PROGRESS'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.schema_version v WHERE v.singleton AND v.version=expected_version AND v.checksum=expected_checksum) THEN
    RAISE EXCEPTION 'INCOMPATIBLE_SCHEMA';
  END IF;
END $$;
CREATE FUNCTION moria.runtime_connection_admit(expected_version integer,expected_checksum text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT moria.runtime_dispatch_bootstrap_approved() THEN RAISE EXCEPTION 'RESTORE_DISPATCH_DISABLED'; END IF;
  PERFORM moria.runtime_schema_admit(expected_version,expected_checksum);
END $$;
CREATE FUNCTION moria.indexing_claim_check(build_id uuid,task_id uuid,worker_id uuid,worker_fence bigint) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.indexing_check(build_id);
  IF task_id IS NULL THEN
    IF NOT EXISTS(SELECT 1 FROM moria.indexing_builds WHERE id=build_id AND owner=worker_id AND fence=worker_fence AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
  ELSIF NOT EXISTS(SELECT 1 FROM moria.indexing_tasks WHERE id=task_id AND build=build_id AND owner=worker_id AND fence=worker_fence AND status='running' AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'LEASE_LOST'; END IF;
END $$;
CREATE TABLE moria.native_service_peers (
  fingerprint text PRIMARY KEY CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
  owner uuid NOT NULL, cell uuid REFERENCES moria.runtime_cells(id),
  roles text[] NOT NULL CHECK(cardinality(roles) BETWEEN 1 AND 32),
  incarnation uuid NOT NULL, enabled boolean NOT NULL DEFAULT true,
  expires_at timestamptz NOT NULL
);
CREATE TABLE moria.native_storage_namespaces (
  environment text NOT NULL REFERENCES moria.environments(id),
  warehouse_id text NOT NULL, table_id text NOT NULL,
  resource text NOT NULL, revision text NOT NULL,
  purpose text NOT NULL CHECK(purpose IN ('dataset-source','index-artifact')),
  enabled boolean NOT NULL DEFAULT true,
  iceberg_uuid uuid, markings text[] NOT NULL DEFAULT '{}',
  PRIMARY KEY(warehouse_id,table_id)
);
CREATE FUNCTION moria.native_storage_check(build_id uuid,task_id uuid,worker_id uuid,worker_fence bigint,
  warehouse text,table_name text,namespace_revision text,for_write boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM moria.indexing_claim_check(build_id,task_id,worker_id,worker_fence);
  IF NOT EXISTS(SELECT 1 FROM moria.native_storage_namespaces n JOIN moria.indexing_builds b ON b.id=build_id
    WHERE n.environment=b.environment AND n.warehouse_id=warehouse AND n.table_id=table_name
      AND n.revision=namespace_revision AND n.enabled
      AND n.markings <@ ARRAY(SELECT jsonb_array_elements_text(b.definition->'markings'))
      AND (NOT for_write OR ARRAY(SELECT jsonb_array_elements_text(b.definition->'markings')) <@ n.markings)
      AND ((n.purpose='index-artifact' AND n.resource=b.target)
        OR (NOT for_write AND n.purpose='dataset-source' AND n.resource=b.source))) THEN
    RAISE EXCEPTION 'STORAGE_NAMESPACE_REVOKED';
  END IF;
END $$;
`;
