import { filesystemRoot } from '../../../packages/contracts/filesystem.js';
import { identityRuntimeSchema } from './identity-runtime-schema.js';
import { executionRuntimeSchema } from './execution-runtime-schema.js';
import { nativeAuthoritySchema } from './native-authority-schema.js';
import { servingRuntimeSchema, servingFunctionReferenceSchema } from './serving-runtime-schema.js';
import { browserOriginSchema } from './browser-origin-schema.js';
import { browserSessionRevocationSchema } from './browser-session-revocation-schema.js';
import { deliveryRuntimeSchema } from './delivery-runtime-schema.js';
import { snapshotRuntimeSchema } from './snapshot-runtime-schema.js';
import { functionRuntimeSchema } from './function-runtime-schema.js';

/** Fresh baseline. Existing instances require an explicit migration. */
export const baseline = `
      CREATE SCHEMA IF NOT EXISTS moria;
      CREATE TABLE IF NOT EXISTS moria.environments (
        id text PRIMARY KEY, revision bigint NOT NULL DEFAULT 0, security_epoch bigint NOT NULL DEFAULT 0
      );
      CREATE TABLE moria.fs_entries (
        id uuid PRIMARY KEY,
        parent_id uuid REFERENCES moria.fs_entries(id),
        name text COLLATE "C" NOT NULL,
        kind text NOT NULL CHECK(kind IN ('directory','file','resource')),
        revision bigint NOT NULL DEFAULT 1 CHECK(revision > 0),
        project_id text,
        environment_id text REFERENCES moria.environments(id),
        resource_id uuid,
        content jsonb,
        deleted boolean NOT NULL DEFAULT false,
        CHECK(parent_id IS NULL OR parent_id <> id),
        CHECK((id='${filesystemRoot}' AND parent_id IS NULL AND name='' AND kind='directory'
          AND project_id IS NULL AND environment_id IS NULL AND NOT deleted)
          OR (id<>'${filesystemRoot}' AND parent_id IS NOT NULL AND name<>'')),
        CHECK(environment_id IS NULL OR project_id IS NOT NULL),
        CHECK((kind='directory' AND content IS NULL AND resource_id IS NULL)
          OR (kind='file' AND content IS NOT NULL AND resource_id IS NULL)
          OR (kind='resource' AND content IS NULL AND resource_id IS NOT NULL
            AND project_id IS NOT NULL AND environment_id IS NOT NULL))
      );
      CREATE UNIQUE INDEX fs_live_names ON moria.fs_entries(parent_id,name) WHERE NOT deleted;
      CREATE UNIQUE INDEX fs_resource_placement ON moria.fs_entries(environment_id,resource_id)
        WHERE kind='resource' AND NOT deleted;
      CREATE TABLE moria.fs_mutations (
        id uuid PRIMARY KEY, scope_hash text NOT NULL, request_key text NOT NULL,
        request_hash text NOT NULL, actor jsonb NOT NULL, receipt jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        UNIQUE(scope_hash,request_key)
      );
      CREATE TABLE moria.fs_entry_versions (
        entry_id uuid NOT NULL REFERENCES moria.fs_entries(id), revision bigint NOT NULL,
        snapshot jsonb NOT NULL, mutation_id uuid REFERENCES moria.fs_mutations(id)
          DEFERRABLE INITIALLY DEFERRED,
        PRIMARY KEY(entry_id,revision)
      );
      CREATE INDEX fs_version_content_locations ON moria.fs_entry_versions((snapshot->'content'->'location'));
      INSERT INTO moria.fs_entries(id,name,kind) VALUES('${filesystemRoot}','','directory');
      INSERT INTO moria.fs_entry_versions(entry_id,revision,snapshot)
        SELECT id,revision,jsonb_build_object('id',id,'parentId',NULL,'name',name,'kind',kind,
          'revision',revision::text,'projectId',NULL,'environmentId',NULL,'resourceId',NULL,
          'content',NULL,'deleted',false) FROM moria.fs_entries;
      CREATE FUNCTION moria.identity_key_hash(value text) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
        SELECT pg_catalog.sha256(pg_catalog.convert_to(value,'UTF8'))
      $$;
      CREATE TABLE IF NOT EXISTS moria.identities (
        domain text COLLATE "C" NOT NULL, canonical_key text COLLATE "C" NOT NULL, uid uuid NOT NULL UNIQUE,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        key_hash bytea GENERATED ALWAYS AS (moria.identity_key_hash(canonical_key)) STORED,
        PRIMARY KEY(domain,key_hash)
      );
      CREATE TABLE IF NOT EXISTS moria.leases (
        environment text NOT NULL REFERENCES moria.environments(id), execution text NOT NULL,
        worker text NOT NULL, fence bigint NOT NULL, expires_at timestamptz NOT NULL,
        PRIMARY KEY(environment,execution)
      );
      CREATE TABLE IF NOT EXISTS moria.dataset_heads (
        environment text NOT NULL REFERENCES moria.environments(id), dataset text NOT NULL,
        revision bigint NOT NULL DEFAULT 0, metadata_location text, snapshot_id text,
        PRIMARY KEY(environment,dataset)
      );
      CREATE TABLE IF NOT EXISTS moria.publications (
        environment text NOT NULL REFERENCES moria.environments(id), execution text NOT NULL,
        request_hash text NOT NULL, receipt jsonb NOT NULL, PRIMARY KEY(environment,execution)
      );
      CREATE TABLE IF NOT EXISTS moria.snapshot_history (
        environment text NOT NULL, dataset text NOT NULL, revision bigint NOT NULL,
        metadata_location text NOT NULL, snapshot_id text NOT NULL, execution text NOT NULL,
        lineage jsonb NOT NULL, PRIMARY KEY(environment,dataset,revision),
        FOREIGN KEY(environment,dataset) REFERENCES moria.dataset_heads(environment,dataset)
      );
      CREATE TABLE IF NOT EXISTS moria.checkpoints (
        environment text NOT NULL REFERENCES moria.environments(id), connection text NOT NULL,
        stream text NOT NULL, config_revision text NOT NULL, version bigint NOT NULL DEFAULT 0,
        value jsonb NOT NULL, PRIMARY KEY(environment,connection,stream,config_revision)
      );
      CREATE TABLE IF NOT EXISTS moria.action_keys (
        environment text NOT NULL REFERENCES moria.environments(id), application text NOT NULL,
        principal text NOT NULL, action text NOT NULL, key text NOT NULL,
        input_hash text NOT NULL, action_revision text NOT NULL, result jsonb,
        expires_at timestamptz NOT NULL,
        PRIMARY KEY(environment,application,principal,action,key)
      );


      CREATE TABLE IF NOT EXISTS moria.principals(environment text NOT NULL REFERENCES moria.environments(id),principal text NOT NULL,enabled boolean NOT NULL,kind text NOT NULL CHECK(kind IN ('human','workload')),attributes jsonb NOT NULL,markings text[] NOT NULL,synced_at timestamptz NOT NULL,PRIMARY KEY(environment,principal));
      CREATE TABLE IF NOT EXISTS moria.application_scopes(environment text NOT NULL REFERENCES moria.environments(id),application text NOT NULL,operations text[] NOT NULL,enabled boolean NOT NULL,PRIMARY KEY(environment,application));
      CREATE TABLE IF NOT EXISTS moria.permissions(environment text NOT NULL,principal text NOT NULL,application text NOT NULL,operations text[] NOT NULL,PRIMARY KEY(environment,principal,application),FOREIGN KEY(environment,principal) REFERENCES moria.principals(environment,principal),FOREIGN KEY(environment,application) REFERENCES moria.application_scopes(environment,application));
      CREATE TABLE IF NOT EXISTS moria.security_audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment text NOT NULL,actor text NOT NULL,operation text NOT NULL,target text NOT NULL,epoch bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
      CREATE TABLE IF NOT EXISTS moria.workload_sessions(token_hash text PRIMARY KEY,environment text NOT NULL,principal text NOT NULL,application text NOT NULL,expires_at timestamptz NOT NULL,disabled boolean NOT NULL DEFAULT false,FOREIGN KEY(environment,principal) REFERENCES moria.principals(environment,principal),FOREIGN KEY(environment,application) REFERENCES moria.application_scopes(environment,application));
      CREATE TABLE moria.instance_state (
        singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), security_epoch bigint NOT NULL DEFAULT 0
      );
      INSERT INTO moria.instance_state DEFAULT VALUES;
      CREATE TABLE moria.instance_principals (
        id uuid PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('human','workload')),
        enabled boolean NOT NULL DEFAULT true, home_entry_id uuid UNIQUE REFERENCES moria.fs_entries(id)
      );
      CREATE TABLE moria.instance_principal_bindings (
        environment text NOT NULL, principal text NOT NULL,
        instance_id uuid NOT NULL REFERENCES moria.instance_principals(id),
        PRIMARY KEY(environment,principal),
        FOREIGN KEY(environment,principal) REFERENCES moria.principals(environment,principal)
      );
      CREATE TABLE moria.fs_policies (
        entry_id uuid PRIMARY KEY REFERENCES moria.fs_entries(id), revision bigint NOT NULL DEFAULT 0,
        markings jsonb NOT NULL DEFAULT '[]', grants jsonb NOT NULL DEFAULT '[]',
        retained_markings jsonb NOT NULL DEFAULT '[]'
      );
      CREATE TABLE moria.instance_audit (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, actor text NOT NULL,
        operation text NOT NULL, target text NOT NULL, epoch bigint NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
      );
      CREATE TABLE moria.fs_roles (
        entry_id uuid PRIMARY KEY REFERENCES moria.fs_entries(id),
        kind text NOT NULL CHECK(kind IN ('system','group','user','agent','project','ontology','repository')),
        project_id uuid, deployment_unit_id uuid UNIQUE, namespace text,
        configuration jsonb NOT NULL, revision bigint NOT NULL DEFAULT 1, latest_release text,
        retired boolean NOT NULL DEFAULT false
      );
      CREATE UNIQUE INDEX fs_source_namespace ON moria.fs_roles(project_id,namespace)
        WHERE kind IN ('ontology','repository');
      CREATE TABLE moria.fs_groups (
        id uuid PRIMARY KEY, environment text NOT NULL REFERENCES moria.environments(id),
        group_key text NOT NULL, home_entry_id uuid NOT NULL UNIQUE REFERENCES moria.fs_entries(id),
        UNIQUE(environment,group_key)
      );
      CREATE TABLE moria.fs_source_releases (
        digest text PRIMARY KEY, root_id uuid NOT NULL REFERENCES moria.fs_roles(entry_id),
        snapshot jsonb NOT NULL, compiled jsonb NOT NULL, owned jsonb NOT NULL,
        markings jsonb NOT NULL, created_by uuid NOT NULL REFERENCES moria.instance_principals(id),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
      );
      CREATE TABLE moria.fs_resource_identities (
        root_id uuid NOT NULL REFERENCES moria.fs_roles(entry_id), reference text NOT NULL,
        resource_id uuid NOT NULL UNIQUE, PRIMARY KEY(root_id,reference)
      );
      CREATE TABLE moria.fs_active_releases (
        root_id uuid NOT NULL REFERENCES moria.fs_roles(entry_id), environment text NOT NULL REFERENCES moria.environments(id),
        revision bigint NOT NULL DEFAULT 0, digest text NOT NULL REFERENCES moria.fs_source_releases(digest),
        resources jsonb NOT NULL, PRIMARY KEY(root_id,environment)
      );
      CREATE TABLE moria.fs_release_plans (
        id uuid PRIMARY KEY, root_id uuid NOT NULL REFERENCES moria.fs_roles(entry_id),
        digest text NOT NULL REFERENCES moria.fs_source_releases(digest),
        environment text NOT NULL REFERENCES moria.environments(id), actor_hash text NOT NULL,
        expected_revision bigint NOT NULL, environment_epoch bigint NOT NULL, instance_epoch bigint NOT NULL,
        deletions jsonb NOT NULL, receipt jsonb,
        expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '15 minutes'
      );
      CREATE TABLE moria.fs_provenance (
        entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),
        source_id uuid NOT NULL REFERENCES moria.fs_entries(id),
        PRIMARY KEY(entry_id,source_id), CHECK(entry_id<>source_id)
      );
      CREATE TABLE moria.fs_resource_bindings (
        environment text NOT NULL REFERENCES moria.environments(id), reference text NOT NULL,
        project_id uuid NOT NULL,
        entry_id uuid NOT NULL UNIQUE REFERENCES moria.fs_entries(id),
        root_id uuid NOT NULL REFERENCES moria.fs_roles(entry_id),
        digest text NOT NULL REFERENCES moria.fs_source_releases(digest),
        active boolean NOT NULL DEFAULT true, PRIMARY KEY(environment,project_id,reference)
      );
      CREATE TABLE moria.fs_resource_dependencies (
        environment text NOT NULL REFERENCES moria.environments(id),
        project_id uuid NOT NULL,
        source_project_id uuid NOT NULL,
        reference text NOT NULL,source_reference text NOT NULL,
        PRIMARY KEY(environment,project_id,reference,source_project_id,source_reference),
        CHECK(project_id<>source_project_id OR reference<>source_reference)
      );
      CREATE TABLE moria.fs_resource_release_history (
        entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),
        digest text NOT NULL REFERENCES moria.fs_source_releases(digest),
        PRIMARY KEY(entry_id,digest)
      );
      CREATE TABLE moria.fs_content_candidates (
        object_id text PRIMARY KEY, location jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
      );
      CREATE TABLE moria.function_invocations (
          value_version integer NOT NULL DEFAULT 1 CHECK(value_version>0),
        id uuid PRIMARY KEY, environment text NOT NULL REFERENCES moria.environments(id),
        resource_id uuid NOT NULL, entry_id uuid NOT NULL REFERENCES moria.fs_entries(id),
        release text NOT NULL REFERENCES moria.fs_source_releases(digest), definition jsonb NOT NULL,
        actor_hash text NOT NULL, context jsonb NOT NULL, request_key text NOT NULL, input_hash text NOT NULL,
        input jsonb NOT NULL, attempt_id uuid NOT NULL, fence uuid NOT NULL, correlation_id uuid NOT NULL,
        status text NOT NULL CHECK(status IN ('accepted','running','succeeded','failed','cancelled')),
        lease_until timestamptz NOT NULL, deadline timestamptz NOT NULL,
          markings jsonb NOT NULL DEFAULT '[]', reads jsonb NOT NULL DEFAULT '[]', read_grants jsonb NOT NULL DEFAULT '[]',
        result jsonb, error jsonb, event_sequence bigint NOT NULL DEFAULT 0, worker_cleaned boolean NOT NULL DEFAULT false,
          parent_id uuid REFERENCES moria.function_invocations(id),
          prepare_only boolean NOT NULL DEFAULT false, point_reads_only boolean NOT NULL DEFAULT false, proposal_commit jsonb,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
        UNIQUE(environment,resource_id,actor_hash,request_key)
      );
        CREATE TABLE moria.function_calls (
          invocation_id uuid NOT NULL REFERENCES moria.function_invocations(id),
          key text NOT NULL, path text NOT NULL, request_hash text NOT NULL, request jsonb NOT NULL,
          status text NOT NULL CHECK(status IN ('started','succeeded','failed','proposed','committed')),
          response jsonb, binding_hash text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          PRIMARY KEY(invocation_id,key)
        );
        CREATE TABLE moria.function_events (
        invocation_id uuid NOT NULL REFERENCES moria.function_invocations(id), sequence bigint NOT NULL,
        type text NOT NULL CHECK(type IN ('state','log')), message text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(invocation_id,sequence)
      );
      CREATE TABLE moria.fs_storage_bindings (
        warehouse_id text NOT NULL,table_id text NOT NULL,binding_hash text NOT NULL,
        PRIMARY KEY(warehouse_id,table_id)
      );
      CREATE TABLE IF NOT EXISTS moria.auth_providers(environment text NOT NULL,application text NOT NULL,provider text NOT NULL,revision text NOT NULL,enabled boolean NOT NULL,PRIMARY KEY(environment,application,provider),FOREIGN KEY(environment,application) REFERENCES moria.application_scopes(environment,application));
      CREATE TABLE IF NOT EXISTS moria.external_identities(environment text NOT NULL,issuer text NOT NULL,subject text NOT NULL,principal text NOT NULL,PRIMARY KEY(environment,issuer,subject),FOREIGN KEY(environment,principal) REFERENCES moria.principals(environment,principal));
      CREATE TABLE IF NOT EXISTS moria.auth_invitations(environment text NOT NULL,application text NOT NULL,issuer text NOT NULL,email text NOT NULL,subject text NOT NULL,PRIMARY KEY(environment,application,issuer,email));
      CREATE TABLE IF NOT EXISTS moria.auth_flows(state_hash text PRIMARY KEY,environment text NOT NULL,application text NOT NULL,provider text,revision text,browser_hash text,nonce text,verifier text,expires_at timestamptz NOT NULL);
      CREATE TABLE IF NOT EXISTS moria.browser_sessions(token_hash text PRIMARY KEY,environment text NOT NULL,principal text NOT NULL,application text NOT NULL,provider text,revision text,subject text,email text,display_name text NOT NULL,access_token text,verified_at timestamptz NOT NULL,expires_at timestamptz NOT NULL,disabled boolean NOT NULL DEFAULT false,FOREIGN KEY(environment,principal) REFERENCES moria.principals(environment,principal),FOREIGN KEY(environment,application) REFERENCES moria.application_scopes(environment,application));

CREATE TABLE IF NOT EXISTS moria.read_cursors(
      token_hash text PRIMARY KEY, environment text NOT NULL REFERENCES moria.environments(id),
      binding_hash text NOT NULL, security_epoch bigint NOT NULL, revision bigint NOT NULL,
      position jsonb NOT NULL, expires_at timestamptz NOT NULL
    ); CREATE INDEX IF NOT EXISTS read_cursors_retention ON moria.read_cursors(environment,revision,expires_at);

      CREATE TABLE IF NOT EXISTS moria.object_homes(uid uuid PRIMARY KEY REFERENCES moria.identities(uid),object_type text NOT NULL);
      CREATE TABLE IF NOT EXISTS moria.source_generations(environment text NOT NULL REFERENCES moria.environments(id),object_type text NOT NULL,revision bigint NOT NULL,PRIMARY KEY(environment,object_type,revision));
      CREATE TABLE IF NOT EXISTS moria.source_rows(environment text NOT NULL,object_type text NOT NULL,revision bigint NOT NULL,uid uuid NOT NULL REFERENCES moria.object_homes(uid),value jsonb NOT NULL,PRIMARY KEY(environment,object_type,revision,uid),FOREIGN KEY(environment,object_type,revision) REFERENCES moria.source_generations(environment,object_type,revision));
      CREATE TABLE IF NOT EXISTS moria.overlay_versions(environment text NOT NULL REFERENCES moria.environments(id),object_type text NOT NULL,uid uuid NOT NULL REFERENCES moria.object_homes(uid),revision bigint NOT NULL,overrides jsonb NOT NULL,tombstone boolean NOT NULL,PRIMARY KEY(environment,object_type,uid,revision));
      CREATE TABLE IF NOT EXISTS moria.source_stages(id uuid PRIMARY KEY,environment text NOT NULL REFERENCES moria.environments(id),object_type text NOT NULL,expected_revision bigint NOT NULL,security_epoch bigint NOT NULL,status text NOT NULL CHECK(status IN ('loading','ready','published','failed')),published_revision bigint,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
      CREATE TABLE IF NOT EXISTS moria.staged_source_rows(stage uuid NOT NULL REFERENCES moria.source_stages(id),uid uuid NOT NULL REFERENCES moria.object_homes(uid),value jsonb NOT NULL,PRIMARY KEY(stage,uid));
      ALTER TABLE moria.source_generations ADD COLUMN IF NOT EXISTS stage uuid REFERENCES moria.source_stages(id);
      ALTER TABLE moria.source_generations ADD COLUMN IF NOT EXISTS object_schema jsonb;
      ALTER TABLE moria.source_stages ADD COLUMN IF NOT EXISTS object_schema jsonb;
      ALTER TABLE moria.source_stages ADD COLUMN IF NOT EXISTS base_source_revision bigint;
      ALTER TABLE moria.source_stages ADD COLUMN IF NOT EXISTS lease_binding jsonb;
      ALTER TABLE moria.source_stages ADD COLUMN IF NOT EXISTS required_markings text[];
      ALTER TABLE moria.source_generations ADD COLUMN IF NOT EXISTS required_markings text[];

CREATE TABLE IF NOT EXISTS moria.index_provenance(
      stage uuid PRIMARY KEY REFERENCES moria.source_stages(id),environment text NOT NULL,
      principal text NOT NULL,application text NOT NULL,contract text NOT NULL,
      source_dataset text NOT NULL,source_revision bigint NOT NULL,metadata_location text NOT NULL,snapshot_id text NOT NULL,
      FOREIGN KEY(environment,source_dataset,source_revision) REFERENCES moria.snapshot_history(environment,dataset,revision));

      CREATE TABLE IF NOT EXISTS moria.jobs (
        id uuid PRIMARY KEY, kind text NOT NULL, input jsonb NOT NULL,
        request_key text NOT NULL UNIQUE, requested_by text NOT NULL,
        status text NOT NULL DEFAULT 'queued', phase text NOT NULL DEFAULT 'Queued',
        attempt integer NOT NULL DEFAULT 0, fence uuid,
        available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        heartbeat_at timestamptz, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), error text, result jsonb
      );
      CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_kind ON moria.jobs(kind)
        WHERE status IN ('queued','running','retrying');
      CREATE TABLE IF NOT EXISTS moria.job_attempts (
        job_id uuid NOT NULL REFERENCES moria.jobs(id), attempt integer NOT NULL,
        status text NOT NULL, started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        finished_at timestamptz, error text, PRIMARY KEY(job_id,attempt)
      );
      CREATE TABLE IF NOT EXISTS moria.job_requests (
        request_key text PRIMARY KEY, job_id uuid NOT NULL REFERENCES moria.jobs(id)
      );
      INSERT INTO moria.job_requests(request_key,job_id) SELECT request_key,id FROM moria.jobs ON CONFLICT DO NOTHING;
      CREATE TABLE IF NOT EXISTS moria.job_schedules (
        kind text PRIMARY KEY, enabled boolean NOT NULL DEFAULT false,
        interval_minutes integer NOT NULL, next_run timestamptz NOT NULL DEFAULT clock_timestamp()
      );

CREATE TABLE IF NOT EXISTS moria.snow_syncs(id uuid PRIMARY KEY,status text NOT NULL,config jsonb NOT NULL,started_at timestamptz NOT NULL DEFAULT clock_timestamp(),completed_at timestamptz,station_count integer,observation_count integer,error text);
ALTER TABLE moria.snow_syncs ADD COLUMN IF NOT EXISTS plan jsonb;
    ALTER TABLE moria.snow_syncs ADD COLUMN IF NOT EXISTS result jsonb;
    CREATE TABLE IF NOT EXISTS moria.index_provenance(
      stage uuid PRIMARY KEY REFERENCES moria.source_stages(id),environment text NOT NULL,
      principal text NOT NULL,application text NOT NULL,contract text NOT NULL,
      source_dataset text NOT NULL,source_revision bigint NOT NULL,metadata_location text NOT NULL,snapshot_id text NOT NULL,
      FOREIGN KEY(environment,source_dataset,source_revision) REFERENCES moria.snapshot_history(environment,dataset,revision));

    CREATE TABLE IF NOT EXISTS moria.weather_publications (
      job_id text PRIMARY KEY, published_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      view jsonb NOT NULL, raw jsonb NOT NULL
    );
    CREATE TABLE IF NOT EXISTS moria.weather_head (
      singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
      job_id text NOT NULL REFERENCES moria.weather_publications(job_id)
    );
  ${indexingSchema}
  ${identityRuntimeSchema}
  ${nativeAuthoritySchema}
  ${executionRuntimeSchema}
  ${deliveryRuntimeSchema}
  ${servingRuntimeSchema}
  ${browserOriginSchema}
  ${browserSessionRevocationSchema}
  ${filesystemRuntimeSchema}
  ${filesystemContentSchema}
  ${sourceRuntimeSchema}
  ${snapshotRuntimeSchema}
  ${functionRuntimeSchema}
  ${servingFunctionReferenceSchema}
  ${agentRuntimeSchema}
  ${jobRuntimeSchema}
  ${scratchRuntimeSchema}
  ${evaluationRuntimeSchema}
  ${changeRuntimeSchema}
  ${modelRuntimeSchema}
  ${projectionRuntimeSchema}
  ${linkConstraintSchema}
  ${searchRuntimeSchema}
  ${searchProjectorSchema}
  ${provenanceRuntimeSchema}
  ${sourceDeclarationSchema}
  ${actionAdmissionSchema}
  ${indexingJobRuntimeSchema}
  ${recoveryRuntimeSchema}
  ${secretRuntimeSchema}
  ${recoveryContentSchema}
  ${retentionRuntimeSchema}
  ${filesystemGcSchema}
  ${executionRootPinsSchema}
  ${directoryRuntimeSchema}
  ${pipelineRuntimeSchema}
  ${automationRuntimeSchema}
  ${mcpRuntimeSchema}
  ${historyRuntimeSchema}
  `;
import { indexingSchema } from './indexing-schema.js';
import { changeRuntimeSchema } from './change-runtime-schema.js';
import { filesystemRuntimeSchema } from './filesystem-runtime-schema.js';
import { modelRuntimeSchema } from './model-runtime-schema.js';
import { linkConstraintSchema } from './link-constraint-schema.js';
import { projectionRuntimeSchema } from './projection-runtime-schema.js';

import { searchRuntimeSchema } from './search-runtime-schema.js';

import { agentRuntimeSchema } from './agent-runtime-schema.js';

import { filesystemContentSchema } from './filesystem-content-schema.js';

import { jobRuntimeSchema } from './job-runtime-schema.js';

import { provenanceRuntimeSchema } from './provenance-runtime-schema.js';
import { actionAdmissionSchema } from './action-admission-schema.js';

import { indexingJobRuntimeSchema } from './indexing-job-runtime-schema.js';

import { sourceRuntimeSchema } from './source-runtime-schema.js';

import { searchProjectorSchema } from './search-projector-schema.js';

import { evaluationRuntimeSchema } from './evaluation-runtime-schema.js';

import { scratchRuntimeSchema } from './scratch-runtime-schema.js';

import { directoryRuntimeSchema } from './directory-runtime-schema.js';

import { pipelineRuntimeSchema } from './pipeline-runtime-schema.js';

import { mcpRuntimeSchema } from './mcp-runtime-schema.js';

import { recoveryRuntimeSchema } from './recovery-runtime-schema.js';

import { retentionRuntimeSchema } from './retention-runtime-schema.js';
import { recoveryContentSchema } from './recovery-content-schema.js';

import { historyRuntimeSchema } from './history-runtime-schema.js';

import { executionRootPinsSchema } from './execution-root-pins-schema.js';

import { automationRuntimeSchema } from './automation-runtime-schema.js';

import { filesystemGcSchema } from './filesystem-gc-schema.js';

import { secretRuntimeSchema } from './secret-runtime-schema.js';

import { sourceDeclarationSchema } from './source-declaration-schema.js';
