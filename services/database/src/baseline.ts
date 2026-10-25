/** Fresh schema version 1. Preserve this DDL and its checksum. */
export const baseline = `
      CREATE SCHEMA IF NOT EXISTS moria;
      CREATE TABLE IF NOT EXISTS moria.environments (
        id text PRIMARY KEY, revision bigint NOT NULL DEFAULT 0, security_epoch bigint NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS moria.identities (
        domain text NOT NULL, canonical_key text NOT NULL, uid uuid NOT NULL UNIQUE,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(domain,canonical_key)
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
  `;
