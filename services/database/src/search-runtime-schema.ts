/** Native projection state is derived data; protected serving owns ranking authority. */
export const searchRuntimeSchema = String.raw`
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
INSERT INTO moria.runtime_resource_classes VALUES('search',true,16,16);
CREATE TABLE moria.native_search_collections (
  id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  environment text NOT NULL REFERENCES moria.environments(id), resource text NOT NULL,
  revision text NOT NULL, definition jsonb NOT NULL, enabled boolean NOT NULL DEFAULT true,
  active_generation uuid, resource_class text NOT NULL DEFAULT 'search' REFERENCES moria.runtime_resource_classes(name),
  partitions integer NOT NULL DEFAULT 16 CHECK(partitions BETWEEN 1 AND 256 AND (partitions & (partitions-1))=0),
  timeout_seconds integer NOT NULL DEFAULT 1800 CHECK(timeout_seconds BETWEEN 1 AND 1800), PRIMARY KEY(environment,resource)
);
CREATE TABLE moria.native_search_generations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), environment text NOT NULL, resource text NOT NULL,
  collection_revision text NOT NULL, definition jsonb NOT NULL, object_binding_revision text NOT NULL,
  model_fingerprint text NOT NULL, model_revision text NOT NULL, dimensions integer NOT NULL CHECK(dimensions BETWEEN 1 AND 4096),
  chunker_revision text NOT NULL, incarnation uuid NOT NULL, base_revision bigint NOT NULL CHECK(base_revision>=0),
  covered_revision bigint NOT NULL CHECK(covered_revision>=base_revision),
  status text NOT NULL CHECK(status IN ('building','ready','active','retired','failed','cancelled')),
  owner uuid, fence bigint NOT NULL DEFAULT 0, lease_until timestamptz, error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY(environment,resource) REFERENCES moria.native_search_collections(environment,resource),
  UNIQUE(environment,resource,id)
);
ALTER TABLE moria.native_search_collections ADD FOREIGN KEY(environment,resource,active_generation)
  REFERENCES moria.native_search_generations(environment,resource,id);
CREATE UNIQUE INDEX native_search_one_active ON moria.native_search_generations(environment,resource) WHERE status='active';
CREATE FUNCTION moria.native_search_marking_hash(labels text[]) RETURNS bytea LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
  SELECT pg_catalog.sha256(pg_catalog.convert_to(to_jsonb(labels)::text,'UTF8'))
$$;
CREATE TABLE moria.native_search_embeddings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), model_fingerprint text NOT NULL,
  text_digest bytea NOT NULL CHECK(octet_length(text_digest)=32), text text NOT NULL CHECK(char_length(text)<=1000),
  dimensions integer NOT NULL CHECK(dimensions BETWEEN 1 AND 4096), embedding public.vector NOT NULL,
  markings text[] NOT NULL, markings_hash bytea GENERATED ALWAYS AS (moria.native_search_marking_hash(markings)) STORED, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(model_fingerprint,text_digest,dimensions,markings_hash),
  CHECK(text_digest=moria.identity_key_hash(text)),
  CHECK(public.vector_dims(embedding)=dimensions AND public.vector_norm(embedding)>0)
);
CREATE FUNCTION moria.native_search_embedding_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='DELETE' THEN RAISE EXCEPTION 'SEARCH_EMBEDDING_RESERVED'; END IF;
  IF (NEW.id,NEW.model_fingerprint,NEW.text_digest,NEW.text,NEW.dimensions,NEW.embedding::text,NEW.created_at)
    IS DISTINCT FROM (OLD.id,OLD.model_fingerprint,OLD.text_digest,OLD.text,OLD.dimensions,OLD.embedding::text,OLD.created_at)
    OR NEW.markings IS DISTINCT FROM OLD.markings THEN RAISE EXCEPTION 'SEARCH_EMBEDDING_RESERVED'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_search_embedding_immutable BEFORE UPDATE OR DELETE ON moria.native_search_embeddings
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_embedding_immutable();
CREATE TABLE moria.native_search_documents (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), generation uuid NOT NULL REFERENCES moria.native_search_generations(id), uid uuid NOT NULL,
  source_revision bigint NOT NULL CHECK(source_revision>=0), source_digest bytea NOT NULL CHECK(octet_length(source_digest)=32),
  chunk_count integer NOT NULL CHECK(chunk_count>=0), UNIQUE(generation,uid,id), UNIQUE(generation,uid,source_revision,source_digest)
);
CREATE TABLE moria.native_search_heads (
  generation uuid NOT NULL, uid uuid NOT NULL, document uuid NOT NULL,
  PRIMARY KEY(generation,uid), FOREIGN KEY(generation,uid,document) REFERENCES moria.native_search_documents(generation,uid,id)
);
CREATE TABLE moria.native_search_chunks (
  document uuid NOT NULL REFERENCES moria.native_search_documents(id), ordinal integer NOT NULL CHECK(ordinal>=0),
  chunk_id text COLLATE "C" NOT NULL, start_offset integer NOT NULL CHECK(start_offset>=0),
  end_offset integer NOT NULL CHECK(end_offset>start_offset AND end_offset-start_offset<=1000),
  embedding uuid NOT NULL REFERENCES moria.native_search_embeddings(id),
  PRIMARY KEY(document,ordinal), UNIQUE(document,chunk_id)
);
CREATE TABLE moria.native_search_rebuilds (
  generation uuid PRIMARY KEY REFERENCES moria.native_search_generations(id), execution uuid NOT NULL UNIQUE REFERENCES moria.runtime_executions(id),
  environment text NOT NULL, resource text NOT NULL, principal text NOT NULL, application text NOT NULL,
  request_key text NOT NULL, source_stage uuid REFERENCES moria.source_stages(id), source_revision bigint NOT NULL,
  source_schema jsonb NOT NULL, partitions integer NOT NULL, markings text[] NOT NULL, base_verified boolean NOT NULL DEFAULT false,
  UNIQUE(environment,resource,principal,application,request_key)
);
CREATE TABLE moria.native_search_execution_sources (
  execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
  generation uuid NOT NULL REFERENCES moria.native_search_generations(id),
  kind text NOT NULL CHECK(kind IN ('rebuild','replay')),
  target_revision bigint NOT NULL CHECK(target_revision>=0),
  model_secret_approval jsonb CHECK(model_secret_approval IS NULL OR (
    jsonb_typeof(model_secret_approval)='object' AND model_secret_approval-ARRAY['id','generation','incarnation']='{}'::jsonb
    AND model_secret_approval ?& ARRAY['id','generation','incarnation']
    AND jsonb_typeof(model_secret_approval->'id')='string' AND jsonb_typeof(model_secret_approval->'generation')='string'
    AND jsonb_typeof(model_secret_approval->'incarnation')='string'))
);
CREATE INDEX native_search_execution_generation ON moria.native_search_execution_sources(generation);
CREATE FUNCTION moria.native_search_execution_source_validate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'IMMUTABLE_SEARCH_EXECUTION'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.runtime_executions e JOIN moria.native_search_generations g ON g.id=NEW.generation
    JOIN moria.native_search_collections c ON c.environment=g.environment AND c.resource=g.resource
    WHERE e.id=NEW.execution AND e.spec->>'domain'='search' AND e.environment=g.environment AND e.resource=c.id
      AND NEW.target_revision>=g.base_revision) THEN RAISE EXCEPTION 'SEARCH_PROJECTOR_UNAVAILABLE'; END IF;
  IF NEW.model_secret_approval IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM moria.native_secret_approvals a JOIN moria.runtime_executions e ON e.id=NEW.execution
      JOIN moria.runtime_execution_bindings b ON b.id=e.binding JOIN moria.native_search_generations g ON g.id=NEW.generation
    WHERE a.id::text=NEW.model_secret_approval->>'id' AND a.generation::text=NEW.model_secret_approval->>'generation'
      AND a.incarnation::text=NEW.model_secret_approval->>'incarnation' AND a.incarnation=(SELECT incarnation FROM moria.indexing_instance)
      AND a.enabled AND a.environment=e.environment AND a.owner_kind='model' AND a.owner_profile::text=g.definition->>'embeddingModel'
      AND a.secret::text||':use'=ANY(b.operations)
  ) THEN RAISE EXCEPTION 'SECRET_APPROVAL_REVOKED'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER native_search_execution_source_valid BEFORE INSERT OR UPDATE OR DELETE ON moria.native_search_execution_sources
  FOR EACH ROW EXECUTE FUNCTION moria.native_search_execution_source_validate();
CREATE TABLE moria.native_search_task_progress (
  task uuid PRIMARY KEY REFERENCES moria.runtime_execution_tasks(id), generation uuid NOT NULL REFERENCES moria.native_search_generations(id),
  lower_uid uuid NOT NULL, upper_uid uuid, after_uid uuid,
  rows_processed bigint NOT NULL DEFAULT 0 CHECK(rows_processed>=0), digest bytea NOT NULL DEFAULT decode(repeat('00',32),'hex'),
  complete boolean NOT NULL DEFAULT false, CHECK(octet_length(digest)=32)
);
CREATE TABLE moria.native_search_replay_progress (
  generation uuid PRIMARY KEY REFERENCES moria.native_search_generations(id),
  frame uuid REFERENCES moria.runtime_change_transactions(id), after_uid uuid, complete boolean NOT NULL DEFAULT false
);
CREATE TABLE moria.native_search_pending_heads (
  generation uuid NOT NULL, frame uuid NOT NULL REFERENCES moria.runtime_change_transactions(id), uid uuid NOT NULL, document uuid,
  PRIMARY KEY(generation,frame,uid), FOREIGN KEY(generation,uid,document) REFERENCES moria.native_search_documents(generation,uid,id)
);
`;
