/** Source schedule dispatch, isolated from fixed-binding scheduler compatibility. */
export const automationRuntimeSchema = String.raw`
INSERT INTO moria.runtime_resource_classes(name,maximum_tasks,maximum_environment_tasks)
 VALUES('automation-control',16,4) ON CONFLICT DO NOTHING;
CREATE TABLE moria.runtime_source_automation_profiles(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment text NOT NULL REFERENCES moria.environments(id),
 resource uuid NOT NULL,revision text NOT NULL,authority uuid NOT NULL REFERENCES moria.runtime_service_authorities(id),
 service_revision text NOT NULL,target_service_revision text NOT NULL,
 resource_class text NOT NULL REFERENCES moria.runtime_resource_classes(name),
 timeout_seconds integer NOT NULL CHECK(timeout_seconds BETWEEN 10 AND 600),enabled boolean NOT NULL DEFAULT true,
 generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),
 UNIQUE(environment,resource,revision)
);
CREATE TABLE moria.runtime_source_automation_schedules(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),profile uuid NOT NULL REFERENCES moria.runtime_source_automation_profiles(id),
 environment text NOT NULL REFERENCES moria.environments(id),resource uuid NOT NULL,release text NOT NULL REFERENCES moria.fs_source_releases(digest),
 definition jsonb NOT NULL,target uuid NOT NULL,target_domain text NOT NULL CHECK(target_domain IN ('transform','sync','agent','evaluation')),
 application uuid NOT NULL,input jsonb NOT NULL CHECK(octet_length(input::text)<=32768),
 interval_seconds integer NOT NULL CHECK(interval_seconds BETWEEN 1 AND 31536000),
 enabled boolean NOT NULL DEFAULT true,next_run timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(profile,release)
);
CREATE INDEX runtime_source_automation_due ON moria.runtime_source_automation_schedules(next_run,id) WHERE enabled;
CREATE INDEX runtime_source_automation_resource ON moria.runtime_source_automation_schedules(environment,resource);
CREATE TABLE moria.runtime_source_automation_firings(
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),schedule uuid NOT NULL REFERENCES moria.runtime_source_automation_schedules(id),
 environment text NOT NULL REFERENCES moria.environments(id),resource uuid NOT NULL,
 scheduled_at timestamptz NOT NULL,outcome text NOT NULL CHECK(outcome IN ('enqueued','overlap','denied')),
 control uuid UNIQUE REFERENCES moria.runtime_executions(id),child uuid UNIQUE REFERENCES moria.runtime_executions(id),
 input jsonb NOT NULL,markings text[] NOT NULL CHECK(cardinality(markings)<=256),
 profile_generation bigint NOT NULL CHECK(profile_generation>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),UNIQUE(schedule,scheduled_at)
);
CREATE INDEX runtime_source_automation_children ON moria.runtime_source_automation_firings(schedule,scheduled_at DESC) WHERE child IS NOT NULL;
CREATE INDEX runtime_source_automation_firing_page ON moria.runtime_source_automation_firings(environment,resource,scheduled_at DESC,id DESC);
CREATE FUNCTION moria.runtime_source_automation_firing_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE schedule moria.runtime_source_automation_schedules;
BEGIN
 SELECT * INTO schedule FROM moria.runtime_source_automation_schedules WHERE id=NEW.schedule;
 IF NOT FOUND OR (NEW.environment IS NOT NULL AND NEW.environment<>schedule.environment)
   OR (NEW.resource IS NOT NULL AND NEW.resource<>schedule.resource) THEN RAISE EXCEPTION 'AUTOMATION_PROFILE_INVALID'; END IF;
 NEW.environment:=schedule.environment;NEW.resource:=schedule.resource;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_source_automation_firing_scope BEFORE INSERT ON moria.runtime_source_automation_firings FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_automation_firing_scope();
CREATE TABLE moria.runtime_source_automation_controls(
 execution uuid PRIMARY KEY REFERENCES moria.runtime_executions(id),
 firing uuid NOT NULL UNIQUE REFERENCES moria.runtime_source_automation_firings(id)
);
CREATE TABLE moria.runtime_source_automation_status(
 environment text NOT NULL REFERENCES moria.environments(id),resource uuid NOT NULL,
 active_firing uuid REFERENCES moria.runtime_source_automation_firings(id),PRIMARY KEY(environment,resource)
);
CREATE TABLE moria.runtime_source_automation_epochs(
 execution uuid PRIMARY KEY REFERENCES moria.runtime_source_automation_controls(execution),
 binding uuid NOT NULL REFERENCES moria.runtime_execution_bindings(id),task uuid NOT NULL REFERENCES moria.runtime_execution_tasks(id),
 owner uuid,fence bigint,incarnation uuid NOT NULL,security_epoch bigint NOT NULL,instance_epoch bigint NOT NULL,
 CHECK((owner IS NULL)=(fence IS NULL))
);
CREATE FUNCTION moria.runtime_source_automation_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'IMMUTABLE_AUTOMATION'; END IF;
 IF TG_TABLE_NAME='runtime_source_automation_profiles' THEN
  IF NEW.generation<>OLD.generation OR (to_jsonb(NEW)-'enabled'-'generation')<>(to_jsonb(OLD)-'enabled'-'generation') THEN RAISE EXCEPTION 'IMMUTABLE_AUTOMATION'; END IF;
  IF NEW.enabled IS DISTINCT FROM OLD.enabled THEN
   NEW.generation:=OLD.generation+1;
   UPDATE moria.environments SET security_epoch=security_epoch+1 WHERE id=NEW.environment;
   IF NOT NEW.enabled THEN UPDATE moria.runtime_source_automation_schedules SET enabled=false WHERE profile=NEW.id; END IF;
  END IF;
 ELSIF TG_TABLE_NAME='runtime_source_automation_schedules' THEN
  IF (to_jsonb(NEW)-'enabled'-'next_run')<>(to_jsonb(OLD)-'enabled'-'next_run') THEN RAISE EXCEPTION 'IMMUTABLE_AUTOMATION'; END IF;
 ELSIF TG_TABLE_NAME='runtime_source_automation_firings' THEN
  IF (to_jsonb(NEW)-'control'-'child')<>(to_jsonb(OLD)-'control'-'child')
   OR (OLD.control IS NOT NULL AND NEW.control IS DISTINCT FROM OLD.control)
   OR (OLD.child IS NOT NULL AND NEW.child IS DISTINCT FROM OLD.child) THEN RAISE EXCEPTION 'IMMUTABLE_AUTOMATION'; END IF;
 ELSE RAISE EXCEPTION 'IMMUTABLE_AUTOMATION'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_source_automation_profile_identity BEFORE UPDATE OR DELETE ON moria.runtime_source_automation_profiles FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_automation_immutable();
CREATE TRIGGER runtime_source_automation_schedule_identity BEFORE UPDATE OR DELETE ON moria.runtime_source_automation_schedules FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_automation_immutable();
CREATE TRIGGER runtime_source_automation_firing_identity BEFORE UPDATE OR DELETE ON moria.runtime_source_automation_firings FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_automation_immutable();
CREATE TRIGGER runtime_source_automation_control_identity BEFORE UPDATE OR DELETE ON moria.runtime_source_automation_controls FOR EACH ROW EXECUTE FUNCTION moria.runtime_source_automation_immutable();

CREATE FUNCTION moria.runtime_automation_dispatch_metadata(private_hash text,target uuid,job_domain text,
 service_id uuid,target_revision text,class_name text) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE control moria.runtime_executions; firing moria.runtime_source_automation_firings;
 schedule moria.runtime_source_automation_schedules; profile moria.runtime_source_automation_profiles;
 credential moria.runtime_job_sessions; state jsonb; target_state jsonb; required text; labels jsonb;
BEGIN
 SELECT * INTO credential FROM moria.runtime_job_sessions WHERE token_hash=private_hash;
 IF NOT FOUND OR credential.task IS NULL THEN RETURN NULL; END IF;
 SELECT e.* INTO control FROM moria.runtime_executions e JOIN moria.runtime_source_automation_controls c ON c.execution=e.id
  WHERE e.id=credential.execution FOR UPDATE OF e;
 IF NOT FOUND THEN RETURN NULL; END IF;
 PERFORM moria.runtime_task_check(credential.task,credential.owner,credential.fence);
 state:=moria.native_serving_session(private_hash);
 SELECT f.* INTO firing FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_controls c ON c.firing=f.id
  WHERE c.execution=control.id FOR UPDATE OF f;
 SELECT * INTO schedule FROM moria.runtime_source_automation_schedules WHERE id=firing.schedule;
 SELECT * INTO profile FROM moria.runtime_source_automation_profiles WHERE id=schedule.profile AND enabled FOR SHARE;
 IF NOT FOUND OR profile.generation<>firing.profile_generation OR firing.outcome<>'enqueued' OR control.spec->>'domain'<>'maintenance'
  OR control.resource<>schedule.resource OR control.resource_class<>profile.resource_class
  OR ROW(schedule.environment,schedule.application::text) IS DISTINCT FROM ROW(state->>'environment',state->>'application')
  OR ROW(schedule.target,schedule.target_domain,profile.authority,profile.target_service_revision)
    IS DISTINCT FROM ROW(target,job_domain,service_id,target_revision)
  THEN RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED'; END IF;
 PERFORM moria.runtime_service_binding_guard(control.binding);
 target_state:=moria.runtime_service_target_authority(service_id,schedule.environment,target,job_domain,target_revision,class_name);
 IF ROW(target_state->>'environment',target_state->>'principal',target_state->>'application')
  IS DISTINCT FROM ROW(state->>'environment',state->>'principal',state->>'application') THEN RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED'; END IF;
 required:=target::text||':'||CASE job_domain WHEN 'transform' THEN 'build' WHEN 'sync' THEN 'sync' WHEN 'agent' THEN 'invoke' ELSE 'run' END;
 IF NOT (state->'identity'->'grantedOperations' ? required) OR NOT (state->'identity'->'applicationScopes' ? required)
  THEN RAISE EXCEPTION 'FORBIDDEN'; END IF;
 labels:=moria.native_resource_release_access_state(state,schedule.resource::text,'run',schedule.release);
 IF NOT labels<@to_jsonb(firing.markings) THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
 RETURN jsonb_build_object('firing',firing.id,'control',control.id,'state',state,'markings',to_jsonb(firing.markings));
END $$;

CREATE FUNCTION moria.runtime_automation_binding_guard(binding_id uuid) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE binding moria.runtime_execution_bindings; firing moria.runtime_source_automation_firings;
 schedule moria.runtime_source_automation_schedules; profile moria.runtime_source_automation_profiles;
 service record; state jsonb; labels jsonb; metadata jsonb;
BEGIN
 SELECT * INTO binding FROM moria.runtime_execution_bindings WHERE id=binding_id;
 IF binding.context->>'automationFiring' IS NULL THEN RETURN; END IF;
 SELECT * INTO firing FROM moria.runtime_source_automation_firings WHERE id=(binding.context->>'automationFiring')::uuid;
 SELECT * INTO schedule FROM moria.runtime_source_automation_schedules WHERE id=firing.schedule;
 SELECT * INTO profile FROM moria.runtime_source_automation_profiles WHERE id=schedule.profile AND enabled FOR SHARE;
 SELECT actual.* INTO service FROM moria.runtime_execution_services actual WHERE actual.binding=binding_id;
 IF NOT FOUND OR profile.id IS NULL OR profile.generation<>firing.profile_generation OR firing.control IS NULL
  OR ROW(binding.environment,binding.resource,binding.spec->>'domain',service.authority,service.target_revision)
   IS DISTINCT FROM ROW(schedule.environment,schedule.target,schedule.target_domain,profile.authority,profile.target_service_revision)
  OR NOT firing.markings<@binding.markings
  OR NOT binding.operations@>ARRAY[schedule.resource::text||':run'] THEN RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED'; END IF;
 IF firing.child IS NULL THEN
  metadata:=moria.runtime_automation_dispatch_metadata(binding.context->>'automationDispatchHash',binding.resource,
   binding.spec->>'domain',service.authority,service.target_revision,binding.spec->>'resourceClass');
  IF metadata IS NULL OR metadata->>'firing'<>firing.id::text THEN RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED'; END IF;
 ELSIF NOT EXISTS(SELECT 1 FROM moria.runtime_executions e WHERE e.id=firing.child AND e.binding=binding_id) THEN
  RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED';
 END IF;
 state:=moria.runtime_service_binding_guard(binding_id);
 labels:=moria.native_resource_release_access_state(state,schedule.resource::text,'run',schedule.release);
 IF NOT labels<@to_jsonb(binding.markings) THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
END $$;

CREATE FUNCTION moria.runtime_automation_child_state(execution_id uuid) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE execution moria.runtime_executions; state jsonb; parent_state jsonb; member text;
BEGIN
 SELECT * INTO execution FROM moria.runtime_executions WHERE id=execution_id;
 IF NOT FOUND OR execution.context->>'automationFiring' IS NULL THEN RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED'; END IF;
 PERFORM moria.runtime_automation_binding_guard(execution.binding);
 state:=moria.runtime_service_binding_guard(execution.binding);
 -- The exact live dispatch credential supplies the accepted/current intersection at acceptance.
 SELECT moria.native_serving_session(execution.context->>'automationDispatchHash') INTO parent_state;
 FOREACH member IN ARRAY ARRAY['grantedOperations','applicationScopes','markings'] LOOP
  state:=jsonb_set(state,ARRAY['identity',member],COALESCE((SELECT jsonb_agg(value ORDER BY value COLLATE "C") FROM (
   SELECT jsonb_array_elements_text(state->'identity'->member) AS value INTERSECT SELECT jsonb_array_elements_text(parent_state->'identity'->member)) allowed),'[]'));
 END LOOP;
 RETURN state;
END $$;

CREATE FUNCTION moria.runtime_automation_link_child() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE firing_id uuid; changed integer;
BEGIN
 IF NEW.context->>'automationFiring' IS NULL THEN RETURN NEW; END IF;
 PERFORM moria.runtime_automation_binding_guard(NEW.binding);
 firing_id:=(NEW.context->>'automationFiring')::uuid;
 UPDATE moria.runtime_source_automation_firings SET child=NEW.id WHERE id=firing_id AND child IS NULL;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>1 THEN RAISE EXCEPTION 'AUTOMATION_ALREADY_DISPATCHED'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runtime_source_automation_child AFTER INSERT ON moria.runtime_executions
 FOR EACH ROW EXECUTE FUNCTION moria.runtime_automation_link_child();

CREATE FUNCTION moria.runtime_automation_claim_authority(execution_id uuid,worker uuid,host uuid) RETURNS void
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,moria AS $$
DECLARE execution moria.runtime_executions; task moria.runtime_execution_tasks; firing moria.runtime_source_automation_firings;
 schedule moria.runtime_source_automation_schedules; profile moria.runtime_source_automation_profiles;
 state jsonb; labels jsonb; task_owner uuid;task_fence bigint;
BEGIN
 SELECT e.* INTO execution FROM moria.runtime_executions e JOIN moria.runtime_source_automation_controls c ON c.execution=e.id WHERE e.id=execution_id FOR UPDATE OF e;
 IF NOT FOUND OR execution.spec->>'domain'<>'maintenance' OR execution.status NOT IN ('queued','running') OR execution.deadline<=clock_timestamp() THEN RAISE EXCEPTION 'EXECUTION_INACTIVE'; END IF;
 PERFORM moria.runtime_worker_check(worker,host,execution.resource_class,'maintenance');
 PERFORM moria.runtime_environment_cell_check(execution.environment);
 SELECT t.* INTO task FROM moria.runtime_execution_tasks t WHERE t.execution=execution_id FOR UPDATE;
 IF NOT FOUND OR task.status NOT IN ('pending','sealed') THEN RAISE EXCEPTION 'RESOURCE_BUSY'; END IF;
 SELECT f.* INTO firing FROM moria.runtime_source_automation_firings f JOIN moria.runtime_source_automation_controls c ON c.firing=f.id WHERE c.execution=execution_id;
 SELECT * INTO schedule FROM moria.runtime_source_automation_schedules WHERE id=firing.schedule;
 SELECT * INTO profile FROM moria.runtime_source_automation_profiles WHERE id=schedule.profile AND enabled FOR SHARE;
 IF NOT FOUND OR profile.generation<>firing.profile_generation OR firing.control IS DISTINCT FROM execution.id
  OR firing.outcome<>'enqueued' OR ROW(schedule.environment,schedule.resource,profile.resource_class) IS DISTINCT FROM ROW(execution.environment,execution.resource,execution.resource_class)
  OR NOT EXISTS(SELECT 1 FROM moria.runtime_execution_services s WHERE s.binding=execution.binding AND s.authority=profile.authority AND s.target_revision=profile.service_revision)
  THEN RAISE EXCEPTION 'AUTOMATION_DISPATCH_REVOKED'; END IF;
 state:=moria.native_serving_job_delegation(execution_id);
 PERFORM moria.runtime_binding_authority(execution.binding,(state->>'securityEpoch')::bigint,(state->>'instanceEpoch')::bigint,execution_id);
 labels:=moria.native_resource_release_access_state(state,schedule.resource::text,'run',schedule.release);
 IF NOT labels<@to_jsonb(firing.markings) THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
 IF task.status='sealed' THEN task_owner:=task.owner;task_fence:=task.fence;END IF;
 INSERT INTO moria.runtime_source_automation_epochs(execution,binding,task,owner,fence,incarnation,security_epoch,instance_epoch)
  SELECT execution.id,execution.binding,task.id,task_owner,task_fence,incarnation,(state->>'securityEpoch')::bigint,(state->>'instanceEpoch')::bigint FROM moria.indexing_instance
  ON CONFLICT ON CONSTRAINT runtime_source_automation_epochs_pkey DO UPDATE SET owner=EXCLUDED.owner,fence=EXCLUDED.fence,security_epoch=EXCLUDED.security_epoch,instance_epoch=EXCLUDED.instance_epoch;
END $$;
`;
