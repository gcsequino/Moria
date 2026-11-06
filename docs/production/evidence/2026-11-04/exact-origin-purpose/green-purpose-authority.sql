CREATE OR REPLACE FUNCTION moria.runtime_binding_authority(binding_id uuid, environment_epoch bigint, instance_epoch bigint, execution_proof uuid DEFAULT NULL) RETURNS void LANGUAGE plpgsql AS $$
DECLARE b moria.runtime_execution_bindings; p record; valid boolean; delegated jsonb; purpose_delegated boolean;
BEGIN
  SELECT * INTO b FROM moria.runtime_execution_bindings WHERE id=binding_id;
  IF NOT FOUND OR NOT b.enabled THEN RAISE EXCEPTION 'BINDING_CHANGED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM moria.environments e,moria.instance_state i,moria.indexing_instance c
    WHERE e.id=b.environment AND e.security_epoch=environment_epoch AND i.security_epoch=instance_epoch
      AND c.incarnation=b.incarnation) THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  SELECT a.enabled AS app_enabled,x.enabled,x.kind,x.synced_at,
    (ib.instance_id IS NULL OR (ip.enabled AND ip.kind=x.kind)) AS instance_enabled,
    x.markings,a.operations AS scopes,g.operations AS grants INTO p
    FROM moria.principals x JOIN moria.application_scopes a ON a.environment=x.environment AND a.application=b.context->>'application'
    JOIN moria.permissions g ON g.environment=x.environment AND g.application=a.application AND g.principal=x.principal
    LEFT JOIN moria.instance_principal_bindings ib ON ib.environment=x.environment AND ib.principal=x.principal
    LEFT JOIN moria.instance_principals ip ON ip.id=ib.instance_id
    WHERE x.environment=b.environment AND x.principal=b.context->>'principal';
  IF NOT FOUND OR NOT p.enabled OR NOT p.app_enabled OR NOT p.instance_enabled
    OR (p.kind='human' AND p.synced_at<=clock_timestamp()-interval '60 seconds')
    OR NOT b.operations<@p.scopes OR NOT b.operations<@p.grants OR NOT b.markings<@p.markings
    THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
  purpose_delegated:=execution_proof IS NOT NULL AND EXISTS(
    SELECT 1 FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id
    WHERE e.id=execution_proof AND e.binding=b.id AND e.context=b.context
      AND d.origin_kind='service' AND d.origin_binding=b.id
      AND ROW(d.environment,d.principal,d.application)=ROW(b.environment,b.context->>'principal',b.context->>'application'));
  IF NOT purpose_delegated THEN
    IF b.context->>'automationFiring' IS NOT NULL THEN PERFORM moria.runtime_automation_binding_guard(b.id); END IF;
    IF b.context->>'evaluationCase' IS NOT NULL THEN PERFORM moria.runtime_evaluation_service_binding_guard(b.id); END IF;
    IF b.context->>'serviceParentExecution' IS NOT NULL THEN PERFORM moria.runtime_sandbox_service_child_binding_guard(b.id); END IF;
  END IF;
  IF execution_proof IS NOT NULL THEN
    BEGIN
      IF NOT EXISTS(SELECT 1 FROM moria.runtime_executions e JOIN moria.runtime_job_delegations d ON d.execution=e.id
        WHERE e.id=execution_proof AND e.binding=b.id AND e.context=b.context
          AND ROW(d.environment,d.principal,d.application)=ROW(b.environment,b.context->>'principal',b.context->>'application'))
        THEN RAISE EXCEPTION 'JOB_DELEGATION_REQUIRED'; END IF;
      delegated:=moria.native_serving_job_delegation(execution_proof);
      IF ROW(delegated->>'environment',delegated->>'principal',delegated->>'application')
        IS DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application')
        OR NOT to_jsonb(b.operations)<@(delegated->'identity'->'grantedOperations')
        OR NOT to_jsonb(b.operations)<@(delegated->'identity'->'applicationScopes')
        OR NOT to_jsonb(b.markings)<@(delegated->'identity'->'markings') THEN RAISE EXCEPTION 'AUTHORIZATION_CHANGED'; END IF;
    EXCEPTION WHEN raise_exception THEN
      -- Preserve direct purpose error precedence while validating a successful delegation once.
      IF purpose_delegated THEN
        IF b.context->>'automationFiring' IS NOT NULL THEN PERFORM moria.runtime_automation_binding_guard(b.id); END IF;
        IF b.context->>'evaluationCase' IS NOT NULL THEN PERFORM moria.runtime_evaluation_service_binding_guard(b.id); END IF;
        IF b.context->>'serviceParentExecution' IS NOT NULL THEN PERFORM moria.runtime_sandbox_service_child_binding_guard(b.id); END IF;
      END IF;
      RAISE;
    END;
  ELSIF b.context->>'sessionHash' IS NOT NULL THEN
    SELECT EXISTS(SELECT 1 FROM moria.workload_sessions s WHERE s.token_hash=b.context->>'sessionHash'
      AND s.environment=b.environment AND s.principal=b.context->>'principal' AND s.application=b.context->>'application'
      AND NOT s.disabled AND s.expires_at>clock_timestamp() AND p.kind='workload'
      UNION ALL SELECT 1 FROM moria.browser_sessions s LEFT JOIN moria.auth_providers a
      ON a.environment=s.environment AND a.application=s.application AND a.provider=s.provider
      WHERE s.token_hash=b.context->>'sessionHash' AND s.environment=b.environment AND s.principal=b.context->>'principal'
      AND s.application=b.context->>'application' AND NOT s.disabled AND s.expires_at>clock_timestamp()
      AND ((s.provider IS NULL AND p.kind='workload') OR (s.provider IS NOT NULL AND p.kind='human'
      AND a.enabled AND a.revision=s.revision AND s.verified_at>clock_timestamp()-interval '60 seconds'))) INTO valid;
    IF NOT valid THEN
      -- Nested admission may originate from an actual live private parent lease.
      -- The common resolver still rejects an expired ordinary hash without that proof.
      delegated:=moria.native_serving_session(b.context->>'sessionHash');
      valid:=ROW(delegated->>'environment',delegated->>'principal',delegated->>'application')
        IS NOT DISTINCT FROM ROW(b.environment,b.context->>'principal',b.context->>'application')
        AND to_jsonb(b.operations)<@(delegated->'identity'->'grantedOperations')
        AND to_jsonb(b.operations)<@(delegated->'identity'->'applicationScopes')
        AND to_jsonb(b.markings)<@(delegated->'identity'->'markings');
    END IF;
    IF NOT valid THEN RAISE EXCEPTION 'UNAUTHENTICATED'; END IF;
  END IF;
END $$;
