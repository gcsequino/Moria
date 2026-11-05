use crate::{
    authority::{Authority, SecuritySnapshot, VerifiedSession},
    database::Database,
    execution::ExecutionSpec,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

/// Metadata authority loaded from an operator-approved target/purpose binding.
/// It is not a session, is not deserializable, and cannot authorize storage IO or inference.
#[derive(Clone)]
pub(crate) struct RegisteredService {
    authority: Uuid,
    environment: String,
    target: Uuid,
    domain: String,
    binding_revision: String,
    resource_class: String,
    principal: String,
    application: String,
}

pub(crate) struct ServiceBinding {
    pub id: Uuid,
    pub security_epoch: String,
    pub instance_epoch: String,
}
enum ChildBinding<'a> {
    Automation(&'a VerifiedSession, &'a Value),
    Evaluation(&'a VerifiedSession, &'a Value),
    Sandbox(&'a VerifiedSession, &'a Value),
}

impl RegisteredService {
    pub(crate) async fn load_in(
        _db: &Database,
        client: &Client,
        environment: &str,
        target: Uuid,
        domain: &str,
        binding_revision: &str,
    ) -> Result<Self> {
        Self::load_current_in(client, environment, target, domain, binding_revision).await
    }
    pub(crate) async fn load_current_in(
        client: &Client,
        environment: &str,
        target: Uuid,
        domain: &str,
        binding_revision: &str,
    ) -> Result<Self> {
        ensure!(
            !environment.is_empty()
                && !domain.is_empty()
                && !binding_revision.is_empty()
                && binding_revision.len() <= 128,
            "SERVICE_PURPOSE_REVOKED"
        );
        let row=client.query_opt("SELECT a.id,a.principal,a.application,t.resource_class FROM moria.runtime_service_targets t JOIN moria.runtime_service_authorities a ON a.id=t.authority AND a.enabled AND a.environment=t.environment JOIN moria.indexing_instance i ON i.incarnation=a.incarnation WHERE t.environment=$1 AND t.resource=$2 AND t.domain=$3 AND t.binding_revision=$4 AND t.enabled AND t.domain=ANY(a.domains) AND t.resource_class=ANY(a.resource_classes) FOR SHARE OF a,t",&[&environment,&target,&domain,&binding_revision]).await?.ok_or_else(||anyhow::anyhow!("SERVICE_PURPOSE_REVOKED"))?;
        let service = Self {
            authority: row.get(0),
            environment: environment.into(),
            target,
            domain: domain.into(),
            binding_revision: binding_revision.into(),
            resource_class: row.get(3),
            principal: row.get(1),
            application: row.get(2),
        };
        service.state_in(client).await?;
        Ok(service)
    }
    pub(crate) fn id(&self) -> Uuid {
        self.authority
    }
    pub(crate) fn binding_revision(&self) -> &str {
        &self.binding_revision
    }
    pub(crate) fn resource_class(&self) -> &str {
        &self.resource_class
    }
    pub(crate) fn domain(&self) -> &str {
        &self.domain
    }
    pub(crate) fn environment(&self) -> &str {
        &self.environment
    }
    pub(crate) fn principal(&self) -> &str {
        &self.principal
    }
    pub(crate) fn application(&self) -> &str {
        &self.application
    }
    pub(crate) fn target(&self) -> Uuid {
        self.target
    }
    pub(crate) async fn state_in(&self, client: &Client) -> Result<SecuritySnapshot> {
        let state: Value = client
            .query_one(
                "SELECT moria.runtime_service_target_authority($1,$2,$3,$4,$5,$6)",
                &[
                    &self.authority,
                    &self.environment,
                    &self.target,
                    &self.domain,
                    &self.binding_revision,
                    &self.resource_class,
                ],
            )
            .await?
            .get(0);
        let state: SecuritySnapshot = serde_json::from_value(state)?;
        ensure!(
            state.environment == self.environment
                && state.principal == self.principal
                && state.application == self.application,
            "SERVICE_PURPOSE_REVOKED"
        );
        Ok(state)
    }
    pub(crate) async fn resource_labels_in(
        &self,
        client: &Client,
        resource: &str,
        operation: &str,
    ) -> Result<Vec<String>> {
        self.resource_labels_for_operations_in(client, resource, operation, None)
            .await
    }
    pub(crate) async fn resource_labels_for_operations_in(
        &self,
        client: &Client,
        resource: &str,
        operation: &str,
        additional_operation: Option<&str>,
    ) -> Result<Vec<String>> {
        let labels: Value = client
            .query_one(
                "SELECT moria.runtime_service_resource_labels($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                &[
                    &self.authority,
                    &self.environment,
                    &self.target,
                    &self.domain,
                    &self.binding_revision,
                    &self.resource_class,
                    &resource,
                    &operation,
                    &additional_operation,
                ],
            )
            .await?
            .get(0);
        Ok(serde_json::from_value(labels)?)
    }
    /// Caller authorization is domain-specific and precedes this private composition.
    /// The requester is retained for audit; it does not become the service actor.
    pub(crate) async fn bind_in(
        &self,
        client: &Client,
        spec: &Value,
        operations: &[String],
        markings: &[String],
        requester: &VerifiedSession,
    ) -> Result<ServiceBinding> {
        ensure!(
            requester.environment() == self.environment,
            "SERVICE_ENVIRONMENT_MISMATCH"
        );
        Authority::current(client, requester).await?;
        self.bind_authorized_in(client, spec, operations, markings, Some(requester), None)
            .await
    }
    pub(crate) async fn bind_background_in(
        &self,
        client: &Client,
        spec: &Value,
        operations: &[String],
        markings: &[String],
    ) -> Result<ServiceBinding> {
        self.bind_authorized_in(client, spec, operations, markings, None, None)
            .await
    }
    pub(crate) async fn bind_dispatch_in(
        &self,
        client: &Client,
        spec: &Value,
        operations: &[String],
        markings: &[String],
        requester: &VerifiedSession,
        metadata: &Value,
    ) -> Result<ServiceBinding> {
        self.bind_authorized_in(
            client,
            spec,
            operations,
            markings,
            Some(requester),
            Some(ChildBinding::Automation(requester, metadata)),
        )
        .await
    }
    pub(crate) async fn bind_evaluation_in(
        &self,
        client: &Client,
        spec: &Value,
        operations: &[String],
        markings: &[String],
        requester: &VerifiedSession,
        metadata: &Value,
    ) -> Result<ServiceBinding> {
        self.bind_authorized_in(
            client,
            spec,
            operations,
            markings,
            Some(requester),
            Some(ChildBinding::Evaluation(requester, metadata)),
        )
        .await
    }
    pub(crate) async fn bind_sandbox_child_in(
        &self,
        client: &Client,
        spec: &Value,
        operations: &[String],
        markings: &[String],
        requester: &VerifiedSession,
        metadata: &Value,
    ) -> Result<ServiceBinding> {
        self.bind_authorized_in(
            client,
            spec,
            operations,
            markings,
            Some(requester),
            Some(ChildBinding::Sandbox(requester, metadata)),
        )
        .await
    }
    async fn bind_authorized_in(
        &self,
        client: &Client,
        spec: &Value,
        operations: &[String],
        markings: &[String],
        requester: Option<&VerifiedSession>,
        dispatch: Option<ChildBinding<'_>>,
    ) -> Result<ServiceBinding> {
        let parsed: ExecutionSpec = serde_json::from_value(spec.clone())?;
        parsed.validate()?;
        ensure!(
            spec["domain"].as_str() == Some(self.domain.as_str())
                && parsed.resource_class == self.resource_class
                && !operations.is_empty()
                && operations.len() <= 256
                && markings.len() <= 256,
            "SERVICE_PURPOSE_REVOKED"
        );
        let state = self.state_in(client).await?;
        ensure!(
            operations.iter().all(|operation| state
                .identity
                .granted_operations
                .contains(operation)
                && state.identity.application_scopes.contains(operation))
                && markings
                    .iter()
                    .all(|marking| state.identity.markings.contains(marking)),
            "SERVICE_PURPOSE_REVOKED"
        );
        let audit = match requester {
            Some(requester) => {
                json!({"kind":"requester","environment":requester.environment(),"principal":requester.principal(),"application":requester.application()})
            }
            None => json!({"kind":"registeredService","authority":self.authority}),
        };
        let mut context = json!({"environment":self.environment,"principal":self.principal,"application":self.application,"serviceAuthority":self.authority,"serviceTargetRevision":self.binding_revision,"requester":audit});
        if let Some(dispatch) = dispatch {
            let (requester, metadata) = match &dispatch {
                ChildBinding::Automation(session, metadata)
                | ChildBinding::Evaluation(session, metadata)
                | ChildBinding::Sandbox(session, metadata) => (*session, *metadata),
            };
            ensure!(
                requester.environment() == self.environment
                    && requester.principal() == self.principal
                    && requester.application() == self.application,
                "AUTOMATION_DISPATCH_REVOKED"
            );
            match dispatch {
                ChildBinding::Automation(..) => {
                    context["automationFiring"] = metadata["firing"].clone();
                    context["automationDispatchHash"] = json!(requester.session_hash());
                }
                ChildBinding::Evaluation(..) => {
                    context["evaluationCase"] = metadata["case"].clone();
                    context["evaluationParentHash"] = json!(requester.session_hash());
                    context["parentExecution"] = metadata["case"].clone();
                    context["parentDeadline"] = metadata["parentDeadline"].clone();
                }
                ChildBinding::Sandbox(..) => {
                    context["serviceParentExecution"] = metadata["parent"].clone();
                    context["serviceParentHash"] = json!(requester.session_hash());
                    context["serviceParentTask"] = metadata["task"].clone();
                    context["serviceParentOwner"] = metadata["owner"].clone();
                    context["serviceParentFence"] = metadata["fence"].clone();
                    context["serviceParentIncarnation"] = metadata["incarnation"].clone();
                    context["parentExecution"] = metadata["parent"].clone();
                    context["parentDeadline"] = metadata["parentDeadline"].clone();
                }
            }
        }
        let row=client.query_one("INSERT INTO moria.runtime_execution_bindings(environment,resource,spec,context,operations,markings,security_epoch,instance_epoch,incarnation) SELECT $1,$2,$3,$4,$5,$6,$7,$8,incarnation FROM moria.indexing_instance RETURNING id,security_epoch::text,instance_epoch::text",&[&self.environment,&self.target,&spec,&context,&operations,&markings,&state.security_epoch.parse::<i64>()?,&state.instance_epoch.parse::<i64>()?]).await?;
        let id: Uuid = row.get(0);
        client.execute("INSERT INTO moria.runtime_execution_services(binding,authority,target_revision) VALUES($1,$2,$3)",&[&id,&self.authority,&self.binding_revision]).await?;
        Ok(ServiceBinding {
            id,
            security_epoch: row.get(1),
            instance_epoch: row.get(2),
        })
    }
}
