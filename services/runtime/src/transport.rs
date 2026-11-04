use crate::{
    database::Database,
    identity::{IdentityRegistry, Ownership},
    wire::runtime::*,
};
use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Clone)]
struct Service {
    db: Database,
    admission: Arc<Semaphore>,
}

struct Peer {
    owner: Uuid,
    cell: Option<Uuid>,
    incarnation: Uuid,
    now: u64,
    fingerprint: String,
    _permit: OwnedSemaphorePermit,
}

impl Service {
    async fn admit<T>(
        &self,
        request: &Request<T>,
        context: Option<&RequestContext>,
        role: &str,
        bootstrap: bool,
    ) -> std::result::Result<Peer, Status> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("ADMISSION_LIMIT"))?;
        let context = context.ok_or_else(|| Status::invalid_argument("CONTEXT_REQUIRED"))?;
        if context.protocol_version != 1 || Uuid::parse_str(&context.request_id).is_err() {
            return Err(Status::invalid_argument("PROTOCOL_OR_REQUEST_INVALID"));
        }
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("SERVICE_IDENTITY_REQUIRED"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("SERVICE_IDENTITY_REQUIRED"))?;
        let fingerprint = format!("{:x}", Sha256::digest(leaf.as_ref()));
        let client = self
            .db
            .get()
            .await
            .map_err(|_| Status::unavailable("AUTHORITY_UNAVAILABLE"))?;
        let row = client.query_opt(
            "SELECT p.owner,p.cell,p.incarnation,floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS now FROM moria.native_service_peers p JOIN moria.indexing_instance i ON i.incarnation=p.incarnation WHERE fingerprint=$1 AND enabled AND expires_at>clock_timestamp() AND $2=ANY(roles)",
            &[&fingerprint,&role],
        ).await.map_err(|_| Status::unavailable("AUTHORITY_UNAVAILABLE"))?.ok_or_else(|| Status::permission_denied("SERVICE_NOT_AUTHORIZED"))?;
        let incarnation: Uuid = row.get("incarnation");
        let now = row.get::<_, i64>("now") as u64;
        if !(context.deadline_unix_ms > now && context.deadline_unix_ms <= now + 30_000) {
            return Err(Status::deadline_exceeded("DEADLINE_INVALID_OR_EXPIRED"));
        }
        if !(bootstrap && context.incarnation.is_empty())
            && context.incarnation != incarnation.to_string()
        {
            return Err(Status::failed_precondition("INCARNATION_CHANGED"));
        }
        if !context.environment_id.is_empty() && Uuid::parse_str(&context.environment_id).is_err() {
            return Err(Status::invalid_argument("ENVIRONMENT_INVALID"));
        }
        Ok(Peer {
            owner: row.get("owner"),
            cell: row.get("cell"),
            incarnation,
            now,
            fingerprint,
            _permit: permit,
        })
    }

    async fn finish(
        &self,
        peer: &Peer,
        role: &str,
        deadline: u64,
    ) -> std::result::Result<(), Status> {
        let client = self
            .db
            .get()
            .await
            .map_err(|_| Status::unavailable("AUTHORITY_UNAVAILABLE"))?;
        let valid = client.query_one(
            "SELECT EXISTS(SELECT 1 FROM moria.native_service_peers p JOIN moria.indexing_instance i ON i.incarnation=p.incarnation WHERE p.fingerprint=$1 AND p.owner=$2 AND p.incarnation=$3 AND p.enabled AND p.expires_at>clock_timestamp() AND $4=ANY(p.roles)) AND floor(extract(epoch FROM clock_timestamp())*1000)<$5::bigint",
            &[&peer.fingerprint,&peer.owner,&peer.incarnation,&role,&i64::try_from(deadline).map_err(|_|Status::invalid_argument("DEADLINE_INVALID"))?],
        ).await.map_err(|_|Status::unavailable("AUTHORITY_UNAVAILABLE"))?.get::<_,bool>(0);
        if !valid {
            return Err(Status::permission_denied("SERVICE_AUTHORIZATION_CHANGED"));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl runtime_metadata_server::RuntimeMetadata for Service {
    async fn inspect_capabilities(
        &self,
        request: Request<CapabilityRequest>,
    ) -> std::result::Result<Response<Capabilities>, Status> {
        let peer = self
            .admit(
                &request,
                request.get_ref().context.as_ref(),
                "metadata",
                true,
            )
            .await?;
        let result = Capabilities {
            protocol_version: 1,
            value_version: 1,
            source_digest: env!("MORIA_INDEXING_SOURCE_SHA256").into(),
            incarnation: peer.incarnation.to_string(),
            database_time_unix_ms: peer.now,
            implemented: vec![
                Capability {
                    name: "runtime.metadata".into(),
                    interface_kind: "grpc".into(),
                    version: 1,
                },
                Capability {
                    name: "identity.resolve".into(),
                    interface_kind: "grpc".into(),
                    version: 1,
                },
                Capability {
                    name: "identity.transfer".into(),
                    interface_kind: "grpc".into(),
                    version: 1,
                },
                Capability {
                    name: "environment.directory".into(),
                    interface_kind: "grpc".into(),
                    version: 1,
                },
                Capability {
                    name: "execution.scheduler".into(),
                    interface_kind: "cli".into(),
                    version: 1,
                },
            ],
        };
        self.finish(
            &peer,
            "metadata",
            request.get_ref().context.as_ref().unwrap().deadline_unix_ms,
        )
        .await?;
        Ok(Response::new(result))
    }
}

fn owner(owner: Option<DomainOwner>) -> std::result::Result<Ownership, Status> {
    let owner = owner.ok_or_else(|| Status::invalid_argument("OWNER_REQUIRED"))?;
    if owner.fence == 0 || owner.fence > i64::MAX as u64 {
        return Err(Status::invalid_argument("FENCE_INVALID"));
    }
    Ok(Ownership {
        cell: Uuid::parse_str(&owner.cell_id)
            .map_err(|_| Status::invalid_argument("CELL_INVALID"))?,
        fence: owner.fence.to_string(),
        incarnation: Uuid::parse_str(&owner.incarnation)
            .map_err(|_| Status::invalid_argument("INCARNATION_INVALID"))?,
    })
}

#[tonic::async_trait]
impl identity_authority_server::IdentityAuthority for Service {
    async fn resolve_identities(
        &self,
        request: Request<ResolveIdentitiesRequest>,
    ) -> std::result::Result<Response<ResolvedIdentities>, Status> {
        let peer = self
            .admit(
                &request,
                request.get_ref().context.as_ref(),
                "identity-owner",
                false,
            )
            .await?;
        let input = request.into_inner();
        let ownership = owner(input.owner)?;
        if peer.cell != Some(ownership.cell) || peer.incarnation != ownership.incarnation {
            return Err(Status::permission_denied("OWNER_MISMATCH"));
        }
        let uids = IdentityRegistry {
            db: self.db.clone(),
        }
        .resolve_for_service(
            &input.domain,
            &input.canonical_keys,
            &ownership,
            &peer.fingerprint,
            peer.owner,
        )
        .await
        .map_err(identity_error)?;
        self.finish(
            &peer,
            "identity-owner",
            input.context.as_ref().unwrap().deadline_unix_ms,
        )
        .await?;
        Ok(Response::new(ResolvedIdentities {
            uids: uids.into_iter().map(|id| id.to_string()).collect(),
        }))
    }

    async fn transfer_domain(
        &self,
        request: Request<TransferDomainRequest>,
    ) -> std::result::Result<Response<DomainOwner>, Status> {
        let peer = self
            .admit(
                &request,
                request.get_ref().context.as_ref(),
                "identity-admin",
                false,
            )
            .await?;
        let input = request.into_inner();
        let next = Uuid::parse_str(&input.next_cell_id)
            .map_err(|_| Status::invalid_argument("CELL_INVALID"))?;
        let result = IdentityRegistry {
            db: self.db.clone(),
        }
        .transfer_for_service(
            &input.domain,
            &owner(input.expected)?,
            next,
            &peer.fingerprint,
            peer.owner,
        )
        .await
        .map_err(identity_error)?;
        self.finish(
            &peer,
            "identity-admin",
            input.context.as_ref().unwrap().deadline_unix_ms,
        )
        .await?;
        Ok(Response::new(DomainOwner {
            cell_id: result.cell.to_string(),
            fence: result
                .fence
                .parse()
                .map_err(|_| Status::internal("FENCE_INVALID"))?,
            incarnation: result.incarnation.to_string(),
        }))
    }
}

fn identity_error(error: anyhow::Error) -> Status {
    for cause in error.chain() {
        if let Some(db) = cause
            .downcast_ref::<tokio_postgres::Error>()
            .and_then(|e| e.as_db_error())
        {
            return match db.message() {
                "OWNERSHIP_LOST" => Status::failed_precondition("OWNERSHIP_LOST"),
                "IDENTITY_REQUEST_INVALID" => Status::invalid_argument("IDENTITY_REQUEST_INVALID"),
                _ => Status::failed_precondition("IDENTITY_OPERATION_REJECTED"),
            };
        }
    }
    Status::failed_precondition("IDENTITY_OPERATION_REJECTED")
}

fn material(name: &str, limit: u64) -> Result<Vec<u8>> {
    use std::io::Read;
    let path = env::var(name).map_err(|_| anyhow::anyhow!("TLS material reference required"))?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "TLS material limit");
    Ok(bytes)
}

fn placement(
    value: crate::directory::Placement,
) -> std::result::Result<EnvironmentPlacement, Status> {
    Ok(EnvironmentPlacement {
        environment_id: value.environment,
        owner: Some(DomainOwner {
            cell_id: value.owner.cell.to_string(),
            fence: value
                .owner
                .fence
                .parse()
                .map_err(|_| Status::internal("DIRECTORY_INVALID"))?,
            incarnation: value.owner.incarnation.to_string(),
        }),
        endpoint_ref: value.endpoint_ref,
        serving: value.serving,
        leases_fenced: value.leases_fenced,
    })
}
#[tonic::async_trait]
impl environment_directory_server::EnvironmentDirectory for Service {
    async fn inspect_placement(
        &self,
        request: Request<InspectPlacementRequest>,
    ) -> std::result::Result<Response<EnvironmentPlacement>, Status> {
        let context = request.get_ref().context.as_ref();
        let peer = self
            .admit(&request, context, "environment-router", false)
            .await?;
        let context = context.unwrap();
        if context.environment_id.is_empty() {
            return Err(Status::invalid_argument("ENVIRONMENT_REQUIRED"));
        }
        let value = crate::directory::EnvironmentDirectory {
            db: self.db.clone(),
        }
        .inspect(&context.environment_id)
        .await
        .map_err(|_| Status::unavailable("DIRECTORY_UNAVAILABLE"))?;
        self.finish(&peer, "environment-router", context.deadline_unix_ms)
            .await?;
        Ok(Response::new(placement(value)?))
    }
    async fn freeze_placement(
        &self,
        request: Request<FreezePlacementRequest>,
    ) -> std::result::Result<Response<EnvironmentPlacement>, Status> {
        let peer = self
            .admit(
                &request,
                request.get_ref().context.as_ref(),
                "environment-admin",
                false,
            )
            .await?;
        let input = request.into_inner();
        let context = input.context.unwrap();
        if context.environment_id.is_empty() {
            return Err(Status::invalid_argument("ENVIRONMENT_REQUIRED"));
        }
        let expected = owner(input.expected)?;
        let next = Uuid::parse_str(&input.next_cell_id)
            .map_err(|_| Status::invalid_argument("CELL_INVALID"))?;
        let value = crate::directory::EnvironmentDirectory {
            db: self.db.clone(),
        }
        .freeze(
            &context.environment_id,
            &expected,
            next,
            Some((&peer.fingerprint, peer.owner)),
        )
        .await
        .map_err(|_| Status::failed_precondition("CELL_OWNERSHIP_LOST"))?;
        self.finish(&peer, "environment-admin", context.deadline_unix_ms)
            .await?;
        Ok(Response::new(placement(value)?))
    }
    async fn activate_placement(
        &self,
        request: Request<ActivatePlacementRequest>,
    ) -> std::result::Result<Response<EnvironmentPlacement>, Status> {
        let peer = self
            .admit(
                &request,
                request.get_ref().context.as_ref(),
                "environment-admin",
                false,
            )
            .await?;
        let input = request.into_inner();
        let context = input.context.unwrap();
        if context.environment_id.is_empty() {
            return Err(Status::invalid_argument("ENVIRONMENT_REQUIRED"));
        }
        let expected = owner(input.expected)?;
        let value = crate::directory::EnvironmentDirectory {
            db: self.db.clone(),
        }
        .activate(
            &context.environment_id,
            &expected,
            Some((&peer.fingerprint, peer.owner)),
        )
        .await
        .map_err(|_| Status::failed_precondition("CELL_OWNERSHIP_LOST"))?;
        self.finish(&peer, "environment-admin", context.deadline_unix_ms)
            .await?;
        Ok(Response::new(placement(value)?))
    }
}

pub async fn serve(db: Database) -> Result<()> {
    let listen: SocketAddr = env::var("MORIA_RPC_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:7443".into())
        .parse()?;
    let maximum: usize = env::var("MORIA_RPC_MAXIMUM_ACTIVE")
        .unwrap_or_else(|_| "32".into())
        .parse()?;
    ensure!(
        (1..=1024).contains(&maximum),
        "Invalid RPC admission budget"
    );
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            material("MORIA_RPC_CERT_FILE", 65536)?,
            material("MORIA_RPC_KEY_FILE", 65536)?,
        ))
        .client_ca_root(Certificate::from_pem(material(
            "MORIA_RPC_CLIENT_CA_FILE",
            65536,
        )?))
        .client_auth_optional(false)
        .timeout(Duration::from_secs(5));
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!(
        "{}",
        serde_json::json!({"event":"rpc-listening","address":listener.local_addr()?.to_string()})
    );
    let service = Service {
        db,
        admission: Arc::new(Semaphore::new(maximum)),
    };
    let lifecycle = crate::lifecycle::Lifecycle::configured()?;
    let shutdown = lifecycle.clone();
    lifecycle
        .run(
            Server::builder()
                .tls_config(tls)?
                .timeout(Duration::from_secs(30))
                .concurrency_limit_per_connection(maximum)
                .add_service(
                    runtime_metadata_server::RuntimeMetadataServer::new(service.clone())
                        .max_decoding_message_size(65536)
                        .max_encoding_message_size(65536),
                )
                .add_service(
                    identity_authority_server::IdentityAuthorityServer::new(service.clone())
                        .max_decoding_message_size(2 * 1024 * 1024)
                        .max_encoding_message_size(128 * 1024),
                )
                .add_service(
                    environment_directory_server::EnvironmentDirectoryServer::new(service)
                        .max_decoding_message_size(65536)
                        .max_encoding_message_size(65536),
                )
                .serve_with_incoming_shutdown(
                    futures::stream::unfold(
                        crate::http_admission::BoundedListener::new(listener),
                        |mut listener| async move {
                            let (socket, _) = axum::serve::Listener::accept(&mut listener).await;
                            Some((Ok::<_, std::io::Error>(socket), listener))
                        },
                    ),
                    async move { shutdown.shutdown_signal().await },
                ),
        )
        .await?;
    Ok(())
}
