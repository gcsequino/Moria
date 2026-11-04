use anyhow::Result;
use moria_runtime::wire::runtime::{
    identity_authority_client::IdentityAuthorityClient,
    runtime_metadata_client::RuntimeMetadataClient, *,
};
use serde::Deserialize;
use std::{env, fs, time::Duration};
use tonic::Code;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    endpoint: String,
    ca: String,
    cert: String,
    key: String,
    rogue_cert: String,
    rogue_key: String,
    incarnation: String,
    cell: String,
    other_cell: String,
    domain: String,
    environment: String,
    peer_fingerprint: String,
}

fn context(incarnation: &str) -> RequestContext {
    RequestContext {
        protocol_version: 1,
        request_id: Uuid::new_v4().to_string(),
        incarnation: incarnation.into(),
        environment_id: String::new(),
        deadline_unix_ms: (chrono::Utc::now().timestamp_millis() + 5000) as u64,
    }
}

async fn channel(f: &Fixture, identity: Option<(&str, &str)>) -> Result<Channel> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(fs::read(&f.ca)?))
        .domain_name("localhost");
    if let Some((cert, key)) = identity {
        tls = tls.identity(Identity::from_pem(fs::read(cert)?, fs::read(key)?));
    }
    Ok(Endpoint::from_shared(f.endpoint.clone())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .tls_config(tls)?
        .connect()
        .await?)
}

#[tokio::test]
#[ignore = "Requires the mandatory PostgreSQL/mutual-TLS integration fixture"]
async fn rpc_authority() -> Result<()> {
    let f: Fixture = serde_json::from_slice(&fs::read(env::var("MORIA_RPC_TEST_FIXTURE")?)?)?;
    let connection = channel(&f, Some((&f.cert, &f.key))).await?;
    let mut metadata = RuntimeMetadataClient::new(connection.clone());
    let capabilities = metadata
        .inspect_capabilities(CapabilityRequest {
            context: Some(context("")),
        })
        .await?
        .into_inner();
    assert_eq!(capabilities.protocol_version, 1);
    assert_eq!(capabilities.value_version, 1);
    assert_eq!(capabilities.incarnation, f.incarnation);
    assert_eq!(capabilities.source_digest.len(), 64);
    assert!(
        capabilities
            .implemented
            .iter()
            .any(|c| c.name == "identity.resolve" && c.interface_kind == "grpc")
    );
    assert!(
        !capabilities
            .implemented
            .iter()
            .any(|c| c.name.contains("search"))
    );

    let mut invalid = context("");
    invalid.protocol_version = 99;
    assert_eq!(
        metadata
            .inspect_capabilities(CapabilityRequest {
                context: Some(invalid)
            })
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut expired = context("");
    expired.deadline_unix_ms = 1;
    assert_eq!(
        metadata
            .inspect_capabilities(CapabilityRequest {
                context: Some(expired)
            })
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    let mut rogue =
        RuntimeMetadataClient::new(channel(&f, Some((&f.rogue_cert, &f.rogue_key))).await?);
    assert_eq!(
        rogue
            .inspect_capabilities(CapabilityRequest {
                context: Some(context(""))
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    if let Ok(connection) = channel(&f, None).await {
        assert!(
            RuntimeMetadataClient::new(connection)
                .inspect_capabilities(CapabilityRequest {
                    context: Some(context(""))
                })
                .await
                .is_err()
        );
    }

    let mut identity = IdentityAuthorityClient::new(connection.clone());
    let owner = DomainOwner {
        cell_id: f.cell.clone(),
        fence: 1,
        incarnation: f.incarnation.clone(),
    };
    let request = ResolveIdentitiesRequest {
        context: Some(context(&f.incarnation)),
        domain: f.domain.clone(),
        owner: Some(owner.clone()),
        canonical_keys: vec!["shared".into(), "other".into()],
    };
    let result = identity
        .resolve_identities(request.clone())
        .await?
        .into_inner();
    assert_eq!(result.uids.len(), 2);
    assert_ne!(result.uids[0], result.uids[1]);
    assert_eq!(
        identity
            .resolve_identities(request.clone())
            .await?
            .into_inner()
            .uids,
        result.uids
    );
    let mut forged = request.clone();
    forged.owner.as_mut().unwrap().cell_id = f.other_cell.clone();
    assert_eq!(
        identity
            .resolve_identities(forged)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        identity
            .transfer_domain(TransferDomainRequest {
                context: Some(context(&f.incarnation)),
                domain: f.domain.clone(),
                expected: Some(owner),
                next_cell_id: f.other_cell.clone()
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    let (db, driver) =
        tokio_postgres::connect(&env::var("MORIA_RPC_TEST_DATABASE")?, tokio_postgres::NoTls)
            .await?;
    tokio::spawn(async move {
        let _ = driver.await;
    });

    let mut directory = environment_directory_client::EnvironmentDirectoryClient::new(connection);
    let env_context = || {
        let mut c = context(&f.incarnation);
        c.environment_id = f.environment.clone();
        Some(c)
    };
    let placed = directory
        .inspect_placement(InspectPlacementRequest {
            context: env_context(),
        })
        .await?
        .into_inner();
    assert_eq!(placed.owner.as_ref().unwrap().cell_id, f.cell);
    assert!(placed.serving && placed.leases_fenced);
    let freezing = FreezePlacementRequest {
        context: env_context(),
        expected: placed.owner.clone(),
        next_cell_id: f.other_cell.clone(),
    };
    assert_eq!(
        directory
            .freeze_placement(freezing.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    db.execute("UPDATE moria.native_service_peers SET roles=array_append(roles,'environment-admin') WHERE fingerprint=$1",&[&f.peer_fingerprint]).await?;
    let frozen = directory
        .freeze_placement(freezing.clone())
        .await?
        .into_inner();
    assert!(!frozen.serving && frozen.leases_fenced);
    assert_eq!(frozen.owner.as_ref().unwrap().fence, 2);
    assert_eq!(
        directory
            .freeze_placement(freezing.clone())
            .await?
            .into_inner()
            .owner
            .unwrap()
            .fence,
        2
    );
    let active = directory
        .activate_placement(ActivatePlacementRequest {
            context: env_context(),
            expected: frozen.owner,
        })
        .await?
        .into_inner();
    assert!(active.serving && active.leases_fenced);
    assert_eq!(active.owner.unwrap().cell_id, f.other_cell);
    assert_eq!(
        directory
            .freeze_placement(freezing)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    db.execute(
        "UPDATE moria.native_service_peers SET enabled=false WHERE fingerprint=$1",
        &[&f.peer_fingerprint],
    )
    .await?;
    assert_eq!(
        metadata
            .inspect_capabilities(CapabilityRequest {
                context: Some(context(""))
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        identity
            .resolve_identities(request.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    db.execute("UPDATE moria.native_service_peers SET enabled=true,expires_at=clock_timestamp()-interval '1 second' WHERE fingerprint=$1",&[&f.peer_fingerprint]).await?;
    assert_eq!(
        metadata
            .inspect_capabilities(CapabilityRequest {
                context: Some(context(""))
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    Ok(())
}
include!("../native-common/artifact.rs");
