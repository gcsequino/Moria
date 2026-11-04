use super::*;

/// Published authorization declarations verify independent administration; they never grant it.
pub(super) async fn validate_in(
    client: &Client,
    session: &VerifiedSession,
    release: &Value,
    resource: &Value,
) -> Result<bool> {
    let root: Uuid = release["rootId"].as_str().unwrap().parse()?;
    let spec = &resource["spec"];
    if resource["kind"] == "Application" {
        let id = bindings::id_in(
            client,
            session.environment(),
            root,
            &bindings::resource_reference(resource),
        )
        .await?;
        let row=client.query_opt("SELECT operations,enabled FROM moria.application_scopes WHERE environment=$1 AND application=$2", &[&session.environment(),&id.to_string()]).await?;
        let Some(row) = row else { return Ok(false) };
        if !row.get::<_, bool>(1) {
            return Ok(false);
        }
        let actual = row
            .get::<_, Vec<String>>(0)
            .into_iter()
            .collect::<BTreeSet<_>>();
        for scope in spec["scopes"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("SOURCE_INVALID"))?
        {
            let target = bindings::id_in(
                client,
                session.environment(),
                root,
                scope["target"].as_str().unwrap(),
            )
            .await?;
            for operation in scope["operations"].as_array().unwrap() {
                if !actual.contains(&format!("{target}:{}", operation.as_str().unwrap())) {
                    return Ok(false);
                }
            }
        }
        return Ok(true);
    }
    ensure!(resource["kind"] == "Grant", "SOURCE_INVALID");
    if spec["environment"] != session.environment() {
        return Ok(false);
    }
    let application = bindings::id_in(
        client,
        session.environment(),
        root,
        spec["application"].as_str().unwrap(),
    )
    .await?;
    let target = bindings::id_in(
        client,
        session.environment(),
        root,
        spec["target"].as_str().unwrap(),
    )
    .await?;
    for subject in spec["subjects"].as_array().unwrap() {
        let row=client.query_opt("SELECT p.operations,a.operations AS scopes FROM moria.permissions p JOIN moria.principals x ON x.environment=p.environment AND x.principal=p.principal AND x.enabled JOIN moria.application_scopes a ON a.environment=p.environment AND a.application=p.application AND a.enabled WHERE p.environment=$1 AND p.principal=$2 AND p.application=$3", &[&session.environment(),&subject.as_str().unwrap(),&application.to_string()]).await?;
        let Some(row) = row else { return Ok(false) };
        let scopes = row
            .get::<_, Vec<String>>(1)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let actual = row
            .get::<_, Vec<String>>(0)
            .into_iter()
            .filter(|operation| scopes.contains(operation))
            .collect::<BTreeSet<_>>();
        for operation in spec["operations"].as_array().unwrap() {
            if !actual.contains(&format!("{target}:{}", operation.as_str().unwrap())) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
