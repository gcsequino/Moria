use super::*;

impl NativePlatformBroker {
    pub(super) async fn link_read(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        resource: &str,
        input: &Value,
        call: &HttpCall,
    ) -> Result<Value> {
        match tokio::time::timeout(
            NativeServing::link_timeout(input)?,
            self.link_read_attempt(context, session, resource, input, call),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                context.block_release();
                Err(anyhow::anyhow!("QUERY_TIMEOUT"))
            }
        }
    }
    async fn link_read_attempt(
        &self,
        context: &SandboxContext,
        session: &VerifiedSession,
        resource: &str,
        input: &Value,
        call: &HttpCall,
    ) -> Result<Value> {
        let admission = Admission::begin(&self.db, session, false).await?;
        let outcome = async {
            let client = admission.client();
            context.check_in(client).await?;
            let mut root = NativeServing::link_root_for_broker(client, session, resource, input).await?;
            let markings = Self::labels(session.environment(), root.markings.clone());
            context.record_consumed_markings(&markings)?;
            Self::record(client, session, context.lease().execution, Some(context.attempt_id()),
                &format!("link-root/{}", call.id), &root.evidence, &markings).await?;
            client.batch_execute("SAVEPOINT native_link_read").await?;
            let observed = NativeServing::traverse_for_broker(client, session, &root).await;
            let callback = match observed {
                Ok(read) => {
                    client.batch_execute("RELEASE SAVEPOINT native_link_read").await?;
                    let markings = Self::labels(session.environment(), read.markings);
                    context.record_consumed_markings(&markings)?;
                    Self::record(client, session, context.lease().execution, Some(context.attempt_id()),
                        &format!("link/{}", call.id), &read.evidence, &markings).await?;
                    Ok(read.value)
                }
                Err(error) => {
                    client.batch_execute("ROLLBACK TO SAVEPOINT native_link_read; RELEASE SAVEPOINT native_link_read").await?;
                    NativeServing::failed_link_read(&mut root, error_code(&error));
                    Self::record(client, session, context.lease().execution, Some(context.attempt_id()),
                        &format!("link-failure/{}", call.id), &root.evidence, &markings).await?;
                    Err(error)
                }
            };
            Self::trace(client, context, call, None, &callback, false).await?;
            context.check_in(client).await?;
            Ok(callback)
        }.await;
        match admission.complete(outcome).await {
            Ok(callback) => callback,
            Err(error) => {
                context.block_release();
                Err(error)
            }
        }
    }
}
