use super::*;

impl DockerRuntime {
    /// Prepare the admitted digest before any container mutation. Callers must
    /// also supervise live authority so renewals cannot reset startup's budget.
    pub async fn prepare_image(&self, spec: &ExecutionSpec) -> Result<(), RuntimeError> {
        let budget = Duration::from_secs(spec.job().spec.timeouts.startup_seconds);
        tokio::time::timeout(budget, self.prepare_pinned_image(&spec.job().spec.image))
            .await
            .map_err(|_| RuntimeError::Deadline)?
    }

    async fn prepare_pinned_image(&self, image: &str) -> Result<(), RuntimeError> {
        match bounded(RPC_TIMEOUT, self.docker.inspect_image(image)).await {
            Ok(_) => return Ok(()),
            Err(RuntimeError::Daemon(404)) => {}
            Err(error) => return Err(error),
        }
        // The validated spec contains a digest, never an import URL or mutable
        // tag. Docker fetches outside the workload's disabled network namespace.
        // API: https://docs.rs/bollard/0.21.1/bollard/struct.Docker.html#method.create_image
        let options = CreateImageOptionsBuilder::default()
            .from_image(image)
            .build();
        let mut stream = self.docker.create_image(Some(options), None, None);
        let mut records = 0;
        while let Some(progress) = stream.next().await {
            let progress = progress.map_err(RuntimeError::from)?;
            records += 1;
            // Bound noisy daemon progress without retaining or logging registry
            // diagnostics, which may contain credentials or private names.
            if records > 65_536 || progress.error_detail.is_some() {
                return Err(RuntimeError::Transport);
            }
        }
        // A completed stream is not proof of a cached image. Resolve the exact
        // admitted digest again; a partial pull cannot authorize later create.
        bounded(RPC_TIMEOUT, self.docker.inspect_image(image)).await?;
        Ok(())
    }
}
