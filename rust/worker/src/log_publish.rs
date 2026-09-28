//! Register sealed log segments while their container is still running.
use crate::{
    control::ControlClient, journal::AsyncJournal, log_delivery::deliver_log,
    log_live::SharedAssembler, supervisor::SupervisedAuthority, transfer::TransferClient,
};
use std::time::Duration;
use tokio::sync::oneshot;

const POLL_INTERVAL: Duration = Duration::from_millis(200);

pub(crate) async fn publish_running(
    assembler: SharedAssembler,
    journal: AsyncJournal,
    mut client: ControlClient,
    transfers: TransferClient,
    attempt_id: String,
    mut authority: SupervisedAuthority,
    mut stop: oneshot::Receiver<()>,
) {
    loop {
        if stop.try_recv().is_ok() {
            return;
        }
        // A sealed segment is immutable, so the spool lock is needed only to
        // inspect or acknowledge it, never during the remote transfer.
        let pending = {
            let guard = assembler.lock().await;
            guard.front().map(|segment| {
                (
                    segment.stream(),
                    segment.first_sequence(),
                    segment.last_sequence(),
                    segment.gaps().to_vec(),
                    segment.size(),
                    segment.sha256().to_owned(),
                )
            })
        };
        let Some((stream, first, last, gaps, size, sha256)) = pending else {
            tokio::select! {
                _ = &mut stop => return,
                _ = tokio::time::sleep(POLL_INTERVAL) => continue,
            }
        };
        // INVARIANT: a segment may be registered only while this attempt still
        // has live authority. Finalization can replay any interrupted journaled
        // transfer, but stale workers must not publish new results.
        let result = authority
            .while_live(async {
                journal
                    .prepare_log(attempt_id.clone(), stream, first, last, gaps, size, sha256)
                    .await
                    .map_err(|_| ())?;
                let source = assembler.lock().await.read_front().map_err(|_| ())?;
                deliver_log(
                    &journal,
                    &mut client,
                    &transfers,
                    &attempt_id,
                    stream,
                    first,
                    source,
                )
                .await
                .map_err(|_| ())?;
                assembler.lock().await.acknowledge_front().map_err(|_| ())
            })
            .await;
        if !matches!(result, Ok(Ok(()))) {
            // If execution reaches finalization, it retries this journaled
            // segment and reports any remaining gap. Logs cannot fail the job.
            return;
        }
    }
}
