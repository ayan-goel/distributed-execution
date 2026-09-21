use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonoTime(Duration);

impl MonoTime {
    pub fn now() -> Result<Self, LeaseError> {
        #[cfg(target_os = "linux")]
        {
            let mut value = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // CLOCK_BOOTTIME includes suspend time. CLOCK_MONOTONIC/Instant can
            // pause during suspend and leave stale authority usable after resume.
            // SAFETY: clock_gettime writes into a valid, exclusively borrowed timespec.
            let result = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut value) };
            if result != 0 || value.tv_sec < 0 || !(0..1_000_000_000).contains(&value.tv_nsec) {
                return Err(LeaseError::Clock);
            }
            Ok(Self(Duration::new(
                value.tv_sec as u64,
                value.tv_nsec as u32,
            )))
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Non-Linux builds support protocol development only; container
            // execution remains Linux-only and cannot rely on this fallback.
            static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
            let origin = ORIGIN.get_or_init(std::time::Instant::now);
            std::time::Instant::now()
                .checked_duration_since(*origin)
                .map(Self)
                .ok_or(LeaseError::Clock)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseError {
    Invalid,
    Clock,
    Expired,
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid authority duration",
            Self::Clock => "worker monotonic clock failed",
            Self::Expired => "local execution authority expired",
        })
    }
}
impl std::error::Error for LeaseError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityWindow {
    sent: MonoTime,
    deadline: MonoTime,
}

impl AuthorityWindow {
    pub fn from_grant(
        sent: MonoTime,
        received: MonoTime,
        lease_ms: u64,
        phase_ms: u64,
    ) -> Result<Self, LeaseError> {
        if received < sent {
            return Err(LeaseError::Clock);
        }
        if lease_ms == 0 || lease_ms > 30_000 || phase_ms == 0 || phase_ms > 604_800_000 {
            return Err(LeaseError::Invalid);
        }
        // Use request-send time, not receipt. Subtract the five-second lease
        // margin before accounting for RPC delay; neither retries nor queued
        // responses can manufacture a fresh full lease on delivery.
        let usable_lease = lease_ms.checked_sub(5_000).ok_or(LeaseError::Expired)?;
        // Phase timeouts may legitimately be one second. They use the same
        // conservative send-time origin without subtracting the lease margin.
        let duration = Duration::from_millis(usable_lease.min(phase_ms));
        let deadline = MonoTime(sent.0.checked_add(duration).ok_or(LeaseError::Invalid)?);
        let window = Self { sent, deadline };
        window.remaining_at(received)?;
        Ok(window)
    }

    pub fn remaining(&self) -> Result<Duration, LeaseError> {
        self.remaining_at(MonoTime::now()?)
    }

    pub fn remaining_at(&self, now: MonoTime) -> Result<Duration, LeaseError> {
        if now < self.sent {
            return Err(LeaseError::Clock);
        }
        if now >= self.deadline {
            return Err(LeaseError::Expired);
        }
        Ok(self.deadline.0 - now.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(seconds: u64) -> MonoTime {
        MonoTime(Duration::from_secs(seconds))
    }

    #[test]
    fn network_delay_never_starts_a_fresh_lease_on_receipt() {
        let grant = AuthorityWindow::from_grant(tick(100), tick(104), 30_000, 300_000).unwrap();
        assert_eq!(
            grant.remaining_at(tick(104)).unwrap(),
            Duration::from_secs(21)
        );
        assert_eq!(
            grant.remaining_at(tick(124)).unwrap(),
            Duration::from_secs(1)
        );
        assert_eq!(grant.remaining_at(tick(125)), Err(LeaseError::Expired));
    }

    #[test]
    fn late_responses_and_host_pauses_expire_authority() {
        assert_eq!(
            AuthorityWindow::from_grant(tick(100), tick(125), 30_000, 300_000),
            Err(LeaseError::Expired)
        );
        let grant = AuthorityWindow::from_grant(tick(100), tick(101), 30_000, 300_000).unwrap();
        assert_eq!(grant.remaining_at(tick(160)), Err(LeaseError::Expired));
    }

    #[test]
    fn short_phase_deadlines_remain_usable_and_bound_execution() {
        let grant = AuthorityWindow::from_grant(tick(100), tick(100), 30_000, 1_000).unwrap();
        assert_eq!(
            grant.remaining_at(tick(100)).unwrap(),
            Duration::from_secs(1)
        );
        assert_eq!(grant.remaining_at(tick(101)), Err(LeaseError::Expired));
    }

    #[test]
    fn invalid_bounds_regression_and_overflow_fail_closed() {
        for (lease, phase) in [
            (0, 1000),
            (30_001, 1000),
            (30_000, 0),
            (30_000, 604_800_001),
            (u64::MAX, u64::MAX),
            (5000, 1000),
        ] {
            assert!(AuthorityWindow::from_grant(tick(100), tick(100), lease, phase).is_err());
        }
        assert_eq!(
            AuthorityWindow::from_grant(tick(100), tick(99), 30_000, 300_000),
            Err(LeaseError::Clock)
        );
        let grant = AuthorityWindow::from_grant(tick(100), tick(100), 30_000, 300_000).unwrap();
        assert_eq!(grant.remaining_at(tick(99)), Err(LeaseError::Clock));
        assert!(AuthorityWindow::from_grant(
            MonoTime(Duration::MAX),
            MonoTime(Duration::MAX),
            30_000,
            300_000
        )
        .is_err());
    }

    #[test]
    fn operating_system_clock_is_available_and_nondecreasing() {
        let first = MonoTime::now().unwrap();
        let second = MonoTime::now().unwrap();
        assert!(second >= first);
    }
}
