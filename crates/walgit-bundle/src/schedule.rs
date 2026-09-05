//! Cron schedule parsing and due evaluation for bundle strategies.
//!
//! Supports the `cron` crate's 6/7-field syntax plus the `@hourly`, `@daily`,
//! `@weekly` (and `@monthly`, `@yearly`) shorthand aliases natively parsed by
//! the `cron` crate.

use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use cron::Schedule;

use crate::BundleError;

/// Parse a cron expression or shorthand alias into a [`Schedule`].
///
/// The `cron` crate already handles `@hourly`/`@daily`/`@weekly`/`@monthly`/
/// `@yearly`, so this is a thin wrapper that surfaces a useful error.
pub fn parse_schedule(expr: &str) -> Result<Schedule, BundleError> {
    Schedule::from_str(expr.trim())
        .map_err(|e| BundleError::InvalidSchedule(format!("{expr}: {e}")))
}

/// Convert [`SystemTime`] to a chrono UTC datetime.
fn to_chrono(t: SystemTime) -> DateTime<Utc> {
    DateTime::<Utc>::from(t)
}

/// Convert a chrono UTC datetime back to [`SystemTime`].
fn to_system(dt: DateTime<Utc>) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(dt.timestamp().max(0).unsigned_abs())
}

/// Next fire time of `schedule` strictly after `after`, or `None` if the
/// schedule is exhausted (e.g. a bounded year range has passed).
pub fn next_fire_after(schedule: &Schedule, after: SystemTime) -> Option<SystemTime> {
    let dt = to_chrono(after);
    schedule.after(&dt).next().map(to_system)
}

/// Whether a strategy is due at `now`.
///
/// * `last_built` = `None` (never built) → due.
/// * `last_built` = `Some(t)` → due iff the next scheduled fire after `t`
///   is at or before `now`.
/// * If the schedule is exhausted → not due.
pub fn is_due(schedule: &Schedule, last_built: Option<SystemTime>, now: SystemTime) -> bool {
    match last_built {
        None => true,
        Some(last) => {
            let next = next_fire_after(schedule, last);
            match next {
                Some(fire) => fire <= now,
                None => false,
            }
        }
    }
}

/// Current Unix timestamp in seconds (for `creation_token` computation).
pub fn unix_now(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_aliases() {
        assert!(parse_schedule("@hourly").is_ok());
        assert!(parse_schedule("@daily").is_ok());
        assert!(parse_schedule("@weekly").is_ok());
        assert!(parse_schedule("@monthly").is_ok());
        assert!(parse_schedule("@yearly").is_ok());
    }

    #[test]
    fn parse_6field() {
        assert!(parse_schedule("0 0 * * * *").is_ok()); // hourly
        assert!(parse_schedule("0 0 0 * * *").is_ok()); // daily
        assert!(parse_schedule("0 0 0 * * 1").is_ok()); // weekly (Sunday, 6 fields)
    }

    #[test]
    fn parse_7field() {
        assert!(parse_schedule("0 30 9 * * * 2024").is_ok());
        assert!(parse_schedule("0 0 0 * * 1 *").is_ok()); // weekly (Sunday, 7 fields)
    }

    #[test]
    fn parse_bad() {
        assert!(parse_schedule("not a cron").is_err());
        assert!(parse_schedule("").is_err());
    }

    #[test]
    fn due_never_built() {
        let s = parse_schedule("@hourly").unwrap();
        let now = SystemTime::now();
        assert!(is_due(&s, None, now));
    }

    #[test]
    fn due_after_fire_time() {
        let s = parse_schedule("@hourly").unwrap();
        let now = SystemTime::now();
        // Last built 2 hours ago → next fire was 1 hour ago → due.
        let last = now - Duration::from_hours(2);
        assert!(is_due(&s, Some(last), now));
    }

    #[test]
    fn not_due_before_fire_time() {
        let s = parse_schedule("@daily").unwrap();
        let now = SystemTime::now();
        // Last built 1 second ago → next fire is ~24h away → not due.
        let last = now - Duration::from_secs(1);
        assert!(!is_due(&s, Some(last), now));
    }

    #[test]
    fn next_fire_advances() {
        let s = parse_schedule("@hourly").unwrap();
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let next = next_fire_after(&s, t).unwrap();
        // Next fire should be after t.
        assert!(next > t);
        // And within 1 hour (hourly schedule).
        assert!(next <= t + Duration::from_hours(1));
    }
}
