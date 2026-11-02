use std::time::Duration;

/// Runtime limits of the agent query API (`[agent_api]` / `ARNEB_AGENT_*`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLimits {
    /// Rows returned per request when the caller names no limit.
    pub default_max_rows: usize,
    /// Ceiling for a caller-supplied row limit.
    pub max_rows: usize,
    /// Response bytes per page when the caller names no limit.
    pub default_page_bytes: usize,
    /// Ceiling for a caller-supplied page byte limit.
    pub max_page_bytes: usize,
    /// Longest rendered cell; longer values are truncated.
    pub max_cell_bytes: usize,
    /// Arrow memory one statement may retain, while collecting and after;
    /// rows past it are dropped (`truncated_by: max_retained_bytes`).
    pub max_retained_bytes: usize,
    /// Arrow memory one user's statements together may retain, running
    /// ones (still collecting) and ended ones alike, so one user cannot take
    /// the whole server budget. A statement that finds it short evicts the
    /// user's own ended statements when that covers the shortfall, then
    /// keeps what fits (`truncated_by: user_budget`).
    pub max_user_retained_bytes: usize,
    /// Arrow memory all statements together may retain, running and ended.
    /// A statement that finds it short evicts ended statements (its user's
    /// own first, then fully read ones, then least recently used) when that
    /// covers the shortfall, then keeps what fits
    /// (`truncated_by: server_budget`).
    pub max_total_retained_bytes: usize,
    /// Longest any request waits before answering "still running".
    pub max_wait: Duration,
    /// Idle time after which a statement is reaped; at least `max_wait`.
    /// A statement under a long-poll is never idle, whatever the values.
    pub idle_timeout: Duration,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            default_max_rows: 1000,
            max_rows: 10_000,
            default_page_bytes: 262_144,
            max_page_bytes: 4_194_304,
            max_cell_bytes: 8192,
            max_retained_bytes: 64 << 20,
            max_user_retained_bytes: 256 << 20,
            max_total_retained_bytes: 1 << 30,
            max_wait: Duration::from_millis(10_000),
            idle_timeout: Duration::from_secs(600),
        }
    }
}

impl AgentLimits {
    /// Check every value is positive and each default fits under its ceiling.
    /// The error names the offending field.
    pub fn validate(&self) -> Result<(), String> {
        let positive = [
            ("default_max_rows", self.default_max_rows == 0),
            ("max_rows", self.max_rows == 0),
            ("default_page_bytes", self.default_page_bytes == 0),
            ("max_page_bytes", self.max_page_bytes == 0),
            ("max_cell_bytes", self.max_cell_bytes == 0),
            ("max_retained_bytes", self.max_retained_bytes == 0),
            ("max_user_retained_bytes", self.max_user_retained_bytes == 0),
            (
                "max_total_retained_bytes",
                self.max_total_retained_bytes == 0,
            ),
            ("max_wait_ms", self.max_wait.is_zero()),
            ("idle_timeout_secs", self.idle_timeout.is_zero()),
        ];
        if let Some((name, _)) = positive.iter().find(|(_, zero)| *zero) {
            return Err(format!("{name} must be greater than 0"));
        }
        if self.default_max_rows > self.max_rows {
            return Err(format!(
                "default_max_rows ({}) must not exceed max_rows ({})",
                self.default_max_rows, self.max_rows
            ));
        }
        if self.default_page_bytes > self.max_page_bytes {
            return Err(format!(
                "default_page_bytes ({}) must not exceed max_page_bytes ({})",
                self.default_page_bytes, self.max_page_bytes
            ));
        }
        // Checked first: the server raises a share below the per-statement
        // cap to it, so a cap above the total must be blamed on itself.
        if self.max_retained_bytes > self.max_total_retained_bytes {
            return Err(format!(
                "max_retained_bytes ({}) must not exceed max_total_retained_bytes ({})",
                self.max_retained_bytes, self.max_total_retained_bytes
            ));
        }
        if self.max_retained_bytes > self.max_user_retained_bytes {
            return Err(format!(
                "max_retained_bytes ({}) must not exceed max_user_retained_bytes ({})",
                self.max_retained_bytes, self.max_user_retained_bytes
            ));
        }
        if self.max_user_retained_bytes > self.max_total_retained_bytes {
            return Err(format!(
                "max_user_retained_bytes ({}) must not exceed max_total_retained_bytes ({})",
                self.max_user_retained_bytes, self.max_total_retained_bytes
            ));
        }
        if self.idle_timeout < self.max_wait {
            return Err(format!(
                "idle_timeout_secs ({}s) must not be shorter than max_wait_ms ({}ms)",
                self.idle_timeout.as_secs(),
                self.max_wait.as_millis()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let l = AgentLimits::default();
        l.validate().unwrap();
        // One user cannot take the whole server budget.
        assert!(l.max_user_retained_bytes < l.max_total_retained_bytes);
    }

    #[test]
    fn zero_and_inverted_limits_are_rejected() {
        let l = AgentLimits {
            max_cell_bytes: 0,
            ..Default::default()
        };
        assert!(l.validate().unwrap_err().contains("max_cell_bytes"));
        let l = AgentLimits {
            default_page_bytes: 10,
            max_page_bytes: 5,
            ..Default::default()
        };
        assert!(l.validate().unwrap_err().contains("default_page_bytes"));
        let l = AgentLimits {
            max_retained_bytes: 512 << 20,
            ..Default::default()
        };
        let err = l.validate().unwrap_err();
        assert!(
            err.contains("max_retained_bytes") && err.contains("max_user_retained_bytes"),
            "{err}"
        );
        // A cap above the total is blamed on the cap, even with the share
        // raised to it.
        let l = AgentLimits {
            max_retained_bytes: 2 << 30,
            max_user_retained_bytes: 2 << 30,
            ..Default::default()
        };
        let err = l.validate().unwrap_err();
        assert!(
            err.starts_with("max_retained_bytes") && err.contains("max_total_retained_bytes"),
            "{err}"
        );
        let l = AgentLimits {
            max_user_retained_bytes: 2 << 30,
            ..Default::default()
        };
        let err = l.validate().unwrap_err();
        assert!(
            err.contains("max_user_retained_bytes") && err.contains("max_total_retained_bytes"),
            "{err}"
        );
    }

    #[test]
    fn idle_timeout_shorter_than_max_wait_is_rejected() {
        let l = AgentLimits {
            idle_timeout: Duration::from_secs(5),
            max_wait: Duration::from_secs(10),
            ..Default::default()
        };
        let err = l.validate().unwrap_err();
        assert!(
            err.contains("idle_timeout_secs") && err.contains("max_wait_ms"),
            "{err}"
        );
        // Equal values are fine: the waiter guard keeps a long-polled
        // statement from being reaped, so no slack is needed.
        for idle in [10, 15] {
            let l = AgentLimits {
                idle_timeout: Duration::from_secs(idle),
                max_wait: Duration::from_secs(10),
                ..Default::default()
            };
            l.validate().unwrap();
        }
    }
}
