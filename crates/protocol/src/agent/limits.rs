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
    /// Longest any request waits before answering "still running".
    pub max_wait: Duration,
    /// Idle time after which a statement is reaped.
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
        AgentLimits::default().validate().unwrap();
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
        let l = AgentLimits {
            idle_timeout: Duration::from_secs(10),
            max_wait: Duration::from_secs(10),
            ..Default::default()
        };
        l.validate().unwrap();
    }
}
