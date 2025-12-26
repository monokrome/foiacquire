//! User agent handling for HTTP requests.

pub const USER_AGENT: &str = "FOIAcquire/0.1 (academic research; github.com/monokrome/foiacquire)";

/// Real browser user agents for impersonate mode.
/// These are current user agents from popular browsers (updated Nov 2024).
pub const IMPERSONATE_USER_AGENTS: &[&str] = &[
    // Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    // Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    // Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    // Firefox on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
    // Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    // Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
];

/// Get a random user agent for impersonate mode.
pub fn random_user_agent() -> &'static str {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as usize)
        .unwrap_or(0);
    IMPERSONATE_USER_AGENTS[nanos % IMPERSONATE_USER_AGENTS.len()]
}

/// Resolve user agent from config value.
/// - None => default FOIAcquire user agent
/// - "impersonate" => random real browser user agent
/// - other => custom user agent string
pub fn resolve_user_agent(config: Option<&str>) -> String {
    match config {
        None => USER_AGENT.to_string(),
        Some("impersonate") => random_user_agent().to_string(),
        Some(custom) => custom.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_user_agent_default() {
        let ua = resolve_user_agent(None);
        assert!(ua.contains("FOIAcquire"));
    }

    #[test]
    fn test_resolve_user_agent_impersonate() {
        let ua = resolve_user_agent(Some("impersonate"));
        assert!(ua.contains("Mozilla"));
        assert!(!ua.contains("FOIAcquire"));
    }

    #[test]
    fn test_resolve_user_agent_custom() {
        let ua = resolve_user_agent(Some("MyBot/1.0"));
        assert_eq!(ua, "MyBot/1.0");
    }

    #[test]
    fn test_random_user_agent_varies() {
        // Check that random_user_agent returns valid user agents
        let ua = random_user_agent();
        assert!(ua.contains("Mozilla"));
    }
}
