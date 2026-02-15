//! Service status models for tracking running services.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Service type identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceType {
    Scraper,
    Ocr,
    Server,
}

impl ServiceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scraper => "scraper",
            Self::Ocr => "ocr",
            Self::Server => "server",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "scraper" => Some(Self::Scraper),
            "ocr" => Some(Self::Ocr),
            "server" => Some(Self::Server),
            _ => None,
        }
    }
}

/// Service status state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceState {
    Starting,
    Running,
    Idle,
    Error,
    Stopped,
}

impl ServiceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Idle => "idle",
            Self::Error => "error",
            Self::Stopped => "stopped",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "idle" => Some(Self::Idle),
            "error" => Some(Self::Error),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

/// Stats for a scraper service.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScraperStats {
    pub session_processed: u64,
    pub session_new: u64,
    pub session_errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_per_min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_failures: Option<u64>,
}

/// Service status record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatus {
    /// Unique identifier (e.g., "scraper:doj", "ocr:worker-1").
    pub id: String,
    /// Type of service.
    pub service_type: ServiceType,
    /// For scrapers: which source they're scraping.
    pub source_id: Option<String>,
    /// Current status.
    pub status: ServiceState,
    /// Last heartbeat timestamp.
    pub last_heartbeat: DateTime<Utc>,
    /// Last time actual work was done.
    pub last_activity: Option<DateTime<Utc>>,
    /// Human-readable current task description.
    pub current_task: Option<String>,
    /// Service-specific stats as JSON.
    pub stats: serde_json::Value,
    /// When the service started.
    pub started_at: DateTime<Utc>,
    /// Container ID or hostname.
    pub host: Option<String>,
    /// App version.
    pub version: Option<String>,
    /// Last error message.
    pub last_error: Option<String>,
    /// When the last error occurred.
    pub last_error_at: Option<DateTime<Utc>>,
    /// Total error count for this session.
    pub error_count: i32,
}

#[allow(dead_code)]
impl ServiceStatus {
    /// Create a new scraper status.
    pub fn new_scraper(source_id: &str) -> Self {
        Self::new_service(
            format!("scraper:{}", source_id),
            ServiceType::Scraper,
            Some(source_id.to_string()),
        )
    }

    /// Create a new server status.
    pub fn new_server() -> Self {
        Self::new_service("server:main".to_string(), ServiceType::Server, None)
    }

    /// Create a new service status with the given parameters.
    fn new_service(id: String, service_type: ServiceType, source_id: Option<String>) -> Self {
        Self {
            id,
            service_type,
            source_id,
            status: ServiceState::Starting,
            last_heartbeat: Utc::now(),
            last_activity: None,
            current_task: None,
            stats: serde_json::json!({}),
            started_at: Utc::now(),
            host: get_hostname(),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            last_error: None,
            last_error_at: None,
            error_count: 0,
        }
    }

    /// Update stats for a scraper.
    pub fn update_scraper_stats(&mut self, stats: ScraperStats) {
        self.stats = serde_json::to_value(stats).unwrap_or_default();
        self.last_heartbeat = Utc::now();
    }

    /// Mark as running with a task description.
    pub fn set_running(&mut self, task: Option<&str>) {
        self.status = ServiceState::Running;
        self.current_task = task.map(|s| s.to_string());
        self.last_heartbeat = Utc::now();
        self.last_activity = Some(Utc::now());
    }

    /// Mark as idle.
    pub fn set_idle(&mut self) {
        self.status = ServiceState::Idle;
        self.current_task = None;
        self.last_heartbeat = Utc::now();
    }

    /// Record an error.
    pub fn record_error(&mut self, error: &str) {
        self.status = ServiceState::Error;
        self.last_error = Some(error.to_string());
        self.last_error_at = Some(Utc::now());
        self.error_count += 1;
        self.last_heartbeat = Utc::now();
    }

    /// Mark as stopped.
    pub fn set_stopped(&mut self) {
        self.status = ServiceState::Stopped;
        self.current_task = None;
        self.last_heartbeat = Utc::now();
    }

    /// Check if the service is stale (no heartbeat for given duration).
    pub fn is_stale(&self, threshold_secs: i64) -> bool {
        let age = Utc::now() - self.last_heartbeat;
        age.num_seconds() > threshold_secs
    }
}

/// Get the current hostname.
fn get_hostname() -> Option<String> {
    hostname::get().ok().and_then(|h| h.into_string().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_type_roundtrip() {
        for stype in [ServiceType::Scraper, ServiceType::Ocr, ServiceType::Server] {
            let s = stype.as_str();
            let parsed = ServiceType::from_str(s);
            assert_eq!(parsed, Some(stype));
        }
    }

    #[test]
    fn test_service_type_from_invalid() {
        assert_eq!(ServiceType::from_str("unknown"), None);
        assert_eq!(ServiceType::from_str(""), None);
    }

    #[test]
    fn test_service_state_roundtrip() {
        for state in [
            ServiceState::Starting,
            ServiceState::Running,
            ServiceState::Idle,
            ServiceState::Error,
            ServiceState::Stopped,
        ] {
            let s = state.as_str();
            let parsed = ServiceState::from_str(s);
            assert_eq!(parsed, Some(state));
        }
    }

    #[test]
    fn test_service_state_from_invalid() {
        assert_eq!(ServiceState::from_str("unknown"), None);
        assert_eq!(ServiceState::from_str(""), None);
    }

    #[test]
    fn test_new_scraper() {
        let status = ServiceStatus::new_scraper("doj");
        assert_eq!(status.id, "scraper:doj");
        assert_eq!(status.service_type, ServiceType::Scraper);
        assert_eq!(status.source_id, Some("doj".to_string()));
        assert_eq!(status.status, ServiceState::Starting);
        assert!(status.host.is_some() || status.host.is_none()); // Depends on system
        assert!(status.version.is_some());
    }

    #[test]
    fn test_new_server() {
        let status = ServiceStatus::new_server();
        assert_eq!(status.id, "server:main");
        assert_eq!(status.service_type, ServiceType::Server);
        assert_eq!(status.source_id, None);
        assert_eq!(status.status, ServiceState::Starting);
    }

    #[test]
    fn test_set_running() {
        let mut status = ServiceStatus::new_scraper("test");
        status.set_running(Some("Processing documents"));
        assert_eq!(status.status, ServiceState::Running);
        assert_eq!(
            status.current_task,
            Some("Processing documents".to_string())
        );
        assert!(status.last_activity.is_some());
    }

    #[test]
    fn test_set_idle() {
        let mut status = ServiceStatus::new_scraper("test");
        status.set_running(Some("Working"));
        status.set_idle();
        assert_eq!(status.status, ServiceState::Idle);
        assert_eq!(status.current_task, None);
    }

    #[test]
    fn test_record_error() {
        let mut status = ServiceStatus::new_scraper("test");
        assert_eq!(status.error_count, 0);

        status.record_error("Connection failed");
        assert_eq!(status.status, ServiceState::Error);
        assert_eq!(status.last_error, Some("Connection failed".to_string()));
        assert!(status.last_error_at.is_some());
        assert_eq!(status.error_count, 1);

        status.record_error("Another error");
        assert_eq!(status.error_count, 2);
    }

    #[test]
    fn test_set_stopped() {
        let mut status = ServiceStatus::new_scraper("test");
        status.set_running(Some("Working"));
        status.set_stopped();
        assert_eq!(status.status, ServiceState::Stopped);
        assert_eq!(status.current_task, None);
    }

    #[test]
    fn test_update_scraper_stats() {
        let mut status = ServiceStatus::new_scraper("test");
        let stats = ScraperStats {
            session_processed: 100,
            session_new: 50,
            session_errors: 2,
            rate_per_min: Some(10.5),
            queue_size: Some(1000),
            browser_failures: None,
        };
        status.update_scraper_stats(stats);

        let stored: ScraperStats = serde_json::from_value(status.stats.clone()).unwrap();
        assert_eq!(stored.session_processed, 100);
        assert_eq!(stored.session_new, 50);
        assert_eq!(stored.session_errors, 2);
        assert_eq!(stored.rate_per_min, Some(10.5));
    }

    #[test]
    fn test_is_stale() {
        let mut status = ServiceStatus::new_scraper("test");
        // Just created, should not be stale
        assert!(!status.is_stale(60));

        // Manually set heartbeat to old time
        status.last_heartbeat = Utc::now() - chrono::Duration::seconds(120);
        assert!(status.is_stale(60));
        assert!(!status.is_stale(180));
    }

    #[test]
    fn test_scraper_stats_default() {
        let stats = ScraperStats::default();
        assert_eq!(stats.session_processed, 0);
        assert_eq!(stats.session_new, 0);
        assert_eq!(stats.session_errors, 0);
        assert_eq!(stats.rate_per_min, None);
        assert_eq!(stats.queue_size, None);
        assert_eq!(stats.browser_failures, None);
    }

    #[test]
    fn test_scraper_stats_serialization() {
        let stats = ScraperStats {
            session_processed: 10,
            session_new: 5,
            session_errors: 1,
            rate_per_min: None,
            queue_size: None,
            browser_failures: None,
        };
        let json = serde_json::to_string(&stats).unwrap();
        // None fields should be skipped
        assert!(!json.contains("rate_per_min"));
        assert!(json.contains("session_processed"));

        let parsed: ScraperStats = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.session_processed, 10);
    }
}
