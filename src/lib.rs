//! Google Calendar v3 incremental sync poller — **scaffold**.
//!
//! Full port from `crates/poller/src/builtins/google_calendar.rs` is
//! tracked as a Phase 96 follow-up. The scaffold ships the trait
//! impl skeleton + manifest wiring + config shape so the daemon can
//! discover the plugin and the broker round-trip can be exercised
//! end-to-end before the fetch logic lands.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use nexo_microapp_sdk::poller::{PollerHandler, TickRequest};
use nexo_poller::{PollerError, PollerHost, TickAck, TickMetrics};

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CalendarJobConfig {
    #[serde(default = "default_calendar_id")]
    pub calendar_id: String,
    #[serde(default = "default_template")]
    pub message_template: String,
    #[serde(default = "default_skip_cancelled")]
    pub skip_cancelled: bool,
    pub deliver: DeliverCfg,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeliverCfg {
    pub channel: String,
    #[serde(alias = "recipient")]
    pub to: String,
}

fn default_calendar_id() -> String {
    "primary".into()
}
fn default_skip_cancelled() -> bool {
    true
}
fn default_template() -> String {
    "📅 {summary} — {start}\n{html_link}".to_string()
}

pub struct GoogleCalendarHandler;

impl GoogleCalendarHandler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GoogleCalendarHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PollerHandler for GoogleCalendarHandler {
    async fn tick(
        &self,
        req: TickRequest,
        host: std::sync::Arc<dyn PollerHost>,
    ) -> Result<TickAck, PollerError> {
        let _cfg: CalendarJobConfig =
            serde_json::from_value(req.config.clone()).map_err(|e| PollerError::Config {
                job: req.job_id.clone(),
                reason: e.to_string(),
            })?;

        // Resolve Google credentials via reverse-RPC. The daemon
        // returns `{ account_id, client_id_path, token_path, ... }`
        // for the agent's bound Google account.
        let _cred = host
            .credentials_get("google".into())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get: {e}")))?;

        // TODO Phase 96 follow-up: port the fetch + diff + dispatch
        // logic from the in-tree builtin. For now report a no-op
        // so the daemon's runner sees a successful round-trip.
        host.log(
            nexo_poller::LogLevel::Info,
            format!("google_calendar tick stub — job {}", req.job_id),
            json!({}),
        )
        .await
        .ok();

        Ok(TickAck {
            next_cursor: None,
            next_interval_hint: None,
            metrics: Some(TickMetrics::default()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let cfg: CalendarJobConfig = serde_json::from_value(json!({
            "deliver": { "channel": "whatsapp", "to": "+57300" },
        }))
        .unwrap();
        assert_eq!(cfg.calendar_id, "primary");
        assert!(cfg.skip_cancelled);
    }

    #[test]
    fn config_accepts_recipient_alias() {
        let cfg: CalendarJobConfig = serde_json::from_value(json!({
            "deliver": { "channel": "telegram", "recipient": "-100" },
        }))
        .unwrap();
        assert_eq!(cfg.deliver.to, "-100");
    }
}
