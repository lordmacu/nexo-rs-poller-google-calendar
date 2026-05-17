//! Google Calendar v3 events incremental sync poller.
//!
//! Cursor stores the `nextSyncToken` Google returns. First tick
//! fetches a window with `timeMin = now`, captures the
//! `nextSyncToken`, dispatches nothing. Subsequent ticks pass
//! `syncToken = <cursor>` and dispatch only the diff. Token expiry
//! (HTTP 410 / invalid_grant) → `Permanent` error so the operator
//! runs `agent pollers reset <id>` to re-baseline.
//!
//! Ported from `nexo-poller::builtins::google_calendar` (V1) during
//! Phase 96. OAuth client + token refresh happen inside this
//! subprocess; the daemon hands credential file paths over via
//! `host.credentials_get("google")` (Phase 96.7 reverse-RPC).

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::{json, Value};

use nexo_microapp_sdk::poller::{PollerHandler, TickRequest};
use nexo_plugin_google::GoogleAuthClient;
use nexo_poller::{PollerError, PollerHost, TickAck, TickMetrics};

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CalendarJobConfig {
    /// Calendar id. `"primary"` resolves to the agent's primary
    /// calendar.
    #[serde(default = "default_calendar_id")]
    pub calendar_id: String,
    /// Mustache-light template. Fields: `{summary}`, `{start}`,
    /// `{end}`, `{location}`, `{status}`, `{html_link}`.
    #[serde(default = "default_template")]
    pub message_template: String,
    /// Skip events whose `status` is `"cancelled"`. Default true.
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

/// One Google OAuth client per account_id. Shared across ticks for
/// the same agent so token refreshes amortise.
pub struct GoogleCalendarHandler {
    clients: DashMap<String, Arc<GoogleAuthClient>>,
}

impl GoogleCalendarHandler {
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
    }
}

impl Default for GoogleCalendarHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
struct GoogleAccountCreds {
    account_id: String,
    client_id_path: String,
    client_secret_path: String,
    token_path: String,
    #[serde(default)]
    scopes: Vec<String>,
}

#[async_trait]
impl PollerHandler for GoogleCalendarHandler {
    async fn tick(
        &self,
        req: TickRequest,
        host: Arc<dyn PollerHost>,
    ) -> Result<TickAck, PollerError> {
        let cfg: CalendarJobConfig =
            serde_json::from_value(req.config.clone()).map_err(|e| PollerError::Config {
                job: req.job_id.clone(),
                reason: e.to_string(),
            })?;

        let cred_value = host
            .credentials_get("google".into())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get: {e}")))?;
        let cred: GoogleAccountCreds =
            serde_json::from_value(cred_value).map_err(|e| PollerError::Permanent(anyhow::anyhow!(
                "credentials_get returned unexpected shape: {e}"
            )))?;

        let client = self.build_client(&cred).await?;

        let sync_token = req
            .cursor_bytes()?
            .and_then(|b| String::from_utf8(b).ok());

        let mut url = format!(
            "https://www.googleapis.com/calendar/v3/calendars/{}/events?singleEvents=true&maxResults=250",
            urlencode(&cfg.calendar_id)
        );
        if let Some(t) = sync_token.as_deref() {
            url.push_str("&syncToken=");
            url.push_str(&urlencode(t));
        } else {
            // First tick: only future events. Avoid back-fill of years.
            url.push_str("&timeMin=");
            url.push_str(&urlencode(&Utc::now().to_rfc3339()));
        }

        let resp: Value = client
            .authorized_call("GET", &url, None)
            .await
            .map_err(classify_calendar_err)?;

        let next_sync = resp
            .get("nextSyncToken")
            .and_then(Value::as_str)
            .map(str::to_string);

        let events = resp
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let items_seen = events.len() as u32;

        // Resolve outbound topic.
        let target_cred = host
            .credentials_get(cfg.deliver.channel.clone())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get outbound: {e}")))?;
        let target_account = target_cred
            .get("account_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PollerError::Permanent(anyhow::anyhow!(
                    "outbound credentials_get('{}') missing account_id",
                    cfg.deliver.channel
                ))
            })?
            .to_string();
        let topic = format!("plugin.outbound.{}.{}", cfg.deliver.channel, target_account);

        let mut items_dispatched = 0u32;
        // Don't dispatch anything on the very first tick — we just
        // want to capture nextSyncToken so subsequent ticks see
        // incrementals.
        if sync_token.is_some() {
            for ev in &events {
                if cfg.skip_cancelled
                    && ev.get("status").and_then(Value::as_str) == Some("cancelled")
                {
                    continue;
                }
                let text = render_event_template(&cfg.message_template, ev);
                let payload = json!({ "to": cfg.deliver.to, "text": text });
                let payload_bytes = serde_json::to_vec(&payload)
                    .map_err(|e| PollerError::Transient(anyhow::Error::from(e)))?;
                host.broker_publish(topic.clone(), payload_bytes)
                    .await
                    .map_err(|e| PollerError::Transient(anyhow::anyhow!("broker_publish: {e}")))?;
                items_dispatched += 1;
            }
        }

        let cursor_bytes = next_sync.map(|t| t.into_bytes());
        Ok(TickAck {
            next_cursor: cursor_bytes,
            next_interval_hint: None,
            metrics: Some(TickMetrics {
                items_seen,
                items_dispatched,
            }),
        })
    }
}

impl GoogleCalendarHandler {
    async fn build_client(
        &self,
        cred: &GoogleAccountCreds,
    ) -> Result<Arc<GoogleAuthClient>, PollerError> {
        if let Some(c) = self.clients.get(&cred.account_id) {
            return Ok(c.clone());
        }
        let cid = std::fs::read_to_string(&cred.client_id_path)
            .map(|s| s.trim().to_string())
            .map_err(|e| {
                PollerError::Transient(anyhow::Error::from(e).context("read client_id_path"))
            })?;
        let cs = std::fs::read_to_string(&cred.client_secret_path)
            .map(|s| s.trim().to_string())
            .map_err(|e| {
                PollerError::Transient(anyhow::Error::from(e).context("read client_secret_path"))
            })?;
        let auth_cfg = nexo_plugin_google::GoogleAuthConfig {
            client_id: cid,
            client_secret: cs,
            scopes: cred.scopes.clone(),
            token_file: cred.token_path.clone(),
            redirect_port: 0,
        };
        let token_path = PathBuf::from(&cred.token_path);
        let workspace = token_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        let client = GoogleAuthClient::new_with_sources(
            auth_cfg,
            &workspace,
            Some(nexo_plugin_google::SecretSources {
                client_id_path: PathBuf::from(&cred.client_id_path),
                client_secret_path: PathBuf::from(&cred.client_secret_path),
            }),
        );
        client
            .load_from_disk()
            .await
            .map_err(|e| PollerError::Permanent(e.context("calendar: load_from_disk")))?;
        self.clients.insert(cred.account_id.clone(), client.clone());
        Ok(client)
    }
}

fn render_event_template(template: &str, ev: &Value) -> String {
    let summary = ev
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("(no title)");
    let start = ev
        .get("start")
        .and_then(|s| s.get("dateTime").or_else(|| s.get("date")))
        .and_then(Value::as_str)
        .unwrap_or("");
    let end = ev
        .get("end")
        .and_then(|s| s.get("dateTime").or_else(|| s.get("date")))
        .and_then(Value::as_str)
        .unwrap_or("");
    let location = ev.get("location").and_then(Value::as_str).unwrap_or("");
    let status = ev.get("status").and_then(Value::as_str).unwrap_or("");
    let html_link = ev.get("htmlLink").and_then(Value::as_str).unwrap_or("");
    template
        .replace("{summary}", summary)
        .replace("{start}", start)
        .replace("{end}", end)
        .replace("{location}", location)
        .replace("{status}", status)
        .replace("{html_link}", html_link)
}

fn classify_calendar_err(err: anyhow::Error) -> PollerError {
    let m = err.to_string();
    if m.contains("410") || m.contains("Gone") || m.contains("invalid_grant") {
        PollerError::Permanent(err.context("calendar"))
    } else if m.contains("401") || m.contains("403") {
        PollerError::Permanent(err.context("calendar: auth"))
    } else {
        PollerError::Transient(err.context("calendar"))
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '~') {
            out.push(ch);
        } else {
            for b in ch.to_string().as_bytes() {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
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

    #[test]
    fn render_substitutes_event_fields() {
        let ev = json!({
            "summary": "Standup",
            "start": { "dateTime": "2026-05-17T10:00:00Z" },
            "end":   { "dateTime": "2026-05-17T10:30:00Z" },
            "location": "Zoom",
            "status": "confirmed",
            "htmlLink": "https://cal.google/event/abc"
        });
        let text = render_event_template(
            "{summary} @ {start} ({location}) — {html_link}",
            &ev,
        );
        assert_eq!(
            text,
            "Standup @ 2026-05-17T10:00:00Z (Zoom) — https://cal.google/event/abc"
        );
    }

    #[test]
    fn render_falls_back_to_date_field_for_all_day_events() {
        let ev = json!({
            "summary": "Holiday",
            "start": { "date": "2026-12-25" },
            "end":   { "date": "2026-12-26" },
        });
        let text = render_event_template("{summary} on {start}", &ev);
        assert_eq!(text, "Holiday on 2026-12-25");
    }

    #[test]
    fn urlencode_preserves_safe_chars_and_pcts_the_rest() {
        assert_eq!(urlencode("primary"), "primary");
        assert_eq!(urlencode("a b"), "a%20b");
        assert_eq!(urlencode("name@host.com"), "name%40host.com");
        assert_eq!(urlencode("AB-_.~12"), "AB-_.~12");
    }

    #[test]
    fn classify_410_as_permanent() {
        let err = anyhow::anyhow!("HTTP 410 Gone");
        assert!(matches!(
            classify_calendar_err(err),
            PollerError::Permanent(_)
        ));
    }

    #[test]
    fn classify_invalid_grant_as_permanent() {
        let err = anyhow::anyhow!("invalid_grant: token expired");
        assert!(matches!(
            classify_calendar_err(err),
            PollerError::Permanent(_)
        ));
    }

    #[test]
    fn classify_500_as_transient() {
        let err = anyhow::anyhow!("HTTP 500 server error");
        assert!(matches!(
            classify_calendar_err(err),
            PollerError::Transient(_)
        ));
    }

    #[test]
    fn classify_401_as_permanent_auth() {
        let err = anyhow::anyhow!("HTTP 401 unauthorized");
        assert!(matches!(
            classify_calendar_err(err),
            PollerError::Permanent(_)
        ));
    }
}
