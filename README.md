# nexo-poller-google-calendar

> Google Calendar v3 events incremental-sync poller plugin for [Nexo](https://github.com/lordmacu/nexo-rs) agents (out-of-tree subprocess).

**v0.1.0 — full port shipped.** Fetch + syncToken cursor + diff +
dispatch + OAuth refresh all working. Ported from
`nexo-poller::builtins::google_calendar` (V1) during Phase 96.

## What it does

- Fetches Calendar v3 events on every tick. First tick captures
  `nextSyncToken` with `timeMin = now` (no back-fill); subsequent
  ticks pass `syncToken = <cursor>` and dispatch only the diff.
- Token expiry (HTTP 410 / `invalid_grant`) returns `Permanent`
  error so the runner auto-pauses; operator runs `agent pollers
  reset <job-id>` to re-baseline.
- OAuth refresh happens inside this subprocess via
  `nexo-plugin-google`. Daemon hands credential file paths
  (`client_id_path`, `client_secret_path`, `token_path`, `scopes`)
  through reverse-RPC `host.credentials_get("google")`.
- Outbound channel (whatsapp / telegram / …) is resolved via
  reverse-RPC then published to
  `plugin.outbound.<channel>.<account_id>`.

## Operator YAML

```yaml
# pollers.yaml fragment
jobs:
  - id: ceo_calendar
    kind: google_calendar
    agent: cody
    schedule: { every: 5m }
    config:
      calendar_id: primary
      skip_cancelled: true
      message_template: "📅 {summary} — {start}\n{html_link}"
      deliver:
        channel: whatsapp
        to: "+573001234567"
```

## Status

| Component | State |
|-----------|-------|
| `[plugin.poller]` manifest | ✅ |
| OAuth refresh via `nexo-plugin-google` | ✅ |
| Calendar fetch + syncToken | ✅ |
| Tests | ✅ 9/9 (config + template + urlencode + error classify) |
| crates.io publish | ⬜ pending Phase 96 release wave |
| CI workflow | ⬜ pending |

## License

MIT OR Apache-2.0.
