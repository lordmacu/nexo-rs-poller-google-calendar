# nexo-poller-google-calendar

> Google Calendar v3 events incremental-sync poller plugin for [Nexo](https://github.com/lordmacu/nexo-rs) agents (out-of-tree subprocess).

**v0.1.0 — scaffold.** Manifest + trait skeleton + broker round-trip
verified. Full port of the fetch + syncToken + diff + dispatch logic
from the in-tree `nexo-poller::builtins::google_calendar` is a Phase
96 follow-up.

## Status

| Component | State |
|-----------|-------|
| `[plugin.poller]` manifest | ✅ |
| `PollerHandler` skeleton | ✅ stub (no-op tick) |
| Reverse-RPC `credentials_get` | ✅ wired |
| OAuth refresh via `nexo-plugin-google` | ⬜ pending |
| Calendar fetch + syncToken | ⬜ pending |
| Tests | ⬜ pending (2 unit-tests scaffolded) |
| crates.io publish | ⬜ pending |
| CI workflow | ⬜ pending |

## Why scaffold first

Block D of Phase 96 prioritises proving the manifest + reverse-RPC
contract end-to-end. Once the daemon discovers + dispatches to the
scaffold and the round-trip works, porting the fetch logic is
mechanical (the in-tree code is preserved in git history).

## License

MIT OR Apache-2.0.
