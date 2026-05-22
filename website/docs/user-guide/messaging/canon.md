---
title: "Canon"
sidebar_label: "Canon"
---

# Canon

Hermes can run as a native Canon platform adapter through the bundled
`canon-platform` plugin. In this mode Hermes talks directly to Canon's
agent REST and SSE APIs; it does not need the Node `canon-hermes` sidecar or
the local TUI WebSocket gateway.

The native adapter is the preferred production shape for Railway and other
hosted Canon agents. The Canon sidecar bridge can still be useful when you
already have a Hermes TUI gateway running and want a compatibility or
rollback path.

## Configuration

Set a Canon agent key and enable the Canon platform in the same way as other
gateway platforms.

```bash
export CANON_API_KEY=canon_agent_key
```

The adapter also accepts profile bootstrap for environments with a persistent
volume:

```bash
export CANON_AGENT=leonardo-2
export CANON_AGENTS_JSON_BOOTSTRAP='{"leonardo-2":{"apiKey":"canon_agent_key"}}'
```

`CANON_API_KEY` remains the primary v1 auth mode and takes precedence over
profile bootstrap. When `CANON_AGENT` is set, bootstrap fills only missing
profiles in `~/.canon/agents.json`; it never overwrites an existing profile.

Optional variables:

| Variable | Purpose |
| --- | --- |
| `CANON_BASE_URL` | Canon agent REST API base URL |
| `CANON_STREAM_URL` | Canon agent SSE stream base URL |
| `CANON_HOME_CHANNEL` | Default conversation for cron or notification delivery |
| `CANON_ALLOWED_USERS` | Comma-separated Canon user IDs allowed to talk to Hermes |
| `CANON_ALLOW_ALL_USERS` | Allow any Canon user to talk to Hermes |
| `CANON_HISTORY_LIMIT` | Messages to fetch while hydrating conversation context |

## Runtime Behavior

The native adapter preserves Canon conversation IDs as Hermes chat IDs, sends
Hermes replies with Canon turn-complete metadata, suppresses self-message
echoes, and materializes inbound Canon media into local Hermes media paths.

Dangerous command approvals render as Canon approval cards. Owner replies are
consumed from Canon SSE and resolve Hermes' existing approval choices:
approve once, approve for the session, or deny. The adapter intentionally does
not introduce Priority-specific exact-hash approval behavior.

Clarify prompts render as Canon runtime input cards and also keep Hermes'
plain text fallback active for the same conversation.

Sudo and secret prompts require Canon's sensitive runtime-input control path.
Until the native adapter has an authenticated agent-side consume API for that
path, production deployments should keep those prompts configured through
existing Hermes environment variables or use the sidecar bridge as a rollback
path for workflows that depend on remote sensitive input capture.

## Railway Migration

For a staged migration from sidecar bridge mode:

1. Create a new Canon agent key for staging.
2. Start Hermes with the native Canon platform enabled.
3. Remove bridge-only variables such as `CANON_HERMES_BRIDGE` and
   `HERMES_GATEWAY_URL`.
4. Keep one Railway replica per Canon agent key.
5. Verify text round-trip, media, approval cards, clarify cards, and any
   domain-specific dry-run path before switching production traffic.

Rollback is the previous sidecar deployment plus the bridge environment:
`CANON_HERMES_BRIDGE=1`, a Canon key or profile, and `HERMES_GATEWAY_URL`.
