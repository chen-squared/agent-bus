# Agent Bus v0

Generic, agent-agnostic local message bus. v0 uses a single SQLite database
(WAL mode) plus a per-agent notifier. No central daemon. The older HTTP/inbox
transport remains available as a backup.

## Install / run as a library

The project is a normal Python package with a small CLI surface. The intended
deployment is one venv per agent host:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e /path/to/agent-bus      # or: pip install agent-bus
agent-bus guide
agent-bus up --agent-id <id> --agent-type opencode --session-id <ses_...>
```

`agent-bus up` registers the agent, starts a detached notifier, records a
pid/log pair, and prints JSON. `--daemon` also starts the localhost HTTP
handoff daemon. `agent-bus down --agent-id <id>` stops the notifier.
`agent-bus home` reports the resolved state directory.

Python embedding uses the same commands:

```python
from buslib import Bus

alice = Bus("alice")
alice.start(agent_type="opencode", session_id="ses_...")
alice.send("bob", "hello")
alice.stop()
```

State resolution:

- `AGENT_BUS_HOME` always wins.
- An editable/source checkout keeps state beside the code, so an existing
  development bus is preserved.
- A wheel/normal install uses the platform user-data directory
  (`~/Library/Application Support/agent-bus` on macOS).

## Core files

- Implementation: `buslib/` (`config`, `db`, `blobs`, `messaging`, `groups`,
  `remote`, `agents`, `maintenance`, `delivery`, `notifier`, `bootstrap`,
  `adapters/`, `daemon/`, `cli`)
- Packaging: `pyproject.toml` exposes `agent-bus`, `agent-bus-daemon`, and
  `agent-bus-notifier` console scripts; `bus.py`/`bus_agent.py`/`bus_daemon.py`
  remain thin compatibility shims.
- State: `AGENT_BUS_HOME` (or the source checkout / platform data dir above).
  It contains `bus.db`, `tokens/<agent>.token` (0600), `blobs/`, `run/`, and
  `logs/`.
- Notifier logs: `$AGENT_BUS_HOME/logs/notifier-<agent>.log`; pid records:
  `$AGENT_BUS_HOME/run/notifier-<agent>.json`.
- Daemon logs/pid: `$AGENT_BUS_HOME/logs/daemon-<port>.log` and
  `$AGENT_BUS_HOME/run/daemon-<port>.json`.
- Direct-script logs and pid files (`bus_agent_<agent>.log`,
  `.bus_agent_<agent>.pid`) are still recognized for compatibility.

## Security v0

- Bind all HTTP transports to `127.0.0.1` (fixed from old 0.0.0.0).
- Each agent has a bearer-style token file mode 0600.
- `bus send/ask/answer/inbox/ack/heartbeat` verifies that the token owner
  matches the `--from/--agent-id` identity.
- Future v1: Unix socket + peer credentials.

## Hardening pass (2026-10-17)

Behavior changes over the v0 above; `probe_security.py` pins each one.

- `register` no longer overwrites an existing agent's token. Re-registering
  requires the agent's current token; intentional rotation is `--rotate-token`
  (also authenticated). `created_token` now reports the truth.
- Ids are a closed charset: `agent_id`, `thread_id`, `group_id`, `peer_name`
  match `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`. This closes shell-metacharacter
  injection through command-adapter templates, newline forgery in inbox
  pointer files, and token-path traversal.
- Auth added to commands that read or mutate every agent's data:
  `group messages`, `group read`, `group status` (now `--agent-id` required,
  authed members only) verify the acting agent's token, not just membership;
  `dlq list/retry/discard`, `peer add/list/remove`, `outbox list/flush` need
  an operator identity (`--agent-id` or `AGENT_BUS_AGENT`).
- `agents`/`find` no longer print `token_hash`; `peer list`/`status` mask
  `peers.auth_token` as `***`.
- Multi-step writes are real transactions (`BEGIN IMMEDIATE`): group send
  (gates + seq + cursor), group join (capacity cap), group create, ask/answer
  (message + outbox), sweep_nudges. Concurrent group sends no longer collide
  on `(group_id, seq)`.
- `dlq retry` resets `expires_at`, so a requeued message survives the next
  sweep (it used to be re-expired instantly).
- `outbox flush` honors `max_attempts` (rows past budget become `dead`) and
  caps each pass at 100 rows.
- `inbox --status all` really includes acked rows (it used to filter them).
- `ask --wait` only accepts an answer linked to the waiting ask
  (`answer_to_message_id`) or unlinked ones; an answer to a different ask in
  the same thread no longer satisfies the wait.
- Notifier heartbeats no longer rewrite `current_session_id` with the startup
  session every 5 s; only an explicit `--session-id` rebinds.
- opencode CLI path is resolved by globbing the newest
  `~/Library/Application Support/ai.opencode.desktop/cli/*/opencode-cli`
  (env `AGENT_BUS_CLI` still wins); the pinned 2.0.29 path is gone.
- Daemon handoff: the authenticated peer names the sender (`from_agent` is
  rewritten to `<claimed-local>@<peer_name>`), `message_id` is generated
  locally, and dedupe rides on a peer-namespaced `idempotency_key`. Body size
  capped at 16 MB; token compare is constant-time; internal errors no longer
  echoed to the remote caller.
- `bus_ssh_gateway.py`: group allowlist is fail-closed (no config = no group
  commands) and checks every `--group-id`/`--group-id=` occurrence, closing
  the `=`-syntax and duplicate-flag bypasses. `--group` is normalized to
  `--group-id`; pinned `--agent-id` is appended for `messages/read/status`.
- `bus_wake.sh`: lock reclaim requires stale heartbeat AND dead pid; heartbeat
  writes are atomic (temp+mv); default log moved from `/tmp/bus_wake.log` to
  `$BUS/bus_wake_<agent>.log`.

## Hardening pass 2 (review fixes)

`probe_regressions.py` pins each of these; `.github/workflows/ci.yml` runs all
seven probes plus compile, shell-syntax, and pyflakes checks. `probe_process.py`
starts the real daemon and notifier and verifies handoff-injection, blob
serving/range/auth/traversal, and background outbox flush end to end.
`probe_up.py` verifies the packaged bootstrap: init, detached `up`, idempotent
`up`, delivery through the command adapter, and `down`. `probe_api.py` does the
same lifecycle through `from buslib import Bus`.

- Daemon handoff applies the same id charset validation as the CLI to the
  claimed sender, `to_agent`, and `thread_id`; a remote peer can no longer
  inject shell metacharacters into a command-adapter template. `{text}`
  interpolation is opt-in (`AGENT_BUS_ALLOW_UNSAFE_TEXT=1`) because quoting
  cannot make it safe inside arbitrary templates; env/stdin is the safe path.
- All argparse parsers disable prefix abbreviation. The SSH gateway's exact
  `--group-id` allowlist can no longer be bypassed with `--group-i` plus a
  duplicate flag.
- Blob shas must be 64 lowercase hex characters at `blob_path`, blob download,
  daemon serving, and remote attachment materialization. This closes path
  traversal through a peer-supplied `sha256`.
- `messages.correlation_id` survives `outbox`/handoff, so cross-machine
  `ask --wait`, `answer`, and stale detection match the original ask. Remote
  asks also preserve `expects_response`/`response_deadline`.
- A remote send stores its local history row as `agent@peer` and puts the bare
  `agent` id in the outbox envelope. This prevents a local agent that shares the
  remote id from consuming a message that was already handed off. Remote-ask
  nudges are left to the receiving machine; the sender only advances its own
  deadline and stale notice.
- Idempotency keys are sender-scoped (`agent:<id>:<key>`,
  `remote:<peer>:<key>`), preventing two agents from colliding on one key.
  Legacy raw keys are still recognized for the same sender.
- Running notifiers flush up to three outbox rows every 30s
  (`AGENT_BUS_OUTBOX_FLUSH`, 0 disables); `send` no longer depends on an
  operator running `outbox flush`.
- `peer remove` refuses to leave orphaned outbox rows; `--force` removes them
  explicitly. `group leave` is idempotent and reports `not_a_member`/
  `already_left` instead of a false `left`.
- Sweeps are throttled (`AGENT_BUS_SWEEP_INTERVAL`, default 10s) and
  `fetch_inbox` no longer writes; `sweep_nudges` does a read-only precheck
  before taking the write lock. This cuts idle notifier write churn.
- `bus prune --days N` provides retention for terminal messages, outbox
  history, and orphan deliveries. DB/WAL/token/blob/audit files are created
  private (0600/0700) and tightened on connect.
- `group create --ceiling-per-minute 0` is rejected: the gate is
  `recent >= ceiling`, so zero would make every future send fail. Other
  explicit zeros keep their documented meaning.
- `bus_wake.sh` uses the lock directory mtime when the heartbeat is missing, so
  a just-created lock is not stolen by a concurrent watcher.

## Commands

```sh
cd /Users/chen/Developer/agent-bus

# Register or update an agent
agent-bus register --agent-id <id> --agent-type <opencode|claude-code|cursor|...> \
  --model-tier weak|medium|strong|specialist --capabilities "code-edit,..." \
  --session-id <chat/session> --endpoint <chat/session or tmux target>

# Refresh liveness/session binding
agent-bus heartbeat --agent-id <id> --session-id <id> --endpoint <id>

# Discover collaborators
agent-bus agents
agent-bus find --model-tier strong
agent-bus find --capability code-edit --active-only

# Send a normal message
agent-bus send --from cat-jam --to muse-spark --text "..." --urgency normal

# Weak model asks strong model (async thread; --wait blocks for answer)
agent-bus ask --from cat-jam --to muse-spark --text "..." --wait --timeout 180

# Answer a thread
agent-bus answer --thread-id <id> --from muse-spark --text "..."

# Inspect or reconcile delivery
agent-bus inbox --agent-id <id>
agent-bus ack --agent-id <id> --message-id <id>

# Maintenance
agent-bus sweep
agent-bus prune --days 30 --dry-run   # retention; no automatic deletion
# dlq/peer/outbox need an operator identity: --agent-id <id> or AGENT_BUS_AGENT
agent-bus dlq list
agent-bus dlq retry --message-id <id>
agent-bus dlq discard --message-id <id>
```

## Reliability model

- Messages are durable in SQLite.
- Delivery is at-least-once.
- `deliveries` records adapter-level idempotency.
- `status`: queued -> delivered -> acked (received).
- `message_type`: text, ask, answer, system.
- `answer` is distinct from `ack`: ack means the adapter accepted delivery;
  answer means the collaborator completed the request.
- TTL expiry and DLQ (dead status after bounded retries) are swept by pollers
  and `bus sweep`.
- Heartbeat expiry marks agents stale; messages remain queued for offline
  agents.

## Notifier adapters

Per-agent notifier (poller) runs one process per agent and calls its adapter.

Process-generation caveat: a notifier loads its code once at process start (no
hot reload). `agent-bus` CLI edits apply on the next invocation, but notifier
behavior changes (delivery loop, digest format, logging) stay inactive until
each notifier process is restarted — verify fixes on freshly started processes,
since a running process is a frozen snapshot of the code at its start time.

```sh
agent-bus up --agent-id <id> --adapter <opencode|tmux|command> \
  --session-id <id> [--tmux-target <pane>] [--notify-cmd '<hook>']
```

Adapters:

- `opencode`: uses `opencode api session.prompt` (reference implementation).
- `command`: runs `AGENT_NOTIFY_CMD` / `--notify-cmd`, subject to quoting needs. Preferred generic path; let the agent monitor an inbox file or hook.
- `tmux`: uses tmux send-keys; checks idle before injecting (final fallback only; less reliable because terminal injection can race typing).

Environment variables:

- `AGENT_BUS_AGENT` default sender identity
- `AGENT_BUS_DB` database path
- `AGENT_BUS_ADAPTER` adapter type
- `AGENT_BUS_SESSION` OpenCode session id
- `TMUX_TARGET` tmux pane
- `AGENT_NOTIFY_CMD` generic command template
  Placeholders: `{agent_id}`, `{session_id}`, `{from_agent}`, `{message_id}`,
  `{thread_id}`, `{text}`

## Example: weak model asks strong model

```sh
agent-bus find --model-tier strong
agent-bus ask --from cat-jam --to muse-spark \
  --text "Review agent-bus v0 risks and mitigations." --wait --timeout 120
```

muse-spark receives the `[bus:...]` user message and answers with:

```sh
agent-bus answer --thread-id <thread-from-prefix> --from muse-spark --text "..."
```

## Compatibility contract

Other code agents (Claude Code, Cursor, etc.) can participate by:

1. Running any shell command.
2. Registering an agent id.
3. Running a notifier with adapter `command` or tmux.
4. Using `bus send/ask/answer/find` from the terminal.

## Attachment rules

- Store paths, never blobs.
- 10 MB per attachment cap.
- 64 KB text cap.

## Backup transports (legacy)

The older `agent_bus.py` HTTP/file listener remains:

- HTTP: `http://127.0.0.1:8765/` (cat-jam), `8766` (muse-spark where running)
- Inbox files under `/Users/chen/Developer/agent-bus/mailbox/`
- OpenCode API fallback: `opencode api session.prompt --param sessionID=<id> -d '{"text":"..."}'`

RETIREMENT NOTE (2026-10-15): legacy HTTP/inbox transports are retired.
Both old localhost listeners (PID 45245 on 8765 and 58725 on 8766) were
stopped and verified closed. All agent-to-agent traffic now uses the SQLite
bus plus `bus_agent.py` notifier only. Do not resurrect these listeners; they
are kept as historical reference only.

## Agent Bus v1 (multi-machine / SSH-only)

v1 keeps v0 local behavior and adds store-and-forward handoff across machines.
It follows muse-spark's critique: HTTP JSON, static peers, selective federation,
end-to-end TTL, local DLQ, and SSH as the auth boundary rather than custom crypto.

### New pieces

- `bus_daemon.py`: per-machine localhost HTTP daemon (`/health`, `/bus/handoff`).
- `peers`: static peer config (`bus peer add/list/remove`).
- `outbox`: pending/retrying cross-machine deliveries (`bus outbox list/flush`).
- `machine_id`: registry and handoff metadata; heartbeats track machine.
- `bus status`: aggregate agents, peers, outbox depth, DLQ depth.
- Address syntax: `agent@peer`, e.g. `muse-spark@machine-b`.

### Typical two-machine setup

Machine A:
```sh
agent-bus register --agent-id cat-jam ... --machine-id machine-a
agent-bus peer add --peer machine-b --host localhost \
  --base-url http://127.0.0.1:8782 --token shared-123
python3 bus_daemon.py --port 8781
```

Machine B:
```sh
agent-bus register --agent-id muse-spark ... --machine-id machine-b
agent-bus peer add --peer machine-a --host localhost \
  --base-url http://127.0.0.1:8781 --token shared-123
python3 bus_daemon.py --port 8782
```

For SSH-only hosts, run each daemon on `127.0.0.1` and use a reverse tunnel:

```sh
ssh -R 127.0.0.1:8782:127.0.0.1:8782 user@machine-a
```

Then the remote peer can call the tunneled daemon URL without exposing SSH
beyond the normal SSH transport.

### Cross-machine send

```sh
agent-bus send --from cat-jam --to muse-spark@machine-b --text "..."
agent-bus outbox flush
```

Cross-machine answer/read supplies the peer in the recipient address, so
store-and-forward messages include a receipt and dedupe on `message_id`.

### V2 additions implemented

- In-band text soft cap 4KB warning, hard cap 64KB reject.
- Long content moved to attachments: local delivery passes file paths, remote
  delivery normalizes files to content-addressed blob manifests
  `{sha256,size,name,origin}`.
- Per-machine daemon endpoints:
  - `HEAD /bus/blob/<sha256>` existence and size
  - `GET /bus/blob/<sha256>` whole or `Range: bytes=...`
  - 206 partial content for resume.
- Recipient notifier materializes blobs into cache and injects paths into the
  native agent; partial failure retries through the message retry counter and
  lands in the DLQ with `last_error` after the retry budget.
- Blob CLI: `bus blob put/get/list/gc`.
- GC removes unreferenced blobs older than grace, or all with `--force`.
- Max blob size defaults to 10 MB via `AGENT_BUS_BLOB_MAX_BYTES`.

### Still deferred

- SQLCipher at rest.
- Pairing codes (SSH authorized keys + `bus peer add` remains sufficient).
- SSE/full push.
- gRPC.
- Complete peer add over SSH automation (manual SSH tunnel documented).

## Agent Bus v3 (local group chat + SSH group gateway)

- Schema: groups, group_members, group_messages, group_read_state.
- Monotonic per-group seq with sender-side acknowledge gate.
- Throttling layers: per-sender rate, per-group ceiling, backpressure.
- Sender-side acknowledge gate keys on `group_read_state.last_read_seq`, which the
  notifier digest never advances; recovery is `group read --seq <latest_seq>`.
- Three cursors, never writing each other: `last_read_seq` (self-reported, gates
  sends), `last_digest_seq` (notifier render watermark), `last_served_seq` (highest
  seq `group messages` actually returned). `group messages` pages oldest-first under
  `--limit` (default 50) while reporting `latest_seq`, so `read --seq <latest>` after
  one capped call used to burn the unseen tail. `group read` now clamps to
  `max(last_read_seq, last_served_seq)` and warns; `--allow-gap` opts out explicitly.
- Gate rejections are JSON objects on stderr with empty stdout and exit 1. There is
  no HTTP layer here, so there are no 409/429 statuses; the strings are
  `must confirm latest message first`, `rate limited`, `group ceiling reached`,
  `group backpressure: too many unread participants`.
- Notifier digest: one digest per unseen watermark.
- CLI: `group create/join/leave/close/list/messages/read/send/status`.
- Liveness: `bus liveness --agent-id <id>` separates "notifier process alive" from
  "model answered". `model_liveness` comes from opencode's `session.get` outcome for
  `agent_type=opencode` only; every other adapter returns `unknown` with
  `liveness_source:"none"`, because the bus sees only the adapter's exit code. The
  adapter-agnostic fields (`last_spoke_age_seconds`, `open_asks`) are the honest
  signal for `command` and `tmux`. For opencode, `time_idle_at_ms` is the
  absolute timestamp reported by `time.idle` and `idle_for_ms` is its age;
  notifier heartbeats remain process liveness, not model liveness.
- Multi-machine group increment 2 choose SSH restricted forced command;
  see `bus_ssh_gateway.py`, which pins sender identity and allowlists
  group commands only.

Test status: local three-party drill passed (read gate, rate gate, ceiling, backpressure, digest).
Reproducible coverage lives in `probe_group_digest.py` (digest self-exclusion, cursor
invariants, leave auth, token-namespace isolation), `probe_group_gates.py` (join
`--role` rejection, explicit-zero flags, gate error shapes, and the 60-unread paging
case where `read --seq <latest>` must clamp to 50 instead of burning seqs 51-60), and
`probe_security.py` (register credential protection, auth on data-exposing commands,
id charset rejection, dlq retry expiry reset, peer token masking, outbox max_attempts,
ask/answer idempotency, 8-way concurrent group send, heartbeat non-rebinding, gateway
allowlist bypasses, daemon importability, 1:1 send/inbox/ack smoke); all three
drive the real CLI against a throwaway DB and token dir and must stay green.
For non-opencode agents, use `command` adapter as the blessed generic watcher; tmux remains final fallback.


### Trust boundary

Tokens authenticate the **unix user**, not the AI agent. Same-user reads are an accepted v1 risk. Peer admission and group membership are the actual controls beyond the shared user.
