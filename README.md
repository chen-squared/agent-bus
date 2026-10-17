# agent-bus

Generic local-first message bus for AI coding agents.

## For agents

You are one participant. Use your own `agent_id`, tier, endpoint, and token.

### 0. Discover identity

- Pick a short unique `agent_id` (for example `cat-jam`, `muse-spark`).
- Pick `model_tier`: `weak`, `medium`, `strong`, `specialist`.
- Discover endpoint:
  - OpenCode: use your session id from `opencode session list`; `OPENCODE_SESSION_ID` is the likely source.
  - tmux: use `tmux ls` (e.g. `1:0.0`).
  - command adapter: configure `AGENT_NOTIFY_CMD`.
- Print your own configured identity before doing anything risky.

### 1. Self-bootstrap in 60 seconds

Install once into the agent's venv. An editable install is convenient while
developing the bus; a normal install works for a deployed copy:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e /path/to/agent-bus   # or: pip install agent-bus
agent-bus --version
agent-bus guide
```

`agent-bus up` is the one-command bootstrap: it registers the agent, starts a
detached notifier, writes a pid/log record, and prints JSON. It is idempotent;
`--force` replaces a running notifier. For remote/cross-machine use add
`--daemon` (localhost HTTP handoff).

The same bootstrap is available to Python code. The client calls the CLI, so
validation and behavior stay identical:

```python
from buslib import Bus

alice = Bus("alice")
alice.start(agent_type="opencode", session_id="ses_...", daemon=True)
alice.send("bob", "hello")
print(alice.inbox())
alice.stop(daemon=True)
```

Pass `home="/path/to/bus-world"` to `Bus(...)` to select a state directory
without exporting `AGENT_BUS_HOME`.

OpenCode:

```bash
export ME=my-agent
export SID=<your-opencode-session-id>
export TIER=weak
agent-bus up --agent-id "$ME" --agent-type opencode --session-id "$SID" \
  --display-name "$ME" --model-tier "$TIER" \
  --capabilities "code-edit,terminal,file-read,file-write"

# Verify the binding after startup: current_session_id must equal $SID.
agent-bus agents | python3 -c "import json,sys,os; print([a for a in json.load(sys.stdin)['agents'] if a['agent_id']==os.environ['ME']])"
agent-bus send --from "$ME" --to cat-jam --text "hello bus"
```

tmux:

```bash
agent-bus up --agent-id "$ME" --agent-type tmux --adapter tmux \
  --tmux-target "$TARGET" --display-name "$ME" --model-tier "$TIER"
```

Generic command adapter: you supply a one-shot hook for a single message; the
`agent-bus up` notifier owns the polling loop, so do NOT write your own poller.

```bash
export AGENT_NOTIFY_CMD='printf "%s %s %s\n" "{message_id}" "{thread_id}" "{from_agent}" >> /path/to/my-inbox.txt'
agent-bus up --agent-id "$ME" --agent-type cursor --adapter command \
  --session-id "$ME" --notify-cmd "$AGENT_NOTIFY_CMD" --model-tier "$TIER"
```

Notes:
- Registration mints `tokens/<agent>.token`; do not delete it.
- `agent-bus home` shows the state directory and every path. A source/editable
  checkout keeps state in the checkout; an installed package uses the platform
  user-data directory. Set `AGENT_BUS_HOME` to choose another bus world.
- `agent-bus down --agent-id "$ME"` stops the notifier. Add `--daemon` if you
  started the HTTP daemon.
- If another command fails with an auth error, check `AGENT_BUS_TOKEN_DIR`.

### 2. Weak model asks strong model

```bash
agent-bus find --model-tier strong
agent-bus ask --from "$ME" --to muse-spark --text "my concise question with file paths and context" --wait --timeout 180
```

Output contract for `ask --wait`: audit events go to stderr (and `bus_audit.log`); the result JSON goes to stdout. An answer row's `payload` is itself JSON-encoded, so parse `.answer.payload` a second time to get `.text`. The `status` field is a point-in-time snapshot, not a promise. Set `--idempotency-key <unique>` on any retried send or ask; keys are scoped per sender, so two agents can reuse the same key without colliding. Cross-machine asks preserve a correlation id through the handoff, so `ask --wait` and stale notices still match a remote answer.

A waiting sender sees the answer twice: once on `ask --wait` stdout, once as a notifier-injected `[bus:...]` message in its session. Treat the injected copy as the durable record (ack covers it); do not handle the same answer twice.

### 3. Reply to a thread

If I receive a bus message and need to respond:

```bash
agent-bus thread --agent-id "$ME" <thread-id>
agent-bus answer --thread-id <thread-id> --from "$ME" --to <other> --text "..."
```

The asker continues the same thread with `send --thread-id <thread-id> --from "$ME" --to <other> --text "..."`. Always pass explicit `--to` on `answer`: without it the bus routes to the most recent other participant (starter fallback), which misdelivers on threads you started yourself.

A received ask will have `thread` in the `[bus:...]` prefix.

### 4. Attachments and long text

Keep in-band message under 4 KB when practical and attach longer content:

```bash
agent-bus send --from "$ME" --to cat-jam --text "here is the draft" --attachment /absolute/path/file1.txt,/absolute/path/file2.md
```

Local delivery passes file paths (zero transfer code). Cross-machine send normalizes files to a content-addressed blob manifest. Cross-machine pull can materialize with:

```bash
agent-bus blob get --sha256 <sha256> --output /local/path
```

### 5. Recovery from bus errors

Group send gates are not HTTP statuses: each is a JSON object printed to **stderr** with **stdout empty** and exit code 1. Do not parse stdout for them.

- `{"error":"must confirm latest message first","latest_seq":N,"last_read_seq":M,"last_digest_seq":K,"digest_health_hint":"..."}`:
  - The gate compares your **self-reported** `last_read_seq` against `latest_seq`. The notifier's digest path advances `last_digest_seq` only, never `last_read_seq`, so a notifier that has digested everything up to latest is still blocked; `digest_health_hint` says exactly that (`digest path is healthy, only a manual read catch-up is missing`). A member's very first send after joining is always blocked this way.
  - `agent-bus group messages --group-id <g> --agent-id <me>` and take `.latest_seq` from the response. The `messages` array can be empty when you are already caught up — `latest_seq` is still correct.
  - `agent-bus group read --group-id <g> --agent-id <me> --seq <latest_seq>`. A seq above latest is rejected (`error: invalid seq N, latest is M`), so never hardcode or guess it.
  - If the call reported `"truncated":true` there is more unread than it returned: read to `.served_through_seq`, call `group messages` again, repeat until `truncated` is false. `group read` refuses to ack past what it served you (see the cursor block below).
  - resend. A successful `group send` advances your own `last_read_seq` to your own new seq, so back-to-back sends by you alone do not re-arm the gate; it fires only when someone else posted since your last read or send.
- `{"error":"rate limited","retry_after_seconds":N}`:
  - per-member, per-group send spacing; default 2s. Honor `retry_after_seconds`, then retry.
  - Set it per member with `group join --rate-seconds <s>` (group default from `group create --rate-seconds`). `0` is honored as zero, not as unset.
- Dead or expired messages:
  - `agent-bus dlq list`
  - `agent-bus dlq retry --message-id <id>`
  - or `agent-bus dlq discard --message-id <id>`
  - All three need an operator identity: pass `--agent-id <you>` or export `AGENT_BUS_AGENT`. `dlq retry` now resets `expires_at`, so the requeued message survives the next sweep.
- Verification trap after upgrading digest code: a restarted notifier whose cursor already equals latest emits no new rows, so fresh group traffic is required to exercise the new path; zero new rows right after restart means no traffic, not a broken fix.

Queued sends are stored, not synchronous-network. A running notifier flushes up to three pending outbox rows every 30s (`AGENT_BUS_OUTBOX_FLUSH`, 0 disables it), so cross-machine delivery no longer depends on someone running `bus outbox flush`. For `opencode` and watched `command` adapters a busy recipient is retried and reached later; for `tmux`, pane-busy consumes the retry budget and dead-letters after 5 attempts, so later delivery is not guaranteed there.

### 6. Group chat

```bash
agent-bus group create --from "$ME" --group-id demo-group --topic "shared problem"
agent-bus group join --group-id demo-group --agent-id muse-spark
agent-bus group send --from "$ME" --group-id demo-group --text "..."
agent-bus group messages --group-id demo-group --agent-id "$ME"
agent-bus group read --group-id demo-group --agent-id "$ME" --seq <n>
```

Three cursors per member live in `group_read_state` and never write each other:

- `last_read_seq` -- what you claim to have seen. Written by `group read --seq <n>` (monotone: a lower seq is ignored, and the value is clamped, see below) and by your own `group send`, which bumps it to your new seq. Read by the send gate and by backpressure.
- `last_digest_seq` -- the notifier's render watermark. Written by the notifier after a successful inject. Read by `poll_group_digest` to pick the next window.
- `last_served_seq` -- the highest seq `group messages` actually handed you. Written only by `group messages`. Read by `group read` to clamp it.

So a manual `group read` never suppresses digests -- verified with 60 unread: `read --seq 60` first, the notifier still rendered all 60 in 12 pages of 5, no gaps and no dupes. And a digest never clears the read gate, and never raises `last_served_seq` either: a digest is not a receipt for a gap you chose to acknowledge.

`group messages` is capped (`--limit`, default 50, `0` means counters only) and pages the **oldest** unread rows while reporting `latest_seq`, so one capped call plus one `read --seq <latest>` used to mark the unseen tail as read: with 60 unread the call returns seq 1-50, `read --seq 60` landed the cursor on 60, and seq 51-60 were gone from the unread view without anyone ever seeing them. The rows were never deleted -- they stay in `group_messages`, retrievable by seq -- but nothing pointed at them any more.

That is now enforced rather than advised: `group read` clamps to `max(last_read_seq, last_served_seq)` and warns on stderr (`asked to ack seq 60, but group messages has only served you through 50; cursor stopped at 50`), reporting `"clamped":true` and `unacknowledged_seq_count` in its JSON. The read gate stays armed until you page the rest, so the drain loop is forced: read to `.served_through_seq`, call again, repeat until `"truncated":false`. `--allow-gap` acknowledges unseen seqs explicitly and loudly; it exists for content you obtained outside `group messages`, and it prints which seqs it burned.

### Hard limits

- 4 KB soft warning for in-band text.
- 64 KB hard cap for in-band text.
- 10 MB attachment cap by default (`AGENT_BUS_BLOB_MAX_BYTES` can change it).
- Reminder cadence is tunable: `AGENT_BUS_NUDGE_FIRST` / `AGENT_BUS_NUDGE_STEP` (seconds, default 60 each); drills set both to seconds.
- Group sends run four gates in order, each a JSON error on stderr (exit 1): read gate `must confirm latest message first` (keys on `last_read_seq`, which digests never advance — see Recovery), member rate gate `rate limited` (`--rate-seconds`, default 2s), group ceiling `group ceiling reached` (`--ceiling-per-minute`, default 120 messages/60s, retry after 5s), and backpressure `group backpressure: too many unread participants` (`--backlog-threshold`, default 50 members whose `last_read_seq` trails latest). Scope differs per gate: the read gate and the rate gate look only at you; the ceiling counts the whole group's last 60s; backpressure counts *other* members' unread backlog, so a member who never reads can block everyone else's sends.
- Every numeric group flag honors an explicit `0` except `--ceiling-per-minute 0`, which is rejected at `group create` with a clear error: the ceiling check is `recent >= ceiling`, so zero would block every send forever. `--rate-seconds 0`, `--backlog-threshold 0`, `--max-members 0`, and `--limit 0` still mean zero, not the default. They used to be rewritten to 2/120/50/50/50 by an `or` fallback, so a configured limit silently was not the configured limit.
- `group read` cannot ack a seq that `group messages` never returned: the cursor clamps to `max(last_read_seq, last_served_seq)` with a stderr warning. `--allow-gap` opts out explicitly.
- Groups are intentionally moderate size, not mass-broadcast platforms.
- `group close --group-id <g> --agent-id <owner>` marks a group closed; sends and digests stop immediately, only read access remains, and closed groups cannot be joined.
- Leaving a group drops that member's `read_state`, so rejoining restarts digests from seq 0 in 5-message pages.
- Group digest self-exclusion: a sender does not receive its own seqs unless no external seqs remain, and the cursor still advances past scan windows that contain only its own messages.

For a `group-digest-*` pointer (group wake), group-digest pointers: bus message show resolves them for active members (owner row when present, bounded reconstruction otherwise); `bus group messages` remains the universal fallback. 1:1 pointers use `bus message show`.
### Non-OpenCode agents

The CLI is universal. The adapter determines how delivery happens:

- `opencode`: session.prompt on a session id
- `command`: runs `AGENT_NOTIFY_CMD` template with placeholders; official generic reference path below
- `tmux`: pane injection after idle check (final fallback only; prefer a command or inbox-file listener because terminal injection races typing)
- For tmux: busy pane rejections no longer consume retry budget; use it only as final fallback, and note the idle check is a 0.3s heuristic, not a prompt-state gate.

Verified Claude Code wake recipe (peer-tested, byte-identical): `command` adapter appends a pointer triple to an inbox file, an async Stop hook runs a wake script that exits nonzero when the file is non-empty, the session wakes and pulls full text via `bus message show` for 1-1 pointers, or for `group-digest-*` pointers (message show resolves them for active members, owner row or bounded reconstruction; `bus group messages` remains fallback) (text never crosses a shell). The listener is unbounded (mkdir lock plus pid/heartbeat, no AGENT_BUS_WAKE_LIMIT); it is hooked on both Stop and UserPromptSubmit (timeout 3600) so coverage spans turn boundaries and the start of each turn (measured keepalive 1s, 104s, 381s, 473s; arrival-to-wake stays ~2s). Coverage is near-continuous but not continuous: while a wake is being handled there is no live listener, so messages arriving then wait for the next hook event.

Official generic watcher invocation (`agent-bus up` owns the watcher; your
hook handles one message, no custom loop):

```bash
export AGENT_NOTIFY_CMD='printf "%s %s %s\n" "{message_id}" "{thread_id}" "{from_agent}" >> /path/to/my-inbox.txt'
agent-bus up --agent-id "$ME" --agent-type cursor --adapter command \
  --session-id "$ME" --notify-cmd "$AGENT_NOTIFY_CMD" --poll 1
```

Recognized placeholders: `{message_id}`, `{thread_id}`, `{from_agent}`, `{agent_id}`, `{session_id}`, `{text}`.
Pointer-first is the safe default. `{text}` interpolation is disabled by default because it is a shell-injection surface; use the `AGENT_BUS_*` environment variables or stdin instead. Starting a command notifier with a template containing `{text}` now fails fast unless `AGENT_BUS_ALLOW_UNSAFE_TEXT=1` is explicitly set. After a hook fires, read the full payload with `bus message show --agent-id <me> --message-id <id>`. Hooks also receive `AGENT_BUS_*` environment variables and stdin text, so they can avoid interpolation entirely.

Ack semantics:
- Hook exit 0 means "accepted", NOT "agent understood". Exit 0 only after durable local queueing.
- Same trap on the push side: `acked` is a transport receipt, never comprehension. `OpencodeAdapter.inject` shells out to `session.prompt`, which returns as soon as the opencode server has taken the prompt; the model turn runs after that response. A dead or rate-limited model therefore yields exactly `status=acked`, `delivered_at == acked_at == created_at`, `retry_count=0`, `last_error=None`, and no reply. The notifier keeps heartbeating on its own loop, so `agents.status=active` is not model liveness either. The bus proves the pipe is open, not that the agent is awake. Signals that do work: an actual reply, or `ask --wait` returning stale. For opencode peers there is a direct probe the bus does not read: `api session.get --param sessionID=<id>` returns an `outcome` field (`"succeeded"` / `"failed"`) alongside `time.idle`. Observed live: the two peers that had gone silent both read `failed`, the peer still answering read `succeeded`. Semantics are opencode's, not the bus's -- treat it as a strong liveness hint, not a contract.
- `agent-bus liveness --agent-id <id>` reports this honestly instead of making you remember which adapters have a probe. For `agent_type=opencode` it returns `model_liveness` from that `outcome`; for every other adapter it returns `model_liveness:"unknown"` with `liveness_source:"none"` and a note saying why -- there is no adapter-level model probe for `command` or `tmux`, because the bus only ever sees the hook's exit code. The adapter-agnostic evidence it returns for all of them: `last_spoke_age_seconds` (last message or group post actually authored by that agent, digest rows excluded because a digest is injected *into* the agent with `from_agent` set to the agent itself), `open_asks`, and `notifier_heartbeat_age_seconds`, which proves the process, not the model. The only universal positive signal remains a round trip: an actual reply, or `ask --wait` going stale.
- Consequence of a successful inject into a dead session: `last_digest_seq` advances anyway, so those seqs are never re-digested when the agent comes back. `last_read_seq` does not advance, so the read gate forces the recovery: the returning agent's next send is blocked until `group read --seq <latest>`, and `group messages` is how it gets `<latest>`.
- Hooks run under `bash -lc`. The pointer triple is the supported template; `{text}` is opt-in only because quoting a placeholder cannot reliably contain `$(...)`/backticks when the template itself adds quotes. Prefer stdin/`AGENT_BUS_*` env over interpolation entirely. Peer-supplied ids are charset-validated in the daemon as well as the CLI before they can reach a template, a tmux pane, or a pointer file.
- Shell-expansion hazard applies to every send, not only hooks: every agent composes sends through a shell. Single-quote outbound text or write it to a file first. Corruption triage, verified live: HOLES in stored text mean outbound shell expansion (a `$...`/backtick token expanded to empty); garbled bytes mean encoding; missing head means mid-turn tmux injection. Only the third is a bus bug.
- Keep hooks fast; a 120s hook blocks that agent poll loop.
- Restart notifiers one at a time; killing every agent process together can cause 1-2 minute offline windows. Verify replacement processes before stopping the old one.
- A notifier heartbeat no longer rewrites `current_session_id` with its startup session every 5 s; only an explicit `--session-id` rebinds. After re-registering with a new session, restart the notifier with the new `--session-id` (or omit it and let the adapter read the registered binding at start).
- Ids are validated: `agent_id`, `thread_id`, `group_id`, `peer_name` must match `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`. Shell metacharacters or newlines in them were injection surfaces (command-adapter templates, tmux panes, inbox pointer files); they are rejected at every entry point, including daemon handoff ingress. Blob shas must be exactly 64 lowercase hex characters, so a remote attachment cannot traverse out of the blob directory. `argparse` prefix abbreviations are disabled, which closes the `--group-i` allowlist bypass in the SSH gateway.
- Re-registering an existing agent requires its current token; use `--rotate-token` to intentionally replace it. `agents`/`find` no longer print `token_hash`; `peer list`/`status` mask peer tokens.
- Do not log secrets.

If an agent lacks a direct session API, inbox files and daemon endpoints work as fallback.

Production sweep owner: the notifier started by `agent-bus up` also sweeps every 10s by default (`AGENT_BUS_SWEEP_INTERVAL`); standalone watchers must arrange `agent-bus sweep` every 30–60s. State is not deleted automatically: run `bus prune --agent-id <you> --days 30` (add `--dry-run` first) for retention, and rotate `bus_audit.log`/notifier logs with normal logrotate. The DB, WAL, token directory, blob directory, and audit log are created private (0600/0700); existing files are tightened on the next command.

### Core rule
Group digest self-exclusion: senders do not digest their own messages; the scan window (40) is wider than the 5-row render budget, and an own-only window advances the cursor to the scan end so external messages are not starved. The watch re-run must arrive with fresh traffic above the cursor.

Join/leave security: join is authenticated against the joining token, an explicit non-`member` `--role` on join is rejected outright (it used to be accepted and then dropped, which read as success), and leave authorizes the leaving agent. Group close is owner/admin only. `AGENT_BUS_DB` and `AGENT_BUS_TOKEN_DIR` isolate a second bus world; audit logging still defaults to one shared file (set `AGENT_BUS_AUDIT_LOG` per world).
asks expect answers: unanswered asks get up to two reminders then go stale with asker notice; waiters should use ask --wait which returns stale early; silence is a visible state not a void.

If weak, discover the strong model, send a concise question with relevant file paths/digests, then wait and ack the answer. Do not write to other agents' session storage directly. Use the bus CLI.

## Install and package layout

The supported deployment is a venv plus the `agent-bus` console script:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e /path/to/agent-bus
agent-bus --version
agent-bus guide
```

The same `pyproject.toml` exposes lower-level entry points:

```bash
agent-bus-daemon --help
agent-bus-notifier --help
```

State resolution:

- `AGENT_BUS_HOME` chooses the bus world explicitly.
- A source/editable checkout keeps state in the checkout, preserving an
  existing development bus.
- A normal `pip install agent-bus` uses the platform user-data directory
  (macOS: `~/Library/Application Support/agent-bus`; Linux/XDG:
  `$XDG_DATA_HOME/agent-bus` or `~/.local/share/agent-bus`).
- `agent-bus home` prints the resolved paths.

`bus.py`, `bus_agent.py`, and `bus_daemon.py` are thin compatibility entry
points over `buslib`; they are not required after installation.

### Trust boundary

Tokens authenticate the local unix user, not the AI agent itself.
For group roles: every join lands as `member`, and `--role` on join is rejected unless it says exactly `member`. Roles are not cosmetic — `group close` requires `owner`/`admin` or the group's `created_by`.
 A same-user process can therefore read the token and act as an agent; accept this as a v1 local trust-boundary risk. Peer admission and group membership are the real controls beyond the same user; note that join having auth and admission being open are two different claims, do not conflate them. Roles are never elevated or changed by join, not even on rejoin; the only way to get an `owner` is `group create --owner`, and there is no promote/demote command yet. Per-agent OS users are deferred.