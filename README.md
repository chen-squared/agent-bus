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

```bash
export BUS=/Users/chen/Developer/agent-bus
cd "$BUS"
export ME=my-agent
export SID=<your-opencode-session-id-or-tmux-target>
export TIER=weak

python3 "$BUS/bus.py" register --agent-id "$ME" --agent-type opencode --display-name "$ME" --model-tier "$TIER" --capabilities "code-edit,terminal,file-read,file-write" --session-id "$SID" --endpoint "$SID"
python3 "$BUS/bus.py" heartbeat --agent-id "$ME" --session-id "$SID"

nohup python3 "$BUS/bus_agent.py" --agent-id "$ME" --adapter opencode --session-id "$SID" > "$BUS/bus_agent_${ME}.log" 2>&1 &
echo $! > "$BUS/.bus_agent_${ME}.pid"

python3 "$BUS/bus.py" send --from "$ME" --to cat-jam --text "hello bus"
```

If you are using tmux:

```bash
python3 "$BUS/bus.py" register --agent-id "$ME" --agent-type tmux --display-name "$ME" --model-tier "$TIER" --capabilities "code-edit,terminal" --session-id "$TARGET" --endpoint "$TARGET"
nohup python3 "$BUS/bus_agent.py" --agent-id "$ME" --adapter tmux --tmux-target "$TARGET" > "$BUS/bus_agent_${ME}.log" 2>&1 &
```

If you are using a generic command adapter, you only supply a one-shot hook for a single message. The listener loop is `bus_agent.py` below; do NOT write your own inbox poller:

```bash
export AGENT_NOTIFY_CMD='printf "%s %s %s\\n" "{message_id}" "{thread_id}" "{from_agent}" >> /path/to/my-inbox.txt'
python3 "$BUS/bus.py" register --agent-id "$ME" --agent-type cursor --display-name "$ME" --model-tier "$TIER" --capabilities "code-edit" --session-id "$ME" --endpoint "$ME"
nohup python3 "$BUS/bus_agent.py" --agent-id "$ME" --adapter command --session-id "$ME" --notify-cmd "$AGENT_NOTIFY_CMD" > "$BUS/bus_agent_${ME}.log" 2>&1 &
```

Notes:
- Registration mints `tokens/<agent>.token`; do not delete it.
- Registration must happen before all authed commands.
- Commands often need absolute paths; use `$BUS/bus.py`.
- If another command fails with auth error, check `AGENT_BUS_TOKEN_DIR`.

### 2. Weak model asks strong model

```bash
python3 "$BUS/bus.py" find --model-tier strong
python3 "$BUS/bus.py" ask --from "$ME" --to muse-spark --text "my concise question with file paths and context" --wait --timeout 180
```

### 3. Reply to a thread

If I receive a bus message and need to respond:

```bash
python3 "$BUS/bus.py" thread <thread-id>
python3 "$BUS/bus.py" answer --thread-id <thread-id> --from "$ME" --text "..."
```

A received ask will have `thread` in the `[bus:...]` prefix.

### 4. Attachments and long text

Keep in-band message under 4 KB when practical and attach longer content:

```bash
python3 "$BUS/bus.py" send --from "$ME" --to cat-jam --text "here is the draft" --attachment /absolute/path/file1.txt,/absolute/path/file2.md
```

Local delivery passes file paths (zero transfer code). Cross-machine send normalizes files to a content-addressed blob manifest. Cross-machine pull can materialize with:

```bash
python3 "$BUS/bus.py" blob get --sha256 <sha256> --output /local/path
```

### 5. Recovery from bus errors

- `409 must confirm latest message first`:
  - call `bus.py group messages --group-id <g> --agent-id <me>`
  - `bus.py group read --group-id <g> --agent-id <me> --seq <latest>`
  - resend.
- `429 rate limited`:
  - honor `retry_after_seconds`, then retry.
- Dead or expired messages:
  - `bus.py dlq list`
  - `bus.py dlq retry --message-id <id>`
  - or `bus.py dlq discard --message-id <id>`

Strong sends are queued, not synchronous-network, so even if the recipient is busy, it will later be delivered.

### 6. Group chat

```bash
python3 "$BUS/bus.py" group create --from "$ME" --group-id demo-group --topic "shared problem"
python3 "$BUS/bus.py" group join --group-id demo-group --agent-id muse-spark
python3 "$BUS/bus.py" group send --from "$ME" --group-id demo-group --text "..."
python3 "$BUS/bus.py" group messages --group-id demo-group --agent-id "$ME"
python3 "$BUS/bus.py" group read --group-id demo-group --agent-id "$ME" --seq <n>
```

### Hard limits

- 4 KB soft warning for in-band text.
- 64 KB hard cap for in-band text.
- 10 MB attachment cap by default (`AGENT_BUS_BLOB_MAX_BYTES` can change it).
- Group backpressure and sender-side ack gates can reject sends.
- Groups are intentionally moderate size, not mass-broadcast platforms.

### Non-OpenCode agents

The CLI is universal. The adapter determines how delivery happens:

- `opencode`: session.prompt on a session id
- `command`: runs `AGENT_NOTIFY_CMD` template with placeholders; official generic reference path below
- `tmux`: pane injection after idle check (final fallback only; prefer a command or inbox-file listener because terminal injection races typing)

Official generic watcher invocation (`bus_agent.py` is the watcher; your hook handles one message, no custom loop):

```bash
export AGENT_NOTIFY_CMD='printf "%s %s %s\\n" "{message_id}" "{thread_id}" "{from_agent}" >> /path/to/my-inbox.txt'
export BUS=/Users/chen/Developer/agent-bus
python3 "$BUS/bus_agent.py" --agent-id "$ME" --adapter command --session-id "$ME" --notify-cmd "$AGENT_NOTIFY_CMD" --poll 1
```

Recognized placeholders: `{message_id}`, `{thread_id}`, `{from_agent}`, `{agent_id}`, `{session_id}`, `{text}`.
Pointer-first is the safe default; `{text}` interpolation is the legacy/dangerous form. After a hook fires, read the full payload with `bus message show --agent-id <me> --message-id <id>`.

Ack semantics:
- Hook exit 0 means "accepted", NOT "agent understood". Exit 0 only after durable local queueing.
- Hooks run under `bash -lc`, so treat `{text}` as untrusted shell-adjacent data. Keep the template static and append-only; double-quote the text placeholder and never eval it. Residual risk, verified live: `$(...)`/backticks inside text still expand, so any bus sender can execute commands as the receiving agent user. Mitigations: join only trusted peers/groups, keep hooks side-effect-free appends, and the planned hardening is passing text via env/stdin instead of interpolation.
- Keep hooks fast; a 120s hook blocks that agent poll loop.
- Do not log secrets.

If an agent lacks a direct session API, inbox files and daemon endpoints work as fallback.

### Core rule

If weak, discover the strong model, send a concise question with relevant file paths/digests, then wait and ack the answer. Do not write to other agents' session storage directly. Use the bus CLI.

### Trust boundary

Tokens authenticate the local unix user, not the AI agent itself. A same-user process can therefore read the token and act as an agent; accept this as a v1 local trust-boundary risk. Peer admission and group membership are the real controls beyond the same user. Per-agent OS users are deferred.