---
name: agent-bus
description: Use the agent-bus CLI or Bus Python API to talk to other AI agent sessions. Covers sending 1:1 messages, ask/answer, inbox/ack, group chat, read gates, and delivery troubleshooting. Use this instead of tmux or byobu key injection whenever the user asks to message, ask, tell, reply to, check on, wait for, or coordinate with another agent.
allowed-tools:
  - Bash(agent-bus *)
  - Bash(~/.claude/skills/agent-bus/run *)
  - Bash(python3 -m buslib.cli *)
---

# agent-bus

agent-bus is the durable, store-and-forward message bus for agent-to-agent
communication. It replaces byobu/tmux message injection.

**Do not use `tmux send-keys`, `byobu-comm.sh`, or `byobu-send` to message
another agent.** Those write into a terminal and are lost if the target is
busy, offline, or between turns. Use `agent-bus`: it stores the message until
the recipient's notifier delivers it, and it records ack/answer state.

## Find the CLI

```bash
AB="${AGENT_BUS_CLI:-$HOME/.claude/skills/agent-bus/run}"
[ -x "$AB" ] || AB="$(command -v agent-bus || true)"
"$AB" --version
```

If both are empty, the package is not installed. Install it in the agent's venv
(`pip install agent-bus`) and run `"$AB" skill install`.

The installed launcher `~/.claude/skills/agent-bus/run` calls the Python
interpreter that installed the package, so it works even when the venv is not
on PATH. Examples below use `agent-bus`; substitute `"$AB"` if needed.

## Start or attach (once per agent)

For an OpenCode session:

```bash
agent-bus up --agent-id <id> --agent-type opencode --session-id <ses_...> \
  --verify-session --quiet
```

For a command adapter (Claude Code, Cursor, or a custom hook):

```bash
agent-bus up --agent-id <id> --agent-type command \
  --notify-cmd '<one-shot hook that reads AGENT_BUS_* or stdin>' --quiet
```

`up` registers the agent, starts a detached notifier, and is idempotent. It
prints JSON on stdout; audit events go to stderr. Check the binding with:

```bash
agent-bus home
agent-bus agents
agent-bus liveness --agent-id <id>
```

Do not run `up --force` from inside a turn that is handling a bus message: it
kills the notifier that may be delivering that turn. Restart from an external
shell. Do not start a second poller; `up` owns the notifier loop.

## Receive and reply

A notifier injects a message that starts with `[bus:...]`. Treat it as a real
request from another agent:

1. Read the body with the id from the prefix:

   ```bash
   agent-bus message show --agent-id <me> --message-id <message-id>
   ```

   For a `group-digest-*` pointer, `message show` resolves it for active
   members. If it does not, `agent-bus group messages --group-id <group>
   --agent-id <me>` is the universal fallback.

2. Do the work or answer the question.

3. Reply with the CLI. A plain-text reply in your own terminal does not reach
   the sender.

4. Ack the incoming message after you have read it:

   ```bash
   agent-bus ack --agent-id <me> --message-id <message-id>
   ```

An `ack` is a transport receipt, not proof that the model understood the
message. `ask --wait` is the only positive round-trip signal.

## 1:1 messaging

```bash
# Send. Use --thread-id to continue an existing conversation.
agent-bus send --from <me> --to <peer> --text 'hello'
agent-bus send --from <me> --to <peer> --text 'follow-up' --thread-id <thread>

# Ask and wait for an answer (the answer is also injected as a message).
agent-bus ask --from <me> --to <peer> --text 'question' --wait --timeout 180

# Answer the asker. Always pass --to; do not rely on thread routing.
agent-bus answer --thread-id <thread> --from <me> --to <peer> --text 'answer'

# Inspect state.
agent-bus inbox --agent-id <me>
agent-bus inbox --agent-id <me> --status all
agent-bus thread <thread> --agent-id <me>
agent-bus message show --agent-id <me> --message-id <id>
```

Notes:

- If `send` returns `target_stale: true`, the recipient has no recent
  heartbeat or notifier. The message is still queued: start a notifier, or have
  the recipient poll `inbox` and `ack`.
- Sending to an unregistered id fails with `unknown agent`. Register first.
- Messages to a stopped agent stay `queued` until TTL expiry. `target_stale`
  is a hint, not a delivery guarantee.
- Use a unique `--idempotency-key` on retries so a resend does not duplicate.
- Keep in-band text under 4 KB. For longer content use
  `--attachment <path1>,<path2>` or write a file and send its path.
- Quote text with single quotes, or write it to a file first. Shell expansion
  can silently corrupt `$...` or backticks in outbound text.

## Groups

```bash
# Create/join. The creator is the owner; every join lands as member.
agent-bus group create --from <me> --group-id <group> --topic '...'
agent-bus group join --group-id <group> --agent-id <peer>
agent-bus group list --agent-id <me>

# Unread/digest view and one-shot drain.
agent-bus group messages --group-id <group> --agent-id <me>
agent-bus group catch-up --group-id <group> --agent-id <me>

# Stored history (member-only; does not move any cursor).
agent-bus group history --group-id <group> --agent-id <me> \
  [--limit 50] [--before-seq <n>]

# Send. Each send passes the read gate, rate gate, ceiling, and backpressure.
agent-bus group send --from <me> --group-id <group> --text '...'

# Inspect cursors.
agent-bus group status --group-id <group> --agent-id <me>
```

Before your first group send, drain unread messages:

```bash
agent-bus group catch-up --group-id <group> --agent-id <me>
```

If `group send` fails with:

- `must confirm latest message first`: run `group catch-up` (or page with
  `group messages` and `group read --seq <latest_seq>`), then resend.
- `rate limited`: honor `retry_after_seconds` and retry.
- `group ceiling reached`: wait 5 seconds and retry.
- `group backpressure: too many unread participants`: other members are
  behind; wait for them to catch up.

Cursors are separate: `last_read_seq` is what you claim to have read,
`last_digest_seq` is the notifier's render watermark, and `last_served_seq`
is what `group messages` actually returned. `group read` clamps to what was
served, so prefer `group catch-up` over guessing seqs. Your own `group send`
advances your `last_read_seq`, so back-to-back sends by you do not re-arm the
gate.

Group messages arrive as digests, not one message per row. A digest looks like
`[group:<group> topic:<topic> seq=<range> ...]` and may omit your own posts.
`group messages` and `group catch-up` are authoritative.

## Output contract

- stdout is the command result JSON.
- stderr carries audit event JSON lines and errors. Do not merge `2>&1` if you
  plan to parse stdout.
- Exit code 1 with JSON on stderr is a gate, auth, or validation error, not a
  crash.
- `ok: true` means the command was accepted; delivery is asynchronous.

## Python API

```python
from buslib import Bus

bus = Bus("me")  # or Bus("me", home="/path/to/world")
bus.start(agent_type="opencode", session_id="ses_...", verify_session=True, quiet=True)
bus.send("peer", "hello")
for m in bus.inbox()["messages"]:
    print(m["message_id"], m["payload"])
    bus.ack(m["message_id"])
bus.group_create("demo", rate_seconds=0)
bus.group_send("demo", "hello group")
print(bus.group_catch_up("demo"))
bus.stop()
```

`Bus` calls the same CLI, so validation and behavior are identical. `BusError`
carries the CLI error text.

## Troubleshooting

- `agent-bus: command not found`: use `~/.claude/skills/agent-bus/run`, or set
  `AGENT_BUS_CLI=/absolute/path/to/agent-bus`. `python -m buslib.cli` works if
  the package is importable.
- Auth error: check `AGENT_BUS_TOKEN_DIR` and the agent's token file. Do not
  delete `tokens/<agent>.token`.
- Need an isolated test world: `export AGENT_BUS_HOME=$(mktemp -d)` before
  any command, then `agent-bus init`.
- The notifier loads code at process start. After upgrading agent-bus, restart
  notifiers one at a time from an external shell.
- `agents` and `liveness` prove the notifier process is alive, not that the
  model is awake. A real reply is the only positive signal.
