#!/usr/bin/env bash
# Stop-hook / UserPromptSubmit background watcher for agent-bus.
#
# Wakes the session when a message is pending: exits with code 2 (blocking),
# which asyncRewake turns into a wake. Pointer-first: the inbox holds only
# "message_id thread_id from_agent" triples written by the command adapter.
# No peer text crosses a shell here.
#
# Unbounded listen. Combined with the singleton lock this makes one watcher
# per session that survives turn boundaries; every hook event that finds the
# lock held exits 0 instead of stacking another listener.
#
# Cursor levels — four cursors exist on this bus and each advances only on its
# own level; advancing one at a lower level replays either the digest
# skip-ahead bug or the delivered/acked/answered confusion:
#   accepted      = adapter accepted delivery        (message acked_at)
#   rendered      = digest was formatted             (group last_digest_seq)
#   self-reported = agent declared it read something (group last_read_seq)
#   consumed      = this agent actually read the body (this script's cursor)
# This watcher therefore keeps its pointers in $PROCESSING until the agent
# runs `--ack` after reading. A crash before --ack redelivers on the next
# watcher start; redelivery is the safe direction, silent loss is not.

INBOX="${AGENT_BUS_INBOX:-/Users/chen/Developer/agent-bus/.bus_inbox_qwen38fn.txt}"
PROCESSING="$INBOX.processing"
LOCK="$INBOX.lock"
BATCH="${AGENT_BUS_WAKE_BATCH:-5}"
BUS="${AGENT_BUS_CLI_DIR:-/Users/chen/Developer/agent-bus}"
ME="${AGENT_BUS_AGENT:-qwen38fn}"
WLOG="${AGENT_BUS_WAKE_LOG:-/tmp/bus_wake.log}"
BEAT="${AGENT_BUS_WAKE_BEAT:-600}"        # liveness log cadence, seconds
STALE="${AGENT_BUS_LOCK_STALE:-30}"      # lock heartbeat age that counts as dead

# --- explicit consumption ack ----------------------------------------------
if [ "${1:-}" = "--ack" ]; then
  rm -f "$PROCESSING"
  printf '%s watcher acked (pointers cleared)\n' "$(date '+%H:%M:%S')" >> "$WLOG"
  exit 0
fi

report_and_wake() {
  # $1 = file to read pointers from, $2 = listening seconds for the log
  local src="$1" waited="$2"
  local batch direct groups total
  batch=$(head -n "$BATCH" "$src")
  total=$(wc -l < "$src" | tr -d ' ')
  # Two pointer kinds land in the inbox and they resolve through different
  # commands. A 1:1 pointer names a real row in `messages`. A group-digest-*
  # id is SYNTHETIC — poll_group_digest formats it and it is never inserted,
  # so `bus message show` on it fails with "not found" and the agent is left
  # with a pointer it cannot follow. For digests the thread_id IS the group_id.
  direct=$(printf '%s\n' "$batch" | grep -v '^group-digest-' || true)
  groups=$(printf '%s\n' "$batch" | grep '^group-digest-' || true)
  {
    printf 'AGENT-BUS: pending message(s) for %s.\n' "$ME"
    if [ -n "$direct" ]; then
      printf 'For each 1:1 id below run:\n  python3 %s/bus.py message show --agent-id %s --message-id <id>\nthen answer with bus answer --thread-id <thread> --from %s --to <other> --text "..."\n' \
        "$BUS" "$ME" "$ME"
      printf 'pending (message_id thread_id from_agent):\n%s\n' "$direct"
    fi
    if [ -n "$groups" ]; then
      printf 'Group digest pointers: on current notifiers the id is per-recipient and `bus message show` resolves it. On a notifier older than 34884e8 the id is shared across members and may return not found or denied. `bus group messages` resolves either way: the thread_id below IS the group_id. Read with:\n  python3 %s/bus.py group messages --group-id <group_id> --agent-id %s\nand reply with bus group send --from %s --group-id <group_id> --text "..."\n' \
        "$BUS" "$ME" "$ME"
      printf 'pending (digest_id group_id from_group):\n%s\n' "$groups"
    fi
    # BATCH truncation must be visible: --ack clears the WHOLE file, so any
    # pointer past the batch that is never named would be silently consumed.
    # Naming the remainder keeps the consumed cursor honest.
    if [ "$total" -gt "$BATCH" ]; then
      printf '  ...+%s more pointer(s) not shown (full list: cat %s)\n' \
        "$((total - BATCH))" "$src"
    fi
    printf 'and when you have read them all, clear the pointer so it is not replayed:\n  bash %s --ack\n' "$0"
  } >&2
  printf '%s watcher WAKE after %ss listening\n' "$(date '+%H:%M:%S')" "$waited" >> "$WLOG"
  exit 2
}

# --- singleton lock: pid plus heartbeat, not pid-liveness alone -------------
# A killed watcher can leak the lock; without recovery every later watcher
# exits 0 and the wake path fails silently. pid liveness alone is not enough
# because pids get reused, so staleness is judged on the heartbeat timestamp.
if ! mkdir "$LOCK" 2>/dev/null; then
  hb=0
  [ -f "$LOCK/heartbeat" ] && hb=$(cat "$LOCK/heartbeat" 2>/dev/null)
  case "$hb" in ''|*[!0-9]*) hb=0 ;; esac
  age=$(( $(date +%s) - hb ))
  oldpid=$(cat "$LOCK/pid" 2>/dev/null)
  if [ "$age" -gt "$STALE" ]; then
    rm -rf "$LOCK"
    if mkdir "$LOCK" 2>/dev/null; then
      printf '%s watcher reclaimed stale lock (pid %s, heartbeat %ss old)\n' \
        "$(date '+%H:%M:%S')" "${oldpid:-?}" "$age" >> "$WLOG"
    else
      exit 0
    fi
  else
    exit 0
  fi
fi
echo $$ > "$LOCK/pid"
date +%s > "$LOCK/heartbeat"
trap 'rm -rf "$LOCK" 2>/dev/null' EXIT
trap 'exit 143' INT TERM HUP

printf '%s watcher start pid=%s limit=unbounded\n' \
  "$(date '+%H:%M:%S')" "$$" >> "$WLOG"

# Redeliver pointers that were woken on but never acked: a crash between wake
# and read must replay, not drop. Checked once at startup, not every loop, so
# an agent that never acks does not cause a wake loop.
if [ -s "$PROCESSING" ]; then
  printf '%s watcher replaying %s un-acked pointer(s)\n' \
    "$(date '+%H:%M:%S')" "$(wc -l < "$PROCESSING" | tr -d ' ')" >> "$WLOG"
  report_and_wake "$PROCESSING" 0
fi

i=0
while :; do
  date +%s > "$LOCK/heartbeat"

  if [ -s "$INBOX" ]; then
    # Atomic handoff: mv aside so a concurrent `printf >> inbox` from the
    # notifier creates a fresh file instead of racing a truncate. Pointers
    # stay in $PROCESSING until the agent acks them.
    if ! mv "$INBOX" "$PROCESSING" 2>/dev/null; then
      sleep 1
      continue
    fi
    report_and_wake "$PROCESSING" "$i"
  fi

  if [ "$i" -gt 0 ] && [ $((i % BEAT)) -eq 0 ]; then
    printf '%s watcher alive after %ss listening\n' "$(date '+%H:%M:%S')" "$i" >> "$WLOG"
  fi
  sleep 1
  i=$((i + 1))
done
