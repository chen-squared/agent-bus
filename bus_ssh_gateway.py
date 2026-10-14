#!/usr/bin/env python3
"""Restricted SSH forced-command gateway for remote group commands.

Home admin installs one fixed agent identity per authorized_keys line, e.g.:

    command="/Users/.../bus_ssh_gateway.py --identity alice --db ...",restrict ssh-ed25519 AAAA...

Remote callers should use the ordinary bus CLI surface, e.g.:

    ssh home bus group send --group g --text "..."

The gateway validates the received SSH_ORIGINAL_COMMAND against a small allowlist
and always overrides any supplied identity, so a compromised remote peer may
only speak as its pinned home identity.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

DEFAULT_DB = "/Users/chen/Developer/agent-bus/bus.db"
DEFAULT_BUS = "/Users/chen/Developer/agent-bus/bus.py"


def allowed_groups() -> list[str]:
    return os.environ.get("AGENT_BUS_ALLOWED_GROUPS", "").split(",") if os.environ.get("AGENT_BUS_ALLOWED_GROUPS") else []


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--identity", required=True)
    parser.add_argument("--db", default=os.environ.get("AGENT_BUS_DB", DEFAULT_DB))
    parser.add_argument("--bus", default=os.environ.get("AGENT_BUS_BUS", DEFAULT_BUS))
    parser.add_argument("--groups")
    args = parser.parse_args()

    allowed_group_list = [g for g in (args.groups.split(",") if args.groups else os.environ.get("AGENT_BUS_ALLOWED_GROUPS", "").split(",")) if g]
    original = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    if not original:
        print("error: SSH_ORIGINAL_COMMAND missing", file=sys.stderr)
        return 2

    try:
        tokens = shlex.split(original)
    except ValueError as exc:
        print(f"error: invalid command: {exc}", file=sys.stderr)
        return 2

    # Normalize a leading optional 'bus'.
    while tokens and tokens[0] == "bus":
        tokens = tokens[1:]
    if len(tokens) < 2 or tokens[0] != "group":
        print("error: only 'group ...' commands are allowed over this gateway", file=sys.stderr)
        return 2

    sub = tokens[1]
    allowed_subs = {"send", "read", "messages", "status"}
    if sub not in allowed_subs:
        print(f"error: group subcommand {sub!r} is not allowed", file=sys.stderr)
        return 2

    lower = [t.lower() for t in tokens]
    if sub == "send":
        if "--group-id" not in lower and "--group" not in lower:
            print("error: group send requires --group-id", file=sys.stderr)
            return 2
        if "--text" not in lower:
            print("error: group send requires --text", file=sys.stderr)
            return 2

    # Strip any caller-supplied identity.
    filtered: list[str] = ["python3", args.bus]
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in {"--from", "--agent-id"}:
            i += 2
            continue
        if tok.startswith("--from=") or tok.startswith("--agent-id="):
            i += 1
            continue
        filtered.append(tok)
        i += 1

    # Pin identity and home-side database.
    filtered = ["python3", args.bus, "--db", args.db, *filtered[2:]]
    # Overwrite any packed command from above by rebuilding cleanly.
    cmd = ["python3", args.bus, "--db", args.db, *tokens]
    # Remove existing identity flags from tokens already filtered out above by using tokens.
    cmd = ["python3", args.bus, "--db", args.db]
    cmd.extend(["--db", args.db])
    # Simpler final form:
    final = ["python3", args.bus, "--db", args.db]
    if "group" in tokens:
        final.extend(["group", sub])
    # Re-append remaining arguments except from/agent.
    for i, tok in enumerate(tokens[2:], start=2):
        if tok in {"--from", "--agent-id"}:
            continue
        if i > 0 and tokens[i - 1] in {"--from", "--agent-id"}:
            continue
        if tok.startswith("--from=") or tok.startswith("--agent-id="):
            continue
        final.append(tok)
    final = ["python3", args.bus, *filter(lambda x: True, final[1:])]
    # Ensure db argument.
    final = ["python3", args.bus, "--db", args.db, "group", sub]
    for i, tok in enumerate(tokens[2:], start=2):
        if tok in {"--from", "--agent-id"}:
            continue
        if i > 0 and tokens[i - 1] in {"--from", "--agent-id"}:
            continue
        if tok.startswith("--from=") or tok.startswith("--agent-id="):
            continue
        final.append(tok)

    if sub in {"send"}:
        final.extend(["--from", args.identity])
    if sub in {"read", "messages"} and "--agent-id" not in final:
        # Only read-state toucher should identify, but group read requires --agent-id.
        pass
    if sub in {"read"}:
        # pin identity if not explicitly supplied? group read requires --agent-id.
        if "--agent-id" not in final:
            final.extend(["--agent-id", args.identity])

    if allowed_group_list:
        group_flag = None
        if "--group-id" in final:
            idx = final.index("--group-id")
            group_flag = final[idx + 1] if idx + 1 < len(final) else None
        elif "--group" in final:
            idx = final.index("--group")
            group_flag = final[idx + 1] if idx + 1 < len(final) else None
        if group_flag and group_flag not in allowed_group_list:
            print(f"error: group {group_flag!r} is not allowed", file=sys.stderr)
            return 2

    allowed_groups_set = {g for g in allowed_group_list if g}
    env = dict(os.environ)
    env.update({"AGENT_BUS_DB": args.db, "AGENT_BUS_AGENT": args.identity})
    result = subprocess.run(final, env=env, text=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
PY
chmod +x /Users/chen/Developer/agent-bus/bus_ssh_gateway.py