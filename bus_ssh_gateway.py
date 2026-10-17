#!/usr/bin/env python3
"""Restricted SSH forced-command gateway for remote group commands.

Home admin installs one fixed agent identity per authorized_keys line, e.g.:

    command="/Users/.../bus_ssh_gateway.py --identity alice --db ... --groups g1,g2",restrict ssh-ed25519 AAAA...

Remote callers use the ordinary bus CLI surface, e.g.:

    ssh home bus group send --group-id g1 --text "..."

The gateway validates the received SSH_ORIGINAL_COMMAND against a small
allowlist and always overrides any supplied identity, so a compromised remote
peer may only speak as its pinned home identity.

Security posture (fixed in this revision):
- The group allowlist is FAIL-CLOSED: with no --groups/AGENT_BUS_ALLOWED_GROUPS
  configured, every group command is rejected. It used to be opt-in and
  off-by-default, which contradicted the documented promise.
- The allowlist checks EVERY --group-id occurrence, including `--group-id=x`
  and duplicate flags. The old check looked only for a standalone
  `--group-id` token at its FIRST occurrence, so `--group-id=evil` skipped the
  check entirely and `--group-id allowed --group-id evil` validated the first
  and executed the second (argparse takes the last).
- `group messages`/`read`/`status` get the pinned --agent-id appended; the
  old code stripped the caller's --agent-id but never re-added it for
  `messages`, so that allowlisted subcommand could never succeed.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys

DEFAULT_DB = "/Users/chen/Developer/agent-bus/bus.db"
DEFAULT_BUS = "/Users/chen/Developer/agent-bus/bus.py"

ALLOWED_SUBS = {"send", "read", "messages", "status"}


def group_flag_values(tokens: list[str]) -> list[str]:
    """Every group id a command names, in any accepted spelling."""
    vals: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--group-id":
            if i + 1 < len(tokens):
                vals.append(tokens[i + 1])
            i += 2
        elif tok.startswith("--group-id="):
            vals.append(tok.split("=", 1)[1])
            i += 1
        else:
            i += 1
    return vals


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
    if sub not in ALLOWED_SUBS:
        print(f"error: group subcommand {sub!r} is not allowed", file=sys.stderr)
        return 2

    # bus.py implements --group-id only; normalize the --group spelling the
    # old docstring advertised so the command actually runs.
    tokens = [
        "--group-id" if t == "--group"
        else ("--group-id=" + t.split("=", 1)[1] if t.startswith("--group=") else t)
        for t in tokens
    ]

    group_vals = group_flag_values(tokens)
    if not group_vals:
        print("error: group commands require --group-id", file=sys.stderr)
        return 2
    if not allowed_group_list:
        print("error: no group allowlist configured (--groups / AGENT_BUS_ALLOWED_GROUPS); group commands are denied", file=sys.stderr)
        return 2
    for g in group_vals:
        if g not in allowed_group_list:
            print(f"error: group {g!r} is not allowed", file=sys.stderr)
            return 2

    if sub == "send" and not any(t == "--text" or t.startswith("--text=") for t in tokens):
        print("error: group send requires --text", file=sys.stderr)
        return 2

    # Build the final argv: pinned db + pinned identity, caller identity flags
    # stripped. List-form subprocess: message text is one argv token and can
    # never inject a shell.
    final = ["python3", args.bus, "--db", args.db, "group", sub]
    i = 2
    while i < len(tokens):
        tok = tokens[i]
        if tok in {"--from", "--agent-id"}:
            i += 2
            continue
        if tok.startswith("--from=") or tok.startswith("--agent-id="):
            i += 1
            continue
        final.append(tok)
        i += 1

    if sub == "send":
        final.extend(["--from", args.identity])
    else:
        final.extend(["--agent-id", args.identity])

    env = dict(os.environ)
    env.update({"AGENT_BUS_DB": args.db, "AGENT_BUS_AGENT": args.identity})
    try:
        result = subprocess.run(final, env=env, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        print("error: bus command timed out", file=sys.stderr)
        return 1
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
