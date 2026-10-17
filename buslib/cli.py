"""Argument parser and command dispatch for bus.py."""

from __future__ import annotations

import argparse
import os
from typing import Any

from . import __version__
from .agents import (
    cmd_agents, cmd_capabilities, cmd_find, cmd_heartbeat, cmd_liveness,
    cmd_register,
)
from .blobs import cmd_blob_gc, cmd_blob_get, cmd_blob_list, cmd_blob_put
from .bootstrap import cmd_down, cmd_guide, cmd_home, cmd_up
from .db import cmd_init
from .groups import (
    cmd_group_close, cmd_group_create, cmd_group_join, cmd_group_leave,
    cmd_group_list, cmd_group_messages, cmd_group_read, cmd_group_send,
    cmd_group_status,
)
from .maintenance import (
    cmd_dlq_discard, cmd_dlq_list, cmd_dlq_retry, cmd_prune, cmd_sweep,
)
from .messaging import (
    cmd_ack, cmd_answer, cmd_ask, cmd_inbox, cmd_message_show, cmd_send,
    cmd_thread,
)
from .remote import (
    cmd_outbox_flush, cmd_outbox_list, cmd_peer_add, cmd_peer_list,
    cmd_peer_remove, cmd_status,
)
from .validation import nonneg_float, nonneg_int, validate_id


class BusArgumentParser(argparse.ArgumentParser):
    """Prefix abbreviations are a security issue here: the SSH gateway
    validates exact --group-id spellings, but argparse would accept --group-i
    and let a later duplicate win. Disable them everywhere."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)


def main() -> int:
    os.umask(0o077)
    parser = BusArgumentParser(description="agent-bus generic message bus (v0)")
    parser.add_argument("--version", action="version", version=f"agent-bus {__version__}")
    parser.add_argument("--db", default=None, help="override database path")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=BusArgumentParser)

    p = sub.add_parser("init", help="initialize database")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("up", help="register an agent and start its notifier in one command")
    p.add_argument("--agent-id", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--agent-type", help="opencode|claude-code|cursor|command|tmux (default: adapter)")
    p.add_argument("--adapter", choices=["opencode", "tmux", "command"], default=os.environ.get("AGENT_BUS_ADAPTER", "opencode"))
    p.add_argument("--session-id", default=os.environ.get("AGENT_BUS_SESSION"))
    p.add_argument("--tmux-target", default=os.environ.get("TMUX_TARGET"))
    p.add_argument("--notify-cmd", default=os.environ.get("AGENT_NOTIFY_CMD"))
    p.add_argument("--allow-unsafe-text", action="store_true", default=os.environ.get("AGENT_BUS_ALLOW_UNSAFE_TEXT") == "1")
    p.add_argument("--cli", default=None, help="opencode CLI path (default: autodetect)")
    p.add_argument("--cwd", default=os.environ.get("AGENT_BUS_CWD"), help="adapter working directory (default: current directory)")
    p.add_argument("--display-name")
    p.add_argument("--model-tier")
    p.add_argument("--capabilities")
    p.add_argument("--metadata")
    p.add_argument("--endpoint")
    p.add_argument("--machine-id")
    p.add_argument("--poll", type=float, default=None)
    p.add_argument("--heartbeat", type=float, default=None)
    p.add_argument("--sweep", type=float, default=None)
    p.add_argument("--outbox-flush", type=float, default=None)
    p.add_argument("--daemon", action="store_true", help="also start the localhost HTTP daemon")
    p.add_argument("--daemon-port", type=int, default=int(os.environ.get("AGENT_BUS_DAEMON_PORT", "8780")))
    p.add_argument("--bind", default=os.environ.get("AGENT_BUS_DAEMON_BIND", "127.0.0.1"))
    p.add_argument("--foreground", action="store_true", help="run the notifier in this process")
    p.add_argument("--force", action="store_true", help="replace an already running notifier")
    p.set_defaults(func=cmd_up)

    p = sub.add_parser("down", help="stop an agent's notifier (and optionally its daemon)")
    p.add_argument("--agent-id", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--daemon", action="store_true", help="also stop the localhost HTTP daemon")
    p.add_argument("--daemon-port", type=int, default=int(os.environ.get("AGENT_BUS_DAEMON_PORT", "8780")))
    p.add_argument("--force", action="store_true", help="stop even if the pid's command line cannot be verified")
    p.set_defaults(func=cmd_down)

    p = sub.add_parser("home", help="show the resolved state directory and paths")
    p.set_defaults(func=cmd_home)

    p = sub.add_parser("guide", help="print an agent-oriented quickstart")
    p.set_defaults(func=cmd_guide)

    p = sub.add_parser("quickstart", help="alias for guide")
    p.set_defaults(func=cmd_guide)

    p = sub.add_parser("register", help="register or update agent")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--agent-type", required=True, help="opencode|claude-code|cursor|...")
    p.add_argument("--display-name")
    p.add_argument("--model-tier")
    p.add_argument("--capabilities", help="comma list or JSON list")
    p.add_argument("--session-id")
    p.add_argument("--endpoint")
    p.add_argument("--machine-id")
    p.add_argument("--metadata")
    p.add_argument("--rotate-token", action="store_true", help="intentionally replace an existing agent's token (requires the current token)")
    p.set_defaults(func=cmd_register)

    p = sub.add_parser("heartbeat", help="refresh agent liveness and session")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--session-id")
    p.add_argument("--endpoint")
    p.set_defaults(func=cmd_heartbeat)

    p = sub.add_parser("send", help="send a message or ask-ish text")
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent", required=True)
    p.add_argument("--text", required=True)
    p.add_argument("--thread-id")
    p.add_argument("--message-type", default="text")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int)
    p.add_argument("--idempotency-key")
    p.add_argument("--attachment", help="comma-separated file paths")
    p.add_argument("--metadata")
    p.set_defaults(func=cmd_send)

    p = sub.add_parser("inbox", help="list undelivered messages for agent")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--status", default="undelivered", choices=["undelivered", "queued", "delivered", "acked", "all"])
    p.add_argument("--limit", type=nonneg_int, default=50)
    p.set_defaults(func=cmd_inbox)

    p = sub.add_parser("ack", help="acknowledge delivered message")
    p.add_argument("--agent-id", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_ack)

    p = sub.add_parser("thread", help="show thread")
    p.add_argument("thread_id")
    p.add_argument("--agent-id", required=True)
    p.set_defaults(func=cmd_thread)

    p = sub.add_parser("message", help="read a single message with ownership check")
    m = p.add_subparsers(dest="message_command", required=True, parser_class=BusArgumentParser)
    ms = m.add_parser("show", help="show one message")
    ms.add_argument("--agent-id", required=True)
    ms.add_argument("--message-id", required=True)
    ms.set_defaults(func=cmd_message_show)

    p = sub.add_parser("agents", help="list agents")
    p.set_defaults(func=cmd_agents)

    p = sub.add_parser("liveness", help="what the bus can and cannot prove about one agent being awake")
    p.add_argument("--agent-id", required=True)
    p.set_defaults(func=cmd_liveness)

    p = sub.add_parser("sweep", help="expire TTL messages and dead-letter retries")
    p.set_defaults(func=cmd_sweep)

    p = sub.add_parser("prune", help="delete old terminal messages/outbox rows (retention)")
    p.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    p.add_argument("--days", type=nonneg_int, default=30)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_prune)

    p = sub.add_parser("ask", help="send an ask and optionally wait for answer")
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent", required=True)
    p.add_argument("--text", required=True)
    p.add_argument("--thread-id")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int, default=300)
    p.add_argument("--timeout", type=nonneg_int, default=120)
    p.add_argument("--idempotency-key")
    p.add_argument("--attachment", help="comma-separated file paths")
    p.add_argument("--metadata")
    p.add_argument("--wait", action="store_true")
    p.add_argument("--no-expect-reply", action="store_true", help="do not monitor/remind for a response")
    p.set_defaults(func=cmd_ask)

    p = sub.add_parser("answer", help="answer an ask thread")
    p.add_argument("--thread-id", required=True)
    p.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    p.add_argument("--to", dest="to_agent")
    p.add_argument("--text", required=True)
    p.add_argument("--answer-to")
    p.add_argument("--urgency", default="normal")
    p.add_argument("--ttl", type=int, default=300)
    p.add_argument("--idempotency-key")
    p.add_argument("--metadata")
    p.set_defaults(func=cmd_answer)

    p = sub.add_parser("capabilities", help="show capability/tier vocabulary")
    p.set_defaults(func=cmd_capabilities)

    p = sub.add_parser("peer", help="manage remote peers")
    peer_sub = p.add_subparsers(dest="peer_command", required=True, parser_class=BusArgumentParser)
    pa = peer_sub.add_parser("add", help="add or update peer")
    pa.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    pa.add_argument("--peer", required=True)
    pa.add_argument("--host")
    pa.add_argument("--base-url", required=True)
    pa.add_argument("--token", required=True)
    pa.set_defaults(func=cmd_peer_add)
    pl = peer_sub.add_parser("list", help="list peers")
    pl.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    pl.set_defaults(func=cmd_peer_list)
    pr = peer_sub.add_parser("remove", help="remove peer")
    pr.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    pr.add_argument("--peer", required=True)
    pr.add_argument("--force", action="store_true", help="also delete this peer's outbox rows")
    pr.set_defaults(func=cmd_peer_remove)

    p = sub.add_parser("status", help="show bus health depths")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("group", help="group chat commands (V3)")
    g = p.add_subparsers(dest="group_command", required=True, parser_class=BusArgumentParser)
    gc = g.add_parser("create", help="create group")
    gc.add_argument("--from", dest="from_agent", default=os.environ.get("AGENT_BUS_AGENT"))
    gc.add_argument("--group-id", required=True)
    gc.add_argument("--topic")
    gc.add_argument("--owner")
    gc.add_argument("--max-members", type=nonneg_int, default=50)
    gc.add_argument("--rate-seconds", type=nonneg_float, default=2)
    gc.add_argument("--ceiling-per-minute", type=nonneg_int, default=120)
    gc.add_argument("--backlog-threshold", type=nonneg_int, default=50)
    gc.add_argument("--mode", default="catch-up-gated")
    gc.set_defaults(func=cmd_group_create)
    gj = g.add_parser("join", help="join group")
    gj.add_argument("--group-id", required=True)
    gj.add_argument("--agent-id", required=True)
    gj.add_argument("--role", default=None, help="not settable on join: every join lands as member; roles come only from group create --owner")
    gj.add_argument("--rate-seconds", type=nonneg_float)
    gj.set_defaults(func=cmd_group_join)
    gl = g.add_parser("leave", help="leave group")
    gl.add_argument("--group-id", required=True)
    gl.add_argument("--agent-id", required=True)
    gl.set_defaults(func=cmd_group_leave)
    gc = g.add_parser("close", help="close group (owner only)")
    gc.add_argument("--group-id", required=True)
    gc.add_argument("--agent-id", required=True)
    gc.set_defaults(func=cmd_group_close)
    glist = g.add_parser("list", help="list groups")
    glist.add_argument("--agent-id")
    glist.set_defaults(func=cmd_group_list)
    gm = g.add_parser("messages", help="list or read new group messages")
    gm.add_argument("--group-id", required=True)
    gm.add_argument("--agent-id")
    gm.add_argument("--limit", type=nonneg_int, default=50, help="oldest unread rows to return; 0 means counters only")
    gm.set_defaults(func=cmd_group_messages)
    gr = g.add_parser("read", help="ack seqs as read (clamped to what group messages actually served)")
    gr.add_argument("--group-id", required=True)
    gr.add_argument("--agent-id", required=True)
    gr.add_argument("--seq", type=nonneg_int, help="default latest; clamped to last_served_seq unless --allow-gap")
    gr.add_argument("--allow-gap", action="store_true", help="ack seqs that were never returned by group messages")
    gr.set_defaults(func=cmd_group_read)
    gs = g.add_parser("send", help="send a group message")
    gs.add_argument("--from", dest="from_agent", required=True)
    gs.add_argument("--group-id", required=True)
    gs.add_argument("--text", required=True)
    gs.add_argument("--message-type", default="text")
    gs.add_argument("--urgency", default="normal")
    gs.add_argument("--ttl", type=int)
    gs.set_defaults(func=cmd_group_send)
    gst = g.add_parser("status", help="show group stats (authed members only)")
    gst.add_argument("--group-id", required=True)
    gst.add_argument("--agent-id", required=True)
    gst.set_defaults(func=cmd_group_status)

    p = sub.add_parser("blob", help="content-addressed blob store (for long attachments)")
    blob_sub = p.add_subparsers(dest="blob_command", required=True, parser_class=BusArgumentParser)
    bp = blob_sub.add_parser("put", help="store a file in blob store")
    bp.add_argument("--path", required=True)
    bp.set_defaults(func=cmd_blob_put)
    bg = blob_sub.add_parser("get", help="copy a blob out of the store")
    bg.add_argument("--sha256", required=True)
    bg.add_argument("--output")
    bg.set_defaults(func=cmd_blob_get)
    bl = blob_sub.add_parser("list", help="list blobs")
    bl.set_defaults(func=cmd_blob_list)
    bgc = blob_sub.add_parser("gc", help="garbage collect unreferenced blobs")
    bgc.add_argument("--grace-seconds", type=nonneg_int, default=86400)
    bgc.add_argument("--force", action="store_true")
    bgc.set_defaults(func=cmd_blob_gc)

    p = sub.add_parser("outbox", help="cross-machine delivery queue")
    out_sub = p.add_subparsers(dest="outbox_command", required=True, parser_class=BusArgumentParser)
    ol = out_sub.add_parser("list", help="list outbox entries")
    ol.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    ol.add_argument("--limit", type=nonneg_int, default=50)
    ol.set_defaults(func=cmd_outbox_list)
    of = out_sub.add_parser("flush", help="retry pending handoffs")
    of.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    of.add_argument("--timeout", type=nonneg_int, default=10)
    of.set_defaults(func=cmd_outbox_flush)

    p = sub.add_parser("find", help="find agents by capability or model tier")
    p.add_argument("--capability")
    p.add_argument("--model-tier")
    p.add_argument("--active-only", action="store_true")
    p.set_defaults(func=cmd_find)

    p = sub.add_parser("dlq", help="dead-letter queue controls")
    dlq_sub = p.add_subparsers(dest="dlq_command", required=True, parser_class=BusArgumentParser)
    lp = dlq_sub.add_parser("list", help="list dead/expired messages")
    lp.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    lp.add_argument("--status", choices=["dead", "expired"])
    lp.add_argument("--limit", type=nonneg_int, default=50)
    lp.set_defaults(func=cmd_dlq_list)
    rp = dlq_sub.add_parser("retry", help="requeue dead/expired message")
    rp.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    rp.add_argument("--message-id", required=True)
    rp.set_defaults(func=cmd_dlq_retry)
    dp = dlq_sub.add_parser("discard", help="discard dead/expired message")
    dp.add_argument("--agent-id", help="operator identity (or set AGENT_BUS_AGENT)")
    dp.add_argument("--message-id", required=True)
    dp.set_defaults(func=cmd_dlq_discard)

    args = parser.parse_args()
    if args.db:
        os.environ["AGENT_BUS_DB"] = args.db
    if getattr(args, "from_agent", None) is None and args.command in {"send", "ask", "answer"}:
        raise SystemExit("error: --from/AGENT_BUS_AGENT is required")
    # Every identity that can reach a token path or a shell template gets the
    # same closed charset, not only the register command.
    for attr in ("agent_id", "from_agent", "owner"):
        value = getattr(args, attr, None)
        if value:
            validate_id("agent_id", value)
    # (Old code did a throwaway connect() here to "ensure init"; every command
    # handler connects anyway, so this just ran the whole schema migration
    # twice per invocation.)
    return args.func(args)


__all__ = [
    "BusArgumentParser",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
