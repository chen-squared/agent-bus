#!/usr/bin/env python3
"""Verifies the group-send gates and the numeric-flag defaults, end to end through
the real CLI.

Two defect classes are covered:
  - `--role` on join was accepted, stored as "member", and reported ok: a silently
    ignored privilege flag. It must now be rejected outright.
  - `or`-defaults swallowed explicit zeros: `--backlog-threshold 0`,
    `--ceiling-per-minute 0` and `--max-members 0` all became their defaults, so a
    configured limit silently was not the configured limit.

Same rule as probe_group_digest.py: everything with a CLI path runs the real
argv, never a hand-built Namespace.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile

root = tempfile.mkdtemp(prefix="bus-gates-")
DB = os.path.join(root, "world", "bus.db")
TOK = os.path.join(root, "world", "tokens")
os.environ["AGENT_BUS_DB"] = DB
os.environ["AGENT_BUS_TOKEN_DIR"] = TOK
os.environ["AGENT_BUS_AUDIT_LOG"] = os.path.join(root, "world", "audit.log")

HERE = os.path.dirname(os.path.abspath(__file__))
FAIL = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def cli(*argv, tok=None):
    env = dict(os.environ)
    if tok:
        env["AGENT_BUS_TOKEN_FILE"] = tok
    p = subprocess.run([sys.executable, os.path.join(HERE, "bus.py"), *argv],
                       capture_output=True, text=True, env=env)
    return p.returncode, p.stdout.strip(), p.stderr.strip()


def gate_err(stderr):
    """Gate rejections are JSON on stderr with empty stdout."""
    try:
        return json.loads(stderr)
    except (json.JSONDecodeError, ValueError):
        return None


def send(who, gid, text):
    """Send the way a real agent must: confirm latest first, then post.

    Kept explicit rather than hidden in a helper because the read-gate is one of
    the things under test -- see K for the raw, ungated call shape.
    """
    rc, out, err = cli("group", "send", "--from", who, "--group-id", gid, "--text", text)
    if rc != 0 and "must confirm latest message first" in err:
        latest = (gate_err(err) or {}).get("latest_seq")
        rc2, out2, err2 = cli("group", "read", "--group-id", gid, "--agent-id", who, "--seq", str(latest))
        if rc2 != 0:
            return rc, out, f"read({latest}) failed: {err2}"
        rc, out, err = cli("group", "send", "--from", who, "--group-id", gid, "--text", text)
    return rc, out, err


def row(sql, *a):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute(sql, a).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


rc, out, err = cli("init")
check("init", rc == 0, err or out)
for a in ("alice", "bob", "carol"):
    rc, out, err = cli("register", "--agent-id", a, "--agent-type", "command")
    check(f"register {a}", rc == 0, err or out)

print("I join --role 必须显式报错，不再静默降级为 member")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "roles", "--topic", "t",
                   "--rate-seconds", "0")
check("create roles", rc == 0, err or out)
rc, out, err = cli("group", "join", "--group-id", "roles", "--agent-id", "bob", "--role", "owner")
check("--role owner 被拒", rc != 0 and "not settable on join" in err, err or out)
check("报错走 stderr 且 stdout 为空", out == "", repr(out))
check("被拒后没有落库成成员",
      row("SELECT * FROM group_members WHERE group_id='roles' AND agent_id='bob'") is None)
rc, out, err = cli("group", "join", "--group-id", "roles", "--agent-id", "bob", "--role", "member")
check("--role member 仍然可用（向后兼容）", rc == 0, err or out)
check("落库为 member",
      (row("SELECT role FROM group_members WHERE group_id='roles' AND agent_id='bob'") or {}).get("role") == "member")
rc, out, err = cli("group", "join", "--group-id", "roles", "--agent-id", "carol")
check("不带 --role 可用", rc == 0, err or out)
check("不带 --role 落库为 member",
      (row("SELECT role FROM group_members WHERE group_id='roles' AND agent_id='carol'") or {}).get("role") == "member")
rc, out, err = cli("group", "leave", "--group-id", "roles", "--agent-id", "bob")
check("bob leave", rc == 0, err or out)
rc, out, err = cli("group", "join", "--group-id", "roles", "--agent-id", "bob", "--role", "admin")
check("rejoin 带 --role admin 也被拒", rc != 0 and "not settable on join" in err, err or out)
check("owner 仍只有 create 时那个",
      [r for r in sqlite3.connect(DB).execute(
          "SELECT agent_id,role FROM group_members WHERE group_id='roles' AND role<>'member'").fetchall()]
      == [("alice", "owner")],
      str(sqlite3.connect(DB).execute(
          "SELECT agent_id,role FROM group_members WHERE group_id='roles' AND role<>'member'").fetchall()))

print("J 数字旗标的 0 不再被 or 吃掉（create 落库 + send 生效两侧）")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "bp", "--topic", "t",
                   "--rate-seconds", "0", "--backlog-threshold", "0")
check("create bp", rc == 0, err or out)
check("backlog_threshold 落库为 0（旧行为会存成 50）",
      row("SELECT backlog_threshold AS v FROM groups WHERE group_id='bp'")["v"] == 0,
      str(row("SELECT backlog_threshold AS v FROM groups WHERE group_id='bp'")))
rc, out, err = cli("group", "join", "--group-id", "bp", "--agent-id", "bob", "--rate-seconds", "0")
check("join bp", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "bp", "--text", "m1")
check("第一条不被背压拦（发之前 latest=0，无人落后）", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "bp", "--text", "m2")
g = gate_err(err)
check("bob 未读时第二条被背压拦（旧行为阈值 50，永不触发）",
      rc != 0 and (g or {}).get("error") == "group backpressure: too many unread participants", err or out)
rc, out, err = cli("group", "read", "--group-id", "bp", "--agent-id", "bob", "--seq", "1")
check("bob 补读到 seq1", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "bp", "--text", "m3")
check("bob 补读后放行", rc == 0, err or out)

rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "ceil", "--topic", "t",
                   "--rate-seconds", "0", "--ceiling-per-minute", "0")
check("create ceil 0", rc == 0, err or out)
check("ceiling_per_minute 落库为 0（旧行为会存成 120）",
      row("SELECT ceiling_per_minute AS v FROM groups WHERE group_id='ceil'")["v"] == 0,
      str(row("SELECT ceiling_per_minute AS v FROM groups WHERE group_id='ceil'")))
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "ceil", "--text", "m1")
g = gate_err(err)
check("ceiling 0 立即拦死", rc != 0 and (g or {}).get("error") == "group ceiling reached", err or out)

rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "cap", "--topic", "t",
                   "--max-members", "0", "--rate-seconds", "0")
check("create cap", rc == 0, err or out)
check("max_members 落库为 0（旧行为会存成 50）",
      row("SELECT max_members AS v FROM groups WHERE group_id='cap'")["v"] == 0,
      str(row("SELECT max_members AS v FROM groups WHERE group_id='cap'")))
rc, out, err = cli("group", "join", "--group-id", "cap", "--agent-id", "bob")
check("max-members 0 时第二个 join 被拒", rc != 0 and "is full" in err, err or out)

print("K 两道发送门的错误形状（stderr JSON + 空 stdout + exit 1）")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "gates", "--topic", "t",
                   "--rate-seconds", "0.001")
check("create gates", rc == 0, err or out)
rc, out, err = cli("group", "join", "--group-id", "gates", "--agent-id", "bob", "--rate-seconds", "0.001")
check("join gates", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "gates", "--text", "m1")
check("alice 首发", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "bob", "--group-id", "gates", "--text", "m2")
g = gate_err(err)
check("新成员首发被 read-gate 拦", rc != 0 and (g or {}).get("error") == "must confirm latest message first", err or out)
check("read-gate 报错走 stderr 且 stdout 为空", out == "", repr(out))
check("报错带 latest_seq / last_read_seq / digest_health_hint",
      all(k in (g or {}) for k in ("latest_seq", "last_read_seq", "digest_health_hint")), str(g))
rc, out, err = cli("group", "messages", "--group-id", "gates", "--agent-id", "bob")
mv = json.loads(out)
check("group messages 给出 latest_seq 供恢复", mv.get("latest_seq") == 1, str(mv)[:120])
rc, out, err = cli("group", "read", "--group-id", "gates", "--agent-id", "bob", "--seq", "99")
check("read 超过 latest 被拒，不静默接受", rc != 0 and "invalid seq" in err, err or out)
rc, out, err = cli("group", "read", "--group-id", "gates", "--agent-id", "bob", "--seq", "1")
check("bob 补读到 latest", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "bob", "--group-id", "gates", "--text", "m2")
check("补读后 bob 首发成功", rc == 0, err or out)
rc, out, err = cli("group", "read", "--group-id", "gates", "--agent-id", "alice", "--seq", "2")
check("alice 补读到 bob 的 seq2", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "gates", "--text", "m3")
check("补读后 alice 发 m3", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "gates", "--text", "m4")
check("alice 自己连发不被 read-gate 拦（send 会推进自己的 last_read_seq）", rc == 0, err or out)
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "gates", "--text", "m5")
check("alice 第三次连发仍放行", rc == 0, err or out)

print("L --rate-seconds 0 在 join 与 create 两侧都按 0 生效")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "fast", "--topic", "t",
                   "--rate-seconds", "0", "--ceiling-per-minute", "1000")
check("create fast", rc == 0, err or out)
rc, out, err = cli("group", "join", "--group-id", "fast", "--agent-id", "bob", "--rate-seconds", "0")
check("join fast", rc == 0, err or out)
check("member rate_seconds 落库为 0",
      row("SELECT rate_seconds AS v FROM group_members WHERE group_id='fast' AND agent_id='bob'")["v"] == 0,
      str(row("SELECT rate_seconds AS v FROM group_members WHERE group_id='fast' AND agent_id='bob'")))
fails = []
for i in range(6):
    who = "alice" if i % 2 == 0 else "bob"
    rc, out, err = send(who, "fast", f"f{i}")
    if rc != 0:
        fails.append((who, err or out))
check("rate 0 时 6 条连发全部成功（read-gate 已按规矩补读）", not fails, str(fails[:2]))
check("rate 0 时确实没有 rate limited",
      not any("rate limited" in e for _, e in fails), str(fails[:2]))

print(f"\nRESULT: {'ALL PASS' if not FAIL else str(len(FAIL)) + ' FAILED: ' + repr(FAIL)}")
sys.exit(0 if not FAIL else 1)
