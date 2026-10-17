#!/usr/bin/env python3
"""Verifies the self-exclusion digest change, the digest-id collision fix,
leave auth, and AGENT_BUS_TOKEN_DIR -- end to end through the real CLI.

Lesson from last round: the probe must be isomorphic to real argv. Everything
that has a CLI path here goes through `python3 bus.py ...`, not a hand-built
Namespace. Only poll_group_digest is called as a function, because that is
exactly how the notifier calls it.
"""
import json
import os
import subprocess
import sys
import tempfile

root = tempfile.mkdtemp(prefix="bus-selfexcl-")
DB = os.path.join(root, "world", "bus.db")
TOK = os.path.join(root, "world", "tokens")
os.environ["AGENT_BUS_DB"] = DB
os.environ["AGENT_BUS_TOKEN_DIR"] = TOK
# Without this the probe appends to the PRODUCTION bus_audit.log (the gates
# probe sets it; the digest probe used to leak into the live audit trail).
os.environ["AGENT_BUS_AUDIT_LOG"] = os.path.join(root, "world", "audit.log")

HERE = os.path.dirname(os.path.abspath(__file__))
FAIL = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(name)


def cli(*argv, tok=None):
    """Real CLI invocation. Token env points at this world's token namespace."""
    env = dict(os.environ)
    if tok:
        env["AGENT_BUS_TOKEN_FILE"] = tok
    p = subprocess.run([sys.executable, os.path.join(HERE, "bus.py"), *argv],
                       capture_output=True, text=True, env=env)
    return p.returncode, p.stdout.strip(), p.stderr.strip()


print("A token 命名空间隔离(AGENT_BUS_TOKEN_DIR)")
for a in ("alice", "bob"):
    rc, out, err = cli("register", "--agent-id", a, "--agent-type", "command")
    check(f"register {a}", rc == 0, err or out)
check("token 落在指定目录", sorted(os.listdir(TOK)) == ["alice.token", "bob.token"],
      str(sorted(os.listdir(TOK)) if os.path.isdir(TOK) else "目录不存在"))
check("默认 token 目录未被写入新 agent",
      not os.path.exists(os.path.join(HERE, "tokens", "alice.token")))

print("B 群与成员(真实 CLI)")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "g1", "--topic", "t",
                   "--rate-seconds", "0.001", "--ceiling-per-minute", "1000")
check("create", rc == 0, err or out)
SENT = []


def send(frm, text):
    """Real send path: the read-gate requires confirming latest before sending,
    and the member rate gate needs a small pause. Both are live behaviour, so the
    probe honours them instead of bypassing them.

    Confirming now requires having been served first: `group read` clamps to the
    highest seq `group messages` actually returned, so the probe drains through the
    real paging call before acking. Reading straight from sqlite would skip that
    step and hide the clamp.
    """
    import time
    last = ""
    for _ in range(8):
        rc, out, err = cli("group", "messages", "--group-id", "g1", "--agent-id", frm, "--limit", "1000")
        lat = json.loads(out)["latest_seq"] if rc == 0 else 0
        cli("group", "read", "--group-id", "g1", "--agent-id", frm, "--seq", str(lat))
        rc, out, err = cli("group", "send", "--from", frm, "--group-id", "g1", "--text", text)
        last = err or out
        if rc == 0:
            SENT.append((True, frm, text, last))
            return True
        time.sleep(0.25)
    SENT.append((False, frm, text, last))
    return False
rc, out, err = cli("group", "join", "--group-id", "g1", "--agent-id", "alice")
check("alice join", rc == 0, err or out)
rc, out, err = cli("group", "join", "--group-id", "g1", "--agent-id", "bob")
check("bob join", rc == 0, err or out)

import bus  # noqa: E402
from buslib.delivery import commit_group_digest, poll_group_digest  # noqa: E402

conn = bus.connect()
digest_of = lambda aid: poll_group_digest(aid, conn)
commit = commit_group_digest
cursor = lambda aid: conn.execute(
    "SELECT last_digest_seq AS s FROM group_read_state WHERE group_id='g1' AND agent_id=?", (aid,)
).fetchone()["s"]

print("C 自己的消息不再 digest 给自己")
for i in range(3):
    send("alice", f"alice own {i}")
check("3 条 send 全部成功", all(x[0] for x in SENT), str([x[3] for x in SENT if not x[0]]))
check("alice 连发 3 条后 poll 返回 None", digest_of("alice") is None)
check("own-only 窗口游标已滑过(不卡死)", cursor("alice") == 3, f"cursor={cursor('alice')}")
m = digest_of("bob")
check("bob 仍收到 3 条", m is not None and m["payload"].count("alice own") == 3,
      "None" if m is None else m["payload"][:120])

print("D own-only 不饿死后来者(上一版会卡窗口)")
for i in range(12):
    send("alice", f"alice burst {i}")
send("bob", "bob must be seen")
check("13 条 send 全部成功", all(x[0] for x in SENT), str([x[3] for x in SENT if not x[0]]))
m = digest_of("alice")
check("alice 在 12 条自发消息后仍收到 bob 的消息",
      m is not None and "bob must be seen" in m["payload"],
      "None" if m is None else m["payload"][:200])
check("digest 行里不含 alice 自己的正文",
      m is not None and "alice burst" not in m["payload"] and "alice own" not in m["payload"])

print("E 渲染上限与 pending 只数外部消息")
for i in range(9):
    send("bob", f"bob many {i}")
check("22 条 send 全部成功", all(x[0] for x in SENT), str([x[3] for x in SENT if not x[0]]))
m = digest_of("alice")
head = json.loads(m["payload"])["text"].splitlines()[0] if m else ""
n_lines = lambda x: json.loads(x["payload"])["text"].count("\n  seq ") if x else -1
check("一轮只渲染 5 行", n_lines(m) == 5, f"实际 {n_lines(m)}")
check("头部 pending 只数外部未读(10 条,不含 alice 自己的)", "of 10 unread" in head, head)
commit(conn, "alice", m)
m2 = digest_of("alice")
check("下一轮继续渲染剩余 5 条", n_lines(m2) == 5, f"实际 {n_lines(m2)}")

print("F digest id 用 _digest_seq,同 latest 两轮不再撞主键")
check("两轮 id 不同", m["message_id"] != m2["message_id"], f"{m['message_id']} / {m2['message_id']}")
commit(conn, "alice", m2)
rows = conn.execute(
    "SELECT COUNT(*) AS c FROM messages WHERE thread_id='g1' AND to_agent='alice' AND message_id LIKE 'group-digest-%'"
).fetchone()["c"]
check("两条 digest 行都在库里(无 INSERT OR IGNORE 丢行)", rows == 2, f"行数={rows}")
for mid in (m["message_id"], m2["message_id"]):
    rc, out, err = cli("message", "show", "--message-id", mid, "--agent-id", "alice")
    check(f"bus message show {mid[-24:]}", rc == 0 and '"ok":true' in out, err or out[:80])

print("G leave 需要认证")
rc, out, err = cli("group", "leave", "--group-id", "g1", "--agent-id", "bob",
                   tok=os.path.join(TOK, "alice.token"))
check("拿 alice 的 token 替 bob leave 被拒", rc != 0 and ("token" in err or "invalid" in err),
      err or out)
rc, out, err = cli("group", "leave", "--group-id", "g1", "--agent-id", "bob",
                   tok=os.path.join(TOK, "bob.token"))
check("bob 自己 leave 成功", rc == 0 and '"ok":true' in out, err or out)
st = conn.execute("SELECT status FROM group_members WHERE group_id='g1' AND agent_id='bob'").fetchone()["status"]
check("leave 生效", st == "left", st)

print("H 注入失败不推进游标(不变量未被 own-only 分支破坏)")
rc, out, err = cli("group", "join", "--group-id", "g1", "--agent-id", "bob")  # leave 删了 read_state
check("bob rejoin 成功", rc == 0, err or out)
check("bob 状态回到 active",
      conn.execute("SELECT status FROM group_members WHERE group_id='g1' AND agent_id='bob'").fetchone()["status"] == "active")
before = cursor("bob")
ok3 = send("alice", "for bob only")
check("H 的 send 成功", ok3, SENT[-1][3] if SENT else "none")
print("     latest=", conn.execute("SELECT COALESCE(MAX(seq),0) s FROM group_messages WHERE group_id='g1'").fetchone()["s"],
      " bob cursor=", before,
      " bob active=", conn.execute("SELECT status FROM group_members WHERE group_id='g1' AND agent_id='bob'").fetchone()["status"])
m3 = digest_of("bob")
check("poll 本身不推进游标", cursor("bob") == before, f"{before} -> {cursor('bob')}")
check("rejoin 后从历史开头 catch-up(已知行为)", m3 is not None and "seq 1 from alice" in m3["payload"])
# Drain every round and assert nothing is lost: the rendered set must equal the
# full set of foreign messages, each exactly once.
rendered = []
rounds = 0
while rounds < 40:
    m = digest_of("bob")
    if m is None:
        break
    rounds += 1
    rendered += [int(l.split()[1]) for l in json.loads(m["payload"])["text"].splitlines() if l.startswith("  seq ")]
    commit(conn, "bob", m)
expected = [r["seq"] for r in conn.execute(
    "SELECT seq FROM group_messages WHERE group_id='g1' AND sender_agent<>'bob' ORDER BY seq")]
check("逐轮排空后外部消息一条不漏、一条不重",
      rendered == expected, f"渲染 {len(rendered)} / 应有 {len(expected)}")
check("最后一条 alice 的消息确实送达 bob", 26 in rendered, f"max rendered={max(rendered) if rendered else None}")

print()
print("RESULT:", "ALL PASS" if not FAIL else f"{len(FAIL)} FAILED: {FAIL}")
sys.exit(1 if FAIL else 0)
