#!/usr/bin/env python3
"""Regression probe for the hardening pass.

Covers what the two group probes do NOT cover: register credential protection,
auth on data-exposing commands (agents/group messages/read/status/dlq/peer),
id charset validation (shell-metacharacter ids), dlq retry expiry reset,
peer token masking, outbox max_attempts, ask/answer idempotency, concurrent
group send under the new BEGIN IMMEDIATE transactions, heartbeat session
non-rebinding, gateway allowlist bypasses, daemon importability.

Runs against a throwaway bus world (AGENT_BUS_DB / AGENT_BUS_TOKEN_DIR /
AGENT_BUS_AUDIT_LOG), never the production one.
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time

root = tempfile.mkdtemp(prefix="bus-sec-")
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


def cli(*argv, tok=None, env_extra=None):
    env = dict(os.environ)
    if tok:
        env["AGENT_BUS_TOKEN_FILE"] = tok
    if env_extra:
        env.update(env_extra)
    p = subprocess.run([sys.executable, os.path.join(HERE, "bus.py"), *argv],
                       capture_output=True, text=True, env=env)
    return p.returncode, p.stdout, p.stderr


def tokfile(agent):
    return os.path.join(TOK, f"{agent}.token")


def jout(out):
    try:
        return json.loads(out)
    except (json.JSONDecodeError, TypeError):
        return {}


print("A register 凭据保护")
rc, out, err = cli("register", "--agent-id", "alice", "--agent-type", "command", "--session-id", "s1", "--endpoint", "s1")
check("alice 注册", rc == 0 and jout(out).get("created_token") is True, err or out[:80])
rc, out, err = cli("register", "--agent-id", "bob", "--agent-type", "command", "--session-id", "s1", "--endpoint", "s1")
check("bob 注册", rc == 0, err or out[:80])
alice_hash_before = None
import sqlite3  # noqa: E402
conn = sqlite3.connect(DB)
alice_hash_before = conn.execute("SELECT token_hash FROM agents WHERE agent_id='alice'").fetchone()[0]
alice_token_before = open(tokfile("alice")).read()
# takeover attempt 1: present bob's token while registering as alice
rc, out, err = cli("register", "--agent-id", "alice", "--agent-type", "command", tok=tokfile("bob"))
check("拿 bob 的 token 重注册 alice 被拒", rc != 0 and "requires its current token" in err, err[:120])
# takeover attempt 2: no token at all
rc, out, err = cli("register", "--agent-id", "alice", "--agent-type", "command", tok=os.path.join(root, "nope.token"))
check("无 token 重注册 alice 被拒", rc != 0, err[:120])
h = conn.execute("SELECT token_hash FROM agents WHERE agent_id='alice'").fetchone()[0]
check("被拒后 alice 的 token_hash 未变", h == alice_hash_before)
check("被拒后 alice 的 token 文件未变", open(tokfile("alice")).read() == alice_token_before)
# legitimate re-register with own token
rc, out, err = cli("register", "--agent-id", "alice", "--agent-type", "command", "--model-tier", "strong", tok=tokfile("alice"))
check("带自己 token 的合法重注册成功", rc == 0 and jout(out).get("created_token") is False, err or out[:80])
# explicit rotation
rc, out, err = cli("register", "--agent-id", "alice", "--agent-type", "command", "--rotate-token", tok=tokfile("alice"))
check("--rotate-token 成功且如实报告 created_token", rc == 0 and jout(out).get("created_token") is True, err or out[:80])
rc, out, err = cli("heartbeat", "--agent-id", "alice", tok=alice_token_before and tokfile("alice"))
check("轮换后新 token 可用(旧文件路径已写入新 token)", rc == 0, err[:120])

print("B agents/find 不再打印 token_hash")
rc, out, err = cli("agents")
check("agents 输出无 token_hash", rc == 0 and "token_hash" not in out, out[:80])
rc, out, err = cli("find", "--model-tier", "strong")
check("find 输出无 token_hash", rc == 0 and "token_hash" not in out, out[:80])

print("C group messages/read/status 需要 token(光猜 group_id+agent_id 不行)")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "secg", "--rate-seconds", "0", tok=tokfile("alice"))
check("group create(带 owner token)", rc == 0, err or out[:80])
rc, out, err = cli("group", "join", "--group-id", "secg", "--agent-id", "bob", tok=tokfile("bob"))
check("bob join", rc == 0, err or out[:80])
rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "secg", "--text", "hi", tok=tokfile("alice"))
check("alice 首发", rc == 0, err or out[:80])
rc, out, err = cli("group", "messages", "--group-id", "secg", "--agent-id", "bob", tok=tokfile("alice"))
check("拿 alice token 读 bob 的未读被拒", rc != 0 and "token" in err, err[:120])
rc, out, err = cli("group", "messages", "--group-id", "secg", "--agent-id", "bob", tok=os.path.join(root, "nope.token"))
check("无 token 读群未读被拒", rc != 0, err[:120])
rc, out, err = cli("group", "read", "--group-id", "secg", "--agent-id", "bob", "--seq", "1", tok=tokfile("alice"))
check("拿别人 token 替 bob 推进 read 游标被拒", rc != 0 and "token" in err, err[:120])
rc, out, err = cli("group", "status", "--group-id", "secg")
check("group status 缺 --agent-id 被 argparse 拒", rc != 0, err[:100])
rc, out, err = cli("group", "status", "--group-id", "secg", "--agent-id", "bob", tok=tokfile("alice"))
check("group status 错 token 被拒", rc != 0 and "token" in err, err[:120])
rc, out, err = cli("group", "status", "--group-id", "secg", "--agent-id", "bob", tok=tokfile("bob"))
check("group status 本成员本 token 放行", rc == 0, err[:120])

print("D id 字符集校验(堵 shell 注入 / 换行伪造)")
canary = os.path.join(root, "pwned")
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "x", "--thread-id", f"$(touch {canary})", tok=tokfile("alice"))
check("thread-id 带 $() 被拒", rc != 0 and "invalid thread_id" in err, err[:120])
check("被拒的 thread-id 没有执行 shell", not os.path.exists(canary))
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "x", "--thread-id", "a\nb", tok=tokfile("alice"))
check("thread-id 带换行被拒", rc != 0 and "invalid thread_id" in err, err[:120])
rc, out, err = cli("register", "--agent-id", "evil;id", "--agent-type", "command")
check("agent-id 带分号被拒", rc != 0 and "invalid agent_id" in err, err[:120])
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "g`x`", tok=tokfile("alice"))
check("group-id 带反引号被拒", rc != 0 and "invalid group_id" in err, err[:120])

print("E dlq 鉴权 + retry 真的重置过期")
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "doomed", "--ttl", "-60", tok=tokfile("alice"))
check("发一条已过期 ttl 的消息", rc == 0, err or out[:80])
mid = jout(out).get("message_id")
cli("sweep")
rc, out, err = cli("dlq", "list")
check("dlq list 无操作者身份被拒", rc != 0 and "operator identity" in err, err[:120])
rc, out, err = cli("dlq", "list", "--agent-id", "bob", tok=tokfile("bob"))
check("dlq list 带身份可列出 expired", rc == 0 and mid in out, err[:120])
rc, out, err = cli("dlq", "retry", "--message-id", mid, "--agent-id", "bob", tok=tokfile("bob"))
check("dlq retry 带身份成功", rc == 0, err or out[:120])
cli("sweep")
st = conn.execute("SELECT status, expires_at FROM messages WHERE message_id=?", (mid,)).fetchone()
check("retry 后消息没有被 sweep 立刻再过期(旧行为:retry 是 no-op)", st is not None and st[0] == "queued" and st[1] is not None and st[1] > time.time(), str(st))
# discard only accepts dead/expired rows: make a second doomed message for it
rc, out, err = cli("send", "--from", "alice", "--to", "bob", "--text", "doomed2", "--ttl", "-60", tok=tokfile("alice"))
mid2 = jout(out).get("message_id")
cli("sweep")
rc, out, err = cli("dlq", "discard", "--message-id", mid2, "--agent-id", "bob", tok=tokfile("bob"))
gone = conn.execute("SELECT 1 FROM messages WHERE message_id=?", (mid2,)).fetchone()
check("dlq discard 带身份成功且真的删行", rc == 0 and gone is None, err or out[:120])

print("F peer/status 不再回显明文 token")
SECRET = "supers3cret-token-value"
rc, out, err = cli("peer", "add", "--peer", "p1", "--base-url", "http://127.0.0.1:9", "--token", SECRET, "--agent-id", "alice", tok=tokfile("alice"))
check("peer add 带操作者身份成功", rc == 0, err or out[:120])
rc, out, err = cli("peer", "list", "--agent-id", "alice", tok=tokfile("alice"))
check("peer list 不回显明文 token", rc == 0 and SECRET not in out and '"***"' in out, out[:120])
rc, out, err = cli("status")
check("status 不回显明文 token", rc == 0 and SECRET not in out, out[:150])
rc, out, err = cli("peer", "list")
check("peer list 无身份被拒", rc != 0 and "operator identity" in err, err[:120])

print("G outbox max_attempts 生效")
now_ts = time.time()
conn.execute(
    "INSERT INTO outbox (outbox_id,peer_name,message_id,from_agent,to_remote,payload,status,attempts,max_attempts,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
    ("o-budget", "p1", "m-budget", "alice", "x@p1", "{}", "pending", 5, 5, now_ts, now_ts),
)
conn.commit()
rc, out, err = cli("outbox", "flush", "--agent-id", "alice", tok=tokfile("alice"))
st = conn.execute("SELECT status FROM outbox WHERE outbox_id='o-budget'").fetchone()
check("超预算的 outbox 行被放弃(status=dead)", st is not None and st[0] == "dead", str(st))
check("flush 报告 given_up", rc == 0 and jout(out).get("given_up", 0) >= 1, out[:120])

print("H ask/answer 幂等键不再炸 traceback")
rc, out, err = cli("ask", "--from", "alice", "--to", "bob", "--text", "q1", "--idempotency-key", "K1", tok=tokfile("alice"))
check("ask 首发", rc == 0, err or out[:80])
rc, out, err = cli("ask", "--from", "alice", "--to", "bob", "--text", "q1", "--idempotency-key", "K1", tok=tokfile("alice"))
check("ask 重复键返回 duplicate 而非 IntegrityError", rc == 0 and jout(out).get("duplicate") is True and "Traceback" not in err, err[:120] or out[:80])
tid = jout(out).get("thread_id")
rc, out, err = cli("answer", "--thread-id", tid, "--from", "bob", "--to", "alice", "--text", "a1", "--idempotency-key", "K2", tok=tokfile("bob"))
check("answer 首发", rc == 0, err or out[:80])
rc, out, err = cli("answer", "--thread-id", tid, "--from", "bob", "--to", "alice", "--text", "a1", "--idempotency-key", "K2", tok=tokfile("bob"))
check("answer 重复键返回 duplicate 而非 IntegrityError", rc == 0 and jout(out).get("duplicate") is True and "Traceback" not in err, err[:120] or out[:80])

print("I 并发 group send:BEGIN IMMEDIATE 下 seq 不再撞车")
rc, out, err = cli("group", "create", "--from", "alice", "--group-id", "race", "--rate-seconds", "0", "--ceiling-per-minute", "100", tok=tokfile("alice"))
check("race 组创建", rc == 0, err or out[:80])
results = []


def fire(i):
    rc, out, err = cli("group", "send", "--from", "alice", "--group-id", "race", "--text", f"r{i}", tok=tokfile("alice"))
    results.append((rc, out, err))


threads = [threading.Thread(target=fire, args=(i,)) for i in range(8)]
for t in threads:
    t.start()
for t in threads:
    t.join()
oks = [r for r in results if r[0] == 0]
seqs = sorted(jout(r[1]).get("seq") for r in oks)
check("8 个并发 send 全部成功(旧行为:撞 UNIQUE 报 insert failed)", len(oks) == 8, f"成功 {len(oks)}/8 " + (next((r[2] for r in results if r[0] != 0), "")[:120]))
check("seq 连续且不重复", seqs == list(range(1, 9)), str(seqs))

print("J 闸门顺序:空文本先于 read-gate 报错")
rc, out, err = cli("group", "join", "--group-id", "race", "--agent-id", "bob", tok=tokfile("bob"))
check("bob join race", rc == 0, err or out[:80])
rc, out, err = cli("group", "send", "--from", "bob", "--group-id", "race", "--text", "  ", tok=tokfile("bob"))
check("bob 未补读时空文本报的是 empty 而不是 read-gate", rc != 0 and "must not be empty" in err, err[:120])

print("K heartbeat 不再覆盖 session 绑定")
rc, out, err = cli("register", "--agent-id", "carol", "--agent-type", "opencode", "--session-id", "ses_new", "--endpoint", "ses_new")
check("carol 注册 session=ses_new", rc == 0, err or out[:80])
rc, out, err = cli("heartbeat", "--agent-id", "carol", tok=tokfile("carol"))
row = conn.execute("SELECT current_session_id FROM agents WHERE agent_id='carol'").fetchone()
check("不带 --session-id 的 heartbeat 保持原绑定", rc == 0 and row[0] == "ses_new", str(row))
rc, out, err = cli("heartbeat", "--agent-id", "carol", "--session-id", "ses_changed", tok=tokfile("carol"))
row = conn.execute("SELECT current_session_id FROM agents WHERE agent_id='carol'").fetchone()
check("显式 --session-id 的 heartbeat 才改绑定", rc == 0 and row[0] == "ses_changed", str(row))

print("L gateway 群允许名单:两个绕过都被堵 + 未配置时 fail-closed")
gw = os.path.join(HERE, "bus_ssh_gateway.py")


def gw_run(cmd, groups=None):
    env = dict(os.environ)
    env["SSH_ORIGINAL_COMMAND"] = cmd
    env.pop("AGENT_BUS_ALLOWED_GROUPS", None)
    argv = [sys.executable, gw, "--identity", "bob", "--db", DB, "--bus", os.path.join(HERE, "bus.py")]
    if groups:
        argv += ["--groups", groups]
    p = subprocess.run(argv, capture_output=True, text=True, env=env)
    return p.returncode, p.stdout, p.stderr


rc, out, err = gw_run("bus group messages --group-id=evil --agent-id bob", groups="secg")
check("--group-id=evil 等号写法不再绕过允许名单", rc != 0 and "not allowed" in err, err[:120])
rc, out, err = gw_run("bus group messages --group-id secg --group-id evil", groups="secg")
check("重复 --group-id 不再只校验第一个", rc != 0 and "not allowed" in err, err[:120])
rc, out, err = gw_run("bus group messages --group-id secg")
check("未配置允许名单时群命令被拒(fail-closed)", rc != 0 and "allowlist" in err, err[:120])
rc, out, err = gw_run("bus group messages --group-id secg", groups="secg")
check("允许名单内的合法命令可执行(且自动钉上 --agent-id)", rc == 0 and '"ok":true' in out, err[:120] or out[:80])
# bob must be caught up before the gateway send (read gate is live behavior)
rc, out, err = cli("group", "messages", "--group-id", "secg", "--agent-id", "bob", "--limit", "1000", tok=tokfile("bob"))
lat = jout(out).get("latest_seq", 0)
cli("group", "read", "--group-id", "secg", "--agent-id", "bob", "--seq", str(lat), tok=tokfile("bob"))
rc, out, err = gw_run("bus group send --group secg --text 'remote hi'", groups="secg")
check("--group 拼写被规范化为 --group-id 并可执行", rc == 0 and '"ok":true' in out, err[:120] or out[:80])

print("M daemon 可 import(heredoc 残留已删)")
try:
    import bus_daemon  # noqa: F401
    check("import bus_daemon 无 NameError", True)
except NameError as exc:
    check("import bus_daemon 无 NameError", False, str(exc))

print("N 1:1 冒烟:send -> inbox -> ack -> thread")
rc, out, err = cli("send", "--from", "alice", "--to", "carol", "--text", "smoke", tok=tokfile("alice"))
mid = jout(out).get("message_id")
tid = jout(out).get("thread_id")
check("send", rc == 0 and mid, err or out[:80])
rc, out, err = cli("inbox", "--agent-id", "carol", tok=tokfile("carol"))
check("inbox 可见", rc == 0 and mid in out, err[:80])
rc, out, err = cli("ack", "--agent-id", "carol", "--message-id", mid, tok=tokfile("carol"))
check("ack", rc == 0, err[:80])
rc, out, err = cli("inbox", "--agent-id", "carol", tok=tokfile("carol"))
check("ack 后不再出现在 undelivered", rc == 0 and mid not in out, out[:80])
rc, out, err = cli("inbox", "--agent-id", "carol", "--status", "all", tok=tokfile("carol"))
check("--status all 现在真的包含 acked 行(旧行为:静默过滤)", rc == 0 and mid in out, out[:120])
rc, out, err = cli("thread", tid, "--agent-id", "alice", tok=tokfile("alice"))
check("thread 可读(参与者)", rc == 0 and mid in out, err[:80])
rc, out, err = cli("thread", tid, "--agent-id", "bob", tok=tokfile("bob"))
check("thread 非参与者被拒", rc != 0 and "denied" in err, err[:80])

conn.close()
print()
if FAIL:
    print(f"RESULT: {len(FAIL)} FAILED: {FAIL}")
    sys.exit(1)
print("RESULT: ALL PASS")
