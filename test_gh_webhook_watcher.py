#!/usr/bin/env python3
"""gh-webhook-watcher 端到端自测：真启服务、真发签名请求，覆盖验签/过滤/去重/SSE/轮询/群推送。

用法：python test_gh_webhook_watcher.py
退出码 0 表示全部通过。
"""
import hashlib
import hmac
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "gh-webhook-watcher.py")
SECRET = "test-secret-please-change"
TOKEN = "test-api-token"
TEST_GROUP_ID = 100000000

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok), detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail and not ok else ""), flush=True)
    return bool(ok)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def sign(secret, body):
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


# --------------------------------------------------------------------------- 假 OneBot

class FakeOneBot:
    def __init__(self):
        self.messages = []
        self.lock = threading.Lock()
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n)
                if self.path.rstrip("/") != "/send_group_msg":
                    self.send_response(404)
                    self.end_headers()
                    return
                data = json.loads(body.decode("utf-8"))
                with outer.lock:
                    outer.messages.append(data)
                out = json.dumps({"status": "ok", "retcode": 0, "data": {"message_id": 1}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

        self.port = free_port()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", self.port), H)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return list(self.messages)

    def wait_for(self, predicate, timeout=10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for m in self.snapshot():
                if predicate(m):
                    return m
            time.sleep(0.1)
        return None

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


# --------------------------------------------------------------------------- HTTP 客户端

def request(url, method="GET", body=None, headers=None, timeout=10):
    req = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def api_get(base, path, token=TOKEN, extra_headers=None):
    headers = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    headers.update(extra_headers or {})
    status, text = request(f"{base}{path}", headers=headers)
    try:
        return status, json.loads(text)
    except json.JSONDecodeError:
        return status, {"_raw": text}


def wait_ready(base, proc, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        try:
            status, _ = api_get(base, "/api/health")
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def post_webhook(base, event_type, payload, delivery, secret=SECRET, signature=None, path="/webhook/github"):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    sig = signature if signature is not None else sign(secret, body)
    headers = {
        "Content-Type": "application/json",
        "X-GitHub-Event": event_type,
        "X-GitHub-Delivery": delivery,
        "X-Hub-Signature-256": sig,
        "User-Agent": "GitHub-Hookshot/test",
    }
    status, text = request(f"{base}{path}", method="POST", body=body, headers=headers)
    try:
        return status, json.loads(text)
    except json.JSONDecodeError:
        return status, {"_raw": text}


# --------------------------------------------------------------------------- 载荷

def issue_payload(owner="furryaxw", repo="demo", number=7, action="opened"):
    full = f"{owner}/{repo}"
    return {
        "action": action,
        "issue": {"number": number, "title": "文档里的命令写错了", "state": "open",
                  "html_url": f"https://github.com/{full}/issues/{number}",
                  "labels": [{"name": "bug"}]},
        "repository": {"full_name": full, "html_url": f"https://github.com/{full}"},
        "sender": {"login": "tester"},
    }


def pr_payload(owner="Syncanid", repo="tool", number=12):
    full = f"{owner}/{repo}"
    return {
        "action": "opened",
        "pull_request": {"number": number, "title": "支持 webhook", "state": "open", "draft": False,
                         "merged": False, "base": {"ref": "main"},
                         "html_url": f"https://github.com/{full}/pull/{number}"},
        "repository": {"full_name": full, "html_url": f"https://github.com/{full}"},
        "sender": {"login": "tester"},
    }


def release_payload(draft=False, tag="v1.2.0", owner="furryaxw", repo="demo"):
    full = f"{owner}/{repo}"
    return {
        "action": "published",
        "release": {"tag_name": tag, "name": "1.2.0", "draft": draft, "prerelease": False,
                    "html_url": f"https://github.com/{full}/releases/tag/{tag}"},
        "repository": {"full_name": full, "html_url": f"https://github.com/{full}"},
        "sender": {"login": "tester"},
    }


def star_payload(owner="furryaxw", repo="demo"):
    full = f"{owner}/{repo}"
    return {
        "action": "created",
        "starred_at": "2026-01-01T00:00:00Z",
        "repository": {"full_name": full, "html_url": f"https://github.com/{full}"},
        "sender": {"login": "fan"},
    }


def push_payload(owner="furryaxw", repo="demo", ref="main", n=2):
    full = f"{owner}/{repo}"
    return {
        "ref": f"refs/heads/{ref}",
        "compare": f"https://github.com/{full}/compare/aaa...bbb",
        "forced": False,
        "commits": [{"message": f"commit {i}\n\nbody", "url": f"https://github.com/{full}/commit/{i}"}
                    for i in range(n)],
        "repository": {"full_name": full, "html_url": f"https://github.com/{full}"},
        "sender": {"login": "tester"},
    }


# --------------------------------------------------------------------------- SSE

def collect_sse(base, path, stop_event, sink, headers=None):
    headers = headers or {}
    if TOKEN:
        headers.setdefault("Authorization", f"Bearer {TOKEN}")
    req = urllib.request.Request(f"{base}{path}", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            while not stop_event.is_set():
                line = resp.readline()
                if not line:
                    break
                decoded = line.decode("utf-8").rstrip("\n")
                if decoded.startswith("data: "):
                    sink.append(json.loads(decoded[6:]))
    except Exception as e:  # 关闭连接时正常抛出
        sink.append({"_error": repr(e)})


# --------------------------------------------------------------------------- 主流程

def main():
    tmp = tempfile.mkdtemp(prefix="ghw-test-")
    onebot = FakeOneBot()
    app_port = free_port()
    base = f"http://127.0.0.1:{app_port}"
    log_path = os.path.join(tmp, "watcher.log")
    stdout_path = os.path.join(tmp, "stdout.log")

    env = dict(os.environ)
    env.update({
        "GHW_HOST": "127.0.0.1",
        "GHW_PORT": str(app_port),
        "GHW_WEBHOOK_SECRET": SECRET,
        "GHW_API_TOKEN": TOKEN,
        "GHW_OWNERS": "furryaxw,Syncanid",
        "GHW_ONEBOT_URL": f"http://127.0.0.1:{onebot.port}",
        "GHW_ONEBOT_GROUP": str(TEST_GROUP_ID),
        "GHW_LOG_FILE": log_path,
        "GHW_STATE_FILE": os.path.join(tmp, "watcher-state.json"),
        "GHW_HEARTBEAT": "1",
        "GHW_EVENT_BUFFER": "50",
        "GHW_VERBOSE": "1",
    })

    proc = None
    try:
        popen_kwargs = {}
        if os.name == "nt":
            # 独立进程组，才能用 CTRL_BREAK_EVENT 触发优雅退出（terminate 是硬杀）
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

        def start(custom_env=None, script=SCRIPT, cwd=tmp, extra=("--no-env-file",)):
            # 默认 --no-env-file：主流程只依赖显式 env，不受仓库里那份真实 .env 影响
            with open(stdout_path, "ab") as out:
                return subprocess.Popen([sys.executable, script, *extra], env=custom_env or env, stdout=out,
                                        stderr=subprocess.STDOUT, cwd=cwd, **popen_kwargs)

        proc = start()
        ready = wait_ready(base, proc)
        if not check("服务启动并就绪", ready, f"stdout={open(stdout_path, encoding='utf-8', errors='replace').read()[-800:]}"):
            return 1

        # 1. 认证
        status, _ = api_get(base, "/api/health", token=None)
        check("无 token 访问 /api/health 返回 401", status == 401, f"status={status}")
        status, health = api_get(base, "/api/health")
        check("带 token 访问 /api/health 返回 200", status == 200 and health.get("ok"), f"status={status}")
        check("health 报告 owner 白名单", health.get("config", {}).get("owners") == ["furryaxw", "syncanid"],
              json.dumps(health.get("config", {}).get("owners")))

        # 2. ping
        status, resp = post_webhook(base, "ping", {"zen": "Practicality beats purity.", "hook_id": 1}, "d-ping")
        check("ping 事件返回 pong", status == 200 and resp.get("msg") == "pong", f"{status} {resp}")

        # 3. 错误签名
        status, resp = post_webhook(base, "issues", issue_payload(), "d-badsig", signature="sha256=deadbeef")
        check("签名错误返回 401", status == 401 and resp.get("error") == "invalid_signature", f"{status} {resp}")
        status, _ = post_webhook(base, "issues", issue_payload(), "d-nosig", secret="wrong")
        check("用错 secret 签名返回 401", status == 401, f"status={status}")

        # 4. 正常 issue
        status, resp = post_webhook(base, "issues", issue_payload(), "d-1")
        check("issue webhook 返回 202 且带 seq", status == 202 and resp.get("seq") == 1 and resp.get("kind") == "Issue",
              f"{status} {resp}")

        # 5. 去重
        status, resp = post_webhook(base, "issues", issue_payload(), "d-1")
        check("同一 delivery 重放被判重复", status == 200 and resp.get("duplicate") is True, f"{status} {resp}")

        # 6. owner 白名单外
        status, resp = post_webhook(base, "issues", issue_payload(owner="someone-else", repo="x"), "d-2")
        check("白名单外 owner 被过滤", status == 200 and resp.get("filtered") is True, f"{status} {resp}")

        # 7. PR / draft release / push
        status, resp = post_webhook(base, "pull_request", pr_payload(), "d-3")
        check("PR webhook 归一化为 PR", status == 202 and resp.get("kind") == "PR", f"{status} {resp}")
        status, resp = post_webhook(base, "release", release_payload(draft=True, tag="v0.0.1"), "d-4")
        check("draft release 被跳过", status == 200 and resp.get("skipped") is True, f"{status} {resp}")
        status, resp = post_webhook(base, "release", release_payload(tag="v1.2.0"), "d-5")
        check("正式 release 被接收", status == 202 and resp.get("kind") == "Release", f"{status} {resp}")
        status, resp = post_webhook(base, "push", push_payload(), "d-6")
        check("push webhook 归一化为 Push", status == 202 and resp.get("kind") == "Push", f"{status} {resp}")

        # 8. 错误路径 / 缺头
        status, _ = post_webhook(base, "issues", issue_payload(), "d-7", path="/nope")
        check("错误路径返回 404", status == 404, f"status={status}")
        body = json.dumps(issue_payload()).encode()
        status, _ = request(f"{base}/webhook/github", method="POST", body=body,
                            headers={"X-Hub-Signature-256": sign(SECRET, body)})
        check("缺少 X-GitHub-Event 返回 400", status == 400, f"status={status}")

        # 9. 轮询
        status, page = api_get(base, "/api/events?since=0&limit=100")
        kinds = [e["kind"] for e in page.get("events", [])]
        check("轮询返回全部已接收事件（ping/重复/过滤/草稿除外）",
              status == 200 and kinds == ["Issue", "PR", "Release", "Push"], f"{kinds}")
        check("轮询游标等于最后一条 seq", page.get("cursor") == page.get("latest_seq") == 4,
              f"cursor={page.get('cursor')} latest={page.get('latest_seq')}")
        check("事件里带可直接展示的文本", "\n" in page["events"][0].get("text", ""),
              page["events"][0].get("text"))
        status, page = api_get(base, "/api/events?since=4")
        check("since 大于最新序号时无新事件", status == 200 and page.get("count") == 0, json.dumps(page)[:200])
        status, page = api_get(base, "/api/events?since=0&kind=pr")
        check("按 kind 过滤（小写也可）", [e["kind"] for e in page["events"]] == ["PR"], json.dumps(page)[:200])
        status, page = api_get(base, "/api/events?since=0&repo=syncanid/tool")
        check("按 repo 过滤", [e["full_name"] for e in page["events"]] == ["Syncanid/tool"], json.dumps(page)[:200])
        status, page = api_get(base, "/api/events?since=0&type=push,release")
        check("按 type 过滤", sorted(e["type"] for e in page["events"]) == ["push", "release"], json.dumps(page)[:200])

        # 10. SSE：先续传，再实时
        sse_events = []
        stop = threading.Event()
        t = threading.Thread(target=collect_sse, args=(base, "/api/stream?since=4", stop, sse_events), daemon=True)
        t.start()
        time.sleep(1.2)
        status, resp = post_webhook(base, "issues", issue_payload(number=99), "d-8")
        live_seq = resp.get("seq")
        ok_live = False
        deadline = time.time() + 8
        while time.time() < deadline:
            if any(e.get("seq") == live_seq for e in sse_events):
                ok_live = True
                break
            time.sleep(0.2)
        check("SSE 实时收到新事件", status == 202 and ok_live, f"seq={live_seq} got={sse_events}")

        sse_backlog = []
        stop2 = threading.Event()
        t2 = threading.Thread(target=collect_sse, args=(base, "/api/stream", stop2, sse_backlog,
                                                       {"Last-Event-ID": "3"}), daemon=True)
        t2.start()
        deadline = time.time() + 8
        while time.time() < deadline and len([e for e in sse_backlog if "seq" in e]) < 2:
            time.sleep(0.2)
        stop2.set()
        got = sorted(e["seq"] for e in sse_backlog if "seq" in e)
        check("SSE 按 Last-Event-ID 续传（seq>3 的补发）", got == [4, 5], f"got={got} raw={sse_backlog[:3]}")

        stop.set()
        t.join(timeout=5)
        stop2.set()
        t2.join(timeout=5)

        # 11. 群推送保留
        msg = onebot.wait_for(lambda m: "文档里的命令写错了" in m.get("message", ""))
        check("OneBot 群推送仍然工作（Issue 推群）", msg is not None and msg.get("group_id") == TEST_GROUP_ID,
              json.dumps(onebot.snapshot())[:300])
        pushed = "".join(m.get("message", "") for m in onebot.snapshot())
        check("推送到群的消息含标题与链接", "https://github.com/furryaxw/demo/issues/7" in pushed, pushed[:200])
        check("Push 事件在默认推群类型内", "[Push] furryaxw/demo main · 2 commits" in pushed, pushed[:300])

        status, resp = post_webhook(base, "star", star_payload(), "d-9")
        time.sleep(1.5)
        pushed_after = "".join(m.get("message", "") for m in onebot.snapshot())
        check("star 事件不推群（默认推群类型之外）", status == 202 and "新 star" not in pushed_after,
              pushed_after[:300])
        status, page = api_get(base, "/api/events?since=0&limit=100")
        check("star 事件仍然对 API 消费者可见", "Star" in [e["kind"] for e in page["events"]],
              json.dumps([e["kind"] for e in page["events"]]))

        # 12. 统计
        status, health = api_get(base, "/api/health")
        counters = health.get("stats", {}).get("counters", {})
        check("health 统计口径正确",
              counters.get("accepted") == 6 and counters.get("duplicate") == 1
              and counters.get("filtered") == 1 and counters.get("skipped") == 1
              and counters.get("bad_signature") == 2 and counters.get("ping") == 1,
              json.dumps(counters))
        subs = health.get("hub", {}).get("subscribers")
        deadline = time.time() + 10
        while subs != 0 and time.time() < deadline:
            time.sleep(0.5)
            status, health = api_get(base, "/api/health")
            subs = health.get("hub", {}).get("subscribers")
        check("SSE 客户端断开后订阅被回收", subs == 0, f"subscribers={subs}")

        # 13. 优雅退出
        if os.name == "nt":
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        try:
            code = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            code = -1
        log_text = open(log_path, encoding="utf-8", errors="replace").read()
        check("收到信号后退出码为 0", code == 0, f"code={code}")
        check("日志覆盖启动/事件/停止", "ready:" in log_text and "event seq=" in log_text
              and "stopped counters=" in log_text, log_text[-300:])

        # 14. 重启后 seq 继续单调递增（消费者持久游标才安全）
        state_path = env["GHW_STATE_FILE"]
        state = json.load(open(state_path, encoding="utf-8"))
        check("退出时把最新 seq 落盘", state.get("last_seq") == 6, json.dumps(state))
        proc = start()
        check("重启后就绪", wait_ready(base, proc))
        status, resp = post_webhook(base, "issues", issue_payload(number=101), "d-10")
        check("重启后 seq 从 7 继续", status == 202 and resp.get("seq") == 7, f"{status} {resp}")
        status, health = api_get(base, "/api/health")
        hub = health.get("hub", {})
        check("重启后缓冲重建但序号连续", hub.get("latest_seq") == 7 and hub.get("buffer_len") == 1,
              json.dumps(hub))
        if os.name == "nt":
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        try:
            code2 = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            code2 = -1
        check("重启后的进程同样优雅退出", code2 == 0, f"code={code2}")
        check("落盘序号推进到 7", json.load(open(state_path, encoding="utf-8")).get("last_seq") == 7,
              open(state_path, encoding="utf-8").read())

        # 15. 显式置空的语义：不限 owner、不推群（空值不能被当成「未设置」而回落默认）
        env_empty = dict(env)
        env_empty["GHW_OWNERS"] = ""
        env_empty["GHW_ONEBOT_GROUP"] = ""
        env_empty["GHW_ONEBOT_KINDS"] = ""
        proc = start(custom_env=env_empty)
        check("置空配置后服务仍能就绪", wait_ready(base, proc))
        status, health = api_get(base, "/api/health")
        cfg = health.get("config", {})
        check("GHW_OWNERS 置空 = 不限仓库", cfg.get("owners") == ["*"], json.dumps(cfg.get("owners")))
        check("GHW_ONEBOT_GROUP 置空 = 关闭推群", cfg.get("onebot", {}).get("enabled") is False,
              json.dumps(cfg.get("onebot")))
        before = len(onebot.snapshot())
        status, resp = post_webhook(base, "issues", issue_payload(owner="anyone-at-all", repo="y"), "d-11")
        check("不限 owner 时白名单外的仓库也被接收", status == 202 and resp.get("kind") == "Issue",
              f"{status} {resp}")
        time.sleep(1.5)
        check("关闭推群后不再有群消息", len(onebot.snapshot()) == before,
              json.dumps(onebot.snapshot()[before:])[:200])
        if os.name == "nt":
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        try:
            code3 = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            code3 = -1
        check("第三个进程也能优雅退出", code3 == 0, f"code={code3}")

        # 16. --env-file：只填空缺、真实环境变量优先
        env_nofile = {k: v for k, v in env.items()
                      if k not in ("GHW_WEBHOOK_SECRET", "GHW_HEARTBEAT", "GHW_MAX_MSG_LEN")}
        env_nofile["GHW_MAX_MSG_LEN"] = "2222"          # 环境变量里有 → 应盖过 .env 的 1111
        dotenv_dir = os.path.join(tmp, "dotenv")
        os.makedirs(dotenv_dir, exist_ok=True)
        dotenv_path = os.path.join(dotenv_dir, "custom.env")
        with open(dotenv_path, "w", encoding="utf-8") as f:
            f.write("# 注释行\n"
                    "GHW_WEBHOOK_SECRET=secret-from-dotenv\n"
                    "GHW_HEARTBEAT=2\n"
                    "GHW_MAX_MSG_LEN=1111\n"
                    "\n"
                    "GHW_OWNERS=furryaxw\n")
        proc = start(custom_env=env_nofile, extra=("--env-file", dotenv_path))
        check("指定 --env-file 后服务就绪", wait_ready(base, proc))
        status, health = api_get(base, "/api/health")
        cfg = health.get("config", {})
        check("环境变量优先于 .env（GHW_MAX_MSG_LEN）", cfg.get("max_msg_len") == 2222,
              json.dumps(cfg.get("max_msg_len")))
        check(".env 补齐环境变量没有的键（GHW_HEARTBEAT）", cfg.get("heartbeat_s") == 2,
              json.dumps(cfg.get("heartbeat_s")))
        status, resp = post_webhook(base, "issues", issue_payload(), "d-12", secret="secret-from-dotenv")
        check("webhook secret 取自 .env 时验签通过", status == 202, f"{status} {resp}")
        status, resp = post_webhook(base, "issues", issue_payload(), "d-12b", secret=SECRET)
        check("用旧 secret 签名被拒", status == 401, f"{status} {resp}")
        if os.name == "nt":
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        try:
            code4 = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            code4 = -1
        check("--env-file 进程优雅退出", code4 == 0, f"code={code4}")

        # 17. 自动读取脚本同目录的 .env（把脚本复制到独立目录，避免用到仓库那份额外的 .env）
        auto_dir = os.path.join(tmp, "auto")
        os.makedirs(auto_dir, exist_ok=True)
        auto_script = os.path.join(auto_dir, "gh-webhook-watcher.py")
        shutil.copy(SCRIPT, auto_script)
        auto_port = free_port()
        auto_base = f"http://127.0.0.1:{auto_port}"
        with open(os.path.join(auto_dir, ".env"), "w", encoding="utf-8") as f:
            f.write(f"GHW_PORT={auto_port}\n"
                    f"GHW_LOG_FILE={os.path.join(auto_dir, 'auto.log')}\n"
                    f"GHW_STATE_FILE={os.path.join(auto_dir, 'auto-state.json')}\n"
                    "GHW_WEBHOOK_SECRET=secret-from-dotenv\n"
                    "GHW_OWNERS=furryaxw\n"
                    "GHW_ONEBOT_GROUP=\n")
        env_clean = {k: v for k, v in os.environ.items() if not k.startswith("GHW_")}
        proc = start(custom_env=env_clean, script=auto_script, cwd=auto_dir, extra=())
        check("同目录 .env 被自动读取", wait_ready(auto_base, proc))
        status, health = api_get(auto_base, "/api/health", token=None)
        cfg = health.get("config", {})
        check("自动读取的 .env 生效（端口/owner/不推群）",
              health.get("config", {}).get("listen") == f"0.0.0.0:{auto_port}"
              and cfg.get("owners") == ["furryaxw"]
              and cfg.get("onebot", {}).get("enabled") is False
              and str(cfg.get("env_file", "")).endswith(".env"),
              json.dumps({"listen": cfg.get("listen"), "owners": cfg.get("owners"),
                          "onebot": cfg.get("onebot", {}).get("enabled"), "env_file": cfg.get("env_file")}))
        if os.name == "nt":
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        try:
            code5 = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            code5 = -1
        check("自动 .env 进程优雅退出", code5 == 0, f"code={code5}")
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
        onebot.stop()
        shutil.rmtree(tmp, ignore_errors=True)

    failed = [n for n, ok, _ in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} 通过")
    if failed:
        print("失败项：" + ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
