#!/usr/bin/env python3
"""GitHub webhook 监听服务：接收 GitHub 推送，转发 OneBot 群，并开放 HTTP API 给其他服务消费。

接口
  POST {webhook_path}   GitHub webhook 入口：HMAC-SHA256 验签 → 仓库白名单过滤 → delivery 去重
                        → 规范化事件 → 202 立即返回（群推送与订阅者扇出都是异步的）
  GET  /api/events      轮询拉取：游标 since=<seq>，支持 repo / type / kind 过滤
  GET  /api/stream      SSE 实时推送：支持 Last-Event-ID 断线续传与同一套过滤参数
  GET  /api/health      运行统计
  GET  /                纯文本接口索引

事件序号 seq 跨重启单调递增（<state_file> 持久化），消费者可以长期保存游标。

事件流向
  GitHub → POST webhook → 校验/过滤/去重 → 事件中心（环形缓冲 + 订阅者扇出）
                                          → OneBot 群消息（独立 worker，失败重试）
                                          → API 消费者（SSE 实时 / 轮询补拉）

配置优先级：命令行参数 > GHW_* 环境变量 > .env 文件 > 内置默认值，`--print-config` 打印生效配置（密钥打码）。
  .env 默认取脚本同目录，`--env-file` 指定别的路径，`--no-env-file` 完全忽略。

日志
  [ISO8601] [LEVEL] inst=<id> message，同时写文件和 stdout，超过 LOG_MAX_BYTES 轮转为 .1。
  --verbose 或 GHW_VERBOSE=1 追加请求级、订阅级 DEBUG 细节。
"""
import argparse
import hashlib
import hmac
import json
import os
import queue
import signal
import sys
import threading
import time
import traceback
import urllib.request
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

VERSION = "1.0.0"
INSTANCE_ID = uuid.uuid4().hex[:8]
MAX_BODY_BYTES = 25 * 1024 * 1024
LOG_MAX_BYTES = 2 * 1024 * 1024
STOP = threading.Event()

CFG = SimpleNamespace()
HUB = None
NOTIFIER = None
STATS = None


# --------------------------------------------------------------------------- 配置


def _env(name, default=""):
    v = os.environ.get(name)
    return default if v is None or v.strip() == "" else v.strip()


def _env_allow_empty(name, default=""):
    """区分「没设置」和「显式设成空」：这几个变量空值本身有含义（不限 owner / 不推群 / 全推）。"""
    v = os.environ.get(name)
    return default if v is None else v.strip()


def _env_int(name, default):
    raw = _env(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name, default=False):
    raw = _env(name, "").lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


class ConfigError(Exception):
    pass


ENV_FILE_INFO = {"path": "", "loaded": 0, "kept": 0, "ignored": 0, "error": ""}


def load_env_file(path):
    """.env 是兜底而不是覆盖源：只填真实环境变量没有提供的 GHW_* 键。

    支持 `KEY=VALUE`、`export KEY=VALUE`、`#` 整行注释、值两侧的引号、BOM 与 CRLF。
    值按字面取，所以行尾不要写注释。
    """
    if not path:
        return
    ENV_FILE_INFO["path"] = path
    try:
        with open(path, encoding="utf-8-sig") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return
    except Exception as e:
        ENV_FILE_INFO["error"] = str(e)
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not key:
            ENV_FILE_INFO["ignored"] += 1
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key in os.environ:
            ENV_FILE_INFO["kept"] += 1
            continue
        os.environ[key] = value
        ENV_FILE_INFO["loaded"] += 1


def _prescan_env_file(argv, script_dir):
    """在 argparse 之前先定出 .env 路径，否则默认值已经按空环境算完了。"""
    args = list(sys.argv[1:] if argv is None else argv)
    if "--no-env-file" in args:
        return None
    for i, a in enumerate(args):
        if a.startswith("--env-file="):
            return a.split("=", 1)[1]
        if a == "--env-file" and i + 1 < len(args):
            return args[i + 1]
    return os.path.join(script_dir, ".env")


def load_config(argv=None):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_file = _prescan_env_file(argv, script_dir)
    load_env_file(env_file)
    p = argparse.ArgumentParser(
        description="GitHub webhook 监听 + 事件 API 服务",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host", default=_env("GHW_HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=_env_int("GHW_PORT", 25056))
    p.add_argument("--webhook-path", default=_env("GHW_WEBHOOK_PATH", "/webhook/github"))
    p.add_argument("--secret", default=_env("GHW_WEBHOOK_SECRET", ""),
                   help="GitHub webhook secret，用于校验 X-Hub-Signature-256")
    p.add_argument("--owners", default=_env_allow_empty("GHW_OWNERS", ""),
                   help="只接收这些 owner 的事件；留空或 * 表示不限")
    p.add_argument("--api-token", default=_env("GHW_API_TOKEN", ""),
                   help="API 访问令牌；设置后 /api/* 必须带 Bearer 或 ?token=")
    p.add_argument("--onebot-url", default=_env("GHW_ONEBOT_URL", "http://127.0.0.1:3000"))
    p.add_argument("--onebot-group", default=_env_allow_empty("GHW_ONEBOT_GROUP", ""),
                   help="留空表示不推群")
    p.add_argument("--onebot-kinds", default=_env_allow_empty("GHW_ONEBOT_KINDS", "Issue,PR,Release,Push,NewRepo"),
                   help="推送到群的事件类型；空串表示全推")
    p.add_argument("--no-onebot", action="store_true", default=_env_bool("GHW_ONEBOT_DISABLED"))
    p.add_argument("--event-buffer", type=int, default=_env_int("GHW_EVENT_BUFFER", 500),
                   help="保留供补拉/续传的最新事件数")
    p.add_argument("--subscriber-queue", type=int, default=_env_int("GHW_SUBSCRIBER_QUEUE", 2000),
                   help="单个订阅者未消费事件的排队上限")
    p.add_argument("--delivery-memory", type=int, default=_env_int("GHW_DELIVERY_MEMORY", 2000),
                   help="delivery id 去重记忆条数")
    p.add_argument("--heartbeat", type=int, default=_env_int("GHW_HEARTBEAT", 15),
                   help="SSE 心跳间隔秒")
    p.add_argument("--max-msg-len", type=int, default=_env_int("GHW_MAX_MSG_LEN", 3000),
                   help="单条群消息长度上限")
    p.add_argument("--log-file", default=_env("GHW_LOG_FILE", os.path.join(script_dir, "gh-webhook-watcher.log")))
    p.add_argument("--state-file", default=_env("GHW_STATE_FILE", os.path.join(script_dir, "gh-webhook-watcher-state.json")),
                   help="持久化最新 seq，重启后事件序号继续单调递增")
    p.add_argument("--include-payload", action="store_true", default=_env_bool("GHW_INCLUDE_PAYLOAD"),
                   help="在事件里附带 GitHub 原始 payload（体积大，按需开启）")
    p.add_argument("--allow-unsigned", action="store_true", default=_env_bool("GHW_ALLOW_UNSIGNED"),
                   help="未配置 secret 时也接受 webhook（不要暴露到公网）")
    p.add_argument("--verbose", action="store_true", default=_env_bool("GHW_VERBOSE"))
    p.add_argument("--env-file", default=env_file or "", help="配置文件路径，默认脚本同目录的 .env")
    p.add_argument("--no-env-file", action="store_true", default=env_file is None,
                   help="完全忽略 .env，只用环境变量与命令行参数")
    p.add_argument("--print-config", action="store_true", help="打印生效配置后退出")
    a = p.parse_args(argv)

    wp = a.webhook_path.strip()
    if not wp.strip("/"):
        raise ConfigError("--webhook-path 不能为空")
    if not 1 <= a.port <= 65535:
        raise ConfigError(f"--port 超出范围: {a.port}")
    if a.heartbeat < 1:
        raise ConfigError("--heartbeat 至少为 1")
    if a.event_buffer < 1 or a.subscriber_queue < 1 or a.delivery_memory < 1:
        raise ConfigError("缓冲/队列/去重参数必须为正整数")
    if not a.secret and not a.allow_unsigned:
        raise ConfigError(
            "未配置 webhook secret：请设置 --secret / GHW_WEBHOOK_SECRET，"
            "或显式使用 --allow-unsigned 关闭验签"
        )
    if a.owners.strip() in ("", "*"):
        owners = []
    else:
        owners = sorted({o.strip().lower() for o in a.owners.split(",") if o.strip()})

    CFG.host = a.host
    CFG.port = a.port
    CFG.webhook_path = "/" + wp.strip("/")
    CFG.secret = a.secret
    CFG.owners = owners
    CFG.api_token = a.api_token
    CFG.onebot_url = a.onebot_url.rstrip("/")
    grp = a.onebot_group.strip()
    # OneBot v11 的 group_id 是 int64，纯数字时按整数发送
    CFG.onebot_group = int(grp) if grp.lstrip("-").isdigit() else grp
    CFG.onebot_kinds = {k.strip() for k in a.onebot_kinds.split(",") if k.strip()}
    CFG.onebot_enabled = (not a.no_onebot) and bool(a.onebot_group)
    CFG.event_buffer = a.event_buffer
    CFG.subscriber_queue = a.subscriber_queue
    CFG.delivery_memory = a.delivery_memory
    CFG.heartbeat = a.heartbeat
    CFG.max_msg_len = a.max_msg_len
    CFG.log_file = a.log_file
    CFG.state_file = a.state_file
    CFG.include_payload = a.include_payload
    CFG.allow_unsigned = a.allow_unsigned
    CFG.verbose = a.verbose
    CFG.print_config = a.print_config
    return CFG


def config_summary():
    mask = lambda v: f"set(len={len(v)})" if v else "unset"  # noqa: E731
    return {
        "version": VERSION,
        "instance": INSTANCE_ID,
        "listen": f"{CFG.host}:{CFG.port}",
        "webhook_path": CFG.webhook_path,
        "webhook_secret": mask(CFG.secret),
        "allow_unsigned": CFG.allow_unsigned,
        "owners": CFG.owners or ["*"],
        "api_token": mask(CFG.api_token),
        "onebot": {
            "enabled": CFG.onebot_enabled,
            "url": CFG.onebot_url,
            "group_id": CFG.onebot_group,
            "kinds": sorted(CFG.onebot_kinds) or ["*"],
        },
        "event_buffer": CFG.event_buffer,
        "subscriber_queue": CFG.subscriber_queue,
        "delivery_memory": CFG.delivery_memory,
        "heartbeat_s": CFG.heartbeat,
        "max_msg_len": CFG.max_msg_len,
        "include_payload": CFG.include_payload,
        "log_file": CFG.log_file,
        "state_file": CFG.state_file,
        "env_file": ENV_FILE_INFO["path"] or None,
        "env_file_keys": {"from_file": ENV_FILE_INFO["loaded"], "kept_from_environment": ENV_FILE_INFO["kept"]},
        "verbose": CFG.verbose,
    }


# --------------------------------------------------------------------------- 日志


def log(msg, level="INFO"):
    line = f"[{datetime.now(timezone.utc).isoformat()}] [{level}] inst={INSTANCE_ID} {msg}"
    try:
        d = os.path.dirname(os.path.abspath(CFG.log_file))
        if d:
            os.makedirs(d, exist_ok=True)
        with open(CFG.log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line, flush=True)


def vlog(msg):
    if CFG.verbose:
        log(msg, level="DEBUG")


def rotate_log():
    try:
        if os.path.getsize(CFG.log_file) > LOG_MAX_BYTES:
            os.replace(CFG.log_file, CFG.log_file + ".1")
            log(f"log rotated -> {CFG.log_file}.1 (previous file exceeded {LOG_MAX_BYTES} bytes)")
    except OSError:
        pass


def load_state():
    """读回上次的 seq，使事件序号跨重启单调递增，消费者可以长期保存游标。"""
    try:
        with open(CFG.state_file, encoding="utf-8") as f:
            data = json.load(f)
        return max(int(data.get("last_seq") or 0), 0)
    except FileNotFoundError:
        return 0
    except Exception as e:
        log(f"state load failed ({e}), seq starts from 0", level="WARN")
        return 0


def save_state(seq):
    """原子替换写入，避免半截文件。"""
    tmp = CFG.state_file + ".tmp"
    try:
        d = os.path.dirname(os.path.abspath(CFG.state_file))
        if d:
            os.makedirs(d, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"last_seq": seq,
                       "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
                      f, ensure_ascii=False)
        os.replace(tmp, CFG.state_file)
    except Exception as e:
        log(f"state save failed: {e}", level="WARN")


class Stats:
    """线程安全的计数器。"""
    def __init__(self):
        self._lock = threading.Lock()
        self.started_wall = datetime.now(timezone.utc)
        self.started_mono = time.monotonic()
        self.counters = {
            "webhook_received": 0,
            "accepted": 0,
            "filtered": 0,
            "duplicate": 0,
            "skipped": 0,
            "ping": 0,
            "bad_signature": 0,
            "bad_request": 0,
            "subscriber_dropped": 0,
        }
        self.last_event_at = ""
        self.last_event_kind = ""

    def inc(self, key, n=1):
        with self._lock:
            self.counters[key] = self.counters.get(key, 0) + n

    def mark_event(self, ev):
        with self._lock:
            self.last_event_at = ev["received_at"]
            self.last_event_kind = ev["kind"]

    def snapshot(self):
        with self._lock:
            c = dict(self.counters)
            return {
                "uptime_s": round(time.monotonic() - self.started_mono, 1),
                "started_at": self.started_wall.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "last_event_at": self.last_event_at,
                "last_event_kind": self.last_event_kind,
                "counters": c,
            }


# --------------------------------------------------------------------------- 事件规范化

_MAPPERS = {}


def _mapper(event_type):
    def deco(fn):
        _MAPPERS[event_type] = fn
        return fn

    return deco


def _short(text, limit=200):
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _repo_of(payload):
    repo = payload.get("repository") or {}
    full = repo.get("full_name") or ""
    if full:
        owner, _, name = full.partition("/")
        return owner, name, repo
    org = (payload.get("organization") or {}).get("login", "")
    return org, "", repo


@_mapper("issues")
def _map_issue(ev, p):
    it = p.get("issue") or {}
    return {
        "kind": "Issue",
        "number": it.get("number", ""),
        "title": it.get("title", ""),
        "url": it.get("html_url", ""),
        "detail": {"state": it.get("state", ""),
                   "labels": [l.get("name") for l in (it.get("labels") or [])]},
    }


@_mapper("pull_request")
def _map_pr(ev, p):
    pr = p.get("pull_request") or {}
    return {
        "kind": "PR",
        "number": pr.get("number", ""),
        "title": pr.get("title", ""),
        "url": pr.get("html_url", ""),
        "detail": {"state": pr.get("state", ""), "draft": bool(pr.get("draft")),
                   "merged": bool(pr.get("merged")), "base": (pr.get("base") or {}).get("ref", "")},
    }


@_mapper("release")
def _map_release(ev, p):
    rel = p.get("release") or {}
    if rel.get("draft"):
        return None  # 草稿 release 不对外可见，不产生事件
    tag = rel.get("tag_name") or ""
    return {
        "kind": "Release",
        "number": tag,
        "title": rel.get("name") or tag,
        "url": rel.get("html_url", ""),
        "detail": {"prerelease": bool(rel.get("prerelease")), "target": rel.get("target_commitish", "")},
    }


@_mapper("push")
def _map_push(ev, p):
    ref = (p.get("ref") or "").replace("refs/heads/", "").replace("refs/tags/", "")
    commits = p.get("commits") or []
    n = len(commits)
    return {
        "kind": "Push",
        "number": "",
        "title": f"{ref} · {n} commit" + ("s" if n != 1 else ""),
        "url": p.get("compare") or (p.get("head_commit") or {}).get("url", ""),
        "detail": {"ref": ref, "commits": [_short((c.get("message") or "").splitlines()[0], 100)
                                            for c in commits[:10]],
                   "commits_total": n, "forced": bool(p.get("forced"))},
    }


@_mapper("repository")
def _map_repository(ev, p):
    repo = p.get("repository") or {}
    if p.get("action") == "created":
        return {"kind": "NewRepo", "number": "", "title": "仓库上线", "url": repo.get("html_url", ""),
                "detail": {"private": bool(repo.get("private"))}}
    return {"kind": "Repo", "number": "", "title": _short(repo.get("description") or "", 80),
            "url": repo.get("html_url", ""), "detail": {"visibility": repo.get("visibility", "")}}


@_mapper("issue_comment")
def _map_issue_comment(ev, p):
    c = p.get("comment") or {}
    it = p.get("issue") or {}
    return {"kind": "Comment", "number": it.get("number", ""), "title": _short(c.get("body") or ""),
            "url": c.get("html_url", ""), "detail": {"on": "PR" if it.get("pull_request") else "Issue"}}


@_mapper("star")
def _map_star(ev, p):
    return {"kind": "Star", "number": "", "title": "新 star", "url": "", "detail": {}}


@_mapper("fork")
def _map_fork(ev, p):
    fork = p.get("forkee") or {}
    return {"kind": "Fork", "number": "", "title": fork.get("full_name", ""),
            "url": fork.get("html_url", ""), "detail": {}}


def _map_ref(ev, p):
    ref_type = p.get("ref_type") or ""
    kind = "Tag" if ref_type == "tag" else "Branch" if ref_type == "branch" else "Ref"
    action = p.get("action") or ev.get("action") or ""
    return {"kind": kind, "number": "", "title": f"{p.get('ref') or ''} {action}".strip(),
            "url": "", "detail": {"ref_type": ref_type}}


_MAPPERS["create"] = _map_ref
_MAPPERS["delete"] = _map_ref


@_mapper("workflow_run")
def _map_workflow_run(ev, p):
    wr = p.get("workflow_run") or {}
    return {"kind": "CI", "number": wr.get("run_number", ""),
            "title": f"{wr.get('name') or 'workflow'} · {wr.get('conclusion') or wr.get('status') or ''}".strip(),
            "url": wr.get("html_url", ""),
            "detail": {"branch": wr.get("head_branch", ""), "event": wr.get("event", "")}}


def normalize_event(event_type, payload, delivery_id, received_at):
    """把 GitHub 原始 payload 收敛成统一结构；返回 None 表示该事件不应对外广播。"""
    owner, name, repo = _repo_of(payload)
    full = f"{owner}/{name}" if owner and name else (repo.get("full_name") or owner or "")
    ev = {
        "id": delivery_id,
        "seq": 0,
        "received_at": received_at,
        "type": event_type,
        "action": payload.get("action") or "",
        "kind": "Other",
        "owner": owner,
        "repo": name,
        "full_name": full,
        "actor": (payload.get("sender") or {}).get("login", ""),
        "number": "",
        "title": "",
        "url": repo.get("html_url", ""),
        "detail": {},
    }
    mapper = _MAPPERS.get(event_type)
    if mapper is not None:
        updates = mapper(ev, payload)
        if updates is None:
            return None
        ev.update(updates)
    else:
        ev["title"] = event_type
    if not ev["url"]:
        ev["url"] = repo.get("html_url", "")
    return ev


def format_event(ev):
    kind = ev["kind"]
    full = ev["full_name"] or "(unknown)"
    if kind == "Release":
        head = f"[Release] {full}"
        if ev["title"] and ev["title"] != ev["number"]:
            head += f" {ev['number']} — {ev['title']}"
        else:
            head += f" {ev['number']}"
    elif kind in ("Issue", "PR", "Comment"):
        head = f"[{kind}] {full} #{ev['number']} {ev['title']}"
    elif kind == "Push":
        head = f"[Push] {full} {ev['title']}"
    elif kind == "NewRepo":
        head = f"[NewRepo] {full} 仓库上线"
    else:
        head = f"[{kind}] {full}"
        if ev["number"]:
            head += f" #{ev['number']}"
        if ev["title"]:
            head += f" {ev['title']}"
        if ev["action"]:
            head += f" ({ev['action']})"
    url = ev.get("url") or f"https://github.com/{full}"
    return f"{head}\n{url}"


# --------------------------------------------------------------------------- 事件中心


def match_filters(filters, ev):
    if filters["repo"]:
        targets = {ev["full_name"].lower(), ev["owner"].lower(), ev["repo"].lower()}
        if not targets & set(filters["repo"]):
            return False
    if filters["type"] and ev["type"].lower() not in filters["type"]:
        return False
    if filters["kind"] and ev["kind"].lower() not in filters["kind"]:
        return False
    return True


def parse_filters(qs):
    def multi(name):
        out = []
        for raw in qs.get(name, []):
            out += [x.strip().lower() for x in raw.split(",") if x.strip()]
        return out

    return {"repo": multi("repo"), "type": multi("type"), "kind": multi("kind")}


class EventHub:
    """环形缓冲 + 订阅者扇出 + delivery 去重。"""

    def __init__(self, buffer_size, queue_max, delivery_memory):
        self._lock = threading.Lock()
        self._buf = deque(maxlen=buffer_size)
        self._subs = {}
        self._next_sub = 1
        self._seq = 0
        self._seen = OrderedDict()
        self.queue_max = queue_max
        self.delivery_memory = delivery_memory

    def restore_seq(self, seq):
        with self._lock:
            self._seq = max(self._seq, int(seq))

    def seen_delivery(self, delivery_id):
        with self._lock:
            if delivery_id in self._seen:
                return True
            self._seen[delivery_id] = True
            while len(self._seen) > self.delivery_memory:
                self._seen.popitem(last=False)
            return False

    def publish(self, ev):
        with self._lock:
            self._seq += 1
            ev["seq"] = self._seq
            self._buf.append(ev)
            subs = list(self._subs.items())
        for _sid, (q, filters) in subs:
            if not match_filters(filters, ev):
                continue
            try:
                q.put_nowait(ev)
            except queue.Full:
                STATS.inc("subscriber_dropped")
        return ev["seq"]

    def subscribe(self, filters, last_seq):
        q = queue.Queue(maxsize=self.queue_max)
        backlog = []
        with self._lock:
            sid = self._next_sub
            self._next_sub += 1
            if last_seq is not None:
                backlog = [e for e in self._buf if e["seq"] > last_seq and match_filters(filters, e)]
            self._subs[sid] = (q, filters)
        return sid, q, backlog

    def unsubscribe(self, sid):
        with self._lock:
            self._subs.pop(sid, None)

    def subscriber_count(self):
        with self._lock:
            return len(self._subs)

    def latest_seq(self):
        with self._lock:
            return self._seq

    def poll(self, filters, since, limit):
        with self._lock:
            picked = [e for e in self._buf if e["seq"] > since and match_filters(filters, e)]
            latest = self._seq
            buffered_min = self._buf[0]["seq"] if self._buf else 0
        page = picked[:limit]
        cursor = page[-1]["seq"] if page else since
        return {
            "events": page,
            "count": len(page),
            "cursor": cursor,
            "latest_seq": latest,
            "more": len(picked) > len(page),
            "buffer_min_seq": buffered_min,
            "since_evicted": buffered_min > since + 1,
        }

    def buffer_len(self):
        with self._lock:
            return len(self._buf)


# --------------------------------------------------------------------------- OneBot 推送


def send_group_msg(text):
    body = json.dumps({"group_id": CFG.onebot_group, "message": text}).encode()
    req = urllib.request.Request(
        CFG.onebot_url + "/send_group_msg",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            r = json.loads(resp.read())
        ok = r.get("status") == "ok" and r.get("retcode") == 0
        if not ok:
            log(f"send_group_msg retcode={r.get('retcode')} status={r.get('status')} msg={str(r)[:200]}",
                level="ERROR")
        return ok
    except Exception as e:
        log(f"send_group_msg exception: {e}", level="ERROR")
        return False


class OneBotNotifier(threading.Thread):
    """把事件攒成不超过 max_msg_len 的群消息，失败重试。"""

    def __init__(self):
        super().__init__(name="onebot-notifier", daemon=True)
        self.enabled = CFG.onebot_enabled
        self.kinds = CFG.onebot_kinds
        self.q = queue.Queue()
        self._pending = deque()
        self.sent = 0
        self.failed = 0
        self.retried = 0
        self.last_error = ""

    def submit(self, ev):
        if not self.enabled:
            return False
        if self.kinds and ev["kind"] not in self.kinds:
            return False
        self.q.put(ev)
        return True

    def _next_event(self, timeout):
        if self._pending:
            return self._pending.popleft()
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return None

    def _send_with_retry(self, text):
        for attempt in range(1, 4):
            if STOP.is_set():
                return
            if send_group_msg(text):
                self.sent += 1
                vlog(f"group push ok ({len(text)} bytes)")
                return
            self.retried += 1
            self.last_error = f"attempt {attempt} failed at {datetime.now(timezone.utc).isoformat()}"
            if attempt < 3:
                time.sleep(attempt)
        self.failed += 1
        log(f"group push failed after 3 attempts ({len(text)} bytes dropped)", level="ERROR")

    def run(self):
        if not self.enabled:
            log("group push disabled (group id 为空或 --no-onebot)：事件只走 API")
            return
        log(f"group push enabled -> {CFG.onebot_url} group={CFG.onebot_group} kinds={sorted(self.kinds) or ['*']}")
        while not STOP.is_set():
            ev = self._next_event(0.5)
            if ev is None:
                continue
            lines = []
            total = 0
            while True:
                line = format_event(ev)
                if lines and total + len(line) + 1 > CFG.max_msg_len:
                    self._pending.append(ev)
                    break
                lines.append(line)
                total += len(line) + 1
                if total >= CFG.max_msg_len:
                    break
                budget = CFG.max_msg_len - total
                ev = self._next_event(0.4 if budget > 0 else 0.0)
                if ev is None:
                    break
            if lines:
                self._send_with_retry("\n".join(lines))

    def stats(self):
        return {
            "enabled": self.enabled,
            "queued": self.q.qsize() + len(self._pending),
            "sent": self.sent,
            "failed": self.failed,
            "retried": self.retried,
            "last_error": self.last_error,
            "kinds": sorted(self.kinds) or ["*"],
        }


# --------------------------------------------------------------------------- HTTP


def verify_signature(secret, body, header_value):
    if not header_value:
        return False
    algo, _, hexsig = header_value.partition("=")
    algo = algo.strip().lower()
    if algo not in ("sha256", "sha1") or not hexsig:
        return False
    digest = hashlib.sha256 if algo == "sha256" else hashlib.sha1
    expected = hmac.new(secret.encode("utf-8"), body, digest).hexdigest()
    return hmac.compare_digest(expected, hexsig.strip().lower())


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = f"gh-webhook-watcher/{VERSION}"

    # -- 基础工具 -------------------------------------------------------------

    def log_message(self, fmt, *args):
        vlog(f"http {self.address_string()} {fmt % args}")

    def log_error(self, fmt, *args):
        vlog(f"http-error {self.address_string()} {fmt % args}")

    def _send(self, code, body, ctype="application/json; charset=utf-8", extra=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Instance-Id", INSTANCE_ID)
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _json(self, code, obj):
        self._send(code, json.dumps(obj, ensure_ascii=False))

    def _text(self, code, text, ctype="text/plain; charset=utf-8"):
        self._send(code, text, ctype=ctype)

    def _qs(self):
        return parse_qs(urlparse(self.path).query, keep_blank_values=True)

    def _check_api_auth(self, qs):
        if not CFG.api_token:
            return True
        header = self.headers.get("Authorization", "")
        token = ""
        if header.lower().startswith("bearer "):
            token = header[7:].strip()
        elif self.headers.get("X-Api-Token"):
            token = self.headers.get("X-Api-Token").strip()
        elif qs.get("token"):
            token = qs["token"][0].strip()
        if token and hmac.compare_digest(token, CFG.api_token):
            return True
        log(f"api auth rejected path={urlparse(self.path).path} from={self.address_string()}", level="WARN")
        self._json(401, {"error": "unauthorized", "hint": "Authorization: Bearer <token> 或 ?token=<token>"})
        return False

    # -- POST：webhook --------------------------------------------------------

    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/") or "/"
        if path != CFG.webhook_path.rstrip("/"):
            STATS.inc("bad_request")
            return self._json(404, {"error": "not_found", "webhook_path": CFG.webhook_path})
        try:
            self._handle_webhook()
        except Exception:
            STATS.inc("bad_request")
            log("webhook handler crashed:\n" + traceback.format_exc(), level="ERROR")
            self._json(500, {"error": "internal"})

    def _handle_webhook(self):
        STATS.inc("webhook_received")
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            STATS.inc("bad_request")
            return self._json(411, {"error": "length_required"})
        if length > MAX_BODY_BYTES:
            STATS.inc("bad_request")
            return self._json(413, {"error": "payload_too_large", "limit": MAX_BODY_BYTES})
        body = self.rfile.read(length)

        if CFG.secret:
            sig = self.headers.get("X-Hub-Signature-256") or self.headers.get("X-Hub-Signature") or ""
            if not verify_signature(CFG.secret, body, sig):
                STATS.inc("bad_signature")
                log(f"webhook signature rejected from={self.address_string()} delivery="
                    f"{self.headers.get('X-GitHub-Delivery', '-')}", level="WARN")
                return self._json(401, {"error": "invalid_signature"})
        else:
            log("webhook accepted without signature (allow-unsigned)", level="WARN")

        event_type = (self.headers.get("X-GitHub-Event") or "").strip()
        if not event_type:
            STATS.inc("bad_request")
            return self._json(400, {"error": "missing_event_header"})
        delivery = (self.headers.get("X-GitHub-Delivery") or "").strip()
        if not delivery:
            delivery = "local-" + uuid.uuid4().hex[:16]
            log(f"webhook without X-GitHub-Delivery, using {delivery}", level="WARN")
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as e:
            STATS.inc("bad_request")
            return self._json(400, {"error": "invalid_json", "detail": str(e)[:200]})
        if not isinstance(payload, dict):
            STATS.inc("bad_request")
            return self._json(400, {"error": "payload_not_object"})

        if event_type == "ping":
            STATS.inc("ping")
            log(f"webhook ping ok zen={_short(payload.get('zen') or '', 40)} hook_id={payload.get('hook_id', '-')}")
            return self._json(200, {"ok": True, "msg": "pong", "instance": INSTANCE_ID})

        owner, name, repo = _repo_of(payload)
        if CFG.owners and owner.lower() not in CFG.owners:
            STATS.inc("filtered")
            vlog(f"filtered owner={owner or '-'} type={event_type} delivery={delivery}")
            return self._json(200, {"ok": True, "filtered": True, "reason": "owner_not_watched", "owner": owner})

        if HUB.seen_delivery(delivery):
            STATS.inc("duplicate")
            vlog(f"duplicate delivery ignored: {delivery}")
            return self._json(200, {"ok": True, "duplicate": True, "delivery": delivery})

        received_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        ev = normalize_event(event_type, payload, delivery, received_at)
        if ev is None:
            STATS.inc("skipped")
            return self._json(200, {"ok": True, "skipped": True, "reason": f"{event_type} not broadcastable"})

        ev["text"] = format_event(ev)
        if CFG.include_payload:
            ev["payload"] = payload
        HUB.publish(ev)
        save_state(ev["seq"])
        STATS.inc("accepted")
        STATS.mark_event(ev)
        pushed = NOTIFIER.submit(ev)
        log(f"event seq={ev['seq']} kind={ev['kind']} repo={ev['full_name']} action={ev['action'] or '-'} "
            f"delay={ev['type']} group_push={'queued' if pushed else 'skip'}")
        return self._json(202, {"ok": True, "seq": ev["seq"], "delivery": delivery, "kind": ev["kind"],
                                "type": event_type, "full_name": ev["full_name"]})

    # -- GET：API ------------------------------------------------------------

    def do_GET(self):
        url = urlparse(self.path)
        path = url.path.rstrip("/") or "/"
        qs = parse_qs(url.query, keep_blank_values=True)
        if path == "/":
            return self._index()
        if path == "/api/health":
            if not self._check_api_auth(qs):
                return
            return self._json(200, {"ok": True, "config": config_summary(), "stats": STATS.snapshot(),
                                    "hub": {"buffer_len": HUB.buffer_len(), "latest_seq": HUB.latest_seq(),
                                            "subscribers": HUB.subscriber_count()},
                                    "onebot": NOTIFIER.stats()})
        if path == "/api/events":
            if not self._check_api_auth(qs):
                return
            return self._events(qs)
        if path == "/api/stream":
            if not self._check_api_auth(qs):
                return
            return self._stream(qs)
        STATS.inc("bad_request")
        return self._json(404, {"error": "not_found", "index": "/"})

    def _index(self):
        webhook = CFG.webhook_path
        body = f"""gh-webhook-watcher {VERSION} (instance {INSTANCE_ID})

POST {webhook}
    GitHub webhook 入口（application/json + X-Hub-Signature-256 验签）

GET /api/events?since=<seq>&limit=100&repo=<owner|owner/repo>&type=push,issues&kind=PR
    轮询拉取新事件；since 不传按 0 处理（返回最近缓冲内最早的一页）
    响应字段：events / count / cursor / latest_seq / more / buffer_min_seq

GET /api/stream?repo=&type=&kind=
    SSE 实时推送；支持 Last-Event-ID 头或 ?since=<seq> 断线续传，每 {CFG.heartbeat}s 心跳

GET /api/health
    运行统计与生效配置

认证：配置了 api_token 时，/api/* 需带 "Authorization: Bearer <token>" 或 "?token=<token>"
"""
        self._text(200, body)

    def _events(self, qs):
        filters = parse_filters(qs)
        try:
            since = int(qs.get("since", ["0"])[0] or 0)
        except ValueError:
            since = 0
        try:
            limit = int(qs.get("limit", ["100"])[0] or 100)
        except ValueError:
            limit = 100
        limit = max(1, min(limit, 1000))
        result = HUB.poll(filters, since, limit)
        result["filters"] = filters
        vlog(f"poll since={since} limit={limit} filters={filters} -> {result['count']} event(s)")
        self._json(200, result)

    def _stream(self, qs):
        filters = parse_filters(qs)
        last_id = self.headers.get("Last-Event-ID") or qs.get("since", [""])[0]
        last_seq = None
        if last_id:
            try:
                last_seq = int(str(last_id).strip())
            except ValueError:
                last_seq = None
        sid, q, backlog = HUB.subscribe(filters, last_seq)
        log(f"sse subscribe sid={sid} from={self.address_string()} filters={filters} "
            f"last_event_id={last_seq if last_seq is not None else '-'} backlog={len(backlog)}")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-transform")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "close")
            self.send_header("X-Instance-Id", INSTANCE_ID)
            self.end_headers()
            self.close_connection = True
            self._sse_write(f"retry: 3000\n\n")
            self._sse_write(f": connected instance={INSTANCE_ID} ts={datetime.now(timezone.utc).isoformat()}\n\n")
            for ev in backlog:
                self._sse_event(ev)
            while not STOP.is_set():
                try:
                    ev = q.get(timeout=CFG.heartbeat)
                except queue.Empty:
                    self._sse_write(f": heartbeat {datetime.now(timezone.utc).isoformat()}\n\n")
                    continue
                self._sse_event(ev)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            vlog(f"sse sid={sid} disconnected: {e}")
        finally:
            HUB.unsubscribe(sid)
            log(f"sse unsubscribe sid={sid}")

    def _sse_write(self, chunk):
        self.wfile.write(chunk.encode("utf-8"))
        self.wfile.flush()

    def _sse_event(self, ev):
        data = json.dumps(ev, ensure_ascii=False)
        self._sse_write(f"id: {ev['seq']}\ndata: {data}\n\n")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


# --------------------------------------------------------------------------- 入口


def main(argv=None):
    global HUB, NOTIFIER, STATS
    try:
        load_config(argv)
    except ConfigError as e:
        print(f"配置错误: {e}", file=sys.stderr)
        return 2
    if CFG.print_config:
        print(json.dumps(config_summary(), ensure_ascii=False, indent=2))
        return 0

    rotate_log()
    STATS = Stats()
    HUB = EventHub(CFG.event_buffer, CFG.subscriber_queue, CFG.delivery_memory)
    restored = load_state()
    HUB.restore_seq(restored)
    NOTIFIER = OneBotNotifier()

    log(f"starting gh-webhook-watcher {VERSION} instance={INSTANCE_ID} "
        f"listen={CFG.host}:{CFG.port} webhook={CFG.webhook_path} "
        f"owners={CFG.owners or ['*']} api_auth={'on' if CFG.api_token else 'off'} "
        f"signature={'on' if CFG.secret else 'off'} last_seq={restored}")
    if ENV_FILE_INFO["path"]:
        extra = f" error={ENV_FILE_INFO['error']}" if ENV_FILE_INFO["error"] else ""
        log(f"env file {ENV_FILE_INFO['path']} loaded={ENV_FILE_INFO['loaded']} "
            f"kept_from_environment={ENV_FILE_INFO['kept']} ignored={ENV_FILE_INFO['ignored']}{extra}")
    NOTIFIER.start()

    try:
        httpd = Server((CFG.host, CFG.port), Handler)
    except OSError as e:
        log(f"listen {CFG.host}:{CFG.port} failed: {e}", level="ERROR")
        return 1

    def _shutdown(signum, _frame):
        log(f"signal {signum} received, shutting down")
        STOP.set()
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    sigs = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):  # Windows: Ctrl+Break / CTRL_BREAK_EVENT 也能优雅退出
        sigs.append(signal.SIGBREAK)
    for sig in sigs:
        try:
            signal.signal(sig, _shutdown)
        except (ValueError, OSError, AttributeError):
            pass

    log(f"ready: webhook=POST {CFG.webhook_path} api=/api/events,/api/stream,/api/health")
    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        STOP.set()
    finally:
        STOP.set()
        httpd.server_close()
        save_state(HUB.latest_seq())
        s = STATS.snapshot()
        log(f"stopped counters={json.dumps(s['counters'], ensure_ascii=False)} "
            f"onebot={json.dumps(NOTIFIER.stats(), ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        log("fatal:\n" + traceback.format_exc(), level="ERROR")
        sys.exit(1)
