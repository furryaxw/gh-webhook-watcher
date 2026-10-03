# gh-webhook-watcher

GitHub 事件监听服务：GitHub 通过 webhook 主动把 issue / PR / release / push 等事件推过来，服务
在收到后做验签、仓库过滤、去重、规范化，然后同时做两件事——

1. **推 OneBot 群**：沿用 `send_group_msg`，攒批、失败重试。
2. **开放 HTTP API**：其他服务用 SSE 实时订阅，或用游标轮询补拉。

常驻进程，不是定时任务；单文件、只用 Python 标准库（3.8+），不需要 `gh` CLI。

## 文件

| 文件 | 作用 |
| --- | --- |
| `gh-webhook-watcher.py` | 服务本体 |
| `.env` | 实际配置（含密钥，已被 `.gitignore` 排除，不要提交） |
| `.gitignore` | 排除 `.env`、日志、序号文件 |
| `test_gh_webhook_watcher.py` | 端到端自测（真起服务、真发签名请求，55 项断言） |
| `LICENSE` | MIT |
| `README.md` | 本文档 |

## 快速开始

配置都在同目录的 `.env` 里（host / port / secret / API 令牌 / owner 白名单 / OneBot 目标），
直接启动即可，服务会自动读取：

```powershell
python gh-webhook-watcher.py --verbose
```

改配置有两种方式：

1. 编辑 `.env`（推荐，改完重启服务）；
2. 临时覆盖：命令行参数 > 真实环境变量 > `.env` > 内置默认值。所以在 MCSM / systemd 里注入的
   `GHW_*` 变量会盖过 `.env`，适合"只改这一台机器"的场景。

```powershell
python gh-webhook-watcher.py --port 9000 --no-onebot      # 只临时改这两项
$env:GHW_VERBOSE = "1"; python gh-webhook-watcher.py      # 环境变量同样优先于 .env
python gh-webhook-watcher.py --env-file D:\other\.env     # 换一份配置文件
python gh-webhook-watcher.py --no-env-file                # 完全不用 .env
```

⚠️ Windows PowerShell 里 `$env:X = ""` 是**删除**变量（见「边界与注意」），要"设成空"请用
`[Environment]::SetEnvironmentVariable('GHW_OWNERS','','Process')`。

先自检配置再启动：

```powershell
python gh-webhook-watcher.py --print-config      # 密钥只显示 set(len=N)，并报告 .env 读了多少键
python test_gh_webhook_watcher.py                # 55/55 通过即环境 OK
```

## GitHub 侧配置

在仓库（或 organization）的 **Settings → Webhooks → Add webhook**：

- **Payload URL**：`http(s)://<你的地址>:25056/webhook/github`
- **Content type**：`application/json`
- **Secret**：填 `.env` 里 `GHW_WEBHOOK_SECRET` 那一串（48 位十六进制）
- **SSL verification**：走 https 时保持开启
- **Which events**：按需勾选，建议 `Issues`、`Pull requests`、`Releases`、`Pushes`、`Repositories`；
  想收 star / fork / 评论就再勾 `Stars`、`Forks`、`Issue comments`
- 保存后 GitHub 会立刻发一个 `ping`，服务返回 `pong` 并在日志留一行 `webhook ping ok`

多仓库/多 owner 时给每个仓库都配一次；服务侧用 `GHW_OWNERS` 再兜一层白名单。
本机调试可以用内网穿透（`cloudflared` / `frp` / `ngrok`）把 25056 暴露出去，GitHub 的
**Recent Deliveries** 里能直接看请求与响应，也能一键 Redeliver。

## 对外 API

配置了 `GHW_API_TOKEN` 时，所有 `/api/*` 都要带令牌（webhook 入口走签名，不用令牌）：

```
Authorization: Bearer <token>       # 推荐
X-Api-Token: <token>
?token=<token>                      # 浏览器 EventSource 无法自定义头时的退路
```

### 1. SSE 实时订阅（推荐）

```bash
curl -N "http://127.0.0.1:25056/api/stream?kind=Issue,PR,Release" \
     -H "Authorization: Bearer $GHW_API_TOKEN"
```

服务端每收到一条事件就推一帧：

```
retry: 3000

id: 12
data: {"seq":12,"id":"a1b2...","received_at":"2026-01-01T00:00:00Z","type":"issues","action":"opened",
       "kind":"Issue","owner":"furryaxw","repo":"demo","full_name":"furryaxw/demo","actor":"someone",
       "number":7,"title":"...","url":"https://github.com/furryaxw/demo/issues/7","detail":{...},
       "text":"[Issue] furryaxw/demo #7 ...\nhttps://github.com/furryaxw/demo/issues/7"}
```

- `id` 就是 `seq`，客户端断线重连时 SSE 客户端会自动带 `Last-Event-ID`，服务端把漏掉的事件补发；
  也可以手动 `?since=<seq>`。
- 空闲时每 `GHW_HEARTBEAT` 秒发一行注释心跳（`: heartbeat ...`），保持连接不被中间设备掐断。
- 过滤参数：`repo`（`owner` 或 `owner/repo`）、`type`（GitHub 事件名，如 `push,issues`）、
  `kind`（归一化类型，如 `PR`），都支持逗号分隔多值、大小写不敏感。

### 2. 轮询（游标式，适合不能长连接的服务）

```bash
curl -s "http://127.0.0.1:25056/api/events?since=0&limit=100&repo=furryaxw/demo" \
     -H "Authorization: Bearer $GHW_API_TOKEN"
```

```json
{"events":[...],"count":1,"cursor":12,"latest_seq":12,"more":false,
 "buffer_min_seq":1,"since_evicted":false,"filters":{"repo":["furryaxw/demo"],"type":[],"kind":[]}}
```

消费方式：保存 `cursor`，下次带 `since=<cursor>`；`more` 为 `true` 就立刻再拉一页。
`since_evicted` 为 `true` 表示传进来的游标早于缓冲窗口（缓冲只留最近 `GHW_EVENT_BUFFER` 条），
可能已经有事件被挤掉——此时应做一次全量对账，而不是继续按游标前进。

**`seq` 跨重启单调递增**（存 `GHW_STATE_FILE`），所以游标可以长期持久化；重启后缓冲是空的，
但新事件的序号会接着往上走，不会与旧游标撞车。

消费端示例（Python，标准库，断线自动重连）：

```python
import json, time, urllib.request

BASE, TOKEN, cursor = "http://127.0.0.1:25056", "your-token", 0
while True:
    try:
        req = urllib.request.Request(f"{BASE}/api/events?since={cursor}&limit=200",
                                     headers={"Authorization": f"Bearer {TOKEN}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            page = json.load(r)
        for ev in page["events"]:
            print(ev["kind"], ev["full_name"], ev["title"], ev["url"])
        cursor = page["cursor"]
        if not page["more"]:
            time.sleep(5)
    except Exception as e:
        print("retry after error:", e)
        time.sleep(5)
```

### 3. 健康检查

`GET /api/health` 返回生效配置（密钥打码）、计数器、缓冲区/订阅者数量、群推送统计。
适合给监控探活，例如直接看 `stats.counters.bad_signature` 是否异常增长。

### 4. 接口索引

`GET /` 返回纯文本的接口清单。

## 事件字段

| 字段 | 说明 |
| --- | --- |
| `seq` | 服务端单调递增序号，也是 SSE 的 `id`，跨重启持久化 |
| `id` | GitHub 的 `X-GitHub-Delivery`，同一投递重放会被去重 |
| `received_at` | 服务端接收时间（UTC ISO8601） |
| `type` / `action` | GitHub 原始事件名与动作，如 `issues` / `opened` |
| `kind` | 归一化类型，见下表 |
| `owner` / `repo` / `full_name` | 仓库定位，`repo` 过滤参数就按这三者匹配 |
| `actor` | 触发者（payload 的 `sender.login`） |
| `number` / `title` / `url` | 编号 / 标题 / 链接；`url` 为空时回落到仓库主页 |
| `detail` | 类型相关补充（标签、分支、commit 列表、是否预发布等） |
| `text` | 已排好版的推送文本，可直接贴到聊天窗口 |
| `payload` | GitHub 原始 payload，仅当开启 `GHW_INCLUDE_PAYLOAD` 时附带 |

`kind` 映射：

| GitHub 事件 | kind |
| --- | --- |
| `issues` | `Issue` |
| `pull_request` | `PR` |
| `release` | `Release`（`draft` 草稿直接跳过，不广播） |
| `push` | `Push` |
| `repository`（`created`） | `NewRepo` |
| `repository`（其他动作） | `Repo` |
| `issue_comment` | `Comment` |
| `star` / `fork` | `Star` / `Fork` |
| `create` / `delete` | `Branch` / `Tag` / `Ref` |
| `workflow_run` | `CI` |
| 其他 | `Other`（标题取事件名） |
| `ping` | 不入库，只回 `pong` |

## 群推送（OneBot）

- 默认只推 `Issue,PR,Release,Push,NewRepo`（`GHW_ONEBOT_KINDS` 可改，置空为全推）；
  star、评论、CI 之类只走 API，不刷群。
- 事件按到达顺序攒批，0.4 秒窗口内尽量合并成一条，单条不超过 `GHW_MAX_MSG_LEN`。
- 发送失败重试 3 次（1s / 2s 退避），仍失败记 ERROR 并丢弃；**只影响群消息，API 与缓冲不受影响**。
- `GHW_ONEBOT_GROUP` 置空或 `--no-onebot` 可整体关掉推群，只当纯 API 服务用；本地演练时请直接用
  `--no-onebot`（或 `GHW_ONEBOT_DISABLED=1`），启动日志会打印 `group push disabled` 或目标群号，
  先确认再发事件。
- ⚠️ **Windows PowerShell 的坑**：`$env:GHW_ONEBOT_GROUP = ""` 等于**删除**该变量（`$env:X = $null`
  同理），服务会把它当成「没设置」并回落到内置默认值——本仓库默认群号是空的（＝不推群），
  所以这里只会表现为"悄悄改回默认配置"，而不是你以为的"显式关掉"。
  确实要设成空值就写 `[Environment]::SetEnvironmentVariable('GHW_ONEBOT_GROUP','','Process')`；
  要关推群始终用 `--no-onebot`。
- 启动日志那行 `group push enabled -> http://... group=<群号>` 或 `group push disabled` 是唯一权威的确认，
  演练前先看一眼。
- `--verbose` 下每次成功发送记一行 `group push ok (N bytes)`，可用来核对投递。

## 配置项

命令行参数与 `GHW_*` 环境变量同名同义。优先级：命令行参数 > 真实环境变量 > `.env` > 下表默认值。
`.env` 里的键只填「环境变量没有的」，所以想临时压过 `.env` 就在启动环境里注入同名变量。

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `GHW_HOST` | `0.0.0.0` | 监听地址 |
| `GHW_PORT` | `25056` | 监听端口 |
| `GHW_WEBHOOK_PATH` | `/webhook/github` | webhook 路径 |
| `GHW_WEBHOOK_SECRET` | 无（必填） | 验签密钥；不配会拒绝启动 |
| `GHW_OWNERS` | 空（不限） | 只收这些 owner 的事件；置空或 `*` 表示不限 |
| `GHW_API_TOKEN` | 空 | 设置后 `/api/*` 需要令牌 |
| `GHW_ONEBOT_URL` | `http://127.0.0.1:3000` | OneBot 反向上端 |
| `GHW_ONEBOT_GROUP` | 空（不推群） | 群号；置空表示不推群 |
| `GHW_ONEBOT_KINDS` | `Issue,PR,Release,Push,NewRepo` | 推群类型；置空表示全推 |
| `GHW_ONEBOT_DISABLED` | `0` | 置 `1` 完全关闭推群 |
| `GHW_EVENT_BUFFER` | `500` | 环形缓冲保留的事件条数（补拉窗口） |
| `GHW_SUBSCRIBER_QUEUE` | `2000` | 单个 SSE 订阅者未消费事件的排队上限 |
| `GHW_DELIVERY_MEMORY` | `2000` | delivery id 去重记忆条数 |
| `GHW_HEARTBEAT` | `15` | SSE 心跳间隔秒 |
| `GHW_MAX_MSG_LEN` | `3000` | 单条群消息长度上限 |
| `GHW_LOG_FILE` | `<脚本目录>/gh-webhook-watcher.log` | 日志文件，超过 2MB 轮转为 `.1` |
| `GHW_STATE_FILE` | `<脚本目录>/gh-webhook-watcher-state.json` | 持久化最新 seq |
| `GHW_INCLUDE_PAYLOAD` | `0` | `1` 时事件附带 GitHub 原始 payload（体积大） |
| `GHW_ALLOW_UNSIGNED` | `0` | `1` 时无 secret 也收 webhook，**仅供隔离内网调试** |
| `GHW_VERBOSE` | `0` | `1` 输出请求级、订阅级 DEBUG 日志 |

## 部署

**MCSM**：作为常驻进程启动（不是计划任务），典型参数：

```
python /path/to/gh-webhook-watcher.py
```

`.env` 与脚本同目录即可生效，MCSM 里不用再填一遍；确需覆盖（例如换端口、临时关推群）就在实例的
环境变量里注入同名 `GHW_*`，它会盖过 `.env`。日志同时进文件和 stdout，MCSM 终端能直接看到。

**systemd**（`.env` 放在 `WorkingDirectory` 下，或用 `EnvironmentFile=` 显式指定）：

```ini
[Unit]
Description=GitHub webhook watcher
After=network-online.target

[Service]
WorkingDirectory=/opt/gh-webhook-watcher
EnvironmentFile=/opt/gh-webhook-watcher/.env
Environment=GHW_STATE_FILE=/var/lib/gh-webhook-watcher/state.json
Environment=GHW_LOG_FILE=/var/log/gh-webhook-watcher/watcher.log
ExecStart=/usr/bin/python3 /opt/gh-webhook-watcher/gh-webhook-watcher.py
Restart=always
User=ghw

[Install]
WantedBy=multi-user.target
```

注意 `EnvironmentFile=` 与内置 `.env` 读取会叠加，两者都写同名键时以 `EnvironmentFile=` 为准
（它先进进程环境），排查时看启动日志里 `env file ... loaded=N kept_from_environment=M` 这行。

**对外暴露**：服务本身不提供 TLS，请放在反向代理后面，并且代理必须关闭 SSE 缓冲：

```nginx
location / {
    proxy_pass http://127.0.0.1:25056;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;          # SSE 必须
    proxy_read_timeout 3600s;     # 长连接别被掐
}
```

## 日志

```
[2026-01-01T00:00:00.123456+00:00] [INFO] inst=a1b2c3d4 event seq=12 kind=Issue repo=furryaxw/demo action=opened delay=issues group_push=queued
```

`inst=` 是本次进程的实例号；`--verbose` 会追加 `http`、`sse subscribe/unsubscribe`、`poll` 等 DEBUG 行。
排错顺序：先看 `ready:` 有没有出现 → 再看 `webhook ping ok`（GitHub 侧通不通）→ 再看 `event seq=`（验签/过滤/去重是不是把事件挡了，对应计数器在 `/api/health`）。

## 边界与注意

- **停机期间不补发**：webhook 是即时投递，服务不在线时 GitHub 的投递会失败。要补救就在 GitHub
  的 Recent Deliveries 里 Redeliver；需要严格不丢事件的话，消费端应自行对 GitHub API 做一次对账。
- **补拉窗口有限**：事件只保留在内存环形缓冲（`GHW_EVENT_BUFFER` 条），轮询间隔过大且事件很多时
  会被挤掉；响应里的 `since_evicted` 会提示这种情况。
- **`seq` 持久、事件不持久**：重启后缓冲为空，序号从落盘值继续。
- **状态在单进程内存里**：多实例并行会各自记一份缓冲和 seq，消费端会看到重复；要横向扩展得先加共享存储。
- **secret 就是唯一门锁**：泄露等于别人可以伪造任意事件推送，务必随机生成；`--allow-unsigned` 只用于
  隔离内网的临时调试。
- **`/api/*` 默认无鉴权**：不配 `GHW_API_TOKEN` 时任何能连上端口的人都能读事件；暴露到内网以外前
  一定配上，并配合反代限制来源。
- **空值是明确语义**：`GHW_OWNERS` 置空＝不限仓库，`GHW_ONEBOT_GROUP` 置空＝不推群，
  `GHW_ONEBOT_KINDS` 置空＝全推。但 bash 能设空值、PowerShell 的 `$env:X=""` 只会删掉变量
  （见上文群推送一节的坑），所以关掉对外集成一律用显式开关 `--no-onebot` / `GHW_ONEBOT_DISABLED=1`。
  启动日志里那行 `group push enabled -> ... group=<群号>` 是唯一权威的确认。
- **`.env` 含明文密钥**：已进 `.gitignore`，别提交、别贴给别人；换机器时重新生成
  `GHW_WEBHOOK_SECRET`（GitHub 后台同步改）与 `GHW_API_TOKEN`。
- **`.env` 只兜底不覆盖**：启动环境里已有的同名 `GHW_*` 会赢。改了 `.env` 却不生效时，
  先看启动日志 `env file ... loaded=N kept_from_environment=M` 里的 `kept_from_environment`。

## 自测

```bash
python test_gh_webhook_watcher.py
```

覆盖：启动就绪、令牌鉴权、ping、错误签名拒绝、issue/PR/release/push 归一化、draft release 跳过、
delivery 去重、owner 白名单过滤、错误路径与缺头、轮询分页与三种过滤、SSE 实时与 `Last-Event-ID` 续传、
订阅回收、群推送（含类型过滤）、`/api/health` 计数口径、信号优雅退出、重启后 seq 连续性、
空值配置语义（不限 owner / 关闭推群且真的不发群消息）、`.env` 显式指定与同目录自动读取、
环境变量对 `.env` 的优先级、secret 取自 `.env` 时验签通过。

自测默认给主流程加 `--no-env-file`，所以仓库里那份真实 `.env` 不会影响断言结果。

## 许可

MIT，见 [LICENSE](LICENSE)。
