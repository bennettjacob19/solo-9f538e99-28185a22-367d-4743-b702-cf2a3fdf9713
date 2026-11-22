# 100-continue HTTP/1.1 上传网关（Python + h11）

站点终端上传诊断包时，先等源站许可再走正文，避免慢链路上白传。本项目用
[h11](https://h11.readthedocs.io/) 状态机 + asyncio 实现一个 HTTP/1.1 上传网关，
并附带本地样例源站与 FastAPI 演示面板。

## 行为约定

每条 TCP 连接只处理一次带 `Content-Length` 与 `Expect: 100-continue` 的 POST：

```
终端                     网关                      源站
 │  POST + Expect 头      │                        │
 │───────────────────────>│  转发请求头（无正文）   │
 │                        │───────────────────────>│
 │                        │   ① 100 Continue        │
 │                        │<───────────────────────│
 │  100 Continue（许可）   │                        │
 │<───────────────────────│                        │
 │  正文分块               │  边收边转正文           │
 │───────────────────────>│───────────────────────>│
 │                        │   最终响应 200/...      │
 │  最终响应               │<───────────────────────│
 │<───────────────────────│                        │
```

* **源站放行（100）**：网关把 100 转给终端，随后边收边转正文，再转回最终响应。
* **源站许可前直接拒收（最终响应）**：网关原样转回最终响应，**正文一字不转**。
* **等许可超时**：网关自己回 `504 Gateway Timeout`，**正文一字不转**。
* **源站中途拒收**：网关**识别到最终响应头的那一刻**就停止向源站写入新的
  上传数据；此后只要拒收正文各段都在读空闲时限内到达，就把原状态码和完整
  正文转回终端——**即使终端已经停发**。源站按分块/关闭定界发的响应，
  网关向终端侧重新加 `Transfer-Encoding: chunked`，保证消息边界明确。
* 连接处理完即关，不做 keep-alive 复用；非法请求（非 POST、缺 Expect、
  缺/错 Content-Length、用 Transfer-Encoding）直接回 4xx。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `app/h11io.py` | h11 与 asyncio 流的粘合层：事件收发、逐跳头过滤、简单响应 |
| `app/gateway.py` | 上传网关核心：请求校验、许可等待、正文边收边转、超时/拒收处理 |
| `app/origin.py` | 样例源站：`/accept` `/reject` `/reject-mid` `/reject-slow` `/hang` + 字节统计 `/stats` |
| `app/rawclient.py` | h11 试传客户端：严格”先许可后正文”，记录毫秒级事件时间线 |
| `app/demo.py` | FastAPI 面板（`/docs`），调用试传客户端并返回时间线与源站计数 |
| `app/config.py` | 环境变量配置 |
| `tests/` | pytest 端到端测试（进程内起服务，10 个用例） |
| `scripts/selftest.py` | 交付前自测脚本（正常上传 + 拒收 + 慢速 413 + 写入阻塞等 8 个场景） |
| `Dockerfile` / `docker-compose.yml` | 容器化，compose 一键起三个服务 |

## 快速开始

### docker compose（推荐）

```bash
docker compose up --build
```

| 服务 | 地址 | 用途 |
| --- | --- | --- |
| origin（样例源站） | `:8081` | 接收/拒收上传，统计实收字节 |
| gateway（上传网关） | `:8080` | 终端实际连接的网关 |
| demo（FastAPI） | `:8000` | 浏览器打开 <http://localhost:8000/docs> |

在 `/docs` 里调 `POST /demo/upload`，`mode` 可选：

* `accept` —— 正常放行；
* `reject` —— 源站看头后直接 403（许可前拒收）；
* `reject-mid` —— 源站收 4096 字节后回 413（中途拒收，默认 body 512KB，
  可观察到终端未发完即中止）；
* `reject-slow` —— 源站收 4096 字节后回 413，且拒收说明按 chunked
  **分 4 段慢发**（段间隔 0.4s）：可观察到网关识别响应头后即停写、
  终端停发后仍收全说明；
* `hang` —— 源站沉默，网关许可超时（compose 中为 3s）后回 504。

响应中 `result.timeline` 是事件时间线（请求头发送 → 100 → 正文开始/中止 →
最终响应），`result.response_body` 是源站的说明正文，
`origin_received_bytes` 是本次试传源站实收的上传正文字节数。

### 本地直接运行

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m app.origin      # :8081
.venv/bin/python -m app.gateway     # :8080
.venv/bin/python -m app.demo        # :8000，然后开 /docs
```

也可以用原生 curl 验证（curl 原生支持 Expect 100-continue）：

```bash
head -c 100000 /dev/zero | tr '\0' a | \
  curl -i -X POST --data-binary @- -H "Expect: 100-continue" \
  http://localhost:8080/accept
```

## 自测

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest tests/ -q      # 10 个端到端用例
.venv/bin/python scripts/selftest.py      # 8 个场景化自测，输出每条断言
```

## 关键配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GATEWAY_HOST/PORT` | `0.0.0.0:8080` | 网关监听 |
| `ORIGIN_HOST/PORT` | `127.0.0.1:8081` | 源站地址 |
| `DEMO_HOST/PORT` | `0.0.0.0:8000` | FastAPI 面板 |
| `PERMISSION_TIMEOUT` | `10` | 等源站 100 许可的秒数，超时回 504 |
| `IDLE_TIMEOUT` | `30` | 连接读空闲超时 |
| `REJECT_AFTER_BYTES` | `4096` | 样例源站 `/reject-mid`、`/reject-slow` 读多少字节后拒收 |
| `REJECT_SLOW_SEGMENTS` | `4` | 样例源站 `/reject-slow` 分段发 413 说明的段数 |
| `REJECT_SLOW_DELAY` | `0.4` | 样例源站 `/reject-slow` 分段间隔秒数 |

## 边界与限制

* 仅支持 `Content-Length` 请求，不支持 `Transfer-Encoding: chunked`；
* 每条连接一个请求，不做连接复用（`Connection: close`）；
* 样例源站为演示用途，生产使用时把 `ORIGIN_HOST/PORT` 指向真实源站即可，
  网关协议逻辑无需改动。
