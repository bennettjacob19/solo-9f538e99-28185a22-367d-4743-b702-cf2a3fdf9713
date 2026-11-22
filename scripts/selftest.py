#!/usr/bin/env python3
"""交付前自测脚本。

覆盖：
  1. 正常上传（/accept）：100 → 正文转发 → 200，源站字节数与上传一致
  2. 许可前拒收（/reject）：终端收 403、正文 0 字节，源站实收 0 字节
  3. 中途拒收（/reject-mid）：终端收 413 且无需发完，源站只收到阈值附近字节
  4. 许可超时（/hang）：网关回 504，正文 0 字节，源站实收 0 字节
  5. 非法请求（缺 Expect / Content-Length）：网关直接 4xx
  6. 慢速 413（/reject-slow）：终端停发后仍收全分段拒收说明，源站实收远小于上传
  7. 写入阻塞：终端只顾写不读，网关停读后完整 413 仍送达
  8. 客户端写入阻塞即返回：读完拒收响应即返回，不等写缓冲区腾空

直接运行：``python scripts/selftest.py``（需在仓库根目录、依赖已安装）。
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import h11  # noqa: E402

from app.h11io import read_event, send_event  # noqa: E402
from app.rawclient import upload_once  # noqa: E402
from tests.harness import Harness  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"


def _tl(result, name: str) -> float | None:
    for item in result.timeline:
        if item["event"] == name:
            return item["t_ms"]
    return None


def _check(label: str, condition: bool, detail: str = "") -> bool:
    mark = PASS if condition else FAIL
    print(f"  [{mark}] {label}" + (f" —— {detail}" if detail else ""))
    return condition


async def run() -> int:
    harness = await Harness(permission_timeout=1.5,
                            reject_after_bytes=4096).start()
    host, port = harness.gateway_addr
    failures = 0
    try:
        # ------------------------------------------------------ 1. 正常上传
        print("场景 1：正常上传 /accept（20KB）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/accept", 20_000,
                              permission_timeout=5)
        stats = await harness.stats.snapshot()
        ok = [
            _check("收到 100 Continue", r.got_100),
            _check("最终状态码 200", r.status_code == 200,
                   f"实际 {r.status_code}"),
            _check("正文 20000 字节全部发出", r.body_bytes_sent == 20_000,
                   f"实际 {r.body_bytes_sent}"),
            _check("100 早于正文发送",
                   _tl(r, "recv_100_continue") is not None
                   and _tl(r, "body_send_start") is not None
                   and _tl(r, "recv_100_continue") < _tl(r, "body_send_start")),
            _check("源站实收正文 20000 字节",
                   stats["body_bytes"].get("/accept") == 20_000,
                   f"统计 {stats['body_bytes']}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 2. 许可前拒收
        print("场景 2：源站许可前拒收 /reject（50KB，应一字不转）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/reject", 50_000,
                              permission_timeout=5)
        stats = await harness.stats.snapshot()
        ok = [
            _check("未收到 100", not r.got_100),
            _check("最终状态码 403", r.status_code == 403,
                   f"实际 {r.status_code}"),
            _check("终端正文发出 0 字节", r.body_bytes_sent == 0),
            _check("源站实收正文 0 字节",
                   stats["body_bytes"].get("/reject", 0) == 0,
                   f"统计 {stats['body_bytes']}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 3. 中途拒收
        print("场景 3：源站中途拒收 /reject-mid（100KB，阈值 4096）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/reject-mid", 100_000)
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        origin_received = stats["body_bytes"].get("/reject-mid", 0)
        ok = [
            _check("先收到 100", r.got_100),
            _check("最终状态码 413", r.status_code == 413,
                   f"实际 {r.status_code}"),
            _check("终端未发完即中止", r.body_bytes_sent < 100_000,
                   f"已发 {r.body_bytes_sent} / 100000"),
            _check("最终响应在发送完成前到达",
                   _tl(r, "recv_final_response") is not None),
            _check("源站只读到阈值附近字节",
                   4096 <= origin_received < 100_000,
                   f"源站实收 {origin_received}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 4. 许可超时
        print("场景 4：源站沉默 /hang（网关许可超时 1.5s，期望 504）")
        await harness.stats.reset()
        t0 = asyncio.get_event_loop().time()
        r = await upload_once(host, port, "/hang", 30_000,
                              permission_timeout=5)
        elapsed = asyncio.get_event_loop().time() - t0
        stats = await harness.stats.snapshot()
        ok = [
            _check("未收到 100", not r.got_100),
            _check("网关回 504", r.status_code == 504,
                   f"实际 {r.status_code}"),
            _check("终端正文发出 0 字节", r.body_bytes_sent == 0),
            _check("源站实收正文 0 字节",
                   stats["body_bytes"].get("/hang", 0) == 0),
            _check("约在许可超时后返回（<4s）", elapsed < 4,
                   f"耗时 {elapsed:.2f}s"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])

        # ------------------------------------------------- 5. 非法请求
        print("场景 5：非法请求直接 4xx")

        async def bad_request(headers):
            reader, writer = await asyncio.open_connection(host, port)
            conn = h11.Connection(our_role=h11.CLIENT)
            await send_event(conn, writer, h11.Request(
                method=b"POST", target=b"/accept", headers=headers))
            event = await read_event(conn, reader, 5)
            writer.close()
            return event.status_code

        s1 = await bad_request([(b"host", b"t"), (b"content-length", b"10")])
        s2 = await bad_request([(b"host", b"t"),
                                (b"expect", b"100-continue")])
        ok = [
            _check("缺 Expect → 417", s1 == 417, f"实际 {s1}"),
            _check("缺 Content-Length → 411", s2 == 411, f"实际 {s2}"),
        ]
        failures += not all(ok)

        # ----------------------------- 6. 慢速 413：终端停发仍收全说明
        print("场景 6：源站慢速分段 413 /reject-slow（512KB，终端停发）")
        await harness.stats.reset()
        r = await upload_once(host, port, "/reject-slow", 512 * 1024)
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        origin_received = stats["body_bytes"].get("/reject-slow", 0)
        events = [i["event"] for i in r.timeline]
        ok = [
            _check("先收到 100", r.got_100),
            _check("最终状态码 413", r.status_code == 413,
                   f"实际 {r.status_code}"),
            _check("终端未发完即中止（停发）", r.body_bytes_sent < 512 * 1024,
                   f"已发 {r.body_bytes_sent} / 524288"),
            _check("完整消息边界（分块编码）",
                   "response_end_of_message" in events),
            _check("拒收说明完整（4 段全到）",
                   r.response_body.startswith(
                       "413 Payload Too Large".encode())
                   and "实收上传正文".encode() in r.response_body
                   and "不再读取".encode() in r.response_body
                   and r.response_body.endswith(
                       "上调接收上限。\n".encode()),
                   f"正文 {len(r.response_body)} 字节"),
            _check("源站实收远小于上传",
                   4096 <= origin_received < 512 * 1024,
                   f"源站实收 {origin_received}"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])
        print("    说明正文:", r.response_body.decode("utf-8", "replace"))

        # ----------------------------- 7. 写入阻塞的终端仍收全 413
        print("场景 7：写入阻塞终端（只顾写不读）经网关收全慢速 413")

        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        # 64MB 远超本机 socket 缓冲区总量，网关停读后必然写不完
        blast_size = 64 * 1024 * 1024
        sent = 0

        async def blast():
            nonlocal sent
            try:
                while sent < blast_size:
                    writer.write(b"y" * 65536)
                    await writer.drain()
                    sent += 65536
            except (ConnectionError, BrokenPipeError,
                    asyncio.CancelledError):
                pass

        await send_event(conn, writer, h11.Request(
            method=b"POST", target=b"/reject-slow",
            headers=[
                (b"host", b"t"),
                (b"expect", b"100-continue"),
                (b"content-length", str(blast_size).encode()),
                (b"connection", b"close"),
            ],
        ))
        ev = await read_event(conn, reader, 5)
        got_100 = isinstance(ev, h11.InformationalResponse)
        blast_task = asyncio.create_task(blast())
        status, body = None, b""
        while True:
            ev = await read_event(conn, reader, 10)
            if isinstance(ev, h11.Response):
                status = ev.status_code
            elif isinstance(ev, h11.Data):
                body += ev.data
            elif isinstance(ev, h11.EndOfMessage):
                break
        sent_when_done = sent
        still_blocked = not blast_task.done()
        blast_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await blast_task
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        await asyncio.sleep(0.2)
        ok = [
            _check("先收到 100", got_100),
            _check("最终状态码 413", status == 413, f"实际 {status}"),
            _check("网关停读后写入被堵住（64MB 未发完）",
                   still_blocked and sent_when_done < blast_size,
                   f"已发 {sent_when_done} / {blast_size}"),
            _check("拒收说明完整送达",
                   body.startswith("413 Payload Too Large".encode())
                   and body.endswith("上调接收上限。\n".encode()),
                   f"正文 {len(body)} 字节"),
        ]
        failures += not all(ok)

        # ----------------------------- 8. 客户端写入阻塞即返回
        print("场景 8：对端停读，客户端读完拒收响应即返回（不等缓冲区腾空）")

        async def fake_origin(fr, fw):
            await fr.readuntil(b"\r\n\r\n")
            fw.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            await fw.drain()
            await asyncio.sleep(0.3)  # 让客户端写满缓冲区；此后不再读正文
            payload = b"rejected: payload too large"
            fw.write(
                b"HTTP/1.1 413 Payload Too Large\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: " + str(len(payload)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + payload)
            await fw.drain()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(fr.read(), timeout=5)
            fw.close()

        server = await asyncio.start_server(fake_origin, "127.0.0.1", 0)
        fake_port = server.sockets[0].getsockname()[1]
        try:
            t0 = asyncio.get_event_loop().time()
            r = await upload_once("127.0.0.1", fake_port, "/upload",
                                  32 * 1024 * 1024,
                                  permission_timeout=5, overall_timeout=15)
            elapsed = asyncio.get_event_loop().time() - t0
            await asyncio.sleep(0.2)
        finally:
            server.close()
            await server.wait_closed()
        events = [i["event"] for i in r.timeline]
        ok = [
            _check("最终状态码 413", r.status_code == 413,
                   f"实际 {r.status_code}"),
            _check("响应正文完整",
                   r.response_body == b"rejected: payload too large"),
            _check("写入未发完（对端停读）",
                   r.body_bytes_sent < 32 * 1024 * 1024,
                   f"已发 {r.body_bytes_sent}"),
            _check("读完响应即放弃发送",
                   "send_abandoned_after_final_response" in events),
            _check("立即返回（远小于 15s 整体超时）", elapsed < 10,
                   f"耗时 {elapsed:.2f}s"),
        ]
        failures += not all(ok)
        print("    时间线:", [(i["event"], i["t_ms"]) for i in r.timeline])
    finally:
        await harness.stop()

    print()
    if failures:
        print(f"自测失败：{failures} 组断言未通过")
        return 1
    print("全部自测通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
