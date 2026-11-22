"""端到端测试：正常上传、许可前拒收、中途拒收、慢速 413、许可超时、连接只处理一次。"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import sys
import time
from pathlib import Path

import h11
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.h11io import read_event, send_event  # noqa: E402
from app.rawclient import upload_once  # noqa: E402
from tests.harness import Harness  # noqa: E402


@pytest.fixture
def harness():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    h = loop.run_until_complete(
        Harness(permission_timeout=1.5, reject_after_bytes=4096).start()
    )
    yield h
    loop.run_until_complete(h.stop())
    loop.close()
    asyncio.set_event_loop(None)


def _timeline_events(result) -> list[str]:
    return [item["event"] for item in result.timeline]


def _event_time(result, name: str) -> float:
    for item in result.timeline:
        if item["event"] == name:
            return item["t_ms"]
    raise AssertionError(f"事件 {name} 未出现: {result.timeline}")


# ---------------------------------------------------------------- 正常上传
def test_accept_full_upload(harness):
    host, port = harness.gateway_addr

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/accept", 20000)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.error is None
    assert result.got_100 is True
    assert result.status_code == 200
    assert result.body_bytes_sent == 20000
    # 时序：请求头 → 100 许可 → 正文开始 → 正文发完 → 最终响应
    events = _timeline_events(result)
    assert events.index("request_headers_sent") < events.index("recv_100_continue")
    assert events.index("recv_100_continue") < events.index("body_send_start")
    assert events.index("body_send_complete") < events.index("recv_final_response")
    assert stats["body_bytes"].get("/accept") == 20000


# ------------------------------------------------------- 源站许可前直接拒收
def test_reject_before_permission_forwards_no_body(harness):
    host, port = harness.gateway_addr

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/reject", 50000,
                                   permission_timeout=5)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is False
    assert result.status_code == 403
    # 没有收到许可，正文一个字节都不能发
    assert result.body_bytes_sent == 0
    assert "body_send_start" not in _timeline_events(result)
    # 源站同样实收 0 字节正文
    assert stats["body_bytes"].get("/reject", 0) == 0


# ----------------------------------------------------------- 源站中途拒收
def test_reject_mid_stream_terminal_need_not_finish(harness):
    host, port = harness.gateway_addr
    body_size = 100_000  # 远大于 4096 的拒收阈值

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/reject-mid", body_size)
        # 给网关一点时间把剩余字节情况落定后再读统计
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is True
    assert result.status_code == 413
    events = _timeline_events(result)
    assert "body_send_aborted" in events
    # 终端在发完之前就拿到了最终响应
    assert result.body_bytes_sent < body_size
    assert _event_time(result, "recv_final_response") <= \
        _event_time(result, "body_send_aborted") + 5
    # 源站只读到阈值附近的字节，远小于整个 body
    origin_received = stats["body_bytes"].get("/reject-mid", 0)
    assert 4096 <= origin_received < body_size
    assert origin_received <= result.body_bytes_sent + 4096


# ------------------------------------------------------------- 许可超时 504
def test_permission_timeout_returns_504(harness):
    host, port = harness.gateway_addr

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/hang", 30000,
                                   permission_timeout=5)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.got_100 is False
    assert result.status_code == 504
    assert result.body_bytes_sent == 0
    assert stats["body_bytes"].get("/hang", 0) == 0


# ------------------------------------------------- 每条连接只处理一次请求def test_connection_handles_single_request_only(harness):
    host, port = harness.gateway_addr

    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        await send_event(conn, writer, h11.Request(
            method=b"POST", target=b"/accept",
            headers=[
                (b"host", b"test"),
                (b"expect", b"100-continue"),
                (b"content-length", b"5"),
                (b"connection", b"keep-alive"),
            ],
        ))
        event = await read_event(conn, reader, 5)
        assert isinstance(event, h11.InformationalResponse)
        assert event.status_code == 100
        await send_event(conn, writer, h11.Data(data=b"hello"))
        await send_event(conn, writer, h11.EndOfMessage())
        # 收完最终响应
        while True:
            event = await read_event(conn, reader, 5)
            if isinstance(event, h11.EndOfMessage):
                break
        # 网关应当关闭连接，而不是接受第二次请求
        try:
            chunk = await asyncio.wait_for(reader.read(1), timeout=3)
        except asyncio.TimeoutError:
            chunk = b"timeout"
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        return chunk

    chunk = asyncio.get_event_loop().run_until_complete(scenario())
    assert chunk == b""


# ------------------------------------------------ 非法请求的直接 4xx 拒绝
@pytest.mark.parametrize(
    "headers,expected_status",
    [
        ([(b"host", b"t"), (b"content-length", b"10")], 417),          # 缺 Expect
        ([(b"host", b"t"), (b"expect", b"100-continue")], 411),        # 缺 Content-Length
    ],
)
def test_invalid_requests_rejected(harness, headers, expected_status):
    host, port = harness.gateway_addr

    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        await send_event(conn, writer, h11.Request(
            method=b"POST", target=b"/accept", headers=headers,
        ))
        event = await read_event(conn, reader, 5)
        writer.close()
        return event.status_code

    status = asyncio.get_event_loop().run_until_complete(scenario())
    assert status == expected_status


# ------------------------------------- 慢速 413：终端停发后仍能收全分段说明
def _expected_slow_413_body(received: int) -> bytes:
    return (
        "413 Payload Too Large：诊断包超过源站接收上限，已拒收。\n"
        f"拒收前源站实收上传正文 {received} 字节。\n"
        "剩余正文不再读取，请停止继续上传。\n"
        "建议：压缩或分片后重试，或联系管理员上调接收上限。\n"
    ).encode("utf-8")


def test_slow_413_terminal_stops_sending_gets_full_explanation(harness):
    """源站分段慢发 413：终端提前停发，网关仍转回原状态码和完整说明。"""
    host, port = harness.gateway_addr
    body_size = 512 * 1024

    async def scenario():
        await harness.stats.reset()
        result = await upload_once(host, port, "/reject-slow", body_size)
        await asyncio.sleep(0.2)
        stats = await harness.stats.snapshot()
        return result, stats

    result, stats = asyncio.get_event_loop().run_until_complete(scenario())

    assert result.error is None
    assert result.got_100 is True
    assert result.status_code == 413
    events = _timeline_events(result)
    # 终端未发完即中止（停发）
    assert "body_send_aborted" in events
    assert result.body_bytes_sent < body_size
    # 完整消息边界由分块编码给出，不依赖连接关闭
    assert "response_end_of_message" in events
    # 说明正文一字不差（与源站实收字节数互证）
    origin_received = stats["body_bytes"].get("/reject-slow", 0)
    assert 4096 <= origin_received < body_size
    assert result.response_body == _expected_slow_413_body(origin_received)


def test_slow_413_terminal_half_close_still_gets_full_body(harness):
    """终端看到 413 响应头后停发并半关（FIN），仍要收全慢速分段说明。"""
    host, port = harness.gateway_addr

    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        try:
            await send_event(conn, writer, h11.Request(
                method=b"POST", target=b"/reject-slow",
                headers=[
                    (b"host", b"test"),
                    (b"expect", b"100-continue"),
                    (b"content-length", b"1048576"),
                    (b"connection", b"close"),
                ],
            ))
            event = await read_event(conn, reader, 5)
            assert isinstance(event, h11.InformationalResponse)
            # 发一部分正文（超过源站 4096 拒收阈值），触发慢速 413
            await send_event(conn, writer, h11.Data(data=b"x" * 8192))
            # 读到 413 响应头
            while True:
                event = await read_event(conn, reader, 5)
                if isinstance(event, h11.Response):
                    status = event.status_code
                    break
            # 终端停发并半关写方向（FIN），只留读方向
            sock = writer.get_extra_info("socket")
            sock.shutdown(socket.SHUT_WR)
            # 继续把分段说明读完
            body = b""
            while True:
                event = await read_event(conn, reader, 5)
                if isinstance(event, h11.Data):
                    body += event.data
                elif isinstance(event, h11.EndOfMessage):
                    break
            return status, body
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    status, body = asyncio.get_event_loop().run_until_complete(scenario())
    assert status == 413
    assert body.startswith("413 Payload Too Large".encode("utf-8"))
    assert "实收上传正文".encode("utf-8") in body
    assert "不再读取".encode("utf-8") in body
    assert body.endswith("上调接收上限。\n".encode("utf-8"))


def test_slow_413_write_blocked_blaster_still_gets_full_body(harness):
    """终端只顾写不读（写入阻塞在缓冲区）：网关停读后，完整 413 仍送达。"""
    host, port = harness.gateway_addr
    # 64MB 远超本机 socket 缓冲区总量，网关停读后终端必然写不完
    body_size = 64 * 1024 * 1024

    async def scenario():
        reader, writer = await asyncio.open_connection(host, port)
        conn = h11.Connection(our_role=h11.CLIENT)
        sent = 0

        async def blast():
            nonlocal sent
            try:
                while sent < body_size:
                    chunk = b"y" * 65536
                    writer.write(chunk)
                    await writer.drain()
                    sent += len(chunk)
            except (ConnectionError, BrokenPipeError, asyncio.CancelledError):
                pass

        try:
            await send_event(conn, writer, h11.Request(
                method=b"POST", target=b"/reject-slow",
                headers=[
                    (b"host", b"test"),
                    (b"expect", b"100-continue"),
                    (b"content-length", str(body_size).encode("ascii")),
                    (b"connection", b"close"),
                ],
            ))
            event = await read_event(conn, reader, 5)
            assert isinstance(event, h11.InformationalResponse)
            blast_task = asyncio.create_task(blast())
            try:
                # 只顾读响应：分段慢发的 413 说明
                status = None
                body = b""
                while True:
                    event = await read_event(conn, reader, 10)
                    if isinstance(event, h11.Response):
                        status = event.status_code
                    elif isinstance(event, h11.Data):
                        body += event.data
                    elif isinstance(event, h11.EndOfMessage):
                        break
                # 同一事件循环节拍内读取发送进度，此时发送必仍堵着
                sent_when_done = sent
                blast_done = blast_task.done()
            finally:
                blast_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await blast_task
            return status, body, sent_when_done, blast_done
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            # 让网关侧把引流/关闭收尾做完，避免拆循环时残留任务
            await asyncio.sleep(0.2)

    status, body, sent_when_done, blast_done = \
        asyncio.get_event_loop().run_until_complete(scenario())
    assert status == 413
    # 网关停读后，终端的 64MB 远没发完（写入被缓冲区堵住）
    assert blast_done is False
    assert sent_when_done < body_size
    # 但拒收说明完整送达
    assert body.startswith("413 Payload Too Large".encode("utf-8"))
    assert "实收上传正文".encode("utf-8") in body
    assert body.endswith("上调接收上限。\n".encode("utf-8"))


# --------------------------- 客户端：读完拒收响应即返回，不等写缓冲区腾空
def test_client_returns_after_rejection_despite_blocked_write():
    """对端停读、发送堵在 drain 时，读完最终响应后客户端必须能立即返回。"""

    async def scenario():
        async def fake_origin(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            await writer.drain()
            # 给客户端时间把发送缓冲区写满（本服务从此不再读正文）
            await asyncio.sleep(0.3)
            body = b"rejected: payload too large"
            writer.write(
                b"HTTP/1.1 413 Payload Too Large\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
                b"Connection: close\r\n"
                b"\r\n" + body,
            )
            await writer.drain()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(reader.read(), timeout=5)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

        server = await asyncio.start_server(fake_origin, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            started = time.monotonic()
            result = await upload_once(
                "127.0.0.1", port, "/upload", 32 * 1024 * 1024,
                permission_timeout=5, overall_timeout=15,
            )
            elapsed = time.monotonic() - started
            # 让假源站读到客户端 EOF 后自行退出，避免拆循环时残留任务
            await asyncio.sleep(0.2)
        finally:
            server.close()
            await server.wait_closed()
        return result, elapsed

    # 本用例不用 harness，自带事件循环
    loop = asyncio.new_event_loop()
    try:
        result, elapsed = loop.run_until_complete(scenario())
    finally:
        loop.close()

    assert result.error is None
    assert result.got_100 is True
    assert result.status_code == 413
    assert result.response_body == b"rejected: payload too large"
    # 写入没发完 32MB（对端停读），但客户端读完响应即返回
    assert result.body_bytes_sent < 32 * 1024 * 1024
    events = _timeline_events(result)
    assert "send_abandoned_after_final_response" in events
    assert "response_end_of_message" in events
    # 立即返回，远小于 overall_timeout=15s
    assert elapsed < 10
