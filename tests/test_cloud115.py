from __future__ import annotations

import asyncio

import httpx
import pytest

from sakuramedia_115_provider import cloud115
from sakuramedia_115_provider.cipher import encrypt_downurl_payload
from sakuramedia_115_provider.cloud115 import (
    Cloud115Client,
    choose_hls_definition,
    exchange_web_cookie_for_alipaymini,
)
from sakuramedia_115_provider.exceptions import (
    Cloud115AuthError,
    Cloud115OfflineTaskExistsError,
    Cloud115RiskControlError,
)


def test_downurl_request_codec_matches_fixed_protocol_vector() -> None:
    assert encrypt_downurl_payload(
        {"pickcode": "abcdef1234567890", "user_id": "123456"}
    ) == (
        "Qp84lxKgBSHQCMxk3A/BVB3j+nwsPqIeUBluxNrWxf/PJc5j9li905fSA12shnn4"
        "i7phjrv/eFzQRmlXQ9eY3HHJS7d8n0gpRD0QW6VfDI2bYdTUkmb+nP5XrjQefIPt"
        "JsiFtBktGIZBzx7Ll9/cWutDfgtRdbMsYQ2BNoQxCFI="
    )


def test_webapi_pacing_is_shared_per_user_and_randomized(monkeypatch) -> None:
    intervals: list[tuple[float, float]] = []
    sleep_delays: list[float] = []

    def interval(low: float, high: float) -> float:
        intervals.append((low, high))
        return 2.0

    async def sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(cloud115.random, "uniform", interval)
    monkeypatch.setattr(cloud115.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(cloud115.asyncio, "sleep", sleep)

    async def pace(cookie: str) -> None:
        client = Cloud115Client(cookie)
        try:
            await client._pace_webapi("https://webapi.115.com/files")
        finally:
            await client.close()

    asyncio.run(pace("UID=987654321_A1_x; CID=c; SEID=s"))
    asyncio.run(pace("UID=987654321_A1_y; CID=c; SEID=s"))
    asyncio.run(pace("UID=987654322_A1_x; CID=c; SEID=s"))

    assert intervals == [(1.0, 3.0)] * 3
    assert sleep_delays == [2.0]


def test_webapi_pacing_can_be_disabled(monkeypatch) -> None:
    sleep_delays: list[float] = []

    async def sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(cloud115.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(cloud115.asyncio, "sleep", sleep)
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {"987654321": 200.0})

    def should_not_pace(_low: float, _high: float) -> float:
        raise AssertionError("disabled WebAPI pacing must not reserve the shared queue")

    monkeypatch.setattr(cloud115.random, "uniform", should_not_pace)

    async def pace() -> None:
        client = Cloud115Client(
            "UID=987654321_A1_x; CID=c; SEID=s",
            pace_webapi=False,
        )
        try:
            await client._pace_webapi("https://webapi.115.com/files")
            await client._pace_webapi("https://webapi.115.com/files")
        finally:
            await client.close()

    asyncio.run(pace())

    assert sleep_delays == []


def test_batch_webapi_pacing_waits_after_each_thirty_requests(monkeypatch) -> None:
    intervals: list[tuple[float, float]] = []
    sleep_delays: list[float] = []
    now = [100.0]

    def interval(low: float, high: float) -> float:
        intervals.append((low, high))
        return 1.0 if high == 3.0 else 17.0

    async def sleep(delay: float) -> None:
        sleep_delays.append(delay)
        now[0] += delay

    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})
    monkeypatch.setattr(cloud115.random, "uniform", interval)
    monkeypatch.setattr(cloud115.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(cloud115.asyncio, "sleep", sleep)

    async def pace() -> None:
        client = Cloud115Client(
            "UID=987654321_A1_x; CID=c; SEID=s",
            batch_pacing=True,
        )
        try:
            for _ in range(31):
                await client._pace_webapi("https://webapi.115.com/files")
        finally:
            await client.close()

    asyncio.run(pace())

    assert intervals.count((10.0, 30.0)) == 1
    assert sleep_delays[-1] == 17.0
    assert cloud115._WEBAPI_NEXT_REQUEST_AT["987654321"] == now[0] + 1.0


def test_http_405_on_any_domain_is_explicit_risk_control(monkeypatch) -> None:
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(405, text="blocked")

    async def request() -> None:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s",
                http_client=http_client,
            )
            with pytest.raises(Cloud115RiskControlError, match="HTTP 405"):
                await client._request("GET", "https://other.example/files")
        finally:
            await http_client.aclose()

    asyncio.run(request())


def test_http_403_remains_an_auth_error_even_with_a_waf_like_body(monkeypatch) -> None:
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="<html>request has been blocked</html>")

    async def request() -> None:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s",
                http_client=http_client,
            )
            with pytest.raises(Cloud115AuthError):
                await client._request("GET", "https://webapi.115.com/files")
        finally:
            await http_client.aclose()

    asyncio.run(request())


def test_http_400_on_webapi_is_risk_control(monkeypatch) -> None:
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    async def request() -> None:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s",
                http_client=http_client,
            )
            with pytest.raises(Cloud115RiskControlError, match="HTTP 400"):
                await client._request("GET", "https://webapi.115.com/files")
        finally:
            await http_client.aclose()

    asyncio.run(request())


def test_iter_files_recursive_uses_server_side_recursive_listing(monkeypatch) -> None:
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})
    monkeypatch.setattr(cloud115.random, "uniform", lambda _low, _high: 0.0)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/files"
        assert request.url.params["cid"] == "source"
        assert request.url.params["limit"] == "1150"
        assert request.url.params["show_dir"] == "0"
        assert request.url.params["cur"] == "0"
        offset = request.url.params["offset"]
        data = (
            [{"fid": "file-1", "cid": "child", "n": "A.mp4", "s": 1, "pc": "pc-1"}]
            if offset == "0"
            else [{"fid": "file-2", "cid": "child", "n": "B.mp4", "s": 2, "pc": "pc-2"}]
        )
        return httpx.Response(
            200,
            json={"state": True, "cid": "source", "count": 2, "data": data},
        )

    async def run():
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s", http_client=http_client
            )
            return [entry async for entry in client.iter_files_recursive("source")]
        finally:
            await http_client.aclose()

    entries = asyncio.run(run())

    assert [entry.name for entry in entries] == ["A.mp4", "B.mp4"]
    assert len(requests) == 2


def test_directory_info_parses_ancestor_breadcrumbs(monkeypatch) -> None:
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})
    monkeypatch.setattr(cloud115.random, "uniform", lambda _low, _high: 0.0)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/category/get"
        assert request.url.params["cid"] == "deep"
        return httpx.Response(
            200,
            json={
                "state": True,
                "file_name": "CD1",
                "paths": [
                    {"file_id": 0, "file_name": "根目录"},
                    {"file_id": "source", "file_name": "downloads"},
                    {"file_id": "mid", "file_name": "ABC-001"},
                ],
            },
        )

    async def run():
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s", http_client=http_client
            )
            return await client.directory_info("deep")
        finally:
            await http_client.aclose()

    directory = asyncio.run(run())

    assert directory.name == "CD1"
    assert directory.ancestors == (
        ("0", "根目录"),
        ("source", "downloads"),
        ("mid", "ABC-001"),
    )


def test_direct_url_issuance_binds_the_requested_user_agent(monkeypatch) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["user-agent"] == "bound-player-ua"
        assert request.headers["referer"] == "https://115.com/"
        assert request.method == "POST"
        assert request.url.path == "/app/chrome/downurl"
        assert request.content
        return httpx.Response(200, json={"state": True, "data": "ciphertext"})

    monkeypatch.setattr(
        cloud115,
        "decrypt_downurl_payload",
        lambda _value: {
            "1": {
                "file_name": "movie.mp4",
                "file_size": 10,
                "sha1": "sha",
                "pick_code": "pc",
                "url": {"url": "https://direct.example/file?t=9999999999"},
            }
        },
    )

    async def run() -> None:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s",
                http_client=http_client,
            )
            direct = await client.get_download_url("pc", user_agent="bound-player-ua")
            assert direct.user_agent == "bound-player-ua"
            assert direct.url == "https://direct.example/file?t=9999999999"
        finally:
            await http_client.aclose()

    asyncio.run(run())


def test_hls_playlists_are_parsed_with_one_stable_user_agent() -> None:
    seen_user_agents: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_user_agents.append(request.headers["user-agent"])
        if request.url.path == "/files/video":
            return httpx.Response(
                200,
                json={
                    "state": True,
                    "file_status": 1,
                    "video_url": "https://hls.example/master.m3u8",
                },
            )
        if request.url.path == "/master.m3u8":
            return httpx.Response(
                200,
                text='''#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=300\nhigh.m3u8\n''',
            )
        if request.url.path == "/high.m3u8":
            return httpx.Response(200, text="#EXTM3U\n#EXTINF:4.0,\nfirst.ts\n")
        raise AssertionError(request.url)

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            cloud = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s; ignored=value",
                user_agent="provider-ua",
                http_client=client,
            )
            info = await cloud.get_video_info("pickcode")
            definition = choose_hls_definition(info.definitions)
            segments = await cloud.get_video_segments(definition)
            assert definition.bandwidth == 300
            assert segments[0].url == "https://hls.example/first.ts"
            assert cloud.snapshot_cookies() == "UID=123456_A1_x; CID=c; SEID=s"
        finally:
            await client.aclose()

    asyncio.run(run())
    assert seen_user_agents == ["provider-ua", "provider-ua", "provider-ua"]


def test_cookie_probe_handles_expired_responses_without_following_redirects() -> None:
    async def check(case: str, expected: bool) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if case == "redirect":
                return httpx.Response(
                    302,
                    headers={"Location": "https://login.example/"},
                    request=request,
                )
            return httpx.Response(200, json={"state": case == "alive"}, request=request)

        http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), follow_redirects=True
        )
        try:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s", http_client=http_client
            )
            assert await client.check_alive() is expected
            assert len(requests) == 1
        finally:
            await http_client.aclose()

    asyncio.run(check("alive", True))
    asyncio.run(check("state_false", False))
    asyncio.run(check("redirect", False))


def test_device_cookie_exchange_uses_prompt_action(monkeypatch) -> None:
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    class FakeClient:
        def __init__(self, _cookie: str) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc) -> None:
            return None

        async def check_alive(self) -> bool:
            return True

        async def _json(self, method: str, url: str, *, params=None, data=None):
            calls.append((method, url, params if params is not None else data))
            if url.endswith("/token/"):
                return {"state": 1, "data": {"uid": "login-token"}}
            if url.endswith("/prompt.php"):
                return {
                    "state": 1,
                    "data": {
                        "do_url": "https://qrcodeapi.115.com/api/2.0/slogin.php",
                        "do_params": {"key": "confirm-key", "uid": "login-token", "client": 0},
                    },
                }
            if url.endswith("/slogin.php"):
                return {"state": 1}
            if url.endswith("/login/qrcode/"):
                return {"state": 1, "data": {"cookie": {"UID": "device-cookie"}}}
            raise AssertionError(url)

    monkeypatch.setattr(cloud115, "Cloud115Client", FakeClient)

    device_cookie = asyncio.run(exchange_web_cookie_for_alipaymini("web-cookie"))

    assert device_cookie == "UID=device-cookie"
    assert calls[2] == (
        "GET",
        "https://qrcodeapi.115.com/api/2.0/slogin.php",
        {"key": "confirm-key", "uid": "login-token", "client": 0},
    )


def test_offline_duplicate_payload_is_distinguished_from_quota() -> None:
    error = Cloud115Client._error_from_payload(
        {"state": False, "errno": 0, "errcode": 10008, "errtype": "war", "error_msg": "任务已存在"}
    )

    assert isinstance(error, Cloud115OfflineTaskExistsError)
