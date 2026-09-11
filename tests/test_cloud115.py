from __future__ import annotations

import asyncio
import hashlib
import io
from contextlib import contextmanager
from urllib.parse import parse_qs

import httpx
import pytest
from sakuramedia_115_provider import cloud115
from sakuramedia_115_provider.cipher import (
    _upload_aes_cbc_decrypt,
    _upload_aes_cbc_encrypt,
    _upload_decrypt_block,
    _upload_encrypt_block,
    _upload_lz4_decompress,
    _upload_round_keys,
    encrypt_downurl_payload,
)
from sakuramedia_115_provider.cloud115 import (
    Cloud115Client,
    choose_hls_definition,
    exchange_web_cookie_for_alipaymini,
)
from sakuramedia_115_provider.exceptions import (
    Cloud115AuthError,
    Cloud115OfflineTaskExistsError,
    Cloud115RequestError,
    Cloud115RiskControlError,
    Cloud115VideoUnavailableError,
)


class _ReaderTransferSource:
    """Test-only source deliberately exposes no filesystem path."""

    def __init__(self, content: bytes) -> None:
        self._content = content
        self.assertions = 0

    @contextmanager
    def open_reader(self):
        yield io.BytesIO(self._content)

    def assert_unchanged(self) -> None:
        self.assertions += 1


def test_downurl_request_codec_matches_fixed_protocol_vector() -> None:
    assert encrypt_downurl_payload(
        {"pickcode": "abcdef1234567890", "user_id": "123456"}
    ) == (
        "Qp84lxKgBSHQCMxk3A/BVB3j+nwsPqIeUBluxNrWxf/PJc5j9li905fSA12shnn4"
        "i7phjrv/eFzQRmlXQ9eY3HHJS7d8n0gpRD0QW6VfDI2bYdTUkmb+nP5XrjQefIPt"
        "JsiFtBktGIZBzx7Ll9/cWutDfgtRdbMsYQ2BNoQxCFI="
    )


def test_upload_cipher_matches_aes_vector_and_lz4_frame() -> None:
    keys = _upload_round_keys(bytes.fromhex("000102030405060708090a0b0c0d0e0f"))
    plaintext = bytes.fromhex("00112233445566778899aabbccddeeff")
    ciphertext = bytes.fromhex("69c4e0d86a7b0430d8cdb78070b4c55a")
    assert _upload_encrypt_block(plaintext, keys) == ciphertext
    assert _upload_decrypt_block(ciphertext, keys) == plaintext
    assert _upload_aes_cbc_decrypt(_upload_aes_cbc_encrypt(b"upload init")) == b"upload init"
    assert _upload_lz4_decompress(b"\x06\x00\x50hello") == b"hello"


@pytest.mark.parametrize("slot", ["R1", "R2"])
def test_rapid_upload_uses_reader_sha1_status_seven_and_pickcode_lookup(monkeypatch, slot) -> None:
    payloads = iter(
        [
            {"status": 7, "statuscode": 701, "sign_key": "sign-key", "sign_check": "2-5"},
            {"status": 2, "statuscode": 0, "fileid": 0, "pickcode": "real-pickcode"},
        ]
    )
    monkeypatch.setattr(cloud115, "decrypt_upload_response", lambda _content: next(payloads))
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "appversion.115.com":
            assert request.headers.get("cookie", "") == ""
            return httpx.Response(200, json={"code": 0, "data": {"Android": {"version_code": "37.2.5"}}})
        if request.url.host == "proapi.115.com":
            assert request.url.path == "/app/uploadinfo"
            return httpx.Response(200, json={"state": True, "userkey": "user-key"})
        if request.url.host == "uplb.115.com":
            assert request.method == "POST"
            assert request.url.path == "/4.0/initupload.php"
            assert request.url.params.get("k_ec")
            assert request.headers["origin"] == "https://115.com"
            assert "115wangpan_android/37.2.5" in request.headers["user-agent"]
            return httpx.Response(200, content=b"encrypted")
        assert request.url.host == "webapi.115.com"
        assert request.url.path == "/files/get_info"
        assert request.url.params["pick_code"] == "real-pickcode"
        return httpx.Response(200, json={"state": True, "data": [{
            "fid": "real-fid", "cid": "target-cid", "n": "source.mp4", "s": 10,
            "sha": hashlib.sha1(b"0123456789").hexdigest().upper(), "pc": "real-pickcode",
        }]})

    async def run():
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                f"UID=123456_{slot}_token; CID=c; SEID=s",
                http_client=http_client,
                pace_webapi=False,
            )
            source = _ReaderTransferSource(b"0123456789")
            result = await client.rapid_upload(
                source, filename="source.mp4", size_bytes=10, parent_cid="target-cid"
            )
            return result, source
        finally:
            await http_client.aclose()

    result, source = asyncio.run(run())
    upload_requests = [request for request in requests if request.url.host == "uplb.115.com"]
    assert result.status == "success"
    assert result.entry is not None and result.entry.entry_id == "real-fid"
    assert source.assertions == 2
    assert len(upload_requests) == 2
    first = parse_qs(_upload_aes_cbc_decrypt(upload_requests[0].content).decode("latin-1"))
    second = parse_qs(_upload_aes_cbc_decrypt(upload_requests[1].content).decode("latin-1"))
    assert first["fileid"] == [hashlib.sha1(b"0123456789").hexdigest().upper()]
    assert "preid" not in first
    assert "sign_val" not in first
    assert "preid" not in second
    assert second["sign_key"] == ["sign-key"]
    assert second["sign_val"] == [hashlib.sha1(b"2345").hexdigest().upper()]


def test_f1_cookie_uses_android_upload_key_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "proapi.115.com"
        assert request.url.path == "/android/2.0/user/upload_key"
        return httpx.Response(200, json={"state": True, "data": {"userkey": "android-key"}})

    async def run() -> str:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_F1_token; CID=c; SEID=s",
                http_client=http_client,
                pace_webapi=False,
            )
            assert client._rapid_upload_protocol() == "android"
            return await client._get_upload_userkey("android")
        finally:
            await http_client.aclose()

    assert asyncio.run(run()) == "android-key"


def test_rapid_upload_rejects_non_f1_r2_cookie_without_a_request() -> None:
    async def run() -> None:
        client = Cloud115Client(
            "UID=123456_A1_token; CID=c; SEID=s",
            pace_webapi=False,
        )
        try:
            with pytest.raises(Cloud115AuthError):
                await client.rapid_upload(
                    _ReaderTransferSource(b"content"),
                    filename="source.mp4",
                    size_bytes=7,
                    parent_cid="target-cid",
                )
        finally:
            await client.close()

    asyncio.run(run())


def test_safe_get_retries_two_transient_server_failures(monkeypatch, log_messages) -> None:
    attempts = 0
    backoffs: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"state": True, "data": {}})

    async def no_wait(delay: float) -> None:
        backoffs.append(delay)

    monkeypatch.setattr(cloud115.asyncio, "sleep", no_wait)

    async def run() -> None:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_R2_token; CID=c; SEID=s",
                http_client=http_client,
                pace_webapi=False,
            )
            await client._json("GET", "https://proapi.115.com/retry-test?sign=private-signature")
        finally:
            await http_client.aclose()

    asyncio.run(run())
    assert attempts == 3
    assert backoffs == [0.5, 1.0]
    assert len(log_messages) == 2
    assert all("status=503" in message and "retry=True" in message for message in log_messages)
    assert "attempt=1" in log_messages[0] and "attempt=2" in log_messages[1]
    assert all("private-signature" not in message and "123456_R2_token" not in message for message in log_messages)


def test_initupload_post_is_never_retried() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        assert request.url.host == "uplb.115.com"
        return httpx.Response(503)

    async def run() -> None:
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            client = Cloud115Client(
                "UID=123456_R2_token; CID=c; SEID=s",
                http_client=http_client,
                pace_webapi=False,
            )

            async def userkey(_protocol):
                return "user-key"

            async def appversion():
                return "37.2.5"

            client._get_upload_userkey = userkey
            client._get_upload_app_version = appversion
            with pytest.raises(Cloud115RequestError):
                await client._upload_init(
                    filename="source.mp4",
                    filesize=7,
                    filesha1="A" * 40,
                    parent_cid="target-cid",
                    upload_protocol="web",
                )
        finally:
            await http_client.aclose()

    asyncio.run(run())
    assert attempts == 1


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


def test_upload_domains_share_the_minimum_request_interval(monkeypatch) -> None:
    sleep_delays: list[float] = []
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})
    monkeypatch.setattr(cloud115.random, "uniform", lambda _low, _high: 1.0)
    monkeypatch.setattr(cloud115.time, "monotonic", lambda: 100.0)

    async def sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(cloud115.asyncio, "sleep", sleep)

    async def run() -> None:
        client = Cloud115Client("UID=123456_R2_x; CID=c; SEID=s")
        try:
            await client._pace_webapi("https://proapi.115.com/app/uploadinfo")
            await client._pace_webapi("https://uplb.115.com/4.0/initupload.php")
            assert client._transfer_state.webapi_count == 0
        finally:
            await client.close()

    asyncio.run(run())
    assert sleep_delays == [1.0]


def test_batch_webapi_pacing_waits_after_each_thirty_requests(monkeypatch) -> None:
    progress_events = []
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
            progress_callback=progress_events.append,
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

    assert progress_events == [{"wait_seconds": 17.0}, {"wait_seconds": 0}]


@pytest.mark.parametrize("status,host,body,error_type", [
    (405, "other.example", "blocked", Cloud115RiskControlError),
    (403, "webapi.115.com", "<html>request has been blocked</html>", Cloud115AuthError),
    (400, "webapi.115.com", "", Cloud115RiskControlError),
    (400, "webapi.115.com", "<h1>Request Header Or Cookie Too Large</h1>", Cloud115RequestError),
])
def test_http_failures_are_classified(status, host, body, error_type):
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(status, text=body)
        )) as http:
            client = Cloud115Client(
                "UID=123456_A1_x; CID=c; SEID=s", http_client=http, pace_webapi=False
            )
            with pytest.raises(error_type) as error:
                await client._request("GET", f"https://{host}/files")
            assert type(error.value) is error_type

    asyncio.run(run())


def test_iter_files_recursive_uses_server_side_recursive_listing(monkeypatch) -> None:
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})
    monkeypatch.setattr(cloud115.random, "uniform", lambda _low, _high: 0.0)
    requests: list[httpx.Request] = []
    progress_events = []

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
                "UID=123456_A1_x; CID=c; SEID=s",
                http_client=http_client,
                progress_callback=progress_events.append,
            )
            return [entry async for entry in client.iter_files_recursive("source")]
        finally:
            await http_client.aclose()

    entries = asyncio.run(run())

    assert [entry.name for entry in entries] == ["A.mp4", "B.mp4"]
    assert len(requests) == 2

    assert [(e["current"], e["total"]) for e in progress_events] == [(1, 2), (2, 2)]


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


@pytest.mark.parametrize("width,height", [(1920, 1080), ("1920", "1080"), (None, None)])
def test_video_metadata_uses_live_api_fields_without_hls(width, height) -> None:
    requests = []

    def handler(request):
        requests.append(request)
        assert request.url.path == "/files/video"
        assert request.url.params["pickcode"] == "pc"
        return httpx.Response(200, json={
            "state": True, "file_size": "8318079847", "play_long": "10773",
            "width": width, "height": height,
            "definition_list": {"3000000": "1080P"},
        })

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = Cloud115Client("UID=123_R1_x; CID=c; SEID=s", http_client=http)
            info = await client.get_video_metadata("pc")
            assert info == {
                "container": {
                    "size_bytes": 8318079847, "duration_seconds": 10773,
                    "bit_rate": 8318079847 * 8 // 10773, "bit_rate_estimated": True,
                },
                "video": {"width": 1920 if width else None, "height": 1080 if height else None},
                "audio": None, "subtitles": [],
            }

    asyncio.run(run())
    assert len(requests) == 1


@pytest.mark.parametrize("field,value", [
    ("play_long", None), ("play_long", "0"), ("play_long", "invalid"),
    ("file_size", None), ("file_size", "0"),
])
def test_video_metadata_rejects_missing_size_or_duration(field, value):
    payload = {"state": True, "file_size": "1000", "play_long": "10", field: value}

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
        ) as http:
            client = Cloud115Client("UID=123_R1_x; CID=c; SEID=s", http_client=http)
            with pytest.raises(Cloud115VideoUnavailableError):
                await client.get_video_metadata("pc")

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
                text="""#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=300\nhigh.m3u8\n""",
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
                        "do_params": {
                            "key": "confirm-key",
                            "uid": "login-token",
                            "client": 0,
                        },
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
        {
            "state": False,
            "errno": 0,
            "errcode": 10008,
            "errtype": "war",
            "error_msg": "任务已存在",
        },
        endpoint="https://115.com/web/lixian/",
    )

    assert isinstance(error, Cloud115OfflineTaskExistsError)


def test_transfer_state_preserves_batch_limit_across_event_loops(monkeypatch):
    state = cloud115.TransferState()
    now = [100.0]
    delays = []
    monkeypatch.setattr(cloud115, "_WEBAPI_NEXT_REQUEST_AT", {})
    monkeypatch.setattr(cloud115.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        cloud115.random, "uniform", lambda low, high: 1.0 if high == 3.0 else 17.0
    )

    async def sleep(delay):
        delays.append(delay)
        now[0] += delay

    monkeypatch.setattr(cloud115.asyncio, "sleep", sleep)

    async def run(count):
        async with Cloud115Client(
            "UID=123_F1_x; CID=c; SEID=s", batch_pacing=True, transfer_state=state
        ) as client:
            for _ in range(count):
                await client._pace_webapi("https://webapi.115.com/files")

    asyncio.run(run(30))
    asyncio.run(run(1))
    assert delays[-1] == 17.0
    assert state.webapi_count == 1


def test_transfer_state_reuses_auth_metadata_across_event_loops(monkeypatch):
    state = cloud115.TransferState()
    calls = []

    async def fake_json(self, method, url, **kwargs):
        calls.append(url)
        if "appversion" in url:
            return {"data": {"Android": {"version_code": "99"}}}
        return {"userkey": "fake-user-key"}

    monkeypatch.setattr(Cloud115Client, "_json", fake_json)

    async def run():
        async with Cloud115Client(
            "UID=123_F1_x; CID=c; SEID=s", transfer_state=state
        ) as client:
            assert await client._get_upload_userkey("android") == "fake-user-key"
            assert await client._get_upload_app_version() == "99"

    asyncio.run(run())
    asyncio.run(run())
    assert len(calls) == 2


@pytest.mark.parametrize(
    "payload",
    [
        {"status": 4, "statuscode": 402, "statusmsg": "invalid target"},
        {"status": 1, "statuscode": 403},
        {"status": 2, "statuscode": 403, "pickcode": "wrong"},
        {"status": 7, "statuscode": 403, "sign_key": "key", "sign_check": "0-1"},
        {"status": 7, "sign_key": "key", "sign_check": "0-1"},
        {"status": 6},
        {"status": 8},
        {},
    ],
)
def test_rapid_upload_errors_are_never_reported_as_not_hit(payload):
    async def run():
        def unexpected_request(request):
            pytest.fail("unexpected HTTP request after upload init")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(unexpected_request)
        ) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )
            calls = []

            async def upload_init(**kwargs):
                calls.append(kwargs)
                return {"state": True, "data": payload}

            client._upload_init = upload_init
            with pytest.raises(Cloud115RequestError):
                await client.rapid_upload(
                    _ReaderTransferSource(b"test"),
                    filename="test.mp4",
                    size_bytes=4,
                    parent_cid="target",
                )
            assert len(calls) <= 2

    asyncio.run(run())


@pytest.mark.parametrize("wrapped", [False, True])
def test_rapid_upload_only_status_one_is_not_hit(wrapped):
    async def run():
        def unexpected_request(request):
            pytest.fail("unexpected HTTP request after upload init")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(unexpected_request)
        ) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )

            async def upload_init(**kwargs):
                data = {"status": 1, "statuscode": 0}
                return {"state": True, "data": data} if wrapped else data

            client._upload_init = upload_init
            result = await client.rapid_upload(
                _ReaderTransferSource(b"test"),
                filename="test.mp4",
                size_bytes=4,
                parent_cid="target",
            )
            assert result.status == "not_hit"

    asyncio.run(run())


def test_numeric_failure_state_stops_delete_and_authentication():
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"state": 0, "errno": 99})
            )
        ) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )
            with pytest.raises(Cloud115AuthError):
                await client.delete_files(["file"])

    asyncio.run(run())


@pytest.mark.parametrize(
    "path,code,missing",
    [
        ("/category/get", 70005, True),
        ("/files/get_info", 70005, False),
        ("/files/get_info", 20018, True),
        ("/files/get_info", 800001, True),
        ("/rb/delete", 800001, False),
        ("/rb/delete", 20018, False),
        ("/category/get", 1001, False),
    ],
)
def test_not_found_error_codes_are_endpoint_scoped(path, code, missing):
    from sakuramedia_115_provider.exceptions import Cloud115NotFoundError

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"state": False, "errNo": code}
                )
            )
        ) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )
            with pytest.raises(cloud115.Cloud115Error) as error:
                await client._json("GET", "https://webapi.115.com" + path)
            assert isinstance(error.value, Cloud115NotFoundError) is missing

    asyncio.run(run())


def test_upload_version_endpoint_does_not_require_state_or_receive_cookie():
    def response(request):
        assert request.url.host == "appversion.115.com"
        assert request.headers.get("cookie", "") == ""
        return httpx.Response(
            200, json={"data": {"Android": {"version_code": "37.2.5"}}}
        )

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )
            assert await client._get_upload_app_version() == "37.2.5"

    asyncio.run(run())


@pytest.mark.parametrize("extra", [{}, {"fid": ""}, {"fid": None}])
def test_file_info_preserves_legacy_file_id_alias(extra):
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    json={
                        "state": True,
                        "data": [
                            {
                                "file_id": "real-file",
                                **extra,
                                "cid": "parent",
                                "n": "test.mp4",
                                "s": 4,
                                "pc": "pick",
                            }
                        ],
                    },
                )
            )
        ) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )
            entry = await client.file_by_pickcode("pick")
            assert entry.entry_id == "real-file" and not entry.is_dir

    asyncio.run(run())


@pytest.mark.parametrize(
    "payload",
    [
        {"count": 1, "data": []},
        {"count": 0},
        {"count": 0, "data": {}},
        {"count": 0, "data": [None]},
        {"data": []},
    ],
)
@pytest.mark.parametrize("recursive", [False, True])
def test_directory_errors_cannot_be_interpreted_as_an_empty_inventory(
    payload, recursive
):
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"state": True, "cid": "folder", **payload}
                )
            )
        ) as http:
            client = Cloud115Client(
                "UID=123_R2_x; CID=c; SEID=s", pace_webapi=False, http_client=http
            )
            with pytest.raises(Cloud115RequestError):
                if recursive:
                    _ = [entry async for entry in client.iter_files_recursive("folder")]
                else:
                    await client.list_directory("folder")

    asyncio.run(run())


def test_upload_wire_payload_matches_pre_plugin_cipher(monkeypatch):
    from sakuramedia_115_provider import cipher

    monkeypatch.setattr(cipher, "time", lambda: 1700000000)
    _, body = cipher.make_upload_payload(
        {
            "appid": 0,
            "appversion": "37.2.5",
            "fileid": "A" * 40,
            "filename": "测试 video.mp4",
            "filesize": 123456,
            "target": "U_1_123",
            "sign_key": "key",
            "sign_val": "B" * 40,
            "topupload": "true",
            "userid": "123",
            "userkey": "fake-userkey",
        }
    )
    # 固定输入来自插件化前 b32c72d^，覆盖编码、签名、token 和 CBC 多块请求。
    assert (
        hashlib.sha256(body).hexdigest()
        == "815aec3289aaa2eac7d7916224275e4893d65c9bb2f53f13e4f3ec031d699c61"
    )
    assert (
        cipher._upload_lz4_decompress(b"\x06\x00\x50hello\x06\x00\x50world" + bytes(7))
        == b"helloworld"
    )
    assert (
        cipher._upload_lz4_decompress(b"\x0d\x00\x40abcd\x04\x00\x50efghi")
        == b"abcdabcdefghi"
    )


def test_offline_duplicate_matches_live_nested_errtype_response():
    # Production R1 response: errcode is top-level; errtype only appears in result[0].
    payload = {
        "state": False,
        "errno": 0,
        "errcode": 10008,
        "result": [{"state": False, "errno": 0, "errcode": 10008, "errtype": "war"}],
    }

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
        ) as http:
            client = Cloud115Client(
                "UID=123_R1_x; CID=c; SEID=s", http_client=http, pace_webapi=False
            )
            with pytest.raises(Cloud115OfflineTaskExistsError):
                await client.add_offline_url("magnet:?xt=urn:btih:" + "a" * 40, save_dir_id="test")

    asyncio.run(run())


@pytest.mark.parametrize("payload", [
    {"state": True, "file_status": 0},
    {"state": True, "file_status": 1},
])
def test_hls_unavailable_is_distinct_from_upstream_failure(payload) -> None:
    async def run() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=payload))
        ) as http_client:
            client = Cloud115Client("UID=123456_A1_x; CID=c; SEID=s", http_client=http_client)
            with pytest.raises(Cloud115VideoUnavailableError):
                await client.get_video_info("pc")

    asyncio.run(run())


@pytest.mark.parametrize("directories", [True, False])
def test_subtree_slim_list_follows_has_next_page(directories):
    calls = []

    def handler(request):
        page = int(request.url.params["page"])
        calls.append(page)
        assert request.url.path == ("/files/downfolders" if directories else "/files/downfiles")
        assert request.url.params["per_page"] == "5000"
        row = {"fid": str(page), "fn": "folder", "pid": "root"} if directories else {"pc": str(page), "fs": 0, "pid": "root"}
        return httpx.Response(200, json={"state": True, "data": {
            "list": [row], "count": 1, "has_next_page": page == 1,
        }})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = Cloud115Client("UID=123_A1; CID=c; SEID=s", http_client=http_client, pace_webapi=False)
            return [row async for row in client.iter_download_nodes("root-pc", directories=directories, progress=lambda _: None)]

    assert len(asyncio.run(run())) == 2
    assert calls == [1, 2]


@pytest.mark.parametrize("failure", ["missing_pagination", "count_mismatch", "repeated_page"])
def test_subtree_slim_list_rejects_incomplete_or_repeated_pages(failure):
    def handler(request):
        data = {"list": [{"fid": "1", "fn": "folder", "pid": "root"}], "count": 1, "has_next_page": False}
        if failure == "missing_pagination":
            del data["has_next_page"]
        elif failure == "count_mismatch":
            data["count"] = 2
        else:
            data["has_next_page"] = True
        return httpx.Response(200, json={"state": True, "data": data})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = Cloud115Client("UID=123_A1; CID=c; SEID=s", http_client=http_client, pace_webapi=False)
            return [row async for row in client.iter_download_nodes("root-pc", directories=True, progress=lambda _: None)]

    with pytest.raises(cloud115.Cloud115Error):
        asyncio.run(run())
