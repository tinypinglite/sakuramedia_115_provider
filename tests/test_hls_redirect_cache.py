from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import httpx
import pytest
from sakuramedia_115_provider import cloud115, playback
from sakuramedia_115_provider.cloud115 import Cloud115DirectUrl
from starlette.requests import Request

from src.plugins.provider_protocol import (
    LibraryHandle,
    MediaHandle,
    PlaybackContext,
    ProviderOperationError,
)


def media(media_id=1, *, library_id=1, pickcode="pc"):
    return MediaHandle(
        media_id=media_id,
        library=LibraryHandle(library_id, "cloud115", {}, "123"),
        storage_ref={"kind": "cloud115_media", "pickcode": pickcode},
        file_name="movie.mp4",
        file_size_bytes=10,
        duration_seconds=10,
    )


def context(user_agent="player"):
    return PlaybackContext(
        request=Request(
            {
                "type": "http",
                "method": "GET",
                "headers": [
                    (b"user-agent", user_agent.encode()),
                ],
            }
        ),
        resource_path="",
        delivery="redirect",
        url_for=lambda path: path,
    )


@pytest.fixture
def upstream(monkeypatch):
    state = SimpleNamespace(
        now=100.0,
        ready=True,
        status=200,
        network_error=False,
        requests=[],
        request_user_agents=[],
        downloads=[],
        entered=None,
        release=None,
        high_url="https://hls.example/high.m3u8",
    )
    monkeypatch.setattr(playback, "_CACHE", playback._PlaybackCache())
    # 只替换播放缓存的时钟，不影响 asyncio 或 HTTP 客户端的时钟。
    monkeypatch.setattr(
        playback,
        "time",
        SimpleNamespace(
            monotonic=lambda: state.now,
            time=time.time,
        ),
    )

    async def handler(request):
        state.requests.append(str(request.url))
        state.request_user_agents.append(request.headers["user-agent"])
        if request.url.host == "webapi.115.com":
            if state.entered is not None:
                state.entered.set()
                await state.release.wait()
            if state.network_error:
                raise httpx.ConnectError("mock connection failed", request=request)
            if state.status != 200:
                return httpx.Response(state.status)
            return httpx.Response(
                200,
                json={
                    "state": True,
                    "file_status": int(state.ready),
                    "video_url": "https://hls.example/master.m3u8",
                },
            )
        return httpx.Response(
            200,
            text=(
                "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\nlow.m3u8\n"
                f"#EXT-X-STREAM-INF:BANDWIDTH=200\n{state.high_url}\n"
            ),
        )

    real_client = httpx.AsyncClient

    class MockClient(real_client):
        def __init__(self, **kwargs):
            super().__init__(transport=httpx.MockTransport(handler), **kwargs)

    def must_not_reserve_queue(*_args):
        raise AssertionError("HLS redirect resolution must not enter the pacing queue")

    async def download_url(self, pickcode, *, user_agent):
        state.downloads.append((pickcode, user_agent))
        return Cloud115DirectUrl(
            "f",
            "movie.mp4",
            10,
            "sha",
            pickcode,
            f"https://files.example/{user_agent}.mp4",
            user_agent,
            0,
        )

    monkeypatch.setattr(cloud115.httpx, "AsyncClient", MockClient)
    monkeypatch.setattr(cloud115.random, "uniform", must_not_reserve_queue)
    monkeypatch.setattr(cloud115.Cloud115Client, "get_download_url", download_url)
    state.player = playback.Cloud115Playback(
        device_cookie="UID=123_A1_x; CID=c; SEID=s"
    )
    return state


def test_hls_redirect_skips_pacing_and_reuses_result_until_ten_minutes(upstream):
    async def run():
        first = await upstream.player.handle(media=media(), context=context())
        assert first.status_code == 302
        assert first.headers["location"] == upstream.high_url
        assert len(upstream.requests) == 2
        upstream.now = 699.99
        second_player = playback.Cloud115Playback(
            device_cookie="UID=123_A1_x; CID=c; SEID=s"
        )
        second = await second_player.handle(media=media(), context=context())
        assert second.headers["location"] == first.headers["location"]
        assert len(upstream.requests) == 2
        upstream.now = 700.0
        upstream.high_url = "https://hls.example/refreshed.m3u8"
        refreshed = await upstream.player.handle(media=media(), context=context())
        assert refreshed.headers["location"] == upstream.high_url
        assert len(upstream.requests) == 4

    asyncio.run(run())


def test_hls_redirect_preserves_player_user_agent_and_isolates_cache(upstream):
    async def run():
        await upstream.player.handle(media=media(), context=context("first-player"))
        assert upstream.request_user_agents == ["first-player", "first-player"]

        second_player = playback.Cloud115Playback(
            device_cookie="UID=123_A1_x; CID=c; SEID=s"
        )
        await second_player.handle(media=media(), context=context("second-player"))
        assert upstream.request_user_agents == [
            "first-player",
            "first-player",
            "second-player",
            "second-player",
        ]

    asyncio.run(run())


def test_unavailable_hls_is_cached_for_five_seconds_without_merging_user_agents(
    upstream,
):
    async def run():
        upstream.ready = False
        first = await upstream.player.handle(media=media(), context=context())
        assert first.headers["location"] == "https://files.example/player.mp4"
        upstream.now = 104.99
        await upstream.player.handle(media=media(), context=context())
        second = await upstream.player.handle(media=media(), context=context("another"))
        assert second.headers["location"] == "https://files.example/another.mp4"
        assert len(upstream.requests) == 2
        assert upstream.request_user_agents == ["player", "another"]
        assert upstream.downloads == [("pc", "player"), ("pc", "another")]
        upstream.now = 105.0
        upstream.ready = True
        ready = await upstream.player.handle(media=media(), context=context())
        assert ready.headers["location"] == upstream.high_url
        assert len(upstream.requests) == 4

    asyncio.run(run())


@pytest.mark.parametrize("status", [401, 403, 405, 400, "network"])
def test_upstream_errors_are_not_cached_or_treated_as_missing_hls(
    upstream, monkeypatch, status
):
    monkeypatch.setattr(cloud115.Cloud115Client, "_MAX_RETRIES", 0)

    async def run():
        upstream.network_error = status == "network"
        upstream.status = status if not upstream.network_error else 200
        for _ in range(2):
            with pytest.raises(ProviderOperationError):
                await upstream.player.handle(media=media(), context=context())
        assert len(upstream.requests) == 2
        assert upstream.downloads == []
        assert playback._CACHE.hls_redirect_tasks == {}
        upstream.status = 200
        upstream.network_error = False
        response = await upstream.player.handle(media=media(), context=context())
        assert response.headers["location"] == upstream.high_url
        assert len(upstream.requests) == 4

    asyncio.run(run())


def test_cache_is_isolated_by_media_library_pickcode_and_credentials(upstream):
    async def run():
        for item in [media(), media(2), media(library_id=2), media(pickcode="other")]:
            await upstream.player.handle(media=item, context=context())
        other = playback.Cloud115Playback(
            device_cookie="UID=123_A1_x; CID=c; SEID=changed"
        )
        await other.handle(media=media(), context=context())
        assert len(upstream.requests) == 10

    asyncio.run(run())


def test_concurrent_requests_share_one_resolution_and_cancellation_does_not_abort_it(
    upstream,
):
    async def run():
        upstream.entered = asyncio.Event()
        upstream.release = asyncio.Event()
        first = asyncio.create_task(
            upstream.player.handle(media=media(), context=context())
        )
        await upstream.entered.wait()
        second_player = playback.Cloud115Playback(
            device_cookie="UID=123_A1_x; CID=c; SEID=s"
        )
        second = asyncio.create_task(
            second_player.handle(media=media(), context=context())
        )
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        upstream.release.set()
        response = await second
        assert response.headers["location"] == upstream.high_url
        assert len(upstream.requests) == 2
        assert playback._CACHE.hls_redirect_tasks == {}
        await upstream.player.handle(media=media(), context=context())
        assert len(upstream.requests) == 2

    asyncio.run(run())


def test_hls_cache_is_bounded_and_evicts_least_recently_used_entry(upstream):
    async def run():
        for media_id in range(128):
            await upstream.player.handle(media=media(media_id), context=context())
        await upstream.player.handle(media=media(0), context=context())
        await upstream.player.handle(media=media(128), context=context())
        count = len(upstream.requests)
        await upstream.player.handle(media=media(0), context=context())
        assert len(upstream.requests) == count
        await upstream.player.handle(media=media(1), context=context())
        assert len(upstream.requests) == count + 2

    asyncio.run(run())
