from __future__ import annotations

import asyncio
from typing import ClassVar

import httpx
import pytest
from sakuramedia_115_provider import playback
from sakuramedia_115_provider.cloud115 import (
    Cloud115Client,
    Cloud115DirectUrl,
    Cloud115VideoDefinition,
    Cloud115VideoInfo,
    Cloud115VideoSegment,
)
from sakuramedia_115_provider.exceptions import Cloud115RequestError
from starlette.requests import Request
from starlette.responses import Response

from src.plugins.provider_protocol import LibraryHandle, MediaHandle, PlaybackContext


class FakeClient:
    fail_hls = False
    download_user_agents: ClassVar[list[str]] = []
    video_info_pickcodes: ClassVar[list[str]] = []

    def __init__(
        self, _cookie: str, *, user_agent: str | None = None, **_kwargs
    ) -> None:
        self.user_agent = user_agent

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def get_video_info(self, pickcode: str) -> Cloud115VideoInfo:
        type(self).video_info_pickcodes.append(pickcode)
        if type(self).fail_hls:
            raise Cloud115RequestError("no hls")
        return Cloud115VideoInfo(
            definitions=(
                Cloud115VideoDefinition(100, "640x360", "low", "https://hls/low.m3u8"),
            )
        )

    async def get_video_segments(self, _definition) -> tuple[Cloud115VideoSegment, ...]:
        return (
            Cloud115VideoSegment(0, "https://hls/0.ts", 4.0),
            Cloud115VideoSegment(1, "https://hls/1.ts", 4.0),
        )

    async def get_download_url(self, _pickcode: str, *, user_agent: str) -> Cloud115DirectUrl:
        type(self).download_user_agents.append(user_agent)
        return Cloud115DirectUrl("f", "movie.mp4", 10, "sha", "pc", "https://direct/file", user_agent, 0)


def _media(media_id: int = 1) -> MediaHandle:
    library = LibraryHandle(1, "cloud115", {}, "123")
    return MediaHandle(
        media_id=media_id,
        library=library,
        storage_ref={
            "version": 1,
            "kind": "cloud115_media",
            "pickcode": "pc",
            "fid": "f",
            "parent_cid": "p",
            "name": "movie.mp4",
            "size_bytes": 10,
            "sha1": "sha",
            "is_dir": False,
        },
        file_name="movie.mp4",
        file_size_bytes=10,
        duration_seconds=0,
    )


def _context(
    *, delivery: str, range_header: str | None = None, resource_path: str = ""
) -> PlaybackContext:
    headers = [(b"user-agent", b"player-ua")]
    if range_header:
        headers.append((b"range", range_header.encode()))
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": headers})
    return PlaybackContext(
        request=request,
        resource_path=resource_path,
        delivery=delivery,  # type: ignore[arg-type]
        url_for=lambda path: f"/media/1/play/{path}?delivery=proxy",
    )


def test_proxy_returns_local_hls_playlist(monkeypatch) -> None:
    FakeClient.fail_hls = False
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    response = asyncio.run(playback.Cloud115Playback(device_cookie="cookie").handle(
        media=_media(), context=_context(delivery="proxy")
    ))

    assert response.media_type == "application/vnd.apple.mpegurl"
    assert "hls/" in response.body.decode()
    assert "https://hls/" not in response.body.decode()


def test_proxy_falls_back_to_fixed_ua_range_relay(monkeypatch) -> None:
    FakeClient.fail_hls = True
    FakeClient.download_user_agents.clear()
    seen: dict[str, object] = {}

    async def relay(self, *, url, user_agent, request, lease):
        seen.update(url=url, user_agent=user_agent, range=request.request.headers.get("range"), lease=lease)
        return Response(status_code=206)

    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    monkeypatch.setattr(playback.Cloud115Playback, "_external_relay", relay)
    response = asyncio.run(playback.Cloud115Playback(device_cookie="different-cookie").handle(
        media=_media(2), context=_context(delivery="proxy", range_header="bytes=0-9")
    ))

    assert response.status_code == 206
    assert seen["url"] == "https://direct/file"
    assert seen["user_agent"] == Cloud115Client.DEFAULT_USER_AGENT
    assert seen["range"] == "bytes=0-9"
    assert FakeClient.download_user_agents == [Cloud115Client.DEFAULT_USER_AGENT]


def test_external_relay_releases_lease_when_connect_is_cancelled(monkeypatch) -> None:
    closed: list[bool] = []

    class CancelledClient:
        def __init__(self, **_kwargs) -> None:
            pass

        def build_request(self, *_args, **_kwargs):
            return object()

        async def send(self, _request, *, stream: bool) -> None:
            raise asyncio.CancelledError

        async def aclose(self) -> None:
            closed.append(True)

    monkeypatch.setattr(playback.httpx, "AsyncClient", CancelledClient)

    async def call_relay() -> tuple[bool, bool]:
        lease = asyncio.Semaphore(1)
        try:
            await playback.Cloud115Playback(device_cookie="cookie")._external_relay(
                url="https://direct/file",
                user_agent="player-ua",
                request=_context(delivery="proxy"),
                lease=lease,
            )
        except asyncio.CancelledError:
            return True, not lease.locked()
        return False, not lease.locked()

    assert asyncio.run(call_relay()) == (True, True)
    assert closed == [True]


def test_hls_relay_ignores_range_and_rejects_partial_segments(monkeypatch) -> None:
    seen_ranges: list[str | None] = []
    status_code = 200

    def handler(request: httpx.Request) -> httpx.Response:
        seen_ranges.append(request.headers.get("range"))
        return httpx.Response(
            status_code,
            content=b"ts",
            headers={"Content-Type": "video/mp2t", "Content-Range": "bytes 0-1/2"},
        )

    real_async_client = httpx.AsyncClient
    transport = httpx.MockTransport(handler)

    class RelayClient(real_async_client):
        def __init__(self, **kwargs) -> None:
            super().__init__(transport=transport, **kwargs)

    monkeypatch.setattr(playback.httpx, "AsyncClient", RelayClient)
    player = playback.Cloud115Playback(device_cookie="hls-range-cookie")
    context = _context(delivery="proxy", range_header="bytes=0-9")

    response = asyncio.run(
        player._external_relay(
            url="https://hls/0.ts",
            user_agent="player-ua",
            request=context,
            lease=None,
            forward_range=False,
        )
    )

    async def read_body() -> bytes:
        return b"".join([chunk async for chunk in response.body_iterator])

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "content-range" not in response.headers
    assert asyncio.run(read_body()) == b"ts"
    assert seen_ranges == [None]

    status_code = 206
    with pytest.raises(playback._UpstreamStatus) as exc_info:
        asyncio.run(
            player._external_relay(
                url="https://hls/0.ts",
                user_agent="player-ua",
                request=context,
                lease=None,
                forward_range=False,
            )
        )
    assert exc_info.value.status_code == 206
    assert seen_ranges == [None, None]


def test_redirect_uses_the_player_user_agent(monkeypatch) -> None:
    FakeClient.fail_hls = False
    FakeClient.download_user_agents.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    response = asyncio.run(playback.Cloud115Playback(device_cookie="redirect-cookie").handle(
        media=_media(3), context=_context(delivery="redirect")
    ))

    assert response.status_code == 302
    assert response.headers["location"] == "https://direct/file"
    assert FakeClient.download_user_agents == ["player-ua"]


def test_redirect_reuses_cached_direct_url(monkeypatch) -> None:
    FakeClient.fail_hls = False
    FakeClient.download_user_agents.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    player = playback.Cloud115Playback(device_cookie="redirect-cache-cookie")
    context = _context(delivery="redirect")

    first = asyncio.run(player.handle(media=_media(5), context=context))
    second = asyncio.run(player.handle(media=_media(5), context=context))

    assert first.headers["location"] == "https://direct/file"
    assert second.headers["location"] == "https://direct/file"
    assert FakeClient.download_user_agents == ["player-ua"]


def test_direct_cache_uses_ten_minute_ttl(monkeypatch) -> None:
    cache = playback._PlaybackCache()
    monkeypatch.setattr(playback.time, "monotonic", lambda: 100.0)
    direct = Cloud115DirectUrl(
        "f", "movie.mp4", 10, "sha", "pc", "https://direct/file", "player-ua", 0
    )

    entry = cache.put_direct(("key",), direct)

    assert entry.usable_until == 100.0 + 10 * 60


def test_hls_segment_refreshes_definition_once(monkeypatch) -> None:
    FakeClient.fail_hls = False
    relay_calls: list[tuple[str, bool]] = []

    async def relay(self, *, url, user_agent, request, lease, forward_range=True):
        relay_calls.append((url, forward_range))
        if len(relay_calls) == 1:
            raise playback._UpstreamStatus(403)
        return Response(status_code=200)

    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    monkeypatch.setattr(playback.Cloud115Playback, "_external_relay", relay)
    player = playback.Cloud115Playback(device_cookie="hls-refresh-cookie")
    root = asyncio.run(player.handle(media=_media(4), context=_context(delivery="proxy")))
    line = next(value for value in root.body.decode().splitlines() if "/segment/0.ts" in value)
    resource_path = line.split("/play/", 1)[1].split("?", 1)[0]

    response = asyncio.run(
        player.handle(
            media=_media(4),
            context=_context(delivery="proxy", resource_path=resource_path),
        )
    )

    assert response.status_code == 200
    assert relay_calls == [("https://hls/0.ts", False), ("https://hls/0.ts", False)]


def test_merged_hls_playlist_concatenates_parts_with_discontinuity(monkeypatch) -> None:
    FakeClient.fail_hls = False
    seen_calls: list[tuple[str, bool]] = []

    async def relay(self, *, url, user_agent, request, lease, forward_range=True):
        seen_calls.append((url, forward_range))
        return Response(status_code=200)

    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    monkeypatch.setattr(playback.Cloud115Playback, "_external_relay", relay)
    player = playback.Cloud115Playback(device_cookie="merged-hls-cookie")
    medias = (_media(10), _media(11))
    root = asyncio.run(
        player.handle_merged(
            medias=medias,
            context=_context(delivery="proxy", resource_path="index.m3u8"),
        )
    )

    playlist = root.body.decode()
    assert root.media_type == "application/vnd.apple.mpegurl"
    assert "#EXT-X-PLAYLIST-TYPE:VOD" in playlist
    assert playlist.count("#EXTINF") == 4
    assert playlist.count("#EXT-X-DISCONTINUITY") == 1
    child_path = next(
        line.split("/play/", 1)[1].split("?", 1)[0]
        for line in playlist.splitlines()
        if "/part/1/segment/0.ts" in line
    )

    response = asyncio.run(
        player.handle_merged(
            medias=medias,
            context=_context(delivery="proxy", resource_path=child_path),
        )
    )

    assert response.status_code == 200
    assert seen_calls == [("https://hls/0.ts", False)]


def test_merged_hls_reuses_layout_for_repeated_playlists(monkeypatch) -> None:
    FakeClient.fail_hls = False
    FakeClient.video_info_pickcodes.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    medias = (_media(20), _media(21))
    context = _context(delivery="proxy", resource_path="index.m3u8")

    first = asyncio.run(
        playback.Cloud115Playback(device_cookie="merged-layout-cache-cookie").handle_merged(
            medias=medias,
            context=context,
        )
    )
    second = asyncio.run(
        playback.Cloud115Playback(device_cookie="merged-layout-cache-cookie").handle_merged(
            medias=medias,
            context=context,
        )
    )

    assert first.body == second.body
    assert FakeClient.video_info_pickcodes == ["pc", "pc"]
