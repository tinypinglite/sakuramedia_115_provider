from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import ClassVar

import pytest
from sakuramedia_115_provider import playback
from sakuramedia_115_provider.cloud115 import (
    Cloud115Client,
    Cloud115DirectUrl,
)
from src.plugins.provider_protocol import LibraryHandle, MediaHandle, PlaybackContext
from starlette.requests import Request
from starlette.responses import Response


class FakeClient:
    download_user_agents: ClassVar[list[str]] = []
    download_options: ClassVar[list[tuple[str, bool, bool]]] = []

    def __init__(
        self, _cookie: str, *, user_agent: str | None = None, **_kwargs
    ) -> None:
        self.user_agent = user_agent

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def get_download_url(
        self, _pickcode: str, *, user_agent: str, **_kwargs
    ) -> Cloud115DirectUrl:
        type(self).download_user_agents.append(user_agent)
        type(self).download_options.append(
            (
                user_agent,
                bool(_kwargs.get("prefer_official")),
                bool(_kwargs.get("fallback_to_downurl", True)),
            )
        )
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
    *,
    delivery: str,
    range_header: str | None = None,
    resource_path: str = "",
    user_agent: str = "player-ua",
) -> PlaybackContext:
    headers = [(b"user-agent", user_agent.encode())]
    if range_header:
        headers.append((b"range", range_header.encode()))
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": headers})
    return PlaybackContext(
        request=request,
        resource_path=resource_path,
        delivery=delivery,  # type: ignore[arg-type]
        url_for=lambda path: f"/media/1/play/{path}?delivery=proxy",
    )


def test_proxy_falls_back_to_fixed_ua_range_relay(monkeypatch) -> None:
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


def test_direct_entry_limits_upstream_range_concurrency_to_two() -> None:
    entry = playback._DirectEntry(
        direct=Cloud115DirectUrl(
            file_id="1",
            file_name="a.mp4",
            file_size_bytes=1,
            sha1="",
            pickcode="pc",
            url="https://direct/file",
            user_agent="ua",
            expires_at=0,
        ),
        usable_until=0.0,
    )

    async def probe() -> bool:
        await entry.slots.acquire()
        await entry.slots.acquire()
        locked = entry.slots.locked()
        entry.slots.release()
        entry.slots.release()
        return locked

    assert asyncio.run(probe()) is True


def test_redirect_uses_the_player_user_agent(monkeypatch) -> None:
    FakeClient.download_user_agents.clear()
    FakeClient.download_options.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    response = asyncio.run(playback.Cloud115Playback(device_cookie="redirect-cookie").handle(
        media=_media(3), context=_context(delivery="redirect")
    ))

    assert response.status_code == 302
    assert response.headers["location"] == "https://direct/file"
    assert FakeClient.download_user_agents == ["player-ua"]
    assert FakeClient.download_options == [("player-ua", False, True)]


def test_redirect_uses_official_profile_for_ios_user_agent(monkeypatch) -> None:
    FakeClient.download_user_agents.clear()
    FakeClient.download_options.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)

    response = asyncio.run(
        playback.Cloud115Playback(device_cookie="ios-redirect-cookie").handle(
            media=_media(4),
            context=_context(
                delivery="redirect",
                user_agent=Cloud115Client.IOS_APP_USER_AGENT,
            ),
        )
    )

    assert response.status_code == 302
    assert FakeClient.download_user_agents == [Cloud115Client.IOS_APP_USER_AGENT]
    assert FakeClient.download_options == [
        (Cloud115Client.IOS_APP_USER_AGENT, True, False)
    ]


def test_redirect_reuses_direct_url_until_expiry(monkeypatch) -> None:
    now = [100.0]
    monkeypatch.setattr(playback, "time", SimpleNamespace(monotonic=lambda: now[0]))
    FakeClient.download_user_agents.clear()
    FakeClient.download_options.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    player = playback.Cloud115Playback(device_cookie="redirect-cache-cookie")
    context = _context(delivery="redirect")

    first = asyncio.run(player.handle(media=_media(5), context=context))
    now[0] = 699.0
    second = asyncio.run(player.handle(media=_media(5), context=context))

    assert first.headers["location"] == "https://direct/file"
    assert second.headers["location"] == "https://direct/file"
    assert FakeClient.download_user_agents == ["player-ua"]

    now[0] = 700.0
    asyncio.run(player.handle(media=_media(5), context=context))
    assert FakeClient.download_user_agents == ["player-ua", "player-ua"]


def test_resource_path_is_not_served(monkeypatch) -> None:
    FakeClient.download_user_agents.clear()
    monkeypatch.setattr(playback, "Cloud115Client", FakeClient)
    with pytest.raises(playback.ProviderOperationError) as error:
        asyncio.run(
            playback.Cloud115Playback(device_cookie="resource-cookie").handle(
                media=_media(91),
                context=_context(
                    delivery="proxy",
                    resource_path="hls/old/segment/0.ts",
                ),
            )
        )
    assert error.value.code == "source_not_found"
