"""115 playback delivery: local proxy/HLS and one-hop external redirect."""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import httpx
from starlette.responses import (
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)

from src.plugins.provider_protocol import (
    MediaHandle,
    PlaybackContext,
    ProviderOperationError,
)

from .cloud115 import (
    Cloud115Client,
    Cloud115DirectUrl,
    Cloud115VideoSegment,
    choose_hls_definition,
)
from .exceptions import (
    Cloud115AuthError,
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115RequestError,
)

_BROWSER_USER_AGENT = Cloud115Client.DEFAULT_USER_AGENT
_DIRECT_URL_CACHE_TTL_SECONDS = 6 * 60 * 60
_MERGED_HLS_CACHE_TTL_SECONDS = 10 * 60
_HLS_PATH = re.compile(r"^hls/([A-Za-z0-9_-]{16,})/segment/(\d+)\.ts$")
_MERGED_HLS_PATH = re.compile(
    r"^merged-hls/([A-Za-z0-9_-]{16,})/part/(\d+)/segment/(\d+)\.ts$"
)
_ONE_RANGE = re.compile(r"^bytes=(?:\d+-\d*|\d*-\d+)$")
_RELAY_HEADERS = frozenset(
    {
        "accept-ranges",
        "cache-control",
        "content-disposition",
        "content-length",
        "content-range",
        "etag",
        "last-modified",
    }
)


@dataclass(slots=True)
class _DirectEntry:
    direct: Cloud115DirectUrl
    usable_until: float
    slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(4))


@dataclass(slots=True)
class _HlsEntry:
    token: str
    library_id: int
    media_id: int
    credential_fingerprint: str
    pickcode: str
    segments: tuple[Cloud115VideoSegment, ...]
    usable_until: float


@dataclass(slots=True)
class _MergedHlsEntry:
    token: str
    library_id: int
    media_ids: tuple[int, ...]
    credential_fingerprint: str
    pickcodes: tuple[str, ...]
    segments_by_part: tuple[tuple[Cloud115VideoSegment, ...], ...]
    usable_until: float


class _PlaybackCache:
    """Bounded, process-local cache.  It never writes signed 115 URLs to disk."""

    def __init__(self) -> None:
        self._direct: OrderedDict[tuple[object, ...], _DirectEntry] = OrderedDict()
        self._hls: OrderedDict[str, _HlsEntry] = OrderedDict()
        self._merged_hls: OrderedDict[str, _MergedHlsEntry] = OrderedDict()

    def direct_for(self, key: tuple[object, ...]) -> _DirectEntry | None:
        entry = self._direct.get(key)
        if entry is None or entry.usable_until <= time.monotonic():
            self._direct.pop(key, None)
            return None
        self._direct.move_to_end(key)
        return entry

    def put_direct(self, key: tuple[object, ...], direct: Cloud115DirectUrl) -> _DirectEntry:
        ttl = float(_DIRECT_URL_CACHE_TTL_SECONDS)
        if direct.expires_at:
            ttl = max(1.0, min(ttl, direct.expires_at - time.time() - 30.0))
        entry = _DirectEntry(direct=direct, usable_until=time.monotonic() + ttl)
        self._direct[key] = entry
        self._direct.move_to_end(key)
        while len(self._direct) > 128:
            self._direct.popitem(last=False)
        return entry

    def discard_direct(self, key: tuple[object, ...]) -> None:
        self._direct.pop(key, None)

    def put_hls(
        self,
        *,
        library_id: int,
        media_id: int,
        credential_fingerprint: str,
        pickcode: str,
        segments: tuple[Cloud115VideoSegment, ...],
    ) -> _HlsEntry:
        entry = _HlsEntry(
            token=secrets.token_urlsafe(18),
            library_id=library_id,
            media_id=media_id,
            credential_fingerprint=credential_fingerprint,
            pickcode=pickcode,
            segments=segments,
            usable_until=time.monotonic() + 15 * 60,
        )
        self._hls[entry.token] = entry
        while len(self._hls) > 64:
            self._hls.popitem(last=False)
        return entry

    def hls_for(
        self,
        token: str,
        *,
        library_id: int,
        media_id: int,
        credential_fingerprint: str,
    ) -> _HlsEntry | None:
        entry = self._hls.get(token)
        if entry is None or entry.usable_until <= time.monotonic():
            self._hls.pop(token, None)
            return None
        if (
            entry.library_id != library_id
            or entry.media_id != media_id
            or entry.credential_fingerprint != credential_fingerprint
        ):
            return None
        self._hls.move_to_end(token)
        return entry

    def refresh_hls(
        self, entry: _HlsEntry, segments: tuple[Cloud115VideoSegment, ...]
    ) -> None:
        entry.segments = segments
        entry.usable_until = time.monotonic() + 15 * 60

    def put_merged_hls(
        self,
        *,
        library_id: int,
        media_ids: tuple[int, ...],
        credential_fingerprint: str,
        pickcodes: tuple[str, ...],
        segments_by_part: tuple[tuple[Cloud115VideoSegment, ...], ...],
    ) -> _MergedHlsEntry:
        entry = _MergedHlsEntry(
            token=secrets.token_urlsafe(18),
            library_id=library_id,
            media_ids=media_ids,
            credential_fingerprint=credential_fingerprint,
            pickcodes=pickcodes,
            segments_by_part=segments_by_part,
            usable_until=time.monotonic() + _MERGED_HLS_CACHE_TTL_SECONDS,
        )
        self._merged_hls[entry.token] = entry
        while len(self._merged_hls) > 64:
            self._merged_hls.popitem(last=False)
        return entry

    def merged_hls_for_layout(
        self,
        *,
        library_id: int,
        media_ids: tuple[int, ...],
        credential_fingerprint: str,
        pickcodes: tuple[str, ...],
    ) -> _MergedHlsEntry | None:
        now = time.monotonic()
        for token, entry in reversed(tuple(self._merged_hls.items())):
            if entry.usable_until <= now:
                continue
            if (
                entry.library_id == library_id
                and entry.media_ids == media_ids
                and entry.credential_fingerprint == credential_fingerprint
                and entry.pickcodes == pickcodes
            ):
                self._merged_hls.move_to_end(token)
                return entry
        return None

    def merged_hls_for(
        self,
        token: str,
        *,
        library_id: int,
        media_ids: tuple[int, ...],
        credential_fingerprint: str,
    ) -> _MergedHlsEntry | None:
        entry = self._merged_hls.get(token)
        if entry is None or entry.usable_until <= time.monotonic():
            self._merged_hls.pop(token, None)
            return None
        if (
            entry.library_id != library_id
            or entry.media_ids != media_ids
            or entry.credential_fingerprint != credential_fingerprint
        ):
            return None
        self._merged_hls.move_to_end(token)
        return entry

    def refresh_merged_hls_part(
        self,
        entry: _MergedHlsEntry,
        part_index: int,
        segments: tuple[Cloud115VideoSegment, ...],
    ) -> None:
        updated_parts = list(entry.segments_by_part)
        updated_parts[part_index] = segments
        entry.segments_by_part = tuple(updated_parts)
        entry.usable_until = time.monotonic() + _MERGED_HLS_CACHE_TTL_SECONDS


_CACHE = _PlaybackCache()


class _UpstreamStatus(RuntimeError):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class Cloud115Playback:
    def __init__(self, *, device_cookie: str) -> None:
        self._device_cookie = device_cookie
        self._credential_fingerprint = hashlib.sha256(device_cookie.encode("utf-8")).hexdigest()

    async def handle(self, *, media: MediaHandle, context: PlaybackContext) -> Response:
        pickcode = self._pickcode(media, operation="playback")
        path = context.resource_path or ""
        if path:
            return await self._hls_segment(media=media, context=context, pickcode=pickcode)
        if context.delivery == "redirect":
            return await self._redirect(media=media, pickcode=pickcode, context=context)
        return await self._proxy_root(media=media, pickcode=pickcode, context=context)

    async def handle_merged(
        self,
        *,
        medias: tuple[MediaHandle, ...],
        context: PlaybackContext,
    ) -> Response:
        if len(medias) < 2:
            raise _provider_error(
                "merged_playback", "unsupported", "合并播放至少需要 2 个分段"
            )
        pickcodes = tuple(
            self._pickcode(media, operation="merged_playback") for media in medias
        )
        if context.resource_path == "index.m3u8":
            try:
                entry = await self._resolve_merged_hls(medias=medias, pickcodes=pickcodes)
            except Cloud115Error as exc:
                raise _cloud_error("merged_playback", exc) from exc
            return PlainTextResponse(
                self._render_merged_hls_playlist(entry, context),
                media_type="application/vnd.apple.mpegurl",
                headers={"Cache-Control": "no-store"},
            )
        return await self._merged_hls_segment(
            medias=medias,
            pickcodes=pickcodes,
            context=context,
        )

    async def _redirect(
        self, *, media: MediaHandle, pickcode: str, context: PlaybackContext
    ) -> Response:
        user_agent = context.request.headers.get("user-agent")
        if not user_agent:
            raise _provider_error(
                "playback", "unsupported", "直连播放需要播放器提供 User-Agent"
            )
        _, entry = await self._direct_entry(
            media=media,
            pickcode=pickcode,
            user_agent=user_agent,
        )
        return RedirectResponse(entry.direct.url, status_code=302)

    def _direct_key(
        self, *, media: MediaHandle, pickcode: str, user_agent: str
    ) -> tuple[object, ...]:
        return (
            media.library.library_id,
            media.media_id,
            self._credential_fingerprint,
            pickcode,
            user_agent,
        )

    async def _direct_entry(
        self, *, media: MediaHandle, pickcode: str, user_agent: str
    ) -> tuple[tuple[object, ...], _DirectEntry]:
        key = self._direct_key(media=media, pickcode=pickcode, user_agent=user_agent)
        entry = _CACHE.direct_for(key)
        if entry is not None:
            return key, entry
        try:
            async with Cloud115Client(self._device_cookie) as client:
                direct = await client.get_download_url(pickcode, user_agent=user_agent)
        except Cloud115Error as exc:
            raise _cloud_error("playback", exc) from exc
        return key, _CACHE.put_direct(key, direct)

    async def _proxy_root(
        self, *, media: MediaHandle, pickcode: str, context: PlaybackContext
    ) -> Response:
        try:
            hls = await self._resolve_hls(media=media, pickcode=pickcode)
        except Cloud115Error:
            # HLS is an optimization.  A direct Range relay remains usable for
            # non-transcoded and non-member files, so it is the deliberate fallback.
            return await self._direct_relay(media=media, pickcode=pickcode, context=context)
        return PlainTextResponse(
            self._render_hls_playlist(hls, context),
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-store"},
        )

    async def _resolve_hls(self, *, media: MediaHandle, pickcode: str) -> _HlsEntry:
        async with Cloud115Client(self._device_cookie) as client:
            info = await client.get_video_info(pickcode)
            segments = await client.get_video_segments(choose_hls_definition(info.definitions))
        return _CACHE.put_hls(
            library_id=media.library.library_id,
            media_id=media.media_id,
            credential_fingerprint=self._credential_fingerprint,
            pickcode=pickcode,
            segments=segments,
        )

    async def _resolve_merged_hls(
        self,
        *,
        medias: tuple[MediaHandle, ...],
        pickcodes: tuple[str, ...],
    ) -> _MergedHlsEntry:
        library_id = medias[0].library.library_id
        media_ids = tuple(media.media_id for media in medias)
        cached = _CACHE.merged_hls_for_layout(
            library_id=library_id,
            media_ids=media_ids,
            credential_fingerprint=self._credential_fingerprint,
            pickcodes=pickcodes,
        )
        if cached is not None:
            return cached

        segments_by_part: list[tuple[Cloud115VideoSegment, ...]] = []
        async with Cloud115Client(
            self._device_cookie, pace_webapi=False
        ) as client:
            for pickcode in pickcodes:
                info = await client.get_video_info(pickcode)
                segments = await client.get_video_segments(
                    choose_hls_definition(info.definitions)
                )
                if not segments:
                    raise Cloud115RequestError("115 HLS 分片为空")
                segments_by_part.append(segments)
        return _CACHE.put_merged_hls(
            library_id=library_id,
            media_ids=media_ids,
            credential_fingerprint=self._credential_fingerprint,
            pickcodes=pickcodes,
            segments_by_part=tuple(segments_by_part),
        )

    async def _hls_segment(
        self, *, media: MediaHandle, context: PlaybackContext, pickcode: str
    ) -> Response:
        match = _HLS_PATH.fullmatch(context.resource_path or "")
        if match is None:
            raise _provider_error("playback", "source_not_found", "115 播放资源不存在")
        entry = _CACHE.hls_for(
            match.group(1),
            library_id=media.library.library_id,
            media_id=media.media_id,
            credential_fingerprint=self._credential_fingerprint,
        )
        if entry is None or entry.pickcode != pickcode:
            raise _provider_error("playback", "unavailable", "115 HLS 播放列表已过期", retryable=True)
        index = int(match.group(2))
        if index >= len(entry.segments):
            raise _provider_error("playback", "source_not_found", "115 HLS 分片不存在")
        try:
            return await self._external_relay(
                url=entry.segments[index].url,
                user_agent=_BROWSER_USER_AGENT,
                request=context,
                lease=None,
                forward_range=False,
            )
        except _UpstreamStatus as exc:
            if exc.status_code not in {401, 403, 404}:
                raise _provider_error("playback", "unavailable", "115 HLS 分片读取失败", retryable=True) from exc
        except Cloud115RequestError:
            pass
        # A signed HLS URL can expire independently. Refresh once and retry the
        # same segment index; retrying repeatedly would turn a player loop into
        # uncontrolled traffic.
        try:
            async with Cloud115Client(self._device_cookie) as client:
                info = await client.get_video_info(pickcode)
                segments = await client.get_video_segments(choose_hls_definition(info.definitions))
            _CACHE.refresh_hls(entry, segments)
            if index >= len(segments):
                raise Cloud115NotFoundError("115 HLS 分片不存在")
            return await self._external_relay(
                url=segments[index].url,
                user_agent=_BROWSER_USER_AGENT,
                request=context,
                lease=None,
                forward_range=False,
            )
        except Cloud115Error as exc:
            raise _cloud_error("playback", exc) from exc
        except _UpstreamStatus as exc:
            raise _provider_error("playback", "unavailable", "115 HLS 分片读取失败", retryable=True) from exc

    async def _merged_hls_segment(
        self,
        *,
        medias: tuple[MediaHandle, ...],
        pickcodes: tuple[str, ...],
        context: PlaybackContext,
    ) -> Response:
        match = _MERGED_HLS_PATH.fullmatch(context.resource_path or "")
        if match is None:
            raise _provider_error("merged_playback", "source_not_found", "115 合并播放资源不存在")
        entry = _CACHE.merged_hls_for(
            match.group(1),
            library_id=medias[0].library.library_id,
            media_ids=tuple(media.media_id for media in medias),
            credential_fingerprint=self._credential_fingerprint,
        )
        if entry is None or entry.pickcodes != pickcodes:
            raise _provider_error(
                "merged_playback", "unavailable", "115 合并 HLS 播放列表已过期", retryable=True
            )
        part_index = int(match.group(2))
        segment_index = int(match.group(3))
        if part_index >= len(entry.segments_by_part):
            raise _provider_error("merged_playback", "source_not_found", "115 合并分段不存在")
        segments = entry.segments_by_part[part_index]
        if segment_index >= len(segments):
            raise _provider_error("merged_playback", "source_not_found", "115 HLS 分片不存在")
        try:
            return await self._external_relay(
                url=segments[segment_index].url,
                user_agent=_BROWSER_USER_AGENT,
                request=context,
                lease=None,
                forward_range=False,
            )
        except _UpstreamStatus as exc:
            if exc.status_code not in {401, 403, 404}:
                raise _provider_error(
                    "merged_playback", "unavailable", "115 HLS 分片读取失败", retryable=True
                ) from exc
        except Cloud115RequestError:
            pass
        try:
            async with Cloud115Client(self._device_cookie) as client:
                info = await client.get_video_info(pickcodes[part_index])
                refreshed_segments = await client.get_video_segments(
                    choose_hls_definition(info.definitions)
                )
            if not refreshed_segments or segment_index >= len(refreshed_segments):
                raise Cloud115NotFoundError("115 HLS 分片不存在")
            _CACHE.refresh_merged_hls_part(entry, part_index, refreshed_segments)
            return await self._external_relay(
                url=refreshed_segments[segment_index].url,
                user_agent=_BROWSER_USER_AGENT,
                request=context,
                lease=None,
                forward_range=False,
            )
        except Cloud115Error as exc:
            raise _cloud_error("merged_playback", exc) from exc
        except _UpstreamStatus as exc:
            raise _provider_error(
                "merged_playback", "unavailable", "115 HLS 分片读取失败", retryable=True
            ) from exc

    async def _direct_relay(
        self, *, media: MediaHandle, pickcode: str, context: PlaybackContext
    ) -> Response:
        range_header = context.request.headers.get("range")
        if range_header and _ONE_RANGE.fullmatch(range_header.strip()) is None:
            return Response(status_code=416, headers={"Accept-Ranges": "bytes"})
        key, entry = await self._direct_entry(
            media=media,
            pickcode=pickcode,
            user_agent=_BROWSER_USER_AGENT,
        )
        for attempt in range(2):
            try:
                return await self._external_relay(
                    url=entry.direct.url,
                    user_agent=entry.direct.user_agent,
                    request=context,
                    lease=entry.slots,
                )
            except _UpstreamStatus as exc:
                if attempt or exc.status_code not in {401, 403}:
                    raise _provider_error("playback", "unavailable", "115 直链读取失败", retryable=True) from exc
                _CACHE.discard_direct(key)
                _, entry = await self._direct_entry(
                    media=media,
                    pickcode=pickcode,
                    user_agent=_BROWSER_USER_AGENT,
                )
            except Cloud115RequestError as exc:
                raise _provider_error("playback", "unavailable", "115 直链读取失败", retryable=True) from exc
        raise AssertionError("unreachable")

    async def _external_relay(
        self,
        *,
        url: str,
        user_agent: str,
        request: PlaybackContext,
        lease: asyncio.Semaphore | None,
        forward_range: bool = True,
    ) -> Response:
        if lease is not None:
            await lease.acquire()
        client = httpx.AsyncClient(timeout=30.0, trust_env=False, follow_redirects=True)
        headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
        range_header = request.request.headers.get("range") if forward_range else None
        if range_header:
            headers["Range"] = range_header
        try:
            upstream = await client.send(
                client.build_request(request.request.method, url, headers=headers),
                stream=True,
            )
        except asyncio.CancelledError:
            await client.aclose()
            if lease is not None:
                lease.release()
            raise
        except httpx.RequestError as exc:
            await client.aclose()
            if lease is not None:
                lease.release()
            raise Cloud115RequestError("115 播放资源网络读取失败") from exc
        accepted_statuses = {200, 206, 416} if forward_range else {200}
        if upstream.status_code not in accepted_statuses:
            status_code = upstream.status_code
            await upstream.aclose()
            await client.aclose()
            if lease is not None:
                lease.release()
            raise _UpstreamStatus(status_code)
        headers = (
            {
                key.title(): value
                for key, value in upstream.headers.items()
                if key.lower() in _RELAY_HEADERS
            }
            if forward_range
            else {"Cache-Control": "no-store"}
        )
        if upstream.status_code == 416 or request.request.method.upper() == "HEAD":
            await upstream.aclose()
            await client.aclose()
            if lease is not None:
                lease.release()
            return Response(
                status_code=upstream.status_code,
                media_type=upstream.headers.get("content-type"),
                headers=headers,
            )

        async def stream():
            try:
                async for chunk in upstream.aiter_bytes():
                    yield chunk
            finally:
                await upstream.aclose()
                await client.aclose()
                if lease is not None:
                    lease.release()

        return StreamingResponse(
            stream(),
            status_code=upstream.status_code,
            media_type=upstream.headers.get("content-type"),
            headers=headers,
        )

    @staticmethod
    def _pickcode(media: MediaHandle, *, operation: str) -> str:
        storage_ref = media.storage_ref
        if not isinstance(storage_ref, dict) or storage_ref.get("kind") != "cloud115_media":
            raise _provider_error(operation, "source_not_found", "115 媒体引用无效")
        pickcode = storage_ref.get("pickcode")
        if not isinstance(pickcode, str) or not pickcode:
            raise _provider_error(operation, "source_not_found", "115 媒体引用无效")
        return pickcode

    @staticmethod
    def _render_hls_playlist(entry: _HlsEntry, context: PlaybackContext) -> str:
        target_duration = max(1, math.ceil(max(segment.duration_seconds for segment in entry.segments)))
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{target_duration}",
            "#EXT-X-MEDIA-SEQUENCE:0",
        ]
        for segment in entry.segments:
            lines.append(f"#EXTINF:{segment.duration_seconds:.3f},")
            lines.append(context.url_for(f"hls/{entry.token}/segment/{segment.index}.ts"))
        lines.append("#EXT-X-ENDLIST")
        return "\n".join(lines) + "\n"

    @staticmethod
    def _render_merged_hls_playlist(
        entry: _MergedHlsEntry, context: PlaybackContext
    ) -> str:
        target_duration = max(
            1,
            math.ceil(
                max(
                    segment.duration_seconds
                    for segments in entry.segments_by_part
                    for segment in segments
                )
            ),
        )
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            "#EXT-X-PLAYLIST-TYPE:VOD",
            f"#EXT-X-TARGETDURATION:{target_duration}",
            "#EXT-X-MEDIA-SEQUENCE:0",
        ]
        for part_index, segments in enumerate(entry.segments_by_part):
            if part_index:
                lines.append("#EXT-X-DISCONTINUITY")
            for segment in segments:
                lines.append(f"#EXTINF:{segment.duration_seconds:.3f},")
                lines.append(
                    context.url_for(
                        f"merged-hls/{entry.token}/part/{part_index}/segment/{segment.index}.ts"
                    )
                )
        lines.append("#EXT-X-ENDLIST")
        return "\n".join(lines) + "\n"


def _provider_error(
    operation: str,
    code: str,
    message: str,
    *,
    retryable: bool = False,
) -> ProviderOperationError:
    return ProviderOperationError(
        provider_key="cloud115",
        operation=operation,
        code=code,  # type: ignore[arg-type]
        safe_message=message,
        retryable=retryable,
    )


def _cloud_error(operation: str, exc: Cloud115Error) -> ProviderOperationError:
    if isinstance(exc, Cloud115NotFoundError):
        return _provider_error(operation, "source_not_found", "115 文件不存在")
    if isinstance(exc, Cloud115AuthError):
        return _provider_error(operation, "authentication_failed", "115 登录已失效")
    return _provider_error(operation, "unavailable", "115 服务暂不可用", retryable=True)
