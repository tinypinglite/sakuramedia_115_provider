"""115 playback delivery: original-file redirect or local Range relay."""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import httpx
from loguru import logger
from src.plugins.provider_protocol import (
    MediaHandle,
    PlaybackContext,
    ProviderOperationError,
)
from starlette.responses import RedirectResponse, Response, StreamingResponse

from .cloud115 import Cloud115Client, Cloud115DirectUrl
from .exceptions import (
    Cloud115AuthError,
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115RequestError,
)

_BROWSER_USER_AGENT = Cloud115Client.DEFAULT_USER_AGENT
_IOS_APP_USER_AGENT = Cloud115Client.IOS_APP_USER_AGENT
_DIRECT_URL_CACHE_TTL_SECONDS = 10 * 60
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
    # 服务端原文件代理由浏览器 UA 的 c=2 链接承载；该链接每 URL 只允许 2 路并发。
    # iOS 客户端的官方直链走 redirect，不经过这个本地 relay。
    slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(2))


class _PlaybackCache:
    """Bounded, process-local cache. It never writes signed 115 URLs to disk."""

    def __init__(self) -> None:
        self._direct: OrderedDict[tuple[object, ...], _DirectEntry] = OrderedDict()

    def direct_for(self, key: tuple[object, ...]) -> _DirectEntry | None:
        entry = self._direct.get(key)
        if entry is None or entry.usable_until <= time.monotonic():
            self._direct.pop(key, None)
            return None
        self._direct.move_to_end(key)
        return entry

    def put_direct(
        self, key: tuple[object, ...], direct: Cloud115DirectUrl
    ) -> _DirectEntry:
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


_CACHE = _PlaybackCache()


class _UpstreamStatus(RuntimeError):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class Cloud115Playback:
    def __init__(self, *, device_cookie: str) -> None:
        self._device_cookie = device_cookie
        self._credential_fingerprint = hashlib.sha256(
            device_cookie.encode("utf-8")
        ).hexdigest()

    async def handle(self, *, media: MediaHandle, context: PlaybackContext) -> Response:
        pickcode = self._pickcode(media, operation="playback")
        if context.resource_path:
            raise _provider_error(
                "playback", "source_not_found", "115 播放资源不存在"
            )
        if context.delivery == "redirect":
            user_agent = context.request.headers.get("user-agent")
            if not user_agent:
                raise _provider_error(
                    "playback",
                    "unsupported",
                    "直连播放需要播放器提供 User-Agent",
                )
            return await self._redirect(
                media=media,
                pickcode=pickcode,
                user_agent=user_agent,
            )
        return await self._direct_relay(
            media=media,
            pickcode=pickcode,
            context=context,
        )

    async def _redirect(
        self, *, media: MediaHandle, pickcode: str, user_agent: str
    ) -> Response:
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
        key = self._direct_key(
            media=media,
            pickcode=pickcode,
            user_agent=user_agent,
        )
        entry = _CACHE.direct_for(key)
        if entry is not None:
            return key, entry
        started = time.monotonic()
        logger.info(
            "115 原文件播放地址解析开始 library_id={} media_id={}",
            media.library.library_id,
            media.media_id,
        )
        try:
            async with Cloud115Client(self._device_cookie) as client:
                use_official = user_agent == _IOS_APP_USER_AGENT
                direct = await client.get_download_url(
                    pickcode,
                    user_agent=user_agent,
                    prefer_official=use_official,
                    fallback_to_downurl=not use_official,
                )
        except Cloud115Error as exc:
            raise _cloud_error("playback", exc) from exc
        logger.info(
            "115 原文件播放地址解析完成 library_id={} media_id={} elapsed_seconds={:.2f}",
            media.library.library_id,
            media.media_id,
            time.monotonic() - started,
        )
        return key, _CACHE.put_direct(key, direct)

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
                    raise _provider_error(
                        "playback",
                        "unavailable",
                        "115 直链读取失败",
                        retryable=True,
                    ) from exc
                logger.warning(
                    "115 原文件播放地址失效，重新解析 library_id={} media_id={} status={}",
                    media.library.library_id,
                    media.media_id,
                    exc.status_code,
                )
                _CACHE.discard_direct(key)
                _, entry = await self._direct_entry(
                    media=media,
                    pickcode=pickcode,
                    user_agent=_BROWSER_USER_AGENT,
                )
            except Cloud115RequestError as exc:
                raise _provider_error(
                    "playback",
                    "unavailable",
                    "115 直链读取失败",
                    retryable=True,
                ) from exc
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
        client = httpx.AsyncClient(
            timeout=30.0,
            trust_env=False,
            follow_redirects=True,
        )
        headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
        range_header = (
            request.request.headers.get("range") if forward_range else None
        )
        if range_header:
            headers["Range"] = range_header
        try:
            upstream = await client.send(
                client.build_request(
                    request.request.method,
                    url,
                    headers=headers,
                ),
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
            except httpx.RequestError as exc:
                logger.warning(
                    "115 播放流传输中断 resource={} error_type={}",
                    request.resource_path or "original",
                    type(exc).__name__,
                )
                raise
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
        if (
            not isinstance(storage_ref, dict)
            or storage_ref.get("kind") != "cloud115_media"
        ):
            raise _provider_error(
                operation,
                "source_not_found",
                "115 媒体引用无效",
            )
        pickcode = storage_ref.get("pickcode")
        if not isinstance(pickcode, str) or not pickcode:
            raise _provider_error(
                operation,
                "source_not_found",
                "115 媒体引用无效",
            )
        return pickcode


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
        return _provider_error(
            operation,
            "authentication_failed",
            "115 登录已失效",
        )
    return _provider_error(
        operation,
        "unavailable",
        "115 服务暂不可用",
        retryable=True,
    )
