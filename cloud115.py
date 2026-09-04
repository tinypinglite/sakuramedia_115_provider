"""Minimal async 115 client used only by this provider plugin.

It deliberately covers the provider's four needs: folders/files, offline
tasks, direct links, and HLS playlists.  It has no dependency on the host's
legacy cloud115 implementation.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import re
import threading
import time
from collections.abc import AsyncIterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Protocol
from urllib.parse import parse_qsl, urljoin, urlsplit

import httpx
from typing_extensions import Self

from .cipher import (
    decrypt_downurl_payload,
    decrypt_upload_response,
    encrypt_downurl_payload,
    make_upload_payload,
)
from .exceptions import (
    Cloud115AuthError,
    Cloud115DuplicateNameError,
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115OfflineTaskExistsError,
    Cloud115RequestError,
    Cloud115RiskControlError,
    Cloud115VideoUnavailableError,
)

_AUTH_ERRNOS = {99, 911, 50003, 50004, 99999, 990009, 990017, 20130827}
_NOT_FOUND_ERRNOS = {20121, 20125, 990002, 4100003, 4100008}
_DUPLICATE_NAME_ERRNOS = {20004}
_M3U8_ATTR = re.compile(r'([A-Z0-9-]+)=(?:"([^"]*)"|([^,]*))')
_UPLOAD_CHUNK_BYTES = 1024 * 1024


class _TransferReader(Protocol):
    """The deliberately narrow cross-provider reader contract."""

    def read(self, size: int = -1) -> bytes: ...

    def seek(self, offset: int, whence: int = 0) -> int: ...


class _TransferSource(Protocol):
    def open_reader(self) -> AbstractContextManager[_TransferReader]: ...

    def assert_unchanged(self) -> None: ...


_WEBAPI_PACE_LOCK = threading.Lock()
_WEBAPI_NEXT_REQUEST_AT: dict[str, float] = {}
_WEBAPI_BATCH_SIZE = 30
_WEBAPI_BATCH_DELAY_RANGE = (10.0, 30.0)
_NGINX_HEADER_TOO_LARGE_MARKER = "Request Header Or Cookie Too Large"


@dataclass(frozen=True, slots=True)
class Cloud115Entry:
    entry_id: str
    parent_id: str
    name: str
    is_dir: bool
    size_bytes: int
    sha1: str | None
    pickcode: str
    modified_at: int
    is_video: bool


@dataclass(frozen=True, slots=True)
class Cloud115DirectoryInfo:
    name: str
    ancestors: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class Cloud115DirectUrl:
    file_id: str
    file_name: str
    file_size_bytes: int
    sha1: str
    pickcode: str
    url: str
    user_agent: str
    expires_at: int


@dataclass(frozen=True, slots=True)
class Cloud115VideoDefinition:
    bandwidth: int
    resolution: str
    label: str
    playlist_url: str


@dataclass(frozen=True, slots=True)
class Cloud115VideoSegment:
    index: int
    url: str
    duration_seconds: float


@dataclass(frozen=True, slots=True)
class Cloud115VideoInfo:
    definitions: tuple[Cloud115VideoDefinition, ...]


@dataclass(frozen=True, slots=True)
class Cloud115OfflineTask:
    info_hash: str
    name: str
    status: int
    progress: float
    file_id: str
    pickcode: str
    save_dir_id: str


@dataclass(frozen=True, slots=True)
class Cloud115RapidUploadResult:
    status: Literal["success", "not_hit"]
    sha1: str
    entry: Cloud115Entry | None = None


@dataclass
class TransferState:
    userkeys: dict[str, str] = field(default_factory=dict)
    app_version: str | None = None
    webapi_count: int = 0
    request_at: float = 0.0


class Cloud115Client:
    """Per-operation HTTP client with a pruned 115 cookie jar."""

    _MAX_RETRIES = 2
    _RETRY_BACKOFF_STEP = 0.5
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
    ESSENTIAL_COOKIE_KEYS = frozenset({"UID", "CID", "SEID", "KID", "acw_tc"})
    _UID_PATTERN = re.compile(r"^(\d+)_")
    _UID_SSOENT_PATTERN = re.compile(r"^\d+_([A-Z]\d)_")
    _RAPID_UPLOAD_PROTOCOL_BY_SSOENT: ClassVar[dict[str, str]] = {
        "F1": "android",
        "R1": "web",
        "R2": "web",
    }
    _UPLOAD_APP_VERSION_URL = "https://appversion.115.com/1.0/web/1.0/api/getMultiVer"

    def __init__(
        self,
        cookies: str,
        *,
        user_agent: str | None = None,
        timeout: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
        pace_webapi: bool = True,
        batch_pacing: bool = False,
        transfer_state: TransferState | None = None,
    ) -> None:
        self._cookies = self._keep_essential(self.parse_cookies(cookies))
        uid = self._cookies.get("UID", "")
        match = self._UID_PATTERN.match(uid)
        if match is None:
            raise Cloud115AuthError("115 cookie 缺少有效 UID")
        self.user_id = match.group(1)
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self._pace_webapi_requests = pace_webapi
        self._batch_pacing = batch_pacing
        self._transfer_state = transfer_state if transfer_state is not None else TransferState()
        self._upload_userkey_lock = asyncio.Lock()
        self._upload_app_version_lock = asyncio.Lock()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=timeout,
            trust_env=False,
            follow_redirects=True,
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def parse_cookies(value: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for part in value.split(";"):
            key, separator, cookie_value = part.strip().partition("=")
            if separator and key:
                result[key.strip()] = cookie_value.strip()
        return result

    @classmethod
    def _keep_essential(cls, values: dict[str, str]) -> dict[str, str]:
        return {key: value for key, value in values.items() if key in cls.ESSENTIAL_COOKIE_KEYS}

    def snapshot_cookies(self) -> str:
        return "; ".join(f"{key}={value}" for key, value in self._cookies.items())

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Cookie": self.snapshot_cookies(),
            "User-Agent": self.user_agent,
        }
        if extra:
            headers.update(extra)
        return headers

    def _merge_set_cookies(self, response: httpx.Response) -> None:
        for line in response.headers.get_list("set-cookie"):
            key, separator, value = line.partition("=")
            if not separator or key not in self.ESSENTIAL_COOKIE_KEYS:
                continue
            cookie_value = value.split(";", 1)[0].strip()
            if cookie_value:
                self._cookies[key] = cookie_value
            else:
                self._cookies.pop(key, None)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, str] | bytes | None = None,
        headers: dict[str, str] | None = None,
        retryable: bool | None = None,
    ) -> httpx.Response:
        should_retry = (
            method.upper() in {"GET", "HEAD", "OPTIONS"}
            if retryable is None
            else retryable
        )
        max_retries = self._MAX_RETRIES if should_retry else 0
        for attempt in range(max_retries + 1):
            await self._pace_webapi(url)
            try:
                request_kwargs: dict[str, object] = {
                    "params": params,
                    "headers": self._headers(headers),
                }
                if isinstance(data, bytes):
                    request_kwargs["content"] = data
                else:
                    request_kwargs["data"] = data
                response = await self._client.request(method, url, **request_kwargs)
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
            ) as exc:
                if attempt >= max_retries:
                    raise Cloud115RequestError("115 网络请求失败") from exc
                await asyncio.sleep(self._RETRY_BACKOFF_STEP * (attempt + 1))
                continue
            except httpx.RequestError as exc:
                raise Cloud115RequestError("115 网络请求失败") from exc
            self._merge_set_cookies(response)
            if response.status_code in {401, 403}:
                raise Cloud115AuthError("115 登录已失效")
            if (
                response.status_code == 400
                and _NGINX_HEADER_TOO_LARGE_MARKER in response.text
            ):
                raise Cloud115RequestError("115 Cookie 请求头过大")
            if response.status_code == 405 or (
                response.status_code == 400
                and urlsplit(url).netloc == "webapi.115.com"
            ):
                raise Cloud115RiskControlError(
                    f"115 请求触发风控（HTTP {response.status_code}）"
                )
            if 500 <= response.status_code < 600 and attempt < max_retries:
                await asyncio.sleep(self._RETRY_BACKOFF_STEP * (attempt + 1))
                continue
            if not 200 <= response.status_code < 300:
                raise Cloud115RequestError(
                    f"115 请求失败（HTTP {response.status_code}）"
                )
            return response
        raise AssertionError("unreachable 115 request retry state")

    async def _pace_webapi(self, url: str) -> None:
        if not self._pace_webapi_requests:
            return
        is_webapi = urlsplit(url).netloc == "webapi.115.com"
        state = self._transfer_state
        with _WEBAPI_PACE_LOCK:
            now = time.monotonic()
            request_at = max(now, _WEBAPI_NEXT_REQUEST_AT.get(self.user_id, 0.0))
            if self._batch_pacing:
                request_at = max(request_at, state.request_at)
                if is_webapi and state.webapi_count >= _WEBAPI_BATCH_SIZE:
                    request_at = max(
                        request_at, now + random.uniform(*_WEBAPI_BATCH_DELAY_RANGE)
                    )
                    state.webapi_count = 0
                state.request_at = request_at
                if is_webapi:
                    state.webapi_count += 1
            _WEBAPI_NEXT_REQUEST_AT[self.user_id] = request_at + random.uniform(
                1.0, 3.0
            )
        if request_at > now:
            await asyncio.sleep(request_at - now)

    async def _json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        allow_missing_state: bool = False,
        retryable: bool | None = None,
    ) -> dict[str, Any]:
        response = await self._request(
            method,
            url,
            params=params,
            data=data,
            headers=headers,
            retryable=retryable,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise Cloud115RequestError("115 返回了无效 JSON") from exc
        if not isinstance(payload, dict):
            raise Cloud115RequestError("115 返回了无效数据")
        if "state" in payload and not payload["state"]:
            raise self._error_from_payload(payload, endpoint=url)
        if not allow_missing_state and "state" not in payload:
            raise Cloud115RequestError("115 返回缺少状态字段")
        return payload

    @staticmethod
    def _error_from_payload(payload: dict[str, Any], *, endpoint: str) -> Cloud115Error:
        raw_errcode = payload.get("errcode")
        try:
            errcode = int(raw_errcode)
        except (TypeError, ValueError):
            errcode = None
        raw_errno = payload.get("errno") or payload.get("errNo") or payload.get("code")
        try:
            errno = int(raw_errno)
        except (TypeError, ValueError):
            errno = None
        message = str(
            payload.get("error")
            or payload.get("error_msg")
            or payload.get("message")
            or payload.get("msg")
            or "115 请求被拒绝"
        )
        if errno in _AUTH_ERRNOS:
            return Cloud115AuthError(message)
        endpoint_path = urlsplit(endpoint).path
        if (
            errno in _NOT_FOUND_ERRNOS
            or (endpoint_path == "/category/get" and errno == 70005)
            or (endpoint_path == "/files/get_info" and errno == 20018)
        ):
            return Cloud115NotFoundError(message)
        if errno in _DUPLICATE_NAME_ERRNOS:
            return Cloud115DuplicateNameError(message)
        errtype = payload.get("errtype")
        if errtype is None and endpoint_path == "/web/lixian/":
            results = payload.get("result")
            if isinstance(results, list) and len(results) == 1 and isinstance(results[0], dict):
                errtype = results[0].get("errtype")
        if errcode == 10008 and str(errtype).lower() == "war":
            return Cloud115OfflineTaskExistsError(message)
        return Cloud115RequestError(message)

    async def check_alive(self) -> bool:
        url = "https://my.115.com/"
        try:
            response = await self._client.request(
                "GET",
                url,
                params={"ct": "guide", "ac": "status"},
                headers=self._headers(),
                follow_redirects=False,
            )
        except httpx.RequestError as exc:
            raise Cloud115RequestError("115 登录状态探测失败") from exc
        self._merge_set_cookies(response)
        if response.status_code in {302, 401, 403}:
            return False
        if response.status_code == 405:
            raise Cloud115RiskControlError("115 请求触发风控（HTTP 405）")
        if not 200 <= response.status_code < 300:
            raise Cloud115RequestError(f"115 登录状态探测失败（HTTP {response.status_code}）")
        try:
            payload = response.json()
        except ValueError as exc:
            raise Cloud115RequestError("115 登录状态探测返回无效 JSON") from exc
        if not isinstance(payload, dict) or "state" not in payload:
            raise Cloud115RequestError("115 登录状态探测返回无效数据")
        return payload["state"] is True

    async def list_dir(
        self,
        cid: str,
        *,
        offset: int = 0,
        limit: int = 1000,
    ) -> tuple[tuple[Cloud115Entry, ...], int]:
        if not cid or limit < 1 or limit > 1150:
            raise ValueError("invalid 115 directory page")
        payload = await self._json(
            "GET",
            "https://webapi.115.com/files",
            params={
                "aid": 1,
                "cid": cid,
                "offset": offset,
                "limit": limit,
                "show_dir": 1,
            },
        )
        return self._directory_page(payload, cid=cid, offset=offset)

    @classmethod
    def _directory_page(cls, payload: dict[str, Any], *, cid: str, offset: int):
        response_cid = payload.get("cid")
        if response_cid is not None and str(response_cid) != str(cid):
            raise Cloud115NotFoundError("115 目录不存在")
        data = payload.get("data")
        try:
            total = int(payload["count"])
        except (KeyError, TypeError, ValueError):
            raise Cloud115RequestError("115 目录列表缺少有效数量") from None
        # 清理和有效性扫描会依赖完整清单，异常响应不能降级为空目录。
        if (
            not isinstance(data, list)
            or any(not isinstance(item, dict) for item in data)
            or total < 0
            or len(data) > total
            or (not data and offset < total)
        ):
            raise Cloud115RequestError("115 目录列表不完整")
        return tuple(cls._entry(item) for item in data), total

    async def list_directory(self, cid: str) -> tuple[Cloud115Entry, ...]:
        offset = 0
        entries: list[Cloud115Entry] = []
        while True:
            page, total = await self.list_dir(cid, offset=offset)
            entries.extend(page)
            offset += len(page)
            if not page or offset >= total:
                return tuple(entries)

    async def iter_files_recursive(self, cid: str) -> AsyncIterator[Cloud115Entry]:
        """Yield every file below ``cid`` through 115's server-side recursive mode."""
        if not cid:
            raise ValueError("115 directory ID is required")
        offset = 0
        total = -1
        while total < 0 or offset < total:
            payload = await self._json(
                "GET",
                "https://webapi.115.com/files",
                params={
                    "aid": 1,
                    "cid": cid,
                    "offset": offset,
                    "limit": 1150,
                    "show_dir": 0,
                    "cur": 0,
                    "o": "file_name",
                    "asc": 1,
                },
            )
            entries, total = self._directory_page(payload, cid=cid, offset=offset)
            if not entries:
                return
            for entry in entries:
                if not entry.is_dir:
                    yield entry
            offset += len(entries)

    async def directory_info(self, cid: str) -> Cloud115DirectoryInfo:
        if not cid:
            raise ValueError("115 directory ID is required")
        if cid == "0":
            return Cloud115DirectoryInfo(name="根目录", ancestors=())
        payload = await self._json(
            "GET", "https://webapi.115.com/category/get", params={"cid": cid}
        )
        ancestors: list[tuple[str, str]] = []
        for item in payload.get("paths") or []:
            if not isinstance(item, dict):
                continue
            ancestor_id = item.get("file_id") if "file_id" in item else item.get("cid")
            ancestor_name = (
                item.get("file_name") if "file_name" in item else item.get("name")
            )
            ancestors.append(
                (
                    "" if ancestor_id is None else str(ancestor_id),
                    "" if ancestor_name is None else str(ancestor_name),
                )
            )
        return Cloud115DirectoryInfo(
            name=str(payload.get("file_name") or ""),
            ancestors=tuple(ancestors),
        )

    async def file_by_pickcode(self, pickcode: str) -> Cloud115Entry:
        return await self._file_info("pick_code", pickcode)

    async def file_by_id(self, file_id: str) -> Cloud115Entry:
        return await self._file_info("file_id", file_id)

    async def _file_info(self, key: str, value: str) -> Cloud115Entry:
        if not value:
            raise ValueError(f"{key} is required")
        payload = await self._json(
            "GET",
            "https://webapi.115.com/files/get_info",
            params={key: value},
        )
        values = payload.get("data") or []
        if not values or not isinstance(values[0], dict):
            raise Cloud115NotFoundError("115 文件不存在")
        values = dict(values[0])
        if not values.get("fid") and values.get("file_id"):
            values["fid"] = values["file_id"]
        return self._entry(values)

    async def rapid_upload(
        self,
        source: _TransferSource,
        *,
        filename: str,
        size_bytes: int,
        parent_cid: str,
        file_sha1: str | None = None,
    ) -> Cloud115RapidUploadResult:
        """Only perform 115's rapid-upload handshake; never upload file bytes.

        This is intentionally a reader-based API. The client never accepts a
        source path, so a target provider cannot recover the local path from
        the cross-provider transfer protocol.
        """
        if not filename or "/" in filename or "\\" in filename:
            raise ValueError("invalid 115 upload filename")
        if not isinstance(size_bytes, int) or size_bytes < 0 or not parent_cid:
            raise ValueError("invalid 115 rapid upload arguments")
        protocol = self._rapid_upload_protocol()
        if file_sha1 is None:
            file_sha1 = await asyncio.to_thread(self._hash_source, source, size_bytes)
        elif not re.fullmatch(r"[0-9A-Fa-f]{40}", file_sha1):
            raise ValueError("invalid 115 rapid upload SHA1")
        file_sha1 = file_sha1.upper()
        source.assert_unchanged()

        response = await self._upload_init(
            filename=filename,
            filesize=size_bytes,
            filesha1=file_sha1,
            parent_cid=parent_cid,
            upload_protocol=protocol,
        )
        data = self._upload_data(response)
        status = _as_int(data.get("status"))
        if status == 7:
            sign_key = _as_text(data.get("sign_key"))
            sign_check = _as_text(data.get("sign_check"))
            if not sign_key or not sign_check:
                raise Cloud115RequestError(
                    "115 秒传校验响应缺少 sign_key 或 sign_check"
                )
            range_sha1 = await asyncio.to_thread(
                self._hash_source_range, source, size_bytes, sign_check
            )
            source.assert_unchanged()
            response = await self._upload_init(
                filename=filename,
                filesize=size_bytes,
                filesha1=file_sha1,
                parent_cid=parent_cid,
                sign_key=sign_key,
                sign_val=range_sha1,
                upload_protocol=protocol,
            )
            data = self._upload_data(response)
            status = _as_int(data.get("status"))

        if status == 1:
            return Cloud115RapidUploadResult(status="not_hit", sha1=file_sha1)
        if status != 2:
            raise Cloud115RequestError("115 秒传返回了非预期状态")

        # The initupload `fileid` field is commonly the integer placeholder 0.
        # Pickcode is the only usable identifier; wait for its eventual index
        # before claiming success or persisting a receipt.
        pickcode = _first_text(data, "pick_code", "pickcode")
        if not pickcode:
            raise Cloud115RequestError("115 秒传成功响应缺少 pickcode")
        entry = await self._wait_pickcode_indexed(pickcode)
        return Cloud115RapidUploadResult(status="success", sha1=file_sha1, entry=entry)

    def _rapid_upload_protocol(self) -> Literal["web", "android"]:
        uid = self._cookies.get("UID", "")
        match = self._UID_SSOENT_PATTERN.match(uid)
        protocol = self._RAPID_UPLOAD_PROTOCOL_BY_SSOENT.get(match.group(1) if match else "")
        if protocol in {"web", "android"}:
            return protocol
        raise Cloud115AuthError("115 秒传只支持 Android (F1)、微信小程序 (R1) 或支付宝小程序 (R2) Cookie")

    async def _get_upload_userkey(self, protocol: Literal["web", "android"]) -> str:
        if userkey := self._transfer_state.userkeys.get(protocol):
            return userkey
        async with self._upload_userkey_lock:
            if userkey := self._transfer_state.userkeys.get(protocol):
                return userkey
            url = (
                "https://proapi.115.com/android/2.0/user/upload_key"
                if protocol == "android"
                else "https://proapi.115.com/app/uploadinfo"
            )
            payload = await self._json("GET", url)
            data = payload.get("data")
            values = data if isinstance(data, dict) else {}
            userkey = _first_text(payload, "userkey", "user_key") or _first_text(
                values, "userkey", "user_key"
            )
            if not userkey:
                raise Cloud115RequestError("115 未返回上传 userkey")
            self._transfer_state.userkeys[protocol] = userkey
            return userkey

    async def _get_upload_app_version(self) -> str:
        if self._transfer_state.app_version:
            return self._transfer_state.app_version
        async with self._upload_app_version_lock:
            if self._transfer_state.app_version:
                return self._transfer_state.app_version
            payload = await self._json(
                "GET",
                self._UPLOAD_APP_VERSION_URL,
                headers={"Cookie": ""},
                allow_missing_state=True,
            )
            data = payload.get("data")
            android = data.get("Android") if isinstance(data, dict) else None
            version = _as_text(android.get("version_code")) if isinstance(android, dict) else ""
            if not version:
                raise Cloud115RequestError("115 未返回 Android 上传版本")
            self._transfer_state.app_version = version
            return version

    async def _upload_init(
        self,
        *,
        filename: str,
        filesize: int,
        filesha1: str,
        parent_cid: str,
        sign_key: str = "",
        sign_val: str = "",
        upload_protocol: Literal["web", "android"],
    ) -> dict[str, object]:
        userkey, appversion = await asyncio.gather(
            self._get_upload_userkey(upload_protocol), self._get_upload_app_version()
        )
        params, body = make_upload_payload(
            {
                "appid": 0,
                "appversion": appversion,
                "fileid": filesha1.upper(),
                "filename": filename,
                "filesize": filesize,
                "target": f"U_1_{parent_cid}",
                "sign_key": sign_key,
                "sign_val": sign_val,
                "topupload": "true",
                "userid": self.user_id,
                "userkey": userkey,
            }
        )
        response = await self._request(
            "POST",
            "https://uplb.115.com/4.0/initupload.php",
            params=params,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://115.com",
                "Referer": "https://115.com/",
                "User-Agent": (
                    f"Mozilla/5.0 115disk/{appversion} 115Browser/{appversion} "
                    f"115wangpan_android/{appversion}"
                ),
            },
            retryable=False,
        )
        try:
            payload = decrypt_upload_response(response.content)
        except Cloud115Error:
            raise
        except Exception as exc:
            raise Cloud115RequestError("115 秒传初始化响应无法解密") from exc
        if "state" in payload and not payload["state"]:
            raise self._error_from_payload(
                payload, endpoint="https://uplb.115.com/4.0/initupload.php"
            )
        return payload

    @staticmethod
    def _upload_data(payload: dict[str, object]) -> dict[str, object]:
        data = payload.get("data")
        if not isinstance(data, dict):
            # uplb 历史响应直接返回 status/statuscode，没有 state/data 包装。
            if "status" not in payload:
                raise Cloud115RequestError("115 秒传初始化响应缺少 data")
            data = payload
        try:
            statuscode = int(data.get("statuscode", 0))
        except (TypeError, ValueError):
            raise Cloud115RequestError("115 秒传返回了无效错误码") from None
        if statuscode not in {0, 701}:
            raise Cloud115RequestError("115 秒传初始化被拒绝")
        return data

    async def _wait_pickcode_indexed(self, pickcode: str) -> Cloud115Entry:
        last_error: Cloud115NotFoundError | None = None
        for delay in (0.0, 0.3, 0.8, 1.5, 2.5):
            if delay:
                await asyncio.sleep(delay)
            try:
                return await self.file_by_pickcode(pickcode)
            except Cloud115NotFoundError as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    @staticmethod
    def _hash_source(source: _TransferSource, size_bytes: int) -> str:
        digest = hashlib.sha1()
        remaining = size_bytes
        with source.open_reader() as reader:
            while remaining:
                chunk = reader.read(min(_UPLOAD_CHUNK_BYTES, remaining))
                if not chunk:
                    raise Cloud115RequestError("传输源读取不足，无法计算 115 SHA1")
                if len(chunk) > remaining:
                    raise Cloud115RequestError("传输源大小与声明不一致")
                digest.update(chunk)
                remaining -= len(chunk)
            if reader.read(1):
                raise Cloud115RequestError("传输源大小与声明不一致")
        return digest.hexdigest().upper()

    @staticmethod
    def _hash_source_range(
        source: _TransferSource, size_bytes: int, sign_check: str
    ) -> str:
        start, end = _parse_sign_check(sign_check, size_bytes)
        digest = hashlib.sha1()
        remaining = end - start + 1
        with source.open_reader() as reader:
            reader.seek(start)
            while remaining:
                chunk = reader.read(min(_UPLOAD_CHUNK_BYTES, remaining))
                if not chunk:
                    raise Cloud115RequestError("115 sign_check 超出传输源范围")
                if len(chunk) > remaining:
                    raise Cloud115RequestError("115 sign_check 读取长度异常")
                digest.update(chunk)
                remaining -= len(chunk)
        return digest.hexdigest().upper()

    async def mkdir(self, parent_cid: str, name: str) -> str:
        payload = await self._json(
            "POST",
            "https://webapi.115.com/files/add",
            data={"pid": parent_cid, "cname": name},
        )
        cid = payload.get("category_id") or payload.get("cid") or payload.get("file_id")
        if not cid:
            raise Cloud115RequestError("115 创建目录没有返回目录 ID")
        return str(cid)


    async def copy_files(self, file_ids: list[str], *, parent_cid: str) -> None:
        await self._file_operation("copy", file_ids, parent_cid)

    async def move_files(self, file_ids: list[str], *, parent_cid: str) -> None:
        await self._file_operation("move", file_ids, parent_cid)

    async def _file_operation(self, operation: str, file_ids: list[str], parent_cid: str) -> None:
        if not file_ids or not parent_cid:
            raise ValueError("file IDs and destination directory are required")
        data = {"pid": parent_cid}
        data.update({f"fid[{index}]": file_id for index, file_id in enumerate(file_ids)})
        await self._json("POST", f"https://webapi.115.com/files/{operation}", data=data)

    async def delete_files(self, file_ids: list[str], *, parent_cid: str | None = None) -> None:
        if not file_ids:
            raise ValueError("file IDs are required")
        data = {f"fid[{index}]": file_id for index, file_id in enumerate(file_ids)}
        if parent_cid:
            data["pid"] = parent_cid
        await self._json("POST", "https://webapi.115.com/rb/delete", data=data)

    async def get_download_url(self, pickcode: str, *, user_agent: str) -> Cloud115DirectUrl:
        if not pickcode or not user_agent:
            raise ValueError("pickcode and user_agent are required")
        payload = await self._json(
            "POST",
            "https://proapi.115.com/app/chrome/downurl",
            data={
                "data": encrypt_downurl_payload(
                    {"pickcode": pickcode, "user_id": self.user_id}
                )
            },
            headers={"User-Agent": user_agent, "Referer": "https://115.com/"},
        )
        encrypted = payload.get("data")
        if not isinstance(encrypted, str) or not encrypted:
            raise Cloud115NotFoundError("115 未返回文件直链")
        decoded = decrypt_downurl_payload(encrypted)
        if not decoded:
            raise Cloud115NotFoundError("115 未返回文件直链")
        file_id, value = next(iter(decoded.items()))
        if not isinstance(value, dict):
            raise Cloud115NotFoundError("115 文件不可访问")
        url_value = value.get("url")
        if not isinstance(url_value, dict) or not isinstance(url_value.get("url"), str):
            raise Cloud115NotFoundError("115 文件不可访问")
        url = url_value["url"]
        return Cloud115DirectUrl(
            file_id=str(file_id),
            file_name=str(value.get("file_name") or ""),
            file_size_bytes=int(value.get("file_size") or 0),
            sha1=str(value.get("sha1") or ""),
            pickcode=str(value.get("pick_code") or pickcode),
            url=url,
            user_agent=user_agent,
            expires_at=self._direct_url_expiry(url),
        )

    async def download_bytes(
        self,
        pickcode: str,
        *,
        user_agent: str,
        max_bytes: int,
    ) -> bytes:
        direct = await self.get_download_url(pickcode, user_agent=user_agent)
        received = 0
        chunks: list[bytes] = []
        try:
            async with self._client.stream(
                "GET", direct.url, headers={"User-Agent": direct.user_agent}
            ) as response:
                if response.status_code == 405:
                    raise Cloud115RiskControlError("115 请求触发风控（HTTP 405）")
                if response.status_code not in {200, 206}:
                    raise Cloud115RequestError("115 文件读取失败")
                async for chunk in response.aiter_bytes():
                    received += len(chunk)
                    if received > max_bytes:
                        raise Cloud115RequestError("115 文件超过读取大小上限")
                    chunks.append(chunk)
        except httpx.RequestError as exc:
            raise Cloud115RequestError("115 文件读取失败") from exc
        return b"".join(chunks)

    async def get_video_info(self, pickcode: str) -> Cloud115VideoInfo:
        payload = await self._json(
            "GET",
            "https://webapi.115.com/files/video",
            params={"pickcode": pickcode},
        )
        try:
            raw_status = payload.get("file_status")
            status = 1 if raw_status is None else int(raw_status)
        except (TypeError, ValueError):
            status = 1
        if status != 1:
            raise Cloud115VideoUnavailableError("115 视频转码尚未就绪")
        master_url = payload.get("video_url")
        if not isinstance(master_url, str) or not master_url:
            raise Cloud115VideoUnavailableError("115 未提供 HLS 播放列表")
        text = await self._text(master_url)
        definitions: list[Cloud115VideoDefinition] = []
        attributes: dict[str, str] | None = None
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if line.startswith("#EXT-X-STREAM-INF:"):
                attributes = self._playlist_attributes(line.partition(":")[2])
                continue
            if not line or line.startswith("#"):
                continue
            current = attributes or {}
            attributes = None
            try:
                bandwidth = int(current.get("BANDWIDTH") or 0)
            except ValueError:
                bandwidth = 0
            definitions.append(
                Cloud115VideoDefinition(
                    bandwidth=bandwidth,
                    resolution=current.get("RESOLUTION", ""),
                    label=current.get("NAME", ""),
                    playlist_url=urljoin(master_url, line),
                )
            )
        if not definitions:
            raise Cloud115VideoUnavailableError("115 HLS 播放列表为空")
        return Cloud115VideoInfo(definitions=tuple(definitions))

    async def get_video_segments(
        self, definition: Cloud115VideoDefinition
    ) -> tuple[Cloud115VideoSegment, ...]:
        text = await self._text(definition.playlist_url)
        segments: list[Cloud115VideoSegment] = []
        duration: float | None = None
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if line.startswith("#EXTINF:"):
                try:
                    duration = float(line.partition(":")[2].split(",", 1)[0])
                except ValueError:
                    duration = 0.0
                continue
            if not line or line.startswith("#"):
                continue
            segments.append(
                Cloud115VideoSegment(
                    index=len(segments),
                    url=urljoin(definition.playlist_url, line),
                    duration_seconds=duration or 0.0,
                )
            )
            duration = None
        if not segments:
            raise Cloud115VideoUnavailableError("115 HLS 分片为空")
        return tuple(segments)

    async def list_offline_tasks(self, *, page: int = 1) -> tuple[tuple[Cloud115OfflineTask, ...], int]:
        payload = await self._json(
            "GET",
            "https://115.com/web/lixian/",
            params={"ct": "lixian", "ac": "task_lists", "page": page, "page_size": 50},
            allow_missing_state=True,
        )
        tasks = tuple(
            self._offline_task(item)
            for item in (payload.get("tasks") or [])
            if isinstance(item, dict)
        )
        return tasks, max(1, int(payload.get("page_count") or 1))

    async def add_offline_url(self, source_uri: str, *, save_dir_id: str) -> str:
        payload = await self._json(
            "POST",
            "https://115.com/web/lixian/",
            params={"ct": "lixian", "ac": "add_task_urls"},
            data={"wp_path_id": save_dir_id, "url[0]": source_uri},
        )
        results = payload.get("result") or []
        if not results or not isinstance(results[0], dict):
            raise Cloud115RequestError("115 离线下载没有返回任务 ID")
        info_hash = results[0].get("info_hash")
        if not isinstance(info_hash, str) or not info_hash:
            raise Cloud115RequestError("115 离线下载没有返回任务 ID")
        return info_hash

    async def delete_offline_task(self, info_hash: str, *, delete_files: bool) -> None:
        await self._json(
            "POST",
            "https://115.com/web/lixian/",
            params={"ct": "lixian", "ac": "task_del"},
            data={"flag": "1" if delete_files else "0", "hash[0]": info_hash},
        )

    async def _text(self, url: str) -> str:
        return (await self._request("GET", url)).text

    @staticmethod
    def _playlist_attributes(value: str) -> dict[str, str]:
        return {
            match.group(1): match.group(2) if match.group(2) is not None else match.group(3)
            for match in _M3U8_ATTR.finditer(value)
        }

    @staticmethod
    def _direct_url_expiry(url: str) -> int:
        try:
            for key, value in parse_qsl(urlsplit(url).query):
                if key == "t" and value.isdigit():
                    return int(value)
        except ValueError:
            pass
        return 0

    @staticmethod
    def _entry(value: dict[str, Any]) -> Cloud115Entry:
        is_dir = "fid" not in value
        entry_id = value.get("cid") if is_dir else value.get("fid")
        parent_id = value.get("pid") if is_dir else value.get("cid")
        return Cloud115Entry(
            entry_id="" if entry_id is None else str(entry_id),
            parent_id="" if parent_id is None else str(parent_id),
            name=str(value.get("n") or ""),
            is_dir=is_dir,
            size_bytes=int(value.get("s") or 0),
            sha1=str(value.get("sha")) if value.get("sha") else None,
            pickcode=str(value.get("pc") or ""),
            modified_at=int(value.get("te") or 0),
            is_video=bool(value.get("iv")) if not is_dir else False,
        )

    @staticmethod
    def _offline_task(value: dict[str, Any]) -> Cloud115OfflineTask:
        try:
            progress = float(value.get("percentDone") or value.get("display_percent") or 0) / 100
        except (TypeError, ValueError):
            progress = 0.0
        return Cloud115OfflineTask(
            info_hash=str(value.get("info_hash") or ""),
            name=str(value.get("name") or ""),
            status=int(value.get("status") if value.get("status") is not None else 0),
            progress=min(1.0, max(0.0, progress)),
            file_id=str(value.get("file_id") or ""),
            pickcode=str(value.get("pick_code") or ""),
            save_dir_id=str(value.get("wp_path_id") or ""),
        )


def _as_text(value: object) -> str:
    return "" if value is None or value == "" else str(value)


def _as_int(value: object) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _first_text(values: dict[str, object], *keys: str) -> str:
    for key in keys:
        value = _as_text(values.get(key))
        if value:
            return value
    return ""


def _parse_sign_check(value: str, size_bytes: int) -> tuple[int, int]:
    try:
        start_text, end_text = value.split("-", 1)
        start, end = int(start_text), int(end_text)
    except (AttributeError, TypeError, ValueError) as exc:
        raise Cloud115RequestError("115 sign_check 格式无效") from exc
    if start < 0 or end < start or end >= size_bytes:
        raise Cloud115RequestError("115 sign_check 超出传输源范围")
    return start, end


async def exchange_web_cookie_for_alipaymini(web_cookie: str) -> str:
    """Use an authenticated web session to create the dedicated R2 cookie."""
    async with Cloud115Client(web_cookie) as client:
        if not await client.check_alive():
            raise Cloud115AuthError("115 Web Cookie 已失效")
        token = await client._json(
            "GET", "https://qrcodeapi.115.com/api/1.0/web/1.0/token/"
        )
        token_data = token.get("data")
        if not isinstance(token_data, dict) or not isinstance(token_data.get("uid"), str):
            raise Cloud115RequestError("115 未返回设备登录令牌")
        uid = token_data["uid"]
        prompt = await client._json(
            "GET",
            "https://qrcodeapi.115.com/api/2.0/prompt.php",
            params={"uid": uid},
        )
        prompt_data = prompt.get("data")
        if not isinstance(prompt_data, dict):
            raise Cloud115RequestError("115 未确认设备登录")
        action_url = prompt_data.get("do_url")
        action_params = prompt_data.get("do_params")
        if not isinstance(action_url, str) or not isinstance(action_params, dict):
            raise Cloud115RequestError("115 未返回设备登录确认信息")
        await client._json("GET", action_url, params=action_params)
        result = await client._json(
            "POST",
            "https://qrcodeapi.115.com/app/1.0/alipaymini/1.0/login/qrcode/",
            data={"account": uid},
        )
        result_data = result.get("data")
        cookie = result_data.get("cookie") if isinstance(result_data, dict) else None
        if not isinstance(cookie, dict) or not cookie:
            raise Cloud115RequestError("115 未返回专用设备 Cookie")
        return "; ".join(f"{key}={value}" for key, value in cookie.items())


async def find_or_create_subdir(
    client: Cloud115Client, *, parent_cid: str, name: str
) -> str:
    for entry in await client.list_directory(parent_cid):
        if entry.is_dir and entry.name == name:
            return entry.entry_id
    try:
        return await client.mkdir(parent_cid, name)
    except Cloud115DuplicateNameError:
        for entry in await client.list_directory(parent_cid):
            if entry.is_dir and entry.name == name:
                return entry.entry_id
        raise


def choose_hls_definition(
    definitions: tuple[Cloud115VideoDefinition, ...], *, lowest: bool = False
) -> Cloud115VideoDefinition:
    if not definitions:
        raise Cloud115VideoUnavailableError("115 HLS 播放列表为空")
    if not lowest:
        return max(definitions, key=lambda item: item.bandwidth)

    def resolution_key(item: Cloud115VideoDefinition) -> tuple[int, int, int, int]:
        try:
            width, height = (int(value) for value in item.resolution.lower().split("x", 1))
        except (AttributeError, TypeError, ValueError):
            return (1, 0, 0, max(0, item.bandwidth))
        if width <= 0 or height <= 0:
            return (1, 0, 0, max(0, item.bandwidth))
        return (0, width * height, height, max(0, item.bandwidth))

    return min(definitions, key=resolution_key)


def run_sync(awaitable):
    """Bridge the existing synchronous provider operations to their async 115 calls."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    awaitable.close()
    raise RuntimeError("115 provider synchronous operation was called from an event loop")
