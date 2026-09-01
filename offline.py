"""115 offline-download component for the generic provider protocol."""

from __future__ import annotations

import base64
import binascii
import re
from urllib.parse import unquote, urlsplit

import httpx

from src.plugins.provider_protocol import (
    ConfigField,
    DownloadClientHandle,
    DownloadSubmission,
    LibraryHandle,
    ProviderDiagnosticCheck,
    ProviderDiagnosticReport,
    ProviderOperationError,
    RemoteDownloadTask,
)

from .cloud115 import Cloud115Client, find_or_create_subdir, run_sync
from .exceptions import (
    Cloud115AuthError,
    Cloud115DuplicateNameError,
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115OfflineTaskExistsError,
)

OFFLINE_REF_VERSION = 1
OFFLINE_SOURCE_KIND = "cloud115_dir"
_BTIH_RE = re.compile(r"urn:btih:([A-Za-z0-9]+)", re.IGNORECASE)
_INFO_HASH_DIR_RE = re.compile(r"^[0-9a-f]{40}$")


def _canonical_btih(value: str) -> str:
    text = value.strip()
    if len(text) == 40:
        try:
            bytes.fromhex(text)
        except ValueError as exc:
            raise ValueError("invalid btih") from exc
        return text.lower()
    if len(text) != 32 or not re.fullmatch(r"[A-Za-z2-7]+", text):
        raise ValueError("invalid btih")
    try:
        return base64.b32decode(text.upper() + "=" * ((8 - len(text) % 8) % 8)).hex()
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid btih") from exc


def _normalise_magnet(value: str) -> str:
    text = value.strip()
    if text[:9].lower() == "magnet://":
        rest = text[9:]
        return "magnet:" + rest if rest.startswith("?") else "magnet:?" + rest
    return text


def _parse_magnet(value: str) -> tuple[str, str]:
    magnet = _normalise_magnet(value)
    match = _BTIH_RE.search(unquote(magnet))
    if match is None:
        raise ValueError("invalid magnet")
    return magnet, _canonical_btih(match.group(1))


def _download_torrent(url: str) -> bytes:
    parsed = urlsplit(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("invalid torrent url")
    with httpx.Client(timeout=120.0, follow_redirects=True, trust_env=False) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.content


def _torrent_info_hash(payload: bytes) -> str:
    try:
        import libtorrent as lt

        return _canonical_btih(str(lt.torrent_info(payload).info_hash()))
    except Exception as exc:
        raise ValueError("invalid torrent file") from exc


def _resolve_source(source_uri: str) -> tuple[str, str]:
    if source_uri.lower().startswith("magnet:"):
        try:
            return _parse_magnet(source_uri)
        except ValueError as exc:
            raise _error("submit_download", "invalid_config", "磁力链接无效") from exc
    try:
        info_hash = _torrent_info_hash(_download_torrent(source_uri))
    except ValueError as exc:
        raise _error("submit_download", "invalid_config", "种子文件无效") from exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise _error("submit_download", "source_not_found", "种子文件不存在") from exc
        if exc.response.status_code >= 500:
            raise _error("submit_download", "unavailable", "种子文件服务暂时不可用", retryable=True) from exc
        raise _error("submit_download", "unsupported", "种子文件地址不受支持") from exc
    except httpx.HTTPError as exc:
        raise _error("submit_download", "unavailable", "种子文件下载失败", retryable=True) from exc
    return f"magnet:?xt=urn:btih:{info_hash}", info_hash


async def _create_task_dir(client: Cloud115Client, *, parent_cid: str, info_hash: str) -> str:
    try:
        return await client.mkdir(parent_cid, info_hash)
    except Cloud115DuplicateNameError:
        return await find_or_create_subdir(client, parent_cid=parent_cid, name=info_hash)


async def _find_offline_task(client: Cloud115Client, *, info_hash: str):
    page = 1
    while True:
        tasks, page_count = await client.list_offline_tasks(page=page)
        for task in tasks:
            if task.info_hash.lower() == info_hash:
                return task
        if page >= page_count or not tasks:
            return None
        page += 1


class Cloud115OfflineDownloadComponent:
    config_fields: tuple[ConfigField, ...] = ()

    def prepare_client(
        self,
        *,
        submitted_config: dict[str, object],
        library: LibraryHandle,
        previous: DownloadClientHandle | None,
    ) -> dict[str, object]:
        if submitted_config:
            raise _error("prepare_download", "invalid_config", "115 离线下载无需额外配置")
        _require_config(library, operation="prepare_download")
        return {}

    def test_client(
        self,
        *,
        submitted_config: dict[str, object],
        library: LibraryHandle,
    ) -> ProviderDiagnosticReport:
        if submitted_config:
            raise _error("test_download", "invalid_config", "115 离线下载无需额外配置")
        device_cookie, _downloads_root = _require_config(library, operation="test_download")

        async def check() -> bool:
            async with Cloud115Client(device_cookie) as client:
                return await client.check_alive()

        try:
            alive = run_sync(check())
        except Cloud115Error as exc:
            raise _cloud_error("test_download", exc) from exc
        if not alive:
            raise _error("test_download", "authentication_failed", "115 登录已失效")
        return ProviderDiagnosticReport(
            status="ok",
            checks=(
                ProviderDiagnosticCheck(
                    key="cloud115_auth",
                    status="ok",
                    code="cloud115_auth_ok",
                    message="115 专用设备登录有效",
                ),
            ),
        )

    def build(self, *, client: DownloadClientHandle) -> Cloud115OfflineDownloadProvider:
        device_cookie, downloads_root = _require_config(client.library, operation="build_download")
        return Cloud115OfflineDownloadProvider(
            device_cookie=device_cookie,
            downloads_root_cid=downloads_root,
        )


class Cloud115OfflineDownloadProvider:
    def __init__(self, *, device_cookie: str, downloads_root_cid: str) -> None:
        self._device_cookie = device_cookie
        self._downloads_root_cid = downloads_root_cid

    def submit(self, *, submission: DownloadSubmission) -> RemoteDownloadTask:
        if not isinstance(submission.source_uri, str) or not submission.source_uri.strip():
            raise _error("submit_download", "invalid_config", "离线下载链接不能为空")
        magnet, info_hash = _resolve_source(submission.source_uri.strip())

        async def create() -> str:
            async with Cloud115Client(self._device_cookie) as client:
                directory = await _create_task_dir(
                    client,
                    parent_cid=self._downloads_root_cid,
                    info_hash=info_hash,
                )
                try:
                    return await client.add_offline_url(magnet, save_dir_id=directory)
                except Cloud115OfflineTaskExistsError:
                    existing = await _find_offline_task(client, info_hash=info_hash)
                    if existing is not None:
                        managed_dirs = {
                            entry.entry_id
                            for entry in await client.list_directory(self._downloads_root_cid)
                            if entry.is_dir
                        }
                        if existing.save_dir_id in managed_dirs:
                            return existing.info_hash
                    raise _error(
                        "submit_download",
                        "task_not_managed",
                        "同哈希离线任务已存在，但不在当前下载目录，当前下载器无法接管",
                    )

        try:
            remote_id = run_sync(create())
        except Cloud115Error as exc:
            raise _cloud_error("submit_download", exc) from exc
        return RemoteDownloadTask(
            remote_id=remote_id,
            name=submission.display_name,
            state="queued",
            progress=0.0,
            completed_source_ref=None,
        )

    def list_tasks(self) -> tuple[RemoteDownloadTask, ...]:
        async def list_managed() -> tuple[RemoteDownloadTask, ...]:
            async with Cloud115Client(self._device_cookie) as client:
                managed_dirs = {
                    entry.entry_id
                    for entry in await client.list_directory(self._downloads_root_cid)
                    if entry.is_dir
                    and (entry.name.startswith("task-") or _INFO_HASH_DIR_RE.fullmatch(entry.name))
                }
                page = 1
                results: list[RemoteDownloadTask] = []
                while True:
                    tasks, page_count = await client.list_offline_tasks(page=page)
                    for task in tasks:
                        if not task.info_hash or task.save_dir_id not in managed_dirs:
                            continue
                        state = {0: "queued", 1: "downloading", 2: "completed", -1: "failed"}.get(
                            task.status, "failed"
                        )
                        completed_ref = (
                            {
                                "version": OFFLINE_REF_VERSION,
                                "kind": OFFLINE_SOURCE_KIND,
                                "cid": task.save_dir_id,
                            }
                            if state == "completed"
                            else None
                        )
                        results.append(
                            RemoteDownloadTask(
                                remote_id=task.info_hash,
                                name=task.name or task.info_hash,
                                state=state,
                                progress=task.progress,
                                completed_source_ref=completed_ref,
                            )
                        )
                    if page >= page_count or not tasks:
                        return tuple(results)
                    page += 1

        try:
            return run_sync(list_managed())
        except Cloud115Error as exc:
            raise _cloud_error("list_downloads", exc) from exc

    def delete_task(self, *, remote_id: str, delete_files: bool) -> None:
        if not isinstance(remote_id, str) or not remote_id:
            raise _error("delete_download", "invalid_config", "115 离线任务 ID 无效")

        async def delete() -> None:
            async with Cloud115Client(self._device_cookie) as client:
                await client.delete_offline_task(remote_id, delete_files=delete_files)

        try:
            run_sync(delete())
        except Cloud115Error as exc:
            if isinstance(exc, Cloud115NotFoundError):
                return
            raise _cloud_error("delete_download", exc) from exc


def _require_config(library: LibraryHandle, *, operation: str) -> tuple[str, str]:
    config = library.provider_config
    if not isinstance(config, dict):
        raise _error(operation, "invalid_config", "115 媒体库配置无效")
    cookie = config.get("device_cookie")
    downloads_root = config.get("downloads_root_cid")
    if not isinstance(cookie, str) or not cookie or not isinstance(downloads_root, str) or not downloads_root:
        raise _error(operation, "invalid_config", "115 媒体库配置不完整")
    return cookie, downloads_root


def _cloud_error(operation: str, exc: Cloud115Error) -> ProviderOperationError:
    if isinstance(exc, Cloud115AuthError):
        return _error(operation, "authentication_failed", "115 登录已失效")
    if isinstance(exc, Cloud115NotFoundError):
        return _error(operation, "source_not_found", "115 文件或目录不存在")
    return _error(operation, "unavailable", "115 服务暂不可用", retryable=True)


def _error(
    operation: str, code: str, message: str, *, retryable: bool = False
) -> ProviderOperationError:
    return ProviderOperationError(
        provider_key="cloud115",
        operation=operation,
        code=code,  # type: ignore[arg-type]
        safe_message=message,
        retryable=retryable,
    )
