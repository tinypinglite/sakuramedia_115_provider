"""Storage, import, thumbnails, and clips for 115-hosted media."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from loguru import logger
from starlette.responses import Response

from src.plugins.provider_protocol import (
    BrowseEntry,
    BrowsePage,
    ClipArtifact,
    ImportFile,
    ImportFileContent,
    ImportPlacement,
    JsonObject,
    LibraryHandle,
    MediaHandle,
    MediaTransferSource,
    PlaybackContext,
    ProviderOperationError,
    ScanProgressCallback,
    StagedMedia,
    StagedMediaTransfer,
    ThumbnailArtifact,
    ThumbnailBackendUnavailable,
    ThumbnailGeneration,
)

from .cloud115 import (
    Cloud115Client,
    Cloud115Entry,
    Cloud115VideoSegment,
    TransferState,
    choose_hls_definition,
    find_or_create_subdir,
    run_sync,
)
from .exceptions import (
    Cloud115AuthError,
    Cloud115DuplicateNameError,
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115RequestError,
    Cloud115VideoUnavailableError,
    safe_error_message,
)
from .hls_reader import Cloud115HlsSegmentReader
from .playback import Cloud115Playback
from .range_reader import Cloud115RangeReader

_BROWSER_USER_AGENT = Cloud115Client.DEFAULT_USER_AGENT
REF_VERSION = 1
DIR_REF_KIND = "cloud115_dir"
ENTRY_REF_KIND = "cloud115_entry"
MEDIA_REF_KIND = "cloud115_media"
STAGE_RECEIPT_KIND = "cloud115_stage"
TRANSFER_RECEIPT_KIND = "cloud115_transfer_stage"
THUMBNAIL_INTERVAL_SECONDS = 10
THUMBNAIL_HLS_MAX_WORKERS = 1
THUMBNAIL_PROGRESS_LOG_SEGMENT_INTERVAL = 50
THUMBNAIL_PROGRESS_LOG_INTERVAL_SECONDS = 5
THUMBNAIL_RANGE_REQUEST_DELAY_RANGE = (3.0, 3.0)
COVER_MAX_FETCHED_BYTES = 64 * 1024 * 1024
_HASH_DOMAIN = b"media-file-hash-v1"
_HASH_HEAD_TAIL_BYTES = 3 * 1024 * 1024
_HASH_MIDDLE_BYTES = 1024 * 1024
_HASH_FULL_THRESHOLD = 8 * 1024 * 1024
_HASH_REQUEST_DELAY_RANGE = (2.0, 4.0)
# 每个媒体库只保留最近一次下载导入批次的目录清单。
_IMPORT_DIRECTORIES: dict[
    tuple[str | None, int, str], tuple[str, dict[str, dict[str, str]]]
] = {}
_VIDEO_SUFFIXES = frozenset(
    {
        ".3gp",
        ".avi",
        ".flv",
        ".m2ts",
        ".m4v",
        ".mkv",
        ".mov",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".mts",
        ".rmvb",
        ".ts",
        ".webm",
        ".wmv",
    }
)


def _metadata_resolution(info: JsonObject) -> str | None:
    width = info["video"]["width"]
    height = info["video"]["height"]
    return f"{width}x{height}" if width and height else None


def _staged_media(
    *,
    storage_ref: JsonObject,
    receipt: JsonObject,
    size_bytes: int,
    duration_seconds: int | None,
    video_info: JsonObject | None,
    resolution: str | None,
) -> StagedMedia:
    """Build a staged result for both pre- and post-resolution v4 hosts."""
    if "resolution" in getattr(StagedMedia, "__dataclass_fields__", {}):
        return StagedMedia(
            storage_ref=storage_ref,
            receipt=receipt,
            size_bytes=size_bytes,
            duration_seconds=duration_seconds,
            video_info=video_info,
            resolution=resolution,
        )
    return StagedMedia(
        storage_ref=storage_ref,
        receipt=receipt,
        size_bytes=size_bytes,
        duration_seconds=duration_seconds,
        video_info=video_info,
    )


class Cloud115StorageProvider:
    def __init__(self, *, library: LibraryHandle, data_dir: Path) -> None:
        config = library.provider_config
        if not isinstance(config, dict):
            raise _error("build_storage", "invalid_config", "115 媒体库配置无效")
        cookie = config.get("device_cookie")
        media_root = config.get("media_root_cid")
        if not isinstance(cookie, str) or not cookie or not isinstance(media_root, str) or not media_root:
            raise _error("build_storage", "invalid_config", "115 媒体库配置不完整")
        self.library = library
        self._device_cookie = cookie
        self._media_root_cid = media_root
        self.data_dir = data_dir
        self._playback = Cloud115Playback(device_cookie=cookie)
        self._transfer_state = TransferState()
        self._transfer_directories: dict[str, dict[str, str]] = {}

    def browse(
        self, *, parent_ref: JsonObject | None, cursor: str | None, limit: int
    ) -> BrowsePage:
        if not isinstance(limit, int) or not 1 <= limit <= 200:
            raise _error("browse", "invalid_config", "浏览分页参数无效")
        if parent_ref is None:
            cid = "0"
        else:
            cid = _directory_ref(parent_ref, operation="browse")
        try:
            offset = 0 if cursor in {None, ""} else int(cursor)
            if offset < 0:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise _error("browse", "invalid_config", "浏览游标无效") from exc

        async def list_page() -> tuple[tuple[Cloud115Entry, ...], int]:
            async with Cloud115Client(self._device_cookie) as client:
                return await client.list_dir(cid, offset=offset, limit=limit)

        try:
            entries, total = run_sync(list_page())
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} cid={} error_type={} reason={}",
                           self.library.library_id, "browse", cid, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("browse", exc) from exc
        result = tuple(self._browse_entry(entry) for entry in entries)
        next_cursor = str(offset + len(entries)) if offset + len(entries) < total else None
        return BrowsePage(entries=result, next_cursor=next_cursor)

    def scan_import_source(
        self, *, source_ref: JsonObject,
        progress_callback: ScanProgressCallback | None = None,
    ) -> tuple[ImportFile, ...]:
        if not isinstance(source_ref, dict):
            raise _error("scan_import_source", "source_not_found", "115 导入源无效")
        kind = source_ref.get("kind")
        if source_ref.get("version") != REF_VERSION or kind not in {DIR_REF_KIND, ENTRY_REF_KIND}:
            raise _error("scan_import_source", "source_not_found", "115 导入源无效")
        try:
            if kind == ENTRY_REF_KIND:
                entry = _entry_ref(source_ref, operation="scan_import_source")
                if entry.is_dir:
                    raise ValueError("directory entry must use directory ref")
                return (self._import_file(entry, relative_path=entry.name),)
            cid = _directory_ref(source_ref, operation="scan_import_source")
            started = time.monotonic()
            logger.info("115 导入源扫描开始 library_id={} cid={}", self.library.library_id, cid)
            files = tuple(run_sync(self._scan_dir(cid, progress_callback)))
            logger.info("115 导入源扫描完成 library_id={} cid={} files={} elapsed_seconds={:.2f}",
                        self.library.library_id, cid, len(files), time.monotonic() - started)
            return files
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} cid={} error_type={} reason={}",
                           self.library.library_id, "scan_import_source", cid, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("scan_import_source", exc) from exc
        except ValueError as exc:
            raise _error("scan_import_source", "source_not_found", "115 导入源无效") from exc

    def get_import_source_identity(self, *, source: ImportFile) -> str | None:
        entry = _entry_ref(source.source_ref, operation="get_import_source_identity")
        if entry.is_dir:
            raise _error(
                "get_import_source_identity", "source_not_found", "115 导入文件不存在"
            )
        if not entry.sha1:
            return None
        payload = json.dumps(
            {
                "fid": entry.entry_id,
                "parent_cid": entry.parent_id,
                "name": entry.name,
                "relative_path": source.relative_path,
                "size_bytes": entry.size_bytes,
                "sha1": entry.sha1,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"cloud115-import-source-v1:{hashlib.sha256(payload).hexdigest()}"

    def scan_media_refs(self, *, source_ref: JsonObject) -> tuple[JsonObject, ...]:
        """Enumerate native media refs without rebuilding import-relative paths."""
        try:
            cid = _directory_ref(source_ref, operation="scan_media_refs")

            async def scan() -> tuple[JsonObject, ...]:
                async with Cloud115Client(self._device_cookie) as client:
                    refs = [
                        _media_ref(entry)
                        async for entry in client.iter_files_recursive(cid)
                    ]
                    return tuple(refs)

            started = time.monotonic()
            logger.info("115 媒体引用扫描开始 library_id={} cid={}", self.library.library_id, cid)
            refs = run_sync(scan())
            logger.info("115 媒体引用扫描完成 library_id={} cid={} files={} elapsed_seconds={:.2f}",
                        self.library.library_id, cid, len(refs), time.monotonic() - started)
            return refs
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} cid={} error_type={} reason={}",
                           self.library.library_id, "scan_media_refs", cid, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("scan_media_refs", exc) from exc
        except ValueError as exc:
            raise _error("scan_media_refs", "source_not_found", "115 扫描源无效") from exc

    def scan_managed_media_ref_keys(self) -> set[str]:
        """Enumerate the configured media root's stable pickcodes once."""
        try:
            async def scan() -> set[str]:
                async with Cloud115Client(
                    self._device_cookie,
                    batch_pacing=True,
                ) as client:
                    return {
                        entry.pickcode
                        async for entry in client.iter_files_recursive(self._media_root_cid)
                        if not entry.is_dir and entry.pickcode
                    }

            started = time.monotonic()
            logger.info("115 媒体库盘点开始 library_id={} cid={}", self.library.library_id, self._media_root_cid)
            keys = run_sync(scan())
            logger.info("115 媒体库盘点完成 library_id={} cid={} files={} elapsed_seconds={:.2f}",
                        self.library.library_id, self._media_root_cid, len(keys), time.monotonic() - started)
            return keys
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} cid={} error_type={} reason={}",
                           self.library.library_id, "scan_managed_media_ref_keys", self._media_root_cid, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("scan_managed_media_ref_keys", exc) from exc

    @staticmethod
    def managed_media_ref_key(*, media_ref: JsonObject) -> str:
        return _media_entry(
            media_ref,
            operation="managed_media_ref_key",
        ).pickcode

    async def _scan_dir(
        self, root_cid: str, progress_callback: ScanProgressCallback | None = None,
    ) -> list[ImportFile]:
        last_report = last_log = float("-inf")
        stage = "扫描文件"
        current = total = 0

        def report(*, force=False, action=""):
            nonlocal last_report, last_log
            now = time.monotonic()
            text = f"{stage} · 已处理 {current}/{total}" if total else f"{stage} · 已检查 {current}"
            if action:
                text += f" · {action}"
            if force or now - last_log >= 10:
                logger.info("115 导入扫描进度 library_id={} cid={} {}", self.library.library_id, root_cid, text)
                last_log = now
            if progress_callback is not None and (force or now - last_report >= 2):
                progress_callback({"current": current, "total": total, "text": text})
                last_report = now

        def file_progress(payload):
            nonlocal current, total
            if "wait_seconds" in payload:
                wait_seconds = payload["wait_seconds"]
                report(force=True, action=(
                    f"115 请求节流 · 等待 {wait_seconds:.0f} 秒"
                    if wait_seconds > 0 else "节流等待结束 · 等待 115 响应"
                ))
            if "current" in payload:
                current, total = payload["current"], payload["total"]
                report(force=current == total)

        report(force=True, action="等待 115 响应")
        async with Cloud115Client(self._device_cookie, progress_callback=file_progress) as client:
            source_entries = [entry async for entry in client.iter_files_recursive(root_cid)]
            current = total = len(source_entries)
            report(force=True, action="完成")
            logger.info("115 导入源文件枚举完成，开始解析相对路径 library_id={} cid={} files={}",
                        self.library.library_id, root_cid, len(source_entries))
            relative_dirs: dict[str, tuple[str, ...]] = {root_cid: ()}
            pending_parent_ids = {
                entry.parent_id
                for entry in source_entries
                if entry.parent_id and entry.parent_id != root_cid
            }
            stage = "解析目录路径"
            current, total = 0, len(pending_parent_ids)
            report(force=True)
            offset = 0
            while pending_parent_ids:
                report(action="等待 115 目录列表响应")
                entries, page_total = await client.list_dir(root_cid, offset=offset, limit=1150)
                for entry in entries:
                    if entry.is_dir and entry.entry_id in pending_parent_ids:
                        relative_dirs[entry.entry_id] = (entry.name,)
                        pending_parent_ids.discard(entry.entry_id)
                        current += 1
                report()
                offset += len(entries)
                if not entries or offset >= page_total:
                    break
            for parent_cid in sorted(pending_parent_ids):
                report(action="等待 115 目录详情响应")
                directory = await client.directory_info(parent_cid)
                parts: list[str] = []
                found_root = False
                for ancestor_cid, ancestor_name in directory.ancestors:
                    if found_root:
                        parts.append(ancestor_name)
                    elif ancestor_cid == root_cid:
                        found_root = True
                if not found_root:
                    raise Cloud115NotFoundError("115 文件不在导入源目录下")
                relative_dirs[parent_cid] = (*parts, directory.name)
                current += 1
                report()
            report(force=True, action="完成")
            files = [
                self._import_file(
                    entry,
                    relative_path="/".join((*relative_dirs[entry.parent_id], entry.name)),
                )
                for entry in source_entries
            ]
        files.sort(key=lambda item: (item.relative_path.casefold(), item.relative_path))
        return files

    def read_import_file(self, *, source: ImportFile) -> ImportFileContent:
        entry = _entry_ref(source.source_ref, operation="read_import_file")
        if entry.is_dir:
            raise _error("read_import_file", "source_not_found", "115 导入文件不存在")

        async def read() -> bytes:
            async with Cloud115Client(self._device_cookie) as client:
                return await client.download_bytes(
                    entry.pickcode,
                    user_agent=_BROWSER_USER_AGENT,
                    max_bytes=20 * 1024 * 1024,
                )

        try:
            content = run_sync(read())
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} fid={} error_type={} reason={}",
                           self.library.library_id, "read_import_file", entry.entry_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("read_import_file", exc) from exc
        return ImportFileContent(
            content=content,
            deletion_receipt={
                "version": REF_VERSION,
                "kind": ENTRY_REF_KIND,
                "fid": entry.entry_id,
                "parent_cid": entry.parent_id,
            },
        )

    def delete_import_file(self, *, receipt: JsonObject) -> None:
        entry = _receipt_entry(receipt, operation="delete_import_file")

        async def delete() -> None:
            async with Cloud115Client(self._device_cookie) as client:
                await client.delete_files([entry.entry_id], parent_cid=entry.parent_id)

        logger.info("115 删除文件开始 library_id={} operation={} fid={}", self.library.library_id, "delete_import_file", entry.entry_id)
        try:
            run_sync(delete())
            logger.info("115 文件操作完成 library_id={} operation={} fid={}", self.library.library_id, "delete_import_file", entry.entry_id)
        except Cloud115NotFoundError:
            logger.info("115 文件已不存在 library_id={} operation={} fid={}", self.library.library_id, "delete_import_file", entry.entry_id)
            return
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} fid={} error_type={} reason={}",
                           self.library.library_id, "delete_import_file", entry.entry_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("delete_import_file", exc) from exc

    def stage_import_file(
        self,
        *,
        source: ImportFile,
        placement: ImportPlacement,
        source_disposition: str,
        operation_key: str,
    ) -> StagedMedia:
        if source_disposition not in {"keep", "delete_after_commit"}:
            raise _error("stage_import", "invalid_config", "115 导入源处置方式无效")
        try:
            source_entry = _entry_ref(source.source_ref, operation="stage_import")
            if source_entry.is_dir:
                raise ValueError("source is a directory")
            placement_parts = _safe_relative_parts(placement.relative_path)
            operation_dir = _operation_directory(operation_key)
        except ValueError as exc:
            raise _error("stage_import", "invalid_config", "115 导入参数无效") from exc
        directory_cache = None
        batch = re.fullmatch(r"task:(\d+):download:\d+:\d+", operation_key)
        if batch is not None:
            key = (self.library.account_key, self.library.library_id, self._media_root_cid)
            batch_id = batch.group(1)
            cached = _IMPORT_DIRECTORIES.get(key)
            if cached is None or cached[0] != batch_id:
                cached = (batch_id, {})
                _IMPORT_DIRECTORIES[key] = cached
            directory_cache = cached[1]
        try:
            return run_sync(
                self._stage(
                    source_entry=source_entry,
                    placement_parts=placement_parts,
                    operation_dir=operation_dir,
                    source_disposition=source_disposition,
                    directory_cache=directory_cache,
                )
            )
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} operation_key={} source_fid={} error_type={} reason={}",
                           self.library.library_id, "stage_import", operation_key, source_entry.entry_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("stage_import", exc) from exc
        except Exception as exc:
            logger.error("115 暂存操作异常 library_id={} operation_key={} target={} error_type={} reason={}",
                         self.library.library_id, operation_key, placement.relative_path, type(exc).__name__, safe_error_message(exc))
            raise

    async def _stage(
        self,
        *,
        source_entry: Cloud115Entry,
        placement_parts: tuple[str, ...],
        operation_dir: str,
        source_disposition: str,
        directory_cache: dict[str, dict[str, str]] | None,
    ) -> StagedMedia:
        started = time.monotonic()
        logger.info("115 网盘导入开始 library_id={} operation_dir={} source_fid={} target={} disposition={}",
                    self.library.library_id, operation_dir, source_entry.entry_id,
                    "/".join(placement_parts), source_disposition)
        async with Cloud115Client(self._device_cookie) as client:
            logger.info("115 导入媒体探测开始 operation_dir={} source_fid={}", operation_dir, source_entry.entry_id)
            video_info = await client.get_video_metadata(source_entry.pickcode)
            duration_seconds = video_info["container"]["duration_seconds"]
            resolution = _metadata_resolution(video_info)
            logger.info("115 导入媒体探测完成 operation_dir={} duration_seconds={} resolution={}",
                        operation_dir, duration_seconds, resolution)
            target_parent = self._media_root_cid
            for component in placement_parts[:-1]:
                target_parent = await self._directory(
                    client, target_parent, component, directory_cache
                )
            target_dir = await self._directory(
                client, target_parent, operation_dir, directory_cache
            )
            existing = tuple(
                entry
                for entry in await client.list_directory(target_dir)
                if not entry.is_dir and entry.name == source_entry.name
            )
            if existing:
                target_entry = existing[0]
                logger.info("115 导入复用已有文件 operation_dir={} target_cid={} target_fid={}",
                            operation_dir, target_dir, target_entry.entry_id)
            else:
                logger.info("115 导入文件操作 operation_dir={} source_fid={} target_cid={} action={}",
                            operation_dir, source_entry.entry_id, target_dir,
                            "copy" if source_disposition == "keep" else "move")
                if source_disposition == "keep":
                    await client.copy_files([source_entry.entry_id], parent_cid=target_dir)
                else:
                    await client.move_files([source_entry.entry_id], parent_cid=target_dir)
                target_entry = _find_staged_entry(
                    await client.list_directory(target_dir), source_entry
                )
        logger.info("115 网盘导入暂存完成 library_id={} operation_dir={} target_fid={} target_cid={} elapsed_seconds={:.2f}",
                    self.library.library_id, operation_dir, target_entry.entry_id,
                    target_entry.parent_id, time.monotonic() - started)
        storage_ref = _media_ref(target_entry)
        return _staged_media(
            storage_ref=storage_ref,
            receipt={
                "version": REF_VERSION,
                "kind": STAGE_RECEIPT_KIND,
                "source_disposition": source_disposition,
                "source_fid": source_entry.entry_id,
                "source_parent_cid": source_entry.parent_id,
                "target_fid": target_entry.entry_id,
                "target_parent_cid": target_entry.parent_id,
                "target_pickcode": target_entry.pickcode,
            },
            size_bytes=target_entry.size_bytes,
            duration_seconds=duration_seconds,
            video_info=video_info,
            resolution=resolution,
        )

    def probe_video_info(self, *, media: MediaHandle) -> JsonObject | None:
        entry = _media_entry(media.storage_ref, operation="probe_video_info")

        async def resolve():
            async with Cloud115Client(self._device_cookie) as client:
                return await client.get_video_metadata(entry.pickcode)

        try:
            return run_sync(resolve())
        except (Cloud115Error, ValueError) as exc:
            logger.warning("115 视频信息获取失败 fid={} reason={}",
                           entry.entry_id, safe_error_message(exc))
            return None

    def probe_duration_seconds(self, *, media: MediaHandle) -> int:
        entry = _media_entry(media.storage_ref, operation="probe_duration_seconds")
        try:
            return run_sync(self._probe_duration(entry))
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} media_id={} error_type={} reason={}",
                           self.library.library_id, "probe_duration_seconds", media.media_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("probe_duration_seconds", exc) from exc

    def probe_resolution(self, *, media: MediaHandle) -> str | None:
        entry = _media_entry(media.storage_ref, operation="probe_resolution")
        try:
            return run_sync(self._probe_resolution(entry))
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} media_id={} error_type={} reason={}",
                           self.library.library_id, "probe_resolution", media.media_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("probe_resolution", exc) from exc

    async def _probe_duration(self, entry: Cloud115Entry) -> int:
        async with Cloud115Client(self._device_cookie) as client:
            return await self._probe_duration_with_client(client, entry)

    async def _probe_resolution(self, entry: Cloud115Entry) -> str | None:
        async with Cloud115Client(self._device_cookie) as client:
            return await self._probe_resolution_with_client(client, entry)

    @staticmethod
    async def _probe_duration_with_client(
        client: Cloud115Client, entry: Cloud115Entry
    ) -> int:
        duration_seconds, _resolution = await Cloud115StorageProvider._probe_duration_and_resolution_with_client(
            client, entry
        )
        return duration_seconds

    @staticmethod
    async def _probe_duration_and_resolution_with_client(
        client: Cloud115Client, entry: Cloud115Entry
    ) -> tuple[int, str | None]:
        info = await client.get_video_metadata(entry.pickcode)
        return info["container"]["duration_seconds"], _metadata_resolution(info)

    @staticmethod
    async def _probe_resolution_with_client(
        client: Cloud115Client, entry: Cloud115Entry
    ) -> str | None:
        info = await client.get_video_metadata(entry.pickcode)
        return _metadata_resolution(info)

    def stage_transfer(
        self,
        *,
        source: MediaTransferSource,
        placement: ImportPlacement,
        operation_key: str,
    ) -> StagedMediaTransfer:
        """Attempt only a 115 rapid upload from a path-free source session."""
        try:
            placement_parts = _safe_relative_parts(placement.relative_path)
            operation_dir = _operation_directory(operation_key)
            source_name = source.info.file_name
            source_size = source.info.size_bytes
            if (
                source_name != placement_parts[-1]
                or not isinstance(source_name, str)
                or not source_name
                or "/" in source_name
                or "\\" in source_name
                or not isinstance(source_size, int)
                or source_size < 0
            ):
                raise ValueError("invalid transfer source metadata")
        except (AttributeError, ValueError) as exc:
            raise _error("stage_transfer", "invalid_config", "媒体传输参数无效") from exc

        try:
            return run_sync(
                self._stage_transfer(
                    source=source,
                    source_name=source_name,
                    source_size=source_size,
                    placement_parts=placement_parts,
                    operation_dir=operation_dir,
                )
            )
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} operation_key={} target={} error_type={} reason={}",
                           self.library.library_id, "stage_transfer", operation_key, placement.relative_path, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("stage_transfer", exc) from None

        except Exception as exc:
            logger.error("115 暂存操作异常 library_id={} operation_key={} target={} error_type={} reason={}",
                         self.library.library_id, operation_key, placement.relative_path, type(exc).__name__, safe_error_message(exc))
            raise

    @staticmethod
    async def _directory(
        client: Cloud115Client, parent: str, name: str,
        cache: dict[str, dict[str, str]] | None,
    ) -> str:
        if cache is None:
            return await find_or_create_subdir(client, parent_cid=parent, name=name)
        for attempt in range(2):
            if parent not in cache:
                entries = await client.list_directory(parent)
                cache[parent] = {
                    entry.name: entry.entry_id for entry in entries if entry.is_dir
                }
            directories = cache[parent]
            if name in directories:
                return directories[name]
            try:
                directories[name] = await client.mkdir(parent, name)
                return directories[name]
            except Cloud115DuplicateNameError:
                cache.pop(parent, None)
                if attempt:
                    raise
        raise AssertionError("unreachable directory lookup")

    async def _stage_transfer(
        self,
        *,
        source: MediaTransferSource,
        source_name: str,
        source_size: int,
        placement_parts: tuple[str, ...],
        operation_dir: str,
    ) -> StagedMediaTransfer:
        started = time.monotonic()
        logger.info("115 秒传开始 library_id={} operation_dir={} file={} size_bytes={} target={}",
                    self.library.library_id, operation_dir, source_name, source_size, "/".join(placement_parts))
        async with Cloud115Client(
            self._device_cookie,
            batch_pacing=True,
            transfer_state=self._transfer_state,
        ) as client:
            # 不支持的 Cookie 在哈希和远端建目录之前拒绝。
            client._rapid_upload_protocol()
            # Hash before mkdir/init; only this successful mkdir owns the operation directory.
            hash_started = time.monotonic()
            logger.info("115 秒传源文件哈希开始 operation_dir={} size_bytes={}", operation_dir, source_size)
            source_sha1 = await asyncio.to_thread(
                client._hash_source, source, source_size
            )
            source.assert_unchanged()
            logger.info("115 秒传源文件哈希完成 operation_dir={} elapsed_seconds={:.2f}",
                        operation_dir, time.monotonic() - hash_started)
            parent = self._media_root_cid
            for component in placement_parts[:-1]:
                parent = await self._directory(client, parent, component, self._transfer_directories)
            operation_cid = await client.mkdir(parent, operation_dir)
            logger.info("115 秒传操作目录已创建，开始提交 operation_dir={} operation_cid={}",
                        operation_dir, operation_cid)
            target_entry = None
            not_hit = False
            try:
                result = await client.rapid_upload(
                    source,
                    filename=source_name,
                    size_bytes=source_size,
                    parent_cid=operation_cid,
                    file_sha1=source_sha1,
                )
                not_hit = result.status == "not_hit"
                entry = result.entry
                if not not_hit and (
                    entry is None
                    or entry.is_dir
                    or entry.parent_id != operation_cid
                    or entry.name != source_name
                    or entry.size_bytes != source_size
                    or not entry.sha1
                    or entry.sha1.upper() != source_sha1.upper()
                    or not entry.pickcode
                    or not entry.entry_id
                ):
                    raise Cloud115RequestError("115 秒传结果校验失败")
                target_entry = entry
                if not not_hit:
                    source.assert_unchanged()
            except Exception:
                try:
                    await self._rollback_transfer_operation(
                        client,
                        operation_cid=operation_cid,
                        target_entry=target_entry,
                        allow_empty_directory=not_hit,
                    )
                except Exception as rollback_error:
                    logger.warning("115 秒传补偿未完成 operation_dir={} operation_cid={} target_fid={} error_type={} reason={}",
                                   operation_dir, operation_cid, target_entry.entry_id if target_entry else None,
                                   type(rollback_error).__name__,
                                   safe_error_message(rollback_error))
                raise
            if not_hit:
                logger.info("115 秒传未命中，开始清理空操作目录 operation_dir={} operation_cid={}", operation_dir, operation_cid)
                await self._rollback_transfer_operation(
                    client,
                    operation_cid=operation_cid,
                    target_entry=None,
                    allow_empty_directory=True,
                )
                logger.info("115 秒传未命中处理完成 operation_dir={} elapsed_seconds={:.2f}",
                            operation_dir, time.monotonic() - started)
                return StagedMediaTransfer(status="not_available")
        logger.info("115 秒传命中，暂存结果已校验 library_id={} operation_dir={} operation_cid={} target_fid={} elapsed_seconds={:.2f}",
                    self.library.library_id, operation_dir, operation_cid, target_entry.entry_id, time.monotonic() - started)
        return StagedMediaTransfer(
            status="staged",
            storage_ref=_media_ref(target_entry),
            receipt={
                "version": REF_VERSION,
                "kind": TRANSFER_RECEIPT_KIND,
                "target_fid": target_entry.entry_id,
                "target_parent_cid": target_entry.parent_id,
                "target_pickcode": target_entry.pickcode,
                "target_name": target_entry.name,
                "target_sha1": source_sha1,
                "target_size_bytes": target_entry.size_bytes,
                "operation_cid": operation_cid,
            },
            file_name=target_entry.name,
            size_bytes=target_entry.size_bytes,
        )

    @staticmethod
    async def _rollback_transfer_operation(
        client: Cloud115Client,
        *,
        operation_cid: str,
        target_entry: Cloud115Entry | None,
        allow_empty_directory: bool,
    ) -> None:
        logger.info("115 秒传回滚开始 operation_cid={} target_fid={}",
                    operation_cid, target_entry.entry_id if target_entry else None)
        if target_entry is not None:
            if target_entry.parent_id != operation_cid:
                raise Cloud115RequestError("115 秒传暂存文件已离开操作目录，拒绝删除")
            try:
                current = await client.file_by_id(target_entry.entry_id)
            except Cloud115NotFoundError:
                current = None
            if current is not None:
                if not Cloud115StorageProvider._same_transfer_entry(
                    current, target_entry
                ):
                    raise Cloud115RequestError("115 秒传暂存文件身份已变化，拒绝删除")
                await client.delete_files(
                    [current.entry_id], parent_cid=current.parent_id
                )
                logger.info("115 秒传回滚已删除暂存文件 operation_cid={} target_fid={}", operation_cid, current.entry_id)
            else:
                logger.info("115 秒传回滚暂存文件已不存在 operation_cid={} target_fid={}", operation_cid, target_entry.entry_id)
        elif not allow_empty_directory:
            logger.warning("115 秒传结果不明，保留操作目录且不搜索或删除 operation_cid={}", operation_cid)
            return

        try:
            remaining = await client.list_directory(operation_cid)
        except Cloud115NotFoundError:
            logger.info("115 秒传回滚操作目录已不存在 operation_cid={}", operation_cid)
            return
        if remaining:
            raise Cloud115RequestError("115 秒传操作目录非空，拒绝删除目录")
        try:
            await client.delete_files([operation_cid])
        except Cloud115NotFoundError:
            logger.info("115 秒传回滚操作目录已不存在 operation_cid={}", operation_cid)
        else:
            logger.info("115 秒传回滚已删除空操作目录 operation_cid={}", operation_cid)

    @staticmethod
    def _same_transfer_entry(left: Cloud115Entry, right: Cloud115Entry) -> bool:
        return (
            not left.is_dir
            and left.entry_id == right.entry_id
            and left.parent_id == right.parent_id
            and left.name == right.name
            and left.size_bytes == right.size_bytes
            and bool(left.sha1)
            and bool(right.sha1)
            and left.sha1.upper() == right.sha1.upper()
            and bool(left.pickcode)
            and left.pickcode == right.pickcode
        )

    @staticmethod
    def _expected_transfer(receipt: JsonObject, operation: str) -> Cloud115Entry:
        transfer = _transfer_receipt(receipt, operation=operation)
        return Cloud115Entry(
            entry_id=transfer["target_fid"],
            parent_id=transfer["operation_cid"],
            name=transfer["target_name"],
            is_dir=False,
            size_bytes=transfer["target_size_bytes"],
            sha1=transfer["target_sha1"],
            pickcode=transfer["target_pickcode"],
            modified_at=0,
            is_video=False,
        )

    def finalize_transfer(self, *, receipt: JsonObject) -> None:
        expected = self._expected_transfer(receipt, "finalize_transfer")

        async def verify():
            async with Cloud115Client(
                self._device_cookie,
                batch_pacing=True,
                transfer_state=self._transfer_state,
            ) as client:
                current = await client.file_by_id(expected.entry_id)
                if not self._same_transfer_entry(current, expected):
                    raise Cloud115RequestError("115 目标身份不匹配")
                async for entry in client.iter_files_recursive(expected.parent_id):
                    if self._same_transfer_entry(entry, expected):
                        return
                raise Cloud115RequestError("115 目标尚未在目录中确认")

        logger.info("115 秒传目标确认开始 library_id={} operation_cid={} target_fid={}",
                    self.library.library_id, expected.parent_id, expected.entry_id)
        try:
            run_sync(verify())
            logger.info("115 秒传目标确认成功 library_id={} operation_cid={} target_fid={}",
                        self.library.library_id, expected.parent_id, expected.entry_id)
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} operation_cid={} target_fid={} error_type={} reason={}",
                           self.library.library_id, "finalize_transfer", expected.parent_id, expected.entry_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("finalize_transfer", exc) from None

    def abort_transfer(self, *, receipt: JsonObject) -> None:
        expected = self._expected_transfer(receipt, "abort_transfer")

        async def abort():
            async with Cloud115Client(
                self._device_cookie,
                batch_pacing=True,
                transfer_state=self._transfer_state,
            ) as client:
                await self._rollback_transfer_operation(
                    client,
                    operation_cid=expected.parent_id,
                    target_entry=expected,
                    allow_empty_directory=True,
                )

        try:
            run_sync(abort())
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} operation_cid={} target_fid={} error_type={} reason={}",
                           self.library.library_id, "abort_transfer", expected.parent_id, expected.entry_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("abort_transfer", exc) from None

    def finalize_import(self, *, receipt: JsonObject) -> None:
        _stage_receipt(receipt, operation="finalize_import")

    def abort_import(self, *, receipt: JsonObject) -> None:
        stage = _stage_receipt(receipt, operation="abort_import")

        logger.info("115 导入撤销开始 library_id={} target_fid={} target_cid={} source_cid={} action={}",
                    self.library.library_id, stage["target_fid"], stage["target_parent_cid"], stage["source_parent_cid"],
                    "delete_copy" if stage["source_disposition"] == "keep" else "move_back")
        async def abort() -> None:
            async with Cloud115Client(self._device_cookie) as client:
                if stage["source_disposition"] == "keep":
                    await client.delete_files(
                        [stage["target_fid"]], parent_cid=stage["target_parent_cid"]
                    )
                else:
                    await client.move_files(
                        [stage["target_fid"]], parent_cid=stage["source_parent_cid"]
                    )

        try:
            run_sync(abort())
            logger.info("115 文件操作完成 library_id={} operation={} fid={}", self.library.library_id, "abort_import", stage["target_fid"])
        except Cloud115NotFoundError:
            logger.info("115 文件已不存在 library_id={} operation={} fid={}", self.library.library_id, "abort_import", stage["target_fid"])
            return
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} target_fid={} error_type={} reason={}",
                           self.library.library_id, "abort_import", stage["target_fid"], type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("abort_import", exc) from exc

    def delete_media(self, *, media: MediaHandle) -> None:
        entry = _media_entry(media.storage_ref, operation="delete_media")

        async def delete() -> None:
            async with Cloud115Client(self._device_cookie) as client:
                await client.delete_files([entry.entry_id], parent_cid=entry.parent_id)

        logger.info("115 删除文件开始 library_id={} operation={} fid={}", self.library.library_id, "delete_media", entry.entry_id)
        try:
            run_sync(delete())
            logger.info("115 文件操作完成 library_id={} operation={} fid={}", self.library.library_id, "delete_media", entry.entry_id)
        except Cloud115NotFoundError:
            logger.info("115 文件已不存在 library_id={} operation={} fid={}", self.library.library_id, "delete_media", entry.entry_id)
            return
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} fid={} error_type={} reason={}",
                           self.library.library_id, "delete_media", entry.entry_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("delete_media", exc) from exc

    def compute_file_hash(self, *, media: MediaHandle) -> str:
        entry = _media_entry(media.storage_ref, operation="compute_file_hash")
        size = media.file_size_bytes
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise _error("compute_file_hash", "invalid_config", "115 媒体文件大小无效")

        started = time.monotonic()
        logger.info("115 文件哈希开始 library_id={} media_id={} fid={} size_bytes={}",
                    self.library.library_id, media.media_id, entry.entry_id, size)
        async def resolve():
            async with Cloud115Client(self._device_cookie) as client:
                await asyncio.sleep(random.uniform(*_HASH_REQUEST_DELAY_RANGE))
                return await client.get_download_url(
                    entry.pickcode,
                    user_agent=_BROWSER_USER_AGENT,
                )

        try:
            direct = run_sync(resolve())
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} media_id={} error_type={} reason={}",
                           self.library.library_id, "compute_file_hash", media.media_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("compute_file_hash", exc) from exc
        if direct.file_size_bytes != size:
            logger.warning("115 文件哈希大小校验失败 library_id={} media_id={} expected_size={} actual_size={}",
                           self.library.library_id, media.media_id, size, direct.file_size_bytes)
            raise _error(
                "compute_file_hash",
                "unavailable",
                "115 媒体文件大小与记录不一致",
                retryable=True,
            )
        if size == 0:
            empty_sha1 = hashlib.sha1(b"").digest()
            payload = _HASH_DOMAIN + b"\x00full\x00" + (0).to_bytes(8, "big") + empty_sha1
            logger.info("115 文件哈希完成 library_id={} media_id={} elapsed_seconds={:.2f}",
                        self.library.library_id, media.media_id, time.monotonic() - started)
            return f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"

        reader = Cloud115RangeReader(
            direct.url,
            user_agent=direct.user_agent,
            file_size_bytes=size,
            chunk_size=_HASH_MIDDLE_BYTES,
            max_fetched_bytes=_HASH_FULL_THRESHOLD,
            request_delay_range=_HASH_REQUEST_DELAY_RANGE,
        )
        try:
            def read_at(offset: int, length: int) -> bytes:
                reader.seek(offset)
                data = reader.read(length)
                if len(data) != length:
                    raise Cloud115RequestError("115 文件 Hash 读取不足")
                return data

            if size < _HASH_FULL_THRESHOLD:
                payload = (
                    _HASH_DOMAIN
                    + b"\x00full\x00"
                    + size.to_bytes(8, "big")
                    + hashlib.sha1(read_at(0, size)).digest()
                )
            else:
                head_sha1 = hashlib.sha1(read_at(0, _HASH_HEAD_TAIL_BYTES)).digest()
                tail_sha1 = hashlib.sha1(
                    read_at(size - _HASH_HEAD_TAIL_BYTES, _HASH_HEAD_TAIL_BYTES)
                ).digest()
                slot_count = (size - 2 * _HASH_HEAD_TAIL_BYTES) // _HASH_MIDDLE_BYTES
                slot_1 = int.from_bytes(head_sha1[:8], "big") % slot_count
                candidate = int.from_bytes(tail_sha1[:8], "big") % (slot_count - 1)
                slot_2 = candidate if candidate < slot_1 else candidate + 1
                middle_1_sha1 = hashlib.sha1(
                    read_at(
                        _HASH_HEAD_TAIL_BYTES + slot_1 * _HASH_MIDDLE_BYTES,
                        _HASH_MIDDLE_BYTES,
                    )
                ).digest()
                middle_2_sha1 = hashlib.sha1(
                    read_at(
                        _HASH_HEAD_TAIL_BYTES + slot_2 * _HASH_MIDDLE_BYTES,
                        _HASH_MIDDLE_BYTES,
                    )
                ).digest()
                payload = (
                    _HASH_DOMAIN
                    + b"\x00sampled\x00"
                    + size.to_bytes(8, "big")
                    + head_sha1
                    + tail_sha1
                    + middle_1_sha1
                    + middle_2_sha1
                )
        except Cloud115Error as exc:
            logger.warning("115 操作失败 library_id={} operation={} media_id={} error_type={} reason={}",
                           self.library.library_id, "compute_file_hash", media.media_id, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error("compute_file_hash", exc) from exc
        finally:
            reader.close()
        logger.info("115 文件哈希完成 library_id={} media_id={} elapsed_seconds={:.2f}",
                    self.library.library_id, media.media_id, time.monotonic() - started)
        return f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"

    async def handle_playback(self, *, media: MediaHandle, context: PlaybackContext) -> Response:
        try:
            return await self._playback.handle(media=media, context=context)
        except (ProviderOperationError, Cloud115Error) as exc:
            cause = exc.__cause__
            logger.warning("115 播放失败 library_id={} media_id={} delivery={} error_type={} reason={}",
                           self.library.library_id, media.media_id, context.delivery,
                           type(cause).__name__ if cause is not None else type(exc).__name__,
                           safe_error_message(cause if cause is not None else exc))
            raise

    async def handle_merged_playback(
        self,
        *,
        medias: tuple[MediaHandle, ...],
        context: PlaybackContext,
    ) -> Response:
        try:
            return await self._playback.handle_merged(medias=medias, context=context)
        except (ProviderOperationError, Cloud115Error) as exc:
            cause = exc.__cause__
            logger.warning("115 合并播放失败 library_id={} media_ids={} error_type={} reason={}",
                           self.library.library_id, tuple(media.media_id for media in medias),
                           type(cause).__name__ if cause is not None else type(exc).__name__,
                           safe_error_message(cause if cause is not None else exc))
            raise

    def open_cover_source(self, *, media: MediaHandle) -> Cloud115RangeReader:
        return self._range_reader(
            media,
            operation="open_cover_source",
            max_fetched_bytes=COVER_MAX_FETCHED_BYTES,
        )

    def generate_thumbnails(
        self, *, media: MediaHandle, workspace: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> ThumbnailGeneration:
        if progress_callback:
            progress_callback("正在获取视频分片")
        try:
            import av
            from PIL import Image
        except ImportError as exc:
            raise ThumbnailBackendUnavailable(
                "缩略图组件不可用", error_code="thumbnail_components_unavailable"
            ) from exc
        workspace = _workspace(workspace, operation="generate_thumbnails")
        try:
            targets, expected_count = run_sync(self._thumbnail_targets(media))
        except Cloud115VideoUnavailableError as exc:
            logger.info("115 缩略图无 HLS，回退原文件 Range library_id={} media_id={} reason={}",
                        self.library.library_id, media.media_id, safe_error_message(exc))
            return self._generate_range_thumbnails(
                media=media,
                workspace=workspace,
                av=av,
                image_module=Image,
                progress_callback=progress_callback,
            )
        except Cloud115NotFoundError as exc:
            logger.warning("115 缩略图目标不存在 library_id={} media_id={} reason={}",
                           self.library.library_id, media.media_id, safe_error_message(exc))
            raise _error("generate_thumbnails", "source_not_found", "115 视频未提供 HLS") from None
        except Cloud115Error as exc:
            logger.warning("115 缩略图目标解析失败 library_id={} media_id={} error_type={} reason={}",
                           self.library.library_id, media.media_id, type(exc).__name__, safe_error_message(exc))
            raise ThumbnailBackendUnavailable(
                "115 缩略图服务暂不可用", error_code="cloud115_thumbnail_unavailable"
            ) from exc

        total_segments = len(targets)
        logger.info(
            "115 thumbnail generation started media_id={} target_segments={} expected_thumbnails={}",
            media.media_id,
            total_segments,
            expected_count,
        )
        started_at = time.monotonic()
        last_progress_log_at = started_at
        completed_segments = 0
        generated_thumbnails = 0
        if progress_callback:
            progress_callback(f"正在生成缩略图 · 已生成 0/{expected_count} 张")

        def log_progress() -> None:
            logger.info(
                "115 thumbnail generation progress media_id={} completed_segments={}/{} "
                "generated_thumbnails={}/{} elapsed_seconds={}",
                media.media_id,
                completed_segments,
                total_segments,
                generated_thumbnails,
                expected_count,
                int(time.monotonic() - started_at),
            )

        artifacts: list[ThumbnailArtifact] = []
        with ThreadPoolExecutor(max_workers=THUMBNAIL_HLS_MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    self._decode_hls_segment,
                    segment=segment,
                    offsets=offsets,
                    workspace=workspace,
                    av=av,
                    image_module=Image,
                ): segment.index
                for segment, offsets in targets
            }
            remaining = set(futures)
            next_segment_log = THUMBNAIL_PROGRESS_LOG_SEGMENT_INTERVAL
            while remaining:
                completed, remaining = wait(
                    remaining,
                    timeout=THUMBNAIL_PROGRESS_LOG_INTERVAL_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                now = time.monotonic()
                if not completed:
                    log_progress()
                    last_progress_log_at = now
                    continue
                for future in completed:
                    completed_segments += 1
                    try:
                        generated = future.result()
                        artifacts.extend(generated)
                        generated_thumbnails += len(generated)
                    except Cloud115RequestError as exc:
                        logger.warning("115 缩略图分片读取失败 library_id={} media_id={} segment_index={} reason={}",
                                       self.library.library_id, media.media_id, futures[future], safe_error_message(exc))
                        raise ThumbnailBackendUnavailable(
                            "115 HLS 分片读取失败",
                            error_code="cloud115_thumbnail_unavailable",
                        ) from exc
                    except Exception as exc:
                        logger.warning(
                            "115 HLS thumbnail segment failed media_id={} segment_index={} detail={}",
                            media.media_id,
                            futures[future],
                            safe_error_message(exc),
                        )
                if progress_callback:
                    progress_callback(
                        f"正在生成缩略图 · 已生成 {generated_thumbnails}/{expected_count} 张"
                        f" · 分片 {completed_segments}/{total_segments}"
                    )
                if (
                    completed_segments >= next_segment_log
                    or now - last_progress_log_at >= THUMBNAIL_PROGRESS_LOG_INTERVAL_SECONDS
                ):
                    log_progress()
                    last_progress_log_at = now
                    while completed_segments >= next_segment_log:
                        next_segment_log += THUMBNAIL_PROGRESS_LOG_SEGMENT_INTERVAL
        artifacts.sort(key=lambda item: item.offset_seconds)
        logger.info(
            "115 thumbnail generation completed media_id={} completed_segments={} "
            "generated_thumbnails={} expected_thumbnails={} elapsed_seconds={}",
            media.media_id,
            completed_segments,
            generated_thumbnails,
            expected_count,
            int(time.monotonic() - started_at),
        )
        return ThumbnailGeneration(expected_count=expected_count, artifacts=tuple(artifacts))

    async def _thumbnail_targets(
        self, media: MediaHandle
    ) -> tuple[list[tuple[Cloud115VideoSegment, list[int]]], int]:
        entry = _media_entry(media.storage_ref, operation="generate_thumbnails")
        async with Cloud115Client(self._device_cookie) as client:
            info = await client.get_video_info(entry.pickcode)
            segments = await client.get_video_segments(
                choose_hls_definition(info.definitions, lowest=True)
            )
        return _thumbnail_targets(segments)

    @staticmethod
    def _decode_hls_segment(
        *,
        segment: Cloud115VideoSegment,
        offsets: list[int],
        workspace: Path,
        av,
        image_module,
    ) -> list[ThumbnailArtifact]:
        reader = Cloud115HlsSegmentReader(
            segment.url,
            user_agent=_BROWSER_USER_AGENT,
        )
        container = None
        try:
            container = av.open(
                reader, format="mpegts", options={"probesize": str(128 * 1024)}
            )
            if not container.streams.video:
                raise ValueError("hls_video_stream_missing")
            frame = next(
                (item for item in container.decode(container.streams.video[0]) if not item.is_corrupt),
                None,
            )
            if frame is None:
                raise ValueError("hls_clean_frame_missing")
            image = frame.to_image()
            try:
                image.thumbnail((640, 360), image_module.Resampling.LANCZOS)
                artifacts = []
                for offset in offsets:
                    destination = workspace / f"thumbnail-{offset}.webp"
                    image.save(destination, format="WEBP", quality=82, method=4)
                    artifacts.append(
                        ThumbnailArtifact(offset_seconds=offset, relative_path=destination.name)
                    )
                return artifacts
            finally:
                image.close()
        finally:
            if container is not None:
                container.close()
            reader.close()

    def _generate_range_thumbnails(
        self,
        *,
        media: MediaHandle,
        workspace: Path,
        av,
        image_module,
        progress_callback: Callable[[str], None] | None,
    ) -> ThumbnailGeneration:
        reader = self._range_reader(
            media,
            operation="generate_thumbnails",
            max_fetched_bytes=media.file_size_bytes,
            request_delay_range=THUMBNAIL_RANGE_REQUEST_DELAY_RANGE,
        )
        container = None
        artifacts: list[ThumbnailArtifact] = []
        expected_count = 0
        started_at = time.monotonic()
        try:
            container = av.open(reader, mode="r")
            if not container.streams.video:
                raise ValueError("video stream missing")
            video = container.streams.video[0]
            duration = _container_duration_seconds(container, video, av) or int(
                media.duration_seconds or 0
            )
            if duration <= 0:
                raise ThumbnailBackendUnavailable(
                    "115 原文件视频时长无效",
                    error_code="cloud115_thumbnail_unavailable",
                )
            offsets = tuple(range(0, duration, THUMBNAIL_INTERVAL_SECONDS))
            expected_count = len(offsets)
            if progress_callback:
                progress_callback(f"正在使用原文件 Range 生成缩略图 · 已生成 0/{expected_count} 张")
            for offset in offsets:
                if progress_callback:
                    progress_callback(
                        f"正在使用原文件 Range 生成缩略图 · 读取 {offset} 秒"
                        f" · 已生成 {len(artifacts)}/{expected_count} 张"
                    )
                try:
                    container.seek(offset * av.time_base, backward=True, any_frame=False)
                    frame = next(
                        (item for item in container.decode(video) if not item.is_corrupt),
                        None,
                    )
                    if frame is None:
                        raise ValueError("clean frame missing")
                    image = frame.to_image()
                    try:
                        image.thumbnail((640, 360), image_module.Resampling.LANCZOS)
                        destination = workspace / f"thumbnail-{offset}.webp"
                        image.save(destination, format="WEBP", quality=82, method=4)
                    finally:
                        image.close()
                except Cloud115RequestError as exc:
                    raise ThumbnailBackendUnavailable(
                        "115 原文件 Range 读取失败",
                        error_code="cloud115_thumbnail_unavailable",
                    ) from exc
                except Exception as exc:
                    logger.warning(
                        "115 原文件 Range 缩略图生成失败 library_id={} media_id={} offset_seconds={} reason={}",
                        self.library.library_id,
                        media.media_id,
                        offset,
                        safe_error_message(exc),
                    )
                    continue
                artifacts.append(
                    ThumbnailArtifact(
                        offset_seconds=offset,
                        relative_path=f"thumbnail-{offset}.webp",
                    )
                )
                if progress_callback:
                    progress_callback(
                        f"正在使用原文件 Range 生成缩略图 · 已生成 {len(artifacts)}/{expected_count} 张"
                    )
        finally:
            if container is not None:
                container.close()
            reader.close()
            logger.info(
                "115 原文件 Range 缩略图生成结束 library_id={} media_id={} generated_thumbnails={} "
                "expected_thumbnails={} fetched_bytes={} elapsed_seconds={:.2f}",
                self.library.library_id,
                media.media_id,
                len(artifacts),
                expected_count,
                reader.fetched_bytes,
                time.monotonic() - started_at,
            )
        return ThumbnailGeneration(
            expected_count=expected_count,
            artifacts=tuple(artifacts),
        )

    def create_clip(
        self,
        *,
        media: MediaHandle,
        start_offset_seconds: int,
        end_offset_seconds: int,
        workspace: Path,
    ) -> ClipArtifact:
        if (
            not isinstance(start_offset_seconds, int)
            or not isinstance(end_offset_seconds, int)
            or start_offset_seconds < 0
            or end_offset_seconds <= start_offset_seconds
        ):
            raise _error("create_clip", "invalid_config", "片段时间范围无效")
        try:
            import av
        except ImportError as exc:
            raise _error("create_clip", "unavailable", "视频剪辑组件不可用", retryable=True) from exc
        workspace = _workspace(workspace, operation="create_clip")
        started = time.monotonic()
        logger.info("115 视频截取开始 library_id={} media_id={} start_seconds={} end_seconds={}",
                    self.library.library_id, media.media_id, start_offset_seconds, end_offset_seconds)
        destination = workspace / "clip.mp4"
        temporary = workspace / ".clip.tmp.mp4"
        reader = self._range_reader(
            media,
            operation="create_clip",
            max_fetched_bytes=1024 * 1024 * 1024,
        )
        input_container = None
        try:
            input_container = av.open(reader, mode="r")
            if not input_container.streams.video:
                raise ValueError("video stream missing")
            video = input_container.streams.video[0]
            selected = [video, *input_container.streams.audio]
            input_container.seek(
                start_offset_seconds * av.time_base,
                backward=True,
                any_frame=False,
            )
            origin_seconds: float | None = None
            for packet in input_container.demux(video):
                if packet.dts is not None and packet.time_base is not None:
                    origin_seconds = float(packet.dts * packet.time_base)
                    break
            if origin_seconds is None:
                raise ValueError("clip seek failed")
            input_container.seek(
                max(0, int(origin_seconds * av.time_base)),
                backward=True,
                any_frame=False,
            )
            with av.open(str(temporary), mode="w", format="mp4", options={"movflags": "+faststart"}) as output:
                stream_map = {stream: output.add_stream_from_template(stream) for stream in selected}
                packets = 0
                for packet in input_container.demux(*selected):
                    if packet.dts is None or packet.time_base is None:
                        continue
                    seconds = float(packet.dts * packet.time_base)
                    if seconds + 1e-9 < origin_seconds:
                        continue
                    if seconds >= end_offset_seconds:
                        break
                    shift = round(origin_seconds / float(packet.time_base))
                    if packet.pts is not None:
                        packet.pts -= shift
                    packet.dts -= shift
                    packet.stream = stream_map[packet.stream]
                    output.mux(packet)
                    packets += 1
                if not packets:
                    raise ValueError("clip packet range empty")
            os.replace(temporary, destination)
            if not destination.is_file() or destination.stat().st_size <= 0:
                raise ValueError("clip output empty")
            logger.info("115 视频截取完成 library_id={} media_id={} elapsed_seconds={:.2f}",
                        self.library.library_id, media.media_id, time.monotonic() - started)
            return ClipArtifact(relative_path=destination.name)
        except ProviderOperationError:
            raise
        except Exception as exc:
            logger.error("115 视频截取失败 library_id={} media_id={} error_type={} reason={}",
                         self.library.library_id, media.media_id, type(exc).__name__, safe_error_message(exc))
            raise _error("create_clip", "unavailable", "115 视频剪辑失败", retryable=True) from exc
        finally:
            if input_container is not None:
                input_container.close()
            reader.close()
            temporary.unlink(missing_ok=True)

    def _range_reader(
        self,
        media: MediaHandle,
        *,
        operation: str,
        max_fetched_bytes: int,
        request_delay_range: tuple[float, float] | None = None,
    ) -> Cloud115RangeReader:
        entry = _media_entry(media.storage_ref, operation=operation)

        async def resolve():
            async with Cloud115Client(self._device_cookie) as client:
                return await client.get_download_url(
                    entry.pickcode,
                    user_agent=_BROWSER_USER_AGENT,
                )

        try:
            direct = run_sync(resolve())
        except Cloud115Error as exc:
            logger.warning("115 媒体读取地址解析失败 library_id={} media_id={} operation={} error_type={} reason={}",
                           self.library.library_id, media.media_id, operation, type(exc).__name__, safe_error_message(exc))
            raise _cloud_error(operation, exc) from exc
        size = direct.file_size_bytes or media.file_size_bytes
        if size <= 0:
            raise _error(operation, "source_not_found", "115 媒体文件大小无效")
        return Cloud115RangeReader(
            direct.url,
            user_agent=direct.user_agent,
            file_size_bytes=size,
            max_fetched_bytes=max_fetched_bytes,
            request_delay_range=request_delay_range,
        )

    @staticmethod
    def _browse_entry(entry: Cloud115Entry) -> BrowseEntry:
        modified_at = (
            datetime.fromtimestamp(entry.modified_at, tz=timezone.utc)
            if entry.modified_at > 0
            else None
        )
        if entry.is_dir:
            source_ref: JsonObject = {
                "version": REF_VERSION,
                "kind": DIR_REF_KIND,
                "cid": entry.entry_id,
            }
        else:
            source_ref = _entry_source_ref(entry)
        return BrowseEntry(
            source_ref=source_ref,
            name=entry.name,
            entry_type="directory" if entry.is_dir else "file",
            size_bytes=None if entry.is_dir else entry.size_bytes,
            modified_at=modified_at,
            is_video=entry.is_video or _is_video(entry.name),
        )

    @staticmethod
    def _import_file(entry: Cloud115Entry, *, relative_path: str) -> ImportFile:
        return ImportFile(
            source_ref=_entry_source_ref(entry),
            name=entry.name,
            relative_path=relative_path,
            size_bytes=entry.size_bytes,
            is_video=entry.is_video or _is_video(entry.name),
        )


def _entry_source_ref(entry: Cloud115Entry) -> JsonObject:
    return {
        "version": REF_VERSION,
        "kind": ENTRY_REF_KIND,
        "fid": entry.entry_id,
        "parent_cid": entry.parent_id,
        "pickcode": entry.pickcode,
        "name": entry.name,
        "size_bytes": entry.size_bytes,
        "sha1": entry.sha1 or "",
        "is_dir": entry.is_dir,
    }


def _media_ref(entry: Cloud115Entry) -> JsonObject:
    result = _entry_source_ref(entry)
    result["kind"] = MEDIA_REF_KIND
    return result


def _entry_ref(ref: object, *, operation: str) -> Cloud115Entry:
    if not isinstance(ref, dict) or ref.get("version") != REF_VERSION or ref.get("kind") != ENTRY_REF_KIND:
        raise _error(operation, "source_not_found", "115 文件引用无效")
    return _entry_from_values(ref, operation=operation)


def _media_entry(ref: object, *, operation: str) -> Cloud115Entry:
    if not isinstance(ref, dict) or ref.get("version") != REF_VERSION or ref.get("kind") != MEDIA_REF_KIND:
        raise _error(operation, "source_not_found", "115 媒体引用无效")
    return _entry_from_values(ref, operation=operation)


def _receipt_entry(receipt: object, *, operation: str) -> Cloud115Entry:
    if not isinstance(receipt, dict) or receipt.get("version") != REF_VERSION or receipt.get("kind") != ENTRY_REF_KIND:
        raise _error(operation, "source_not_found", "115 文件删除回执无效")
    fid = receipt.get("fid")
    parent = receipt.get("parent_cid")
    if not isinstance(fid, str) or not fid or not isinstance(parent, str) or not parent:
        raise _error(operation, "source_not_found", "115 文件删除回执无效")
    return Cloud115Entry(fid, parent, "", False, 0, None, "", 0, False)


def _entry_from_values(values: dict[str, Any], *, operation: str) -> Cloud115Entry:
    fid = values.get("fid")
    parent = values.get("parent_cid")
    pickcode = values.get("pickcode")
    name = values.get("name")
    size = values.get("size_bytes")
    sha1 = values.get("sha1")
    is_dir = values.get("is_dir")
    if (
        not isinstance(fid, str)
        or not fid
        or not isinstance(parent, str)
        or not parent
        or not isinstance(pickcode, str)
        or not pickcode
        or not isinstance(name, str)
        or not name
        or not isinstance(size, int)
        or size < 0
        or not isinstance(sha1, str)
        or not isinstance(is_dir, bool)
    ):
        raise _error(operation, "source_not_found", "115 文件引用无效")
    return Cloud115Entry(fid, parent, name, is_dir, size, sha1 or None, pickcode, 0, False)


def _directory_ref(ref: object, *, operation: str) -> str:
    if not isinstance(ref, dict) or ref.get("version") != REF_VERSION or ref.get("kind") != DIR_REF_KIND:
        raise _error(operation, "source_not_found", "115 目录引用无效")
    cid = ref.get("cid")
    if not isinstance(cid, str) or not cid:
        raise _error(operation, "source_not_found", "115 目录引用无效")
    return cid


def _stage_receipt(receipt: object, *, operation: str) -> dict[str, str]:
    if not isinstance(receipt, dict) or receipt.get("version") != REF_VERSION or receipt.get("kind") != STAGE_RECEIPT_KIND:
        raise _error(operation, "source_not_found", "115 导入回执无效")
    fields = (
        "source_disposition",
        "source_fid",
        "source_parent_cid",
        "target_fid",
        "target_parent_cid",
        "target_pickcode",
    )
    result = {field: receipt.get(field) for field in fields}
    if any(not isinstance(value, str) or not value for value in result.values()):
        raise _error(operation, "source_not_found", "115 导入回执无效")
    if result["source_disposition"] not in {"keep", "delete_after_commit"}:
        raise _error(operation, "source_not_found", "115 导入回执无效")
    return result  # type: ignore[return-value]


def _transfer_receipt(receipt: object, *, operation: str) -> dict[str, Any]:
    if (
        not isinstance(receipt, dict)
        or receipt.get("version") != REF_VERSION
        or receipt.get("kind") != TRANSFER_RECEIPT_KIND
    ):
        raise _error(operation, "source_not_found", "115 传输回执无效")
    fields = (
        "target_fid",
        "target_parent_cid",
        "target_pickcode",
        "target_name",
        "target_sha1",
        "operation_cid",
    )
    values = {field: receipt.get(field) for field in fields}
    if any(not isinstance(value, str) or not value for value in values.values()):
        raise _error(operation, "source_not_found", "115 传输回执无效")
    size_bytes = receipt.get("target_size_bytes")
    if (
        not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes < 0
    ):
        raise _error(operation, "source_not_found", "115 传输回执无效")
    values["target_size_bytes"] = size_bytes
    if values["target_parent_cid"] != values["operation_cid"]:
        raise _error(operation, "source_not_found", "115 传输回执无效")
    return values


def _find_staged_entry(
    entries: tuple[Cloud115Entry, ...], source: Cloud115Entry
) -> Cloud115Entry:
    matches = [
        entry
        for entry in entries
        if not entry.is_dir
        and entry.name == source.name
        and (not source.sha1 or entry.sha1 == source.sha1)
    ]
    if len(matches) != 1:
        raise Cloud115NotFoundError("115 未找到暂存后的文件")
    return matches[0]


def _safe_relative_parts(value: object) -> tuple[str, ...]:
    if not isinstance(value, str) or not value or "\\" in value or "\0" in value:
        raise ValueError("unsafe relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("unsafe relative path")
    return path.parts


def _operation_directory(operation_key: object) -> str:
    if not isinstance(operation_key, str) or not operation_key:
        raise ValueError("invalid operation key")
    return f"op-{hashlib.sha256(operation_key.encode('utf-8')).hexdigest()[:24]}"


def _workspace(workspace: Path, *, operation: str) -> Path:
    try:
        path = Path(workspace).resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path
    except OSError as exc:
        raise _error(operation, "unavailable", "工作目录不可用", retryable=True) from exc


def _thumbnail_targets(
    segments: tuple[Cloud115VideoSegment, ...],
) -> tuple[list[tuple[Cloud115VideoSegment, list[int]]], int]:
    timeline: list[tuple[Cloud115VideoSegment, float]] = []
    duration = 0.0
    for segment in segments:
        length = max(0.0, float(segment.duration_seconds))
        if length <= 0:
            continue
        duration += length
        timeline.append((segment, duration))
    if not timeline:
        raise Cloud115VideoUnavailableError("115 HLS 分片时长无效")

    grouped: dict[int, tuple[Cloud115VideoSegment, list[int]]] = {}
    index = 0
    for offset in range(0, int(duration), THUMBNAIL_INTERVAL_SECONDS):
        while index < len(timeline) - 1 and offset >= timeline[index][1]:
            index += 1
        segment = timeline[index][0]
        grouped.setdefault(segment.index, (segment, []))[1].append(offset)
    if not grouped:
        segment = timeline[0][0]
        grouped[segment.index] = (segment, [0])
    targets = list(grouped.values())
    return targets, sum(len(offsets) for _, offsets in targets)


def _container_duration_seconds(container, video, av) -> int:
    stream_duration = getattr(video, "duration", None)
    stream_time_base = getattr(video, "time_base", None)
    if stream_duration and stream_time_base:
        seconds = int(stream_duration * stream_time_base)
        if seconds > 0:
            return seconds
    container_duration = getattr(container, "duration", None)
    time_base = getattr(av, "time_base", None)
    if container_duration and time_base:
        seconds = int(container_duration / time_base)
        if seconds > 0:
            return seconds
    return 0


def _is_video(name: str) -> bool:
    return Path(name).suffix.lower() in _VIDEO_SUFFIXES


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
