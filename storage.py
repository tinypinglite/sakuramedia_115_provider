"""Storage, import, thumbnails, and clips for 115-hosted media."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from loguru import logger
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
    PlaybackContext,
    ProviderOperationError,
    StagedMedia,
    ThumbnailArtifact,
    ThumbnailBackendUnavailable,
    ThumbnailGeneration,
    ThumbnailGenerationDeferred,
)
from starlette.responses import Response

from .cloud115 import (
    Cloud115Client,
    Cloud115Entry,
    Cloud115VideoSegment,
    choose_hls_definition,
    find_or_create_subdir,
    run_sync,
)
from .exceptions import (
    Cloud115AuthError,
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115RequestError,
    Cloud115VideoUnavailableError,
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
THUMBNAIL_INTERVAL_SECONDS = 10
THUMBNAIL_HLS_MAX_WORKERS = 3
THUMBNAIL_PROGRESS_LOG_SEGMENT_INTERVAL = 50
THUMBNAIL_PROGRESS_LOG_INTERVAL_SECONDS = 5
COVER_MAX_FETCHED_BYTES = 64 * 1024 * 1024
_HASH_DOMAIN = b"media-file-hash-v1"
_HASH_HEAD_TAIL_BYTES = 3 * 1024 * 1024
_HASH_MIDDLE_BYTES = 1024 * 1024
_HASH_FULL_THRESHOLD = 8 * 1024 * 1024
_HASH_REQUEST_DELAY_RANGE = (2.0, 4.0)
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
            raise _cloud_error("browse", exc) from exc
        result = tuple(self._browse_entry(entry) for entry in entries)
        next_cursor = str(offset + len(entries)) if offset + len(entries) < total else None
        return BrowsePage(entries=result, next_cursor=next_cursor)

    def scan_import_source(self, *, source_ref: JsonObject) -> tuple[ImportFile, ...]:
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
            return tuple(run_sync(self._scan_dir(cid)))
        except Cloud115Error as exc:
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

            return run_sync(scan())
        except Cloud115Error as exc:
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

            return run_sync(scan())
        except Cloud115Error as exc:
            raise _cloud_error("scan_managed_media_ref_keys", exc) from exc

    @staticmethod
    def managed_media_ref_key(*, media_ref: JsonObject) -> str:
        return _media_entry(
            media_ref,
            operation="managed_media_ref_key",
        ).pickcode

    async def _scan_dir(self, root_cid: str) -> list[ImportFile]:
        async with Cloud115Client(self._device_cookie) as client:
            source_entries = [entry async for entry in client.iter_files_recursive(root_cid)]
            relative_dirs: dict[str, tuple[str, ...]] = {root_cid: ()}
            pending_parent_ids = {
                entry.parent_id
                for entry in source_entries
                if entry.parent_id and entry.parent_id != root_cid
            }
            offset = 0
            while pending_parent_ids:
                entries, total = await client.list_dir(root_cid, offset=offset, limit=1150)
                for entry in entries:
                    if entry.is_dir and entry.entry_id in pending_parent_ids:
                        relative_dirs[entry.entry_id] = (entry.name,)
                        pending_parent_ids.discard(entry.entry_id)
                offset += len(entries)
                if not entries or offset >= total:
                    break
            for parent_cid in sorted(pending_parent_ids):
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

        try:
            run_sync(delete())
        except Cloud115NotFoundError:
            return
        except Cloud115Error as exc:
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
        try:
            return run_sync(
                self._stage(
                    source_entry=source_entry,
                    placement_parts=placement_parts,
                    operation_dir=operation_dir,
                    source_disposition=source_disposition,
                )
            )
        except Cloud115Error as exc:
            raise _cloud_error("stage_import", exc) from exc

    async def _stage(
        self,
        *,
        source_entry: Cloud115Entry,
        placement_parts: tuple[str, ...],
        operation_dir: str,
        source_disposition: str,
    ) -> StagedMedia:
        async with Cloud115Client(self._device_cookie) as client:
            duration_seconds, resolution = await self._probe_duration_and_resolution_with_client(
                client, source_entry
            )
            target_parent = self._media_root_cid
            for component in placement_parts[:-1]:
                target_parent = await find_or_create_subdir(
                    client, parent_cid=target_parent, name=component
                )
            target_dir = await find_or_create_subdir(
                client, parent_cid=target_parent, name=operation_dir
            )
            existing = tuple(
                entry
                for entry in await client.list_directory(target_dir)
                if not entry.is_dir and entry.name == source_entry.name
            )
            if existing:
                target_entry = existing[0]
            else:
                if source_disposition == "keep":
                    await client.copy_files([source_entry.entry_id], parent_cid=target_dir)
                else:
                    await client.move_files([source_entry.entry_id], parent_cid=target_dir)
                target_entry = _find_staged_entry(
                    await client.list_directory(target_dir), source_entry
                )
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
            video_info=None,
            resolution=resolution,
        )

    def probe_duration_seconds(self, *, media: MediaHandle) -> int:
        entry = _media_entry(media.storage_ref, operation="probe_duration_seconds")
        try:
            return run_sync(self._probe_duration(entry))
        except Cloud115Error as exc:
            raise _cloud_error("probe_duration_seconds", exc) from exc

    def probe_resolution(self, *, media: MediaHandle) -> str | None:
        entry = _media_entry(media.storage_ref, operation="probe_resolution")
        try:
            return run_sync(self._probe_resolution(entry))
        except Cloud115Error as exc:
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
        info = await client.get_video_info(entry.pickcode)
        definition = choose_hls_definition(info.definitions)
        segments = await client.get_video_segments(definition)
        duration_seconds = int(
            sum(segment.duration_seconds for segment in segments) + 1e-6
        )
        if duration_seconds <= 0:
            raise Cloud115VideoUnavailableError("115 视频时长不可用")
        return duration_seconds, definition.resolution or None

    @staticmethod
    async def _probe_resolution_with_client(
        client: Cloud115Client, entry: Cloud115Entry
    ) -> str | None:
        info = await client.get_video_info(entry.pickcode)
        return choose_hls_definition(info.definitions).resolution or None

    def finalize_import(self, *, receipt: JsonObject) -> None:
        _stage_receipt(receipt, operation="finalize_import")

    def abort_import(self, *, receipt: JsonObject) -> None:
        stage = _stage_receipt(receipt, operation="abort_import")

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
        except Cloud115NotFoundError:
            return
        except Cloud115Error as exc:
            raise _cloud_error("abort_import", exc) from exc

    def delete_media(self, *, media: MediaHandle) -> None:
        entry = _media_entry(media.storage_ref, operation="delete_media")

        async def delete() -> None:
            async with Cloud115Client(self._device_cookie) as client:
                await client.delete_files([entry.entry_id], parent_cid=entry.parent_id)

        try:
            run_sync(delete())
        except Cloud115NotFoundError:
            return
        except Cloud115Error as exc:
            raise _cloud_error("delete_media", exc) from exc

    def compute_file_hash(self, *, media: MediaHandle) -> str:
        entry = _media_entry(media.storage_ref, operation="compute_file_hash")
        size = media.file_size_bytes
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise _error("compute_file_hash", "invalid_config", "115 媒体文件大小无效")

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
            raise _cloud_error("compute_file_hash", exc) from exc
        if direct.file_size_bytes != size:
            raise _error(
                "compute_file_hash",
                "unavailable",
                "115 媒体文件大小与记录不一致",
                retryable=True,
            )
        if size == 0:
            empty_sha1 = hashlib.sha1(b"").digest()
            payload = _HASH_DOMAIN + b"\x00full\x00" + (0).to_bytes(8, "big") + empty_sha1
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
            raise _cloud_error("compute_file_hash", exc) from exc
        finally:
            reader.close()
        return f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"

    async def handle_playback(self, *, media: MediaHandle, context: PlaybackContext) -> Response:
        return await self._playback.handle(media=media, context=context)

    async def handle_merged_playback(
        self,
        *,
        medias: tuple[MediaHandle, ...],
        context: PlaybackContext,
    ) -> Response:
        return await self._playback.handle_merged(medias=medias, context=context)

    def open_cover_source(self, *, media: MediaHandle) -> Cloud115RangeReader:
        return self._range_reader(
            media,
            operation="open_cover_source",
            max_fetched_bytes=COVER_MAX_FETCHED_BYTES,
        )

    def generate_thumbnails(self, *, media: MediaHandle, workspace: Path) -> ThumbnailGeneration:
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
            raise ThumbnailGenerationDeferred(
                "115 视频转码尚未完成",
                error_code="cloud115_video_transcoding",
                max_deferred_attempts=5,
                deferred_backoff_base_seconds=12 * 60 * 60,
            ) from exc
        except Cloud115NotFoundError:
            raise _error("generate_thumbnails", "source_not_found", "115 视频未提供 HLS") from None
        except Cloud115Error as exc:
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
                        raise ThumbnailBackendUnavailable(
                            "115 HLS 分片读取失败",
                            error_code="cloud115_thumbnail_unavailable",
                        ) from exc
                    except Exception as exc:
                        logger.warning(
                            "115 HLS thumbnail segment failed media_id={} segment_index={} detail={}",
                            media.media_id,
                            futures[future],
                            exc,
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
            return ClipArtifact(relative_path=destination.name)
        except ProviderOperationError:
            raise
        except Exception as exc:
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
            raise _cloud_error(operation, exc) from exc
        size = direct.file_size_bytes or media.file_size_bytes
        if size <= 0:
            raise _error(operation, "source_not_found", "115 媒体文件大小无效")
        return Cloud115RangeReader(
            direct.url,
            user_agent=direct.user_agent,
            file_size_bytes=size,
            max_fetched_bytes=max_fetched_bytes,
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
