"""Manual cleanup jobs for imported 115 downloads and empty media folders."""

from __future__ import annotations

import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal

from loguru import logger
from pydantic import BaseModel, ConfigDict

from .cloud115 import Cloud115Client, run_sync
from .exceptions import Cloud115Error
from .offline import _INFO_HASH_DIR_RE

DELETE_BATCH_SIZE = 1000
PROVIDER_KEY = "cloud115"
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
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


class CleanupConfirmParams(BaseModel):
    """Explicit acknowledgement required by both destructive manual jobs."""

    model_config = ConfigDict(extra="forbid")

    confirm: Literal[True]


def cleanup_imported_downloads(
    reporter: Any | None,
    params: dict[str, Any] | None,
) -> dict[str, int]:
    """Delete 115 download directories whose imported media can be verified.

    Media SHA1s are used instead of the download-task ledger because the host
    may prune a task row after the import succeeds.  The explicit confirmation
    remains required because the final operation deletes remote directories.
    """

    _require_confirmation(params)
    return run_sync(_cleanup_imported_downloads(reporter=reporter))


_SKIP_REASONS = {
    "skipped_unmanaged_directories": "非托管任务目录",
    "skipped_unmatched_directories": "没有匹配到已入库媒体",
    "skipped_offline_directories": "仍关联离线任务",
    "skipped_unimported_video_directories": "存在未入库的大视频",
    "skipped_unresolved_directories": "无法确认目录归属",
}
_DOWNLOAD_COUNTS = (
    "scanned_files",
    "matched_files",
    "ignored_small_files",
    "candidate_directories",
    "deleted_directories",
    "skipped_directories",
    *_SKIP_REASONS,
)


class _CleanupProgress:
    def __init__(self, reporter):
        self.reporter = reporter
        self.library = ""
        self.stage = ""
        self.current = self.total = 0
        self.last_report = self.last_log = float("-inf")

    def __call__(self, payload):
        stage = payload.get("text", self.stage)
        current = payload.get("current", self.current)
        total = payload.get("total", self.total)
        wait = payload.get("wait_seconds")
        force = (
            stage != self.stage or (total > 0 and current == total) or wait is not None
        )
        self.stage, self.current, self.total = stage, current, total
        text = f"{self.library}{stage}"
        if total > 0:
            text += f"：{current}/{total}"
        if wait is not None and wait > 0:
            text += f" · 请求间隔等待 {wait:.0f} 秒"
        now = time.monotonic()
        if force or now - self.last_log >= 10:
            logger.info(
                "115 下载清理 task_run_id={} {}",
                getattr(self.reporter, "task_run_id", None),
                text,
            )
            self.last_log = now
        if force or now - self.last_report >= 2:
            if self.reporter is not None:
                self.reporter.progress_callback(
                    {"text": text, "current": current, "total": total}
                )
            self.last_report = now


async def _cleanup_imported_downloads(*, reporter: Any | None) -> dict[str, int]:
    started = time.monotonic()
    progress = _CleanupProgress(reporter)
    progress({"text": "读取已入库媒体"})
    groups = _load_imported_media_groups()
    stats = dict.fromkeys(_DOWNLOAD_COUNTS, 0)
    stats.update(libraries=len(groups), failed_libraries=0)
    for index, (library_id, (library, media_sha1s)) in enumerate(groups.items(), 1):
        progress.library = f"媒体库 {index}/{len(groups)}（ID {library_id}）· "
        progress({"text": "准备清理", "current": 0, "total": 0})
        result: dict[str, int] = {}
        try:
            await _cleanup_download_group(
                library=library,
                media_sha1s=media_sha1s,
                result=result,
                progress=progress,
            )
        except (Cloud115Error, OSError, TypeError, ValueError) as exc:
            stats["failed_libraries"] += 1
            logger.warning(
                "115 下载清理失败 library_id={} stage={} error={}",
                library_id,
                progress.stage,
                exc,
            )
        for key in _DOWNLOAD_COUNTS:
            stats[key] += result[key]
        _emit_progress(reporter, stats, f"媒体库 {index}/{len(groups)} 已处理")
    stats["elapsed_seconds"] = round(time.monotonic() - started)
    _emit_progress(reporter, stats, "")
    logger.info("115 imported-download cleanup finished stats={}", stats)
    if stats["failed_libraries"]:
        raise Cloud115Error(f"115 imported-download cleanup failed: {stats}")
    return stats


async def _cleanup_download_group(
    *,
    library: Any,
    media_sha1s: dict[str, set[int]],
    result: dict[str, int],
    progress: _CleanupProgress,
) -> dict[str, int]:
    result.update(dict.fromkeys(_DOWNLOAD_COUNTS, 0))
    if not media_sha1s:
        progress({"text": "没有可匹配的已入库媒体，跳过清理", "current": 0, "total": 0})
        return result

    cookie, root_cid = _library_config(
        library,
        cookie_key="device_cookie",
        root_key="downloads_root_cid",
    )
    minimum_video_size = _minimum_video_size_bytes()

    async with Cloud115Client(
        cookie, batch_pacing=True, progress_callback=progress
    ) as client:
        progress({"text": "扫描下载文件", "current": 0, "total": 0})
        source_entries = [
            entry async for entry in client.iter_files_recursive(root_cid)
        ]
        progress(
            {
                "text": "扫描下载文件",
                "current": len(source_entries),
                "total": len(source_entries),
            }
        )
        result["scanned_files"] = len(source_entries)
        result["matched_files"] = sum(
            _media_matches_entry(entry, media_sha1s) for entry in source_entries
        )
        result["ignored_small_files"] = sum(
            _is_video_entry(entry)
            and entry.size_bytes < minimum_video_size
            and not _media_matches_entry(entry, media_sha1s)
            for entry in source_entries
        )
        relevant_entries = tuple(
            entry
            for entry in source_entries
            if _media_matches_entry(entry, media_sha1s)
            or (_is_video_entry(entry) and entry.size_bytes >= minimum_video_size)
        )
        if not relevant_entries:
            return result

        progress({"text": "读取下载根目录", "current": 0, "total": 0})
        root_entries = await client.list_directory(root_cid)
        direct_dirs = {
            entry.entry_id: entry
            for entry in root_entries
            if entry.is_dir and entry.parent_id == root_cid
        }
        parent_ids = {
            entry.parent_id
            for entry in relevant_entries
            if entry.parent_id and entry.parent_id != root_cid
        }
        parent_to_top = await _resolve_top_level_directories(
            client=client,
            root_cid=root_cid,
            direct_dirs=direct_dirs,
            parent_ids=parent_ids,
            progress=progress,
        )
        grouped: dict[str, list[Any]] = defaultdict(list)
        unresolved = False
        for entry in relevant_entries:
            if entry.parent_id == root_cid:
                continue
            top_cid = parent_to_top.get(entry.parent_id)
            if top_cid is None:
                unresolved = True
                continue
            grouped[top_cid].append(entry)

        if unresolved:
            # A relevant file whose ancestry cannot be proven to be below
            # the configured root makes the whole snapshot non-deletable.
            result["skipped_directories"] = result["skipped_unresolved_directories"] = (
                len(grouped)
            )
            logger.warning(
                "115 下载清理保留全部候选目录 library_id={} reason=无法确认目录归属 count={}",
                library.id,
                len(grouped),
            )
            return result

        managed_ids = {
            cid
            for cid, entry in direct_dirs.items()
            if _is_managed_download_directory(entry.name)
        }
        candidate_ids = set(grouped) & managed_ids
        protected_ids = await _list_existing_offline_task_directories(
            client, candidate_ids, progress
        )
        progress({"text": "筛选可删除目录", "current": 0, "total": 0})
        deletable: list[str] = []
        for cid, entries in grouped.items():
            matched = [
                entry for entry in entries if _media_matches_entry(entry, media_sha1s)
            ]
            blocked_by_large_video = any(
                _is_video_entry(entry)
                and entry.size_bytes >= minimum_video_size
                and not _media_matches_entry(entry, media_sha1s)
                for entry in entries
            )
            if cid not in managed_ids:
                reason = "skipped_unmanaged_directories"
            elif not matched:
                reason = "skipped_unmatched_directories"
            elif cid in protected_ids:
                reason = "skipped_offline_directories"
            elif blocked_by_large_video:
                reason = "skipped_unimported_video_directories"
            else:
                reason = None
            if reason:
                result[reason] += 1
                logger.info(
                    "115 下载清理保留目录 library_id={} cid={} name={} reason={}",
                    library.id,
                    cid,
                    direct_dirs[cid].name,
                    _SKIP_REASONS[reason],
                )
                continue
            deletable.append(cid)

        result["candidate_directories"] = len(deletable)
        result["skipped_directories"] = len(grouped) - len(deletable)
        progress({"text": "删除目录", "current": 0, "total": len(deletable)})
        for batch in _chunks(deletable, DELETE_BATCH_SIZE):
            await client.delete_files(batch, parent_cid=root_cid)
            result["deleted_directories"] += len(batch)
            for cid in batch:
                logger.info(
                    "115 下载清理已删除目录 library_id={} cid={} name={}",
                    library.id,
                    cid,
                    direct_dirs[cid].name,
                )
            progress(
                {
                    "text": "删除目录",
                    "current": result["deleted_directories"],
                    "total": len(deletable),
                }
            )
    return result


def cleanup_empty_media_dirs(
    reporter: Any | None,
    params: dict[str, Any] | None,
) -> dict[str, int]:
    """Delete empty descendants below every configured 115 media root."""

    _require_confirmation(params)
    return run_sync(_cleanup_empty_media_dirs(reporter=reporter))


async def _cleanup_empty_media_dirs(*, reporter: Any | None) -> dict[str, int]:
    libraries = _load_media_libraries()
    stats = {
        "libraries": len(libraries),
        "scanned_directories": 0,
        "candidate_directories": 0,
        "deleted_directories": 0,
        "failed_libraries": 0,
    }
    for library in libraries:
        try:
            cookie, root_cid = _library_config(
                library,
                cookie_key="device_cookie",
                root_key="media_root_cid",
            )
            async with Cloud115Client(cookie, batch_pacing=True) as client:
                deletions, scanned = await _find_empty_directories(client, root_cid)
                stats["scanned_directories"] += scanned
                stats["candidate_directories"] += sum(
                    len(ids) for ids in deletions.values()
                )
                for parent_cid in sorted(deletions):
                    for batch in _chunks(
                        sorted(deletions[parent_cid]), DELETE_BATCH_SIZE
                    ):
                        # 整棵子树复核无文件后才删除，避免沿用扫描阶段的判空结果。
                        empty = []
                        for cid in batch:
                            async for _entry in client.iter_files_recursive(cid):
                                break
                            else:
                                empty.append(cid)
                        if empty:
                            await client.delete_files(empty, parent_cid=parent_cid)
                            stats["deleted_directories"] += len(empty)
        except (Cloud115Error, OSError, TypeError, ValueError) as exc:
            stats["failed_libraries"] += 1
            logger.warning(
                "115 empty-media-directory cleanup failed library_id={} error={}",
                library.id,
                exc,
            )
        _emit_progress(reporter, stats, "清理 115 媒体目录中的空目录")
    logger.info("115 empty-media-directory cleanup finished stats={}", stats)
    return stats


async def _find_empty_directories(
    client: Cloud115Client, root_cid: str
) -> tuple[dict[str, list[str]], int]:
    deletions: dict[str, list[str]] = defaultdict(list)
    visited: set[str] = set()

    async def visit(cid: str, *, is_root: bool) -> bool:
        if cid in visited:
            raise Cloud115Error("115 目录树存在循环")
        visited.add(cid)
        entries = await client.list_directory(cid)
        has_file = False
        has_non_empty_child = False
        empty_children: list[str] = []
        for entry in entries:
            if not entry.is_dir:
                has_file = True
                continue
            if entry.parent_id != cid:
                raise Cloud115Error("115 目录列表的父目录不一致")
            if await visit(entry.entry_id, is_root=False):
                empty_children.append(entry.entry_id)
            else:
                has_non_empty_child = True
        if not has_file and not has_non_empty_child:
            if is_root:
                # The configured root is never removed; only its empty direct
                # children are candidates.
                deletions[cid].extend(empty_children)
                return False
            return True
        if empty_children:
            deletions[cid].extend(empty_children)
        return False

    await visit(root_cid, is_root=True)
    return dict(deletions), len(visited)


def _load_imported_media_groups() -> dict[int, tuple[Any, dict[str, set[int]]]]:
    """Index valid cloud115 media by the SHA1 stored in their media refs."""

    from src.model import Media, MediaLibrary

    groups: dict[int, tuple[Any, dict[str, set[int]]]] = {
        library.id: (library, {}) for library in _load_media_libraries()
    }
    query = (
        Media.select(Media, MediaLibrary)
        .join(MediaLibrary)
        .where((MediaLibrary.provider_key == PROVIDER_KEY) & (Media.valid == True))
    )
    for media in query:
        current = groups.get(media.library_id)
        if current is None:
            continue
        storage_ref = media.storage_ref
        if not isinstance(storage_ref, dict):
            continue
        sha1 = _normalise_sha1(storage_ref.get("sha1"))
        if sha1 is None:
            continue
        sizes = current[1].setdefault(sha1, set())
        for size in (media.file_size_bytes, storage_ref.get("size_bytes")):
            if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
                sizes.add(size)
    return groups


def _normalise_sha1(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value.upper() if _SHA1_RE.fullmatch(value) else None


def _media_matches_entry(entry: Any, media_sha1s: dict[str, set[int]]) -> bool:
    sha1 = _normalise_sha1(entry.sha1)
    if sha1 is None or sha1 not in media_sha1s:
        return False
    sizes = media_sha1s[sha1]
    return not sizes or entry.size_bytes in sizes


def _is_video_entry(entry: Any) -> bool:
    return entry.is_video or Path(entry.name).suffix.lower() in _VIDEO_SUFFIXES


def _is_managed_download_directory(name: str) -> bool:
    return name.startswith("task-") or _INFO_HASH_DIR_RE.fullmatch(name) is not None


def _minimum_video_size_bytes() -> int:
    from src.config.config import settings

    value = settings.media.allowed_min_video_file_size
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("invalid minimum video file size")
    return value


async def _resolve_top_level_directories(
    *,
    client: Any,
    root_cid: str,
    direct_dirs: dict[str, Any],
    parent_ids: set[str],
    progress: _CleanupProgress,
) -> dict[str, str | None]:
    resolved: dict[str, str | None] = {}
    progress({"text": "核对目录归属", "current": 0, "total": len(parent_ids)})
    for parent_cid in sorted(parent_ids):
        if parent_cid in direct_dirs:
            resolved[parent_cid] = parent_cid
            progress(
                {
                    "text": "核对目录归属",
                    "current": len(resolved),
                    "total": len(parent_ids),
                }
            )
            continue
        directory = await client.directory_info(parent_cid)
        found_root = False
        top_cid: str | None = None
        for ancestor_cid, _ancestor_name in directory.ancestors:
            if found_root:
                top_cid = ancestor_cid
                break
            if ancestor_cid == root_cid:
                found_root = True
        resolved[parent_cid] = top_cid if top_cid in direct_dirs else None
        progress(
            {"text": "核对目录归属", "current": len(resolved), "total": len(parent_ids)}
        )
    return resolved


async def _list_existing_offline_task_directories(
    client: Any, candidate_ids: set[str], progress: _CleanupProgress
) -> set[str]:
    if not candidate_ids:
        return set()
    progress({"text": "检查离线任务页数", "current": 0, "total": 0})
    protected: set[str] = set()
    page = 1
    while True:
        tasks, page_count = await client.list_offline_tasks(page=page)
        progress({"text": "检查离线任务页数", "current": page, "total": page_count})
        for task in tasks:
            if task.save_dir_id in candidate_ids:
                protected.add(task.save_dir_id)
        if page >= page_count or not tasks:
            return protected
        page += 1


def _load_media_libraries() -> tuple[Any, ...]:
    from src.model import MediaLibrary

    return tuple(
        MediaLibrary.select()
        .where(MediaLibrary.provider_key == PROVIDER_KEY)
        .order_by(MediaLibrary.id.asc())
    )


def _library_config(library: Any, *, cookie_key: str, root_key: str) -> tuple[str, str]:
    config = library.provider_config
    if not isinstance(config, dict):
        raise TypeError(f"115 媒体库配置无效 library_id={library.id}")
    cookie = config.get(cookie_key)
    root_cid = config.get(root_key)
    if (
        not isinstance(cookie, str)
        or not cookie
        or not isinstance(root_cid, str)
        or not root_cid
    ):
        raise ValueError(f"115 媒体库配置不完整 library_id={library.id}")
    return cookie, root_cid


def _require_confirmation(params: dict[str, Any] | None) -> None:
    CleanupConfirmParams.model_validate(params or {})


def _chunks(values: list[str], size: int):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _emit_progress(reporter: Any | None, stats: dict[str, int], text: str) -> None:
    if reporter is not None:
        reporter.progress_callback(
            {
                "text": text,
                "current": 0,
                "total": 0,
                "summary_patch": dict(stats),
            }
        )
