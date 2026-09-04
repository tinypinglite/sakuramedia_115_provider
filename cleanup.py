"""Manual cleanup jobs for imported 115 downloads and empty media folders."""

from __future__ import annotations

import re
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


async def _cleanup_imported_downloads(*, reporter: Any | None) -> dict[str, int]:
    groups = _load_imported_media_groups()
    stats = {
        "libraries": len(groups),
        "scanned_files": 0,
        "matched_files": 0,
        "ignored_small_files": 0,
        "candidate_directories": 0,
        "deleted_directories": 0,
        "skipped_directories": 0,
        "failed_libraries": 0,
    }
    for library_id, (library, media_sha1s) in groups.items():
        result: dict[str, int] = {}
        try:
            await _cleanup_download_group(
                library=library,
                media_sha1s=media_sha1s,
                result=result,
            )
        except (Cloud115Error, OSError, TypeError, ValueError) as exc:
            stats["failed_libraries"] += 1
            logger.warning(
                "115 imported-download cleanup failed library_id={} error={}",
                library_id,
                exc,
            )
        for key in (
            "scanned_files",
            "matched_files",
            "ignored_small_files",
            "candidate_directories",
            "deleted_directories",
            "skipped_directories",
        ):
            stats[key] += result[key]
        _emit_progress(reporter, stats, "清理已导入的 115 下载任务")
    logger.info("115 imported-download cleanup finished stats={}", stats)
    return stats


async def _cleanup_download_group(
    *, library: Any, media_sha1s: dict[str, set[int]], result: dict[str, int]
) -> dict[str, int]:
    result.update(
        {
            "scanned_files": 0,
            "matched_files": 0,
            "ignored_small_files": 0,
            "candidate_directories": 0,
            "deleted_directories": 0,
            "skipped_directories": 0,
        }
    )
    if not media_sha1s:
        return result

    cookie, root_cid = _library_config(
        library,
        cookie_key="device_cookie",
        root_key="downloads_root_cid",
    )
    minimum_video_size = _minimum_video_size_bytes()

    async with Cloud115Client(cookie, batch_pacing=True) as client:
        source_entries = [
            entry async for entry in client.iter_files_recursive(root_cid)
        ]
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
            result["skipped_directories"] = len(grouped)
            return result

        managed_ids = {
            cid
            for cid, entry in direct_dirs.items()
            if _is_managed_download_directory(entry.name)
        }
        candidate_ids = set(grouped) & managed_ids
        protected_ids = await _list_existing_offline_task_directories(
            client, candidate_ids
        )
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
            if (
                cid not in managed_ids
                or not matched
                or cid in protected_ids
                or blocked_by_large_video
            ):
                continue
            deletable.append(cid)

        result["candidate_directories"] = len(deletable)
        result["skipped_directories"] = len(grouped) - len(deletable)
        for batch in _chunks(deletable, DELETE_BATCH_SIZE):
            await client.delete_files(batch, parent_cid=root_cid)
            result["deleted_directories"] += len(batch)
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
) -> dict[str, str | None]:
    resolved: dict[str, str | None] = {}
    for parent_cid in sorted(parent_ids):
        if parent_cid in direct_dirs:
            resolved[parent_cid] = parent_cid
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
    return resolved


async def _list_existing_offline_task_directories(
    client: Any, candidate_ids: set[str]
) -> set[str]:
    if not candidate_ids:
        return set()
    protected: set[str] = set()
    page = 1
    while True:
        tasks, page_count = await client.list_offline_tasks(page=page)
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
                "summary_patch": dict(stats),
            }
        )
