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
from .exceptions import Cloud115Error, safe_error_message
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
    def __init__(self, reporter, *, label="115 下载清理"):
        self.reporter = reporter
        self.label = label
        self.detail = ""
        self.location = None
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
        self.location = payload.get("location", self.location if stage == self.stage else None)
        self.detail = payload.get("detail", self.detail if stage == self.stage else "")
        self.stage, self.current, self.total = stage, current, total
        text = f"{self.library}{stage}"
        if total > 0:
            text += f"：{current}/{total}"
        if self.detail:
            text += f" · {self.detail}"
        if wait is not None and wait > 0:
            text += f" · 请求间隔等待 {wait:.0f} 秒"
        now = time.monotonic()
        if force or now - self.last_log >= 10:
            logger.info(
                "{} task_run_id={} {}",
                self.label,
                getattr(self.reporter, "task_run_id", None),
                text + (f" cid={self.location[0]}" if self.location else ""),
            )
            self.last_log = now
        if force or now - self.last_report >= 2:
            if self.reporter is not None:
                if self.location and len(self.location[1]) > 100:
                    path = self.location[1]
                    text = text.replace(path, path[:40] + "…" + path[-59:])
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

        _pickcode, directories, _paths = await _load_directory_tree(client, root_cid, progress)
        direct_dirs = {
            cid: node for cid, node in directories.items() if node["pid"] == root_cid
        }
        parent_ids = {
            entry.parent_id
            for entry in relevant_entries
            if entry.parent_id and entry.parent_id != root_cid
        }
        parent_to_top = _resolve_top_level_directories(
            root_cid=root_cid, directories=directories,
            parent_ids=parent_ids, progress=progress,
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
            if _is_managed_download_directory(entry["fn"])
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
                    direct_dirs[cid]["fn"],
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
                    direct_dirs[cid]["fn"],
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
    started = time.monotonic()
    progress = _CleanupProgress(reporter, label="115 空目录清理")
    progress({"text": "读取媒体库"})
    libraries = _load_media_libraries()
    stats = {
        "libraries": len(libraries),
        "scanned_directories": 0,
        "candidate_directories": 0,
        "deleted_directories": 0,
        "skipped_directories": 0,
        "failed_libraries": 0,
    }
    for index, library in enumerate(libraries, 1):
        progress.library = f"媒体库 {index}/{len(libraries)}（ID {library.id}）· "
        progress({"text": "准备扫描", "current": 0, "total": 0})
        try:
            cookie, root_cid = _library_config(
                library,
                cookie_key="device_cookie",
                root_key="media_root_cid",
            )
            async with Cloud115Client(cookie, batch_pacing=True) as client:
                deletions, paths = await _find_empty_directories(client, root_cid, progress)
                scanned = len(paths)
                stats["scanned_directories"] += scanned
                total = sum(len(ids) for ids in deletions.values())
                stats["candidate_directories"] += total
                logger.info("115 空目录扫描完成 library_id={} root_cid={} scanned={} candidates={}",
                            library.id, root_cid, scanned, total)
                processed = 0
                progress({"text": "复核清理", "current": 0, "total": total,
                          "detail": "没有空目录需要清理" if not total else ""})
                for parent_cid in sorted(deletions):
                    for batch in _chunks(sorted(deletions[parent_cid]), DELETE_BATCH_SIZE):
                        # 整棵子树复核无文件后才删除，避免沿用扫描阶段的判空结果。
                        empty = []
                        for cid in batch:
                            progress({"detail": f"当前：{paths[cid]}", "location": (cid, paths[cid])})
                            async for _entry in client.iter_files_recursive(cid):
                                stats["skipped_directories"] += 1
                                logger.info("115 空目录清理保留目录 library_id={} cid={} path={} reason=复核发现文件",
                                            library.id, cid, paths[cid])
                                break
                            else:
                                empty.append(cid)
                        if empty:
                            progress({"detail": f"正在删除 {len(empty)} 个目录，父目录：{paths[parent_cid]}",
                                      "location": (parent_cid, paths[parent_cid])})
                            await client.delete_files(empty, parent_cid=parent_cid)
                            stats["deleted_directories"] += len(empty)
                            for cid in empty:
                                logger.info("115 空目录清理已删除目录 library_id={} parent_cid={} cid={} path={}",
                                            library.id, parent_cid, cid, paths[cid])
                        processed += len(batch)
                        progress({"current": processed, "detail": "", "location": None})
        except (Cloud115Error, OSError, TypeError, ValueError) as exc:
            stats["failed_libraries"] += 1
            logger.warning("115 空目录清理失败 library_id={} stage={} detail={} location={} error_type={} reason={}",
                           library.id, progress.stage, progress.detail, progress.location, type(exc).__name__, safe_error_message(exc))
        if reporter is not None:
            reporter.progress_callback({"summary_patch": dict(stats)})
    stats["elapsed_seconds"] = round(time.monotonic() - started)
    _emit_progress(reporter, stats,
                   f"空目录清理结束：扫描 {stats['scanned_directories']} 个目录，"
                   f"删除 {stats['deleted_directories']} 个，保留 {stats['skipped_directories']} 个，"
                   f"失败媒体库 {stats['failed_libraries']} 个")
    logger.info("115 empty-media-directory cleanup finished stats={}", stats)
    return stats


async def _load_directory_tree(
    client: Cloud115Client, root_cid: str, progress: _CleanupProgress
) -> tuple[str, dict[str, Any], dict[str, str]]:
    progress({"text": "读取目录简表", "detail": "正在读取媒体库根目录…", "current": 0, "total": 0})
    if root_cid == "0":
        raise Cloud115Error("115 账号根目录不支持子树简表，请配置具体目录")
    root = await client.file_by_id(root_cid)
    if not root.is_dir or root.entry_id != root_cid:
        raise Cloud115Error("115 返回的根目录信息不一致")
    nodes = {}
    async for row in client.iter_download_nodes(root.pickcode, directories=True, progress=progress):
        cid, parent = str(row["fid"]), str(row["pid"])
        if cid == root_cid or cid in nodes:
            raise Cloud115Error("115 目录简表包含根目录或重复目录")
        nodes[cid] = {"pid": parent, "fn": row["fn"]}
    paths = {root_cid: "/"}
    for cid in nodes:
        chain: list[str] = []
        visiting: set[str] = set()
        current = cid
        while current not in paths:
            if current not in nodes or current in visiting:
                raise Cloud115Error("115 目录简表缺少父目录或存在循环")
            visiting.add(current)
            chain.append(current)
            current = nodes[current]["pid"]
        for child in reversed(chain):
            node = nodes[child]
            paths[child] = f"{paths[node['pid']].rstrip('/')}/{node['fn']}"
    return root.pickcode, nodes, paths


async def _find_empty_directories(
    client: Cloud115Client, root_cid: str, progress: _CleanupProgress
) -> tuple[dict[str, list[str]], dict[str, str]]:
    pickcode, nodes, paths = await _load_directory_tree(client, root_cid, progress)
    occupied = {root_cid}
    async for row in client.iter_download_nodes(pickcode, directories=False, progress=progress):
        cid = str(row["pid"])
        if cid not in paths:
            raise Cloud115Error("115 文件简表包含未知父目录，停止清理")
        while cid not in occupied:
            occupied.add(cid)
            cid = nodes[cid]["pid"]
    deletions: dict[str, list[str]] = defaultdict(list)
    for cid, node in nodes.items():
        if cid not in occupied and node["pid"] in occupied:
            deletions[node["pid"]].append(cid)
    logger.info("115 空目录筛选完成：directories={} candidates={}", len(nodes), sum(map(len, deletions.values())))
    return dict(deletions), paths


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


def _resolve_top_level_directories(
    *, root_cid: str, directories: dict[str, Any], parent_ids: set[str],
    progress: _CleanupProgress,
) -> dict[str, str | None]:
    resolved: dict[str, str | None] = {}
    progress({"text": "核对目录归属", "current": 0, "total": len(parent_ids)})
    for parent_cid in sorted(parent_ids):
        current = parent_cid
        while current in directories and directories[current]["pid"] != root_cid:
            current = directories[current]["pid"]
        resolved[parent_cid] = current if current in directories else None
        progress({"text": "核对目录归属", "current": len(resolved), "total": len(parent_ids)})
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
