from types import SimpleNamespace

import pytest
from sakuramedia_115_provider import cleanup

SHA1 = "A" * 40


def directory(cid, parent="root"):
    return SimpleNamespace(
        entry_id=cid, parent_id=parent, name=f"task-{cid}", is_dir=True
    )


def video(parent, *, sha1=SHA1, size=100):
    return SimpleNamespace(
        parent_id=parent,
        name="movie.mp4",
        is_dir=False,
        sha1=sha1,
        size_bytes=size,
        is_video=True,
    )


class Reporter:
    task_run_id = 42

    def __init__(self):
        self.events = []
        self.summary = {}

    def progress_callback(self, payload):
        self.events.append(payload)
        self.summary.update(payload.get("summary_patch", {}))


class TreeClient:
    def __init__(self):
        self.tree = {}
        self.files = {}
        self.deleted = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        pass

    async def file_by_id(self, cid):
        return SimpleNamespace(pickcode=cid, entry_id=cid, is_dir=True)

    async def iter_download_nodes(self, pickcode, *, directories, progress):
        for entries in self.tree.values():
            for entry in entries:
                if entry.is_dir == directories:
                    if directories:
                        yield {"fid": entry.entry_id, "pid": entry.parent_id, "fn": entry.name}
                    else:
                        yield {"pid": entry.parent_id, "fs": entry.size_bytes}

    async def iter_files_recursive(self, cid):
        for entry in self.files[cid]:
            yield entry

    async def delete_files(self, ids, *, parent_cid):
        self.deleted.append((ids, parent_cid))


@pytest.fixture
def client(monkeypatch):
    client = TreeClient()
    library = SimpleNamespace(
        id=1,
        provider_config={
            "device_cookie": "cookie",
            "media_root_cid": "root",
        },
    )
    monkeypatch.setattr(cleanup, "Cloud115Client", lambda *args, **kwargs: client)
    monkeypatch.setattr(cleanup, "_load_media_libraries", lambda: (library,))
    return client


def test_empty_cleanup_preserves_files_and_root_and_rechecks_nested_writes(client, log_messages):
    client.tree = {
        "root": (
            video("root"),
            directory("outer"),
            directory("changed"),
            directory("full"),
        ),
        "outer": (directory("inner", "outer"),),
        "inner": (),
        "changed": (directory("changed-inner", "changed"),),
        "changed-inner": (),
        "full": (video("full"), directory("empty", "full")),
        "empty": (),
    }
    # 初次扫描为空的子树，在删除前复核时出现了深层文件。
    client.files = {"outer": (), "changed": (video("changed-inner"),), "empty": ()}

    reporter = Reporter()
    result = cleanup.cleanup_empty_media_dirs(reporter, {"confirm": True})

    assert client.deleted == [(["empty"], "full"), (["outer"], "root")]
    assert result["scanned_directories"] == 7
    assert result["skipped_directories"] == 1
    assert reporter.summary == result
    assert any("正在读取媒体库根目录…" in event.get("text", "") for event in reporter.events)
    assert any(event.get("current") == event.get("total") == 3 for event in reporter.events)
    assert any("保留目录" in message and "cid=changed" in message and "path=/task-changed" in message for message in log_messages)
    assert any("已删除目录" in message and "cid=outer" in message and "path=/task-outer" in message for message in log_messages)


def test_cleanup_preserves_counts_when_second_batch_fails(client, monkeypatch):
    client.tree = {"root": (directory("a"), directory("b")), "a": (), "b": ()}
    client.files = {"root": (video("a"), video("b")), "a": (), "b": ()}
    delete_files = client.delete_files

    async def fail_second_batch(ids, *, parent_cid):
        if client.deleted:
            raise cleanup.Cloud115Error("second batch failed")
        await delete_files(ids, parent_cid=parent_cid)

    monkeypatch.setattr(client, "delete_files", fail_second_batch)
    monkeypatch.setattr(cleanup, "DELETE_BATCH_SIZE", 1)

    reporter = Reporter()
    result = cleanup.cleanup_empty_media_dirs(reporter, {"confirm": True})

    assert client.deleted == [(["a"], "root")]
    assert result["deleted_directories"] == 1
    assert result["failed_libraries"] == 1


@pytest.mark.parametrize("params", [{}, {"confirm": False}])
def test_cleanup_requires_confirmation_before_accessing_data(monkeypatch, params):
    def unexpected_access():
        pytest.fail("未确认时不应读取清理数据")

    monkeypatch.setattr(cleanup, "_load_media_libraries", unexpected_access)

    with pytest.raises(ValueError):
        cleanup.cleanup_empty_media_dirs(None, params)


def test_empty_cleanup_reports_directory_stage_before_waiting_for_115(client, monkeypatch):
    client.tree["root"] = (directory("a"),)
    reporter = Reporter()

    async def fail_directory_info(cid):
        assert reporter.events[-1]["text"].endswith("正在读取媒体库根目录…")
        raise cleanup.Cloud115Error("115 unavailable")

    monkeypatch.setattr(client, "file_by_id", fail_directory_info)
    result = cleanup.cleanup_empty_media_dirs(reporter, {"confirm": True})
    assert not client.deleted
    assert result["failed_libraries"] == 1


def test_cleanup_throttles_progress_and_restores_stage_after_wait(
    monkeypatch, log_messages
):
    now = [0.0]
    monkeypatch.setattr(cleanup.time, "monotonic", lambda: now[0])
    reporter = Reporter()
    progress = cleanup._CleanupProgress(reporter)
    progress({"text": "核对目录归属", "current": 0, "total": 100})
    for value in range(1, 12):
        now[0] = value
        progress({"text": "核对目录归属", "current": value, "total": 100})
    assert len(reporter.events) == 6
    assert len(log_messages) == 2
    progress({"wait_seconds": 23})
    assert reporter.events[-1]["text"].endswith("请求间隔等待 23 秒")
    assert reporter.events[-1]["current"] == 11
    progress({"wait_seconds": 0})
    assert reporter.events[-1]["text"] == "核对目录归属：11/100"
    progress({"text": "读取目录", "current": 0, "total": 0})
    assert reporter.events[-1]["total"] == 0


@pytest.mark.parametrize("invalid", ["orphan", "cycle", "unknown_file_parent"])
def test_empty_cleanup_rejects_incomplete_tree_before_deletion(client, invalid):
    client.tree = {"root": (directory("empty"),)}
    if invalid == "orphan":
        client.tree["bad"] = (directory("orphan", "missing"),)
    elif invalid == "cycle":
        client.tree["bad"] = (directory("a", "b"), directory("b", "a"))
    else:
        client.tree["bad"] = (video("missing"),)
    result = cleanup.cleanup_empty_media_dirs(Reporter(), {"confirm": True})
    assert result["failed_libraries"] == 1
    assert client.deleted == []


def test_empty_cleanup_preserves_zero_byte_files_and_selects_maximal_subtrees(client):
    client.tree = {
        "root": (directory("empty"), directory("occupied")),
        "empty": (directory("child", "empty"),),
        "occupied": (video("occupied", size=0),),
    }
    client.files["empty"] = ()
    result = cleanup.cleanup_empty_media_dirs(Reporter(), {"confirm": True})
    assert result["failed_libraries"] == 0
    assert client.deleted == [(["empty"], "root")]
