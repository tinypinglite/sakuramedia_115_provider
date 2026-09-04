from types import SimpleNamespace

import pytest
from sakuramedia_115_provider import cleanup
from sakuramedia_115_provider.cloud115 import Cloud115DirectoryInfo

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


class TreeClient:
    def __init__(self):
        self.tree = {}
        self.files = {}
        self.directories = {}
        self.tasks = ()
        self.deleted = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        pass

    async def list_directory(self, cid):
        return self.tree[cid]

    async def iter_files_recursive(self, cid):
        for entry in self.files[cid]:
            yield entry

    async def directory_info(self, cid):
        return self.directories[cid]

    async def list_offline_tasks(self, *, page):
        return self.tasks, 1

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
            "downloads_root_cid": "root",
        },
    )
    monkeypatch.setattr(cleanup, "Cloud115Client", lambda *args, **kwargs: client)
    monkeypatch.setattr(cleanup, "_load_media_libraries", lambda: (library,))
    monkeypatch.setattr(
        cleanup, "_load_imported_media_groups", lambda: {1: (library, {SHA1: {100}})}
    )
    monkeypatch.setattr(cleanup, "_minimum_video_size_bytes", lambda: 100)
    return client


def test_download_cleanup_deletes_imported_copies_and_preserves_unsafe_sources(client):
    ids = ("imported", "copy", "unmatched", "active", "completed", "wrong-size")
    client.tree["root"] = tuple(directory(cid) for cid in ids)
    client.files["root"] = (
        video("nested"),
        video("nested", sha1=None, size=1),
        video("copy"),
        video("unmatched"),
        video("unmatched", sha1="B" * 40),
        video("active"),
        video("completed"),
        video("wrong-size", size=101),
    )
    client.directories["nested"] = Cloud115DirectoryInfo(
        name="nested",
        ancestors=(("root", "downloads"), ("imported", "task-imported")),
    )
    client.tasks = (
        SimpleNamespace(save_dir_id="active", status=1),
        SimpleNamespace(save_dir_id="completed", status=2),
    )

    cleanup.cleanup_imported_downloads(None, {"confirm": True})

    assert client.deleted == [(["imported", "copy"], "root")]


def test_empty_cleanup_preserves_files_and_root_and_rechecks_nested_writes(client):
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

    cleanup.cleanup_empty_media_dirs(None, {"confirm": True})

    assert client.deleted == [(["empty"], "full"), (["outer"], "root")]


@pytest.mark.parametrize(
    "handler", [cleanup.cleanup_empty_media_dirs, cleanup.cleanup_imported_downloads]
)
def test_cleanup_preserves_counts_when_second_batch_fails(client, monkeypatch, handler):
    client.tree = {"root": (directory("a"), directory("b")), "a": (), "b": ()}
    client.files = {"root": (video("a"), video("b")), "a": (), "b": ()}
    delete_files = client.delete_files

    async def fail_second_batch(ids, *, parent_cid):
        if client.deleted:
            raise cleanup.Cloud115Error("second batch failed")
        await delete_files(ids, parent_cid=parent_cid)

    monkeypatch.setattr(client, "delete_files", fail_second_batch)
    monkeypatch.setattr(cleanup, "DELETE_BATCH_SIZE", 1)

    result = handler(None, {"confirm": True})

    assert client.deleted == [(["a"], "root")]
    assert result["deleted_directories"] == 1
    assert result["failed_libraries"] == 1


@pytest.mark.parametrize(
    "handler", [cleanup.cleanup_empty_media_dirs, cleanup.cleanup_imported_downloads]
)
@pytest.mark.parametrize("params", [{}, {"confirm": False}])
def test_cleanup_requires_confirmation_before_accessing_data(
    monkeypatch, handler, params
):
    def unexpected_access():
        pytest.fail("未确认时不应读取清理数据")

    monkeypatch.setattr(cleanup, "_load_media_libraries", unexpected_access)
    monkeypatch.setattr(cleanup, "_load_imported_media_groups", unexpected_access)

    with pytest.raises(ValueError):
        handler(None, params)
