from __future__ import annotations

import asyncio

import pytest
from sakuramedia_115_provider import offline
from sakuramedia_115_provider.cloud115 import Cloud115Entry, Cloud115OfflineTask
from sakuramedia_115_provider.exceptions import (
    Cloud115Error,
    Cloud115NotFoundError,
    Cloud115OfflineTaskExistsError,
)

from src.plugins.provider_protocol import DownloadSubmission, ProviderOperationError


class FakeClient:
    def __init__(self, _cookie: str) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def add_offline_url(self, source_uri: str, *, save_dir_id: str) -> str:
        assert source_uri == "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        assert save_dir_id == "task-dir"
        return "remote-hash"

    async def list_directory(self, _cid: str):
        return (Cloud115Entry("task-dir", "downloads", "task-a", True, 0, None, "", 0, False),)

    async def list_offline_tasks(self, *, page: int):
        assert page == 1
        return (
            (
                Cloud115OfflineTask("remote-hash", "movie", 2, 1.0, "", "", "task-dir"),
                Cloud115OfflineTask("other", "other", 1, 0.5, "", "", "outside"),
            ),
            1,
        )


def test_offline_submission_uses_display_name_directory(monkeypatch) -> None:
    async def create(_client, *, parent_cid: str, display_name: str, info_hash: str) -> str:
        assert parent_cid == "downloads"
        assert display_name == "movie"
        assert info_hash == "0123456789abcdef0123456789abcdef01234567"
        return "task-dir"

    monkeypatch.setattr(offline, "Cloud115Client", FakeClient)
    monkeypatch.setattr(offline, "_create_task_dir", create)
    provider = offline.Cloud115OfflineDownloadProvider(
        device_cookie="cookie", downloads_root_cid="downloads"
    )
    submitted = provider.submit(
        submission=DownloadSubmission(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567", "movie"
        )
    )

    assert submitted.remote_id == "remote-hash"
    assert submitted.state == "queued"

    listed = provider.list_tasks()
    assert len(listed) == 1
    assert listed[0].state == "completed"
    assert listed[0].completed_source_ref == {
        "version": 1,
        "kind": "cloud115_dir",
        "cid": "task-dir",
    }


def test_offline_submission_uses_magnet_from_torrent_redirect(monkeypatch) -> None:
    magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"

    class Response:
        is_redirect = True
        headers = {"location": magnet}

    class HTTPClient:
        def __init__(self, **kwargs) -> None:
            assert kwargs == {"timeout": 120.0, "follow_redirects": False, "trust_env": False}

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def get(self, url: str) -> Response:
            assert url == "https://index.example/movie.torrent"
            return Response()

    monkeypatch.setattr(offline.httpx, "Client", HTTPClient)

    resolved_magnet, info_hash = offline._resolve_source("https://index.example/movie.torrent")

    assert resolved_magnet == magnet
    assert info_hash == "0123456789abcdef0123456789abcdef01234567"


@pytest.mark.parametrize("managed", [True, False])
def test_offline_submission_only_reuses_managed_tasks(monkeypatch, managed):
    info_hash = "0123456789abcdef0123456789abcdef01234567"

    class DuplicateClient(FakeClient):
        async def add_offline_url(self, _source_uri, *, save_dir_id):
            raise Cloud115OfflineTaskExistsError("任务已存在")

        async def list_offline_tasks(self, *, page):
            cid = "old-dir" if managed else "outside-dir"
            return (Cloud115OfflineTask(info_hash, "movie", 1, 0.5, "", "", cid),), 1

        async def list_directory(self, _cid):
            return (Cloud115Entry("old-dir", "downloads", "task-old", True, 0, None, "", 0, False),)

    async def create(_client, **_kwargs):
        return "new-dir"

    monkeypatch.setattr(offline, "Cloud115Client", DuplicateClient)
    monkeypatch.setattr(offline, "_create_task_dir", create)
    provider = offline.Cloud115OfflineDownloadProvider(
        device_cookie="cookie", downloads_root_cid="downloads"
    )
    submission = DownloadSubmission(f"magnet:?xt=urn:btih:{info_hash}", "movie")

    if managed:
        assert provider.submit(submission=submission).remote_id == info_hash
    else:
        with pytest.raises(ProviderOperationError) as error:
            provider.submit(submission=submission)
        assert error.value.code == "task_not_managed"


INFO_HASH = "0123456789abcdef0123456789abcdef01234567"


def _offline_task(save_dir_id: str) -> Cloud115OfflineTask:
    return Cloud115OfflineTask(INFO_HASH, "movie", 2, 1.0, "", "", save_dir_id)


def _dir_entry(entry_id: str, name: str) -> Cloud115Entry:
    return Cloud115Entry(entry_id, "downloads", name, True, 0, None, "", 0, False)


def _make_delete_client(
    *,
    tasks,
    entries,
    calls,
    delete_error: Exception | None = None,
    delete_files_error: Exception | None = None,
):
    class DeleteClient:
        def __init__(self, _cookie: str) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc) -> None:
            return None

        async def list_offline_tasks(self, *, page: int):
            return (tuple(tasks), 1)

        async def list_directory(self, _cid: str):
            return tuple(entries)

        async def delete_offline_task(self, remote_id: str, *, delete_files: bool) -> None:
            if delete_error is not None:
                raise delete_error
            calls["tasks"].append((remote_id, delete_files))

        async def delete_files(self, file_ids, *, parent_cid: str | None = None) -> None:
            calls["files"].append((tuple(file_ids), parent_cid))
            if delete_files_error is not None:
                raise delete_files_error

    return DeleteClient


def _delete_provider(monkeypatch, client_cls) -> offline.Cloud115OfflineDownloadProvider:
    monkeypatch.setattr(offline, "Cloud115Client", client_cls)
    return offline.Cloud115OfflineDownloadProvider(
        device_cookie="cookie", downloads_root_cid="downloads"
    )


def test_create_task_dir_uses_sanitized_display_name() -> None:
    calls = []

    class DirClient:
        async def mkdir(self, parent_cid: str, name: str) -> str:
            calls.append((parent_cid, name))
            return "dir-cid"

    async def run(display_name):
        return await offline._create_task_dir(
            DirClient(),
            parent_cid="downloads",
            display_name=display_name,
            info_hash=INFO_HASH,
        )

    assert asyncio.run(run("AB/CD-abcdef")) == "dir-cid"
    assert asyncio.run(run("..")) == "dir-cid"
    assert asyncio.run(run(None)) == "dir-cid"
    assert calls == [
        ("downloads", "AB CD-abcdef"),
        ("downloads", INFO_HASH),
        ("downloads", INFO_HASH),
    ]


def test_managed_dir_name_rules() -> None:
    assert offline._is_managed_dir_name("task-old")
    assert offline._is_managed_dir_name(INFO_HASH)
    assert offline._is_managed_dir_name(f"ABC-001-{INFO_HASH[:6]}")
    assert not offline._is_managed_dir_name("random-folder")
    assert not offline._is_managed_dir_name("ABC-001-ABCDEF")
    assert not offline._is_managed_dir_name("")


def test_delete_task_with_files_removes_managed_directory(monkeypatch) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(_offline_task("managed-dir"),),
        entries=(_dir_entry("managed-dir", f"ABC-001-{INFO_HASH[:6]}"),),
        calls=calls,
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=True)

    assert calls["tasks"] == [(INFO_HASH, True)]
    assert calls["files"] == [(("managed-dir",), "downloads")]


def test_delete_task_without_files_keeps_directory(monkeypatch) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(_offline_task("managed-dir"),),
        entries=(_dir_entry("managed-dir", f"ABC-001-{INFO_HASH[:6]}"),),
        calls=calls,
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=False)

    assert calls["tasks"] == [(INFO_HASH, False)]
    assert calls["files"] == []


@pytest.mark.parametrize(
    "directory_name",
    [INFO_HASH, f"ABC-001-{INFO_HASH[:6]}"],
)
def test_delete_task_falls_back_to_leftover_directory(monkeypatch, directory_name) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(),
        entries=(_dir_entry("leftover-dir", directory_name),),
        calls=calls,
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=True)

    assert calls["tasks"] == [(INFO_HASH, True)]
    assert calls["files"] == [(("leftover-dir",), "downloads")]


def test_delete_task_keeps_unmanaged_save_directory(monkeypatch) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(_offline_task("outside-dir"),),
        entries=(_dir_entry("outside-dir", "random-folder"),),
        calls=calls,
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=True)

    assert calls["tasks"] == [(INFO_HASH, True)]
    assert calls["files"] == []


def test_delete_task_skips_ambiguous_leftover_directories(monkeypatch) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(),
        entries=(
            _dir_entry("first-dir", f"AAA-{INFO_HASH[:6]}"),
            _dir_entry("second-dir", f"BBB-{INFO_HASH[:6]}"),
        ),
        calls=calls,
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=True)

    assert calls["files"] == []


def test_delete_task_is_best_effort_when_directory_delete_fails(monkeypatch) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(_offline_task("managed-dir"),),
        entries=(_dir_entry("managed-dir", f"ABC-001-{INFO_HASH[:6]}"),),
        calls=calls,
        delete_files_error=Cloud115Error("boom"),
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=True)

    assert calls["tasks"] == [(INFO_HASH, True)]
    assert calls["files"] == [(("managed-dir",), "downloads")]


def test_delete_task_removes_leftover_directory_when_remote_task_is_gone(
    monkeypatch,
) -> None:
    calls = {"tasks": [], "files": []}
    client_cls = _make_delete_client(
        tasks=(),
        entries=(_dir_entry("leftover-dir", f"ABC-001-{INFO_HASH[:6]}"),),
        calls=calls,
        delete_error=Cloud115NotFoundError("gone"),
    )
    provider = _delete_provider(monkeypatch, client_cls)

    provider.delete_task(remote_id=INFO_HASH, delete_files=True)

    assert calls["files"] == [(("leftover-dir",), "downloads")]
