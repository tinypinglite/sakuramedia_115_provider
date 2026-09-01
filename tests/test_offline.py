from __future__ import annotations

import pytest
from sakuramedia_115_provider import offline
from sakuramedia_115_provider.cloud115 import Cloud115Entry, Cloud115OfflineTask
from sakuramedia_115_provider.exceptions import Cloud115OfflineTaskExistsError

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


def test_offline_submission_uses_info_hash_directory(monkeypatch) -> None:
    async def create(_client, *, parent_cid: str, info_hash: str) -> str:
        assert parent_cid == "downloads"
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


def test_offline_submission_converts_torrent_to_magnet(monkeypatch) -> None:
    monkeypatch.setattr(offline, "_download_torrent", lambda _url: b"torrent")
    monkeypatch.setattr(
        offline, "_torrent_info_hash", lambda _payload: "0123456789abcdef0123456789abcdef01234567"
    )

    magnet, info_hash = offline._resolve_source("https://index.example/movie.torrent")

    assert magnet == "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
    assert info_hash == "0123456789abcdef0123456789abcdef01234567"


def test_offline_submission_reuses_existing_task_in_managed_directory(monkeypatch) -> None:
    class DuplicateClient(FakeClient):
        async def add_offline_url(self, _source_uri: str, *, save_dir_id: str) -> str:
            assert save_dir_id == "new-dir"
            raise Cloud115OfflineTaskExistsError("任务已存在")

        async def list_offline_tasks(self, *, page: int):
            assert page == 1
            return (
                (
                    Cloud115OfflineTask(
                        "0123456789abcdef0123456789abcdef01234567",
                        "movie",
                        1,
                        0.5,
                        "",
                        "",
                        "old-dir",
                    ),
                ),
                1,
            )

        async def list_directory(self, _cid: str):
            return (
                Cloud115Entry("old-dir", "downloads", "task-old", True, 0, None, "", 0, False),
            )

    async def create(_client, *, parent_cid: str, info_hash: str) -> str:
        assert parent_cid == "downloads"
        assert info_hash == "0123456789abcdef0123456789abcdef01234567"
        return "new-dir"

    monkeypatch.setattr(offline, "Cloud115Client", DuplicateClient)
    monkeypatch.setattr(offline, "_create_task_dir", create)
    provider = offline.Cloud115OfflineDownloadProvider(
        device_cookie="cookie", downloads_root_cid="downloads"
    )

    submitted = provider.submit(
        submission=DownloadSubmission(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567", "movie"
        )
    )

    assert submitted.remote_id == "0123456789abcdef0123456789abcdef01234567"


def test_offline_submission_rejects_existing_task_outside_managed_directory(monkeypatch) -> None:
    class DuplicateClient(FakeClient):
        async def add_offline_url(self, _source_uri: str, *, save_dir_id: str) -> str:
            assert save_dir_id == "new-dir"
            raise Cloud115OfflineTaskExistsError("任务已存在")

        async def list_offline_tasks(self, *, page: int):
            assert page == 1
            return (
                (
                    Cloud115OfflineTask(
                        "0123456789abcdef0123456789abcdef01234567",
                        "movie",
                        1,
                        0.5,
                        "",
                        "",
                        "outside-dir",
                    ),
                ),
                1,
            )

        async def list_directory(self, _cid: str):
            return (
                Cloud115Entry("new-dir", "downloads", "task-new", True, 0, None, "", 0, False),
            )

    async def create(_client, *, parent_cid: str, info_hash: str) -> str:
        assert parent_cid == "downloads"
        assert info_hash == "0123456789abcdef0123456789abcdef01234567"
        return "new-dir"

    monkeypatch.setattr(offline, "Cloud115Client", DuplicateClient)
    monkeypatch.setattr(offline, "_create_task_dir", create)
    provider = offline.Cloud115OfflineDownloadProvider(
        device_cookie="cookie", downloads_root_cid="downloads"
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.submit(
            submission=DownloadSubmission(
                "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567", "movie"
            )
        )

    assert error.value.code == "task_not_managed"
    assert error.value.safe_message == "同哈希离线任务已存在，但不在当前下载目录，当前下载器无法接管"
