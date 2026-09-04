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
