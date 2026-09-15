from __future__ import annotations

from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import ClassVar

import pytest
from sakuramedia_115_provider import storage
from sakuramedia_115_provider.cloud115 import (
    Cloud115Client,
    Cloud115DirectoryInfo,
    Cloud115DirectUrl,
    Cloud115Entry,
    Cloud115RapidUploadResult,
    Cloud115VideoDefinition,
    Cloud115VideoInfo,
    Cloud115VideoSegment,
)
from sakuramedia_115_provider.exceptions import (
    Cloud115NotFoundError,
    Cloud115RiskControlError,
    Cloud115VideoUnavailableError,
)

from src.plugins.provider_protocol import (
    ImportFile,
    ImportPlacement,
    LibraryHandle,
    MediaHandle,
    MediaTransferSourceInfo,
    ProviderOperationError,
    ThumbnailArtifact,
    ThumbnailGeneration,
)


class FakeClient:
    entries: ClassVar[dict[str, list[Cloud115Entry]]] = {}

    def __init__(self, _cookie: str, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def list_directory(self, cid: str):
        return tuple(type(self).entries.get(cid, []))

    async def copy_files(self, _file_ids, *, parent_cid: str) -> None:
        type(self).entries[parent_cid] = [
            Cloud115Entry("target-fid", parent_cid, "movie.mp4", False, 99, "sha", "target-pc", 0, True)
        ]

    async def move_files(self, _file_ids, *, parent_cid: str) -> None:
        await self.copy_files([], parent_cid=parent_cid)

    async def delete_files(self, _file_ids, *, parent_cid: str | None = None) -> None:
        if parent_cid:
            type(self).entries[parent_cid] = []

    async def get_video_metadata(self, _pickcode: str):
        return {
            "container": {"size_bytes": 99, "duration_seconds": 31,
                          "bit_rate": 25, "bit_rate_estimated": True},
            "video": {"width": 1920, "height": 1080},
            "audio": None, "subtitles": [],
        }

    async def get_video_info(self, _pickcode: str) -> Cloud115VideoInfo:
        return Cloud115VideoInfo(
            definitions=(
                Cloud115VideoDefinition(1, "1920x1080", "原画", "https://hls.example/video.m3u8"),
            )
        )

    async def get_video_segments(
        self, _definition: Cloud115VideoDefinition
    ) -> tuple[Cloud115VideoSegment, ...]:
        return (
            Cloud115VideoSegment(0, "https://hls.example/0.ts", 10.4),
            Cloud115VideoSegment(1, "https://hls.example/1.ts", 20.6),
        )


class ScanClient:
    recursive_entries: ClassVar[tuple[Cloud115Entry, ...]] = ()
    root_entries: ClassVar[tuple[Cloud115Entry, ...]] = ()
    directory_infos: ClassVar[dict[str, Cloud115DirectoryInfo]] = {}
    recursive_calls: ClassVar[list[str]] = []
    list_calls: ClassVar[list[tuple[str, int, int]]] = []
    directory_info_calls: ClassVar[list[str]] = []

    def __init__(self, _cookie: str, **_kwargs: object) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def iter_files_recursive(self, cid: str):
        type(self).recursive_calls.append(cid)
        for entry in type(self).recursive_entries:
            yield entry

    async def list_dir(self, cid: str, *, offset: int, limit: int):
        type(self).list_calls.append((cid, offset, limit))
        entries = type(self).root_entries
        return entries[offset : offset + limit], len(entries)

    async def directory_info(self, cid: str) -> Cloud115DirectoryInfo:
        type(self).directory_info_calls.append(cid)
        return type(self).directory_infos[cid]


class TransferClient:
    delete_calls: ClassVar[list[tuple[tuple[str, ...], str | None]]] = []
    rename_calls: ClassVar[list[tuple[str, str]]] = []
    rapid_status: ClassVar[str] = "success"

    def __init__(self, _cookie: str, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    @staticmethod
    def _rapid_upload_protocol():
        return "web"

    @staticmethod
    def _hash_source(_source, _size_bytes: int) -> str:
        return "A" * 40

    async def list_directory(self, _cid: str):
        return ()

    async def mkdir(self, parent_cid, name):
        return "folder-cid" if name == "folder" else "op-cid"

    async def iter_files_recursive(self, cid):
        yield await self.file_by_id("fid")

    async def rapid_upload(
        self, _source, *, filename: str, size_bytes: int, parent_cid: str, file_sha1: str
    ):
        assert filename == "source.mp4"
        assert size_bytes == 99
        assert parent_cid == "op-cid"
        assert file_sha1 == "A" * 40
        if type(self).rapid_status == "not_hit":
            return Cloud115RapidUploadResult("not_hit", "A" * 40)
        entry = Cloud115Entry(
            "fid", "op-cid", "source.mp4", False, 99, "A" * 40, "pick", 0, True
        )
        return Cloud115RapidUploadResult("success", "A" * 40, entry)

    async def rename_file(self, file_id: str, name: str) -> None:
        type(self).rename_calls.append((file_id, name))

    async def file_by_id(self, file_id: str):
        return Cloud115Entry(
            file_id, "op-cid", "source.mp4", False, 99, "A" * 40, "pick", 0, True
        )

    async def delete_files(self, file_ids, *, parent_cid: str | None = None) -> None:
        type(self).delete_calls.append((tuple(file_ids), parent_cid))


class TransferSource:
    info = MediaTransferSourceInfo(file_name="source.mp4", size_bytes=99)

    def open_reader(self):
        raise AssertionError("the fake rapid client must not read this source")

    def assert_unchanged(self) -> None:
        return None


class MovedReceiptClient(TransferClient):
    async def file_by_id(self, file_id: str):
        return Cloud115Entry(
            file_id, "other-cid", "target.mp4", False, 99, "A" * 40, "pick", 0, True
        )


class RenamedReceiptClient(TransferClient):
    async def file_by_id(self, file_id: str):
        return Cloud115Entry(
            file_id, "op-cid", "renamed.mp4", False, 99, "A" * 40, "pick", 0, True
        )


class MissingReceiptFileClient(TransferClient):
    async def file_by_id(self, _file_id: str):
        raise Cloud115NotFoundError("missing")

    async def list_directory(self, _cid: str):
        return (
            Cloud115Entry(
                "unknown", "op-cid", "unknown.mp4", False, 1, "B" * 40, "other", 0, True
            ),
        )


def _scan_provider(tmp_path) -> storage.Cloud115StorageProvider:
    return storage.Cloud115StorageProvider(
        library=LibraryHandle(
            1,
            "cloud115",
            {"device_cookie": "cookie", "media_root_cid": "media"},
            "123",
        ),
        data_dir=tmp_path,
    )


def test_stage_transfer_uses_operation_directory_and_abort_deletes_file_first(
    monkeypatch, tmp_path
) -> None:
    TransferClient.delete_calls = []
    TransferClient.rename_calls = []
    TransferClient.rapid_status = "success"

    async def find_dir(_client, *, parent_cid: str, name: str) -> str:
        if name == "folder":
            assert parent_cid == "media"
            return "folder-cid"
        assert parent_cid == "folder-cid"
        assert name.startswith("op-")
        return "op-cid"

    monkeypatch.setattr(storage, "Cloud115Client", TransferClient)
    monkeypatch.setattr(storage, "find_or_create_subdir", find_dir)
    provider = _scan_provider(tmp_path)
    staged = provider.stage_transfer(
        source=TransferSource(),
        placement=ImportPlacement(relative_path="folder/source.mp4"),
        operation_key="task:1:item:2",
    )

    assert staged.status == "staged"
    assert staged.storage_ref is not None and staged.storage_ref["fid"] == "fid"
    assert staged.file_name == "source.mp4"
    assert TransferClient.rename_calls == []
    assert staged.receipt is not None
    provider.finalize_transfer(receipt=staged.receipt)
    provider.abort_transfer(receipt=staged.receipt)
    assert TransferClient.delete_calls == [(("fid",), "op-cid"), (("op-cid",), None)]


def test_stage_transfer_not_hit_removes_its_empty_operation_directory(monkeypatch, tmp_path) -> None:
    TransferClient.delete_calls = []
    TransferClient.rename_calls = []
    TransferClient.rapid_status = "not_hit"

    async def find_dir(_client, *, parent_cid: str, name: str) -> str:
        return "folder-cid" if name == "folder" else "op-cid"

    monkeypatch.setattr(storage, "Cloud115Client", TransferClient)
    monkeypatch.setattr(storage, "find_or_create_subdir", find_dir)
    staged = _scan_provider(tmp_path).stage_transfer(
        source=TransferSource(),
        placement=ImportPlacement(relative_path="folder/source.mp4"),
        operation_key="task:1:item:3",
    )

    assert staged.status == "not_available"
    assert TransferClient.delete_calls == [(("op-cid",), None)]


def test_stage_transfer_never_adopts_existing_operation_directory(monkeypatch, tmp_path):
    from sakuramedia_115_provider.exceptions import Cloud115DuplicateNameError

    class DuplicateOperationClient(TransferClient):
        async def mkdir(self, parent_cid, name):
            if name.startswith("op-"):
                raise Cloud115DuplicateNameError("duplicate")
            return "folder-cid"

        async def rapid_upload(self, *_args, **_kwargs):
            raise AssertionError("must not adopt an old operation")

    DuplicateOperationClient.delete_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", DuplicateOperationClient)
    with pytest.raises(ProviderOperationError):
        _scan_provider(tmp_path).stage_transfer(
            source=TransferSource(), placement=ImportPlacement(relative_path="folder/source.mp4"), operation_key="task:duplicate",
        )
    assert DuplicateOperationClient.delete_calls == []


def _valid_transfer_receipt() -> dict[str, object]:
    return {
        "version": 1,
        "kind": storage.TRANSFER_RECEIPT_KIND,
        "target_fid": "fid",
        "target_parent_cid": "op-cid",
        "target_pickcode": "pick",
        "target_name": "source.mp4",
        "target_sha1": "A" * 40,
        "target_size_bytes": 99,
        "operation_cid": "op-cid",
    }


@pytest.mark.parametrize("client_type", [MovedReceiptClient, RenamedReceiptClient])
def test_abort_transfer_preserves_changed_file(monkeypatch, tmp_path, client_type):
    client_type.delete_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", client_type)

    with pytest.raises(ProviderOperationError):
        _scan_provider(tmp_path).abort_transfer(receipt=_valid_transfer_receipt())

    assert client_type.delete_calls == []


def test_abort_transfer_preserves_nonempty_directory_when_receipt_file_is_missing(
    monkeypatch, tmp_path
) -> None:
    MissingReceiptFileClient.delete_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", MissingReceiptFileClient)

    with pytest.raises(ProviderOperationError) as error:
        _scan_provider(tmp_path).abort_transfer(receipt=_valid_transfer_receipt())

    assert error.value.code == "unavailable"
    assert MissingReceiptFileClient.delete_calls == []


def test_scan_import_source_skips_historical_empty_directories(monkeypatch, tmp_path) -> None:
    ScanClient.recursive_entries = (
        Cloud115Entry("movie-a", "task-a", "ABC-001.mp4", False, 99, "sha-a", "pc-a", 0, True),
        Cloud115Entry("subtitle-a", "task-a", "ABC-001.srt", False, 1, "sub-a", "pc-sub", 0, False),
        Cloud115Entry("movie-b", "task-b", "ABC-002.mp4", False, 99, "sha-b", "pc-b", 0, True),
    )
    ScanClient.root_entries = (
        *(Cloud115Entry(f"empty-{index}", "source", f"old-{index}", True, 0, None, "", 0, False) for index in range(200)),
        Cloud115Entry("task-a", "source", "ABC-001", True, 0, None, "", 0, False),
        Cloud115Entry("task-b", "source", "ABC-002", True, 0, None, "", 0, False),
    )
    ScanClient.directory_infos = {}
    ScanClient.recursive_calls = []
    ScanClient.list_calls = []
    ScanClient.directory_info_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", ScanClient)

    files = _scan_provider(tmp_path).scan_import_source(
        source_ref={"version": 1, "kind": "cloud115_dir", "cid": "source"}
    )

    assert [item.relative_path for item in files] == [
        "ABC-001/ABC-001.mp4",
        "ABC-001/ABC-001.srt",
        "ABC-002/ABC-002.mp4",
    ]
    assert ScanClient.recursive_calls == ["source"]
    assert ScanClient.list_calls == [("source", 0, 1150)]
    assert ScanClient.directory_info_calls == []


def test_import_source_identity_tracks_115_source_location_and_content(
    tmp_path,
) -> None:
    provider = _scan_provider(tmp_path)
    source = ImportFile(
        source_ref={
            "version": 1,
            "kind": "cloud115_entry",
            "fid": "source-fid",
            "parent_cid": "source-parent",
            "pickcode": "source-pc",
            "name": "movie.mp4",
            "size_bytes": 99,
            "sha1": "source-sha",
            "is_dir": False,
        },
        name="movie.mp4",
        relative_path="folder/movie.mp4",
        size_bytes=99,
        is_video=True,
    )

    identity = provider.get_import_source_identity(source=source)
    assert provider.get_import_source_identity(source=source) == identity
    assert (
        provider.get_import_source_identity(
            source=replace(
                source, source_ref={**source.source_ref, "parent_cid": "new-parent"}
            )
        )
        != identity
    )
    assert (
        provider.get_import_source_identity(
            source=replace(
                source,
                source_ref={**source.source_ref, "name": "renamed.mp4"},
                name="renamed.mp4",
                relative_path="folder/renamed.mp4",
            )
        )
        != identity
    )
    assert (
        provider.get_import_source_identity(
            source=replace(source, source_ref={**source.source_ref, "sha1": "changed-sha"})
        )
        != identity
    )
    assert (
        provider.get_import_source_identity(
            source=replace(source, source_ref={**source.source_ref, "sha1": ""})
        )
        is None
    )


def test_scan_import_source_rebuilds_nested_relative_path(monkeypatch, tmp_path) -> None:
    ScanClient.recursive_entries = (
        Cloud115Entry("movie", "deep", "ABC-001.mp4", False, 99, "sha", "pc", 0, True),
    )
    ScanClient.root_entries = (
        Cloud115Entry("mid", "source", "ABC-001", True, 0, None, "", 0, False),
    )
    ScanClient.directory_infos = {
        "deep": Cloud115DirectoryInfo(
            name="CD1",
            ancestors=(("0", "根目录"), ("source", "downloads"), ("mid", "ABC-001")),
        )
    }
    ScanClient.recursive_calls = []
    ScanClient.list_calls = []
    ScanClient.directory_info_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", ScanClient)

    progress = []
    files = _scan_provider(tmp_path).scan_import_source(
        source_ref={"version": 1, "kind": "cloud115_dir", "cid": "source"},
        progress_callback=progress.append,
    )

    assert [item.relative_path for item in files] == ["ABC-001/CD1/ABC-001.mp4"]
    assert ScanClient.list_calls == [("source", 0, 1150)]
    assert ScanClient.directory_info_calls == ["deep"]
    assert progress[0]["total"] == 0
    assert any(p["text"].startswith("扫描文件") and p["current"] == p["total"] == 1 for p in progress)
    directory_progress = [p for p in progress if p["text"].startswith("解析目录路径")]
    assert directory_progress[0]["current"] == 0
    assert directory_progress[-1]["current"] == directory_progress[-1]["total"] == 1


def test_scan_reports_throttle_waits_without_changing_counts(monkeypatch, tmp_path) -> None:
    progress = []

    class WaitingClient(ScanClient):
        def __init__(self, _cookie, *, progress_callback):
            self.progress_callback = progress_callback

        def report_wait(self, stage, current, total):
            for seconds, text in [(30, "等待 30 秒"), (0, "节流等待结束")]:
                before = len(progress)
                self.progress_callback({"wait_seconds": seconds})
                assert len(progress) == before + 1
                assert progress[-1]["current"] == current
                assert progress[-1]["total"] == total
                assert progress[-1]["text"].startswith(stage)
                assert text in progress[-1]["text"]

        async def iter_files_recursive(self, cid):
            self.progress_callback({"current": 1, "total": 2})
            self.report_wait("扫描文件", 1, 2)
            yield Cloud115Entry("movie", "child", "ABC-001.mp4", False, 99, "sha", "pc", 0, True)

        async def list_dir(self, cid, *, offset, limit):
            self.report_wait("解析目录路径", 0, 1)
            return (Cloud115Entry("child", cid, "folder", True, 0, None, "", 0, False),), 1

    monkeypatch.setattr(storage, "Cloud115Client", WaitingClient)
    monkeypatch.setattr(storage.time, "monotonic", lambda: 0.0)
    files = _scan_provider(tmp_path).scan_import_source(
        source_ref={"version": 1, "kind": "cloud115_dir", "cid": "source"},
        progress_callback=progress.append,
    )
    assert [item.relative_path for item in files] == ["folder/ABC-001.mp4"]
    assert progress[-1]["current"] == progress[-1]["total"] == 1


def test_scan_media_refs_skips_relative_path_queries(monkeypatch, tmp_path) -> None:
    ScanClient.recursive_entries = (
        Cloud115Entry("movie", "deep", "ABC-001.mp4", False, 99, "sha", "pc", 0, True),
    )
    ScanClient.root_entries = ()
    ScanClient.recursive_calls = []
    ScanClient.list_calls = []
    ScanClient.directory_info_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", ScanClient)

    refs = _scan_provider(tmp_path).scan_media_refs(
        source_ref={"version": 1, "kind": "cloud115_dir", "cid": "source"}
    )

    assert refs == (
        {
            "version": 1,
            "kind": "cloud115_media",
            "fid": "movie",
            "parent_cid": "deep",
            "pickcode": "pc",
            "name": "ABC-001.mp4",
            "size_bytes": 99,
            "sha1": "sha",
            "is_dir": False,
        },
    )
    assert ScanClient.recursive_calls == ["source"]
    assert ScanClient.list_calls == []
    assert ScanClient.directory_info_calls == []


def test_scan_managed_media_ref_keys_enumerates_configured_media_root(
    monkeypatch, tmp_path
) -> None:
    ScanClient.recursive_entries = (
        Cloud115Entry("movie", "media", "movie.mp4", False, 99, "sha", "pc", 0, True),
        Cloud115Entry("subtitle", "media", "movie.srt", False, 1, "sub-sha", "sub-pc", 0, False),
    )
    ScanClient.recursive_calls = []
    ScanClient.list_calls = []
    ScanClient.directory_info_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", ScanClient)

    keys = _scan_provider(tmp_path).scan_managed_media_ref_keys()

    assert keys == {"pc", "sub-pc"}
    assert ScanClient.recursive_calls == ["media"]
    assert ScanClient.list_calls == []
    assert ScanClient.directory_info_calls == []


def test_managed_media_ref_key_uses_pickcode(tmp_path) -> None:
    key = _scan_provider(tmp_path).managed_media_ref_key(
        media_ref={
            "version": 1,
            "kind": "cloud115_media",
            "fid": "old-fid",
            "parent_cid": "old-parent",
            "pickcode": "stable-pickcode",
            "name": "old-name.mp4",
            "size_bytes": 1,
            "sha1": "old-sha",
            "is_dir": False,
        }
    )

    assert key == "stable-pickcode"


class RiskScanClient(ScanClient):
    async def iter_files_recursive(self, _cid: str):
        raise Cloud115RiskControlError("115 请求触发风控")
        yield  # pragma: no cover


def test_scan_managed_media_ref_keys_maps_risk_control_to_retryable_unavailable(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(storage, "Cloud115Client", RiskScanClient)

    with pytest.raises(ProviderOperationError) as error:
        _scan_provider(tmp_path).scan_managed_media_ref_keys()

    assert error.value.operation == "scan_managed_media_ref_keys"
    assert error.value.code == "unavailable"
    assert error.value.retryable is True


def test_stage_copy_returns_remote_media_ref_and_abort_removes_copy(monkeypatch, tmp_path) -> None:
    async def ensure(_client, *, parent_cid: str, name: str) -> str:
        cid = f"{parent_cid}/{name}"
        FakeClient.entries.setdefault(cid, [])
        return cid

    FakeClient.entries = {}
    monkeypatch.setattr(storage, "Cloud115Client", FakeClient)
    monkeypatch.setattr(storage, "find_or_create_subdir", ensure)
    library = LibraryHandle(
        1,
        "cloud115",
        {"device_cookie": "cookie", "media_root_cid": "media"},
        "123",
    )
    provider = storage.Cloud115StorageProvider(library=library, data_dir=tmp_path)
    async def forbidden(*_args, **_kwargs):
        raise AssertionError("metadata must not read direct URLs or HLS")

    monkeypatch.setattr(FakeClient, "get_video_info", forbidden)
    monkeypatch.setattr(FakeClient, "get_video_segments", forbidden)

    source = ImportFile(
        source_ref={
            "version": 1,
            "kind": "cloud115_entry",
            "fid": "source-fid",
            "parent_cid": "source-parent",
            "pickcode": "source-pc",
            "name": "movie.mp4",
            "size_bytes": 99,
            "sha1": "sha",
            "is_dir": False,
        },
        name="movie.mp4",
        relative_path="movie.mp4",
        size_bytes=99,
        is_video=True,
    )

    staged = provider.stage_import_file(
        source=source,
        placement=ImportPlacement(relative_path="jav/ABC-001/movie.mp4"),
        source_disposition="keep",
        operation_key="import:1",
    )

    assert staged.storage_ref["kind"] == "cloud115_media"
    assert staged.storage_ref["pickcode"] == "target-pc"
    assert staged.video_info["container"]["bit_rate"] == 25
    assert staged.video_info["container"]["bit_rate_estimated"] is True
    assert staged.video_info["video"] == {"width": 1920, "height": 1080}
    assert staged.duration_seconds == 31
    assert staged.resolution == "1920x1080"
    assert provider.probe_duration_seconds(
        media=MediaHandle(
            media_id=1,
            library=library,
            storage_ref=staged.storage_ref,
            file_name="movie.mp4",
            file_size_bytes=99,
            duration_seconds=0,
        )
    ) == 31
    assert provider.probe_resolution(
        media=MediaHandle(
            media_id=1,
            library=library,
            storage_ref=staged.storage_ref,
            file_name="movie.mp4",
            file_size_bytes=99,
            duration_seconds=0,
        )
    ) == "1920x1080"
    provider.abort_import(receipt=staged.receipt)
    target_dir = staged.receipt["target_parent_cid"]
    assert FakeClient.entries[target_dir] == []


def test_stage_supports_legacy_staged_media_contract(monkeypatch, tmp_path) -> None:
    @dataclass
    class LegacyStagedMedia:
        storage_ref: dict
        receipt: dict
        size_bytes: int
        duration_seconds: int | None
        video_info: dict | None

    async def ensure(_client, *, parent_cid: str, name: str) -> str:
        cid = f"{parent_cid}/{name}"
        FakeClient.entries.setdefault(cid, [])
        return cid

    FakeClient.entries = {}
    monkeypatch.setattr(storage, "StagedMedia", LegacyStagedMedia)
    monkeypatch.setattr(storage, "Cloud115Client", FakeClient)
    monkeypatch.setattr(storage, "find_or_create_subdir", ensure)
    provider = storage.Cloud115StorageProvider(
        library=LibraryHandle(
            1,
            "cloud115",
            {"device_cookie": "cookie", "media_root_cid": "media"},
            "123",
        ),
        data_dir=tmp_path,
    )
    source = ImportFile(
        source_ref={
            "version": 1,
            "kind": "cloud115_entry",
            "fid": "source-fid",
            "parent_cid": "source-parent",
            "pickcode": "source-pc",
            "name": "movie.mp4",
            "size_bytes": 99,
            "sha1": "sha",
            "is_dir": False,
        },
        name="movie.mp4",
        relative_path="movie.mp4",
        size_bytes=99,
        is_video=True,
    )

    staged = provider.stage_import_file(
        source=source,
        placement=ImportPlacement(relative_path="jav/ABC-001/movie.mp4"),
        source_disposition="keep",
        operation_key="legacy-import",
    )

    assert isinstance(staged, LegacyStagedMedia)
    assert staged.duration_seconds == 31
    assert not hasattr(staged, "resolution")


def test_resolution_probe_treats_missing_api_resolution_as_unknown() -> None:
    class NoResolutionClient:
        async def get_video_metadata(self, _pickcode: str):
            return {
                "container": {"duration_seconds": 31},
                "video": {"width": None, "height": None},
            }

    entry = Cloud115Entry(
        "source-fid",
        "source-parent",
        "movie.mp4",
        False,
        99,
        "sha",
        "source-pc",
        0,
        True,
    )
    client = NoResolutionClient()

    assert storage.run_sync(
        storage.Cloud115StorageProvider._probe_duration_and_resolution_with_client(
            client, entry
        )
    ) == (31, None)
    assert storage.run_sync(
        storage.Cloud115StorageProvider._probe_resolution_with_client(client, entry)
    ) is None


def test_stage_does_not_create_remote_paths_when_duration_probe_fails(
    monkeypatch, tmp_path
) -> None:
    created_directories: list[tuple[str, str]] = []

    async def ensure(_client, *, parent_cid: str, name: str) -> str:
        created_directories.append((parent_cid, name))
        return f"{parent_cid}/{name}"

    async def unavailable(_client, _pickcode):
        raise Cloud115VideoUnavailableError("115 视频大小或时长不可用")

    FakeClient.entries = {}
    monkeypatch.setattr(storage, "Cloud115Client", FakeClient)
    monkeypatch.setattr(storage, "find_or_create_subdir", ensure)
    monkeypatch.setattr(
        FakeClient,
        "get_video_metadata",
        unavailable,
    )
    library = LibraryHandle(
        1,
        "cloud115",
        {"device_cookie": "cookie", "media_root_cid": "media"},
        "123",
    )
    provider = storage.Cloud115StorageProvider(library=library, data_dir=tmp_path)
    source = ImportFile(
        source_ref={
            "version": 1,
            "kind": "cloud115_entry",
            "fid": "source-fid",
            "parent_cid": "source-parent",
            "pickcode": "source-pc",
            "name": "movie.mp4",
            "size_bytes": 99,
            "sha1": "sha",
            "is_dir": False,
        },
        name="movie.mp4",
        relative_path="movie.mp4",
        size_bytes=99,
        is_video=True,
    )

    with pytest.raises(ProviderOperationError, match="115 服务暂不可用") as exc_info:
        provider.stage_import_file(
            source=source,
            placement=ImportPlacement(relative_path="jav/ABC-001/movie.mp4"),
            source_disposition="keep",
            operation_key="import:1",
        )

    assert exc_info.value.code == "unavailable"
    assert created_directories == []
    assert FakeClient.entries == {}


class HashClient:
    file_size_bytes = 0

    def __init__(self, _cookie: str, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def get_download_url(self, pickcode: str, *, user_agent: str) -> Cloud115DirectUrl:
        return Cloud115DirectUrl(
            "target-fid",
            "movie.mp4",
            type(self).file_size_bytes,
            "sha",
            pickcode,
            "https://direct.example/file",
            user_agent,
            0,
        )


class VirtualRangeReader:
    def __init__(
        self,
        _url: str,
        *,
        user_agent: str,
        file_size_bytes: int,
        chunk_size: int,
        max_fetched_bytes: int,
        request_delay_range: tuple[float, float] | None = None,
    ) -> None:
        assert user_agent == Cloud115Client.DEFAULT_USER_AGENT
        assert chunk_size == 1024 * 1024
        assert max_fetched_bytes == 8 * 1024 * 1024
        assert request_delay_range == storage._HASH_REQUEST_DELAY_RANGE
        self._position = 0
        self._size = file_size_bytes

    def seek(self, offset: int) -> int:
        self._position = offset
        return offset

    def read(self, length: int) -> bytes:
        start = self._position
        end = min(start + length, self._size)
        self._position = end
        return bytes(
            (((1_103_515_245 * index + 12_345) % 2**32) >> 24) & 0xFF
            for index in range(start, end)
        )

    def close(self) -> None:
        pass


def _hash_media(size_bytes: int) -> MediaHandle:
    return MediaHandle(
        media_id=1,
        library=LibraryHandle(
            1,
            "cloud115",
            {"device_cookie": "cookie", "media_root_cid": "media"},
            "123",
        ),
        storage_ref={
            "version": 1,
            "kind": "cloud115_media",
            "fid": "target-fid",
            "parent_cid": "media",
            "pickcode": "target-pickcode",
            "name": "movie.mp4",
            "size_bytes": size_bytes,
            "sha1": "sha",
            "is_dir": False,
        },
        file_name="movie.mp4",
        file_size_bytes=size_bytes,
        duration_seconds=0,
    )


def test_compute_file_hash_matches_shared_protocol_vectors(monkeypatch, tmp_path) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    HashClient.file_size_bytes = 8 * 1024 * 1024
    monkeypatch.setattr(storage, "Cloud115Client", HashClient)
    monkeypatch.setattr(storage, "Cloud115RangeReader", VirtualRangeReader)
    monkeypatch.setattr(storage.asyncio, "sleep", no_sleep)
    provider = storage.Cloud115StorageProvider(
        library=_hash_media(HashClient.file_size_bytes).library,
        data_dir=tmp_path,
    )

    assert provider.compute_file_hash(media=_hash_media(HashClient.file_size_bytes)) == (
        "media-file-hash-v1:52385d3512a8a9ff8b6e6c5aa315e46633b28d9a"
    )

    HashClient.file_size_bytes = 0
    assert provider.compute_file_hash(media=_hash_media(0)) == (
        "media-file-hash-v1:524935ebf533f3b952f2397f80691a87a7b289c7"
    )


def test_compute_file_hash_delays_download_url(monkeypatch, tmp_path) -> None:
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(storage, "Cloud115Client", HashClient)
    monkeypatch.setattr(storage.asyncio, "sleep", sleep)
    monkeypatch.setattr(storage.random, "uniform", lambda low, high: 3.0)
    HashClient.file_size_bytes = 0
    provider = storage.Cloud115StorageProvider(
        library=_hash_media(0).library,
        data_dir=tmp_path,
    )

    provider.compute_file_hash(media=_hash_media(0))

    assert delays == [3.0]


def test_compute_file_hash_rejects_a_changed_remote_size(monkeypatch, tmp_path) -> None:
    async def no_sleep(_delay: float) -> None:
        return None

    HashClient.file_size_bytes = 101
    monkeypatch.setattr(storage, "Cloud115Client", HashClient)
    monkeypatch.setattr(storage.asyncio, "sleep", no_sleep)
    provider = storage.Cloud115StorageProvider(
        library=_hash_media(100).library,
        data_dir=tmp_path,
    )

    with pytest.raises(ProviderOperationError, match="大小与记录不一致") as exc_info:
        provider.compute_file_hash(media=_hash_media(100))

    assert exc_info.value.code == "unavailable"


def test_thumbnail_generation_reports_target_lookup_and_generated_counts(monkeypatch, tmp_path):
    media = _hash_media(10)
    provider = storage.Cloud115StorageProvider(library=media.library, data_dir=tmp_path)
    progress = []

    async def targets(_media):
        assert progress == ["正在获取视频分片"]
        return [(SimpleNamespace(index=0), [0, 10])], 2

    monkeypatch.setattr(provider, "_thumbnail_targets", targets)
    monkeypatch.setattr(
        provider, "_decode_hls_segment",
        lambda **_kwargs: [ThumbnailArtifact(0, "0.webp"), ThumbnailArtifact(10, "10.webp")],
    )
    generation = provider.generate_thumbnails(
        media=media, workspace=tmp_path / "thumbnails", progress_callback=progress.append,
    )
    assert len(generation.artifacts) == 2
    assert "已生成 0/2 张" in progress[1]
    assert "已生成 2/2 张" in progress[-1]
    assert "分片 1/1" in progress[-1]


def test_thumbnail_generation_falls_back_to_range_when_hls_is_unavailable(
    monkeypatch, tmp_path
) -> None:
    media = replace(_hash_media(99), duration_seconds=20)
    provider = storage.Cloud115StorageProvider(library=media.library, data_dir=tmp_path)
    calls = {}

    async def unavailable(_media):
        raise Cloud115VideoUnavailableError("115 未提供 HLS 播放列表")

    def fallback(**kwargs):
        calls.update(kwargs)
        return ThumbnailGeneration(
            expected_count=2,
            artifacts=(ThumbnailArtifact(0, "thumbnail-0.webp"), ThumbnailArtifact(10, "thumbnail-10.webp")),
        )

    monkeypatch.setattr(provider, "_thumbnail_targets", unavailable)
    monkeypatch.setattr(provider, "_generate_range_thumbnails", fallback)

    generation = provider.generate_thumbnails(media=media, workspace=tmp_path / "thumbnails")

    assert generation.expected_count == 2
    assert len(generation.artifacts) == 2
    assert calls["media"] is media


def test_generate_range_thumbnails_uses_three_second_delay_and_writes_frames(
    monkeypatch, tmp_path
) -> None:
    media = replace(_hash_media(99), duration_seconds=25)
    provider = storage.Cloud115StorageProvider(library=media.library, data_dir=tmp_path)
    workspace = tmp_path / "thumbnails"
    workspace.mkdir()
    reader_calls = []

    class Reader:
        fetched_bytes = 123

        def close(self):
            reader_calls.append(("close",))

    class Image:
        def __init__(self):
            self.thumbnail_args = None

        def thumbnail(self, size, resampling):
            self.thumbnail_args = (size, resampling)

        def save(self, destination, **_kwargs):
            destination.write_bytes(b"webp")

        def close(self):
            pass

    class Frame:
        is_corrupt = False

        def to_image(self):
            return Image()

    class Container:
        streams = SimpleNamespace(video=[object()])

        def seek(self, offset, **_kwargs):
            seeks.append(offset)

        def decode(self, _video):
            return iter((Frame(),))

        def close(self):
            pass

    seeks = []

    class AV:
        time_base = 1

        @staticmethod
        def open(_reader, *, mode):
            assert mode == "r"
            return Container()

    class ImageModule:
        class Resampling:
            LANCZOS = object()

    def range_reader(_media, *, operation, max_fetched_bytes, request_delay_range):
        reader_calls.append((operation, max_fetched_bytes, request_delay_range))
        return Reader()

    monkeypatch.setattr(provider, "_range_reader", range_reader)
    progress = []

    generation = provider._generate_range_thumbnails(
        media=media,
        workspace=workspace,
        av=AV,
        image_module=ImageModule,
        progress_callback=progress.append,
    )

    assert generation.expected_count == 3
    assert [artifact.offset_seconds for artifact in generation.artifacts] == [0, 10, 20]
    assert [artifact.relative_path for artifact in generation.artifacts] == [
        "thumbnail-0.webp",
        "thumbnail-10.webp",
        "thumbnail-20.webp",
    ]
    assert seeks == [0, 10, 20]
    assert reader_calls == [
        ("generate_thumbnails", 99, storage.THUMBNAIL_RANGE_REQUEST_DELAY_RANGE),
        ("close",),
    ]
    assert all((workspace / name).read_bytes() == b"webp" for name in (
        "thumbnail-0.webp",
        "thumbnail-10.webp",
        "thumbnail-20.webp",
    ))
    assert progress[0] == "正在使用原文件 Range 生成缩略图 · 已生成 0/3 张"
    assert progress[-1] == "正在使用原文件 Range 生成缩略图 · 已生成 3/3 张"


def test_generate_range_thumbnails_reads_duration_from_container_when_media_lacks_it(
    monkeypatch, tmp_path
) -> None:
    media = replace(_hash_media(99), duration_seconds=0)
    provider = storage.Cloud115StorageProvider(library=media.library, data_dir=tmp_path)
    workspace = tmp_path / "thumbnails"
    workspace.mkdir()
    seeks = []

    class Reader:
        fetched_bytes = 0

        def close(self):
            pass

    class Image:
        def thumbnail(self, *_args):
            pass

        def save(self, destination, **_kwargs):
            destination.write_bytes(b"webp")

        def close(self):
            pass

    class Frame:
        is_corrupt = False

        def to_image(self):
            return Image()

    class Container:
        duration = 25_000_000
        streams = SimpleNamespace(video=[object()])

        def seek(self, offset, **_kwargs):
            seeks.append(offset)

        def decode(self, _video):
            return iter((Frame(),))

        def close(self):
            pass

    class AV:
        time_base = 1_000_000

        @staticmethod
        def open(_reader, *, mode):
            assert mode == "r"
            return Container()

    class ImageModule:
        class Resampling:
            LANCZOS = object()

    monkeypatch.setattr(provider, "_range_reader", lambda *_args, **_kwargs: Reader())

    generation = provider._generate_range_thumbnails(
        media=media,
        workspace=workspace,
        av=AV,
        image_module=ImageModule,
        progress_callback=None,
    )

    assert generation.expected_count == 3
    assert [artifact.offset_seconds for artifact in generation.artifacts] == [0, 10, 20]
    assert seeks == [0, 10_000_000, 20_000_000]


def test_thumbnail_targets_group_offsets_by_hls_segment() -> None:
    targets, expected_count = storage._thumbnail_targets(
        (
            Cloud115VideoSegment(0, "https://hls.example/0.ts", 6),
            Cloud115VideoSegment(1, "https://hls.example/1.ts", 6),
            Cloud115VideoSegment(2, "https://hls.example/2.ts", 6),
        )
    )

    assert expected_count == 2
    assert [(segment.index, offsets) for segment, offsets in targets] == [
        (0, [0]),
        (1, [10]),
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("entry_id", "other"),
        ("parent_id", "other"),
        ("name", "other.mp4"),
        ("size_bytes", 100),
        ("sha1", "B" * 40),
        ("pickcode", "other"),
        ("is_dir", True),
    ],
)
def test_finalize_transfer_rejects_changed_target(monkeypatch, tmp_path, field, value):
    from dataclasses import replace

    class ChangedClient(TransferClient):
        async def file_by_id(self, file_id):
            return replace(await super().file_by_id(file_id), **{field: value})

    monkeypatch.setattr(storage, "Cloud115Client", ChangedClient)
    with pytest.raises(ProviderOperationError):
        _scan_provider(tmp_path).finalize_transfer(receipt=_valid_transfer_receipt())


def test_finalize_transfer_requires_directory_visibility(monkeypatch, tmp_path):
    class UnindexedClient(TransferClient):
        async def iter_files_recursive(self, cid):
            for entry in ():
                yield entry

    monkeypatch.setattr(storage, "Cloud115Client", UnindexedClient)
    with pytest.raises(ProviderOperationError):
        _scan_provider(tmp_path).finalize_transfer(receipt=_valid_transfer_receipt())


def test_init_unknown_does_not_search_or_delete_operation(monkeypatch, tmp_path, log_messages):
    from sakuramedia_115_provider.exceptions import Cloud115RequestError

    class UnknownClient(TransferClient):
        async def rapid_upload(self, *_args, **_kwargs):
            raise Cloud115RequestError("timeout https://upstream.example/file?sign=private-signature UID=private-cookie; SEID=private-session")

        async def list_directory(self, cid):
            assert cid != "op-cid", "must not search an unknown upload"
            return ()

    UnknownClient.delete_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", UnknownClient)
    with pytest.raises(ProviderOperationError):
        _scan_provider(tmp_path).stage_transfer(
            source=TransferSource(),
            placement=ImportPlacement(relative_path="folder/source.mp4"),
            operation_key="task:unknown",
        )
    assert UnknownClient.delete_calls == []
    assert any("保留操作目录" in message and "operation_cid=op-cid" in message for message in log_messages)
    assert any("operation_key=task:unknown" in message and "timeout" in message for message in log_messages)
    assert not any("已删除" in message or "秒传命中" in message for message in log_messages)
    assert not any(secret in "\n".join(log_messages) for secret in ("private-signature", "private-cookie", "private-session"))


def test_transfer_batch_reuses_parent_inventory(monkeypatch, tmp_path):
    calls = []

    class CachedClient(TransferClient):
        async def list_directory(self, cid):
            calls.append(cid)
            return ()

    CachedClient.rapid_status = "success"
    monkeypatch.setattr(storage, "Cloud115Client", CachedClient)
    provider = _scan_provider(tmp_path)
    for i in range(2):
        provider.stage_transfer(
            source=TransferSource(),
            placement=ImportPlacement(relative_path=f"folder/movie-{i}/source.mp4"),
            operation_key=f"task:{i}",
        )
    assert calls.count("media") == 1
    assert calls.count("folder-cid") == 1


def test_invalid_upload_cookie_is_rejected_before_hash_or_directory_requests(
    monkeypatch, tmp_path
):
    from sakuramedia_115_provider.cloud115 import Cloud115Client

    class RejectUnexpectedClient(Cloud115Client):
        @staticmethod
        def _hash_source(source, size):
            pytest.fail("unsupported upload cookie must not read the source")

        async def _request(self, *args, **kwargs):
            pytest.fail("unsupported upload cookie must not send requests")

    monkeypatch.setattr(storage, "Cloud115Client", RejectUnexpectedClient)
    provider = storage.Cloud115StorageProvider(
        library=LibraryHandle(
            1,
            "cloud115",
            {"device_cookie": "UID=123_A1_x; CID=c; SEID=s", "media_root_cid": "media"},
            "123",
        ),
        data_dir=tmp_path,
    )
    with pytest.raises(ProviderOperationError) as error:
        provider.stage_transfer(
            source=TransferSource(),
            placement=ImportPlacement(relative_path="folder/source.mp4"),
            operation_key="task:bad-cookie",
        )
    assert error.value.code == "authentication_failed"


@pytest.mark.parametrize("response_data", [None, {"count": 1, "data": []}])
def test_abort_cannot_delete_an_operation_using_an_invalid_empty_listing(
    monkeypatch, tmp_path, response_data
):
    import httpx
    from sakuramedia_115_provider.cloud115 import Cloud115Client

    requests = []

    def respond(request):
        requests.append(request)
        if request.url.path == "/files/get_info":
            return httpx.Response(200, json={"state": True, "data": []})
        assert request.url.path == "/files"
        return httpx.Response(
            200, json={"state": True, "cid": "op-cid", **(response_data or {})}
        )

    class Client(Cloud115Client):
        def __init__(self, cookie, **kwargs):
            super().__init__(
                "UID=123_R2_x; CID=c; SEID=s",
                pace_webapi=False,
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
                **kwargs,
            )
            self._owns_client = True

    monkeypatch.setattr(storage, "Cloud115Client", Client)
    with pytest.raises(ProviderOperationError):
        _scan_provider(tmp_path).abort_transfer(receipt=_valid_transfer_receipt())
    assert all(request.method == "GET" for request in requests)


@pytest.mark.parametrize(
    "next_batch,next_library,next_account,next_root,expected_reads",
    [
        ("7", 1, "123", "media", 1),
        ("8", 1, "123", "media", 2),
        ("7", 2, "123", "media", 2),
        ("7", 1, "456", "media", 2),
        ("7", 1, "123", "other", 2),
    ],
)
def test_download_import_directory_cache_scope(
    monkeypatch, tmp_path, next_batch, next_library, next_account, next_root, expected_reads
):
    reads = []
    created = []

    class DirectoryClient(FakeClient):
        entries = {}

        async def list_directory(self, cid):
            reads.append(cid)
            return await super().list_directory(cid)

        async def mkdir(self, parent_cid, name):
            cid = f"{parent_cid}/{name}"
            created.append(cid)
            self.entries.setdefault(parent_cid, []).append(
                Cloud115Entry(cid, parent_cid, name, True, 0, "", "", 0, False)
            )
            self.entries[cid] = []
            return cid

    monkeypatch.setattr(storage, "Cloud115Client", DirectoryClient)
    monkeypatch.setattr(storage, "_IMPORT_DIRECTORIES", {})
    source = ImportFile(
        source_ref={
            "version": 1, "kind": "cloud115_entry", "fid": "source-fid",
            "parent_cid": "source-parent", "pickcode": "source-pc",
            "name": "movie.mp4", "size_bytes": 99, "sha1": "sha", "is_dir": False,
        },
        name="movie.mp4", relative_path="movie.mp4", size_bytes=99, is_video=True,
    )
    for index, (batch, library_id, account, root) in enumerate([
        ("7", 1, "123", "media"),
        (next_batch, next_library, next_account, next_root),
    ]):
        provider = storage.Cloud115StorageProvider(
            library=LibraryHandle(
                library_id, "cloud115",
                {"device_cookie": "cookie", "media_root_cid": root}, account,
            ),
            data_dir=tmp_path,
        )
        result = provider.stage_import_file(
            source=source, placement=ImportPlacement(relative_path="jav/ABC-001/movie.mp4"),
            source_disposition="keep", operation_key=f"task:{batch}:download:{index + 1}:1",
        )
        assert result.storage_ref["pickcode"] == "target-pc"
    assert sum(reads.count(root) for root in {"media", next_root}) == expected_reads
    assert sum(reads.count(f"{root}/jav") for root in {"media", next_root}) == expected_reads
    assert created.count("media/jav") == 1
    assert created.count("media/jav/ABC-001") == 1


def test_video_info_uses_api_without_reading_original_or_hls(monkeypatch, tmp_path):
    calls = []

    class Client:
        def __init__(self, _cookie):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def get_video_metadata(self, pickcode):
            calls.append(pickcode)
            return {
                "container": {"size_bytes": 8318079847, "duration_seconds": 10773,
                              "bit_rate": 8318079847 * 8 // 10773, "bit_rate_estimated": True},
                "video": {"width": 1920, "height": 1080},
                "audio": None, "subtitles": [],
            }

    monkeypatch.setattr(storage, "Cloud115Client", Client)
    provider = _scan_provider(tmp_path)
    entry = Cloud115Entry("fid", "parent", "movie.mp4", False, 99, "sha", "pc", 0, True)
    media = MediaHandle(
        media_id=1, library=provider.library, storage_ref=storage._media_ref(entry),
        file_name="movie.mp4", file_size_bytes=99, duration_seconds=0,
    )
    info = provider.probe_video_info(media=media)
    assert info["container"]["size_bytes"] == 8318079847
    assert info["container"]["bit_rate_estimated"] is True
    assert info["video"] == {"width": 1920, "height": 1080}
    assert info["audio"] is None
    assert calls == ["pc"]


def test_video_info_unavailable_keeps_missing_value(monkeypatch, tmp_path):
    class Client(FakeClient):
        async def get_video_metadata(self, _pickcode):
            raise Cloud115VideoUnavailableError("video metadata unavailable")

    monkeypatch.setattr(storage, "Cloud115Client", Client)
    entry = Cloud115Entry("fid", "parent", "movie.mp4", False, 99, "sha", "pc", 0, True)
    provider = _scan_provider(tmp_path)
    media = MediaHandle(1, provider.library, storage._media_ref(entry), "movie.mp4", 99, 0)
    assert provider.probe_video_info(media=media) is None
