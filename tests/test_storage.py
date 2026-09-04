from __future__ import annotations

from dataclasses import dataclass, replace
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


def test_abort_transfer_refuses_to_delete_a_moved_or_replaced_file(
    monkeypatch, tmp_path
) -> None:
    MovedReceiptClient.delete_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", MovedReceiptClient)

    with pytest.raises(ProviderOperationError) as error:
        _scan_provider(tmp_path).abort_transfer(receipt=_valid_transfer_receipt())

    assert error.value.code == "unavailable"
    assert MovedReceiptClient.delete_calls == []


def test_abort_transfer_refuses_to_delete_a_renamed_file(monkeypatch, tmp_path) -> None:
    RenamedReceiptClient.delete_calls = []
    monkeypatch.setattr(storage, "Cloud115Client", RenamedReceiptClient)

    with pytest.raises(ProviderOperationError) as error:
        _scan_provider(tmp_path).abort_transfer(receipt=_valid_transfer_receipt())

    assert error.value.code == "unavailable"
    assert RenamedReceiptClient.delete_calls == []


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

    files = _scan_provider(tmp_path).scan_import_source(
        source_ref={"version": 1, "kind": "cloud115_dir", "cid": "source"}
    )

    assert [item.relative_path for item in files] == ["ABC-001/CD1/ABC-001.mp4"]
    assert ScanClient.list_calls == [("source", 0, 1150)]
    assert ScanClient.directory_info_calls == ["deep"]


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


def test_resolution_probe_treats_missing_hls_resolution_as_unknown() -> None:
    class NoResolutionClient:
        async def get_video_info(self, _pickcode: str) -> Cloud115VideoInfo:
            return Cloud115VideoInfo(
                definitions=(
                    Cloud115VideoDefinition(
                        300,
                        "",
                        "原画",
                        "https://hls.example/video.m3u8",
                    ),
                )
            )

        async def get_video_segments(
            self, _definition: Cloud115VideoDefinition
        ) -> tuple[Cloud115VideoSegment, ...]:
            return (Cloud115VideoSegment(0, "https://hls.example/0.ts", 31),)

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

    async def unavailable(_client, _entry) -> int:
        raise Cloud115VideoUnavailableError("115 视频转码尚未就绪")

    FakeClient.entries = {}
    monkeypatch.setattr(storage, "Cloud115Client", FakeClient)
    monkeypatch.setattr(storage, "find_or_create_subdir", ensure)
    monkeypatch.setattr(
        storage.Cloud115StorageProvider,
        "_probe_duration_and_resolution_with_client",
        staticmethod(unavailable),
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


def test_open_cover_source_uses_a_bounded_range_reader(monkeypatch, tmp_path) -> None:
    provider = storage.Cloud115StorageProvider(
        library=_hash_media(100).library,
        data_dir=tmp_path,
    )
    reader = object()
    calls = []

    def range_reader(media, *, operation, max_fetched_bytes):
        calls.append((media, operation, max_fetched_bytes))
        return reader

    monkeypatch.setattr(provider, "_range_reader", range_reader)

    assert provider.open_cover_source(media=_hash_media(100)) is reader
    assert calls == [
        (
            _hash_media(100),
            "open_cover_source",
            storage.COVER_MAX_FETCHED_BYTES,
        )
    ]


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


def test_generate_thumbnails_logs_start_progress_and_completion(monkeypatch, tmp_path) -> None:
    provider = storage.Cloud115StorageProvider(
        library=_hash_media(100).library,
        data_dir=tmp_path,
    )
    targets = [
        (Cloud115VideoSegment(index, f"https://hls.example/{index}.ts", 10), [index * 10])
        for index in range(3)
    ]
    log_records: list[tuple[str, tuple[object, ...]]] = []

    def resolve_targets(coroutine):
        coroutine.close()
        return targets, 3

    def decode(*, segment, offsets, **_kwargs):
        return [
            ThumbnailArtifact(
                offset_seconds=offset,
                relative_path=f"thumbnail-{offset}.webp",
            )
            for offset in offsets
        ]

    def info(message, *_args) -> None:
        log_records.append((message, _args))

    monkeypatch.setattr(storage, "run_sync", resolve_targets)
    monkeypatch.setattr(
        storage.Cloud115StorageProvider,
        "_decode_hls_segment",
        staticmethod(decode),
    )
    monkeypatch.setattr(storage, "THUMBNAIL_PROGRESS_LOG_SEGMENT_INTERVAL", 2)
    monkeypatch.setattr(storage, "THUMBNAIL_PROGRESS_LOG_INTERVAL_SECONDS", 60)
    monkeypatch.setattr(storage.logger, "info", info)

    result = provider.generate_thumbnails(media=_hash_media(100), workspace=tmp_path)

    assert result.expected_count == 3
    assert len(result.artifacts) == 3
    assert [message for message, _args in log_records] == [
        "115 thumbnail generation started media_id={} target_segments={} expected_thumbnails={}",
        (
            "115 thumbnail generation progress media_id={} completed_segments={}/{} "
            "generated_thumbnails={}/{} elapsed_seconds={}"
        ),
        (
            "115 thumbnail generation completed media_id={} completed_segments={} "
            "generated_thumbnails={} expected_thumbnails={} elapsed_seconds={}"
        ),
    ]
    assert log_records[0][1] == (1, 3, 3)
    assert log_records[1][1][:-1] == (1, 3, 3, 3, 3)
    assert log_records[2][1][:-1] == (1, 3, 3, 3)


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


def test_init_unknown_does_not_search_or_delete_operation(monkeypatch, tmp_path):
    from sakuramedia_115_provider.exceptions import Cloud115RequestError

    class UnknownClient(TransferClient):
        async def rapid_upload(self, *_args, **_kwargs):
            raise Cloud115RequestError("timeout")

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
