from sakuramedia_115_provider import merged_source
from sakuramedia_115_provider.cloud115 import Cloud115DirectUrl


class _FakeReader:
    def __init__(self, *_args, **_kwargs) -> None:
        self._data = b"abcdefghij"
        self._position = 0

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def seek(self, position: int) -> int:
        self._position = position
        return position

    def read(self, size: int = -1) -> bytes:
        end = len(self._data) if size < 0 else self._position + size
        data = self._data[self._position : end]
        self._position += len(data)
        return data


def test_cloud115_merged_source_reads_and_streams(monkeypatch) -> None:
    monkeypatch.setattr(merged_source, "Cloud115RangeReader", _FakeReader)
    source = merged_source.Cloud115MergedSource(
        size=10,
        identity=("test", 10),
        url_factory=lambda: Cloud115DirectUrl(
            file_id="f",
            file_name="a.mp4",
            file_size_bytes=10,
            sha1="",
            pickcode="pc",
            url="https://direct/file",
            user_agent="ua",
            expires_at=9999999999,
        ),
    )

    assert source.read_at(2, 3) == b"cde"
    assert b"".join(source.iter_range(2, 3)) == b"cde"
