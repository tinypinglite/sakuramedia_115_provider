from __future__ import annotations

import httpx
import pytest
from sakuramedia_115_provider import range_reader
from sakuramedia_115_provider.exceptions import Cloud115RequestError


class _Chunks(httpx.SyncByteStream):
    def __iter__(self):
        yield b"123456"
        yield b"789012"

    def close(self) -> None:
        pass


def test_range_reader_rejects_full_response_that_exceeds_budget(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["range"] == "bytes=0-0"
        return httpx.Response(200, stream=_Chunks(), request=request)

    real_client = httpx.Client

    def build_client(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(range_reader.httpx, "Client", build_client)
    reader = range_reader.Cloud115RangeReader(
        "https://direct.example/file",
        user_agent="fixed-ua",
        file_size_bytes=12,
        chunk_size=1,
        max_fetched_bytes=10,
    )

    with pytest.raises(Cloud115RequestError, match="预算"):
        reader.read(1)

    reader.close()
