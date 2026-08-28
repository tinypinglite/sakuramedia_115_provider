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


def test_range_reader_delays_every_configured_request(monkeypatch) -> None:
    request_ranges: list[str] = []
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_range = request.headers["range"]
        request_ranges.append(request_range)
        if request_range == "bytes=0-1":
            return httpx.Response(
                206,
                content=b"a",
                headers={"Content-Range": "bytes 0-0/2"},
                request=request,
            )
        assert request_range == "bytes=1-1"
        return httpx.Response(
            206,
            content=b"b",
            headers={"Content-Range": "bytes 1-1/2"},
            request=request,
        )

    real_client = httpx.Client

    def build_client(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(range_reader.httpx, "Client", build_client)
    monkeypatch.setattr(range_reader.random, "uniform", lambda low, high: 3.0)
    monkeypatch.setattr(range_reader.time, "sleep", delays.append)
    reader = range_reader.Cloud115RangeReader(
        "https://direct.example/file",
        user_agent="fixed-ua",
        file_size_bytes=2,
        chunk_size=1,
        request_delay_range=(2.0, 4.0),
    )

    assert reader.read(2) == b"ab"
    assert request_ranges == ["bytes=0-1", "bytes=1-1"]
    assert delays == [3.0, 3.0]

    reader.close()
