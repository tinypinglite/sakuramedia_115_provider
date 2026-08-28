from __future__ import annotations

import httpx
from sakuramedia_115_provider.hls_reader import Cloud115HlsSegmentReader


def test_hls_reader_streams_only_requested_bytes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["user-agent"] == "thumbnail-ua"
        return httpx.Response(200, content=b"abcdef", request=request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    reader = Cloud115HlsSegmentReader(
        "https://hls.example/segment.ts",
        user_agent="thumbnail-ua",
        chunk_size=2,
        http_client=client,
    )
    try:
        assert reader.read(3) == b"abc"
        assert reader.read(3) == b"def"
    finally:
        reader.close()
        client.close()
