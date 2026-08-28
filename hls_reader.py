"""Forward-only reader for a single 115 HLS segment."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
from typing_extensions import Self

from .exceptions import Cloud115RequestError


class Cloud115HlsSegmentReader:
    """Expose a TS segment as a streaming, non-seekable PyAV input."""

    def __init__(
        self,
        url: str,
        *,
        user_agent: str,
        chunk_size: int = 16 * 1024,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._url = url
        self._user_agent = user_agent
        self._chunk_size = chunk_size
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=30.0, trust_env=False)
        self._stream_context = None
        self._iterator: Iterator[bytes] | None = None
        self._buffer = bytearray()
        self._position = 0
        self._eof = False
        self._closed = False

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._position

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed HLS reader")
        if size == 0:
            return b""
        self._ensure_open()
        if size < 0:
            while not self._eof:
                self._consume_next_chunk()
            size = len(self._buffer)
        else:
            while len(self._buffer) < size and not self._eof:
                self._consume_next_chunk()
        content = bytes(self._buffer[:size])
        del self._buffer[:size]
        self._position += len(content)
        return content

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._stream_context is not None:
                self._stream_context.__exit__(None, None, None)
        finally:
            self._stream_context = None
            self._iterator = None
            self._buffer.clear()
            if self._owns_client:
                self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._iterator is not None:
            return
        try:
            self._stream_context = self._client.stream(
                "GET", self._url, headers={"User-Agent": self._user_agent}
            )
            response = self._stream_context.__enter__()
        except httpx.RequestError as exc:
            self.close()
            raise Cloud115RequestError("115 HLS 分片读取失败") from exc
        if response.status_code not in {200, 206}:
            self.close()
            raise Cloud115RequestError("115 HLS 分片读取失败")
        self._iterator = response.iter_bytes(chunk_size=self._chunk_size)

    def _consume_next_chunk(self) -> None:
        if self._iterator is None:
            self._eof = True
            return
        try:
            chunk = next(self._iterator)
        except StopIteration:
            self._eof = True
            return
        except httpx.RequestError as exc:
            raise Cloud115RequestError("115 HLS 分片读取失败") from exc
        if chunk:
            self._buffer.extend(chunk)
