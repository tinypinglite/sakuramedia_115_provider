"""A sequential, seekable Range reader for 115 direct URLs."""

from __future__ import annotations

import random
import re
import time

import httpx
from typing_extensions import Self

from .exceptions import Cloud115RequestError

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")


class Cloud115RangeReader:
    """File-like source for PyAV; only one upstream Range request is in flight."""

    def __init__(
        self,
        url: str,
        *,
        user_agent: str,
        file_size_bytes: int,
        chunk_size: int = 4 * 1024 * 1024,
        max_fetched_bytes: int | None = None,
        request_delay_range: tuple[float, float] | None = None,
    ) -> None:
        if not url or not user_agent or file_size_bytes <= 0 or chunk_size <= 0:
            raise ValueError("invalid 115 range reader arguments")
        self._url = url
        self._user_agent = user_agent
        self._file_size = file_size_bytes
        self._chunk_size = chunk_size
        self._max_fetched_bytes = max_fetched_bytes
        self._request_delay_range = request_delay_range
        self._client = httpx.Client(timeout=30.0, trust_env=False, follow_redirects=True)
        self._position = 0
        self._buffer = b""
        self._buffer_start = 0
        self.fetched_bytes = 0

    @property
    def file_size(self) -> int:
        return self._file_size

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset
        elif whence == 2:
            self._position = self._file_size + offset
        else:
            raise ValueError("unsupported seek origin")
        self._position = max(0, self._position)
        return self._position

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = self._file_size - self._position
        output = bytearray()
        while size > 0 and self._position < self._file_size:
            offset = self._position - self._buffer_start
            if 0 <= offset < len(self._buffer):
                take = min(size, len(self._buffer) - offset)
                output += self._buffer[offset : offset + take]
                self._position += take
                size -= take
                continue
            self._fetch(self._position, size)
        return bytes(output)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _fetch(self, start: int, requested: int) -> None:
        count = max(self._chunk_size, requested)
        remaining: int | None = None
        if self._max_fetched_bytes is not None:
            remaining = self._max_fetched_bytes - self.fetched_bytes
            if remaining <= 0:
                raise Cloud115RequestError("115 Range 读取超过预算")
            count = min(count, remaining)
        end = min(start + count, self._file_size) - 1
        try:
            if self._request_delay_range is not None:
                time.sleep(random.uniform(*self._request_delay_range))
            with self._client.stream(
                "GET",
                self._url,
                headers={"User-Agent": self._user_agent, "Range": f"bytes={start}-{end}"},
            ) as response:
                if response.status_code not in {200, 206}:
                    raise Cloud115RequestError("115 Range 读取失败")
                content_length = response.headers.get("Content-Length")
                if remaining is not None and content_length:
                    try:
                        if int(content_length) > remaining:
                            raise Cloud115RequestError("115 Range 读取超过预算")
                    except ValueError:
                        pass
                body_buffer = bytearray()
                for chunk in response.iter_bytes():
                    if remaining is not None and len(body_buffer) + len(chunk) > remaining:
                        raise Cloud115RequestError("115 Range 读取超过预算")
                    body_buffer.extend(chunk)
                body = bytes(body_buffer)
                if response.status_code == 206:
                    match = _CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", "").strip())
                    if match is None:
                        raise Cloud115RequestError("115 Range 响应无效")
                    actual_start, actual_end, actual_total = map(int, match.groups())
                    if (
                        actual_start != start
                        or actual_end < actual_start
                        or actual_end > end
                        or actual_total != self._file_size
                        or len(body) != actual_end - actual_start + 1
                    ):
                        raise Cloud115RequestError("115 Range 响应不一致")
                elif start != 0 or len(body) != self._file_size:
                    raise Cloud115RequestError("115 未正确支持 Range")
        except httpx.RequestError as exc:
            raise Cloud115RequestError("115 Range 网络读取失败") from exc
        if not body:
            raise Cloud115RequestError("115 Range 返回空数据")
        self._buffer = body
        self._buffer_start = start
        self.fetched_bytes += len(body)
