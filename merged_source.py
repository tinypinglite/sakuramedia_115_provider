"""Remote random-access source for 115 virtual-MP4 merged playback."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable

from .cloud115 import Cloud115DirectUrl
from .exceptions import Cloud115RequestError
from .range_reader import Cloud115RangeReader


class Cloud115MergedSource:
    """Expose one 115 media file through the generic MP4 merge source protocol."""

    def __init__(
        self,
        *,
        size: int,
        identity: tuple[object, ...],
        url_factory: Callable[[], Cloud115DirectUrl],
    ) -> None:
        if size <= 0:
            raise ValueError("115 merged source size must be positive")
        self.size = size
        self.identity = identity
        self._url_factory = url_factory
        self._direct: Cloud115DirectUrl | None = None
        self._cached_until = 0.0

    def _current(self) -> Cloud115DirectUrl:
        now = time.time()
        if self._direct is None or self._cached_until <= now:
            direct = self._url_factory()
            self._direct = direct
            self._cached_until = (
                direct.expires_at - 30 if direct.expires_at else now + 300
            )
        return self._direct

    def _reader(self, *, length: int, chunk_size: int) -> Cloud115RangeReader:
        direct = self._current()
        return Cloud115RangeReader(
            direct.url,
            user_agent=direct.user_agent,
            file_size_bytes=self.size,
            chunk_size=chunk_size,
            max_fetched_bytes=length,
        )

    def read_at(self, offset: int, length: int) -> bytes:
        if length <= 0:
            return b""
        with self._reader(
            length=length,
            chunk_size=max(64 * 1024, length),
        ) as reader:
            reader.seek(offset)
            data = reader.read(length)
        if len(data) != length:
            raise Cloud115RequestError("115 合并播放读取不完整")
        return data

    def iter_range(self, offset: int, length: int) -> Iterable[bytes]:
        if length <= 0:
            return
        with self._reader(
            length=length,
            chunk_size=min(1024 * 1024, max(64 * 1024, length)),
        ) as reader:
            reader.seek(offset)
            remaining = length
            while remaining > 0:
                chunk = reader.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise Cloud115RequestError("115 合并播放读取中断")
                remaining -= len(chunk)
                yield chunk
