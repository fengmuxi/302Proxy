"""Request deduplication module.

Intercepts identical requests (same IP + URL + Method + Range) within a short
time window to prevent burst duplicate requests, while allowing legitimate
video/audio range requests with different byte ranges.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DedupConfig:
    enabled: bool = False
    window_seconds: float = 2.0
    max_cache_entries: int = 10000


@dataclass
class _CacheEntry:
    timestamp: float
    status: int
    headers: dict
    body: bytes


class RequestDedup:
    def __init__(self, config: Optional[DedupConfig] = None):
        self.config = config or DedupConfig()
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._total_hits = 0

    @property
    def total_hits(self) -> int:
        return self._total_hits

    def _make_key(self, client_ip: str, method: str, url: str, range_header: str) -> str:
        return f"{client_ip}|{method}|{url}|{range_header}"

    def _evict_expired(self, now: float) -> None:
        cutoff = now - self.config.window_seconds
        expired = [k for k, v in self._cache.items() if v.timestamp < cutoff]
        for k in expired:
            del self._cache[k]

    def _evict_oldest(self) -> None:
        if len(self._cache) <= self.config.max_cache_entries:
            return
        oldest_key = min(self._cache, key=lambda k: self._cache[k].timestamp)
        del self._cache[oldest_key]

    async def check(
        self,
        client_ip: str,
        method: str,
        url: str,
        range_header: str = "",
    ) -> Optional[_CacheEntry]:
        if not self.config.enabled:
            return None
        if method not in ("GET", "HEAD"):
            return None

        key = self._make_key(client_ip, method, url, range_header)
        now = time.time()

        async with self._lock:
            self._evict_expired(now)
            entry = self._cache.get(key)
            if entry is not None and (now - entry.timestamp) < self.config.window_seconds:
                self._total_hits += 1
                return entry
        return None

    async def store(
        self,
        client_ip: str,
        method: str,
        url: str,
        range_header: str,
        status: int,
        headers: dict,
        body: bytes,
    ) -> None:
        if not self.config.enabled:
            return
        if method not in ("GET", "HEAD"):
            return

        key = self._make_key(client_ip, method, url, range_header)
        now = time.time()

        async with self._lock:
            self._evict_expired(now)
            self._evict_oldest()
            self._cache[key] = _CacheEntry(
                timestamp=now,
                status=status,
                headers=headers,
                body=body,
            )

    def clear(self) -> None:
        self._cache.clear()
