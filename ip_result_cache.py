"""
同IP请求同链接结果缓存模块

本模块实现基于客户端IP和目标URL的结果缓存，用于快速返回之前的处理结果：
1. 302重定向结果：直接返回之前的目标重定向地址，跳过重定向链跟踪
2. 流式传输结果（200/206）：直接使用最终URL发起请求，跳过重定向链跟踪，
   同时保留客户端的Range头部以支持断点续传和进度变化

核心设计：
- 缓存键：hash(client_ip + target_url)
- 缓存值：包含状态码、最终URL、响应头部等
- 过期策略：TTL + 最大条目数限制（LRU淘汰）
- 兼容流式传输：缓存命中时仍会向上游发起新请求获取数据（仅跳过重定向链）
"""

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

logger = logging.getLogger("proxy.ip_cache")


@dataclass
class IpCacheEntry:
    result_type: str = ""
    status_code: int = 0
    redirect_url: str = ""
    final_url: str = ""
    response_headers: Dict[str, str] = field(default_factory=dict)
    created_at: float = 0.0


class IpResultCache:
    def __init__(self, enabled: bool = True, ttl_seconds: int = 300, max_entries: int = 5000):
        self.enabled = enabled
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._cache: OrderedDict[str, IpCacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    def _make_key(self, client_ip: str, target_url: str) -> str:
        raw = f"{client_ip}|{target_url}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _is_expired(self, entry: IpCacheEntry) -> bool:
        if self.ttl_seconds <= 0:
            return False
        return time.time() - entry.created_at > self.ttl_seconds

    async def get(self, client_ip: str, target_url: str) -> Optional[IpCacheEntry]:
        if not self.enabled:
            return None
        key = self._make_key(client_ip, target_url)
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            if self._is_expired(entry):
                del self._cache[key]
                self._misses += 1
                return None
            self._cache.move_to_end(key)
            self._hits += 1
            return entry

    async def put_redirect(
        self,
        client_ip: str,
        target_url: str,
        status_code: int,
        redirect_url: str,
        response_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        if not self.enabled:
            return
        entry = IpCacheEntry(
            result_type="redirect",
            status_code=status_code,
            redirect_url=redirect_url,
            response_headers=response_headers or {},
            created_at=time.time(),
        )
        await self._put(client_ip, target_url, entry)

    async def put_streaming(
        self,
        client_ip: str,
        target_url: str,
        status_code: int,
        final_url: str,
        response_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        if not self.enabled:
            return
        entry = IpCacheEntry(
            result_type="streaming",
            status_code=status_code,
            final_url=final_url,
            response_headers=response_headers or {},
            created_at=time.time(),
        )
        await self._put(client_ip, target_url, entry)

    async def _put(self, client_ip: str, target_url: str, entry: IpCacheEntry) -> None:
        key = self._make_key(client_ip, target_url)
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self.max_entries:
                evicted_key, _ = self._cache.popitem(last=False)
            self._cache[key] = entry

    async def invalidate(self, client_ip: str, target_url: str) -> bool:
        key = self._make_key(client_ip, target_url)
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def clear(self) -> int:
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    async def cleanup_expired(self) -> int:
        async with self._lock:
            expired_keys = [k for k, v in self._cache.items() if self._is_expired(v)]
            for k in expired_keys:
                del self._cache[k]
            return len(expired_keys)

    def get_stats(self) -> dict:
        total = self._hits + self._misses
        hit_rate = f"{(self._hits / total * 100):.1f}%" if total > 0 else "0.0%"
        return {
            "enabled": self.enabled,
            "ttl_seconds": self.ttl_seconds,
            "max_entries": self.max_entries,
            "current_entries": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
        }
