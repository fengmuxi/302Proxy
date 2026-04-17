"""
流式缓存管理器模块

本模块实现了专门针对流式媒体传输优化的缓存系统，支持边传输边缓存、
HTTP Range 请求、断点续传等高级功能。

主要特性：
1. 边传输边缓存 - 在流式传输过程中实时缓存数据，无需等待完整下载
2. Range 请求支持 - 完整支持 HTTP Range 请求，可从缓存中读取任意范围的数据
3. 断点续传 - 支持下载中断后从断点继续下载
4. LRU 淘汰算法 - 基于最近最少使用原则自动淘汰旧缓存
5. 缓存校验 - 支持 ETag 和 Last-Modified 校验缓存有效性
6. 预加载机制 - 支持后台预加载指定 URL 的内容

设计思路：
- 使用异步 I/O（aiohttp/aiofiles）实现高性能并发处理
- 缓存数据存储在文件系统，元数据存储在 JSON 文件中
- 使用 OrderedDict 实现 LRU 淘汰算法
- 集成 CacheLogger 实现详细的操作日志追踪
"""

import asyncio
import os
import time
import hashlib
import json
import re
import uuid
import aiohttp
import aiofiles
from typing import Optional, Dict, Any, List, Tuple, AsyncGenerator
from dataclasses import dataclass, field
from collections import OrderedDict
import logging
from pathlib import Path
from cache_logger import CacheLogger

# 获取代理服务器的日志记录器
logger = logging.getLogger('proxy')


@dataclass
class CacheEntry:
    """
    缓存条目数据类
    
    存储单个缓存项的所有元数据信息，包括缓存内容的位置、大小、
    有效期、HTTP 头信息等。
    
    属性说明：
        url: 原始请求的完整 URL
        file_path: 缓存文件的完整路径
        size: 缓存数据大小（字节）
        content_type: 内容类型（MIME 类型）
        created_at: 缓存创建时间戳（秒）
        last_accessed: 最后访问时间戳（秒）
        access_count: 访问次数，用于 LRU 淘汰算法
        etag: HTTP ETag 响应头，用于缓存校验
        last_modified: HTTP Last-Modified 响应头，用于缓存校验
        expires_at: 缓存过期时间戳（秒），None 表示永不过期
        is_complete: 缓存是否完整（True 表示已完全下载）
        is_streaming: 是否通过流式传输缓存
        temp_file: 临时文件路径（下载未完成时使用）
        request_id: 创建此缓存的请求 ID，用于日志追踪
        range_header: Range 请求头（如果有）
        cache_status: 缓存状态（UNKNOWN/HIT/MISS/STORED/EXPIRED）
    """
    url: str
    file_path: str
    size: int
    content_type: str
    created_at: float
    last_accessed: float
    access_count: int
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    expires_at: Optional[float] = None
    is_complete: bool = True
    is_streaming: bool = False
    temp_file: Optional[str] = None
    cache_key: Optional[str] = None
    parent_key: Optional[str] = None
    range_start: int = 0
    range_end: Optional[int] = None
    source_size: Optional[int] = None
    request_id: Optional[str] = None
    range_header: Optional[str] = None
    cache_status: str = 'UNKNOWN'


@dataclass
class CacheStats:
    """
    缓存统计数据类
    
    记录缓存系统的运行统计信息，用于监控和性能分析。
    
    属性说明：
        total_requests: 总请求数
        cache_hits: 缓存命中次数
        cache_misses: 缓存未命中次数
        partial_hits: 部分命中次数（Range 请求部分命中）
        range_requests: Range 请求数量
        bytes_served_from_cache: 从缓存提供的字节数
        bytes_downloaded: 从源站下载的字节数
        evicted_entries: 被淘汰的缓存条目数
        preload_requests: 预加载请求数
        preload_hits: 预加载命中数
        streaming_cache_writes: 流式缓存写入次数
        validation_requests: 缓存校验请求数
        eviction_events: 淘汰事件数
    """
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    partial_hits: int = 0
    range_requests: int = 0
    bytes_served_from_cache: int = 0
    bytes_downloaded: int = 0
    evicted_entries: int = 0
    preload_requests: int = 0
    preload_hits: int = 0
    streaming_cache_writes: int = 0
    validation_requests: int = 0
    eviction_events: int = 0
    
    @property
    def hit_rate(self) -> float:
        """
        计算缓存命中率
        
        返回：
            缓存命中率百分比（0-100）
        
        实现思路：
        如果总请求数为 0，返回 0.0 避免除零错误
        """
        if self.total_requests == 0:
            return 0.0
        return self.cache_hits / self.total_requests * 100


class LRUCache:
    """
    LRU（最近最少使用）缓存实现
    
    使用 OrderedDict 实现 LRU 淘汰算法，当缓存空间不足时自动淘汰
    最久未访问的条目。
    
    属性说明：
        max_size: 最大缓存大小（字节）
        max_entries: 最大缓存条目数
        current_size: 当前缓存大小（字节）
        cache: 有序字典存储缓存条目，按访问时间排序
        _lock: 异步锁，保证线程安全
        stats: 缓存统计对象引用
    
    实现思路：
    - OrderedDict 的 popitem(last=False) 方法移除最早插入的条目
    - 每次访问时将条目移到字典末尾，实现 LRU 效果
    - 使用异步锁保证并发访问安全
    """
    
    def __init__(self, max_size: int, max_entries: int, stats: CacheStats):
        """
        初始化 LRU 缓存
        
        参数：
            max_size: 最大缓存大小（字节）
            max_entries: 最大缓存条目数
            stats: 缓存统计对象，用于记录淘汰事件
        """
        self.max_size = max_size
        self.max_entries = max_entries
        self.current_size = 0
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()
        self.stats = stats
    
    async def get(self, key: str) -> Optional[CacheEntry]:
        """
        从缓存中获取条目
        
        如果条目存在，将其移到字典末尾（标记为最近访问），
        并更新访问统计信息。
        
        参数：
            key: 缓存键
        
        返回：
            缓存条目，如果不存在返回 None
        
        实现思路：
        1. 使用异步锁保证线程安全
        2. 如果键存在，先 pop 再重新插入，实现移到末尾
        3. 更新条目的访问时间和访问次数
        """
        async with self._lock:
            if key in self.cache:
                # 将条目移到字典末尾（标记为最近访问）
                entry = self.cache.pop(key)
                entry.last_accessed = time.time()
                entry.access_count += 1
                self.cache[key] = entry
                
                # 记录 LRU 访问日志
                logger.debug(
                    f"[LRU访问] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"操作: 访问 | 键: {key[:16]}... | "
                    f"访问次数: {entry.access_count}"
                )
                return entry
            return None
    
    async def put(self, key: str, entry: CacheEntry) -> Tuple[bool, List[Tuple[str, CacheEntry]]]:
        """
        将条目存入缓存
        
        如果缓存空间不足，自动淘汰最久未访问的条目直到有足够空间。
        
        参数：
            key: 缓存键
            entry: 缓存条目
        
        返回：
            被淘汰的文件路径列表，需要调用者删除这些文件
        
        实现思路：
        1. 如果键已存在，先移除旧条目
        2. 循环淘汰最久未访问的条目，直到空间足够
        3. 将新条目插入字典末尾
        4. 记录淘汰日志
        """
        evicted_entries: List[Tuple[str, CacheEntry]] = []
        async with self._lock:
            if self.max_entries <= 0 or entry.size > self.max_size:
                logger.warning(
                    f"[缓存淘汰] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"操作: 拒绝写入 | 键: {key[:16]}... | "
                    f"大小: {entry.size / 1024 / 1024:.2f} MB | "
                    f"原因: 超过缓存限制"
                )
                return False, evicted_entries

            # 如果键已存在，移除旧条目
            if key in self.cache:
                old_entry = self.cache.pop(key)
                self.current_size -= old_entry.size
            
            # 淘汰最久未访问的条目直到空间足够
            while (self.current_size + entry.size > self.max_size or 
                   len(self.cache) >= self.max_entries):
                if not self.cache:
                    break
                
                # 移除最早插入的条目（最久未访问）
                oldest_key, oldest_entry = self.cache.popitem(last=False)
                self.current_size -= oldest_entry.size
                evicted_entries.append((oldest_key, oldest_entry))
                
                # 更新淘汰统计
                self.stats.evicted_entries += 1
                self.stats.eviction_events += 1
                
                # 记录淘汰日志
                logger.info(
                    f"[缓存淘汰] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"操作: 淘汰 | 键: {oldest_key[:16]}... | "
                    f"大小: {oldest_entry.size / 1024 / 1024:.2f} MB | "
                    f"访问次数: {oldest_entry.access_count} | "
                    f"原因: 缓存空间不足"
                )
            
            # 插入新条目
            self.cache[key] = entry
            self.current_size += entry.size
        
        return True, evicted_entries

    async def trim_to_limits(self) -> List[Tuple[str, CacheEntry]]:
        """
        主动收缩缓存，确保满足 max_size 和 max_entries 约束。
        """
        evicted_entries: List[Tuple[str, CacheEntry]] = []
        async with self._lock:
            while self.cache and (
                self.current_size > self.max_size or
                len(self.cache) > self.max_entries
            ):
                oldest_key, oldest_entry = self.cache.popitem(last=False)
                self.current_size -= oldest_entry.size
                evicted_entries.append((oldest_key, oldest_entry))
                self.stats.evicted_entries += 1
                self.stats.eviction_events += 1
                logger.info(
                    f"[缓存淘汰] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"操作: 收缩 | 键: {oldest_key[:16]}... | "
                    f"大小: {oldest_entry.size / 1024 / 1024:.2f} MB | "
                    f"原因: 超过缓存限制"
                )
        return evicted_entries
    
    async def remove(self, key: str) -> Optional[CacheEntry]:
        """
        从缓存中移除条目
        
        参数：
            key: 缓存键
        
        返回：
            被移除的缓存条目，如果不存在返回 None
        """
        async with self._lock:
            if key in self.cache:
                entry = self.cache.pop(key)
                self.current_size -= entry.size
                return entry
            return None
    
    async def clear(self) -> List[str]:
        """
        清空所有缓存
        
        返回：
            所有缓存文件的路径列表，需要调用者删除这些文件
        """
        files = []
        async with self._lock:
            for entry in self.cache.values():
                files.append(entry.file_path)
                if entry.temp_file:
                    files.append(entry.temp_file)
            self.cache.clear()
            self.current_size = 0
        return files
    
    def get_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        返回：
            包含缓存统计信息的字典：
            - entries: 当前条目数
            - max_entries: 最大条目数
            - current_size: 当前大小
            - max_size: 最大大小
            - usage_percent: 使用率百分比
        """
        return {
            'entries': len(self.cache),
            'max_entries': self.max_entries,
            'current_size': self.current_size,
            'max_size': self.max_size,
            'usage_percent': round(self.current_size / self.max_size * 100, 2) if self.max_size > 0 else 0
        }


class StreamingCacheManager:
    """
    流式缓存管理器
    
    管理流式媒体内容的缓存，支持边传输边缓存、Range 请求、
    断点续传等高级功能。
    
    核心功能：
    1. 缓存生命周期管理 - 创建、读取、更新、删除缓存
    2. Range 请求支持 - 支持任意范围的缓存读取
    3. 流式缓存 - 边传输边缓存，无需等待完整下载
    4. 断点续传 - 支持下载中断后继续
    5. 缓存校验 - 使用 ETag/Last-Modified 校验有效性
    6. 预加载 - 后台预加载指定内容
    
    目录结构：
    cache/
    ├── temp/          # 临时文件目录（下载未完成）
    ├── meta/          # 元数据目录（JSON 文件）
    └── *.cache        # 缓存数据文件
    
    使用示例：
        manager = StreamingCacheManager(
            cache_dir="./cache",
            max_size=5 * 1024 * 1024 * 1024,  # 5GB
            max_entries=1000
        )
        
        # 获取缓存
        result = await manager.get_stream(url, "bytes=0-1023")
        if result:
            entry, stream, start, end, total = result
            async for chunk in stream:
                # 处理数据块
                pass
    """
    
    def __init__(
        self,
        cache_dir: str,
        max_size: int,
        max_entries: int,
        default_ttl: int = 86400,
        chunk_size: int = 64 * 1024,
        max_entry_size: int = 0,
        max_request_cache_size: int = 0,
        enable_preload: bool = True,
        preload_concurrency: int = 3,
        enable_validation: bool = True,
        validation_interval: int = 3600
    ):
        """
        初始化流式缓存管理器
        
        参数：
            cache_dir: 缓存目录路径
            max_size: 最大缓存大小（字节）
            max_entries: 最大缓存条目数
            default_ttl: 默认缓存有效期（秒），默认 86400（24小时）
            chunk_size: 数据块大小（字节），默认 64KB
            max_entry_size: 单个缓存文件大小上限，0 表示不限制
            max_request_cache_size: 单次请求缓存大小上限，0 表示不限制
            enable_preload: 是否启用预加载
            preload_concurrency: 预加载并发数
            enable_validation: 是否启用缓存校验
            validation_interval: 后台维护间隔（秒）
        
        初始化流程：
        1. 创建缓存目录结构
        2. 初始化 LRU 缓存
        3. 加载已有缓存索引
        """
        self.cache_dir = Path(cache_dir)
        self.temp_dir = self.cache_dir / 'temp'
        self.meta_dir = self.cache_dir / 'meta'
        self.stats = CacheStats()
        self.cache = LRUCache(max_size, max_entries, self.stats)
        self.default_ttl = default_ttl
        self.chunk_size = chunk_size
        self.max_entry_size = max(0, max_entry_size)
        self.max_request_cache_size = max(0, max_request_cache_size)
        self.enable_preload = enable_preload
        self.preload_concurrency = max(1, preload_concurrency)
        self.enable_validation = enable_validation
        self.validation_interval = max(1, validation_interval)
        self._session: Optional[aiohttp.ClientSession] = None
        self._download_tasks: Dict[str, asyncio.Task] = {}
        self._cache_writers: Dict[str, asyncio.Task] = {}
        self._preload_queue: asyncio.Queue = None
        self._preload_workers: List[asyncio.Task] = []
        self._preload_semaphore = asyncio.Semaphore(self.preload_concurrency)
        self._maintenance_task: Optional[asyncio.Task] = None
        self._running = False
        self.cache_logger = CacheLogger()
        
        # 初始化目录和加载缓存索引
        self._setup_directories()
        self._load_cache_index()
    
    def _setup_directories(self):
        """
        创建缓存目录结构
        
        创建三个目录：
        - cache_dir: 主缓存目录
        - temp_dir: 临时文件目录
        - meta_dir: 元数据目录
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_key(self, url: str) -> str:
        """
        根据URL生成缓存键
        
        使用 SHA256 哈希算法生成唯一键，避免 URL 中的特殊字符影响文件名。
        
        参数：
            url: 完整的 URL
        
        返回：
            64 位十六进制哈希字符串
        """
        return hashlib.sha256(url.encode()).hexdigest()
    
    def _get_cache_path(self, key: str) -> Path:
        """
        获取缓存文件路径
        
        参数：
            key: 缓存键
        
        返回：
            缓存文件的完整路径
        """
        return self.cache_dir / f"{key}.cache"
    
    def _get_meta_path(self, key: str) -> Path:
        """
        获取元数据文件路径
        
        参数：
            key: 缓存键
        
        返回：
            元数据 JSON 文件的完整路径
        """
        return self.meta_dir / f"{key}.json"
    
    def _get_temp_path(self, key: str) -> Path:
        """
        获取临时文件路径
        
        临时文件用于下载未完成时的数据存储。
        
        参数：
            key: 缓存键
        
        返回：
            临时文件的完整路径
        """
        return self.temp_dir / f"{key}.tmp"

    def _get_segment_key(self, url: str, start_byte: int, end_byte: int) -> str:
        """
        为部分 Range 缓存生成唯一键。
        """
        return f"{self._get_cache_key(url)}_{start_byte}_{end_byte}"

    def _parse_content_range(self, content_range: Optional[str]) -> Optional[Tuple[int, int, int]]:
        """
        解析标准的 Content-Range 头。
        """
        if not content_range:
            return None

        match = re.match(r'^bytes\s+(\d+)-(\d+)/(\d+)$', content_range.strip(), re.IGNORECASE)
        if not match:
            return None

        start_byte = int(match.group(1))
        end_byte = int(match.group(2))
        total_size = int(match.group(3))
        if start_byte > end_byte or total_size <= 0:
            return None

        return start_byte, end_byte, total_size

    def _parse_range_header(
        self,
        range_header: Optional[str],
        total_size: Optional[int] = None
    ) -> Optional[Tuple[int, Optional[int]]]:
        """
        解析单个 bytes Range 请求。
        """
        if not range_header:
            if total_size is None:
                return None
            return 0, total_size - 1

        range_text = range_header.strip()
        if not range_text.lower().startswith('bytes='):
            return None

        requested_range = range_text[6:]
        if ',' in requested_range:
            return None

        start_text, _, end_text = requested_range.partition('-')
        if not start_text and not end_text:
            return None

        if not start_text:
            if total_size is None:
                return None
            suffix_length = int(end_text)
            if suffix_length <= 0:
                return None
            start_byte = max(total_size - suffix_length, 0)
            end_byte = total_size - 1
        else:
            start_byte = int(start_text)
            if start_byte < 0:
                return None
            end_byte = int(end_text) if end_text else None

        if total_size is not None:
            if start_byte >= total_size:
                return None
            if end_byte is None or end_byte >= total_size:
                end_byte = total_size - 1

        if end_byte is not None and end_byte < start_byte:
            return None

        return start_byte, end_byte

    def _normalize_entry(self, key: str, entry: CacheEntry) -> CacheEntry:
        """
        兼容旧元数据，并补齐区间缓存的必要字段。
        """
        entry.cache_key = entry.cache_key or key
        entry.parent_key = entry.parent_key or self._get_cache_key(entry.url)

        if entry.source_size is None:
            if entry.is_complete:
                entry.source_size = entry.size
            elif entry.range_end is not None:
                entry.source_size = max(entry.range_end + 1, entry.size)

        if entry.range_end is None:
            if entry.is_complete:
                total_size = entry.source_size or entry.size
                entry.range_start = 0
                entry.range_end = total_size - 1 if total_size > 0 else -1
            elif entry.size > 0:
                entry.range_end = entry.range_start + entry.size - 1

        if entry.is_complete and entry.source_size is None:
            entry.source_size = entry.size

        # 兼容历史条目：缺失 expires_at 时补齐默认过期时间，避免缓存永久占用磁盘。
        if entry.expires_at is None:
            base_time = entry.created_at if isinstance(entry.created_at, (int, float)) and entry.created_at > 0 else time.time()
            entry.expires_at = base_time + self.default_ttl

        return entry

    def _get_entry_total_size(self, entry: CacheEntry) -> Optional[int]:
        """
        获取源文件总大小。
        """
        entry = self._normalize_entry(entry.cache_key or self._get_cache_key(entry.url), entry)
        return entry.source_size or (entry.range_end + 1 if entry.range_end is not None else None)

    def _entry_covers_range(self, entry: CacheEntry, start_byte: int, end_byte: int) -> bool:
        """
        判断缓存条目是否完整覆盖目标 Range。
        """
        entry = self._normalize_entry(entry.cache_key or self._get_cache_key(entry.url), entry)
        if entry.range_end is None:
            return False
        return entry.range_start <= start_byte and entry.range_end >= end_byte

    def _entry_overlaps_range(self, entry: CacheEntry, start_byte: int, end_byte: int) -> bool:
        """
        判断缓存条目是否与目标 Range 有交集。
        """
        entry = self._normalize_entry(entry.cache_key or self._get_cache_key(entry.url), entry)
        if entry.range_end is None:
            return False
        return not (entry.range_end < start_byte or entry.range_start > end_byte)

    async def _resolve_cached_request(
        self,
        url: str,
        range_header: Optional[str]
    ) -> Optional[Tuple[str, CacheEntry, int, int, int, int]]:
        """
        为当前请求解析可直接命中的缓存条目。
        返回 (cache_key, entry, start_byte, end_byte, total_size, local_offset)。
        """
        base_key = self._get_cache_key(url)
        now = time.time()
        expired_entries: Dict[str, CacheEntry] = {}

        async def cleanup_expired_candidates():
            if not expired_entries:
                return
            for expired_key, expired_entry in expired_entries.items():
                await self.cache.remove(expired_key)
                await self._delete_cache_files(expired_key, expired_entry)

        base_entry = await self.cache.get(base_key)

        if base_entry:
            base_entry = self._normalize_entry(base_key, base_entry)
            if base_entry.expires_at and now > base_entry.expires_at:
                expired_entries[base_key] = base_entry
            else:
                total_size = self._get_entry_total_size(base_entry)
                if total_size:
                    if range_header:
                        requested_range = self._parse_range_header(range_header, total_size)
                        if requested_range:
                            start_byte, end_byte = requested_range
                            if end_byte is not None and self._entry_covers_range(base_entry, start_byte, end_byte):
                                await cleanup_expired_candidates()
                                return base_key, base_entry, start_byte, end_byte, total_size, start_byte - base_entry.range_start
                    elif base_entry.is_complete:
                        await cleanup_expired_candidates()
                        return base_key, base_entry, 0, total_size - 1, total_size, 0

        if not range_header:
            await cleanup_expired_candidates()
            return None

        parent_key = base_key
        best_candidate = None
        partial_overlap = False

        for key, candidate in list(self.cache.cache.items()):
            if key == base_key:
                continue

            candidate = self._normalize_entry(key, candidate)
            if candidate.parent_key != parent_key:
                continue

            if candidate.expires_at and now > candidate.expires_at:
                expired_entries[key] = candidate
                continue

            total_size = self._get_entry_total_size(candidate)
            if not total_size:
                continue

            requested_range = self._parse_range_header(range_header, total_size)
            if not requested_range:
                continue

            start_byte, end_byte = requested_range
            if end_byte is None:
                continue

            if self._entry_covers_range(candidate, start_byte, end_byte):
                cached_span = candidate.range_end - candidate.range_start
                if best_candidate is None or cached_span < best_candidate[0]:
                    best_candidate = (cached_span, key, start_byte, end_byte, total_size)
            elif self._entry_overlaps_range(candidate, start_byte, end_byte):
                partial_overlap = True

        if not best_candidate:
            if partial_overlap:
                logger.info(
                    f"[缓存查询] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"操作: 查询 | 键: {base_key[:16]}... | 结果: 未命中 | 原因: 仅部分区间已缓存"
                )
            await cleanup_expired_candidates()
            return None

        _, key, start_byte, end_byte, total_size = best_candidate
        entry = await self.cache.get(key)
        if not entry:
            await cleanup_expired_candidates()
            return None

        entry = self._normalize_entry(key, entry)
        if entry.expires_at and now > entry.expires_at:
            expired_entries[key] = entry
            await cleanup_expired_candidates()
            return None

        await cleanup_expired_candidates()
        return key, entry, start_byte, end_byte, total_size, start_byte - entry.range_start

    def _is_range_cached(self, url: str, start_byte: int, end_byte: int) -> bool:
        """
        判断目标 Range 是否已经被完整缓存。
        """
        parent_key = self._get_cache_key(url)
        now = time.time()
        for key, entry in self.cache.cache.items():
            entry = self._normalize_entry(key, entry)
            if entry.parent_key != parent_key:
                continue
            if entry.expires_at and now > entry.expires_at:
                continue
            if self._entry_covers_range(entry, start_byte, end_byte):
                return True
        return False

    def _get_expected_response_size(self, headers: Dict[str, str]) -> Optional[int]:
        """
        获取当前响应预计会写入缓存的字节数。
        """
        content_length = headers.get('Content-Length')
        if content_length:
            try:
                parsed = int(content_length)
                if parsed >= 0:
                    return parsed
            except ValueError:
                pass

        parsed_content_range = self._parse_content_range(headers.get('Content-Range'))
        if parsed_content_range:
            start_byte, end_byte, _ = parsed_content_range
            return end_byte - start_byte + 1

        return None

    def _check_cache_size_limits(self, size_bytes: int) -> Optional[str]:
        """
        检查缓存条目大小是否超过配置限制。
        """
        if size_bytes < 0:
            return '缓存大小无效'
        if self.max_entry_size and size_bytes > self.max_entry_size:
            return f'超过单文件缓存上限 {self.max_entry_size} 字节'
        if size_bytes > self.cache.max_size:
            return f'超过缓存总上限 {self.cache.max_size} 字节'
        return None

    def _check_request_cache_limit(self, size_bytes: int) -> Optional[str]:
        """
        检查单次请求可缓存字节数是否超过限制。
        """
        if size_bytes < 0:
            return '请求缓存大小无效'
        if self.max_request_cache_size and size_bytes > self.max_request_cache_size:
            return f'超过单次请求缓存上限 {self.max_request_cache_size} 字节'
        return None

    async def _remove_entries_by_parent_key(self, parent_key: str) -> int:
        """
        删除同一 URL 关联的所有缓存条目。
        """
        removed_count = 0
        for entry_key, entry in list(self.cache.cache.items()):
            entry = self._normalize_entry(entry_key, entry)
            if entry.parent_key != parent_key:
                continue
            await self.cache.remove(entry_key)
            await self._delete_cache_files(entry_key, entry)
            removed_count += 1
        return removed_count

    async def cleanup_orphan_temp_files(self) -> int:
        """
        启动时清理遗留的临时文件。
        """
        request_id = str(uuid.uuid4())[:8]
        removed_count = 0
        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 扫描临时文件 | 目录: {self.temp_dir}"
        )
        for temp_file in self.temp_dir.glob('*.tmp'):
            try:
                temp_file.unlink()
                removed_count += 1
            except Exception as e:
                logger.warning(
                    f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"请求ID: {request_id} | 操作: 删除临时文件 | 文件: {temp_file} | 结果: 失败 | 错误: {e}"
                )
        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 清理临时文件完成 | 文件数: {removed_count}"
        )
        return removed_count

    async def cleanup_orphan_cache_files(self) -> int:
        """
        清理孤儿缓存文件（无元数据且未被当前索引引用）。
        """
        request_id = str(uuid.uuid4())[:8]
        removed_count = 0
        removed_size = 0
        now = time.time()
        orphan_grace_seconds = max(30, min(self.validation_interval, 300))
        active_cache_files = set()

        for key, entry in list(self.cache.cache.items()):
            entry = self._normalize_entry(key, entry)
            if not entry.file_path:
                continue
            try:
                active_cache_files.add(str(Path(entry.file_path).resolve()))
            except Exception:
                active_cache_files.add(str(Path(entry.file_path)))

        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 扫描孤儿缓存文件 | 目录: {self.cache_dir}"
        )

        for cache_file in self.cache_dir.glob('*.cache'):
            cache_path = str(cache_file.resolve())
            if cache_path in active_cache_files:
                continue

            meta_path = self._get_meta_path(cache_file.stem)
            if meta_path.exists():
                continue

            try:
                stat = cache_file.stat()
                # 避免误删刚写入但元数据还未来得及落盘的缓存文件。
                if now - stat.st_mtime < orphan_grace_seconds:
                    continue

                cache_file.unlink()
                removed_count += 1
                removed_size += stat.st_size
            except Exception as e:
                logger.warning(
                    f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"请求ID: {request_id} | 操作: 删除孤儿缓存文件 | 文件: {cache_file} | 结果: 失败 | 错误: {e}"
                )

        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 清理孤儿缓存完成 | 文件数: {removed_count} | "
            f"释放空间: {removed_size / 1024 / 1024:.2f} MB"
        )
        return removed_count

    async def cleanup_expired_entries(self) -> int:
        """
        清理已过期的缓存条目。
        """
        request_id = str(uuid.uuid4())[:8]
        now = time.time()
        expired_entries = []
        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 扫描过期缓存 | 当前条目数: {len(self.cache.cache)}"
        )
        for key, entry in list(self.cache.cache.items()):
            entry = self._normalize_entry(key, entry)
            if entry.expires_at and now > entry.expires_at:
                expired_entries.append((key, entry))

        for key, entry in expired_entries:
            logger.info(
                f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"请求ID: {request_id} | 操作: 删除过期缓存 | 键: {key[:16]}... | 文件: {entry.file_path}"
            )
            await self.cache.remove(key)
            await self._delete_cache_files(key, entry)

        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 清理过期缓存完成 | 条目数: {len(expired_entries)}"
        )
        return len(expired_entries)

    async def enforce_cache_limits(self) -> int:
        """
        主动收缩缓存，确保满足 max_size 和 max_entries。
        """
        request_id = str(uuid.uuid4())[:8]
        cache_info = self.cache.get_info()
        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 检查缓存限额 | "
            f"当前大小: {cache_info['current_size']} | 总上限: {cache_info['max_size']} | "
            f"当前条目数: {cache_info['entries']} | 条目上限: {cache_info['max_entries']}"
        )
        evicted_entries = await self.cache.trim_to_limits()
        await self._delete_evicted_entries(evicted_entries)
        logger.info(
            f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"请求ID: {request_id} | 操作: 缓存限额检查完成 | "
            f"淘汰条目数: {len(evicted_entries)} | 当前大小: {self.cache.current_size} | "
            f"当前条目数: {len(self.cache.cache)}"
        )
        return len(evicted_entries)

    async def validate_existing_entries(self) -> int:
        """
        按 parent_key 对现有缓存做有效性校验。
        """
        if not self.enable_validation:
            return 0

        grouped_entries: Dict[str, Tuple[str, CacheEntry]] = {}
        for key, entry in list(self.cache.cache.items()):
            entry = self._normalize_entry(key, entry)
            current = grouped_entries.get(entry.parent_key)
            if current is None or entry.is_complete:
                grouped_entries[entry.parent_key] = (key, entry)

        invalidated = 0
        for parent_key, (key, entry) in grouped_entries.items():
            is_valid = await self.validate_cache(key, entry)
            if not is_valid:
                invalidated += await self._remove_entries_by_parent_key(parent_key)
            else:
                new_expiry = time.time() + self.default_ttl
                for entry_key, candidate in list(self.cache.cache.items()):
                    candidate = self._normalize_entry(entry_key, candidate)
                    if candidate.parent_key != parent_key:
                        continue
                    candidate.expires_at = new_expiry
                    await self._save_meta(entry_key, candidate)

        if invalidated:
            logger.info(
                f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"操作: 清理失效缓存 | 条目数: {invalidated}"
            )
        return invalidated

    async def _maintenance_worker(self):
        """
        周期性执行过期清理、校验和缓存限额收缩。
        """
        while True:
            try:
                await asyncio.sleep(self.validation_interval)
                await self.cleanup_orphan_cache_files()
                await self.cleanup_expired_entries()
                await self.enforce_cache_limits()
                if self.enable_validation:
                    await self.validate_existing_entries()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[缓存维护] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 失败 | 错误: {e}")
    
    def _load_cache_index(self):
        """
        加载缓存索引（同步版本）
        
        从元数据目录加载所有缓存条目的元数据，恢复缓存索引。
        只在程序启动时调用一次。
        
        实现思路：
        1. 遍历 meta 目录下的所有 JSON 文件
        2. 解析 JSON 文件创建 CacheEntry 对象
        3. 验证缓存文件是否存在
        4. 同步恢复条目到 LRU 缓存（避免异步时序问题）
        
        注意：
        此方法必须同步执行，确保在服务器开始处理请求前
        所有缓存条目都已加载到内存中。如果使用异步任务，
        可能导致第一个请求到达时缓存索引还未加载完成。
        """
        try:
            loaded_count = 0
            total_size = 0
            
            for meta_file in self.meta_dir.glob('*.json'):
                try:
                    with open(meta_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    entry = CacheEntry(**data)
                    entry = self._normalize_entry(meta_file.stem, entry)
                    cache_file = self._get_cache_path(meta_file.stem)
                    
                    # 验证缓存文件是否存在
                    if cache_file.exists():
                        entry.file_path = str(cache_file)
                        
                        # 检查是否过期
                        if entry.expires_at and time.time() > entry.expires_at:
                            # 过期缓存，删除文件
                            try:
                                cache_file.unlink(missing_ok=True)
                                meta_file.unlink(missing_ok=True)
                                logger.info(f"[缓存加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 清理过期 | 文件: {meta_file.stem} | 结果: 成功")
                            except Exception as e:
                                logger.warning(f"[缓存加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 清理过期 | 文件: {meta_file.stem} | 结果: 失败 | 错误: {e}")
                            continue
                        
                        # 同步添加到 LRU 缓存（直接操作内部数据结构）
                        key = meta_file.stem
                        self.cache.cache[key] = entry
                        self.cache.current_size += entry.size
                        loaded_count += 1
                        total_size += entry.size
                    else:
                        try:
                            meta_file.unlink(missing_ok=True)
                            logger.info(f"[缓存加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 清理孤立元数据 | 文件: {meta_file.stem} | 结果: 成功")
                        except Exception as e:
                            logger.warning(f"[缓存加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 清理孤立元数据 | 文件: {meta_file.stem} | 结果: 失败 | 错误: {e}")
                        
                except Exception as e:
                    logger.warning(f"[缓存加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 加载元数据 | 文件: {meta_file.name} | 结果: 失败 | 错误: {e}")
            
            if loaded_count > 0:
                logger.info(f"[缓存索引] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 加载完成 | 条目数: {loaded_count} | 总大小: {total_size / 1024 / 1024:.2f} MB")
                
        except Exception as e:
            logger.error(f"[缓存索引] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 加载索引 | 结果: 失败 | 错误: {e}")
    
    async def _restore_entry(self, key: str, entry: CacheEntry):
        """
        恢复缓存条目
        
        将从元数据文件加载的条目恢复到 LRU 缓存中。
        如果条目已过期，则删除。
        
        参数：
            key: 缓存键
            entry: 缓存条目
        """
        try:
            # 检查是否过期
            if entry.expires_at and time.time() > entry.expires_at:
                await self._delete_cache_files(key, entry)
                logger.info(f"[缓存恢复] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 清理过期 | 键: {key[:16]}... | 结果: 成功")
                return
            
            # 恢复到 LRU 缓存
            stored, evicted = await self.cache.put(key, entry)
            await self._delete_evicted_entries(evicted)
            if not stored:
                await self._delete_cache_files(key, entry)
                logger.warning(f"[缓存恢复] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 恢复条目 | 键: {key[:16]}... | 结果: 跳过 | 原因: 超过缓存限制")
                return
            logger.info(f"[缓存恢复] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 恢复条目 | 键: {key[:16]}... | 大小: {entry.size / 1024 / 1024:.2f} MB | 结果: 成功")
        except Exception as e:
            logger.error(f"[缓存恢复] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 恢复 | 键: {key[:16]}... | 结果: 失败 | 错误: {e}")
    
    async def _delete_cache_files(self, key: str, entry: CacheEntry):
        """
        删除缓存相关文件
        
        删除缓存数据文件、临时文件和元数据文件。
        
        参数：
            key: 缓存键
            entry: 缓存条目
        """
        files = [entry.file_path]
        if entry.temp_file:
            files.append(entry.temp_file)
        await self._delete_files(files)
        
        # 删除元数据文件
        meta_path = self._get_meta_path(key)
        try:
            meta_path.unlink(missing_ok=True)
        except Exception:
            pass

    async def _delete_evicted_entries(self, evicted_entries: List[Tuple[str, CacheEntry]]):
        """Delete cache data and metadata for entries evicted by LRU maintenance."""
        for evicted_key, evicted_entry in evicted_entries:
            await self._delete_cache_files(evicted_key, evicted_entry)

    async def _delete_files(self, files: List[str]):
        """
        批量删除文件
        
        参数：
            files: 文件路径列表
        """
        for file_path in files:
            try:
                if file_path and os.path.exists(file_path):
                    os.unlink(file_path)
                    logger.debug(f"[文件删除] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 文件: {file_path} | 结果: 成功")
            except Exception as e:
                logger.warning(f"[文件删除] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 文件: {file_path} | 结果: 失败 | 错误: {e}")
    
    async def _save_meta(self, key: str, entry: CacheEntry):
        """
        保存缓存元数据
        
        将缓存条目的元数据保存到 JSON 文件。
        
        参数：
            key: 缓存键
            entry: 缓存条目
        """
        meta_path = self._get_meta_path(key)
        try:
            async with aiofiles.open(meta_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps({
                    'url': entry.url,
                    'file_path': entry.file_path,
                    'size': entry.size,
                    'content_type': entry.content_type,
                    'created_at': entry.created_at,
                    'last_accessed': entry.last_accessed,
                    'access_count': entry.access_count,
                    'etag': entry.etag,
                    'last_modified': entry.last_modified,
                    'expires_at': entry.expires_at,
                    'is_complete': entry.is_complete,
                    'is_streaming': entry.is_streaming,
                    'temp_file': entry.temp_file,
                    'cache_key': entry.cache_key,
                    'parent_key': entry.parent_key,
                    'range_start': entry.range_start,
                    'range_end': entry.range_end,
                    'source_size': entry.source_size,
                }, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.error(f"[元数据保存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 保存 | 键: {key[:16]}... | 结果: 失败 | 错误: {e}")

    async def start(self):
        """
        启动后台维护任务。
        """
        await self.cleanup_orphan_temp_files()
        await self.cleanup_orphan_cache_files()
        await self.cleanup_expired_entries()
        await self.enforce_cache_limits()

        if not self._maintenance_task or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(self._maintenance_worker())
    
    async def get_session(self) -> aiohttp.ClientSession:
        """
        获取 HTTP 会话
        
        使用单例模式管理 aiohttp 会话，避免重复创建。
        
        返回：
            aiohttp ClientSession 对象
        """
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=3600, connect=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session
    
    async def close(self):
        """
        关闭缓存管理器
        
        清理所有资源：
        1. 关闭 HTTP 会话
        2. 取消预加载任务
        3. 取消所有下载任务
        4. 取消所有缓存写入任务
        """
        self._running = False

        if self._maintenance_task:
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass

        for task in self._preload_workers:
            task.cancel()
        for task in self._preload_workers:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._preload_workers.clear()
        
        for task in self._download_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        for task in self._cache_writers.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get_range(
        self,
        url: str,
        range_header: Optional[str] = None
    ) -> Optional[Tuple[CacheEntry, bytes, int, int, int]]:
        """
        获取缓存的 Range 数据（一次性读取）
        
        从缓存中读取指定范围的数据，一次性返回完整数据。
        适用于小范围请求。
        
        参数：
            url: 请求的完整 URL
            range_header: HTTP Range 请求头，如 "bytes=0-1023"
        
        返回：
            元组 (entry, data, start_byte, end_byte, total_size) 或 None
            - entry: 缓存条目
            - data: 读取的数据
            - start_byte: 实际起始字节
            - end_byte: 实际结束字节
            - total_size: 源文件总大小
        
        实现流程：
        1. 开始请求追踪
        2. 查找缓存
        3. 检查缓存有效性
        4. 解析 Range 请求
        5. 读取指定范围数据
        6. 更新统计信息
        """
        # 开始请求追踪
        request_id = self.cache_logger.start_trace(url, range_header)
        trace = self.cache_logger.get_trace(request_id)
        
        # 更新统计
        self.stats.total_requests += 1
        if range_header:
            self.stats.range_requests += 1
        
        # 生成缓存键
        key = self._get_cache_key(url)
        if trace:
            trace.cache_key = key
        # 记录请求开始日志
        step_start = time.time()
        logger.info(f"[请求开始] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | URL: {url[:80]}... | Range: {range_header or '无'}")
        
        # 查找缓存
        resolved_cache = await self._resolve_cached_request(url, range_header)
        step_duration = (time.time() - step_start) * 1000
        
        if trace:
            trace.add_step('缓存查找', step_duration, {'found': resolved_cache is not None})
        
        if not resolved_cache:
            self.stats.cache_misses += 1
            logger.info(f"[缓存查询] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 查询 | 键: {key[:16]}... | 结果: 未命中 | 原因: 无可用缓存区间")
            self.cache_logger.end_trace(request_id)
            return None

        entry_key, entry, start_byte, end_byte, total_size, local_offset = resolved_cache
        entry = self._normalize_entry(entry_key, entry)
        
        # 检查缓存是否过期
        if entry.expires_at and time.time() > entry.expires_at:
            await self.cache.remove(entry_key)
            await self._delete_cache_files(entry_key, entry)
            self.stats.cache_misses += 1
            logger.info(f"[缓存查询] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 查询 | 键: {entry_key[:16]}... | 结果: 未命中 | 原因: 缓存过期")
            self.cache_logger.end_trace(request_id)
            return None
        
        try:
            # 获取缓存文件大小
            step_start = time.time()
            cached_size = os.path.getsize(entry.file_path)
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('获取文件大小', step_duration, {'cached_size': cached_size})

            bytes_to_read = end_byte - start_byte + 1
            if local_offset < 0 or local_offset + bytes_to_read > cached_size:
                logger.warning(
                    f"[Range请求] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                    f"操作: 读取 | 键: {entry_key[:16]}... | 范围: {start_byte}-{end_byte} | "
                    f"缓存区间: {entry.range_start}-{entry.range_end} | 结果: 缓存文件长度不足"
                )
                if trace:
                    trace.add_error('RangeError', '缓存文件长度不足')
                self.cache_logger.end_trace(request_id)
                return None

            if range_header:
                logger.info(
                    f"[Range请求] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                    f"操作: 解析Range | 键: {entry_key[:16]}... | Range头: {range_header} | "
                    f"解析结果: bytes={start_byte}-{end_byte}/{total_size} | 请求大小: {(bytes_to_read) / 1024:.2f} KB"
                )
            
            # 读取指定范围的数据
            step_start = time.time()
            async with aiofiles.open(entry.file_path, 'rb') as f:
                await f.seek(local_offset)
                data = await f.read(bytes_to_read)
            
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('文件读取', step_duration, {'bytes_read': len(data)})
            
            # 更新统计
            self.stats.cache_hits += 1
            if not entry.is_complete:
                self.stats.partial_hits += 1
            self.stats.bytes_served_from_cache += len(data)
            
            logger.info(
                f"[缓存命中] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 读取 | 键: {entry_key[:16]}... | URL: {url[:50]}... | Range: {start_byte}-{end_byte} | "
                f"缓存区间: {entry.range_start}-{entry.range_end} | 大小: {len(data) / 1024 / 1024:.2f} MB | "
                f"耗时: {step_duration:.2f} ms | 结果: 成功"
            )
            
            self.cache_logger.end_trace(request_id)
            return entry, data, start_byte, end_byte, total_size
            
        except Exception as e:
            logger.warning(f"[缓存读取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 读取 | 键: {entry_key[:16]}... | 结果: 失败 | 错误: {e}")
            if trace:
                trace.add_error('ReadError', str(e))
            # 读取失败，移除损坏的缓存
            await self.cache.remove(entry_key)
            await self._delete_cache_files(entry_key, entry)
            self.stats.cache_misses += 1
            self.cache_logger.end_trace(request_id)
            return None
    
    async def get_stream(self, url: str, range_header: Optional[str] = None) -> Optional[Tuple[CacheEntry, AsyncGenerator[bytes, None], int, int, int]]:
        """
        获取缓存的流式数据
        
        从缓存中读取数据并以流式方式返回，适用于大文件传输。
        
        参数：
            url: 请求的完整 URL
            range_header: HTTP Range 请求头
        
        返回：
            元组 (entry, stream_generator, start_byte, end_byte, total_size) 或 None
            - entry: 缓存条目
            - stream_generator: 异步数据生成器
            - start_byte: 实际起始字节
            - end_byte: 实际结束字节
            - total_size: 文件总大小
        
        实现思路：
        与 get_range 类似，但返回生成器而非完整数据，
        适合大文件流式传输，减少内存占用。
        """
        # 开始请求追踪
        request_id = self.cache_logger.start_trace(url, range_header)
        trace = self.cache_logger.get_trace(request_id)
        
        # 更新统计
        self.stats.total_requests += 1
        if range_header:
            self.stats.range_requests += 1
        
        # 生成缓存键
        key = self._get_cache_key(url)
        if trace:
            trace.cache_key = key
        
        # 记录请求开始日志
        step_start = time.time()
        logger.info(f"[请求开始] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | URL: {url[:80]}... | Range: {range_header or '无'}")
        
        # 查找缓存
        resolved_cache = await self._resolve_cached_request(url, range_header)
        step_duration = (time.time() - step_start) * 1000
        
        if trace:
            trace.add_step('缓存查找', step_duration, {'found': resolved_cache is not None})
        
        if not resolved_cache:
            self.stats.cache_misses += 1
            logger.info(f"[缓存查询] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 流式查询 | 键: {key[:16]}... | URL: {url[:50]}... | 结果: 未命中 | 原因: 无可用缓存区间")
            self.cache_logger.end_trace(request_id)
            return None

        entry_key, entry, start_byte, end_byte, total_size, local_offset = resolved_cache
        entry = self._normalize_entry(entry_key, entry)
        
        # 检查缓存是否过期
        if entry.expires_at and time.time() > entry.expires_at:
            await self.cache.remove(entry_key)
            await self._delete_cache_files(entry_key, entry)
            self.stats.cache_misses += 1
            logger.info(f"[缓存查询] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 流式查询 | 键: {entry_key[:16]}... | 结果: 未命中 | 原因: 缓存过期")
            self.cache_logger.end_trace(request_id)
            return None
        
        try:
            # 获取缓存文件大小
            step_start = time.time()
            cached_size = os.path.getsize(entry.file_path)
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('获取文件大小', step_duration, {'cached_size': cached_size})

            bytes_to_read = end_byte - start_byte + 1
            if local_offset < 0 or local_offset + bytes_to_read > cached_size:
                logger.warning(
                    f"[Range请求] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                    f"操作: 流式读取 | 键: {entry_key[:16]}... | 范围: {start_byte}-{end_byte} | "
                    f"缓存区间: {entry.range_start}-{entry.range_end} | 结果: 缓存文件长度不足"
                )
                if trace:
                    trace.add_error('RangeError', '缓存文件长度不足')
                self.cache_logger.end_trace(request_id)
                return None

            if range_header:
                logger.info(
                    f"[Range请求] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                    f"操作: 解析Range | 键: {entry_key[:16]}... | Range头: {range_header} | "
                    f"解析结果: bytes={start_byte}-{end_byte}/{total_size} | 请求大小: {(bytes_to_read) / 1024:.2f} KB"
                )
            
            # 定义流式生成器
            async def stream_generator():
                """
                流式数据生成器
                
                按块读取缓存文件并生成数据，减少内存占用。
                """
                self.cache_logger.log_cache_stage(
                    request_id,
                    'cache_hit_stream',
                    'START',
                    entry_key,
                    {
                        'range': f'{start_byte}-{end_byte}',
                        'bytes_total': bytes_to_read,
                        'source_bytes': total_size
                    }
                )
                chunk_count = 0
                bytes_sent = 0
                try:
                    async with aiofiles.open(entry.file_path, 'rb') as f:
                        await f.seek(local_offset)
                        bytes_remaining = bytes_to_read
                        
                        # 循环读取数据块
                        while bytes_remaining > 0:
                            chunk_size = min(self.chunk_size, bytes_remaining)
                            chunk = await f.read(chunk_size)
                            if not chunk:
                                break
                            bytes_remaining -= len(chunk)
                            chunk_count += 1
                            bytes_sent += len(chunk)
                            self.stats.bytes_served_from_cache += len(chunk)
                            self.cache_logger.log_streaming_progress(
                                request_id,
                                bytes_sent,
                                bytes_to_read,
                                chunk_count,
                                stage_name='cache_hit_stream',
                                cache_key=entry_key,
                                chunk_size=len(chunk),
                                served_to_client=True
                            )
                            yield chunk
                    
                    self.stats.cache_hits += 1
                    if not entry.is_complete:
                        self.stats.partial_hits += 1
                    self.cache_logger.log_cache_stage(
                        request_id,
                        'cache_hit_stream',
                        'SUCCESS',
                        entry_key,
                        {
                            'bytes_sent': bytes_sent,
                            'chunks': chunk_count
                        }
                    )
                    logger.info(
                        f"[缓存流式传输] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                        f"操作: 传输完成 | 键: {entry_key[:16]}... | Range: {start_byte}-{end_byte} | "
                        f"缓存区间: {entry.range_start}-{entry.range_end} | 块数: {chunk_count} | 结果: 成功"
                    )
                    
                except Exception as e:
                    self.cache_logger.log_cache_stage(
                        request_id,
                        'cache_hit_stream',
                        'FAIL',
                        entry_key,
                        {
                            'bytes_sent': bytes_sent,
                            'chunks': chunk_count,
                            'error': str(e)
                        }
                    )
                    logger.error(f"[缓存流式传输] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 传输 | 键: {entry_key[:16]}... | 结果: 失败 | 错误: {e}")
                    if trace:
                        trace.add_error('StreamError', str(e))
                    raise
                finally:
                    self.cache_logger.end_trace(request_id)
            
            logger.info(
                f"[缓存命中] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 流式读取 | 键: {entry_key[:16]}... | URL: {url[:50]}... | Range: {start_byte}-{end_byte} | "
                f"缓存区间: {entry.range_start}-{entry.range_end} | 总大小: {total_size / 1024 / 1024:.2f} MB | 结果: 成功"
            )
            
            return entry, stream_generator(), start_byte, end_byte, total_size
            
        except Exception as e:
            logger.warning(f"[缓存读取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 流式读取 | 键: {entry_key[:16]}... | 结果: 失败 | 错误: {e}")
            if trace:
                trace.add_error('ReadError', str(e))
            await self.cache.remove(entry_key)
            await self._delete_cache_files(entry_key, entry)
            self.stats.cache_misses += 1
            self.cache_logger.end_trace(request_id)
            return None
    
    async def cache_streaming_response(
        self, 
        url: str,
        stream_generator: AsyncGenerator[bytes, None],
        headers: Dict[str, str] = None
    ) -> AsyncGenerator[bytes, None]:
        """
        边传输边缓存流式响应
        
        在流式传输过程中实时缓存数据，实现边传边存。
        
        参数：
            url: 请求的完整 URL
            stream_generator: 源站响应的数据流生成器
            headers: HTTP 响应头
        
        返回：
            异步数据生成器，同时传输和缓存数据
        
        实现思路：
        1. 创建临时文件
        2. 从源站读取数据块
        3. 同时写入临时文件和返回给客户端
        4. 传输完成后重命名为正式缓存文件
        5. 创建缓存条目并保存元数据
        """
        # 开始请求追踪
        request_id = self.cache_logger.start_trace(url)
        trace = self.cache_logger.get_trace(request_id)
        
        # 提取响应头信息
        headers = headers or {}
        content_type = headers.get('Content-Type', 'application/octet-stream')
        etag = headers.get('ETag')
        last_modified = headers.get('Last-Modified')
        parsed_content_range = self._parse_content_range(headers.get('Content-Range'))
        expected_cache_size = self._get_expected_response_size(headers)
        
        # 获取预期大小
        total_size = 0
        if 'Content-Length' in headers:
            try:
                total_size = int(headers['Content-Length'])
            except ValueError:
                pass

        range_start = 0
        range_end = total_size - 1 if total_size > 0 else None
        is_complete = True
        key = self._get_cache_key(url)

        if parsed_content_range:
            range_start, range_end, total_size = parsed_content_range
            is_complete = range_start == 0 and range_end == total_size - 1
            if not is_complete:
                key = self._get_segment_key(url, range_start, range_end)
        elif total_size <= 0:
            range_end = None

        if trace:
            trace.cache_key = key

        # 获取路径
        temp_path = self._get_temp_path(key)
        cache_path = self._get_cache_path(key)
        
        if trace:
            trace.add_step(
                '开始流式缓存',
                details={
                    'key': key,
                    'expected_size': total_size,
                    'range_start': range_start,
                    'range_end': range_end
                }
            )
        
        # 记录开始缓存日志
        if total_size > 0:
            logger.info(f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 开始缓存 | 键: {key[:16]}... | URL: {url[:50]}... | 预期大小: {total_size / 1024 / 1024:.2f} MB")
        else:
            logger.info(f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 开始缓存 | 键: {key[:16]}... | URL: {url[:50]}... | 预期大小: 未知")
        self.cache_logger.log_cache_stage(
            request_id,
            'stream_cache',
            'START',
            key,
            {
                'expected_bytes': expected_cache_size or total_size,
                'range_start': range_start,
                'range_end': range_end,
                'is_complete': is_complete
            }
        )
        
        cache_file = None
        bytes_cached = 0
        chunk_count = 0
        cache_skip_reason = None

        if expected_cache_size is not None:
            cache_skip_reason = (
                self._check_request_cache_limit(expected_cache_size) or
                self._check_cache_size_limits(expected_cache_size)
            )
            if cache_skip_reason:
                self.cache_logger.log_cache_stage(
                    request_id,
                    'stream_cache',
                    'SKIP',
                    key,
                    {
                        'reason': cache_skip_reason,
                        'expected_bytes': expected_cache_size
                    }
                )
                logger.info(
                    f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                    f"操作: 跳过缓存 | 键: {key[:16]}... | 原因: {cache_skip_reason}"
                )
                async for chunk in stream_generator:
                    self.stats.bytes_downloaded += len(chunk)
                    yield chunk
                self.cache_logger.end_trace(request_id)
                return
        
        try:
            # 打开临时文件
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_open_temp',
                'START',
                key,
                {'temp_path': str(temp_path)}
            )
            step_start = time.time()
            cache_file = await aiofiles.open(temp_path, 'wb')
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('打开临时文件', step_duration)
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_open_temp',
                'END',
                key,
                {'temp_path': str(temp_path)}
            )
            
            # 边传输边缓存
            async for chunk in stream_generator:
                try:
                    if cache_file is None:
                        self.stats.bytes_downloaded += len(chunk)
                        yield chunk
                        continue

                    next_size = bytes_cached + len(chunk)
                    limit_reason = (
                        self._check_request_cache_limit(next_size) or
                        self._check_cache_size_limits(next_size)
                    )
                    if limit_reason:
                        cache_skip_reason = limit_reason
                        self.cache_logger.log_cache_stage(
                            request_id,
                            'stream_cache_write',
                            'ABORT',
                            key,
                            {
                                'bytes_cached': bytes_cached,
                                'reason': cache_skip_reason,
                                'chunks': chunk_count
                            }
                        )
                        await cache_file.close()
                        cache_file = None
                        try:
                            temp_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                        logger.info(
                            f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                            f"操作: 中止缓存 | 键: {key[:16]}... | 已缓存: {bytes_cached / 1024 / 1024:.2f} MB | 原因: {cache_skip_reason}"
                        )
                        self.stats.bytes_downloaded += len(chunk)
                        yield chunk
                        continue

                    write_start = time.time()
                    self.stats.bytes_downloaded += len(chunk)
                    await cache_file.write(chunk)
                    bytes_cached = next_size
                    chunk_count += 1
                    self.cache_logger.log_streaming_progress(
                        request_id,
                        bytes_cached,
                        expected_cache_size or total_size,
                        chunk_count,
                        stage_name='stream_cache_write',
                        cache_key=key,
                        chunk_size=len(chunk),
                        served_to_client=False
                    )
                    yield chunk
                    
                except Exception as e:
                    logger.error(f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 写入 | 键: {key[:16]}... | 块号: {chunk_count} | 结果: 失败 | 错误: {e}")
                    if trace:
                        trace.add_error('WriteError', str(e))
                    raise

            if cache_skip_reason or bytes_cached <= 0:
                self.cache_logger.log_cache_stage(
                    request_id,
                    'stream_cache',
                    'ABORT',
                    key,
                    {
                        'reason': cache_skip_reason or 'empty_cache_file',
                        'bytes_cached': bytes_cached,
                        'chunks': chunk_count
                    }
                )
                self.cache_logger.end_trace(request_id)
                return
            
            # 关闭临时文件
            await cache_file.close()
            cache_file = None
            
            # 重命名为正式缓存文件
            # 注意：Path.rename() 是同步方法，需要在异步上下文中正确处理
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_finalize',
                'START',
                key,
                {
                    'temp_path': str(temp_path),
                    'cache_path': str(cache_path)
                }
            )
            step_start = time.time()
            import shutil
            shutil.move(str(temp_path), str(cache_path))
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('重命名文件', step_duration)
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_finalize',
                'END',
                key,
                {
                    'cache_path': str(cache_path)
                }
            )
            
            # 获取最终大小
            final_size = cache_path.stat().st_size
            if range_end is None:
                range_end = range_start + final_size - 1
            if total_size <= 0:
                total_size = range_end + 1
            if is_complete:
                total_size = final_size
                range_start = 0
                range_end = final_size - 1
            
            # 创建缓存条目
            entry = CacheEntry(
                url=url,
                file_path=str(cache_path),
                size=final_size,
                content_type=content_type,
                created_at=time.time(),
                last_accessed=time.time(),
                access_count=1,
                etag=etag,
                last_modified=last_modified,
                expires_at=time.time() + self.default_ttl,
                is_complete=is_complete,
                is_streaming=True,
                cache_key=key,
                parent_key=self._get_cache_key(url),
                range_start=range_start,
                range_end=range_end,
                source_size=total_size,
                request_id=request_id
            )
            
            # 存入 LRU 缓存
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_store',
                'START',
                key,
                {'size_bytes': final_size}
            )
            step_start = time.time()
            stored, evicted_entries = await self.cache.put(key, entry)
            evicted_files = [evicted_entry.file_path for _, evicted_entry in evicted_entries]
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('存入LRU缓存', step_duration, {'evicted_count': len(evicted_files)})
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_store',
                'END',
                key,
                {
                    'size_bytes': final_size,
                    'evicted_count': len(evicted_files)
                }
            )
            
            # 删除被淘汰的文件
            await self._delete_evicted_entries(evicted_entries)
            if evicted_files:
                for evicted_file in evicted_files:
                    logger.info(f"[缓存淘汰] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 淘汰旧缓存 | 文件: {evicted_file} | 原因: 空间不足")

            if not stored:
                self.cache_logger.log_cache_stage(
                    request_id,
                    'stream_cache',
                    'ABORT',
                    key,
                    {
                        'reason': 'cache_put_rejected',
                        'size_bytes': final_size
                    }
                )
                await self._delete_files([str(cache_path)])
                logger.info(
                    f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                    f"操作: 放弃缓存 | 键: {key[:16]}... | 原因: 超过缓存限制"
                )
                self.cache_logger.end_trace(request_id)
                return
            
            # 保存元数据
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_metadata',
                'START',
                key,
                {'size_bytes': final_size}
            )
            step_start = time.time()
            await self._save_meta(key, entry)
            step_duration = (time.time() - step_start) * 1000
            if trace:
                trace.add_step('保存元数据', step_duration)
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache_metadata',
                'END',
                key,
                {'size_bytes': final_size}
            )
            
            # 更新统计
            self.stats.streaming_cache_writes += 1
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache',
                'SUCCESS',
                key,
                {
                    'size_bytes': final_size,
                    'chunks': chunk_count,
                    'is_complete': is_complete
                }
            )
            
            logger.info(f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 缓存完成 | 键: {key[:16]}... | URL: {url[:50]}... | 大小: {final_size / 1024 / 1024:.2f} MB | 块数: {chunk_count} | 结果: 成功")
            
            self.cache_logger.end_trace(request_id)
            
        except asyncio.CancelledError:
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache',
                'ABORT',
                key,
                {
                    'reason': 'cancelled',
                    'bytes_cached': bytes_cached,
                    'chunks': chunk_count
                }
            )
            logger.info(f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 取消 | 键: {key[:16]}... | 已缓存: {bytes_cached / 1024 / 1024:.2f} MB")
            if trace:
                trace.add_error('Cancelled', 'Request was cancelled')
            self.cache_logger.end_trace(request_id)
            raise
        except Exception as e:
            self.cache_logger.log_cache_stage(
                request_id,
                'stream_cache',
                'FAIL',
                key,
                {
                    'error': str(e),
                    'bytes_cached': bytes_cached,
                    'chunks': chunk_count
                }
            )
            logger.error(f"[流式缓存] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 缓存 | 键: {key[:16]}... | 结果: 失败 | 错误: {e}")
            if trace:
                trace.add_error('CacheError', str(e))
            # 清理临时文件
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except:
                pass
            self.cache_logger.end_trace(request_id)
            raise
        finally:
            if cache_file:
                try:
                    await cache_file.close()
                except:
                    pass

    async def schedule_following_range_prefetch(
        self,
        url: str,
        request_range: Optional[str],
        response_headers: Dict[str, str],
        request_headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        基于当前 Range 请求异步预取下一段连续区间。
        """
        if not self.enable_preload:
            return False

        if not request_range:
            return False

        parsed_content_range = self._parse_content_range(response_headers.get('Content-Range'))
        if not parsed_content_range:
            return False

        current_start, current_end, total_size = parsed_content_range
        if current_end >= total_size - 1:
            return False

        segment_size = current_end - current_start + 1
        if segment_size <= 0:
            return False

        next_start = current_end + 1
        next_end = min(total_size - 1, current_end + segment_size)
        return await self.schedule_range_prefetch(url, next_start, next_end, request_headers)

    async def schedule_range_prefetch(
        self,
        url: str,
        start_byte: int,
        end_byte: int,
        request_headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        调度单个 Range 的后台预取任务。
        """
        if not self.enable_preload:
            return False

        if start_byte > end_byte or self._is_range_cached(url, start_byte, end_byte):
            return False

        task_key = f"{self._get_cache_key(url)}:{start_byte}-{end_byte}"
        existing_task = self._cache_writers.get(task_key)
        if existing_task and not existing_task.done():
            return False

        task = asyncio.create_task(
            self._prefetch_range_task(url, start_byte, end_byte, request_headers, task_key)
        )
        self._cache_writers[task_key] = task
        return True

    async def _prefetch_range_task(
        self,
        url: str,
        start_byte: int,
        end_byte: int,
        request_headers: Optional[Dict[str, str]],
        task_key: str
    ):
        """
        执行后台区间预取。
        """
        request_id = str(uuid.uuid4())[:8]
        try:
            if self._is_range_cached(url, start_byte, end_byte):
                return

            headers = request_headers.copy() if request_headers else {}
            headers['Range'] = f'bytes={start_byte}-{end_byte}'

            logger.info(
                f"[Range预取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 开始 | URL: {url[:50]}... | 范围: {start_byte}-{end_byte}"
            )

            session = await self.get_session()
            async with self._preload_semaphore:
                async with session.get(url, headers=headers, allow_redirects=True) as response:
                    if response.status not in (200, 206):
                        logger.warning(
                            f"[Range预取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                            f"操作: 预取 | URL: {url[:50]}... | 状态码: {response.status} | 结果: 跳过"
                        )
                        return

                    async for _ in self.cache_streaming_response(
                        url,
                        response.content.iter_chunked(self.chunk_size),
                        dict(response.headers)
                    ):
                        pass

            logger.info(
                f"[Range预取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 完成 | URL: {url[:50]}... | 范围: {start_byte}-{end_byte} | 结果: 成功"
            )

        except asyncio.CancelledError:
            logger.info(
                f"[Range预取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 取消 | URL: {url[:50]}... | 范围: {start_byte}-{end_byte}"
            )
            raise
        except Exception as e:
            logger.warning(
                f"[Range预取] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 失败 | URL: {url[:50]}... | 范围: {start_byte}-{end_byte} | 错误: {e}"
            )
        finally:
            self._cache_writers.pop(task_key, None)
    
    async def download_and_cache(self, url: str, headers: Dict[str, str] = None,
                                  progress_callback=None) -> Optional[CacheEntry]:
        """
        下载并缓存内容
        
        从源站下载内容并缓存到本地。
        
        参数：
            url: 请求的完整 URL
            headers: HTTP 请求头
            progress_callback: 进度回调函数
        
        返回：
            缓存条目，如果失败返回 None
        
        实现思路：
        如果已有相同 URL 的下载任务在进行，等待其完成而非重复下载。
        """
        # 开始请求追踪
        request_id = self.cache_logger.start_trace(url)
        trace = self.cache_logger.get_trace(request_id)
        
        # 生成缓存键
        key = self._get_cache_key(url)
        if trace:
            trace.cache_key = key
        
        # 检查是否有进行中的下载任务
        if key in self._download_tasks:
            logger.info(f"[下载任务] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 等待现有任务 | 键: {key[:16]}... | URL: {url[:50]}...")
            try:
                return await self._download_tasks[key]
            except Exception as e:
                logger.error(f"[下载任务] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 等待 | 键: {key[:16]}... | 结果: 失败 | 错误: {e}")
                return None
        
        # 创建下载任务
        task = asyncio.create_task(self._do_download(key, url, headers, progress_callback, request_id))
        self._download_tasks[key] = task
        
        try:
            entry = await task
            return entry
        finally:
            self._download_tasks.pop(key, None)
    
    async def _do_download(self, key: str, url: str, headers: Dict[str, str],
                           progress_callback, request_id: str = None) -> Optional[CacheEntry]:
        """
        执行实际的下载操作
        
        支持断点续传，如果临时文件存在则继续下载。
        
        参数：
            key: 缓存键
            url: 请求的完整 URL
            headers: HTTP 请求头
            progress_callback: 进度回调函数
            request_id: 请求 ID
        
        返回：
            缓存条目，如果失败返回 None
        """
        if not request_id:
            request_id = self.cache_logger.start_trace(url)
        trace = self.cache_logger.get_trace(request_id)
        if trace:
            trace.cache_key = key
        self.cache_logger.log_cache_stage(
            request_id,
            'download_cache',
            'START',
            key,
            {'url': url[:120]}
        )
        
        # 获取路径
        temp_path = self._get_temp_path(key)
        cache_path = self._get_cache_path(key)
        
        # 准备请求头
        request_headers = headers.copy() if headers else {}
        
        # 检查是否有部分下载的文件（断点续传）
        partial_size = 0
        if temp_path.exists():
            partial_size = temp_path.stat().st_size
            if partial_size > 0:
                request_headers['Range'] = f'bytes={partial_size}-'
                logger.info(f"[断点续传] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 继续 | 键: {key[:16]}... | 已下载: {partial_size / 1024 / 1024:.2f} MB")
                if trace:
                    trace.add_step('断点续传', details={'partial_size': partial_size})
        
        # 获取 HTTP 会话
        session = await self.get_session()
        
        try:
            # 发起下载请求
            step_start = time.time()
            if trace:
                trace.add_step('发起下载请求', details={'url': url})
            
            async with self._preload_semaphore:
                async with session.get(url, headers=request_headers, allow_redirects=True) as response:
                # 检查响应状态
                    if response.status not in (200, 206):
                        logger.warning(f"[下载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 下载 | 键: {key[:16]}... | 状态码: {response.status} | 结果: 失败")
                        if trace:
                            trace.add_error('HTTPError', f'状态码 {response.status}')
                        self.cache_logger.end_trace(request_id)
                        return None
                
                    # 提取响应头信息
                    content_type = response.headers.get('Content-Type', 'application/octet-stream')
                    etag = response.headers.get('ETag')
                    last_modified = response.headers.get('Last-Modified')
                
                    # 获取总大小
                    total_size = int(response.headers.get('Content-Length', 0))
                    if response.status == 206:
                        content_range = response.headers.get('Content-Range', '')
                        if content_range:
                            total_size = int(content_range.split('/')[-1])
                
                    # 判断是否为断点续传
                    is_resume = response.status == 206
                    expected_request_cache_size = max(total_size - partial_size, 0) if total_size > 0 else None
                    request_limit_reason = None
                    if expected_request_cache_size is not None:
                        request_limit_reason = self._check_request_cache_limit(expected_request_cache_size)
                    entry_limit_reason = self._check_cache_size_limits(total_size) if total_size > 0 else None

                    if request_limit_reason or entry_limit_reason:
                        skip_reason = request_limit_reason or entry_limit_reason
                        self.cache_logger.log_cache_stage(
                            request_id,
                            'download_cache',
                            'SKIP',
                            key,
                            {
                                'reason': skip_reason,
                                'expected_bytes': expected_request_cache_size or total_size
                            }
                        )
                        logger.info(
                            f"[下载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                            f"操作: 跳过缓存 | 键: {key[:16]}... | 原因: {skip_reason}"
                        )
                        self.cache_logger.end_trace(request_id)
                        return None
                
                    step_duration = (time.time() - step_start) * 1000
                    if trace:
                        trace.add_step('下载响应接收', step_duration, {'status': response.status, 'total_size': total_size})
                
                    # 确定文件打开模式
                    mode = 'ab' if is_resume else 'wb'
                    downloaded = partial_size if is_resume else 0
                    request_cached = 0
                    limit_reached = False
                
                    # 记录下载开始日志
                    if total_size > 0:
                        logger.info(f"[下载开始] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 下载 | 键: {key[:16]}... | URL: {url[:50]}... | 总大小: {total_size / 1024 / 1024:.2f} MB")
                    else:
                        logger.info(f"[下载开始] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 下载 | 键: {key[:16]}... | URL: {url[:50]}... | 总大小: 未知")
                
                    # 下载并写入文件
                    async with aiofiles.open(temp_path, mode) as f:
                        chunk_count = 0
                        async for chunk in response.content.iter_chunked(self.chunk_size):
                            next_request_cached = request_cached + len(chunk)
                            next_downloaded = downloaded + len(chunk)
                            request_limit_reason = self._check_request_cache_limit(next_request_cached)
                            entry_limit_reason = self._check_cache_size_limits(next_downloaded)
                            if request_limit_reason or entry_limit_reason:
                                limit_reached = True
                                self.cache_logger.log_cache_stage(
                                    request_id,
                                    'download_cache_write',
                                    'ABORT',
                                    key,
                                    {
                                        'reason': request_limit_reason or entry_limit_reason,
                                        'downloaded_bytes': downloaded,
                                        'request_cached_bytes': request_cached,
                                        'chunks': chunk_count
                                    }
                                )
                                logger.info(
                                    f"[下载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                                    f"操作: 中止缓存 | 键: {key[:16]}... | 已缓存: {downloaded / 1024 / 1024:.2f} MB | "
                                    f"原因: {request_limit_reason or entry_limit_reason}"
                                )
                                break

                            await f.write(chunk)
                            downloaded = next_downloaded
                            request_cached = next_request_cached
                            chunk_count += 1
                            self.stats.bytes_downloaded += len(chunk)
                            self.cache_logger.log_streaming_progress(
                                request_id,
                                downloaded,
                                total_size,
                                chunk_count,
                                stage_name='download_cache_write',
                                cache_key=key,
                                chunk_size=len(chunk),
                                served_to_client=False
                            )
                            
                            # 调用进度回调
                            if progress_callback:
                                await progress_callback(downloaded, total_size)

                    if limit_reached:
                        self.cache_logger.log_cache_stage(
                            request_id,
                            'download_cache',
                            'ABORT',
                            key,
                            {
                                'reason': 'size_limit_reached',
                                'downloaded_bytes': downloaded,
                                'request_cached_bytes': request_cached,
                                'chunks': chunk_count
                            }
                        )
                        try:
                            temp_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                        self.cache_logger.end_trace(request_id)
                        return None
                
                    # 重命名为正式缓存文件
                    step_start = time.time()
                    import shutil
                    shutil.move(str(temp_path), str(cache_path))
                    step_duration = (time.time() - step_start) * 1000
                    if trace:
                        trace.add_step('重命名缓存文件', step_duration)
                
                    # 获取最终大小
                    final_size = cache_path.stat().st_size
                    final_limit_reason = (
                        self._check_request_cache_limit(request_cached) or
                        self._check_cache_size_limits(final_size)
                    )
                    if final_limit_reason:
                        self.cache_logger.log_cache_stage(
                            request_id,
                            'download_cache',
                            'ABORT',
                            key,
                            {
                                'reason': final_limit_reason,
                                'downloaded_bytes': downloaded,
                                'request_cached_bytes': request_cached,
                                'chunks': chunk_count
                            }
                        )
                        await self._delete_files([str(cache_path)])
                        logger.info(
                            f"[下载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                            f"操作: 放弃缓存 | 键: {key[:16]}... | 原因: {final_limit_reason}"
                        )
                        self.cache_logger.end_trace(request_id)
                        return None
                
                    # 创建缓存条目
                    entry = CacheEntry(
                        url=url,
                        file_path=str(cache_path),
                        size=final_size,
                        content_type=content_type,
                        created_at=time.time(),
                        last_accessed=time.time(),
                        access_count=1,
                        etag=etag,
                        last_modified=last_modified,
                        expires_at=time.time() + self.default_ttl,
                        is_complete=True,
                        cache_key=key,
                        parent_key=key,
                        range_start=0,
                        range_end=final_size - 1,
                        source_size=final_size,
                        request_id=request_id
                    )
                
                    # 存入 LRU 缓存
                    step_start = time.time()
                    stored, evicted_entries = await self.cache.put(key, entry)
                    evicted_files = [evicted_entry.file_path for _, evicted_entry in evicted_entries]
                    step_duration = (time.time() - step_start) * 1000
                    if trace:
                        trace.add_step('存入LRU缓存', step_duration, {'evicted_count': len(evicted_files)})
                
                    # 删除被淘汰的文件
                    await self._delete_evicted_entries(evicted_entries)

                    if not stored:
                        self.cache_logger.log_cache_stage(
                            request_id,
                            'download_cache',
                            'ABORT',
                            key,
                            {
                                'reason': 'cache_put_rejected',
                                'size_bytes': final_size
                            }
                        )
                        await self._delete_files([str(cache_path)])
                        logger.info(
                            f"[下载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                            f"操作: 放弃缓存 | 键: {key[:16]}... | 原因: 超过缓存限制"
                        )
                        self.cache_logger.end_trace(request_id)
                        return None
                
                    # 保存元数据
                    step_start = time.time()
                    await self._save_meta(key, entry)
                    step_duration = (time.time() - step_start) * 1000
                    if trace:
                        trace.add_step('保存元数据', step_duration)
                    self.cache_logger.log_cache_stage(
                        request_id,
                        'download_cache',
                        'SUCCESS',
                        key,
                        {
                            'size_bytes': final_size,
                            'downloaded_bytes': downloaded,
                            'chunks': chunk_count
                        }
                    )
                 
                    logger.info(f"[下载完成] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 缓存 | 键: {key[:16]}... | URL: {url[:50]}... | 大小: {final_size / 1024 / 1024:.2f} MB | 结果: 成功")
                
                    self.cache_logger.end_trace(request_id)
                    return entry
        
        except asyncio.CancelledError:
            self.cache_logger.log_cache_stage(
                request_id,
                'download_cache',
                'ABORT',
                key,
                {'reason': 'cancelled'}
            )
            logger.info(f"[下载取消] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 取消 | 键: {key[:16]}... | URL: {url[:50]}...")
            if trace:
                trace.add_error('Cancelled', 'Download was cancelled')
            self.cache_logger.end_trace(request_id)
            raise
        except Exception as e:
            self.cache_logger.log_cache_stage(
                request_id,
                'download_cache',
                'FAIL',
                key,
                {'error': str(e)}
            )
            logger.error(f"[下载失败] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 下载 | 键: {key[:16]}... | URL: {url[:50]}... | 结果: 失败 | 错误: {e}")
            if trace:
                trace.add_error('DownloadError', str(e))
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass
            self.cache_logger.end_trace(request_id)
            return None
    
    async def validate_cache(self, key: str, entry: CacheEntry) -> bool:
        """
        校验缓存有效性
        
        使用 ETag 或 Last-Modified 校验缓存是否仍然有效。
        
        参数：
            key: 缓存键
            entry: 缓存条目
        
        返回：
            True 表示缓存有效，False 表示缓存过期
        
        实现思路：
        发送条件请求（If-None-Match 或 If-Modified-Since），
        根据响应判断缓存是否有效。
        """
        request_id = str(uuid.uuid4())[:8]
        step_start = time.time()

        if not self.enable_validation:
            return True
        
        # 如果没有校验标识，默认有效
        if not entry.etag and not entry.last_modified:
            logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 跳过 | 原因: 无校验标识")
            return True
        
        # 更新校验请求统计
        self.stats.validation_requests += 1
        
        # 准备条件请求头
        session = await self.get_session()
        headers = {}
        if entry.etag:
            headers['If-None-Match'] = entry.etag
        if entry.last_modified:
            headers['If-Modified-Since'] = entry.last_modified
        
        try:
            logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 发起校验请求 | 键: {key[:16]}... | URL: {entry.url[:50]}...")
            
            # 发送 HEAD 请求校验
            async with session.head(entry.url, headers=headers, allow_redirects=True) as response:
                duration_ms = (time.time() - step_start) * 1000
                
                # 304 Not Modified 表示缓存有效
                if response.status == 304:
                    logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 有效 | 耗时: {duration_ms:.2f} ms")
                    return True
                # 200 OK 表示需要检查是否有更新
                elif response.status == 200:
                    new_etag = response.headers.get('ETag')
                    new_last_modified = response.headers.get('Last-Modified')
                    # 检查 ETag 是否变化
                    if new_etag and new_etag != entry.etag:
                        logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 已过期 | 原因: ETag变化 | 耗时: {duration_ms:.2f} ms")
                        return False
                    # 检查 Last-Modified 是否变化
                    if new_last_modified and new_last_modified != entry.last_modified:
                        logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 已过期 | 原因: Last-Modified变化 | 耗时: {duration_ms:.2f} ms")
                        return False
                    logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 有效 | 耗时: {duration_ms:.2f} ms")
                    return True
                # 其他状态码表示缓存无效
                logger.info(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 无效 | 状态码: {response.status} | 耗时: {duration_ms:.2f} ms")
                return False
        except Exception as e:
            duration_ms = (time.time() - step_start) * 1000
            logger.warning(f"[缓存校验] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 校验 | 键: {key[:16]}... | 结果: 失败 | 错误: {e} | 耗时: {duration_ms:.2f} ms")
            # 校验失败时默认缓存有效，避免不必要的重新下载
            return True
    
    async def preload(self, urls: List[str], priority: int = 0):
        """
        预加载 URL 列表
        
        将 URL 添加到预加载队列，后台异步下载。
        
        参数：
            urls: 要预加载的 URL 列表
            priority: 优先级（暂未实现）
        """
        if not self.enable_preload:
            logger.info(f"[预加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 操作: 跳过 | 原因: 预加载功能已禁用")
            return

        if not self._preload_queue:
            self._preload_queue = asyncio.Queue()

        if not self._running:
            self._running = True
            self._preload_workers = [
                asyncio.create_task(self._preload_worker())
                for _ in range(self.preload_concurrency)
            ]

        for url in urls:
            await self._preload_queue.put((priority, url))
    
    async def _preload_worker(self):
        """
        预加载工作协程
        
        从队列中取出 URL 并下载，如果已缓存则跳过。
        """
        while self._running:
            got_item = False
            try:
                priority, url = await asyncio.wait_for(
                    self._preload_queue.get(), 
                    timeout=1.0
                )
                got_item = True
                
                request_id = str(uuid.uuid4())[:8]
                self.stats.preload_requests += 1
                
                key = self._get_cache_key(url)
                
                logger.info(f"[预加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 开始 | 键: {key[:16]}... | URL: {url[:50]}...")
                
                # 检查是否已缓存
                existing = await self.cache.get(key)
                if existing:
                    existing = self._normalize_entry(key, existing)
                    if existing.expires_at and time.time() > existing.expires_at:
                        await self.cache.remove(key)
                        await self._delete_cache_files(key, existing)
                        existing = None
                
                if existing and existing.is_complete:
                    self.stats.preload_hits += 1
                    logger.info(f"[预加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 命中 | 键: {key[:16]}... | URL: {url[:50]}...")
                    continue
                
                # 下载并缓存
                await self.download_and_cache(url)
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[预加载] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 执行 | 结果: 失败 | 错误: {e}")
            finally:
                if got_item and self._preload_queue:
                    self._preload_queue.task_done()
    
    async def clear_cache(self) -> Dict[str, Any]:
        """
        清空所有缓存
        
        删除所有缓存文件、临时文件和元数据文件。
        
        返回：
            包含清理结果的字典：
            - cleared_files: 清理的文件数
            - freed_size: 释放的空间（字节）
        """
        request_id = str(uuid.uuid4())[:8]
        step_start = time.time()
        
        logger.info(f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 开始清理")
        
        # 清空 LRU 缓存并获取文件列表
        files = await self.cache.clear()
        total_size = 0
        
        # 删除缓存文件
        for file_path in files:
            try:
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    total_size += size
                    os.unlink(file_path)
            except Exception as e:
                logger.warning(f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 删除文件 | 文件: {file_path} | 结果: 失败 | 错误: {e}")
        
        # 删除所有元数据文件
        for meta_file in self.meta_dir.glob('*.json'):
            try:
                meta_file.unlink()
            except Exception:
                pass
        
        # 删除所有临时文件
        for temp_file in self.temp_dir.glob('*.tmp'):
            try:
                temp_file.unlink()
            except Exception:
                pass
        
        duration_ms = (time.time() - step_start) * 1000
        
        logger.info(f"[缓存清理] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 清理完成 | 文件数: {len(files)} | 释放空间: {total_size / 1024 / 1024:.2f} MB | 耗时: {duration_ms:.2f} ms")
        
        return {
            'cleared_files': len(files),
            'freed_size': total_size
        }
    
    async def get_cache_info(self) -> Dict[str, Any]:
        """
        获取缓存信息
        
        返回缓存统计信息和条目列表。
        
        返回：
            包含缓存信息的字典：
            - cache_info: 缓存基本信息
            - stats: 缓存统计信息
            - entries: 缓存条目列表（最多100个）
            - total_entries: 总条目数
        """
        cache_info = self.cache.get_info()
        
        # 构建条目列表
        entries = []
        for key, entry in self.cache.cache.items():
            entry = self._normalize_entry(key, entry)
            entries.append({
                'key': key,
                'parent_key': entry.parent_key,
                'url': entry.url[:100],
                'size': entry.size,
                'content_type': entry.content_type,
                'created_at': entry.created_at,
                'last_accessed': entry.last_accessed,
                'access_count': entry.access_count,
                'is_complete': entry.is_complete,
                'is_streaming': entry.is_streaming,
                'range_start': entry.range_start,
                'range_end': entry.range_end,
                'source_size': entry.source_size,
                'expires_at': entry.expires_at,
                'request_id': entry.request_id if hasattr(entry, 'request_id') else None
            })
        
        return {
            'cache_info': cache_info,
            'stats': {
                'total_requests': self.stats.total_requests,
                'cache_hits': self.stats.cache_hits,
                'cache_misses': self.stats.cache_misses,
                'partial_hits': self.stats.partial_hits,
                'range_requests': self.stats.range_requests,
                'hit_rate': f"{self.stats.hit_rate:.2f}%",
                'bytes_served_from_cache': self.stats.bytes_served_from_cache,
                'mb_served_from_cache': round(self.stats.bytes_served_from_cache / 1024 / 1024, 2),
                'bytes_downloaded': self.stats.bytes_downloaded,
                'mb_downloaded': round(self.stats.bytes_downloaded / 1024 / 1024, 2),
                'evicted_entries': self.stats.evicted_entries,
                'preload_requests': self.stats.preload_requests,
                'preload_hits': self.stats.preload_hits,
                'streaming_cache_writes': self.stats.streaming_cache_writes,
                'validation_requests': self.stats.validation_requests,
                'eviction_events': self.stats.eviction_events
            },
            'entries': entries[:100],
            'total_entries': len(entries)
        }
    
    async def remove_entry(self, url: str) -> bool:
        """
        删除指定 URL 的缓存条目
        
        参数：
            url: 要删除的 URL
        
        返回：
            True 表示删除成功，False 表示条目不存在
        """
        request_id = str(uuid.uuid4())[:8]
        key = self._get_cache_key(url)
        
        logger.info(f"[缓存删除] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 删除 | 键: {key[:16]}... | URL: {url[:50]}...")
        removed_count = await self._remove_entries_by_parent_key(key)

        if removed_count:
            logger.info(
                f"[缓存删除] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | "
                f"操作: 删除完成 | 键: {key[:16]}... | 条目数: {removed_count} | 结果: 成功"
            )
            return True

        logger.info(f"[缓存删除] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 删除 | 键: {key[:16]}... | 结果: 条目不存在")
        return False
    
    async def update_entry(self, url: str) -> Optional[CacheEntry]:
        """
        更新指定 URL 的缓存
        
        校验缓存有效性，如果过期则重新下载。
        
        参数：
            url: 要更新的 URL
        
        返回：
            更新后的缓存条目，如果失败返回 None
        """
        request_id = str(uuid.uuid4())[:8]
        key = self._get_cache_key(url)
        
        logger.info(f"[缓存更新] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 更新检查 | 键: {key[:16]}... | URL: {url[:50]}...")
        
        # 获取现有条目
        entry = await self.cache.get(key)
        
        if entry:
            # 校验缓存有效性
            is_valid = await self.validate_cache(key, entry)
            if not is_valid:
                logger.info(f"[缓存更新] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 更新 | 键: {key[:16]}... | URL: {url[:50]}... | 原因: 缓存过期")
                # 删除过期缓存
                await self._remove_entries_by_parent_key(key)
                # 重新下载
                return await self.download_and_cache(url)
            return entry
        
        # 没有缓存，直接下载
        logger.info(f"[缓存更新] 时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | 请求ID: {request_id} | 操作: 下载 | 键: {key[:16]}... | URL: {url[:50]}... | 原因: 无缓存")
        return await self.download_and_cache(url)
