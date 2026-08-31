"""
IP封禁管理模块

本模块实现IP封禁/黑名单功能：
1. 支持将指定IP加入封禁列表
2. 支持设置封禁原因和过期时间
3. 请求处理时检查IP是否被封禁
4. 提供封禁列表的增删查API
"""

import asyncio
import ipaddress
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger("proxy.ip_ban")


@dataclass
class BanEntry:
    ip: str = ""
    reason: str = ""
    banned_by: str = ""
    banned_at: float = 0.0
    expire_at: float = 0.0
    permanent: bool = False
    path_prefix: str = ""  # 封禁作用域：为空表示全局封禁，否则只封禁该路径前缀下的请求


class IpBanManager:
    def __init__(self):
        self._bans: Dict[str, BanEntry] = {}
        self._cidr_bans: List[Tuple[Union[ipaddress.IPv4Network, ipaddress.IPv6Network], BanEntry]] = []
        self._lock = asyncio.Lock()
        self._total_bans = 0
        self._total_hits = 0

    def _is_cidr(self, ip_str: str) -> bool:
        """判断字符串是否为 CIDR 网段格式（如 192.168.1.0/24）。"""
        return "/" in ip_str

    def _parse_cidr(self, cidr_str: str) -> Optional[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]:
        """解析 CIDR 字符串为网络对象，失败返回 None。"""
        try:
            return ipaddress.ip_network(cidr_str, strict=False)
        except ValueError:
            return None

    def _is_expired(self, entry: BanEntry) -> bool:
        if entry.permanent or entry.expire_at <= 0:
            return False
        return time.time() > entry.expire_at

    def _matches_path(self, entry: BanEntry, path: str) -> bool:
        """检查封禁记录是否作用于指定路径。path_prefix 为空匹配所有路径。"""
        if not entry.path_prefix:
            return True
        return path.startswith(entry.path_prefix)

    async def is_banned(self, ip: str, path: str = "") -> Optional[BanEntry]:
        async with self._lock:
            # 1. 先精确匹配单 IP（快速路径，大多数封禁是单 IP）
            entry = self._bans.get(ip)
            if entry is not None:
                if self._is_expired(entry):
                    del self._bans[ip]
                elif self._matches_path(entry, path):
                    self._total_hits += 1
                    return entry
            # 2. 再遍历 CIDR 网段列表匹配
            if self._cidr_bans:
                try:
                    parsed_ip = ipaddress.ip_address(ip)
                except ValueError:
                    parsed_ip = None
                if parsed_ip is not None:
                    expired_cidrs = []
                    matched = None
                    for net, cidr_entry in self._cidr_bans:
                        if self._is_expired(cidr_entry):
                            expired_cidrs.append((net, cidr_entry))
                            continue
                        if parsed_ip in net and self._matches_path(cidr_entry, path):
                            matched = cidr_entry
                            break
                    # 清理过期的 CIDR 条目
                    for item in expired_cidrs:
                        self._cidr_bans.remove(item)
                        self._bans.pop(item[1].ip, None)
                    if matched:
                        self._total_hits += 1
                        return matched
            return None

    async def ban_ip(
        self,
        ip: str,
        reason: str = "",
        banned_by: str = "admin",
        duration_seconds: int = 0,
        permanent: bool = False,
        path_prefix: str = "",
    ) -> BanEntry:
        expire_at = 0.0
        if not permanent and duration_seconds > 0:
            expire_at = time.time() + duration_seconds
        elif permanent:
            expire_at = 0.0
            permanent = True
        else:
            permanent = True

        entry = BanEntry(
            ip=ip,
            reason=reason,
            banned_by=banned_by,
            banned_at=time.time(),
            expire_at=expire_at,
            permanent=permanent,
            path_prefix=path_prefix,
        )
        async with self._lock:
            self._bans[ip] = entry
            # 如果是 CIDR 网段，同时加入 CIDR 列表用于网段匹配
            if self._is_cidr(ip):
                net = self._parse_cidr(ip)
                if net is not None:
                    self._cidr_bans.append((net, entry))
            self._total_bans += 1
        logger.info(
            "IP已封禁: %s 原因=%s 操作者=%s 永久=%s 到期=%s 作用域=%s",
            ip, reason, banned_by, permanent,
            "永久" if permanent else f"{duration_seconds}秒后" if duration_seconds else "",
            path_prefix or "全局",
        )
        return entry

    async def unban_ip(self, ip: str) -> bool:
        async with self._lock:
            if ip in self._bans:
                del self._bans[ip]
                # 如果是 CIDR 网段，同时从 CIDR 列表移除
                if self._is_cidr(ip):
                    self._cidr_bans = [(n, e) for n, e in self._cidr_bans if e.ip != ip]
                logger.info("IP已解封: %s", ip)
                return True
            return False

    async def extend_ban(self, ip: str, duration_seconds: float) -> Optional[BanEntry]:
        """延长临时封禁时长。永久封禁或已过期则从当前时间起算，未过期则在原 expire_at 基础上累加。"""
        if duration_seconds <= 0:
            return None
        now = time.time()
        async with self._lock:
            entry = self._bans.get(ip)
            if entry is None:
                return None
            if entry.permanent or entry.expire_at <= 0 or entry.expire_at < now:
                new_expire = now + duration_seconds
            else:
                new_expire = entry.expire_at + duration_seconds
            entry.expire_at = new_expire
            entry.permanent = False
            logger.info(
                "IP封禁时长已延长: %s 延长 %s 秒 新到期=%s",
                ip, duration_seconds, new_expire,
            )
            return entry

    async def get_ban(self, ip: str) -> Optional[BanEntry]:
        async with self._lock:
            entry = self._bans.get(ip)
            if entry and self._is_expired(entry):
                del self._bans[ip]
                return None
            return entry

    async def list_bans(self, include_expired: bool = False) -> List[BanEntry]:
        async with self._lock:
            if include_expired:
                return list(self._bans.values())
            active = []
            expired_keys = []
            for ip, entry in self._bans.items():
                if self._is_expired(entry):
                    expired_keys.append(ip)
                else:
                    active.append(entry)
            for k in expired_keys:
                del self._bans[k]
            return active

    async def clear_all(self) -> int:
        async with self._lock:
            count = len(self._bans)
            self._bans.clear()
            self._cidr_bans.clear()
            return count

    async def cleanup_expired(self) -> int:
        async with self._lock:
            expired_keys = [ip for ip, e in self._bans.items() if self._is_expired(e)]
            for k in expired_keys:
                del self._bans[k]
            # 同步清理过期的 CIDR 条目
            if self._cidr_bans:
                self._cidr_bans = [(n, e) for n, e in self._cidr_bans if not self._is_expired(e)]
            return len(expired_keys)

    async def import_bans(self, bans: List[Dict]) -> int:
        count = 0
        for b in bans:
            ip = b.get("ip", "")
            if not ip:
                continue
            permanent = bool(b.get("permanent", True))
            expire_at = float(b.get("expire_at", 0))
            entry = BanEntry(
                ip=ip,
                reason=b.get("reason", ""),
                banned_by=b.get("banned_by", "import"),
                banned_at=float(b.get("banned_at", time.time())),
                expire_at=expire_at,
                permanent=permanent,
                path_prefix=str(b.get("path_prefix", "") or ""),
            )
            async with self._lock:
                self._bans[ip] = entry
                # 如果是 CIDR 网段，同时加入 CIDR 列表
                if self._is_cidr(ip):
                    net = self._parse_cidr(ip)
                    if net is not None:
                        self._cidr_bans.append((net, entry))
                count += 1
        return count

    def get_stats(self) -> dict:
        active_count = sum(1 for e in self._bans.values() if not self._is_expired(e))
        return {
            "active_bans": active_count,
            "total_entries": len(self._bans),
            "total_ban_operations": self._total_bans,
            "total_blocked_hits": self._total_hits,
        }
