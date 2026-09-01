from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from config import AutoBanConfig, EmailConfig
from email_notifier import EmailNotifier
from config_store import ConfigStore


logger = logging.getLogger("proxy.auto_ban")


@dataclass
class IpRequestStats:
    timestamps: List[float] = field(default_factory=list)
    error_404_count: int = 0
    last_404_time: float = 0.0


class AutoBanMonitor:
    def __init__(
        self,
        config: AutoBanConfig,
        ban_callback: Callable[[str, str], Any],
        email_config: Optional[EmailConfig] = None,
        config_store: Optional[ConfigStore] = None,
    ):
        self.config = config
        self.ban_callback = ban_callback
        self._email_config = email_config
        self._email_notifier: Optional[EmailNotifier] = EmailNotifier(email_config) if email_config else None
        self._config_store = config_store
        self._stats: Dict[str, IpRequestStats] = defaultdict(IpRequestStats)
        self._whitelist: Set[str] = set()
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._total_requests: int = 0
        self._total_bans: int = 0
        self._base_url: str = ""
        self._update_whitelist()
    
    def set_base_url(self, base_url: str) -> None:
        """设置服务器基础URL，用于生成封禁链接"""
        self._base_url = base_url.rstrip("/")

    def _update_whitelist(self) -> None:
        raw = self.config.whitelist or ""
        self._whitelist = {
            ip.strip() for ip in raw.split(",") if ip.strip()
        }

    def update_config(self, config: AutoBanConfig) -> None:
        self.config = config
        self._update_whitelist()

    def update_email_config(self, email_config: EmailConfig) -> None:
        """更新邮件配置"""
        self._email_config = email_config
        if self._email_notifier:
            self._email_notifier.update_config(email_config)
        else:
            self._email_notifier = EmailNotifier(email_config)

    def is_whitelisted(self, ip: str) -> bool:
        return ip in self._whitelist

    async def record_request(self, ip: str, status_code: int) -> None:
        if not self.config.enabled or self.is_whitelisted(ip):
            return
        async with self._lock:
            now = time.time()
            stats = self._stats[ip]
            stats.timestamps.append(now)
            self._total_requests += 1
            if status_code == 404 and self.config.auto_ban_on_404:
                stats.error_404_count += 1
                stats.last_404_time = now
            await self._check_and_ban(ip, stats, now)

    async def _check_and_ban(self, ip: str, stats: IpRequestStats, now: float) -> None:
        window_start = now - self.config.window_seconds
        stats.timestamps = [t for t in stats.timestamps if t > window_start]
        
        request_count = len(stats.timestamps)
        error_404_count = stats.error_404_count
        
        if request_count >= self.config.max_requests:
            await self._ban_ip(ip, f"请求频率超限: {request_count}次/{self.config.window_seconds}秒")
            return
        if self.config.auto_ban_on_404 and error_404_count >= self.config.max_404:
            await self._ban_ip(ip, f"404错误频率超限: {error_404_count}次/{self.config.window_seconds}秒")
            return
        
        await self._check_email_alert(ip, request_count, error_404_count, now)

    async def _check_email_alert(self, ip: str, request_count: int, error_404_count: int, now: float) -> None:
        """检查是否需要发送邮件提醒"""
        if not self._email_notifier or not self._email_notifier._config.enabled:
            return
        
        email_config = self._email_notifier._config
        
        if request_count >= email_config.alert_max_requests:
            block_link_url = self._generate_block_link(ip, "请求频率超限")
            await self._email_notifier.send_alert(
                ip=ip,
                alert_type="请求频率超限",
                current_count=request_count,
                threshold=email_config.alert_max_requests,
                window_seconds=email_config.alert_window_seconds,
                block_link_url=block_link_url,
            )
        
        if self.config.auto_ban_on_404 and error_404_count >= email_config.alert_max_404:
            block_link_url = self._generate_block_link(ip, "404错误频率超限")
            await self._email_notifier.send_alert(
                ip=ip,
                alert_type="404错误频率超限",
                current_count=error_404_count,
                threshold=email_config.alert_max_404,
                window_seconds=email_config.alert_window_seconds,
                block_link_url=block_link_url,
            )
    
    def _generate_block_link(self, ip: str, reason: str) -> Optional[str]:
        """生成封禁链接URL"""
        if not self._config_store:
            return None
        
        try:
            # 优先使用邮件配置中手动设置的域名，否则使用自动检测的base_url
            base_url = ""
            if self._email_notifier and self._email_notifier._config.block_link_base_url:
                base_url = self._email_notifier._config.block_link_base_url.rstrip("/")
            elif self._base_url:
                base_url = self._base_url
            
            if not base_url:
                return None
            
            # 生成token，30分钟有效期
            token = self._config_store.create_block_token(ip, reason, expires_in_seconds=1800)
            return f"{base_url}/_block/{token}"
        except Exception as e:
            logger.error("生成封禁链接失败: %s", e)
            return None

    async def _ban_ip(self, ip: str, reason: str) -> None:
        if ip in self._stats:
            del self._stats[ip]
        self._total_bans += 1
        logger.warning("自动封禁: IP=%s 原因=%s 时长=%d秒", ip, reason, self.config.ban_duration_seconds)
        if asyncio.iscoroutinefunction(self.ban_callback):
            await self.ban_callback(ip, reason)
        else:
            self.ban_callback(ip, reason)
        if self.config.email_on_ban and self._email_notifier and self._email_notifier._config.enabled:
            try:
                await self._email_notifier.send_ban_alert(
                    ip=ip,
                    reason=reason,
                    ban_duration_seconds=self.config.ban_duration_seconds,
                    current_count=0,
                    threshold=0,
                    window_seconds=self.config.window_seconds,
                )
            except Exception as exc:
                logger.error("自动封禁邮件提醒发送失败: %s", exc)

    async def start_cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(60)
                await self._cleanup_old_stats()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("自动封禁清理循环异常: %s", exc)

    async def _cleanup_old_stats(self) -> None:
        async with self._lock:
            now = time.time()
            cutoff = now - self.config.window_seconds * 2
            expired_ips = [
                ip for ip, stats in self._stats.items()
                if not stats.timestamps or max(stats.timestamps) < cutoff
            ]
            for ip in expired_ips:
                del self._stats[ip]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "enabled": self.config.enabled,
            "tracked_ips": len(self._stats),
            "whitelisted_ips": len(self._whitelist),
            "total_requests": self._total_requests,
            "total_bans": self._total_bans,
            "config": {
                "window_seconds": self.config.window_seconds,
                "max_requests": self.config.max_requests,
                "ban_duration_seconds": self.config.ban_duration_seconds,
                "max_404": self.config.max_404,
                "auto_ban_on_404": self.config.auto_ban_on_404,
            },
        }

    def get_tracked_ips(self) -> List[Dict[str, Any]]:
        result = []
        now = time.time()
        window_start = now - self.config.window_seconds
        for ip, stats in self._stats.items():
            recent = [t for t in stats.timestamps if t > window_start]
            result.append({
                "ip": ip,
                "request_count": len(recent),
                "error_404_count": stats.error_404_count,
            })
        result.sort(key=lambda x: x["request_count"], reverse=True)
        return result
