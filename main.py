"""
HTTP 反向代理服务器主模块

本模块实现了一个功能完整的 HTTP 反向代理服务器，支持以下核心功能：
1. 302 重定向跟踪 - 自动跟踪上游服务器的重定向响应
2. 流式传输 - 支持大文件和流媒体的边传边发
3. Range 请求 - 支持断点续传和部分内容请求
4. 健康检查 - 提供服务状态监控接口
5. 统计信息 - 提供请求统计和缓存统计接口

模块结构：
- format_bytes: 字节格式化工具函数
- ProxyServer: 代理服务器核心类
- main: 命令行入口函数

使用方式：
    python main.py -c config.yaml -p 8080

作者: nginx302_proxy 项目
"""

import asyncio
import argparse
import re
import signal
import sys
import time
from aiohttp import web
from typing import Dict, Any, Optional, Tuple
import json
import logging
from datetime import datetime, timezone

from config import Config, load_config, setup_logging, normalize_request_host, prune_app_log_files, set_request_id, get_request_id, generate_request_id
from admin_console import AdminConsole
from auto_ban_monitor import AutoBanMonitor
from config_store import ConfigStore
from geo_service import GeoResolver
from offline_geoip_sync import OfflineGeoIPSyncService
from proxy_core import ProxyRequestHandler, ProxyStats, StreamingResponse
from ip_result_cache import IpResultCache
from ip_ban_manager import IpBanManager

logger = logging.getLogger('proxy')


def format_bytes(bytes_value: int) -> str:
    if bytes_value >= 1024 * 1024 * 1024:
        return f"{bytes_value / 1024 / 1024 / 1024:.2f} GB"
    elif bytes_value >= 1024 * 1024:
        return f"{bytes_value / 1024 / 1024:.2f} MB"
    elif bytes_value >= 1024:
        return f"{bytes_value / 1024:.2f} KB"
    else:
        return f"{bytes_value} B"


class ProxyServer:
    def __init__(self, config: Config, config_store: ConfigStore):
        self.config = config
        self.config_store = config_store
        self.geo_resolver = GeoResolver()
        self.geo_resolver.set_online_cache_ttl_seconds(config.geoip.online_cache_ttl_seconds, reset_existing=True)
        self.offline_geoip_sync_service = OfflineGeoIPSyncService(
            config_store=self.config_store,
            geo_resolver=self.geo_resolver,
        )
        self.ip_result_cache = IpResultCache(
            enabled=config.ip_result_cache.enabled,
            ttl_seconds=config.ip_result_cache.ttl_seconds,
            max_entries=config.ip_result_cache.max_entries,
        )
        self.ip_ban_manager = IpBanManager()
        self.auto_ban_monitor = AutoBanMonitor(
            config=config.auto_ban,
            ban_callback=self._auto_ban_ip,
            email_config=config.email,
            config_store=self.config_store,
        )
        self.request_handler = ProxyRequestHandler(config, geo_resolver=self.geo_resolver, ip_cache=self.ip_result_cache, ip_ban_manager=self.ip_ban_manager)
        self.geo_resolver.set_session_provider(self.request_handler.get_session)
        self.stats = ProxyStats()
        self._ban_cleanup_task: Optional[asyncio.Task] = None
        self._auto_ban_cleanup_task: Optional[asyncio.Task] = None
        self._log_cleanup_task: Optional[asyncio.Task] = None
        self.admin_console = AdminConsole(
            config_store=self.config_store,
            reload_callback=self.reload_runtime_config,
            offline_sync_callback=self.sync_offline_geoip_now,
            offline_rollback_callback=self.rollback_offline_geoip_now,
            online_cache_clear_callback=self.clear_online_geoip_cache,
            ban_manager_callback=self._sync_ban_manager,
            log_cleanup_callback=self._cleanup_log_files,
            log_file_cleanup_callback=self._cleanup_log_files_on_disk,
        )
        self.app = web.Application(client_max_size=config.streaming.max_request_body_size)
        self._setup_routes()
        self._setup_middleware()
    
    def _setup_routes(self):
        self.app.router.add_get('/_health', self.health_check)
        self.app.router.add_get('/_admin/api/stats', self.get_stats)
        self.app.router.add_get('/_admin/api/ip-cache/stats', self.get_ip_cache_stats)
        self.app.router.add_post('/_admin/api/ip-cache/clear', self.clear_ip_cache)
        self.app.router.add_get('/_admin/api/ban/list', self.list_bans)
        self.app.router.add_post('/_admin/api/ban/add', self.ban_ip)
        self.app.router.add_post('/_admin/api/ban/remove', self.unban_ip)
        self.app.router.add_post('/_admin/api/ban/clear', self.clear_all_bans)
        self.app.router.add_get('/_admin/api/ban/stats', self.get_ban_stats)
        self.app.router.add_get('/_admin/api/auto-ban/stats', self.get_auto_ban_stats)
        self.app.router.add_get('/_admin/api/auto-ban/tracked', self.get_auto_ban_tracked)
        self.app.router.add_get('/_block/{token}', self.block_token_page)
        self.app.router.add_get('/_block/{token}/info', self.block_token_info)
        self.app.router.add_post('/_block/{token}/confirm', self.block_token_confirm)
        self.admin_console.register(self.app)
        self.app.router.add_route('*', '/{path:.*}', self.handle_proxy)
    
    async def check_admin_auth(self, request: web.Request) -> bool:
        config = self.admin_console._get_auth_config()
        if not config.enabled:
            return True
        return self.admin_console._is_authenticated(request)
    
    def _add_security_headers(self, response: web.Response) -> web.Response:
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        if self.config.ssl.enabled:
            response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        return response
    
    def _setup_middleware(self):
        @web.middleware
        async def logging_middleware(request: web.Request, handler):
            request_id = generate_request_id()
            token = set_request_id(request_id)
            request["_request_id"] = request_id
            start_time = time.perf_counter()
            client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote or ""
            user_agent = request.headers.get("User-Agent", "")
            full_url = request.path
            if request.query_string:
                full_url += "?" + request.query_string
            try:
                response = await handler(request)
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    "%s %s %d %.1fms %s",
                    request.method, full_url, response.status, duration_ms, client_ip,
                )
                logger.debug("UA=%s", user_agent[:120] if user_agent else "-")
                if client_ip:
                    # 排除内部接口，不进行自动封禁监控
                    if not request.path.startswith("/_"):
                        await self.auto_ban_monitor.record_request(client_ip, response.status)
                return response
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.error(
                    "%s %s ERROR:%s %.1fms %s",
                    request.method, full_url, e, duration_ms, client_ip,
                )
                raise
            finally:
                set_request_id("-")
                _ = token
        
        self.app.middlewares.append(logging_middleware)

    async def reload_runtime_config(self) -> None:
        self.config = self.config_store.load_runtime_config()
        self.geo_resolver.set_online_cache_ttl_seconds(self.config.geoip.online_cache_ttl_seconds, reset_existing=True)
        self.request_handler.update_config(self.config)
        cache_cfg = self.config.ip_result_cache
        if self.ip_result_cache.enabled != cache_cfg.enabled or \
           self.ip_result_cache.ttl_seconds != cache_cfg.ttl_seconds or \
           self.ip_result_cache.max_entries != cache_cfg.max_entries:
            self.ip_result_cache = IpResultCache(
                enabled=cache_cfg.enabled,
                ttl_seconds=cache_cfg.ttl_seconds,
                max_entries=cache_cfg.max_entries,
            )
            self.request_handler.set_ip_cache(self.ip_result_cache)
            logger.info("请求结果缓存配置已更新: enabled=%s ttl=%ds max=%d", cache_cfg.enabled, cache_cfg.ttl_seconds, cache_cfg.max_entries)
        self.auto_ban_monitor.update_config(self.config.auto_ban)
        self.auto_ban_monitor.update_email_config(self.config.email)

    async def sync_offline_geoip_now(self) -> Dict[str, Any]:
        return await self.offline_geoip_sync_service.sync_now(force=True)

    async def rollback_offline_geoip_now(self) -> Dict[str, Any]:
        return await self.offline_geoip_sync_service.rollback_now()

    async def clear_online_geoip_cache(self) -> Dict[str, Any]:
        summary_before = self.geo_resolver.get_online_cache_summary()
        result = self.geo_resolver.clear_online_cache()
        return {
            "message": f"已清除 {result.get('cleared_count', 0)} 条在线定位缓存。",
            "cleared_count": result.get('cleared_count', 0),
            "ttl_seconds": summary_before.get("ttl_seconds", self.config.geoip.online_cache_ttl_seconds),
        }

    async def _sync_ban_manager(self, action: str, payload: Dict[str, Any]) -> None:
        if action == "ban":
            ip = payload.get("ip", "")
            reason = payload.get("reason", "")
            banned_by = payload.get("banned_by", "admin")
            duration_seconds = int(payload.get("duration_seconds", 0) or 0)
            permanent = bool(payload.get("permanent", True))
            path_prefix = str(payload.get("path_prefix", "") or "").strip()
            await self.ip_ban_manager.ban_ip(
                ip=ip, reason=reason, banned_by=banned_by,
                duration_seconds=duration_seconds, permanent=permanent,
                path_prefix=path_prefix,
            )
        elif action == "unban":
            ip = payload.get("ip", "")
            await self.ip_ban_manager.unban_ip(ip)
        elif action == "extend":
            ip = payload.get("ip", "")
            duration_seconds = float(payload.get("duration_seconds", 0) or 0)
            await self.ip_ban_manager.extend_ban(ip, duration_seconds)
        elif action == "clear":
            await self.ip_ban_manager.clear_all()

    async def _auto_ban_ip(self, ip: str, reason: str) -> None:
        await self.ip_ban_manager.ban_ip(
            ip=ip,
            reason=f"[自动封禁] {reason}",
            banned_by="auto_ban_monitor",
            duration_seconds=self.config.auto_ban.ban_duration_seconds,
            permanent=False,
            path_prefix="",
        )

    def _request_duration_ms(self, request: web.Request) -> int:
        started_at = request.get("_route_log_started_at")
        if started_at is None:
            return 0
        return max(0, int((time.perf_counter() - float(started_at)) * 1000))

    async def _render_403_page(self, reason: str) -> web.Response:
        """渲染 403 错误页面"""
        from pathlib import Path
        
        # 读取 403.html 模板
        static_dir = Path(__file__).parent / "static"
        error_page_path = static_dir / "403.html"
        
        html_content = ""
        try:
            if error_page_path.exists():
                html_content = error_page_path.read_text(encoding="utf-8")
                # 在页面中注入原因（通过修改 hidden 属性）
                html_content = html_content.replace(
                    'id="error-reason-container" hidden>',
                    'id="error-reason-container">'
                )
                html_content = html_content.replace(
                    '<div class="error-reason-text" id="error-reason-text">-</div>',
                    f'<div class="error-reason-text" id="error-reason-text">{reason}</div>'
                )
            else:
                # 回退到简单的 HTML
                html_content = self._fallback_403_html(reason)
        except Exception as e:
            logger.error("读取 403 页面失败: %s", e)
            html_content = self._fallback_403_html(reason)
        
        return web.Response(
            text=html_content,
            status=403,
            content_type="text/html",
            charset="utf-8",
        )
    
    def _fallback_403_html(self, reason: str) -> str:
        """生成回退的 403 HTML"""
        import html
        safe_reason = html.escape(reason)
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>403 - 访问受限</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           display: flex; align-items: center; justify-content: center;
           min-height: 100vh; margin: 0; background: #f8fafc; color: #1e293b; }}
    .container {{ text-align: center; padding: 2rem; max-width: 480px; }}
    .code {{ font-size: 4rem; font-weight: 700; color: #dc2626; }}
    .title {{ font-size: 1.5rem; margin: 1rem 0; }}
    .reason {{ background: #fee2e2; padding: 1rem; border-radius: 8px; margin: 1rem 0;
               text-align: left; font-size: 0.875rem; }}
    a {{ color: #2563eb; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="code">403</div>
    <h1 class="title">访问受限</h1>
    <p>您没有权限访问此资源。</p>
    <div class="reason"><strong>原因：</strong>{safe_reason}</div>
    <p><a href="/">返回首页</a></p>
  </div>
</body>
</html>"""

    async def _render_404_page(self, reason: str = "") -> web.Response:
        """渲染 404 错误页面"""
        from pathlib import Path

        static_dir = Path(__file__).parent / "static"
        error_page_path = static_dir / "404.html"

        html_content = ""
        try:
            if error_page_path.exists():
                html_content = error_page_path.read_text(encoding="utf-8")
                html_content = html_content.replace(
                    'id="error-reason-container" hidden>',
                    'id="error-reason-container">'
                )
                html_content = html_content.replace(
                    '<div class="error-reason-text" id="error-reason-text">-</div>',
                    f'<div class="error-reason-text" id="error-reason-text">{reason}</div>'
                )
            else:
                html_content = self._fallback_404_html(reason)
        except Exception as e:
            logger.error("读取 404 页面失败: %s", e)
            html_content = self._fallback_404_html(reason)

        return web.Response(
            text=html_content,
            status=404,
            content_type="text/html",
            charset="utf-8",
        )

    def _fallback_404_html(self, reason: str) -> str:
        """生成回退的 404 HTML"""
        import html
        safe_reason = html.escape(reason) if reason else "未找到匹配的代理规则"
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>404 - 未找到匹配规则</title>
  <style>
    body {{ font-family: -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: #f8fafc; color: #0f172a; }}
    .card {{ background: #fff; border-radius: 12px; padding: 48px; box-shadow: 0 4px 24px rgba(0,0,0,0.08); text-align: center; max-width: 480px; }}
    h1 {{ font-size: 72px; margin: 0; color: #2563eb; }}
    p {{ color: #64748b; margin: 16px 0 0; }}
    a {{ color: #2563eb; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>404</h1>
    <p>{safe_reason}</p>
    <p><a href="/">返回首页</a></p>
  </div>
</body>
</html>"""

    async def _render_500_page(self, reason: str = "") -> web.Response:
        """渲染 500 错误页面，隐藏内部异常细节，仅展示通用原因分类"""
        from pathlib import Path

        static_dir = Path(__file__).parent / "static"
        error_page_path = static_dir / "500.html"

        html_content = ""
        try:
            if error_page_path.exists():
                html_content = error_page_path.read_text(encoding="utf-8")
                if reason:
                    html_content = html_content.replace(
                        'id="error-reason-container" hidden>',
                        'id="error-reason-container">'
                    )
                    html_content = html_content.replace(
                        '<div class="error-reason-text" id="error-reason-text">-</div>',
                        f'<div class="error-reason-text" id="error-reason-text">{reason}</div>'
                    )
            else:
                html_content = self._fallback_500_html(reason)
        except Exception as e:
            logger.error("读取 500 页面失败: %s", e)
            html_content = self._fallback_500_html(reason)

        return web.Response(
            text=html_content,
            status=500,
            content_type="text/html",
            charset="utf-8",
        )

    def _fallback_500_html(self, reason: str = "") -> str:
        """生成回退的 500 HTML"""
        import html
        safe_reason = html.escape(reason) if reason else "服务异常"
        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>500 - 服务异常</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           display: flex; align-items: center; justify-content: center;
           min-height: 100vh; margin: 0; background: #f8fafc; color: #1e293b; }}
    .container {{ text-align: center; padding: 2rem; max-width: 480px; }}
    .code {{ font-size: 4rem; font-weight: 700; color: #d97706; }}
    .title {{ font-size: 1.5rem; margin: 1rem 0; }}
    .reason {{ background: #fef3c7; padding: 1rem; border-radius: 8px; margin: 1rem 0;
               text-align: left; font-size: 0.875rem; }}
    a {{ color: #2563eb; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="code">500</div>
    <h1 class="title">服务异常</h1>
    <p>代理服务在处理请求时遇到问题。</p>
    <div class="reason"><strong>类型：</strong>{safe_reason}</div>
    <p><a href="/">返回首页</a></p>
  </div>
</body>
</html>"""

    @staticmethod
    def _classify_proxy_error(error: Exception) -> str:
        """将内部异常分类为用户可见的通用原因，避免暴露实现细节"""
        error_type = type(error).__name__
        error_msg = str(error).lower()

        if "timeout" in error_type.lower() or "timeout" in error_msg:
            return "请求超时"
        if "redirect" in error_msg or "redirect" in error_type.lower():
            return "重定向次数超限"
        if "connection" in error_type.lower() or "connection" in error_msg:
            return "上游连接异常"
        if "client" in error_type.lower() or "client" in error_msg:
            return "上游请求异常"
        return "代理服务异常"

    def _infer_route_log_result_status(self, *, route_decision, upstream_status: int, cache_status: str, error_message: str = "") -> str:
        if error_message:
            return "proxy_error"
        if route_decision is None:
            return "no_route"
        if upstream_status >= 500:
            return "upstream_error"
        if upstream_status >= 400:
            return "forwarded_client_error"
        return "forwarded"

    @staticmethod
    def _extract_redirect_location(redirect_info) -> str:
        if not redirect_info:
            return ""
        if redirect_info.redirect_chain:
            return redirect_info.redirect_chain[-1].get("from", "")
        if redirect_info.redirect_count > 0:
            return redirect_info.redirect_url
        if redirect_info.status_code in {301, 302, 303, 307, 308}:
            return redirect_info.redirect_url
        return ""

    def _record_route_log(self, request: web.Request, *, route_decision=None, upstream_status: int = 0, cache_status: str = "", redirect_info=None, transport_mode: str = "", error_message: str = "") -> None:
        try:
            geo_location = route_decision.geo_location if route_decision else None
            geo_source = geo_location.source if geo_location else ""
            request_headers = {str(key): str(value) for key, value in request.headers.items()}
            request_host = (
                route_decision.request_host
                if route_decision
                else self.request_handler.extract_request_host(request_headers)
            )
            if not request_host:
                request_host = normalize_request_host(request.host or "")
            if (
                geo_location
                and geo_location.online_cache_hit
                and str(geo_source).startswith("online:")
            ):
                geo_source = f"{geo_source}|cache_hit"
            payload = {
                "request_id": request.get("_request_id", ""),
                "request_method": request.method,
                "request_path": request.path,
                "request_query_string": request.query_string,
                "request_host": request_host,
                "path_prefix": route_decision.rule.path_prefix if route_decision else "",
                "rule_id": route_decision.rule.rule_id if route_decision else None,
                "rule_name": route_decision.rule.name if route_decision else "",
                "rule_request_host": (
                    route_decision.rule_request_host
                    if route_decision
                    else ""
                ),
                "rule_source": route_decision.rule.source if route_decision else "",
                "target_url": route_decision.target_url if route_decision else "",
                "original_client_ip": request.remote or "",
                "client_ip": route_decision.client_ip if route_decision else (request.remote or ""),
                "region_matching_enabled": route_decision.region_matching_enabled if route_decision else False,
                "geo_source": geo_source,
                "geo_summary": geo_location.summary if geo_location else "",
                "geo_country": geo_location.country if geo_location else "",
                "geo_region": geo_location.region if geo_location else "",
                "geo_city": geo_location.city if geo_location else "",
                "configured_ip_whitelist": route_decision.rule.ip_whitelist if route_decision else "",
                "matched_ip_whitelist": route_decision.matched_ip_whitelist if route_decision else "",
                "configured_regions": route_decision.rule.region_filters if route_decision else "",
                "matched_region": route_decision.matched_region if route_decision else "",
                "match_strategy": route_decision.match_strategy if route_decision else "no_route",
                "match_detail": route_decision.match_detail if route_decision else "no_matching_rule_found",
                "upstream_status": upstream_status,
                "cache_status": cache_status,
                "redirect_count": redirect_info.redirect_count if redirect_info else 0,
                "redirect_location": self._extract_redirect_location(redirect_info),
                "transport_mode": transport_mode,
                "operation_duration_ms": self._request_duration_ms(request),
                "result_status": self._infer_route_log_result_status(
                    route_decision=route_decision,
                    upstream_status=upstream_status,
                    cache_status=cache_status,
                    error_message=error_message,
                ),
                "error_message": error_message,
                "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            self.config_store.insert_route_log(payload)
        except Exception as exc:
            logger.warning("记录路由日志失败: %s", exc)

    def should_use_streaming(self, headers: Dict[str, str], content_length: int = None) -> bool:
        if not self.config.streaming.enabled:
            return False
        
        content_type = headers.get('Content-Type', '').lower()
        
        streaming_types = [
            'video/',
            'audio/',
            'application/octet-stream',
            'application/x-mpegurl',
            'application/vnd.apple.mpegurl',
            'application/dash+xml',
            'multipart/'
        ]
        
        for stream_type in streaming_types:
            if stream_type in content_type:
                return True
        
        transfer_encoding = headers.get('Transfer-Encoding', '').lower()
        if 'chunked' in transfer_encoding:
            return True
        
        if content_length is not None and content_length > self.config.streaming.large_file_threshold:
            return True
        
        return False
    
    def _parse_content_range(self, content_range: Optional[str]) -> Optional[Tuple[int, int, int]]:
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

    async def handle_proxy(self, request: web.Request) -> web.StreamResponse:
        route_decision = None
        try:
            request["_route_log_started_at"] = time.perf_counter()
            method = request.method
            raw_path = request.raw_path
            query_string = request.query_string
            path_decoded = request.path
            headers = dict(request.headers)
            body = await request.read()
            client_host = request.remote or ''
            scheme = request.scheme

            logger.debug("路由匹配开始: %s %s", method, raw_path)

            route_decision = await self.request_handler.select_route(
                path_decoded,
                headers,
                client_host,
                query_string if query_string else None,
            )

            if route_decision:
                logger.debug(
                    "路由匹配完成: 规则=%s 目标=%s 匹配=%s",
                    route_decision.rule.path_prefix, route_decision.target_url,
                    route_decision.match_strategy,
                )
            else:
                logger.warning("无匹配路由: %s %s", method, raw_path)
                return await self._render_404_page("未找到匹配的代理规则")

            # 黑白名单拦截检查：命中黑名单或不在白名单内时跳转到 403 页面
            if route_decision and route_decision.blocked:
                block_reason = route_decision.block_reason or "请求被访问控制规则拦截"
                logger.warning(
                    "黑白名单拦截: IP=%s 路径=%s 策略=%s 原因=%s",
                    route_decision.client_ip, path_decoded,
                    route_decision.match_strategy, block_reason,
                )
                self._record_route_log(
                    request,
                    route_decision=route_decision,
                    upstream_status=403,
                    cache_status="BLOCKED",
                    transport_mode="none",
                    error_message=block_reason,
                )
                # 返回 403 页面
                return await self._render_403_page(block_reason)

            # IP 封禁检查：被封禁的 IP 直接返回 403 页面
            if route_decision:
                ban_entry = await self.ip_ban_manager.is_banned(
                    route_decision.client_ip, path_decoded
                )
                if ban_entry:
                    ban_reason = f"IP已被封禁: {route_decision.client_ip}"
                    if ban_entry.reason:
                        ban_reason += f" 原因: {ban_entry.reason}"
                    scope = ban_entry.path_prefix or "全局"
                    logger.warning(
                        "IP封禁拦截: IP=%s 路径=%s 作用域=%s 原因=%s",
                        route_decision.client_ip, path_decoded, scope,
                        ban_entry.reason or "未指定",
                    )
                    self._record_route_log(
                        request,
                        route_decision=route_decision,
                        upstream_status=403,
                        cache_status="BANNED",
                        transport_mode="none",
                        error_message=ban_reason,
                    )
                    return await self._render_403_page(ban_reason)

            target_url = route_decision.target_url if route_decision else None
            use_streaming_mode = bool(
                route_decision and route_decision.rule.enable_streaming and self.config.streaming.enabled
            )
            
            range_header = headers.get('Range')

            logger.debug(
                "转发决策: 目标=%s 模式=%s Range=%s",
                target_url, "streaming" if use_streaming_mode else "standard",
                range_header or "-",
            )
            
            # 直接转发请求到上游服务器
            if use_streaming_mode:
                streaming_response = await self.request_handler.handle_request_streaming(
                    method=method,
                    path=path_decoded,
                    headers=headers,
                    body=body if body else None,
                    client_host=client_host,
                    scheme=scheme,
                    query_string=query_string if query_string else None,
                    route_decision=route_decision,
                )
                actual_cache_status = streaming_response.cache_status or "BYPASS"
                logger.debug("流式响应: 状态=%s 缓存=%s", streaming_response.status, actual_cache_status)
                return await self._send_streaming_response(request, streaming_response, cache_status=actual_cache_status)
            else:
                status, response_headers, response_body, redirect_info, route_decision = await self.request_handler.handle_request(
                    method=method,
                    path=path_decoded,
                    headers=headers,
                    body=body if body else None,
                    client_host=client_host,
                    scheme=scheme,
                    query_string=query_string if query_string else None,
                    route_decision=route_decision,
                )

                redirect_count = redirect_info.redirect_count if redirect_info else 0
                cache_status = self.request_handler.get_last_cache_status()
                logger.debug(
                    "标准响应: 状态=%d 缓存=%s 重定向=%d",
                    status, cache_status, redirect_count,
                )
                
                await self.stats.record_request(
                    redirected=redirect_info is not None and redirect_info.redirect_count > 0,
                    redirect_count=redirect_info.redirect_count if redirect_info else 0,
                    failed=status >= 400
                )
                self._record_route_log(
                    request,
                    route_decision=route_decision,
                    upstream_status=status,
                    cache_status=self.request_handler.get_last_cache_status(),
                    redirect_info=redirect_info,
                    transport_mode="standard",
                )
                
                if redirect_info and redirect_info.redirect_count > 0:
                    response_headers['X-Redirect-Count'] = str(redirect_info.redirect_count)
                    response_headers['X-Original-URL'] = redirect_info.original_url
                    response_headers['X-Final-URL'] = redirect_info.redirect_url

                return web.Response(
                    status=status,
                    headers=response_headers,
                    body=response_body
                )
        
        except Exception as e:
            logger.exception(f"处理请求时发生错误: {e}")
            await self.stats.record_request(failed=True)
            self._record_route_log(
                request,
                route_decision=route_decision,
                upstream_status=500,
                cache_status="",
                transport_mode="streaming" if request.get("_route_log_started_at") and route_decision and route_decision.rule.enable_streaming else "standard",
                error_message=str(e),
            )
            # 返回 500 错误页面，仅展示通用原因分类，不暴露内部异常细节
            return await self._render_500_page(self._classify_proxy_error(e))
    
    async def _send_streaming_response(self, request: web.Request, streaming_response: StreamingResponse, cache_status: str = "BYPASS") -> web.StreamResponse:
        redirect_info = streaming_response.redirect_info
        response_headers = streaming_response.headers
        
        await self.stats.record_request(
            redirected=redirect_info is not None and redirect_info.redirect_count > 0,
            redirect_count=redirect_info.redirect_count if redirect_info else 0,
            failed=streaming_response.status >= 400,
            streaming=True
        )
        
        if redirect_info and redirect_info.redirect_count > 0:
            response_headers['X-Redirect-Count'] = str(redirect_info.redirect_count)
            response_headers['X-Original-URL'] = redirect_info.original_url
            response_headers['X-Final-URL'] = redirect_info.redirect_url
        
        response = web.StreamResponse(
            status=streaming_response.status,
            headers=response_headers
        )
        
        await response.prepare(request)
        
        bytes_transferred = 0
        try:
            async for chunk in streaming_response.body_stream:
                try:
                    await response.write(chunk)
                    bytes_transferred += len(chunk)
                except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
                    logger.info(f"客户端断开连接: {type(e).__name__}, 已传输 {format_bytes(bytes_transferred)}")
                    break
                except asyncio.CancelledError:
                    logger.info(f"流传输被客户端取消, 已传输 {format_bytes(bytes_transferred)}")
                    raise
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
            logger.info(f"客户端连接丢失: {type(e).__name__}, 已传输 {format_bytes(bytes_transferred)}")
        except asyncio.CancelledError:
            logger.info(f"流传输被取消, 已传输 {format_bytes(bytes_transferred)}")
            raise
        except Exception as e:
            error_msg = str(e)
            if 'Cannot write to closing transport' in error_msg or 'Connection reset' in error_msg:
                logger.info(f"客户端断开连接, 已传输 {format_bytes(bytes_transferred)}")
            elif 'ContentLengthError' in error_msg or 'payload is not completed' in error_msg:
                logger.warning(f"上游服务器 Content-Length 不匹配，已传输 {format_bytes(bytes_transferred)}，错误：{e}")
            else:
                logger.error(f"流传输响应时发生错误：{e}")
                raise
        finally:
            await self.stats.record_request(bytes_count=bytes_transferred)
            self._record_route_log(
                request,
                route_decision=streaming_response.route_decision,
                upstream_status=streaming_response.status,
                cache_status=cache_status,
                redirect_info=redirect_info,
                transport_mode="streaming",
            )
        
        return response
    
    async def health_check(self, request: web.Request) -> web.Response:
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                'status': 'healthy',
                'timestamp': int(time.time()),
                'route_group_count': len(self.config.route_groups),
                'rule_count': len(self.config.proxy_rules)
            }, ensure_ascii=False).encode()
        ))
    
    async def get_stats(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        stats = self.stats.get_stats()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(stats).encode()
        ))

    async def get_ip_cache_stats(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        stats = self.ip_result_cache.get_stats()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(stats, ensure_ascii=False).encode()
        ))

    async def clear_ip_cache(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        count = await self.ip_result_cache.clear()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({"message": f"已清除 {count} 条请求结果缓存"}, ensure_ascii=False).encode()
        ))

    async def list_bans(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        bans = await self.ip_ban_manager.list_bans()
        items = []
        for entry in bans:
            items.append({
                "ip": entry.ip,
                "reason": entry.reason,
                "banned_by": entry.banned_by,
                "banned_at": entry.banned_at,
                "expire_at": entry.expire_at,
                "permanent": entry.permanent,
            })
        stats = self.ip_ban_manager.get_stats()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({"items": items, "stats": stats}, ensure_ascii=False).encode()
        ))

    async def ban_ip(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        payload = await request.json()
        ip = str(payload.get("ip", "")).strip()
        reason = str(payload.get("reason", "")).strip()
        banned_by = str(payload.get("banned_by", "admin")).strip()
        duration_seconds = int(payload.get("duration_seconds", 0) or 0)
        permanent = bool(payload.get("permanent", True))
        if not ip:
            return self._add_security_headers(web.Response(
                status=400,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({"error": "IP地址不能为空"}, ensure_ascii=False).encode()
            ))
        entry = await self.ip_ban_manager.ban_ip(
            ip=ip, reason=reason, banned_by=banned_by,
            duration_seconds=duration_seconds, permanent=permanent,
        )
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                "message": f"IP {ip} 已封禁",
                "ip": entry.ip,
                "permanent": entry.permanent,
                "expire_at": entry.expire_at,
            }, ensure_ascii=False).encode()
        ))

    async def unban_ip(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        payload = await request.json()
        ip = str(payload.get("ip", "")).strip()
        if not ip:
            return self._add_security_headers(web.Response(
                status=400,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({"error": "IP地址不能为空"}, ensure_ascii=False).encode()
            ))
        removed = await self.ip_ban_manager.unban_ip(ip)
        if not removed:
            return self._add_security_headers(web.Response(
                status=404,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({"error": f"IP {ip} 不在封禁列表中"}, ensure_ascii=False).encode()
            ))
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({"message": f"IP {ip} 已解封"}, ensure_ascii=False).encode()
        ))

    async def clear_all_bans(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        count = await self.ip_ban_manager.clear_all()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({"message": f"已清除 {count} 条封禁记录"}, ensure_ascii=False).encode()
        ))

    async def get_ban_stats(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        stats = self.ip_ban_manager.get_stats()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(stats, ensure_ascii=False).encode()
        ))

    async def get_auto_ban_stats(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        stats = self.auto_ban_monitor.get_stats()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(stats, ensure_ascii=False).encode()
        ))

    async def get_auto_ban_tracked(self, request: web.Request) -> web.Response:
        if not await self.check_admin_auth(request):
            return self._add_security_headers(web.Response(status=401, headers={'Content-Type': 'application/json'}, body=json.dumps({"error": "未授权"}).encode()))
        tracked = self.auto_ban_monitor.get_tracked_ips()
        return self._add_security_headers(web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({"items": tracked}, ensure_ascii=False).encode()
        ))
    
    async def block_token_page(self, request: web.Request) -> web.Response:
        """显示封禁确认页面"""
        from pathlib import Path
        token = request.match_info.get('token', '')
        
        static_dir = Path(__file__).parent / "static"
        page_path = static_dir / "confirm_block.html"
        
        if page_path.exists():
            html_content = page_path.read_text(encoding="utf-8")
            return web.Response(
                text=html_content,
                content_type="text/html",
                charset="utf-8",
            )
        
        return web.Response(
            status=404,
            text="页面未找到",
            content_type="text/plain",
        )
    
    async def block_token_info(self, request: web.Request) -> web.Response:
        """获取token信息（IP和原因）"""
        import json as json_module
        token = request.match_info.get('token', '')
        
        info = self.config_store.validate_block_token(token)
        if not info:
            return web.Response(
                status=400,
                headers={'Content-Type': 'application/json'},
                body=json_module.dumps({
                    "ok": False,
                    "error": "链接无效或已过期"
                }, ensure_ascii=False).encode()
            )
        
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json_module.dumps({
                "ok": True,
                "ip": info["ip"],
                "reason": info["reason"],
            }, ensure_ascii=False).encode()
        )
    
    async def block_token_confirm(self, request: web.Request) -> web.Response:
        """确认封禁IP"""
        import json as json_module
        token = request.match_info.get('token', '')
        
        info = self.config_store.validate_block_token(token)
        if not info:
            return web.Response(
                status=400,
                headers={'Content-Type': 'application/json'},
                body=json_module.dumps({
                    "ok": False,
                    "error": "链接无效或已过期"
                }, ensure_ascii=False).encode()
            )
        
        # 标记token为已使用
        if not self.config_store.use_block_token(token):
            return web.Response(
                status=400,
                headers={'Content-Type': 'application/json'},
                body=json_module.dumps({
                    "ok": False,
                    "error": "链接已被使用"
                }, ensure_ascii=False).encode()
            )
        
        # 执行封禁
        ip = info["ip"]
        reason = info["reason"]
        await self.ip_ban_manager.ban_ip(
            ip=ip,
            reason=f"邮件链接封禁: {reason}",
            banned_by="email_link",
            duration_seconds=0,
            permanent=True,
        )
        
        logger.info(f"通过邮件链接封禁IP: {ip}, 原因: {reason}")
        
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json_module.dumps({
                "ok": True,
                "message": f"IP {ip} 已成功封禁"
            }, ensure_ascii=False).encode()
        )
    
    async def on_startup(self, app: web.Application):
        logger.info(f"代理服务器启动于 {self.config.server.host}:{self.config.server.port}")
        logger.info(f"代理规则数量: {len(self.config.proxy_rules)}")
        logger.info(f"路径前缀分组数量: {len(self.config.route_groups)}")
        logger.info(
            f"启用地区匹配的分组数量: {sum(1 for group in self.config.route_groups if group.region_matching_enabled)}"
        )
        pruned_logs = self.config_store.prune_route_logs(force=True)
        logger.info(
            f"规则日志保留天数: {self.config_store.get_route_log_settings().get('retention_days', 30)}, "
            f"启动时清理过期日志数量: {pruned_logs.get('deleted_count', 0)}"
        )
        self.offline_geoip_sync_service.start()
        
        logger.info(f"流式传输已启用: {self.config.streaming.enabled}")
        if self.config.streaming.enabled:
            logger.info(f"  块大小: {format_bytes(self.config.streaming.chunk_size)}")
            logger.info(f"  大文件阈值: {format_bytes(self.config.streaming.large_file_threshold)}")
            logger.info(f"  流超时: {self.config.streaming.stream_timeout} 秒")
            logger.info(f"  读取超时: {self.config.streaming.read_timeout} 秒")
            logger.info(f"  写入超时: {self.config.streaming.write_timeout} 秒")
            logger.info(f"  缓冲区大小: {format_bytes(self.config.streaming.buffer_size)}")
            logger.info(f"  范围支持: {'启用' if self.config.streaming.enable_range_support else '禁用'}")
            logger.info(f"  最大请求体大小: {format_bytes(self.config.streaming.max_request_body_size) if self.config.streaming.max_request_body_size else '无限制'}")
        
        for rule in self.config.proxy_rules:
            logger.info(f"  {rule.path_prefix} -> {rule.target_url} (流式: {rule.enable_streaming})")

        logger.info(
            f"请求结果缓存: {'启用' if self.ip_result_cache.enabled else '禁用'}, "
            f"TTL={self.ip_result_cache.ttl_seconds}秒, "
            f"最大条目={self.ip_result_cache.max_entries}"
        )
        
        # 设置封禁链接的基础URL
        host = self.config.server.host
        if host == "0.0.0.0":
            host = "127.0.0.1"
        base_url = f"http://{host}:{self.config.server.port}"
        self.auto_ban_monitor.set_base_url(base_url)
        logger.info(f"封禁链接基础URL: {base_url}")
        
        await self._load_bans_from_db()
        self._start_ban_cleanup_task()
        self._start_auto_ban_cleanup_task()
        self._start_log_cleanup_task()
        logger.info("IP封禁管理器已初始化")

    async def _load_bans_from_db(self) -> None:
        """启动时从数据库加载封禁记录到内存，并清理已过期记录。"""
        try:
            expired_count = self.config_store.cleanup_expired_bans()
            if expired_count > 0:
                logger.info(f"启动时清理过期IP封禁记录: {expired_count} 条")
            bans = self.config_store.list_banned_ips()
            if not bans:
                logger.info("IP封禁列表为空，无需加载")
                return
            imported = await self.ip_ban_manager.import_bans(bans)
            logger.info(f"已从数据库加载 {imported} 条IP封禁记录到内存")
        except Exception as exc:
            logger.warning(f"加载IP封禁记录失败: {exc}")

    def _start_ban_cleanup_task(self) -> None:
        """启动定时任务，定期清理内存和数据库中过期的临时封禁。"""
        if self._ban_cleanup_task is None or self._ban_cleanup_task.done():
            self._ban_cleanup_task = asyncio.create_task(
                self._ban_cleanup_loop(), name="ip-ban-cleanup"
            )

    def _start_auto_ban_cleanup_task(self) -> None:
        """启动自动封禁监控器的清理任务。"""
        if self._auto_ban_cleanup_task is None or self._auto_ban_cleanup_task.done():
            self._auto_ban_cleanup_task = asyncio.create_task(
                self.auto_ban_monitor.start_cleanup_loop(), name="auto-ban-cleanup"
            )

    async def _ban_cleanup_loop(self) -> None:
        """每60秒清理一次过期的临时封禁记录（内存 + 数据库）。"""
        while True:
            try:
                await asyncio.sleep(60)
                in_memory = await self.ip_ban_manager.cleanup_expired()
                in_db = self.config_store.cleanup_expired_bans()
                if in_memory > 0 or in_db > 0:
                    logger.info(
                        f"清理过期IP封禁: 内存 {in_memory} 条, 数据库 {in_db} 条"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"清理过期IP封禁任务异常: {exc}")

    def _start_log_cleanup_task(self) -> None:
        """启动定时任务，定期清理过期的运行日志文件。"""
        if self._log_cleanup_task is None or self._log_cleanup_task.done():
            self._log_cleanup_task = asyncio.create_task(
                self._log_cleanup_loop(), name="log-file-cleanup"
            )

    async def _log_cleanup_loop(self) -> None:
        """每小时检查并清理超过保留天数的运行日志文件。"""
        while True:
            try:
                await asyncio.sleep(3600)
                self._prune_log_files()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(f"清理过期日志文件任务异常: {exc}")

    def _prune_log_files(self) -> int:
        """清理超过保留天数的运行日志文件，返回删除的文件数。"""
        from config import prune_app_log_files
        if not self.config.logging.file_path:
            return 0
        log_path = Path(self.config.logging.file_path)
        log_dir = log_path.parent
        log_name = log_path.name
        import glob as glob_module
        import time
        pattern = str(log_dir / f"{log_name}.*-20*")
        cutoff = time.time() - self.config.logging.retention_days * 86400
        deleted = 0
        for f in glob_module.glob(pattern):
            try:
                if Path(f).stat().st_mtime < cutoff:
                    Path(f).unlink()
                    deleted += 1
            except OSError:
                pass
        if deleted > 0:
            logger.info(f"定时清理过期日志文件: 删除 {deleted} 个文件")
        return deleted

    async def _cleanup_log_files(self) -> Dict[str, Any]:
        """手动触发规则转发日志清理，供管理后台调用。"""
        result = self.config_store.prune_route_logs(force=True)
        return result

    async def _cleanup_log_files_on_disk(self) -> Dict[str, Any]:
        """手动触发运行日志文件清理，供管理后台调用。"""
        deleted = self._prune_log_files()
        log_path = self.config.logging.file_path or ""
        retention = self.config.logging.retention_days
        return {
            "deleted_count": deleted,
            "retention_days": retention,
            "log_path": log_path,
        }

    async def on_shutdown(self, app: web.Application):
        logger.info("正在关闭代理服务器...")
        if self._ban_cleanup_task is not None:
            self._ban_cleanup_task.cancel()
            try:
                await self._ban_cleanup_task
            except asyncio.CancelledError:
                pass
            self._ban_cleanup_task = None
        if self._auto_ban_cleanup_task is not None:
            self._auto_ban_cleanup_task.cancel()
            try:
                await self._auto_ban_cleanup_task
            except asyncio.CancelledError:
                pass
            self._auto_ban_cleanup_task = None
        if self._log_cleanup_task is not None:
            self._log_cleanup_task.cancel()
            try:
                await self._log_cleanup_task
            except asyncio.CancelledError:
                pass
            self._log_cleanup_task = None
        await self.request_handler.close()
        await self.offline_geoip_sync_service.stop()
        await self.geo_resolver.close()
        logger.info("代理服务器已停止")
    
    def run(self):
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        
        if self.config.ssl.enabled:
            ssl_context = None
            try:
                import ssl
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                ssl_context.load_cert_chain(
                    self.config.ssl.cert_file,
                    self.config.ssl.key_file
                )
                logger.info("SSL 已启用")
            except Exception as e:
                logger.error(f"SSL 设置失败: {e}")
                sys.exit(1)
        else:
            ssl_context = None
        
        web.run_app(
            self.app,
            host=self.config.server.host,
            port=self.config.server.port,
            ssl_context=ssl_context,
            access_log=None
        )


def main():
    parser = argparse.ArgumentParser(description='HTTP反向代理服务器 - 支持302重定向和流式传输')
    
    parser.add_argument('-c', '--config', type=str, default=None, help='配置文件路径 (默认: config.yaml)')
    parser.add_argument('-p', '--port', type=int, default=None, help='覆盖配置文件中的端口')
    parser.add_argument('--host', type=str, default=None, help='覆盖配置文件中的主机地址')
    parser.add_argument('-v', '--verbose', action='store_true', help='启用详细日志输出')
    parser.add_argument('--no-streaming', action='store_true', help='禁用流式传输模式')
    
    args = parser.parse_args()
    
    bootstrap_config = load_config(args.config)
    config_store = ConfigStore(bootstrap_config.database_path, bootstrap_config=bootstrap_config)
    config = config_store.load_runtime_config()
    
    from pathlib import Path
    if config.logging.file_path:
        Path(config.logging.file_path).parent.mkdir(parents=True, exist_ok=True)
    if config.geoip.offline.db_path:
        Path(config.geoip.offline.db_path).parent.mkdir(parents=True, exist_ok=True)

    if args.port is None:
        config.server.port = bootstrap_config.server.port
    if args.host is None:
        config.server.host = bootstrap_config.server.host
    
    if args.verbose:
        config.logging.level = 'DEBUG'
    if args.no_streaming:
        config.streaming.enabled = False
    
    logger = setup_logging(config.logging)
    
    if config.logging.file_path:
        prune_app_log_files(config.logging.file_path, config.logging.retention_days)
    
    if not config.proxy_rules:
        logger.warning("当前本地数据库尚无代理规则。")
        logger.info("如果已启用远程同步，服务启动时会尝试加载远程规则；否则请在 /_admin 中新增规则。")
    
    server = ProxyServer(config, config_store)
    
    def signal_handler(sig, frame):
        logger.info(f"收到信号 {sig}, 正在关闭...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("服务器被用户停止")
    except Exception as e:
        logger.exception(f"服务器错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
