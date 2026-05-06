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

from config import Config, load_config, setup_logging, normalize_request_host
from admin_console import AdminConsole
from config_store import ConfigStore
from geo_service import GeoResolver
from offline_geoip_sync import OfflineGeoIPSyncService
from proxy_core import ProxyRequestHandler, ProxyStats, StreamingResponse

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
        self.request_handler = ProxyRequestHandler(config, geo_resolver=self.geo_resolver)
        self.geo_resolver.set_session_provider(self.request_handler.get_session)
        self.stats = ProxyStats()
        self.admin_console = AdminConsole(
            config_store=self.config_store,
            reload_callback=self.reload_runtime_config,
            offline_sync_callback=self.sync_offline_geoip_now,
            offline_rollback_callback=self.rollback_offline_geoip_now,
            online_cache_clear_callback=self.clear_online_geoip_cache,
        )
        self.app = web.Application(client_max_size=config.streaming.max_request_body_size)
        self._setup_routes()
        self._setup_middleware()
    
    def _setup_routes(self):
        self.app.router.add_get('/_health', self.health_check)
        self.app.router.add_get('/_stats', self.get_stats)
        self.admin_console.register(self.app)
        self.app.router.add_route('*', '/{path:.*}', self.handle_proxy)
    
    def _setup_middleware(self):
        @web.middleware
        async def logging_middleware(request: web.Request, handler):
            start_time = time.perf_counter()
            try:
                response = await handler(request)
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    f"{request.method} {request.path} - 状态码: {response.status} - 耗时: {duration_ms:.2f}毫秒"
                )
                return response
            except Exception as e:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.error(
                    f"{request.method} {request.path} - 错误: {e} - 耗时: {duration_ms:.2f}毫秒"
                )
                raise
        
        self.app.middlewares.append(logging_middleware)

    async def reload_runtime_config(self) -> None:
        self.config = self.config_store.load_runtime_config()
        self.geo_resolver.set_online_cache_ttl_seconds(self.config.geoip.online_cache_ttl_seconds, reset_existing=True)
        self.request_handler.update_config(self.config)

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

    def _request_duration_ms(self, request: web.Request) -> int:
        started_at = request.get("_route_log_started_at")
        if started_at is None:
            return 0
        return max(0, int((time.perf_counter() - float(started_at)) * 1000))

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
            route_decision = await self.request_handler.select_route(
                path_decoded,
                headers,
                client_host,
                query_string if query_string else None,
            )
            target_url = route_decision.target_url if route_decision else None
            use_streaming_mode = bool(
                route_decision and route_decision.rule.enable_streaming and self.config.streaming.enabled
            )
            
            range_header = headers.get('Range')
            
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
                
                return await self._send_streaming_response(request, streaming_response)
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
                
                await self.stats.record_request(
                    redirected=redirect_info is not None and redirect_info.redirect_count > 0,
                    redirect_count=redirect_info.redirect_count if redirect_info else 0,
                    failed=status >= 400
                )
                self._record_route_log(
                    request,
                    route_decision=route_decision,
                    upstream_status=status,
                    cache_status="BYPASS",
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
            return web.Response(
                status=500,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '内部服务器错误'}).encode()
            )
    
    async def _send_streaming_response(self, request: web.Request, streaming_response: StreamingResponse) -> web.StreamResponse:
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
                cache_status="BYPASS",
                redirect_info=redirect_info,
                transport_mode="streaming",
            )
        
        return response
    
    async def health_check(self, request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                'status': 'healthy',
                'database_path': str(self.config_store.db_path),
                'route_group_count': len(self.config.route_groups),
                'region_enabled_group_count': sum(
                    1 for group in self.config.route_groups if group.region_matching_enabled
                ),
                'rule_count': len(self.config.proxy_rules)
            }, ensure_ascii=False).encode()
        )
    
    async def get_stats(self, request: web.Request) -> web.Response:
        stats = self.stats.get_stats()
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(stats).encode()
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
    
    async def on_shutdown(self, app: web.Application):
        logger.info("正在关闭代理服务器...")
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
