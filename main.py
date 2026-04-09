"""
HTTP 反向代理服务器主模块

本模块实现了一个功能完整的 HTTP 反向代理服务器，支持以下核心功能：
1. 302 重定向跟踪 - 自动跟踪上游服务器的重定向响应
2. 流式传输 - 支持大文件和流媒体的边传边发
3. 流式缓存 - 支持流式媒体的边传边缓存
4. Range 请求 - 支持断点续传和部分内容请求
5. 健康检查 - 提供服务状态监控接口
6. 统计信息 - 提供请求统计和缓存统计接口

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
from aiohttp import web
from typing import Dict, Any, AsyncGenerator, Optional, Tuple
import json
import logging

from config import Config, load_config, setup_logging
from proxy_core import ProxyRequestHandler, ProxyStats, StreamingResponse

# ============================================================================
# 流式缓存模块导入
# ============================================================================
# 尝试导入流式缓存管理器，该模块依赖 aiofiles 库
# 如果导入失败，程序仍可正常运行，但缓存功能将被禁用
# 这种设计确保了程序的健壮性，避免因可选依赖缺失导致程序无法启动
try:
    from streaming_cache_manager import StreamingCacheManager
    CACHE_AVAILABLE = True
except ImportError as e:
    StreamingCacheManager = None
    CACHE_AVAILABLE = False
    import_warning = str(e)

# 获取当前模块的日志记录器
# 日志配置由 config.py 中的 setup_logging 函数统一管理
logger = logging.getLogger('proxy')


def format_bytes(bytes_value: int) -> str:
    """
    将字节数格式化为人类可读的字符串
    
    根据字节数大小自动选择合适的单位（B/KB/MB/GB），
    使日志输出和统计信息更易于阅读。
    
    参数:
        bytes_value: 要格式化的字节数，必须为非负整数
    
    返回:
        格式化后的字符串，例如：
        - 512 -> "512 B"
        - 1024 -> "1.00 KB"
        - 1048576 -> "1.00 MB"
        - 1073741824 -> "1.00 GB"
    
    示例:
        >>> format_bytes(1536)
        '1.50 KB'
        >>> format_bytes(1073741824)
        '1.00 GB'
    """
    # 1 GB = 1024^3 字节
    if bytes_value >= 1024 * 1024 * 1024:
        return f"{bytes_value / 1024 / 1024 / 1024:.2f} GB"
    # 1 MB = 1024^2 字节
    elif bytes_value >= 1024 * 1024:
        return f"{bytes_value / 1024 / 1024:.2f} MB"
    # 1 KB = 1024 字节
    elif bytes_value >= 1024:
        return f"{bytes_value / 1024:.2f} KB"
    # 小于 1 KB 时直接显示字节数
    else:
        return f"{bytes_value} B"


class ProxyServer:
    """
    HTTP 反向代理服务器核心类
    
    负责管理整个代理服务器的生命周期，包括：
    - 请求路由和处理
    - 流式传输管理
    - 缓存管理
    - 统计信息收集
    - 服务器启动和关闭
    
    属性:
        config: 服务器配置对象，包含所有配置参数
        request_handler: 请求处理器，负责实际的反向代理逻辑
        stats: 统计信息收集器，记录请求数量、流量等
        cache_manager: 流式缓存管理器，可选，依赖 aiofiles
        app: aiohttp Web 应用实例
    
    设计模式:
        - 策略模式：根据配置选择是否启用流式传输和缓存
        - 中间件模式：通过中间件实现请求日志记录
        - 依赖注入：缓存管理器作为可选依赖注入
    """
    
    def __init__(self, config: Config):
        """
        初始化代理服务器
        
        创建并配置所有必要的组件，包括：
        - 请求处理器
        - 统计收集器
        - Web 应用
        - 路由和中间件
        
        参数:
            config: 配置对象，包含服务器的所有配置参数
        """
        self.config = config
        # 创建请求处理器，负责处理代理请求的核心逻辑
        self.request_handler = ProxyRequestHandler(config)
        # 创建统计收集器，用于记录请求统计信息
        self.stats = ProxyStats()
        # 缓存管理器，在启动时根据配置和依赖可用性初始化
        self.cache_manager: StreamingCacheManager = None
        # 创建 aiohttp Web 应用
        # client_max_size 设置请求体最大大小，用于限制上传文件大小
        self.app = web.Application(client_max_size=config.streaming.max_request_body_size)
        # 设置路由规则
        self._setup_routes()
        # 设置中间件
        self._setup_middleware()
    
    def _setup_routes(self):
        """
        设置路由规则
        
        定义所有 API 端点和对应的处理函数：
        
        代理路由:
            /{path:.*} - 所有路径都通过代理处理（支持所有 HTTP 方法）
        
        管理接口:
            GET /_health - 健康检查接口
            GET /_stats - 获取服务器统计信息
            GET /_cache/stats - 获取缓存统计信息
            GET /_cache/info - 获取详细缓存信息
            POST /_cache/clear - 清空缓存
            POST /_cache/preload - 预加载 URL 到缓存
            DELETE /_cache/entry - 删除指定缓存条目
        
        注意：
            管理接口使用 /_ 前缀，避免与代理路径冲突
        """
        # 代理路由：匹配所有路径，支持所有 HTTP 方法
        self.app.router.add_route('*', '/{path:.*}', self.handle_proxy)
        
        # 健康检查接口 - 用于负载均衡器或监控系统检测服务状态
        self.app.router.add_get('/_health', self.health_check)
        
        # 统计接口 - 返回服务器运行统计信息
        self.app.router.add_get('/_stats', self.get_stats)
        
        # 缓存管理接口
        self.app.router.add_get('/_cache/stats', self.get_cache_stats)
        self.app.router.add_get('/_cache/info', self.get_cache_info)
        self.app.router.add_post('/_cache/clear', self.clear_cache)
        self.app.router.add_post('/_cache/preload', self.preload_cache)
        self.app.router.add_delete('/_cache/entry', self.remove_cache_entry)
    
    def _setup_middleware(self):
        """
        设置中间件
        
        创建并注册请求日志中间件，用于：
        - 记录每个请求的方法、路径、状态码和耗时
        - 捕获并记录请求处理过程中的异常
        
        中间件执行顺序：
        1. 记录开始时间
        2. 执行请求处理
        3. 计算耗时并记录日志
        4. 如果发生异常，记录错误日志并重新抛出
        """
        @web.middleware
        async def logging_middleware(request: web.Request, handler):
            """
            请求日志中间件
            
            为每个请求记录详细的访问日志，包括：
            - HTTP 方法和请求路径
            - 响应状态码
            - 请求处理耗时
            
            参数:
                request: 客户端请求对象
                handler: 实际的请求处理函数
            
            返回:
                处理函数返回的响应对象
            """
            # 记录请求开始时间，使用事件循环时间以获得更高精度
            start_time = asyncio.get_event_loop().time()
            try:
                # 执行实际的请求处理
                response = await handler(request)
                # 计算请求处理耗时
                duration = asyncio.get_event_loop().time() - start_time
                # 记录成功请求的日志
                logger.info(
                    f"{request.method} {request.path} - 状态码: {response.status} - 耗时: {duration:.3f}秒"
                )
                return response
            except Exception as e:
                # 计算请求处理耗时（即使发生异常）
                duration = asyncio.get_event_loop().time() - start_time
                # 记录失败请求的日志
                logger.error(
                    f"{request.method} {request.path} - 错误: {e} - 耗时: {duration:.3f}秒"
                )
                raise
        
        # 将中间件添加到应用
        self.app.middlewares.append(logging_middleware)
    
    def should_use_streaming(self, headers: Dict[str, str], content_length: int = None) -> bool:
        """
        判断是否应该使用流式传输
        
        根据响应头和内容长度判断是否需要使用流式传输模式。
        流式传输适用于以下场景：
        1. 视频和音频内容
        2. 大文件下载
        3. 分块传输编码
        4. 超过阈值大小的文件
        
        参数:
            headers: HTTP 响应头字典
            content_length: 内容长度（字节），可选
        
        返回:
            True 表示应该使用流式传输，False 表示使用普通传输
        
        判断逻辑:
            1. 检查配置是否启用流式传输
            2. 检查 Content-Type 是否为流式媒体类型
            3. 检查是否使用分块传输编码
            4. 检查文件大小是否超过阈值
        """
        # 如果配置中禁用了流式传输，直接返回 False
        if not self.config.streaming.enabled:
            return False
        
        # 获取内容类型并转换为小写，便于匹配
        content_type = headers.get('Content-Type', '').lower()
        
        # 定义需要使用流式传输的内容类型列表
        # 这些类型通常是大文件或流媒体，需要边传边发
        streaming_types = [
            'video/',                    # 视频文件（如 video/mp4, video/webm）
            'audio/',                    # 音频文件（如 audio/mpeg, audio/ogg）
            'application/octet-stream',  # 二进制流，可能是任意大文件
            'application/x-mpegurl',     # HLS 播放列表（m3u8）
            'application/vnd.apple.mpegurl',  # Apple HLS 播放列表
            'application/dash+xml',      # DASH 流媒体清单
            'multipart/'                 # 多部分内容（如 multipart/x-mixed-replace）
        ]
        
        # 检查内容类型是否匹配任何流式类型
        for stream_type in streaming_types:
            if stream_type in content_type:
                return True
        
        # 检查是否使用分块传输编码
        # 分块传输通常用于动态生成的内容或大文件
        transfer_encoding = headers.get('Transfer-Encoding', '').lower()
        if 'chunked' in transfer_encoding:
            return True
        
        # 检查文件大小是否超过配置的阈值
        # 大文件需要流式传输以避免内存占用过高
        if content_length is not None and content_length > self.config.streaming.large_file_threshold:
            return True
        
        # 以上条件都不满足，使用普通传输模式
        return False
    
    def _parse_content_range(self, content_range: Optional[str]) -> Optional[Tuple[int, int, int]]:
        """
        解析标准的 Content-Range 头，返回 (start, end, total_size)。
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

    def _is_full_file_range_response(
        self,
        request_range: Optional[str],
        response_headers: Dict[str, str]
    ) -> bool:
        """
        判断当前 206 响应是否实际返回了完整文件。

        典型场景是播放器首次发起 `Range: bytes=0-`，上游返回 206，
        但响应体其实覆盖了 0 到文件末尾的全部内容。这类响应可以安全落盘为完整缓存。
        """
        if not request_range:
            return False

        request_range = request_range.strip()
        if not request_range.lower().startswith('bytes='):
            return False

        requested_range = request_range[6:]
        if ',' in requested_range:
            return False

        start_text, _, end_text = requested_range.partition('-')
        if not start_text or int(start_text) != 0:
            return False

        parsed_content_range = self._parse_content_range(response_headers.get('Content-Range'))
        if not parsed_content_range:
            return False

        start_byte, end_byte, total_size = parsed_content_range
        if start_byte != 0 or end_byte != total_size - 1:
            return False

        if end_text and int(end_text) != end_byte:
            return False

        content_length = response_headers.get('Content-Length')
        if content_length and int(content_length) != total_size:
            return False

        return True

    def _build_cached_response_headers(
        self,
        entry,
        start_byte: int,
        end_byte: int,
        total_size: int,
        is_range_request: bool
    ) -> Dict[str, str]:
        """
        构建缓存命中时返回给客户端的响应头。
        """
        response_headers = {
            'Content-Type': entry.content_type,
            'Accept-Ranges': 'bytes',
            'Content-Length': str(end_byte - start_byte + 1) if is_range_request else str(total_size),
            'X-Cache-Status': 'HIT'
        }

        if is_range_request:
            response_headers['Content-Range'] = f'bytes {start_byte}-{end_byte}/{total_size}'

        if entry.etag:
            response_headers['ETag'] = entry.etag
        if entry.last_modified:
            response_headers['Last-Modified'] = entry.last_modified

        return response_headers

    def _build_prefetch_request_headers(self, request: web.Request) -> Dict[str, str]:
        """
        构建后台 Range 预取使用的上游请求头。
        """
        request_headers = self.request_handler.filter_headers(dict(request.headers), is_request=True)
        return self.request_handler.add_forward_headers(
            request_headers,
            request.remote or '',
            request.scheme
        )

    async def handle_proxy(self, request: web.Request) -> web.StreamResponse:
        """
        处理代理请求的主入口
        
        这是所有代理请求的核心处理函数，负责：
        1. 解析客户端请求
        2. 检查缓存是否命中
        3. 根据配置选择传输模式（流式/普通）
        4. 处理缓存存储
        5. 返回响应给客户端
        
        参数:
            request: 客户端请求对象，包含请求方法、路径、头部等信息
        
        返回:
            web.StreamResponse 或 web.Response 对象
        
        处理流程:
            1. 解析请求参数（方法、路径、头部、请求体等）
            2. 如果启用缓存且为 GET 请求，尝试从缓存获取
            3. 缓存命中：直接返回缓存内容
            4. 缓存未命中：转发请求到上游服务器
            5. 根据响应类型选择是否缓存
            6. 返回响应给客户端
        
        异常处理:
            - 捕获所有异常并返回 500 错误
            - 记录详细的错误日志
        """
        try:
            # ====================================================================
            # 第一阶段：解析客户端请求
            # ====================================================================
            method = request.method           # HTTP 方法（GET, POST, PUT 等）
            # 获取请求路径和查询字符串
            # request.path: URL 解码后的路径（不含查询字符串）
            # request.raw_path: 原始编码的路径（不含查询字符串）
            # request.query_string: 查询字符串（不含 ? 前缀）
            # request.path_qs: URL 编码后的路径 + 查询字符串
            # 
            # 重要说明：
            # - 使用 raw_path 构建目标 URL，保持原始编码格式
            # - 使用 path_qs 作为缓存键，保证缓存一致性
            # 
            raw_path = request.raw_path         # 原始编码路径，用于构建目标 URL
            query_string = request.query_string  # 查询字符串
            path_decoded = request.path         # 解码后的路径，用于路由匹配
            path_for_cache = request.path_qs  # 编码后的完整路径，用于缓存键
            headers = dict(request.headers)   # 请求头（转换为字典）
            body = await request.read()       # 请求体（用于 POST/PUT 等）
            client_host = request.remote or ''  # 客户端 IP 地址
            scheme = request.scheme           # 请求协议（http/https）
            
            # 获取 Range 请求头，用于断点续传
            # 格式：bytes=start-end 或 bytes=start-
            range_header = headers.get('Range')
            
            # ====================================================================
            # 第二阶段：尝试从缓存获取响应
            # ====================================================================
            # 只有 GET 请求且缓存可用时才尝试缓存查找
            if self.cache_manager and method == 'GET':
                # 查找匹配的代理规则（使用解码后的路径进行匹配）
                rule = self.request_handler.find_matching_rule(path_decoded)
                if rule:
                    # 构建目标 URL（使用解码后的路径 + 查询字符串）
                    target_url = self.request_handler.build_target_url(path_decoded, rule, query_string)
                    
                    # ----------------------------------------------------------------
                    # 流式传输模式下的缓存处理
                    # ----------------------------------------------------------------
                    if self.config.streaming.enabled:
                        # 尝试从缓存获取流式响应
                        # get_stream 返回一个生成器，可以按需读取数据块
                        cached_stream = await self.cache_manager.get_stream(target_url, range_header)
                        if cached_stream:
                            # 缓存命中，解包返回的数据
                            # entry: 缓存条目元数据
                            # stream_gen: 数据流生成器
                            # start_byte, end_byte: 请求的字节范围
                            # total_size: 文件总大小
                            entry, stream_gen, start_byte, end_byte, total_size = cached_stream
                            
                            # 构建响应头
                            response_headers = {
                                'Content-Type': entry.content_type,
                                'Accept-Ranges': 'bytes',  # 告知客户端支持 Range 请求
                                'Content-Range': f'bytes {start_byte}-{end_byte}/{total_size}',
                                'Content-Length': str(end_byte - start_byte + 1)
                            }
                            # 添加可选的验证头
                            if entry.etag:
                                response_headers['ETag'] = entry.etag
                            if entry.last_modified:
                                response_headers['Last-Modified'] = entry.last_modified
                            # 标记缓存命中状态
                            response_headers['X-Cache-Status'] = 'HIT'
                            
                            # 创建流式响应对象
                            # 如果有 Range 请求头，返回 206 Partial Content
                            # 否则返回 200 OK（完整内容）
                            response_headers = self._build_cached_response_headers(
                                entry,
                                start_byte,
                                end_byte,
                                total_size,
                                bool(range_header)
                            )
                            await self.cache_manager.schedule_following_range_prefetch(
                                target_url,
                                range_header,
                                response_headers,
                                self._build_prefetch_request_headers(request)
                            )
                            response = web.StreamResponse(
                                status=206 if range_header else 200,
                                headers=response_headers
                            )
                            await response.prepare(request)
                            
                            # 流式传输数据
                            bytes_transferred = 0
                            try:
                                # 从缓存流中读取数据块并写入响应
                                async for chunk in stream_gen:
                                    await response.write(chunk)
                                    bytes_transferred += len(chunk)
                            # ----------------------------------------------------------------
                            # 连接异常处理
                            # ----------------------------------------------------------------
                            # 这些异常表示客户端断开连接，属于正常情况
                            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
                                logger.info(f"客户端断开连接: {type(e).__name__}, 已传输 {format_bytes(bytes_transferred)}")
                            # 任务被取消（如客户端主动断开）
                            except asyncio.CancelledError:
                                logger.info(f"流传输被取消，已传输 {format_bytes(bytes_transferred)}")
                                raise
                            # 其他异常
                            except Exception as e:
                                error_msg = str(e)
                                # Content-Length 不匹配警告
                                # 这通常发生在上游服务器返回的数据量与声明不符
                                if 'ContentLengthError' in error_msg or 'payload is not completed' in error_msg:
                                    logger.warning(f"缓存流读取时 Content-Length 不匹配，已传输 {format_bytes(bytes_transferred)}，错误：{e}")
                                else:
                                    logger.error(f"缓存流读取错误：{e}")
                                    raise
                            
                            # 记录请求统计
                            await self.stats.record_request(
                                streaming=True,
                                bytes_count=bytes_transferred
                            )
                            return response
                    
                    # ----------------------------------------------------------------
                    # 非流式模式下的缓存处理（小文件或禁用流式传输）
                    # ----------------------------------------------------------------
                    else:
                        # 尝试从缓存获取完整数据
                        cached = await self.cache_manager.get_range(target_url, range_header)
                        if cached:
                            # 缓存命中，解包返回的数据
                            entry, data, start_byte, end_byte, total_size = cached
                            
                            # 构建响应头
                            response_headers = {'Content-Type': entry.content_type}
                            if entry.etag:
                                response_headers['ETag'] = entry.etag
                            if entry.last_modified:
                                response_headers['Last-Modified'] = entry.last_modified
                            response_headers['X-Cache-Status'] = 'HIT'
                            
                            # 记录请求统计
                            await self.stats.record_request(bytes_count=len(data))
                            
                            # 返回完整响应
                            response_headers = self._build_cached_response_headers(
                                entry,
                                start_byte,
                                end_byte,
                                total_size,
                                bool(range_header)
                            )
                            await self.cache_manager.schedule_following_range_prefetch(
                                target_url,
                                range_header,
                                response_headers,
                                self._build_prefetch_request_headers(request)
                            )

                            return web.Response(
                                status=206 if range_header else 200,
                                headers=response_headers,
                                body=data
                            )
            
            # ====================================================================
            # 第三阶段：缓存未命中，转发请求到上游服务器
            # ====================================================================
            if self.config.streaming.enabled:
                # 流式传输模式
                # 调用请求处理器获取流式响应
                streaming_response = await self.request_handler.handle_request_streaming(
                    method=method,
                    path=path_decoded,
                    headers=headers,
                    body=body if body else None,
                    client_host=client_host,
                    scheme=scheme,
                    query_string=query_string if query_string else None
                )
                
                # 发送流式响应并尝试缓存
                return await self._send_streaming_response_with_cache(
                    request, streaming_response, path_decoded
                )
            else:
                # 普通传输模式
                # 调用请求处理器获取完整响应
                status, response_headers, response_body, redirect_info = await self.request_handler.handle_request(
                    method=method,
                    path=path_decoded,
                    headers=headers,
                    body=body if body else None,
                    client_host=client_host,
                    scheme=scheme,
                    query_string=query_string if query_string else None
                )
                
                # 记录请求统计
                await self.stats.record_request(
                    redirected=redirect_info is not None and redirect_info.redirect_count > 0,
                    redirect_count=redirect_info.redirect_count if redirect_info else 0,
                    failed=status >= 400
                )
                
                # 如果发生了重定向，添加重定向信息到响应头
                if redirect_info and redirect_info.redirect_count > 0:
                    response_headers['X-Redirect-Count'] = str(redirect_info.redirect_count)
                    response_headers['X-Original-URL'] = redirect_info.original_url
                    response_headers['X-Final-URL'] = redirect_info.redirect_url
                
                # 返回完整响应
                return web.Response(
                    status=status,
                    headers=response_headers,
                    body=response_body
                )
        
        # ====================================================================
        # 异常处理：捕获所有未处理的异常
        # ====================================================================
        except Exception as e:
            logger.exception(f"处理请求时发生错误: {e}")
            await self.stats.record_request(failed=True)
            # 返回 500 内部服务器错误
            return web.Response(
                status=500,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '内部服务器错误'}).encode()
            )
    
    async def _send_streaming_response_with_cache(
        self,
        request: web.Request,
        streaming_response: StreamingResponse,
        path: str
    ) -> web.StreamResponse:
        """
        发送流式响应并处理缓存
        
        将上游服务器的流式响应发送给客户端，同时根据条件决定是否缓存。
        这是流式传输模式的核心方法，实现了边传边缓存的功能。
        
        参数:
            request: 客户端请求对象
            streaming_response: 上游服务器的流式响应
            path: 请求路径（用于查找代理规则）
        
        返回:
            流式响应对象
        
        缓存条件:
            1. 缓存管理器可用
            2. 请求方法为 GET
            3. 响应状态码为 200
            4. 内容类型适合流式缓存
        
        实现思路:
            使用 tee 模式实现边传边缓存：
            - 数据同时发送给客户端和缓存存储
            - 如果缓存失败，不影响客户端传输
            - 如果客户端断开，缓存继续完成
        """
        # 获取重定向信息和响应头
        redirect_info = streaming_response.redirect_info
        response_headers = streaming_response.headers
        
        # 记录请求统计
        await self.stats.record_request(
            redirected=redirect_info is not None and redirect_info.redirect_count > 0,
            redirect_count=redirect_info.redirect_count if redirect_info else 0,
            failed=streaming_response.status >= 400,
            streaming=True
        )
        
        # 如果发生了重定向，添加重定向信息到响应头
        if redirect_info and redirect_info.redirect_count > 0:
            response_headers['X-Redirect-Count'] = str(redirect_info.redirect_count)
            response_headers['X-Original-URL'] = redirect_info.original_url
            response_headers['X-Final-URL'] = redirect_info.redirect_url
        
        # ====================================================================
        # 判断是否应该缓存此响应
        # ====================================================================
        has_cacheable_content_range = (
            streaming_response.status == 206 and
            self._parse_content_range(response_headers.get('Content-Range')) is not None
        )
        should_cache = (
            self.cache_manager and
            request.method == 'GET' and
            streaming_response.status in (200, 206) and
            (streaming_response.status == 200 or has_cacheable_content_range) and
            self.should_use_streaming(response_headers, streaming_response.content_length)
        )

        if should_cache:
            # 查找代理规则并构建目标 URL
            # 从请求对象获取路径信息
            path_decoded = request.path  # 解码后的路径
            query_string = request.query_string  # 查询字符串
            
            rule = self.request_handler.find_matching_rule(path_decoded)
            if rule:
                # 使用解码后的路径构建目标 URL
                target_url = self.request_handler.build_target_url(path_decoded, rule, query_string if query_string else None)
                
                try:
                    # ----------------------------------------------------------------
                    # 边传边缓存的核心逻辑
                    # ----------------------------------------------------------------
                    # cache_streaming_response 返回一个生成器
                    # 该生成器在产出数据的同时将数据写入缓存
                    cached_stream = self.cache_manager.cache_streaming_response(
                        target_url,
                        streaming_response.body_stream,
                        dict(response_headers)
                    )
                    await self.cache_manager.schedule_following_range_prefetch(
                        target_url,
                        request.headers.get('Range'),
                        dict(response_headers),
                        self._build_prefetch_request_headers(request)
                    )
                    
                    # 标记缓存未命中（MISS 表示数据来自上游而非缓存）
                    response_headers['X-Cache-Status'] = 'MISS'
                    # 确保 Accept-Ranges 头存在
                    if 'Accept-Ranges' not in response_headers:
                        response_headers['Accept-Ranges'] = 'bytes'
                    
                    # 创建流式响应
                    response = web.StreamResponse(
                        status=streaming_response.status,
                        headers=response_headers
                    )
                    await response.prepare(request)
                    
                    # 流式传输数据
                    bytes_transferred = 0
                    try:
                        # 从缓存流中读取数据并写入响应
                        # 注意：cached_stream 会在产出数据的同时写入缓存
                        async for chunk in cached_stream:
                            await response.write(chunk)
                            bytes_transferred += len(chunk)
                    # ----------------------------------------------------------------
                    # 连接异常处理
                    # ----------------------------------------------------------------
                    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
                        logger.info(f"客户端断开连接：{type(e).__name__}, 已传输 {format_bytes(bytes_transferred)}")
                    except asyncio.CancelledError:
                        logger.info(f"流传输被取消，已传输 {format_bytes(bytes_transferred)}")
                        raise
                    except Exception as e:
                        error_msg = str(e)
                        if 'ContentLengthError' in error_msg or 'payload is not completed' in error_msg:
                            logger.warning(f"上游服务器 Content-Length 不匹配，已传输 {format_bytes(bytes_transferred)}，错误：{e}")
                        else:
                            logger.error(f"缓存流传输错误：{e}")
                            raise
                    
                    # 记录统计
                    await self.stats.record_request(bytes_count=bytes_transferred)
                    return response
                    
                except Exception as e:
                    # 缓存失败，记录错误但继续传输
                    logger.error(f"缓存流式响应时出错：{e}, 继续传输但不缓存")
        
        # ====================================================================
        # 不缓存的情况：直接传输
        # ====================================================================
        # 标记缓存绕过状态
        response_headers['X-Cache-Status'] = 'BYPASS'
        
        # 创建流式响应
        response = web.StreamResponse(
            status=streaming_response.status,
            headers=response_headers
        )
        
        await response.prepare(request)
        
        # 流式传输数据（不缓存）
        bytes_transferred = 0
        try:
            async for chunk in streaming_response.body_stream:
                try:
                    await response.write(chunk)
                    bytes_transferred += len(chunk)
                # 处理客户端断开连接
                except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
                    logger.info(f"客户端断开连接: {type(e).__name__}, 已传输 {format_bytes(bytes_transferred)}")
                    break
                except asyncio.CancelledError:
                    logger.info(f"流传输被客户端取消, 已传输 {format_bytes(bytes_transferred)}")
                    raise
        # 处理外层异常
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
            logger.info(f"客户端连接丢失: {type(e).__name__}, 已传输 {format_bytes(bytes_transferred)}")
        except asyncio.CancelledError:
            logger.info(f"流传输被取消, 已传输 {format_bytes(bytes_transferred)}")
            raise
        except Exception as e:
            error_msg = str(e)
            # 处理特定的连接错误
            if 'Cannot write to closing transport' in error_msg or 'Connection reset' in error_msg:
                logger.info(f"客户端断开连接，已传输 {format_bytes(bytes_transferred)}")
            elif 'ContentLengthError' in error_msg or 'payload is not completed' in error_msg:
                logger.warning(f"上游服务器 Content-Length 不匹配，已传输 {format_bytes(bytes_transferred)}，错误：{e}")
            else:
                logger.error(f"流传输响应时发生错误：{e}")
                raise
        finally:
            # 确保统计被记录
            await self.stats.record_request(bytes_count=bytes_transferred)
        
        return response
    
    async def _send_streaming_response(
        self,
        request: web.Request,
        streaming_response: StreamingResponse
    ) -> web.StreamResponse:
        """
        发送流式响应（不缓存）
        
        将上游服务器的流式响应直接发送给客户端，不进行缓存。
        这是一个简化的流式传输方法，用于不需要缓存的场景。
        
        参数:
            request: 客户端请求对象
            streaming_response: 上游服务器的流式响应
        
        返回:
            流式响应对象
        
        注意:
            此方法已被 _send_streaming_response_with_cache 替代，
            保留是为了向后兼容或特殊场景使用。
        """
        # 获取重定向信息和响应头
        redirect_info = streaming_response.redirect_info
        response_headers = streaming_response.headers
        
        # 记录请求统计
        await self.stats.record_request(
            redirected=redirect_info is not None and redirect_info.redirect_count > 0,
            redirect_count=redirect_info.redirect_count if redirect_info else 0,
            failed=streaming_response.status >= 400,
            streaming=True
        )
        
        # 添加重定向信息头
        if redirect_info and redirect_info.redirect_count > 0:
            response_headers['X-Redirect-Count'] = str(redirect_info.redirect_count)
            response_headers['X-Original-URL'] = redirect_info.original_url
            response_headers['X-Final-URL'] = redirect_info.redirect_url
        
        # 创建流式响应
        response = web.StreamResponse(
            status=streaming_response.status,
            headers=response_headers
        )
        
        await response.prepare(request)
        
        # 流式传输数据
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
            if 'Cannot write to closing transport' in str(e) or 'Connection reset' in str(e):
                logger.info(f"客户端断开连接, 已传输 {format_bytes(bytes_transferred)}")
            else:
                logger.error(f"流传输响应时发生错误: {e}")
                raise
        finally:
            await self.stats.record_request(bytes_count=bytes_transferred)
        
        return response
    
    async def health_check(self, request: web.Request) -> web.Response:
        """
        健康检查接口
        
        提供一个简单的健康检查端点，用于：
        - 负载均衡器检测服务状态
        - 监控系统检测服务是否存活
        - Kubernetes 存活探针
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含状态信息
            {
                "status": "healthy"
            }
        """
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({'status': 'healthy'}).encode()
        )
    
    async def get_stats(self, request: web.Request) -> web.Response:
        """
        获取服务器统计信息
        
        返回服务器的运行统计信息，包括：
        - 请求总数
        - 成功/失败请求数
        - 重定向统计
        - 流量统计
        - 缓存统计（如果启用）
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含统计信息
        """
        # 获取基础统计信息
        stats = self.stats.get_stats()
        # 如果缓存可用，添加缓存统计
        if self.cache_manager:
            cache_info = await self.cache_manager.get_cache_info()
            stats['cache'] = cache_info['stats']
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(stats).encode()
        )
    
    async def get_cache_stats(self, request: web.Request) -> web.Response:
        """
        获取缓存统计信息
        
        返回缓存的统计信息，包括：
        - 缓存条目数量
        - 缓存大小
        - 命中率
        - 淘汰次数
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含缓存统计信息
            如果缓存未启用，返回 404 错误
        """
        if not self.cache_manager:
            return web.Response(
                status=404,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '缓存未启用'}).encode()
            )

        if not self.config.cache.enable_preload:
            return web.Response(
                status=403,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '预加载功能已禁用'}).encode()
            )
        
        cache_info = await self.cache_manager.get_cache_info()
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(cache_info['stats']).encode()
        )
    
    async def get_cache_info(self, request: web.Request) -> web.Response:
        """
        获取详细缓存信息
        
        返回缓存的详细信息，包括：
        - 统计信息
        - 配置信息
        - 缓存条目列表（可选）
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含详细缓存信息
            如果缓存未启用，返回 404 错误
        """
        if not self.cache_manager:
            return web.Response(
                status=404,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '缓存未启用'}).encode()
            )
        
        cache_info = await self.cache_manager.get_cache_info()
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(cache_info).encode()
        )
    
    async def clear_cache(self, request: web.Request) -> web.Response:
        """
        清空缓存
        
        删除所有缓存条目，释放磁盘空间。
        这是一个管理操作，应谨慎使用。
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含清理结果：
            - 清理的文件数量
            - 释放的空间大小
            如果缓存未启用，返回 404 错误
        """
        if not self.cache_manager:
            return web.Response(
                status=404,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '缓存未启用'}).encode()
            )
        
        # 执行缓存清理
        result = await self.cache_manager.clear_cache()
        return web.Response(
            status=200,
            headers={'Content-Type': 'application/json'},
            body=json.dumps({
                'message': '缓存已清理',
                'cleared_files': result['cleared_files'],
                'freed_size': format_bytes(result['freed_size'])
            }).encode()
        )
    
    async def preload_cache(self, request: web.Request) -> web.Response:
        """
        预加载缓存
        
        将指定的 URL 列表添加到预加载队列。
        预加载会在后台异步下载并缓存这些资源。
        
        请求体格式：
        {
            "urls": ["http://example.com/video.mp4", ...],
            "priority": 0  // 可选，优先级（数字越大优先级越高）
        }
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含预加载结果
            如果缓存未启用，返回 404 错误
        """
        if not self.cache_manager:
            return web.Response(
                status=404,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '缓存未启用'}).encode()
            )
        
        try:
            # 解析请求体
            data = await request.json()
            urls = data.get('urls', [])
            priority = data.get('priority', 0)
            
            # 验证 URL 列表
            if not urls:
                return web.Response(
                    status=400,
                    headers={'Content-Type': 'application/json'},
                    body=json.dumps({'error': '未提供URL列表'}).encode()
                )
            
            # 添加到预加载队列
            await self.cache_manager.preload(urls, priority)
            
            return web.Response(
                status=200,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({
                    'message': f'已添加 {len(urls)} 个URL到预加载队列',
                    'urls': urls[:10]  # 只返回前 10 个 URL，避免响应过大
                }).encode()
            )
        except Exception as e:
            return web.Response(
                status=500,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': str(e)}).encode()
            )
    
    async def remove_cache_entry(self, request: web.Request) -> web.Response:
        """
        删除指定缓存条目
        
        根据URL删除对应的缓存条目。
        
        请求体格式：
        {
            "url": "http://example.com/video.mp4"
        }
        
        参数:
            request: 客户端请求对象
        
        返回:
            JSON 响应，包含删除结果
            如果缓存未启用，返回 404 错误
        """
        if not self.cache_manager:
            return web.Response(
                status=404,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': '缓存未启用'}).encode()
            )
        
        try:
            # 解析请求体
            data = await request.json()
            url = data.get('url')
            
            # 验证 URL
            if not url:
                return web.Response(
                    status=400,
                    headers={'Content-Type': 'application/json'},
                    body=json.dumps({'error': '未提供URL'}).encode()
                )
            
            # 删除缓存条目
            removed = await self.cache_manager.remove_entry(url)
            
            return web.Response(
                status=200,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({
                    'message': '缓存条目已删除' if removed else '缓存条目不存在',
                    'removed': removed
                }).encode()
            )
        except Exception as e:
            return web.Response(
                status=500,
                headers={'Content-Type': 'application/json'},
                body=json.dumps({'error': str(e)}).encode()
            )
    
    async def on_startup(self, app: web.Application):
        """
        服务器启动回调
        
        在服务器启动时执行初始化操作：
        - 打印启动信息
        - 初始化缓存管理器
        - 打印代理规则
        
        参数:
            app: aiohttp 应用实例
        """
        # 打印服务器启动信息
        logger.info(f"代理服务器启动于 {self.config.server.host}:{self.config.server.port}")
        logger.info(f"代理规则数量: {len(self.config.proxy_rules)}")
        
        # 打印流式传输配置
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
        
        # 初始化缓存管理器
        if self.config.cache.enabled:
            if not CACHE_AVAILABLE:
                # 缓存依赖不可用，打印警告
                logger.warning(f"缓存功能不可用: {import_warning}")
                logger.warning("请安装 aiofiles: pip install aiofiles")
                logger.warning("缓存功能已禁用，程序将继续运行")
            else:
                # 创建缓存管理器实例
                self.cache_manager = StreamingCacheManager(
                    cache_dir=self.config.cache.cache_dir,
                    max_size=self.config.cache.max_size,
                    max_entries=self.config.cache.max_entries,
                    default_ttl=self.config.cache.default_ttl,
                    chunk_size=self.config.cache.chunk_size,
                    max_entry_size=self.config.cache.max_entry_size,
                    max_request_cache_size=self.config.cache.max_request_cache_size,
                    enable_preload=self.config.cache.enable_preload,
                    preload_concurrency=self.config.cache.preload_concurrency,
                    enable_validation=self.config.cache.enable_validation,
                    validation_interval=self.config.cache.validation_interval
                )
                await self.cache_manager.start()
                # 打印缓存配置
                logger.info(f"流式缓存已启用:")
                logger.info(f"  缓存目录: {self.config.cache.cache_dir}")
                logger.info(f"  最大大小: {format_bytes(self.config.cache.max_size)}")
                logger.info(f"  最大条目: {self.config.cache.max_entries}")
                logger.info(f"  默认TTL: {self.config.cache.default_ttl} 秒")
                logger.info(f"  单文件缓存上限: {format_bytes(self.config.cache.max_entry_size) if self.config.cache.max_entry_size else '无限制'}")
                logger.info(f"  单请求缓存上限: {format_bytes(self.config.cache.max_request_cache_size) if self.config.cache.max_request_cache_size else '无限制'}")
                logger.info(f"  预加载: {'启用' if self.config.cache.enable_preload else '禁用'}")
                logger.info(f"  预加载并发: {self.config.cache.preload_concurrency}")
                logger.info(f"  缓存校验: {'启用' if self.config.cache.enable_validation else '禁用'}")
                logger.info(f"  维护间隔: {self.config.cache.validation_interval} 秒")
                logger.info(f"  支持 Range 请求和流式缓存")
        
        # 打印代理规则
        for rule in self.config.proxy_rules:
            logger.info(f"  {rule.path_prefix} -> {rule.target_url} (流式: {rule.enable_streaming})")
    
    async def on_shutdown(self, app: web.Application):
        """
        服务器关闭回调
        
        在服务器关闭时执行清理操作：
        - 关闭请求处理器
        - 关闭缓存管理器
        - 打印关闭信息
        
        参数:
            app: aiohttp 应用实例
        """
        logger.info("正在关闭代理服务器...")
        # 关闭请求处理器（释放连接池等资源）
        await self.request_handler.close()
        # 关闭缓存管理器
        if self.cache_manager:
            await self.cache_manager.close()
        logger.info("代理服务器已停止")
    
    def run(self):
        """
        启动服务器
        
        配置并启动 aiohttp 服务器，包括：
        - 注册启动和关闭回调
        - 配置 SSL（如果启用）
        - 绑定主机和端口
        - 开始监听请求
        """
        # 注册生命周期回调
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        
        # 配置 SSL（如果启用）
        if self.config.ssl.enabled:
            ssl_context = None
            try:
                import ssl
                # 创建 SSL 上下文
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                # 加载证书和私钥
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
        
        # 启动服务器
        # access_log=None 禁用 aiohttp 默认的访问日志
        # 我们使用自定义的中间件来记录访问日志
        web.run_app(
            self.app,
            host=self.config.server.host,
            port=self.config.server.port,
            ssl_context=ssl_context,
            access_log=None
        )


def main():
    """
    命令行入口函数
    
    解析命令行参数，加载配置，并启动代理服务器。
    
    命令行参数：
        -c, --config: 配置文件路径（默认: config.yaml）
        -p, --port: 覆盖配置文件中的端口
        --host: 覆盖配置文件中的主机地址
        -v, --verbose: 启用详细日志输出
        --no-streaming: 禁用流式传输模式
        --no-cache: 禁用缓存
    
    使用示例：
        python main.py -c config.yaml -p 8080
        python main.py --verbose --no-cache
    """
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='HTTP反向代理服务器 - 支持302重定向、流式传输和缓存')
    
    # 配置文件路径参数
    parser.add_argument(
        '-c', '--config',
        type=str,
        default=None,
        help='配置文件路径 (默认: config.yaml)'
    )
    
    # 端口覆盖参数
    parser.add_argument(
        '-p', '--port',
        type=int,
        default=None,
        help='覆盖配置文件中的端口'
    )
    
    # 主机地址覆盖参数
    parser.add_argument(
        '--host',
        type=str,
        default=None,
        help='覆盖配置文件中的主机地址'
    )
    
    # 详细日志开关
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='启用详细日志输出'
    )
    
    # 禁用流式传输开关
    parser.add_argument(
        '--no-streaming',
        action='store_true',
        help='禁用流式传输模式'
    )
    
    # 禁用缓存开关
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='禁用缓存'
    )
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 加载配置文件
    config = load_config(args.config)
    
    # 应用命令行参数覆盖
    if args.port is not None:
        config.server.port = args.port
    if args.host is not None:
        config.server.host = args.host
    if args.verbose:
        config.logging.level = 'DEBUG'
    if args.no_streaming:
        config.streaming.enabled = False
    if args.no_cache:
        config.cache.enabled = False
    
    # 设置日志
    logger = setup_logging(config.logging)
    
    # 检查代理规则
    if not config.proxy_rules:
        logger.warning("未配置代理规则。代理服务器将不会转发任何请求。")
        logger.info("请在 config.yaml 中配置代理规则。")
    
    # 创建服务器实例
    server = ProxyServer(config)
    
    # 注册信号处理函数
    # 用于优雅地处理 Ctrl+C 和 kill 命令
    def signal_handler(sig, frame):
        logger.info(f"收到信号 {sig}, 正在关闭...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 启动服务器
    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("服务器被用户停止")
    except Exception as e:
        logger.exception(f"服务器错误: {e}")
        sys.exit(1)


# Python 模块入口点
if __name__ == '__main__':
    main()
