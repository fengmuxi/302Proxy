"""
代理核心模块

本模块实现了 HTTP 反向代理的核心功能，包括：
1. 重定向处理 - 自动跟踪 HTTP 302/301 等重定向响应
2. 流式传输 - 支持大文件和流媒体的边传边发
3. 请求转发 - 将客户端请求转发到目标服务器
4. 统计收集 - 记录请求统计信息

模块结构：
- RedirectInfo: 重定向信息数据类
- StreamingResponse: 流式响应数据类
- RedirectHandler: 重定向处理器
- ProxyRequestHandler: 代理请求处理器
- ProxyStats: 代理统计收集器

设计模式：
- 数据类模式：使用 @dataclass 定义数据结构
- 异步生成器：实现流式数据传输
- 连接池复用：通过 aiohttp ClientSession 管理

作者: nginx302_proxy 项目
"""

import aiohttp
import asyncio
from typing import Optional, Dict, Any, Tuple, AsyncGenerator
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field
import logging
import time
from config import Config, ProxyRule

# 获取当前模块的日志记录器
logger = logging.getLogger('proxy')


@dataclass
class RedirectInfo:
    """
    重定向信息数据类
    
    记录请求过程中发生的重定向信息，用于：
    - 追踪完整的重定向链路
    - 统计重定向次数
    - 提供调试和日志信息
    
    属性:
        original_url: 最初请求的 URL
        redirect_url: 最终重定向到的 URL（可能是原始 URL，如果没有重定向）
        status_code: 最终响应的状态码
        redirect_count: 发生的重定向次数
        redirect_chain: 重定向链列表，记录每一步重定向的详细信息
    
    示例:
        如果请求 http://a.com 重定向到 http://b.com 再到 http://c.com：
        - original_url = "http://a.com"
        - redirect_url = "http://c.com"
        - redirect_count = 2
        - redirect_chain = [
            {'from': 'http://a.com', 'to': 'http://b.com', 'status': 301},
            {'from': 'http://b.com', 'to': 'http://c.com', 'status': 302}
          ]
    """
    original_url: str           # 原始请求 URL
    redirect_url: str           # 最终 URL
    status_code: int            # 最终状态码
    redirect_count: int         # 重定向次数
    redirect_chain: list        # 重定向链（每一步的详细信息）


@dataclass
class StreamingResponse:
    """
    流式响应数据类
    
    封装流式 HTTP 响应的所有信息，用于：
    - 流式传输响应内容
    - 传递响应头和状态码
    - 携带重定向信息
    
    属性:
        status: HTTP 状态码（如 200, 206, 404 等）
        headers: 响应头字典
        body_stream: 响应体异步生成器，按需产生数据块
        redirect_info: 重定向信息（如果有重定向发生）
        content_length: 内容长度（字节），可选
    
    设计说明:
        body_stream 使用异步生成器实现惰性读取：
        - 数据按需读取，不会一次性加载到内存
        - 支持任意大小的文件传输
        - 客户端断开连接时可及时停止读取
    """
    status: int                                     # HTTP 状态码
    headers: Dict[str, str]                         # 响应头字典
    body_stream: Optional[AsyncGenerator[bytes, None]]  # 响应体异步生成器
    redirect_info: Optional[RedirectInfo]           # 重定向信息
    content_length: Optional[int] = None            # 内容长度（可选）


class RedirectHandler:
    """
    重定向处理器
    
    处理 HTTP 重定向响应，自动跟踪 301/302/303/307/308 状态码。
    支持流式和非流式两种模式。
    
    核心功能：
    1. 自动跟踪重定向链
    2. 正确处理不同类型的重定向状态码
    3. 防止无限重定向循环
    4. 支持自定义超时配置
    
    重定向状态码处理规则（遵循 HTTP 规范）：
    - 301 Moved Permanently: 永久重定向，POST/PUT 可能转为 GET
    - 302 Found: 临时重定向，POST/PUT 可能转为 GET
    - 303 See Other: 总是转为 GET
    - 307 Temporary Redirect: 保持原请求方法
    - 308 Permanent Redirect: 保持原请求方法
    
    属性:
        max_redirects: 最大重定向次数，防止无限循环
        timeout: 普通请求超时配置
        stream_timeout: 流式请求超时配置（通常更长）
    """
    
    def __init__(self, max_redirects: int = 10, timeout: int = 30, stream_timeout: int = 3600):
        """
        初始化重定向处理器
        
        参数:
            max_redirects: 最大重定向次数，默认 10 次
                          超过此次数将停止跟踪并返回错误
            timeout: 普通请求超时时间（秒），默认 30 秒
            stream_timeout: 流式请求超时时间（秒），默认 3600 秒（1小时）
                           流式请求通常需要更长的超时时间
        """
        self.max_redirects = max_redirects
        
        # 创建普通请求的超时配置
        # total: 整个请求的总超时时间
        # connect: 建立连接的超时时间
        self.timeout = aiohttp.ClientTimeout(total=timeout, connect=timeout)
        
        # 创建流式请求的超时配置
        # 流式请求需要更长的超时时间，因为数据传输可能持续较长时间
        self.stream_timeout = aiohttp.ClientTimeout(
            total=stream_timeout,
            connect=timeout,
            sock_read=timeout  # socket 读取超时
        )
    
    async def follow_redirects_streaming(
        self,
        url: str,
        method: str = 'GET',
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Tuple[Optional[aiohttp.ClientResponse], RedirectInfo]:
        """
        跟踪重定向（流式模式）
        
        发送请求并自动跟踪重定向，返回流式响应。
        与 follow_redirects 的区别：
        - 不自动读取响应体
        - 返回的 response 对象需要手动关闭
        - 适用于大文件和流媒体传输
        
        参数:
            url: 请求的 URL
            method: HTTP 方法（GET, POST, PUT 等）
            headers: 请求头字典
            body: 请求体（用于 POST/PUT 等方法）
            session: aiohttp 会话对象，如果为 None 则创建新会话
        
        返回:
            元组 (response, redirect_info):
            - response: 最终的响应对象，可能为 None（超过最大重定向次数）
            - redirect_info: 重定向信息对象
        
        注意:
            - 如果传入了 session，调用者负责关闭 response
            - 如果未传入 session，方法内部创建的 session 在异常时自动关闭
            - 正常返回时，调用者需要关闭 response
        
        实现思路:
            使用 while 循环跟踪重定向链：
            1. 发送请求，禁止自动重定向（allow_redirects=False）
            2. 检查状态码是否为重定向状态码
            3. 如果是，获取 Location 头并构建新的 URL
            4. 根据状态码类型决定是否改变请求方法
            5. 继续请求新 URL，直到获得非重定向响应
        """
        # 初始化重定向跟踪变量
        redirect_chain = []       # 记录重定向链
        current_url = url         # 当前请求的 URL
        redirect_count = 0        # 已发生的重定向次数
        own_session = session is None  # 是否需要自己管理 session
        
        # 如果没有传入 session，创建一个新的
        if own_session:
            session = aiohttp.ClientSession(timeout=self.stream_timeout)
        
        try:
            # 循环跟踪重定向，直到达到最大次数或获得最终响应
            while redirect_count <= self.max_redirects:
                try:
                    # 复制请求头，避免修改原始 headers
                    request_headers = headers.copy() if headers else {}
                    
                    # 根据HTTP方法决定是否发送请求体
                    # GET/HEAD/OPTIONS/TRACE 通常不发送请求体
                    if method.upper() in ['GET', 'HEAD', 'OPTIONS', 'TRACE']:
                        request_body = None
                    else:
                        request_body = body
                    
                    # 发送请求
                    # allow_redirects=False 禁止 aiohttp 自动跟踪重定向
                    # ssl=False 禁用 SSL 证书验证（适用于自签名证书）
                    response = await session.request(
                        method=method,
                        url=current_url,
                        headers=request_headers,
                        data=request_body,
                        allow_redirects=False,
                        ssl=False
                    )
                    
                    # ============================================================
                    # 处理重定向状态码
                    # ============================================================
                    # 301: 永久重定向
                    # 302: 临时重定向（Found）
                    # 303: See Other，要求使用 GET
                    # 307: 临时重定向，保持方法
                    # 308: 永久重定向，保持方法
                    if response.status in (301, 302, 303, 307, 308):
                        # 获取重定向目标 URL
                        location = response.headers.get('Location')
                        if not location:
                            # 重定向状态但没有 Location 头，这是异常情况
                            logger.warning(f"重定向状态 {response.status} 但没有 Location 头")
                            return response, RedirectInfo(
                                original_url=url,
                                redirect_url=current_url,
                                status_code=response.status,
                                redirect_count=redirect_count,
                                redirect_chain=redirect_chain
                            )
                        
                        # 构建完整的重定向 URL
                        # urljoin 可以正确处理相对路径和绝对路径
                        redirect_url = urljoin(current_url, location)
                        
                        # 记录重定向步骤
                        redirect_chain.append({
                            'from': current_url,
                            'to': redirect_url,
                            'status': response.status
                        })
                        
                        logger.info(f"重定向 {redirect_count + 1}: {response.status} {current_url} -> {redirect_url}")
                        
                        # 关闭当前响应，准备下一次请求
                        response.close()
                        
                        # 更新 URL 和计数器
                        current_url = redirect_url
                        redirect_count += 1
                        
                        # ========================================================
                        # 根据状态码调整请求方法和请求体
                        # ========================================================
                        # 303 See Other: 总是转为 GET 方法
                        if response.status == 303:
                            method = 'GET'
                            body = None
                        # 301/302: 对于非 GET/HEAD 方法，转为 GET
                        # 这是历史遗留行为，某些客户端和服务器的约定
                        elif response.status in (301, 302) and method.upper() not in ['GET', 'HEAD']:
                            method = 'GET'
                            body = None
                        # 307/308: 保持原请求方法和请求体
                        # 这些状态码明确要求保持原方法
                    
                    # ============================================================
                    # 非重定向状态码：返回最终响应
                    # ============================================================
                    else:
                        return response, RedirectInfo(
                            original_url=url,
                            redirect_url=current_url,
                            status_code=response.status,
                            redirect_count=redirect_count,
                            redirect_chain=redirect_chain
                        )
                
                # ============================================================
                # 异常处理
                # ============================================================
                except asyncio.TimeoutError:
                    logger.error(f"请求超时: {current_url}")
                    raise
                except aiohttp.ClientError as e:
                    logger.error(f"请求客户端错误 {current_url}: {e}")
                    raise
            
            # ================================================================
            # 超过最大重定向次数
            # ================================================================
            logger.warning(f"超过最大重定向次数 ({self.max_redirects}): {url}")
            return None, RedirectInfo(
                original_url=url,
                redirect_url=current_url,
                status_code=310,  # 自定义状态码，表示重定向次数过多
                redirect_count=redirect_count,
                redirect_chain=redirect_chain
            )
        
        # 异常情况下关闭自己创建的 session
        except Exception:
            if own_session:
                await session.close()
            raise
    
    async def follow_redirects(
        self,
        url: str,
        method: str = 'GET',
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Tuple[Optional[aiohttp.ClientResponse], RedirectInfo]:
        """
        跟踪重定向（非流式模式）
        
        发送请求并自动跟踪重定向，返回完整响应。
        与 follow_redirects_streaming 的区别：
        - 使用 async with 自动管理响应生命周期
        - 响应体在返回前已被读取
        - 适用于小文件和普通请求
        
        参数:
            url: 请求的 URL
            method: HTTP 方法（GET, POST, PUT 等）
            headers: 请求头字典
            body: 请求体（用于 POST/PUT 等方法）
            session: aiohttp 会话对象，如果为 None 则创建新会话
        
        返回:
            元组 (response, redirect_info):
            - response: 最终的响应对象，可能为 None（超过最大重定向次数）
            - redirect_info: 重定向信息对象
        
        注意:
            - 返回的 response 已在 async with 块外，需要调用者处理
            - 如果需要读取响应体，应在返回前完成
        """
        # 初始化重定向跟踪变量
        redirect_chain = []
        current_url = url
        redirect_count = 0
        own_session = session is None
        
        # 如果没有传入 session，创建一个新的
        if own_session:
            session = aiohttp.ClientSession(timeout=self.timeout)
        
        try:
            while redirect_count <= self.max_redirects:
                try:
                    request_headers = headers.copy() if headers else {}
                    
                    if method.upper() in ['GET', 'HEAD', 'OPTIONS', 'TRACE']:
                        request_body = None
                    else:
                        request_body = body
                    
                    # 使用 async with 自动管理响应生命周期
                    async with session.request(
                        method=method,
                        url=current_url,
                        headers=request_headers,
                        data=request_body,
                        allow_redirects=False,
                        ssl=False
                    ) as response:
                        # 处理重定向状态码（逻辑与流式模式相同）
                        if response.status in (301, 302, 303, 307, 308):
                            location = response.headers.get('Location')
                            if not location:
                                logger.warning(f"重定向状态 {response.status} 但没有 Location 头")
                                return response, RedirectInfo(
                                    original_url=url,
                                    redirect_url=current_url,
                                    status_code=response.status,
                                    redirect_count=redirect_count,
                                    redirect_chain=redirect_chain
                                )
                            
                            redirect_url = urljoin(current_url, location)
                            redirect_chain.append({
                                'from': current_url,
                                'to': redirect_url,
                                'status': response.status
                            })
                            
                            logger.info(f"重定向 {redirect_count + 1}: {response.status} {current_url} -> {redirect_url}")
                            
                            current_url = redirect_url
                            redirect_count += 1
                            
                            if response.status == 303:
                                method = 'GET'
                                body = None
                            elif response.status in (301, 302) and method.upper() not in ['GET', 'HEAD']:
                                method = 'GET'
                                body = None
                        else:
                            return response, RedirectInfo(
                                original_url=url,
                                redirect_url=current_url,
                                status_code=response.status,
                                redirect_count=redirect_count,
                                redirect_chain=redirect_chain
                            )
                
                except asyncio.TimeoutError:
                    logger.error(f"请求超时: {current_url}")
                    raise
                except aiohttp.ClientError as e:
                    logger.error(f"请求客户端错误 {current_url}: {e}")
                    raise
            
            logger.warning(f"超过最大重定向次数 ({self.max_redirects}): {url}")
            return None, RedirectInfo(
                original_url=url,
                redirect_url=current_url,
                status_code=310,
                redirect_count=redirect_count,
                redirect_chain=redirect_chain
            )
        
        finally:
            # 确保关闭自己创建的 session
            if own_session:
                await session.close()


class ProxyRequestHandler:
    """
    代理请求处理器
    
    处理客户端的代理请求，将其转发到目标服务器。
    这是代理服务器的核心业务逻辑处理类。
    
    主要职责：
    1. 路由匹配 - 根据请求路径找到匹配的代理规则
    2. URL 构建 - 将请求路径转换为目标服务器 URL
    3. 请求头处理 - 过滤和添加必要的请求头
    4. 响应处理 - 处理上游服务器的响应
    5. 错误处理 - 处理超时、重试等异常情况
    
    属性:
        config: 配置对象
        redirect_handler: 重定向处理器
        _session: aiohttp 客户端会话（连接池）
    
    设计模式:
        - 单例模式：会话对象复用，避免频繁创建连接
        - 策略模式：根据配置选择流式或非流式处理
    """
    
    def __init__(self, config: Config):
        """
        初始化代理请求处理器
        
        参数:
            config: 配置对象，包含所有配置参数
        """
        self.config = config
        
        # 创建重定向处理器
        # 使用配置中的最大重定向次数和超时设置
        self.redirect_handler = RedirectHandler(
            max_redirects=config.max_redirects,
            timeout=config.default_timeout,
            stream_timeout=config.streaming.stream_timeout if hasattr(config, 'streaming') else 3600
        )
        
        # aiohttp 客户端会话，延迟初始化
        # 使用连接池复用 TCP 连接，提高性能
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        """
        获取或创建 aiohttp 客户端会话
        
        实现延迟初始化和会话复用：
        - 第一次调用时创建会话
        - 后续调用返回同一个会话
        - 如果会话已关闭，重新创建
        
        返回:
            aiohttp.ClientSession 实例
        
        会话配置说明:
            - timeout: 超时配置
            - connector: 连接器配置
              - limit: 最大连接数
              - limit_per_host: 每个主机的最大连接数
              - enable_cleanup_closed: 自动清理已关闭的连接
              - force_close: 是否强制关闭连接（False 表示复用）
        """
        # 检查会话是否存在且未关闭
        if self._session is None or self._session.closed:
            # 获取流式传输超时配置
            stream_timeout = self.config.streaming.stream_timeout if hasattr(self.config, 'streaming') else 3600
            
            # 创建超时配置
            timeout = aiohttp.ClientTimeout(
                total=stream_timeout,  # 总超时时间
                connect=self.config.default_timeout,  # 连接超时
                sock_read=self.config.streaming.read_timeout if hasattr(self.config, 'streaming') else 300  # 读取超时
            )
            
            # 创建连接器（连接池）
            connector = aiohttp.TCPConnector(
                limit=self.config.server.max_connections,  # 最大连接数
                limit_per_host=self.config.server.max_connections_per_host,  # 每主机最大连接数
                enable_cleanup_closed=True,  # 自动清理已关闭连接
                force_close=False  # 允许连接复用
            )
            
            # 创建会话
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector
            )
        
        return self._session
    
    async def close(self):
        """
        关闭请求处理器
        
        清理资源，关闭客户端会话。
        应在服务器关闭时调用。
        """
        if self._session and not self._session.closed:
            await self._session.close()
    
    def find_matching_rule(self, path: str) -> Optional[ProxyRule]:
        """
        查找匹配的代理规则
        
        根据请求路径查找第一个匹配的代理规则。
        匹配规则：路径以规则的 path_prefix 开头。
        
        参数:
            path: 请求路径（如 /api/users）
        
        返回:
            匹配的 ProxyRule 对象，如果没有匹配则返回 None
        
        注意:
            - 使用第一个匹配的规则（按配置顺序）
            - 前缀匹配，不是精确匹配
        """
        for rule in self.config.proxy_rules:
            if path.startswith(rule.path_prefix):
                return rule
        return None
    
    def build_target_url(self, path: str, rule: ProxyRule, query_string: str = None) -> str:
        """
        构建目标 URL
        
        根据代理规则将请求路径转换为目标服务器的 URL。
        
        参数:
            path: 原始请求路径（URL 解码后的路径）
            rule: 匹配的代理规则
            query_string: 查询字符串（可选，不含 ? 前缀）
        
        返回:
            目标服务器的完整 URL
        
        处理逻辑:
            - 如果 strip_prefix 为 True： 移除路径前缀
            - 如果 strip_prefix 为 False： 保留完整路径
            - 如果有查询字符串，添加到 URL 末尾
        
        示例:
            path = "/api/v1/users"
            rule.path_prefix = "/api/v1"
            rule.target_url = "http://backend.example.com"
            
            如果 strip_prefix=True:
                target_url = "http://backend.example.com/users"
            如果 strip_prefix=False:
                target_url = "http://backend.example.com/api/v1/users"
        """
        from urllib.parse import urljoin, urlparse, urlunparse, quote
        
        # 解析目标 URL 的组成部分
        parsed_target = urlparse(rule.target_url)
        
        if rule.strip_prefix:
            # 移除路径前缀
            remaining_path = path[len(rule.path_prefix):]
            # 确保路径以 / 开头
            if not remaining_path.startswith('/'):
                remaining_path = '/' + remaining_path
            # 构建新的路径
            new_path = parsed_target.path + remaining_path
        else:
            # 保留完整路径
            new_path = parsed_target.path + path
        
        # 重新构建 URL
        # 使用 quote 对路径进行编码，避免 urljoin 的编码问题
        # 注意：quote 会对路径进行安全编码，保留 / 和特殊字符
        encoded_path = quote(new_path, safe='/')
        base_url = f"{parsed_target.scheme}://{parsed_target.netloc}{encoded_path}"
        
        # 添加查询字符串（如果有）
        if query_string:
            return f"{base_url}?{query_string}"
        return base_url
    
    def filter_headers(self, headers: Dict[str, str], is_request: bool = True) -> Dict[str, str]:
        """
        过滤请求头/响应头
        
        移除逐跳头部（hop-by-hop headers），这些头部不应该被转发。
        
        逐跳头部是只对单次连接有效的头部，包括：
        - Connection: 连接控制
        - Keep-Alive: 保持连接
        - Proxy-Authenticate: 代理认证
        - Proxy-Authorization: 代理授权
        - TE: 传输编码
        - Trailers: 尾部头部
        - Transfer-Encoding: 传输编码
        - Upgrade: 协议升级
        
        参数:
            headers: 原始头部字典
            is_request: True 表示请求头，False 表示响应头
        
        返回:
            过滤后的头部字典
        
        注意:
            使用配置中的 hop_by_hop_headers 列表进行过滤
        """
        filtered = {}
        # 将配置中的逐跳头部转换为小写集合，便于比较
        hop_by_hop = set(h.lower() for h in self.config.hop_by_hop_headers)
        
        for key, value in headers.items():
            key_lower = key.lower()
            # 只保留非逐跳头部
            if key_lower not in hop_by_hop:
                filtered[key] = value
        
        return filtered
    
    def add_forward_headers(self, headers: Dict[str, str], client_host: str, scheme: str) -> Dict[str, str]:
        """
        添加转发头部
        
        添加标准的代理转发头部，用于告知目标服务器原始请求的信息。
        
        参数:
            headers: 请求头字典（会被修改）
            client_host: 客户端 IP 地址
            scheme: 原始请求的协议（http/https）
        
        返回:
            添加了转发头部的字典
        
        添加的头部:
            - X-Forwarded-For: 客户端 IP 地址
            - X-Forwarded-Proto: 原始请求协议
            - X-Forwarded-Host: 原始请求的 Host 头
        
        注意:
            只有在 trust_forward_headers 配置为 True 时才添加
        """
        original_host = headers.get('Host', '')

        if self.config.trust_forward_headers:
            existing_forwarded_for = headers.get('X-Forwarded-For', '').strip()
            if existing_forwarded_for and client_host:
                headers['X-Forwarded-For'] = f"{existing_forwarded_for}, {client_host}"
            else:
                headers['X-Forwarded-For'] = existing_forwarded_for or client_host

            headers['X-Forwarded-Proto'] = scheme
            headers['X-Forwarded-Host'] = original_host

        # Host 必须由 aiohttp 根据当前上游 URL 自动生成。
        # 如果把客户端的 Host 原样透传到上游，虚拟主机场景下会直接命中错误站点并返回 4xx。
        headers.pop('Host', None)
        return headers
    
    def is_streaming_content(self, headers: Dict[str, str], content_length: Optional[int] = None) -> bool:
        """
        判断是否为流式内容
        
        根据响应头和内容长度判断是否需要使用流式传输。
        
        参数:
            headers: 响应头字典
            content_length: 内容长度（字节），可选
        
        返回:
            True 表示应该使用流式传输，False 表示普通传输
        
        判断依据:
            1. Content-Type 为流式媒体类型（video, audio 等）
            2. Transfer-Encoding 为 chunked
            3. 文件大小超过阈值
        """
        # 获取内容类型
        content_type = headers.get('Content-Type', '').lower()
        
        # 定义流式内容类型列表
        streaming_types = [
            'video/',                    # 视频文件
            'audio/',                    # 音频文件
            'application/octet-stream',  # 二进制流
            'application/x-mpegurl',     # HLS 播放列表
            'application/vnd.apple.mpegurl',  # Apple HLS
            'application/dash+xml',      # DASH 流媒体
            'multipart/'                 # 多部分内容
        ]
        
        # 检查内容类型
        for stream_type in streaming_types:
            if stream_type in content_type:
                return True
        
        # 检查是否为分块传输
        transfer_encoding = headers.get('Transfer-Encoding', '').lower()
        if 'chunked' in transfer_encoding:
            return True
        
        # 检查文件大小
        if content_length is not None:
            large_file_threshold = self.config.streaming.large_file_threshold if hasattr(self.config, 'streaming') else 10 * 1024 * 1024
            if content_length > large_file_threshold:
                return True
        
        return False
    
    async def stream_response(
        self,
        response: aiohttp.ClientResponse,
        chunk_size: int = 64 * 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        流式读取响应体
        
        创建一个异步生成器，按块读取响应体。
        
        参数:
            response: aiohttp 响应对象
            chunk_size: 每次读取的块大小（字节），默认 64KB
        
        生成:
            bytes: 响应体的数据块
        
        异常处理:
            - CancelledError: 客户端取消请求，记录日志并重新抛出
            - ConnectionResetError/BrokenPipeError: 连接断开，记录日志并抛出
            - 其他异常: 记录错误日志并抛出
        
        清理:
            无论成功还是失败，都会关闭响应对象
        """
        try:
            # 按块读取响应体
            async for chunk in response.content.iter_chunked(chunk_size):
                yield chunk
        except asyncio.CancelledError:
            logger.info("流传输被客户端取消")
            raise
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
            logger.info(f"客户端断开连接: {type(e).__name__}")
            raise
        except Exception as e:
            logger.error(f"流传输错误: {e}")
            raise
        finally:
            # 确保关闭响应对象
            response.close()
    
    async def handle_request_streaming(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        client_host: str,
        scheme: str = 'http',
        query_string: str = None
    ) -> StreamingResponse:
        """
        处理流式代理请求
        
        将客户端请求转发到目标服务器，以流式方式返回响应。
        支持重定向跟踪、重试机制。
        
        参数:
            method: HTTP 方法
            path: 请求路径（URL 解码后的路径）
            headers: 请求头字典
            body: 请求体（可选）
            client_host: 客户端 IP 地址
            scheme: 请求协议（http/https）
        
        返回:
            StreamingResponse 对象，包含状态码、响应头、数据流等
        
        处理流程:
            1. 查找匹配的代理规则
            2. 构建目标 URL
            3. 处理请求头
            4. 发送请求并跟踪重定向
            5. 处理响应头
            6. 返回流式响应
        
        重试机制:
            如果请求失败（超时或网络错误），会自动重试。
            重试次数由代理规则的 retry_times 配置决定。
        """
        # 查找匹配的代理规则
        rule = self.find_matching_rule(path)
        
        # 没有匹配的规则，返回 404 错误
        if not rule:
            async def error_stream():
                yield b'{"error": "No matching proxy rule found"}'
            
            return StreamingResponse(
                status=404,
                headers={'Content-Type': 'application/json'},
                body_stream=error_stream(),
                redirect_info=None
            )
        
        # 构建目标 URL
        target_url = self.build_target_url(path, rule, query_string)
        logger.info(f"代理流式请求: {method} {path} -> {target_url}")
        
        # 获取客户端会话
        session = await self.get_session()
        
        # 处理请求头
        request_headers = self.filter_headers(headers, is_request=True)
        request_headers = self.add_forward_headers(request_headers, client_host, scheme)
        
        # 创建重定向处理器（使用规则中的配置）
        redirect_handler = RedirectHandler(
            max_redirects=rule.max_redirects,
            timeout=rule.timeout,
            stream_timeout=self.config.streaming.stream_timeout if hasattr(self.config, 'streaming') else 3600
        )
        
        # 重试机制
        retry_count = 0
        last_error = None
        last_error_type = None
        
        while retry_count < rule.retry_times:
            try:
                logger.debug(f"发送流式请求到 {target_url}, 尝试 {retry_count + 1}/{rule.retry_times}")
                
                # 发送请求并跟踪重定向
                response, redirect_info = await redirect_handler.follow_redirects_streaming(
                    url=target_url,
                    method=method,
                    headers=request_headers,
                    body=body,
                    session=session
                )
                
                # 超过最大重定向次数
                if response is None:
                    logger.error(f"流式请求失败: 超过最大重定向次数 ({rule.max_redirects}), URL: {target_url}")
                    async def error_stream():
                        yield b'{"error": "Too many redirects"}'
                    
                    return StreamingResponse(
                        status=502,
                        headers={'Content-Type': 'application/json'},
                        body_stream=error_stream(),
                        redirect_info=redirect_info
                    )
                
                # 处理响应头
                response_headers = dict(response.headers)
                filtered_headers = self.filter_headers(response_headers, is_request=False)
                
                # 解析 Content-Length
                content_length = None
                if 'Content-Length' in response_headers:
                    try:
                        content_length = int(response_headers['Content-Length'])
                    except ValueError:
                        pass
                
                # 确保 Accept-Ranges 头存在（支持断点续传）
                if 'Accept-Ranges' not in filtered_headers:
                    filtered_headers['Accept-Ranges'] = 'bytes'
                
                # 记录重定向信息
                if redirect_info.redirect_count > 0:
                    logger.info(
                        f"流式请求完成，共 {redirect_info.redirect_count} 次重定向: "
                        f"{redirect_info.original_url} -> {redirect_info.redirect_url}"
                    )
                
                logger.info(f"流式请求成功: {target_url}, 状态码: {response.status}")
                
                # 创建流式响应体
                chunk_size = self.config.streaming.chunk_size if hasattr(self.config, 'streaming') else 64 * 1024
                body_stream = self.stream_response(response, chunk_size)
                
                return StreamingResponse(
                    status=response.status,
                    headers=filtered_headers,
                    body_stream=body_stream,
                    redirect_info=redirect_info,
                    content_length=content_length
                )
            
            # 超时错误处理
            except asyncio.TimeoutError as e:
                last_error = str(e)
                last_error_type = "TimeoutError"
                retry_count += 1
                logger.warning(f"流式请求超时 {target_url}, 重试 {retry_count}/{rule.retry_times}, 错误: {e}")
                await asyncio.sleep(1)  # 等待 1 秒后重试
            
            # 客户端错误处理
            except aiohttp.ClientError as e:
                last_error = str(e)
                last_error_type = type(e).__name__
                retry_count += 1
                logger.warning(f"流式请求客户端错误 {target_url}: {type(e).__name__}: {e}, 重试 {retry_count}/{rule.retry_times}")
                await asyncio.sleep(1)
            
            # 其他异常处理
            except Exception as e:
                last_error = str(e)
                last_error_type = type(e).__name__
                retry_count += 1
                logger.error(f"流式请求未知错误 {target_url}: {type(e).__name__}: {e}, 重试 {retry_count}/{rule.retry_times}")
                await asyncio.sleep(1)
        
        # 所有重试都失败
        logger.error(f"流式请求最终失败: {target_url}, 重试次数: {rule.retry_times}, 最后错误类型: {last_error_type}, 错误: {last_error}")
        async def error_stream():
            yield f'{{"error": "Failed after {rule.retry_times} retries: {last_error_type}: {last_error}"}}'.encode()
        
        return StreamingResponse(
            status=502,
            headers={'Content-Type': 'application/json'},
            body_stream=error_stream(),
            redirect_info=None
        )
    
    async def handle_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        client_host: str,
        scheme: str = 'http',
        query_string: str = None
    ) -> Tuple[int, Dict[str, str], bytes, Optional[RedirectInfo]]:
        """
        处理普通代理请求
        
        将客户端请求转发到目标服务器，返回完整响应。
        适用于小文件和普通请求。
        
        参数:
            method: HTTP 方法
            path: 请求路径（URL 解码后的路径）
            headers: 请求头字典
            body: 请求体（可选）
            client_host: 客户端 IP 地址
            scheme: 请求协议（http/https）
            query_string: 查询字符串（可选，不含 ? 前缀）
        
        返回:
            元组 (status, headers, body, redirect_info):
            - status: HTTP 状态码
            - headers: 响应头字典
            - body: 响应体字节
            - redirect_info: 重定向信息（可选）
        
        与 handle_request_streaming 的区别:
            - 响应体完整读取到内存
            - 不支持断点续传
            - 适用于小文件和 API 请求
        """
        # 查找匹配的代理规则
        rule = self.find_matching_rule(path)
        
        # 没有匹配的规则
        if not rule:
            error_body = b'{"error": "No matching proxy rule found"}'
            return 404, {'Content-Type': 'application/json'}, error_body, None
        
        # 构建目标 URL
        target_url = self.build_target_url(path, rule, query_string)
        logger.info(f"代理请求: {method} {path} -> {target_url}")
        
        # 获取会话
        session = await self.get_session()
        
        # 处理请求头
        request_headers = self.filter_headers(headers, is_request=True)
        request_headers = self.add_forward_headers(request_headers, client_host, scheme)
        
        # 创建重定向处理器
        redirect_handler = RedirectHandler(
            max_redirects=rule.max_redirects,
            timeout=rule.timeout
        )
        
        # 重试机制
        retry_count = 0
        last_error = None
        last_error_type = None
        
        while retry_count < rule.retry_times:
            try:
                # 发送请求并跟踪重定向
                response, redirect_info = await redirect_handler.follow_redirects(
                    url=target_url,
                    method=method,
                    headers=request_headers,
                    body=body,
                    session=session
                )
                
                # 超过最大重定向次数
                if response is None:
                    logger.error(f"请求失败 {target_url}: 超过最大重定向次数 {rule.max_redirects}")
                    error_body = b'{"error": "Too many redirects"}'
                    return 502, {'Content-Type': 'application/json'}, error_body, redirect_info
                
                # 处理响应头
                response_headers = dict(response.headers)
                filtered_headers = self.filter_headers(response_headers, is_request=False)
                
                # 读取完整响应体
                response_body = await response.read()
                
                # 记录重定向信息
                if redirect_info.redirect_count > 0:
                    logger.info(
                        f"请求完成，共 {redirect_info.redirect_count} 次重定向: "
                        f"{redirect_info.original_url} -> {redirect_info.redirect_url}"
                    )
                
                return response.status, filtered_headers, response_body, redirect_info
            
            
            except asyncio.TimeoutError as e:
                last_error = str(e)
                last_error_type = "TimeoutError"
                retry_count += 1
                logger.warning(f"请求超时 {target_url}, 重试 {retry_count}/{rule.retry_times}")
                await asyncio.sleep(1)
            
            except aiohttp.ClientError as e:
                last_error = str(e)
                last_error_type = type(e).__name__
                retry_count += 1
                logger.warning(f"客户端错误 {target_url}: {type(e).__name__}: {e}, 重试 {retry_count}/{rule.retry_times}")
                await asyncio.sleep(1)
            
            except Exception as e:
                last_error = str(e)
                last_error_type = type(e).__name__
                retry_count += 1
                logger.error(f"请求未知错误 {target_url}: {type(e).__name__}: {e}, 重试 {retry_count}/{rule.retry_times}")
                await asyncio.sleep(1)
        
        # 所有重试都失败
        logger.error(f"请求最终失败 {target_url}, 重试次数: {rule.retry_times}, 最后错误类型: {last_error_type}, 错误: {last_error}")
        error_body = f'{{"error": "Failed after {rule.retry_times} retries: {last_error_type}: {last_error}"}}'.encode()
        return 502, {'Content-Type': 'application/json'}, error_body, None


class ProxyStats:
    """
    代理统计收集器
    
    收集和统计代理服务器的运行数据，包括：
    - 请求总数
    - 重定向请求数
    - 失败请求数
    - 流式请求数
    - 传输字节数
    - 运行时间
    
    线程安全：
        使用 asyncio.Lock 确保并发安全。
    
    属性:
        total_requests: 总请求数
        redirected_requests: 发生重定向的请求数
        failed_requests: 失败请求数
        total_redirects: 总重定向次数（可能大于 redirected_requests）
        streaming_requests: 流式请求数
        bytes_transferred: 传输的总字节数
        start_time: 统计开始时间
        _lock: 异步锁，确保并发安全
    """
    
    def __init__(self):
        """初始化统计收集器"""
        self.total_requests = 0          # 总请求数
        self.redirected_requests = 0     # 发生重定向的请求数
        self.failed_requests = 0         # 失败请求数
        self.total_redirects = 0         # 总重定向次数
        self.streaming_requests = 0      # 流式请求数
        self.bytes_transferred = 0       # 传输字节数
        self.start_time = time.time()    # 统计开始时间
        self._lock = asyncio.Lock()      # 异步锁
    
    async def record_request(
        self,
        redirected: bool = False,
        redirect_count: int = 0,
        failed: bool = False,
        streaming: bool = False,
        bytes_count: int = 0
    ):
        """
        记录一次请求的统计信息
        
        参数:
            redirected: 是否发生了重定向
            redirect_count: 重定向次数
            failed: 是否失败
            streaming: 是否为流式请求
            bytes_count: 传输的字节数
        
        线程安全:
            使用异步锁确保并发安全
        """
        async with self._lock:
            self.total_requests += 1
            if redirected:
                self.redirected_requests += 1
                self.total_redirects += redirect_count
            if failed:
                self.failed_requests += 1
            if streaming:
                self.streaming_requests += 1
            self.bytes_transferred += bytes_count
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息
        
        返回:
            包含统计信息的字典：
            - total_requests: 总请求数
            - redirected_requests: 重定向请求数
            - failed_requests: 失败请求数
            - total_redirects: 总重定向次数
            - streaming_requests: 流式请求数
            - bytes_transferred: 传输字节数
            - uptime_seconds: 运行时间（秒）
            - requests_per_second: 每秒请求数
            - mb_transferred: 传输的 MB 数
        """
        # 计算运行时间
        uptime = time.time() - self.start_time
        
        return {
            'total_requests': self.total_requests,
            'redirected_requests': self.redirected_requests,
            'failed_requests': self.failed_requests,
            'total_redirects': self.total_redirects,
            'streaming_requests': self.streaming_requests,
            'bytes_transferred': self.bytes_transferred,
            'uptime_seconds': round(uptime, 2),
            'requests_per_second': round(self.total_requests / uptime, 2) if uptime > 0 else 0,
            'mb_transferred': round(self.bytes_transferred / (1024 * 1024), 2)
        }
