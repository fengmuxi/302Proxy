"""
配置管理模块

本模块负责代理服务器的配置管理，包括：
1. 配置数据结构定义 - 使用 dataclass 定义配置类
2. 配置文件加载 - 从 YAML 文件加载配置
3. 配置文件保存 - 将配置保存到 YAML 文件
4. 日志系统初始化 - 设置日志格式和输出

模块结构：
- parse_size: 大小解析工具函数
- ProxyRule: 代理规则配置类
- ServerConfig: 服务器配置类
- SSLConfig: SSL 配置类
- LoggingConfig: 日志配置类
- StreamingConfig: 流式传输配置类
- CacheConfig: 缓存配置类
- Config: 主配置类
- setup_logging: 日志系统初始化函数
- load_config: 配置加载函数

配置文件格式：
    server:
      host: "0.0.0.0"
      port: 8080
    streaming:
      enabled: true
      chunk_size: "64KB"
    cache:
      enabled: true
      max_size: "5GB"
    proxy_rules:
      - path_prefix: "/api"
        target_url: "http://backend.example.com"

作者: nginx302_proxy 项目
"""

import yaml
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
import logging


def parse_size(value: Union[str, int]) -> int:
    """
    将带单位的字符串转换为字节数
    
    解析配置文件中的大小值，支持多种单位格式。
    这是配置解析的辅助函数，用于将人类可读的大小值转换为字节数。
    
    参数:
        value: 大小值，可以是字符串或整数
               字符串格式：数字 + 单位（如 "10MB", "64KB"）
               整数格式：直接作为字节数
    
    返回:
        转换后的字节数（整数）
    
    支持的单位:
        - B: 字节
        - KB: 千字节 (1024 字节)
        - MB: 兆字节 (1024 * 1024 字节)
        - GB: 吉字节 (1024^3 字节)
        - TB: 太字节 (1024^4 字节)
    
    示例:
        >>> parse_size("10MB")
        10485760
        >>> parse_size("64KB")
        65536
        >>> parse_size("10GB")
        10737418240
        >>> parse_size(100)
        100
    
    异常:
        ValueError: 当字符串格式无法解析时抛出
    
    注意:
        - 单位不区分大小写
        - 如果没有单位，默认为字节
        - 支持小数（如 "1.5GB"）
    """
    # 如果已经是整数，直接返回
    if isinstance(value, int):
        return value
    
    # 如果不是字符串，尝试转换为整数
    if not isinstance(value, str):
        return int(value)
    
    # 转换为大写并去除首尾空格
    value = value.strip().upper()
    
    # 定义单位到字节数的映射
    # 使用二进制前缀（1KB = 1024B），而非十进制前缀（1KB = 1000B）
    units = {
        'B': 1,                                    # 字节
        'KB': 1024,                                # 千字节
        'MB': 1024 * 1024,                         # 兆字节
        'GB': 1024 * 1024 * 1024,                  # 吉字节
        'TB': 1024 * 1024 * 1024 * 1024            # 太字节
    }
    
    # 使用正则表达式解析数字和单位
    # 匹配格式：数字（可选小数）+ 可选单位
    match = re.match(r'^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)?$', value)
    if not match:
        raise ValueError(f"无法解析大小值：{value}")
    
    # 提取数字部分
    number = float(match.group(1))
    # 提取单位部分，如果没有单位则默认为 B
    unit = match.group(2) or 'B'
    
    # 计算并返回字节数
    return int(number * units[unit])


@dataclass
class ProxyRule:
    """
    代理规则配置类
    
    定义单个代理规则的所有配置项。代理规则决定了如何将
    客户端请求路由到目标服务器。
    
    属性:
        path_prefix: 路径前缀，用于匹配客户端请求路径
                    例如："/api" 将匹配所有以 /api 开头的请求
        target_url: 目标服务器的基础 URL
                   例如："http://backend.example.com"
        strip_prefix: 是否从请求路径中移除前缀
                     True: /api/users -> /users
                     False: /api/users -> /api/users
        timeout: 请求超时时间（秒）
        max_redirects: 最大重定向跟踪次数
        retry_times: 失败重试次数
        enable_streaming: 是否启用流式传输
    
    示例配置:
        path_prefix: "/video"
        target_url: "http://video.example.com"
        strip_prefix: true
        timeout: 60
        max_redirects: 5
        retry_times: 3
        enable_streaming: true
    
    路由匹配规则:
        - 使用前缀匹配，不是精确匹配
        - 按配置顺序匹配，第一个匹配的规则生效
        - 如果没有匹配的规则，返回 404 错误
    """
    path_prefix: str           # 路径前缀，用于匹配请求
    target_url: str            # 目标服务器 URL
    strip_prefix: bool = False  # 是否移除路径前缀
    timeout: int = 30          # 请求超时时间（秒）
    max_redirects: int = 10    # 最大重定向次数
    retry_times: int = 3       # 重试次数
    enable_streaming: bool = True  # 是否启用流式传输


@dataclass
class ServerConfig:
    """
    服务器配置类
    
    定义代理服务器的网络配置和性能参数。
    
    属性:
        host: 监听地址
              "0.0.0.0" 表示监听所有网卡
              "127.0.0.1" 表示只监听本地回环
        port: 监听端口号
        workers: 工作进程数（当前未使用，预留给多进程模式）
        keepalive_timeout: HTTP Keep-Alive 连接超时时间（秒）
        max_connections: 最大并发连接数
        max_connections_per_host: 每个目标主机的最大连接数
    
    性能调优建议:
        - max_connections: 根据服务器内存和预期负载设置
        - max_connections_per_host: 避免对单个服务器造成过大压力
        - keepalive_timeout: 根据客户端行为调整，过长会占用资源
    """
    host: str = "0.0.0.0"              # 监听地址
    port: int = 8080                   # 监听端口
    workers: int = 1                   # 工作进程数
    keepalive_timeout: int = 75        # Keep-Alive 超时（秒）
    max_connections: int = 1000        # 最大连接数
    max_connections_per_host: int = 100  # 每主机最大连接数


@dataclass
class SSLConfig:
    """
    SSL/TLS 配置类
    
    定义 HTTPS 服务器的 SSL 证书配置。
    
    属性:
        enabled: 是否启用 SSL/TLS
        cert_file: SSL 证书文件路径
        key_file: SSL 私钥文件路径
    
    证书要求:
        - 证书文件格式：PEM
        - 私钥文件格式：PEM
        - 证书和私钥需要匹配
    
    示例配置:
        enabled: true
        cert_file: "/etc/ssl/certs/server.crt"
        key_file: "/etc/ssl/private/server.key"
    
    注意:
        - 启用 SSL 后，服务器将只接受 HTTPS 连接
        - 证书文件需要有正确的权限设置
    """
    enabled: bool = False              # 是否启用 SSL
    cert_file: Optional[str] = None    # 证书文件路径
    key_file: Optional[str] = None     # 私钥文件路径


@dataclass
class LoggingConfig:
    """
    日志配置类
    
    定义日志系统的输出格式和存储方式。
    
    属性:
        level: 日志级别
               DEBUG: 详细调试信息
               INFO: 一般信息
               WARNING: 警告信息
               ERROR: 错误信息
               CRITICAL: 严重错误
        format: 日志格式字符串
                %(asctime)s: 时间戳
                %(name)s: 日志记录器名称
                %(levelname)s: 日志级别
                %(message)s: 日志消息
        file_path: 日志文件路径，None 表示只输出到控制台
        max_size: 单个日志文件最大大小（字节）
        backup_count: 保留的日志文件数量
    
    日志轮转:
        当日志文件达到 max_size 时，会自动轮转：
        - 当前文件重命名为 .1
        - 创建新的日志文件
        - 旧文件依次重命名（.1 -> .2 -> ...）
        - 超过 backup_count 的文件会被删除
    """
    level: str = "INFO"                # 日志级别
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # 日志格式
    file_path: Optional[str] = None    # 日志文件路径
    max_size: int = 10 * 1024 * 1024   # 单文件最大 10MB
    backup_count: int = 5              # 保留 5 个备份


@dataclass
class StreamingConfig:
    """
    流式传输配置类
    
    定义流式传输（大文件和流媒体）的相关参数。
    
    属性:
        enabled: 是否启用流式传输模式
        chunk_size: 数据块大小（字节）
                   较大的值减少系统调用次数，但增加内存使用
                   较小的值减少延迟，但增加 CPU 开销
        large_file_threshold: 大文件阈值（字节）
                             超过此大小的文件自动使用流式传输
        stream_timeout: 流式传输总超时时间（秒）
                       对于大文件或慢速网络，可能需要较长时间
        read_timeout: 读取超时时间（秒）
                     从上游服务器读取数据的超时
        write_timeout: 写入超时时间（秒）
                     向客户端写入数据的超时
        buffer_size: 缓冲区大小（字节）
                    用于平滑网络波动
        enable_range_support: 是否支持 Range 请求（断点续传）
        max_request_body_size: 请求体最大大小（字节）
                              限制上传文件大小，None 表示无限制
    
    性能调优建议:
        - chunk_size: 64KB 是一个平衡的选择
        - large_file_threshold: 根据内存大小设置
        - stream_timeout: 根据最大文件大小和网络速度估算
    """
    enabled: bool = True               # 是否启用流式传输
    chunk_size: int = 64 * 1024        # 数据块大小 64KB
    large_file_threshold: int = 10 * 1024 * 1024  # 大文件阈值 10MB
    stream_timeout: int = 3600         # 流超时 1小时
    read_timeout: int = 300            # 读取超时 5分钟
    write_timeout: int = 300           # 写入超时 5分钟
    buffer_size: int = 64 * 1024       # 缓冲区大小 64KB
    enable_range_support: bool = True  # 支持 Range 请求
    max_request_body_size: int = 0     # 请求体最大大小，0 表示无限制


@dataclass
class CacheConfig:
    """
    缓存配置类
    
    定义流式缓存系统的相关参数。
    
    属性:
        enabled: 是否启用缓存
        cache_dir: 缓存文件存储目录
        max_size: 缓存最大总大小（字节）
                 超过此大小会触发 LRU 淘汰
        max_entries: 最大缓存条目数
                    限制缓存的文件数量
        default_ttl: 默认缓存有效期（秒）
                    过期的缓存会被自动清理
        chunk_size: 缓存读写块大小（字节）
        max_entry_size: 单个缓存文件允许的最大大小（字节）
        max_request_cache_size: 单次请求允许写入缓存的最大大小（字节）
        enable_preload: 是否启用预加载功能
        preload_concurrency: 预加载并发数
                            同时预加载的 URL 数量
        enable_validation: 是否启用缓存验证
                          定期验证缓存的有效性
        validation_interval: 验证间隔时间（秒）
    
    缓存策略:
        - LRU（最近最少使用）淘汰算法
        - 支持手动预加载热门内容
        - 支持 TTL 自动过期
        - 支持缓存验证（检查上游更新）
    
    存储结构:
        cache_dir/
        ├── metadata.json     # 缓存元数据
        ├── abc123.cache      # 缓存数据文件
        └── def456.cache      # 缓存数据文件
    """
    enabled: bool = False              # 是否启用缓存
    cache_dir: str = './cache'         # 缓存目录
    max_size: int = 10 * 1024 * 1024 * 1024  # 最大 10GB
    max_entries: int = 1000            # 最大条目数
    default_ttl: int = 86400           # 默认 TTL 24小时
    chunk_size: int = 64 * 1024        # 缓存块大小 64KB
    max_entry_size: int = 0            # 单个缓存文件大小上限，0 表示不限制
    max_request_cache_size: int = 0    # 单次请求可缓存大小上限，0 表示不限制
    enable_preload: bool = True        # 启用预加载
    preload_concurrency: int = 3       # 预加载并发数
    enable_validation: bool = True     # 启用缓存验证
    validation_interval: int = 3600    # 验证间隔 1小时


@dataclass
class Config:
    """
    主配置类
    
    整合所有配置模块，提供统一的配置访问接口。
    支持从 YAML 文件加载和保存配置。
    
    属性:
        server: 服务器配置
        ssl: SSL 配置
        logging: 日志配置
        streaming: 流式传输配置
        cache: 缓存配置
        proxy_rules: 代理规则列表
        default_timeout: 默认请求超时时间
        max_redirects: 默认最大重定向次数
        follow_redirects: 是否自动跟踪重定向
        trust_forward_headers: 是否信任并转发 X-Forwarded-* 头部
        hop_by_hop_headers: 逐跳头部列表（不应被转发的头部）
    
    使用示例:
        # 从文件加载配置
        config = Config.from_yaml('config.yaml')
        
        # 访问配置项
        print(config.server.port)
        print(config.cache.max_size)
        
        # 修改并保存配置
        config.server.port = 9090
        config.save_yaml('config.yaml')
    
    配置加载顺序:
        1. 检查指定的配置文件路径
        2. 检查默认路径：config.yaml, config.yml, etc/config.yaml
        3. 如果都没有，使用默认配置
    """
    server: ServerConfig = field(default_factory=ServerConfig)
    ssl: SSLConfig = field(default_factory=SSLConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    proxy_rules: List[ProxyRule] = field(default_factory=list)
    
    # 全局默认值
    default_timeout: int = 30          # 默认超时 30秒
    max_redirects: int = 10            # 默认最大重定向 10次
    follow_redirects: bool = True      # 自动跟踪重定向
    trust_forward_headers: bool = True  # 信任转发头部
    
    # 逐跳头部列表
    # 这些头部只对单次连接有效，不应该被代理转发
    hop_by_hop_headers: List[str] = field(default_factory=lambda: [
        'Connection',           # 连接控制
        'Keep-Alive',           # 保持连接
        'Proxy-Authenticate',   # 代理认证
        'Proxy-Authorization',  # 代理授权
        'TE',                   # 传输编码
        'Trailers',             # 尾部头部
        'Transfer-Encoding',    # 传输编码
        'Upgrade',              # 协议升级
    ])
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'Config':
        """
        从 YAML 文件加载配置
        
        解析 YAML 配置文件并创建 Config 对象。
        支持所有配置项的完整加载。
        
        参数:
            yaml_path: YAML 配置文件的路径
        
        返回:
            Config 对象
        
        异常:
            FileNotFoundError: 配置文件不存在
            yaml.YAMLError: YAML 格式错误
        
        配置文件示例:
            server:
              host: "0.0.0.0"
              port: 8080
            streaming:
              enabled: true
              chunk_size: "64KB"
            cache:
              enabled: true
              max_size: "5GB"
            proxy_rules:
              - path_prefix: "/api"
                target_url: "http://backend.example.com"
        """
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        return cls._parse_config(data)
    
    @classmethod
    def _parse_config(cls, data: dict) -> 'Config':
        """
        解析配置字典
        
        将 YAML 解析后的字典转换为 Config 对象。
        处理所有配置项的类型转换和默认值填充。
        
        参数:
            data: YAML 解析后的字典
        
        返回:
            Config 对象
        
        实现说明:
            - 使用 get() 方法提供默认值
            - 使用 parse_size() 解析大小值
            - 嵌套配置使用对应的数据类
        """
        config = cls()
        
        # 解析服务器配置
        if 'server' in data:
            server_data = data['server']
            config.server = ServerConfig(
                host=server_data.get('host', config.server.host),
                port=server_data.get('port', config.server.port),
                workers=server_data.get('workers', config.server.workers),
                keepalive_timeout=server_data.get('keepalive_timeout', config.server.keepalive_timeout),
                max_connections=server_data.get('max_connections', config.server.max_connections),
                max_connections_per_host=server_data.get('max_connections_per_host', config.server.max_connections_per_host)
            )
        
        # 解析 SSL 配置
        if 'ssl' in data:
            ssl_data = data['ssl']
            config.ssl = SSLConfig(
                enabled=ssl_data.get('enabled', config.ssl.enabled),
                cert_file=ssl_data.get('cert_file', config.ssl.cert_file),
                key_file=ssl_data.get('key_file', config.ssl.key_file)
            )
        
        # 解析日志配置
        if 'logging' in data:
            log_data = data['logging']
            config.logging = LoggingConfig(
                level=log_data.get('level', config.logging.level),
                format=log_data.get('format', config.logging.format),
                file_path=log_data.get('file_path', config.logging.file_path),
                max_size=parse_size(log_data.get('max_size', config.logging.max_size)),
                backup_count=log_data.get('backup_count', config.logging.backup_count)
            )
        
        # 解析流式传输配置
        if 'streaming' in data:
            stream_data = data['streaming']
            config.streaming = StreamingConfig(
                enabled=stream_data.get('enabled', config.streaming.enabled),
                chunk_size=parse_size(stream_data.get('chunk_size', config.streaming.chunk_size)),
                large_file_threshold=parse_size(stream_data.get('large_file_threshold', config.streaming.large_file_threshold)),
                stream_timeout=stream_data.get('stream_timeout', config.streaming.stream_timeout),
                read_timeout=stream_data.get('read_timeout', config.streaming.read_timeout),
                write_timeout=stream_data.get('write_timeout', config.streaming.write_timeout),
                buffer_size=parse_size(stream_data.get('buffer_size', config.streaming.buffer_size)),
                enable_range_support=stream_data.get('enable_range_support', config.streaming.enable_range_support),
                max_request_body_size=parse_size(stream_data.get('max_request_body_size', 0))
            )
        
        # 解析缓存配置
        if 'cache' in data:
            cache_data = data['cache']
            config.cache = CacheConfig(
                enabled=cache_data.get('enabled', config.cache.enabled),
                cache_dir=cache_data.get('cache_dir', config.cache.cache_dir),
                max_size=parse_size(cache_data.get('max_size', config.cache.max_size)),
                max_entries=cache_data.get('max_entries', config.cache.max_entries),
                default_ttl=cache_data.get('default_ttl', config.cache.default_ttl),
                chunk_size=parse_size(cache_data.get('chunk_size', config.cache.chunk_size)),
                max_entry_size=parse_size(cache_data.get('max_entry_size', config.cache.max_entry_size)),
                max_request_cache_size=parse_size(cache_data.get('max_request_cache_size', config.cache.max_request_cache_size)),
                enable_preload=cache_data.get('enable_preload', config.cache.enable_preload),
                preload_concurrency=cache_data.get('preload_concurrency', config.cache.preload_concurrency),
                enable_validation=cache_data.get('enable_validation', config.cache.enable_validation),
                validation_interval=cache_data.get('validation_interval', config.cache.validation_interval)
            )
        
        # 解析代理规则列表
        if 'proxy_rules' in data:
            for rule_data in data['proxy_rules']:
                rule = ProxyRule(
                    path_prefix=rule_data.get('path_prefix', ''),
                    target_url=rule_data.get('target_url', ''),
                    strip_prefix=rule_data.get('strip_prefix', False),
                    timeout=rule_data.get('timeout', 30),
                    max_redirects=rule_data.get('max_redirects', 10),
                    retry_times=rule_data.get('retry_times', 3),
                    enable_streaming=rule_data.get('enable_streaming', True)
                )
                config.proxy_rules.append(rule)
        
        # 解析全局默认值
        config.default_timeout = data.get('default_timeout', config.default_timeout)
        config.max_redirects = data.get('max_redirects', config.max_redirects)
        config.follow_redirects = data.get('follow_redirects', config.follow_redirects)
        config.trust_forward_headers = data.get('trust_forward_headers', config.trust_forward_headers)
        
        return config
    
    def save_yaml(self, yaml_path: str):
        """
        保存配置到 YAML 文件
        
        将当前配置对象序列化为 YAML 格式并保存到文件。
        可用于生成配置模板或保存运行时修改。
        
        参数:
            yaml_path: 目标 YAML 文件路径
        
        输出格式:
            - 使用块样式（非流式）
            - 支持中文（allow_unicode）
            - 保持可读性
        
        示例:
            config = Config()
            config.server.port = 9090
            config.save_yaml('my_config.yaml')
        """
        # 构建配置字典
        data = {
            'server': {
                'host': self.server.host,
                'port': self.server.port,
                'workers': self.server.workers,
                'keepalive_timeout': self.server.keepalive_timeout,
                'max_connections': self.server.max_connections,
                'max_connections_per_host': self.server.max_connections_per_host
            },
            'ssl': {
                'enabled': self.ssl.enabled,
                'cert_file': self.ssl.cert_file,
                'key_file': self.ssl.key_file
            },
            'logging': {
                'level': self.logging.level,
                'format': self.logging.format,
                'file_path': self.logging.file_path,
                'max_size': self.logging.max_size,
                'backup_count': self.logging.backup_count
            },
            'streaming': {
                'enabled': self.streaming.enabled,
                'chunk_size': self.streaming.chunk_size,
                'large_file_threshold': self.streaming.large_file_threshold,
                'stream_timeout': self.streaming.stream_timeout,
                'read_timeout': self.streaming.read_timeout,
                'write_timeout': self.streaming.write_timeout,
                'buffer_size': self.streaming.buffer_size,
                'enable_range_support': self.streaming.enable_range_support,
                'max_request_body_size': self.streaming.max_request_body_size
            },
            'cache': {
                'enabled': self.cache.enabled,
                'cache_dir': self.cache.cache_dir,
                'max_size': self.cache.max_size,
                'max_entries': self.cache.max_entries,
                'default_ttl': self.cache.default_ttl,
                'chunk_size': self.cache.chunk_size,
                'max_entry_size': self.cache.max_entry_size,
                'max_request_cache_size': self.cache.max_request_cache_size,
                'enable_preload': self.cache.enable_preload,
                'preload_concurrency': self.cache.preload_concurrency,
                'enable_validation': self.cache.enable_validation,
                'validation_interval': self.cache.validation_interval
            },
            'proxy_rules': [
                {
                    'path_prefix': rule.path_prefix,
                    'target_url': rule.target_url,
                    'strip_prefix': rule.strip_prefix,
                    'timeout': rule.timeout,
                    'max_redirects': rule.max_redirects,
                    'retry_times': rule.retry_times,
                    'enable_streaming': rule.enable_streaming
                }
                for rule in self.proxy_rules
            ],
            'default_timeout': self.default_timeout,
            'max_redirects': self.max_redirects,
            'follow_redirects': self.follow_redirects,
            'trust_forward_headers': self.trust_forward_headers
        }
        
        # 写入 YAML 文件
        with open(yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)


def setup_logging(config: LoggingConfig) -> logging.Logger:
    """
    设置日志系统
    
    根据配置初始化日志系统，包括：
    - 设置日志级别
    - 配置日志格式
    - 添加控制台处理器
    - 添加文件处理器（如果配置了文件路径）
    
    参数:
        config: 日志配置对象
    
    返回:
        配置好的日志记录器
    
    日志处理器:
        1. 控制台处理器：输出到标准错误流
        2. 文件处理器：输出到指定文件（可选）
           - 支持日志轮转
           - 支持文件大小限制
           - 支持备份数量限制
    
    示例:
        log_config = LoggingConfig(
            level="DEBUG",
            file_path="/var/log/proxy.log"
        )
        logger = setup_logging(log_config)
        logger.info("服务器启动")
    """
    # 获取代理服务器的日志记录器
    logger = logging.getLogger('proxy')
    
    # 设置日志级别
    logger.setLevel(getattr(logging, config.level.upper()))
    
    # 创建日志格式器
    formatter = logging.Formatter(config.format)
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 如果配置了日志文件，添加文件处理器
    if config.file_path:
        from logging.handlers import RotatingFileHandler
        from pathlib import Path
        
        # 确保日志目录存在
        log_path = Path(config.file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 创建轮转文件处理器
        file_handler = RotatingFileHandler(
            str(log_path),
            maxBytes=config.max_size,      # 单文件最大大小
            backupCount=config.backup_count,  # 备份文件数量
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def load_config(config_path: Optional[str] = None) -> Config:
    """
    加载配置文件
    
    从指定路径或默认路径加载配置文件。
    如果找不到配置文件，返回默认配置。
    
    参数:
        config_path: 配置文件路径，可选
                    如果为 None，将搜索默认路径
    
    返回:
        Config 对象
    
    配置文件搜索顺序:
        1. 指定的配置文件路径（如果提供）
        2. config.yaml
        3. config.yml
        4. etc/config.yaml
        5. 如果都没有，使用默认配置
    
    示例:
        # 从指定路径加载
        config = load_config('/etc/proxy/config.yaml')
        
        # 从默认路径加载
        config = load_config()
        
        # 使用默认配置
        config = load_config('nonexistent.yaml')
    """
    # 如果指定了配置文件路径且文件存在
    if config_path and os.path.exists(config_path):
        return Config.from_yaml(config_path)
    
    # 搜索默认配置文件路径
    default_paths = ['config.yaml', 'config.yml', 'etc/config.yaml']
    for path in default_paths:
        if os.path.exists(path):
            return Config.from_yaml(path)
    
    # 没有找到配置文件，返回默认配置
    return Config()
