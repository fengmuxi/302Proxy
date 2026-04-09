"""
缓存日志追踪模块

本模块提供了用于追踪和记录 HTTP Range 请求处理和缓存操作的详细日志功能。
主要功能包括：
1. 请求生命周期追踪 - 从请求开始到结束的完整记录
2. 步骤级日志记录 - 记录每个处理步骤的耗时和详情
3. 缓存操作日志 - 记录缓存查找、存储、读取、淘汰等操作
4. Range 请求日志 - 记录 HTTP Range 请求的详细信息
5. 错误追踪 - 记录处理过程中发生的错误

设计思路：
- 使用 RequestTrace 数据类存储单个请求的完整追踪信息
- 使用 CacheLogger 类管理所有请求的追踪记录
- 提供上下文管理器简化步骤日志记录
- 提供装饰器简化函数级日志记录
"""

import time
import uuid
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from contextlib import contextmanager
from functools import wraps
import asyncio

# 获取代理服务器的日志记录器
logger = logging.getLogger('proxy')


@dataclass
class RequestTrace:
    """
    请求追踪数据类
    
    用于存储单个 HTTP 请求从开始到结束的完整追踪信息，
    包括缓存操作、处理步骤、错误信息等。
    
    属性说明：
        request_id: 请求唯一标识符（8位UUID），用于关联所有相关日志
        url: 请求的完整 URL 地址
        start_time: 请求开始时间戳（秒）
        range_header: HTTP Range 请求头，如 "bytes=0-1023"，None 表示完整请求
        cache_key: 缓存键（URL 的 SHA256 哈希值），用于缓存查找和存储
        cache_hit: 是否命中缓存，True 表示数据来自缓存
        cache_status: 缓存状态，可选值：UNKNOWN/HIT/MISS/STORED/EXPIRED
        bytes_served: 已传输给客户端的字节数
        bytes_from_cache: 从缓存读取的字节数
        errors: 错误列表，每个元素包含错误类型、消息和时间
        steps: 处理步骤列表，每个元素包含步骤名、耗时和详情
    """
    request_id: str
    url: str
    start_time: float
    range_header: Optional[str] = None
    cache_key: Optional[str] = None
    cache_hit: bool = False
    cache_status: str = 'UNKNOWN'
    bytes_served: int = 0
    bytes_from_cache: int = 0
    errors: list = field(default_factory=list)
    steps: list = field(default_factory=list)
    stage_starts: Dict[str, float] = field(default_factory=dict)
    progress_markers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def add_step(self, step_name: str, duration_ms: float = 0, details: Dict[str, Any] = None):
        """
        添加处理步骤记录
        
        记录请求处理过程中的一个步骤，包括步骤名称、耗时和详细信息。
        
        参数：
            step_name: 步骤名称，如 "缓存查找"、"文件读取" 等
            duration_ms: 步骤执行耗时（毫秒）
            details: 步骤详细信息字典，如 {"found": True, "size": 1024}
        """
        self.steps.append({
            'step': step_name,
            'time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_ms': round(duration_ms, 2),
            'details': details or {}
        })
    
    def add_error(self, error_type: str, error_message: str):
        """
        添加错误记录
        
        记录处理过程中发生的错误，包括错误类型和详细消息。
        
        参数：
            error_type: 错误类型，如 "ReadError"、"RangeError" 等
            error_message: 错误详细消息
        """
        self.errors.append({
            'type': error_type,
            'message': error_message,
            'time': time.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    def get_duration_ms(self) -> float:
        """
        获取请求总耗时
        
        计算从请求开始到当前时间的总耗时。
        
        返回：
            请求总耗时（毫秒）
        """
        return (time.time() - self.start_time) * 1000
    
    def to_summary(self) -> Dict[str, Any]:
        """
        生成请求摘要
        
        将请求追踪信息转换为可读的摘要字典，用于日志输出。
        
        返回：
            包含请求摘要信息的字典，包括：
            - request_id: 请求ID
            - url: URL（截断到100字符）
            - range: Range请求头
            - cache_key: 缓存键（截断到16字符）
            - cache_status: 缓存状态
            - cache_hit: 是否命中缓存
            - bytes_served: 传输字节数
            - bytes_from_cache: 缓存提供字节数
            - duration_ms: 总耗时
            - steps_count: 步骤数
            - errors_count: 错误数
        """
        return {
            'request_id': self.request_id,
            'url': self.url[:100],
            'range': self.range_header,
            'cache_key': self.cache_key[:16] + '...' if self.cache_key else None,
            'cache_status': self.cache_status,
            'cache_hit': self.cache_hit,
            'bytes_served': self.bytes_served,
            'bytes_from_cache': self.bytes_from_cache,
            'duration_ms': round(self.get_duration_ms(), 2),
            'steps_count': len(self.steps),
            'errors_count': len(self.errors)
        }


class CacheLogger:
    """
    缓存日志管理器
    
    管理所有请求的追踪记录，提供日志记录和查询功能。
    
    核心功能：
    1. 请求追踪生命周期管理（开始、获取、结束）
    2. 步骤级日志记录（使用上下文管理器）
    3. 缓存操作日志（查找、存储、读取、淘汰）
    4. Range 请求日志
    5. 流式传输进度日志
    6. 错误日志
    
    使用示例：
        logger = CacheLogger()
        request_id = logger.start_trace(url, range_header)
        # ... 处理请求 ...
        logger.end_trace(request_id)
    
    设计思路：
    - 使用字典存储活跃请求的追踪记录，以 request_id 为键
    - 请求结束时自动从字典中移除并输出摘要日志
    - 所有日志方法都接受 request_id 参数以关联请求
    """
    
    def __init__(self):
        """
        初始化缓存日志管理器
        
        创建空的追踪记录字典，用于存储所有活跃请求的追踪信息。
        """
        self._traces: Dict[str, RequestTrace] = {}
    
    def start_trace(self, url: str, range_header: Optional[str] = None) -> str:
        """
        开始请求追踪
        
        为新的 HTTP 请求创建追踪记录，生成唯一请求ID。
        
        参数：
            url: 请求的完整 URL
            range_header: HTTP Range 请求头（可选）
        
        返回：
            请求唯一标识符（8位UUID），用于后续所有日志记录
        
        实现思路：
        1. 生成8位UUID作为请求ID（足够短且唯一）
        2. 创建 RequestTrace 对象记录请求信息
        3. 将追踪记录存入字典以便后续查询
        """
        request_id = str(uuid.uuid4())[:8]
        trace = RequestTrace(
            request_id=request_id,
            url=url,
            start_time=time.time(),
            range_header=range_header
        )
        self._traces[request_id] = trace
        return request_id
    
    def get_trace(self, request_id: str) -> Optional[RequestTrace]:
        """
        获取请求追踪记录
        
        根据请求ID获取对应的追踪记录对象。
        
        参数：
            request_id: 请求唯一标识符
        
        返回：
            RequestTrace 对象，如果不存在则返回 None
        """
        return self._traces.get(request_id)
    
    def end_trace(self, request_id: str) -> Optional[RequestTrace]:
        """
        结束请求追踪
        
        从追踪字典中移除请求记录，并输出请求摘要日志。
        
        参数：
            request_id: 请求唯一标识符
        
        返回：
            被移除的 RequestTrace 对象，如果不存在则返回 None
        
        实现思路：
        1. 从字典中弹出追踪记录（原子操作）
        2. 如果存在，输出请求摘要日志
        3. 返回追踪记录供调用者进一步处理
        """
        trace = self._traces.pop(request_id, None)
        if trace:
            self._log_trace_summary(trace)
        return trace
    
    def _log_trace_summary(self, trace: RequestTrace):
        """
        输出请求摘要日志
        
        将请求追踪信息格式化输出到日志。
        
        参数：
            trace: 请求追踪对象
        
        日志格式：
        [请求完成] ID: xxx | URL: xxx | Range: xxx | 缓存状态: xxx | 
        命中: xxx | 传输: xxx KB | 缓存提供: xxx KB | 耗时: xxx ms | 
        步骤数: xxx | 错误数: xxx
        """
        summary = trace.to_summary()
        logger.info(
            f"[请求完成] ID: {summary['request_id']} | "
            f"URL: {summary['url'][:50]}... | "
            f"Range: {summary['range'] or '完整请求'} | "
            f"缓存状态: {summary['cache_status']} | "
            f"命中: {summary['cache_hit']} | "
            f"传输: {summary['bytes_served'] / 1024:.2f} KB | "
            f"缓存提供: {summary['bytes_from_cache'] / 1024:.2f} KB | "
            f"耗时: {summary['duration_ms']:.2f} ms | "
            f"步骤数: {summary['steps_count']} | "
            f"错误数: {summary['errors_count']}"
        )

    @staticmethod
    def _format_bytes(value: Optional[int]) -> str:
        if value is None or value < 0:
            return 'unknown'
        size = float(value)
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        for unit in units:
            if size < 1024 or unit == units[-1]:
                if unit == 'B':
                    return f'{int(size)} {unit}'
                return f'{size:.2f} {unit}'
            size /= 1024
        return f'{value} B'

    @staticmethod
    def _format_details(details: Optional[Dict[str, Any]]) -> str:
        if not details:
            return 'none'
        formatted = []
        for key, value in details.items():
            if isinstance(value, bool):
                formatted.append(f'{key}={value}')
            elif isinstance(value, int) and 'bytes' in key.lower():
                formatted.append(f'{key}={CacheLogger._format_bytes(value)}')
            else:
                formatted.append(f'{key}={value}')
        return ', '.join(formatted)

    def log_cache_stage(
        self,
        request_id: str,
        stage_name: str,
        status: str,
        cache_key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        trace = self.get_trace(request_id)
        duration_ms = None
        if trace:
            if cache_key:
                trace.cache_key = cache_key
            if status == 'START':
                trace.stage_starts[stage_name] = time.time()
            else:
                started_at = trace.stage_starts.pop(stage_name, None)
                if started_at is not None:
                    duration_ms = (time.time() - started_at) * 1000
                if status in ('END', 'DONE', 'SUCCESS', 'SKIP', 'ABORT', 'FAIL'):
                    trace.add_step(stage_name, duration_ms or 0, details)

        key_label = f"{cache_key[:16]}..." if cache_key else 'none'
        duration_label = f" | 耗时: {duration_ms:.2f} ms" if duration_ms is not None else ''
        logger.info(
            f"[缓存阶段] ID: {request_id} | 阶段: {stage_name} | 状态: {status} | "
            f"键: {key_label} | 详情: {self._format_details(details)}{duration_label}"
        )

    @contextmanager
    def log_step(self, request_id: str, step_name: str, **kwargs):
        """
        步骤日志上下文管理器
        
        使用 Python 上下文管理器自动记录步骤的开始、完成和失败。
        自动计算步骤耗时并添加到追踪记录中。
        
        参数：
            request_id: 请求唯一标识符
            step_name: 步骤名称
            **kwargs: 步骤详细信息（会被记录到日志中）
        
        使用示例：
            with cache_logger.log_step(request_id, "文件读取", file="test.mp4"):
                # 执行文件读取操作
                data = read_file()
        
        实现思路：
        1. 进入上下文时记录步骤开始日志
        2. 执行 with 块中的代码
        3. 正常退出时记录步骤完成日志
        4. 异常退出时记录步骤失败日志并重新抛出异常
        """
        trace = self.get_trace(request_id)
        step_start = time.time()
        
        # 记录步骤开始日志
        if trace:
            logger.info(
                f"[步骤开始] ID: {request_id} | 步骤: {step_name} | "
                f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"详情: {kwargs if kwargs else '无'}"
            )
        
        try:
            # 执行 with 块中的代码
            yield
            # 计算步骤耗时
            duration_ms = (time.time() - step_start) * 1000
            
            # 记录步骤完成日志
            if trace:
                trace.add_step(step_name, duration_ms, kwargs)
                logger.info(
                    f"[步骤完成] ID: {request_id} | 步骤: {step_name} | "
                    f"耗时: {duration_ms:.2f} ms | 结果: 成功"
                )
        
        except Exception as e:
            # 计算步骤耗时
            duration_ms = (time.time() - step_start) * 1000
            
            # 记录步骤失败日志
            if trace:
                trace.add_step(step_name, duration_ms, kwargs)
                trace.add_error(step_name, str(e))
                logger.error(
                    f"[步骤失败] ID: {request_id} | 步骤: {step_name} | "
                    f"耗时: {duration_ms:.2f} ms | 错误: {e}"
                )
            raise
    
    def log_range_request(
        self, 
        request_id: str,
        range_header: str,
        start_byte: int,
        end_byte: int,
        total_size: int,
        status: str = 'PROCESSING'
    ):
        """
        记录 HTTP Range 请求信息
        
        记录 Range 请求的详细信息，包括请求范围、大小、覆盖率等。
        
        参数：
            request_id: 请求唯一标识符
            range_header: Range 请求头字符串
            start_byte: 请求起始字节位置
            end_byte: 请求结束字节位置
            total_size: 文件总大小（字节）
            status: 请求状态，默认 'PROCESSING'
        
        日志格式：
        [Range请求] ID: xxx | 范围: bytes=0-1023 | 请求大小: 1.00 KB | 
        总大小: 10.00 MB | 覆盖率: 0.1% | 状态: PROCESSING
        """
        trace = self.get_trace(request_id)
        if trace:
            trace.range_header = range_header
        
        # 计算请求大小和覆盖率
        range_size = end_byte - start_byte + 1
        coverage = (range_size / total_size * 100) if total_size > 0 else 0
        
        logger.info(
            f"[Range请求] ID: {request_id} | "
            f"范围: bytes={start_byte}-{end_byte} | "
            f"请求大小: {range_size / 1024:.2f} KB | "
            f"总大小: {total_size / 1024 / 1024:.2f} MB | "
            f"覆盖率: {coverage:.1f}% | "
            f"状态: {status}"
        )
    
    def log_cache_lookup(
        self,
        request_id: str,
        cache_key: str,
        found: bool,
        reason: str = None
    ):
        """
        记录缓存查找操作
        
        记录缓存查找的结果，包括是否命中、缓存键和原因。
        
        参数：
            request_id: 请求唯一标识符
            cache_key: 缓存键（URL 的哈希值）
            found: 是否找到缓存
            reason: 查找结果原因（可选），如 "无缓存"、"缓存过期" 等
        
        日志格式：
        [缓存查找] ID: xxx | 键: abc123... | 结果: 命中 | 原因: 存在
        """
        trace = self.get_trace(request_id)
        if trace:
            trace.cache_key = cache_key
            trace.cache_status = 'HIT' if found else 'MISS'
            trace.cache_hit = found
        
        logger.info(
            f"[缓存查找] ID: {request_id} | "
            f"键: {cache_key[:16]}... | "
            f"结果: {'命中' if found else '未命中'} | "
            f"原因: {reason or ('存在' if found else '不存在')}"
        )
    
    def log_cache_validation(
        self,
        request_id: str,
        cache_key: str,
        is_valid: bool,
        method: str = 'ETag',
        details: str = None
    ):
        """
        记录缓存校验操作
        
        记录缓存有效性校验的结果，包括校验方法和详情。
        
        参数：
            request_id: 请求唯一标识符
            cache_key: 缓存键
            is_valid: 缓存是否有效
            method: 校验方法，默认 'ETag'，也可以是 'Last-Modified'
            details: 校验详情（可选）
        
        日志格式：
        [缓存校验] ID: xxx | 键: abc123... | 方法: ETag | 结果: 有效 | 详情: 无
        """
        logger.info(
            f"[缓存校验] ID: {request_id} | "
            f"键: {cache_key[:16]}... | "
            f"方法: {method} | "
            f"结果: {'有效' if is_valid else '过期'} | "
            f"详情: {details or '无'}"
        )
    
    def log_cache_storage(
        self,
        request_id: str,
        cache_key: str,
        size: int,
        evicted: bool = False,
        evicted_size: int = 0
    ):
        """
        记录缓存存储操作
        
        记录数据写入缓存的操作，包括存储大小和是否触发了淘汰。
        
        参数：
            request_id: 请求唯一标识符
            cache_key: 缓存键
            size: 存储的数据大小（字节）
            evicted: 是否触发了缓存淘汰
            evicted_size: 被淘汰的数据大小（字节）
        
        日志格式：
        [缓存存储] ID: xxx | 键: abc123... | 大小: 10.00 MB | 淘汰: 是 | 淘汰大小: 5.00 MB
        """
        trace = self.get_trace(request_id)
        if trace:
            trace.cache_status = 'STORED'
        
        logger.info(
            f"[缓存存储] ID: {request_id} | "
            f"键: {cache_key[:16]}... | "
            f"大小: {size / 1024 / 1024:.2f} MB | "
            f"淘汰: {'是' if evicted else '否'} | "
            f"淘汰大小: {evicted_size / 1024 / 1024:.2f} MB" if evicted else ""
        )
    
    def log_cache_retrieval(
        self,
        request_id: str,
        cache_key: str,
        start_byte: int,
        end_byte: int,
        bytes_read: int,
        duration_ms: float
    ):
        """
        记录缓存读取操作
        
        记录从缓存读取数据的操作，包括读取范围、大小、耗时和吞吐量。
        
        参数：
            request_id: 请求唯一标识符
            cache_key: 缓存键
            start_byte: 读取起始字节位置
            end_byte: 读取结束字节位置
            bytes_read: 实际读取的字节数
            duration_ms: 读取耗时（毫秒）
        
        日志格式：
        [缓存读取] ID: xxx | 键: abc123... | 范围: 0-1023 | 读取: 1.00 KB | 
        耗时: 15.23 ms | 吞吐: 65.43 MB/s
        
        实现思路：
        - 自动计算吞吐量（MB/s）
        - 累加 bytes_from_cache 统计
        """
        trace = self.get_trace(request_id)
        if trace:
            trace.bytes_from_cache += bytes_read
        
        # 计算吞吐量（MB/s）
        throughput = (bytes_read / 1024 / 1024) / (duration_ms / 1000) if duration_ms > 0 else 0
        
        logger.info(
            f"[缓存读取] ID: {request_id} | "
            f"键: {cache_key[:16]}... | "
            f"范围: {start_byte}-{end_byte} | "
            f"读取: {bytes_read / 1024:.2f} KB | "
            f"耗时: {duration_ms:.2f} ms | "
            f"吞吐: {throughput:.2f} MB/s"
        )
    
    def log_cache_eviction(
        self,
        cache_key: str,
        size: int,
        reason: str = 'LRU',
        request_id: str = None
    ):
        """
        记录缓存淘汰操作
        
        记录缓存条目被淘汰的操作，包括淘汰原因。
        
        参数：
            cache_key: 被淘汰的缓存键
            size: 被淘汰的数据大小（字节）
            reason: 淘汰原因，默认 'LRU'（最近最少使用）
            request_id: 关联的请求ID（可选），系统触发时为 None
        
        日志格式：
        [缓存淘汰] ID: xxx | 键: abc123... | 大小: 10.00 MB | 原因: LRU
        """
        logger.info(
            f"[缓存淘汰] ID: {request_id or 'SYSTEM'} | "
            f"键: {cache_key[:16]}... | "
            f"大小: {size / 1024 / 1024:.2f} MB | "
            f"原因: {reason}"
        )
    
    def log_streaming_progress(
        self,
        request_id: str,
        bytes_transferred: int,
        total_bytes: int = 0,
        chunk_number: int = 0,
        stage_name: str = 'streaming',
        cache_key: Optional[str] = None,
        chunk_size: int = 0,
        force: bool = False,
        served_to_client: bool = True,
        bytes_step: int = 1024 * 1024,
        percent_step: float = 5.0
    ):
        """
        记录流式传输进度
        
        记录流式传输的进度信息，包括已传输字节数、总字节数和块数。
        为避免日志过多，只在每10个块或传输完成时记录。
        
        参数：
            request_id: 请求唯一标识符
            bytes_transferred: 已传输的字节数
            total_bytes: 总字节数
            chunk_number: 当前块编号
        
        日志格式：
        [流式传输] ID: xxx | 进度: 50.0% | 已传输: 5.00 MB | 总大小: 10.00 MB | 块数: 80
        
        实现思路：
        - 自动计算进度百分比
        - 更新 bytes_served 统计
        - 使用 DEBUG 级别避免过多日志
        - 每10个块记录一次，减少日志量
        """
        # 计算进度百分比
        progress = (bytes_transferred / total_bytes * 100) if total_bytes > 0 else 0
        trace = self.get_trace(request_id)
        if trace:
            trace.bytes_served = bytes_transferred
        
        # 每10个块或传输完成时记录日志
        if chunk_number % 10 == 0 or bytes_transferred == total_bytes:
            logger.debug(
                f"[流式传输] ID: {request_id} | "
                f"进度: {progress:.1f}% | "
                f"已传输: {bytes_transferred / 1024 / 1024:.2f} MB | "
                f"总大小: {total_bytes / 1024 / 1024:.2f} MB | "
                f"块数: {chunk_number}"
            )
    
    def log_error(
        self,
        request_id: str,
        error_type: str,
        error_message: str,
        recoverable: bool = True
    ):
        """
        记录错误信息
        
        记录处理过程中发生的错误，包括错误类型、消息和是否可恢复。
        
        参数：
            request_id: 请求唯一标识符
            error_type: 错误类型，如 "ReadError"、"NetworkError" 等
            error_message: 错误详细消息
            recoverable: 错误是否可恢复，可恢复错误使用 WARNING 级别，
                        不可恢复错误使用 ERROR 级别
        
        日志格式：
        [错误] ID: xxx | 类型: ReadError | 消息: 文件不存在 | 可恢复: 是
        """
        trace = self.get_trace(request_id)
        if trace:
            trace.add_error(error_type, error_message)
        
        # 根据是否可恢复选择日志级别
        log_level = logger.warning if recoverable else logger.error
        log_level(
            f"[错误] ID: {request_id} | "
            f"类型: {error_type} | "
            f"消息: {error_message} | "
            f"可恢复: {'是' if recoverable else '否'}"
        )


# 创建全局缓存日志管理器实例
# 所有模块都可以使用这个实例记录日志
def _cache_logger_log_streaming_progress(
    self,
    request_id: str,
    bytes_transferred: int,
    total_bytes: int = 0,
    chunk_number: int = 0,
    stage_name: str = 'streaming',
    cache_key: Optional[str] = None,
    chunk_size: int = 0,
    force: bool = False,
    served_to_client: bool = True,
    bytes_step: int = 1024 * 1024,
    percent_step: float = 5.0
):
    """Log progress for cache read/write and client streaming."""
    progress = (bytes_transferred / total_bytes * 100) if total_bytes > 0 else 0
    trace = self.get_trace(request_id)
    if trace:
        if served_to_client:
            trace.bytes_served = bytes_transferred
        if stage_name == 'cache_hit_stream':
            trace.bytes_from_cache = bytes_transferred
        if cache_key:
            trace.cache_key = cache_key
        marker = trace.progress_markers.setdefault(
            stage_name,
            {
                'last_bytes': -1,
                'last_percent': -1.0,
                'last_chunk': 0
            }
        )
    else:
        marker = {
            'last_bytes': -1,
            'last_percent': -1.0,
            'last_chunk': 0
        }

    should_log = force or chunk_number <= 1
    if not should_log and total_bytes > 0 and progress >= 100:
        should_log = True
    if not should_log and bytes_transferred - marker['last_bytes'] >= bytes_step:
        should_log = True
    if not should_log and total_bytes > 0 and progress - marker['last_percent'] >= percent_step:
        should_log = True

    if not should_log:
        return

    if trace:
        marker['last_bytes'] = bytes_transferred
        marker['last_percent'] = progress
        marker['last_chunk'] = chunk_number

    key_label = f"{cache_key[:16]}..." if cache_key else 'none'
    total_label = self._format_bytes(total_bytes) if total_bytes > 0 else 'unknown'
    progress_label = f'{progress:.1f}%' if total_bytes > 0 else 'unknown'
    logger.info(
        f"[缓存进度] ID: {request_id} | 阶段: {stage_name} | 键: {key_label} | "
        f"进度: {progress_label} | 已处理: {self._format_bytes(bytes_transferred)} | "
        f"总量: {total_label} | 块: {chunk_number} | 块大小: {self._format_bytes(chunk_size)}"
    )


CacheLogger.log_streaming_progress = _cache_logger_log_streaming_progress
cache_logger = CacheLogger()


def log_cache_operation(operation_name: str):
    """
    缓存操作日志装饰器
    
    用于装饰函数，自动记录函数的开始、完成和失败日志。
    支持同步函数和异步函数。
    
    参数：
        operation_name: 操作名称，会显示在日志中
    
    使用示例：
        @log_cache_operation("缓存下载")
        async def download_file(url: str, request_id: str = None):
            # 下载文件
            pass
    
    实现思路：
    1. 使用 functools.wraps 保留原函数的元信息
    2. 检测函数是否为异步函数
    3. 自动生成或使用传入的 request_id
    4. 记录函数开始、完成和失败日志
    5. 自动计算函数执行耗时
    
    返回：
        装饰后的函数
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            """
            异步函数包装器
            
            用于包装异步函数，自动记录日志。
            """
            # 获取或生成请求ID
            request_id = kwargs.get('request_id') or str(uuid.uuid4())[:8]
            start_time = time.time()
            
            # 记录操作开始日志
            logger.info(
                f"[{operation_name}] ID: {request_id} | "
                f"开始: {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            try:
                # 执行原函数
                result = await func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                
                # 记录操作完成日志
                logger.info(
                    f"[{operation_name}] ID: {request_id} | "
                    f"完成: 耗时 {duration_ms:.2f} ms | 结果: 成功"
                )
                return result
            
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                
                # 记录操作失败日志
                logger.error(
                    f"[{operation_name}] ID: {request_id} | "
                    f"失败: 耗时 {duration_ms:.2f} ms | 错误: {e}"
                )
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            """
            同步函数包装器
            
            用于包装同步函数，自动记录日志。
            """
            # 获取或生成请求ID
            request_id = kwargs.get('request_id') or str(uuid.uuid4())[:8]
            start_time = time.time()
            
            # 记录操作开始日志
            logger.info(
                f"[{operation_name}] ID: {request_id} | "
                f"开始: {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            try:
                # 执行原函数
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                
                # 记录操作完成日志
                logger.info(
                    f"[{operation_name}] ID: {request_id} | "
                    f"完成: 耗时 {duration_ms:.2f} ms | 结果: 成功"
                )
                return result
            
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                
                # 记录操作失败日志
                logger.error(
                    f"[{operation_name}] ID: {request_id} | "
                    f"失败: 耗时 {duration_ms:.2f} ms | 错误: {e}"
                )
                raise
        
        # 根据函数类型返回对应的包装器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
