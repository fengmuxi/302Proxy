"""Prometheus 指标导出（P2-3.4）。

 prometheus_client 为可选依赖：未安装时本模块退化为 no-op（record_* 全部
跳过，is_available()=False，/metrics 返回 503 提示），保证核心代理功能
不因监控依赖缺失而受影响。

指标清单：
  - proxy_requests_total{method, result_status, cache_status}  请求计数
  - proxy_request_duration_seconds                              请求耗时直方图
  - proxy_bytes_transferred_total                               转发字节数累计
  - proxy_streaming_concurrent                                  流式并发瞬时值
"""

import logging
from typing import Optional

logger = logging.getLogger("proxy")

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
    _AVAILABLE = True
except ImportError:  # pragma: no cover - 依赖缺失时降级
    _AVAILABLE = False

# 与 router_logs 的 cache_status 对齐的分桶标签值
_CACHE_STATUSES = ("HIT", "MISS", "BYPASS", "RATE_LIMITED", "BLOCKED")

if _AVAILABLE:
    REQUESTS_TOTAL = Counter(
        "proxy_requests_total",
        "代理请求总数",
        labelnames=("method", "result_status", "cache_status"),
    )
    REQUEST_DURATION = Histogram(
        "proxy_request_duration_seconds",
        "代理请求耗时（秒）",
        labelnames=("method",),
        buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    )
    BYTES_TRANSFERRED = Counter(
        "proxy_bytes_transferred_total",
        "转发到客户端的字节总数",
        labelnames=("direction",),
    )
    STREAMING_CONCURRENT = Gauge(
        "proxy_streaming_concurrent",
        "当前流式转发并发数",
    )
else:
    REQUESTS_TOTAL = None
    REQUEST_DURATION = None
    BYTES_TRANSFERRED = None
    STREAMING_CONCURRENT = None


def is_available() -> bool:
    """prometheus_client 是否可用。"""
    return _AVAILABLE


def exposition() -> tuple[Optional[bytes], str]:
    """生成 text exposition 格式的指标载荷。

    Returns:
        (body_bytes, content_type)；依赖缺失时返回 (None, "")。
    """
    if not _AVAILABLE:
        return None, ""
    return generate_latest(), CONTENT_TYPE_LATEST


def record_request(method: str, status: int, cache_status: str, duration_seconds: float) -> None:
    """在日志中间件里累加请求计数与耗时。

    result_status 按状态码大类聚合（2xx/3xx/4xx/429/5xx），避免基数爆炸；
    cache_status 限定在枚举内，未知值归入 BYPASS。
    """
    if not _AVAILABLE:
        return
    try:
        if status == 429:
            result = "429"
        elif status < 300:
            result = "2xx"
        elif status < 400:
            result = "3xx"
        elif status < 500:
            result = "4xx"
        else:
            result = "5xx"
        cs = cache_status if cache_status in _CACHE_STATUSES else "BYPASS"
        REQUESTS_TOTAL.labels(method=method, result_status=result, cache_status=cs).inc()
        REQUEST_DURATION.labels(method=method).observe(duration_seconds)
    except Exception as exc:  # pragma: no cover - 监控失败不影响转发
        logger.debug("指标记录失败: %s", exc)


def record_bytes(n: int) -> None:
    """累计转发到客户端的字节数（流式写出循环调用）。"""
    if not _AVAILABLE or n <= 0:
        return
    try:
        BYTES_TRANSFERRED.labels(direction="response").inc(n)
    except Exception as exc:  # pragma: no cover
        logger.debug("指标记录失败: %s", exc)


def streaming_enter() -> None:
    """流式转发开始：并发 +1。"""
    if not _AVAILABLE:
        return
    try:
        STREAMING_CONCURRENT.inc()
    except Exception:  # pragma: no cover
        pass


def streaming_exit() -> None:
    """流式转发结束：并发 -1。"""
    if not _AVAILABLE:
        return
    try:
        STREAMING_CONCURRENT.dec()
    except Exception:  # pragma: no cover
        pass
