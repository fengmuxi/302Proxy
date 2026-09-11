"""主动速率限制（阶段二 P1-2.1）：进程内令牌桶。

设计要点：
- 每个 key（客户端 IP 或全局）独立一个桶，桶容量 = burst，填充速率 = rps。
- 请求到达先按「距上次补充的时长 × rps」补令牌（封顶 burst），够 1 个则放行并扣 1，
  否则返回需等待的秒数（用作 429 的 Retry-After）。
- 单 worker 进程内足够（本项目单进程多协程模型）；如需多 worker 共享需换 Redis 等外部存储。
- 桶数量封顶 max_keys，超出近似 LRU 淘汰最旧桶，避免恶意构造海量 key 撑爆内存。
"""

import threading
import time
from typing import List, Tuple


class RateLimiter:
    def __init__(self, max_keys: int = 20000):
        self._lock = threading.Lock()
        # key -> [tokens: float, last: float(monotonic 秒)]
        self._buckets: dict = {}
        self._max_keys = max_keys

    def allow(self, key: str, rps: float, burst: int) -> Tuple[bool, float]:
        """判断 key 是否放行。

        Args:
            key: 限流键（如客户端 IP；per_ip=False 时用固定全局键）。
            rps: 令牌填充速率（请求/秒），调用方须 > 0。
            burst: 桶容量（允许瞬时突发的最大令牌数），调用方须 >= 1。

        Returns:
            (ok, retry_after)：ok 为 True 表示放行；否则 retry_after 为建议重试等待秒数。
        """
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                if len(self._buckets) >= self._max_keys:
                    try:
                        oldest = next(iter(self._buckets))
                        self._buckets.pop(oldest, None)
                    except StopIteration:
                        pass
                bucket = [float(burst), now]
                self._buckets[key] = bucket
            tokens, last = bucket
            tokens = min(float(burst), tokens + (now - last) * rps)
            if tokens >= 1.0:
                self._buckets[key] = [tokens - 1.0, now]
                return True, 0.0
            retry = (1.0 - tokens) / rps if rps > 0 else 1.0
            # 拒绝时仅刷新 last，不放行也不补充（保持当前剩余令牌）
            self._buckets[key] = [tokens, now]
            return False, max(0.0, retry)

    def clear(self) -> None:
        with self._lock:
            self._buckets.clear()
