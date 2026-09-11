"""上游健康检查与多目标选择（P1-2.2）。

设计要点：
- 仅在「规则配置了多个上游（target_urls）或启用了健康检查」时介入；单上游且未启用健康检查的
  规则表现与旧版完全一致（selection 回落到 rule.target_url）。
- 后台守护线程按各规则 health_check_interval 周期探测：health_check_path 非空 → HTTP GET
  探测（status<500 视为健康），否则对 target 的 host:port 做 TCP 建连探测。
- 连续失败达到阈值（HEALTH_FAIL_THRESHOLD）判定为不健康，从可用列表剔除；恢复后重新纳入。
- select_route 通过 pick_target() 在健康目标间轮询；转发连接失败时由 proxy_core 调 mark_failure
  加速「感知」，从而尽快切流（路由级 failover）。
- 线程安全：所有状态访问走 self._lock。
"""

import asyncio
import logging
import socket
import threading
import time
from typing import Dict, List, Optional

import aiohttp
from urllib.parse import urlparse

logger = logging.getLogger("proxy.upstream_health")

# 连续探测/转发失败达到该次数才判定为不健康，避免瞬时抖动误杀
HEALTH_FAIL_THRESHOLD = 3
# 后台循环基础节拍（秒）：每个目标是否探测由其 interval 决定
PROBE_TICK_SECONDS = 5


class UpstreamHealthMonitor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # rule_id -> {
        #   "targets": [url, ...],
        #   "state": {url: {"healthy": bool, "fails": int}},
        #   "rr": int,                       # 轮询游标
        #   "enabled": bool,                # 是否启用主动探测
        #   "path": str, "interval": int, "timeout": int,
        #   "last_probe": float,            # monotonic，上次探测时刻
        # }
        self._rules: Dict[int, dict] = {}
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ 规则注册
    def update_rules(self, rules) -> None:
        """由 ProxyServer 在加载/重载配置后调用，同步监控目标与探针参数。"""
        new_ids = set()
        with self._lock:
            for r in rules:
                rid = r.rule_id
                if rid is None:
                    continue
                new_ids.add(rid)
                targets = r.all_targets()
                entry = self._rules.get(rid)
                if entry is None:
                    state = {t: {"healthy": True, "fails": 0} for t in targets}
                    self._rules[rid] = {
                        "targets": targets,
                        "state": state,
                        "rr": 0,
                        "enabled": False,
                        "path": "",
                        "interval": 30,
                        "timeout": 5,
                        "last_probe": 0.0,
                    }
                else:
                    old_state = entry["state"]
                    new_state = {t: old_state.get(t, {"healthy": True, "fails": 0}) for t in targets}
                    entry["targets"] = targets
                    entry["state"] = new_state
                entry = self._rules[rid]
                entry["enabled"] = bool(r.health_check_enabled) and len(targets) >= 1
                entry["path"] = r.health_check_path or ""
                entry["interval"] = max(1, int(r.health_check_interval or 30))
                entry["timeout"] = max(1, int(r.health_check_timeout or 5))
            for rid in list(self._rules.keys()):
                if rid not in new_ids:
                    del self._rules[rid]

    # ------------------------------------------------------------------ 查询接口
    def get_health(self, rule_id: int) -> Dict[str, bool]:
        with self._lock:
            entry = self._rules.get(rule_id)
            if not entry:
                return {}
            return {t: entry["state"].get(t, {"healthy": True})["healthy"] for t in entry["targets"]}

    def get_all_health(self) -> Dict[int, Dict[str, bool]]:
        """全量健康快照（{rule_id: {target: healthy}}），供后台 UI 渲染徽标。"""
        with self._lock:
            out: Dict[int, Dict[str, bool]] = {}
            for rid, entry in self._rules.items():
                out[rid] = {t: entry["state"].get(t, {"healthy": True})["healthy"] for t in entry["targets"]}
            return out

    def get_available_targets(self, rule_id: int) -> List[str]:
        """健康目标（原顺序过滤）；若过滤后为空，返回全部（全宕时也不至于彻底无路可走）。"""
        with self._lock:
            entry = self._rules.get(rule_id)
            if not entry:
                return []
            healthy = [t for t in entry["targets"] if entry["state"].get(t, {"healthy": True})["healthy"]]
            return healthy or list(entry["targets"])

    def pick_target(self, rule_id: int) -> Optional[str]:
        """健康目标间轮询；无健康项返回 None（调用方回落 rule.target_url）。"""
        with self._lock:
            entry = self._rules.get(rule_id)
            if not entry:
                return None
            healthy = [t for t in entry["targets"] if entry["state"].get(t, {"healthy": True})["healthy"]]
            if not healthy:
                return None
            idx = entry["rr"] % len(healthy)
            entry["rr"] = (entry["rr"] + 1) % len(healthy)
            return healthy[idx]

    # ------------------------------------------------------------------ 失败/成功反馈
    def mark_failure(self, rule_id: int, target: str) -> None:
        with self._lock:
            entry = self._rules.get(rule_id)
            if not entry:
                return
            st = entry["state"].get(target)
            if st is None:
                st = {"healthy": True, "fails": 0}
                entry["state"][target] = st
            st["fails"] += 1
            if st["fails"] >= HEALTH_FAIL_THRESHOLD and st["healthy"]:
                st["healthy"] = False
                logger.warning("上游判定为不健康: rule=%s target=%s (连续失败 %d 次)", rule_id, target, st["fails"])

    def mark_success(self, rule_id: int, target: str) -> None:
        with self._lock:
            entry = self._rules.get(rule_id)
            if not entry:
                return
            st = entry["state"].get(target)
            if st is None:
                st = {"healthy": True, "fails": 0}
                entry["state"][target] = st
            if not st["healthy"]:
                logger.info("上游恢复健康: rule=%s target=%s", rule_id, target)
            st["healthy"] = True
            st["fails"] = 0

    # ------------------------------------------------------------------ 后台探测
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop = False
        self._thread = threading.Thread(target=self._run, name="upstream-health", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while not self._stop:
                now = time.monotonic()
                probes = []
                with self._lock:
                    for rid, entry in self._rules.items():
                        if not entry["enabled"]:
                            continue
                        if now - entry["last_probe"] < entry["interval"]:
                            continue
                        entry["last_probe"] = now
                        for t in entry["targets"]:
                            probes.append((rid, t, entry["path"], entry["timeout"]))
                for rid, target, path, timeout in probes:
                    if self._stop:
                        break
                    ok = self._probe(loop, target, path, timeout)
                    if ok:
                        self.mark_success(rid, target)
                    else:
                        self.mark_failure(rid, target)
                # 分段 sleep，保证 stop 能较快退出
                for _ in range(PROBE_TICK_SECONDS):
                    if self._stop:
                        break
                    time.sleep(1)
        finally:
            loop.close()

    def _probe(self, loop, target: str, path: str, timeout: int) -> bool:
        parsed = urlparse(target)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if not host:
            return False
        if path:
            url = target.rstrip("/") + "/" + path.lstrip("/")
            try:
                return bool(loop.run_until_complete(self._http_probe(url, timeout)))
            except Exception as exc:  # noqa: BLE001
                logger.debug("HTTP 探测异常 %s: %s", url, exc)
                return False
        # TCP 建连探测
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("TCP 探测失败 %s:%s: %s", host, port, exc)
            return False

    @staticmethod
    async def _http_probe(url: str, timeout: int) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    return resp.status < 500
        except Exception:  # noqa: BLE001
            return False
