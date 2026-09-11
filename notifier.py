"""Webhook / IM 告警通知（P1-2.4）。

在邮件（email_notifier.EmailNotifier，保持不动）之外提供 Webhook 推送通道：
  - generic：POST 完整 JSON 事件体（自定义接收端）
  - feishu：飞书自定义机器人文本消息（支持加签）
  - dingtalk：钉钉机器人文本消息（支持加签，签名拼在 URL 上）
  - slack：{"text": ...}

设计要点：
- dispatch(event) 永不抛异常、由调用方以 create_task 兜底——通知失败绝不影响封禁主流程；
- 每次调用重建 aiohttp 会话（告警频率极低，不值得常驻连接池）；
- 接线点：AutoBanMonitor 自动封禁 / 后台手动封禁（main._sync_ban_manager）。
"""

import asyncio
import base64
import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import aiohttp

logger = logging.getLogger("proxy")

_TIMEOUT = aiohttp.ClientTimeout(total=10)


def build_event_text(event: Dict[str, Any]) -> str:
    """事件 → 人类可读文本（所有通道共用）。"""
    etype = str(event.get("type", "event"))
    titles = {
        "auto_ban": "自动封禁触发",
        "manual_ban": "手动封禁",
        "unban": "解除封禁",
        "test": "通知测试",
    }
    title = titles.get(etype, etype)
    lines = [f"【代理监控】{title}"]
    for key, label in (
        ("ip", "IP"),
        ("reason", "原因"),
        ("duration", "时长(秒)"),
        ("source", "来源"),
        ("detail", "详情"),
    ):
        value = event.get(key)
        if value not in (None, "", 0):
            lines.append(f"{label}：{value}")
    lines.append(f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}")
    return "\n".join(lines)


def _feishu_sign(secret: str, timestamp: int) -> str:
    """飞书自定义机器人加签：HMAC-SHA256(key=timestamp+\\n+secret, msg=b"") → base64。"""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), b"", hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _dingtalk_sign(secret: str, timestamp_ms: int) -> str:
    """钉钉机器人加签：HMAC-SHA256(key=secret, msg=timestamp+\\n+secret) → base64 → urlencode。"""
    string_to_sign = f"{timestamp_ms}\n{secret}"
    digest = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    return quote_plus(base64.b64encode(digest).decode("utf-8"))


class NotificationDispatcher:
    """按启用的 Webhook 渠道广播事件。"""

    def __init__(self, notifications_config: Any = None):
        # notifications_config: NotificationsConfig（ duck-typing：enabled + channels ）
        self._enabled = False
        self._channels: List[Dict[str, Any]] = []
        self.update_config(notifications_config)

    def update_config(self, notifications_config: Any) -> None:
        if not notifications_config:
            self._enabled, self._channels = False, []
            return
        self._enabled = bool(getattr(notifications_config, "enabled", False))
        self._channels = list(getattr(notifications_config, "channels", []) or [])

    async def dispatch(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """向全部启用渠道广播；返回各渠道结果（ok/message/channel）。永不抛异常。"""
        results: List[Dict[str, Any]] = []
        if not self._enabled or not self._channels:
            return results
        for channel in self._channels:
            if not channel.get("enabled"):
                continue
            try:
                ok, message = await self._send_to_channel(channel, event)
            except Exception as exc:  # noqa: BLE001 - 通知失败不影响主流程
                ok, message = False, str(exc)
            results.append({
                "channel": f"{channel.get('type')}:{channel.get('name', '')}",
                "ok": ok,
                "message": message,
            })
            if not ok:
                logger.warning("Webhook 通知失败: channel=%s error=%s", results[-1]["channel"], message)
        return results

    def dispatch_bg(self, event: Dict[str, Any]) -> None:
        """后台广播（fire-and-forget），供封禁主流程调用，绝不阻塞。"""
        try:
            asyncio.get_running_loop()
            asyncio.ensure_future(self.dispatch(event))
        except RuntimeError:
            pass  # 无事件循环（如同步上下文），静默放弃

    async def test_channel(self, channel: Dict[str, Any]) -> Tuple[bool, str]:
        """向单渠道发送测试事件，返回 (ok, message)。"""
        return await self._send_to_channel(channel, {"type": "test", "detail": "这是一条测试通知"})

    async def _send_to_channel(self, channel: Dict[str, Any], event: Dict[str, Any]) -> Tuple[bool, str]:
        url = str(channel.get("url", "") or "").strip()
        if not url:
            return False, "未配置 Webhook URL"
        ch_type = str(channel.get("type", "generic"))
        secret = str(channel.get("secret", "") or "").strip()
        text = build_event_text(event)
        payload: Dict[str, Any]
        headers = {"Content-Type": "application/json"}

        if ch_type == "feishu":
            payload: Dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
            if secret:
                ts = int(time.time())
                payload["timestamp"] = str(ts)
                payload["sign"] = _feishu_sign(secret, ts)
        elif ch_type == "dingtalk":
            if secret:
                ts_ms = int(time.time() * 1000)
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}timestamp={ts_ms}&sign={_dingtalk_sign(secret, ts_ms)}"
            payload = {"msgtype": "text", "text": {"content": text}}
        elif ch_type == "slack":
            payload = {"text": text}
        else:  # generic
            payload = {"text": text, "event": event}

        try:
            async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    body = (await resp.text())[:200]
                    if 200 <= resp.status < 300:
                        # 飞书/钉钉 HTTP 200 但业务失败（如签名错）会在 body 里带 code != 0
                        if ch_type == "feishu" and '"code":0' not in body.replace(" ", "") and '"StatusCode":0' not in body:
                            return False, f"飞书返回异常: {body}"
                        return True, "ok"
                    return False, f"HTTP {resp.status}: {body}"
        except asyncio.TimeoutError:
            return False, "请求超时（10s）"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
