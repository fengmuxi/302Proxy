"""签名 URL 核心：时效性 HMAC 签名/校验（HOTLINK_PROTECTION.md 阶段 4）。

纯函数、无 I/O、无外部依赖，便于单测与复用。签名消息体统一为
``"{path}\\n{st}"``（换行符分隔，防路径与时间戳拼接歧义），
``_st`` 为到期 Unix 秒，``_sig`` 为 HMAC-SHA256 的十六进制摘要。

用法：
    url = sign_url("/d/x.m3u8", secret, ttl_seconds=3600)
    ok, reason = verify_signed_url("/d/x.m3u8", st, sig, secret)
"""

from __future__ import annotations

import hmac
import hashlib
import time
from typing import Optional, Tuple
from urllib.parse import parse_qsl, urlencode


def _hmac_hex(path: str, st: int, secret: str) -> str:
    """计算签名：HMAC-SHA256(secret, "path\\nst") 的十六进制摘要。"""
    message = f"{path}\n{int(st)}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def sign_url(
    path: str,
    secret: str,
    ttl_seconds: int = 3600,
    now: Optional[int] = None,
) -> str:
    """签出一个相对 URL，形如 ``/d/x.m3u8?_st=1700000000&_sig=abcd...``。

    Args:
        path: 代理路径（以 ``/`` 开头）。
        secret: 签名密钥（32 位 hex）。
        ttl_seconds: 有效期秒数。
        now: 当前 Unix 秒（注入以便测试；默认取当前时间）。

    Returns:
        携带 ``_st``/``_sig`` 查询参数的相对 URL。

    Raises:
        ValueError: secret 为空或 path 不以 ``/`` 开头。
    """
    if not secret:
        raise ValueError("签名密钥不能为空")
    if not path.startswith("/"):
        raise ValueError("签名路径必须以 / 开头")
    if ttl_seconds < 1:
        raise ValueError("有效期必须为正数")

    expiry = int(now if now is not None else time.time()) + int(ttl_seconds)
    sig = _hmac_hex(path, expiry, secret)
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}_st={expiry}&_sig={sig}"


def verify_signed_url(
    path: str,
    st: Optional[str],
    sig: Optional[str],
    secret: str,
    now: Optional[int] = None,
) -> Tuple[bool, str]:
    """校验签名是否有效。

    Args:
        path: 请求路径。
        st: ``_st`` 查询参数（到期 Unix 秒）。
        sig: ``_sig`` 查询参数（HMAC 十六进制）。
        secret: 签名密钥。
        now: 当前 Unix 秒（注入以便测试；默认取当前时间）。

    Returns:
        ``(是否通过, 失败原因)``；失败原因取值：
        ``missing``（缺参数）/ ``expired``（已过期）/ ``invalid``（签名错误或格式非法）。
    """
    if not secret:
        return False, "invalid"
    if not st or not sig:
        return False, "missing"

    try:
        expiry = int(st)
    except (TypeError, ValueError):
        return False, "invalid"

    current = int(now if now is not None else time.time())
    if current > expiry:
        return False, "expired"

    expected = _hmac_hex(path, expiry, secret)
    if not hmac.compare_digest(expected, str(sig)):
        return False, "invalid"
    return True, ""


def strip_signature_params(query_string: str) -> str:
    """从 query_string 移除 ``_st``/``_sig``，返回干净 query（供上游转发）。

    保留其余参数与顺序；无 query 或仅含签名参数时返回空串。
    """
    if not query_string:
        return ""
    pairs = [
        (k, v)
        for k, v in parse_qsl(query_string, keep_blank_values=True)
        if k not in ("_st", "_sig")
    ]
    return urlencode(pairs)
