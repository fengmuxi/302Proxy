"""签名 URL 核心：时效性 HMAC 签名/校验（HOTLINK_PROTECTION.md 阶段 4）。

纯函数、无 I/O、无外部依赖，便于单测与复用。签名消息体统一为
``"{path}\\n{st}"``（换行符分隔，防路径与时间戳拼接歧义），
``_st`` 为到期 Unix 秒，``_sig`` 为 HMAC-SHA256 的十六进制摘要。

用法：
    url = sign_url("/d/x.m3u8", secret, ttl_seconds=3600)
    ok, reason = verify_signed_url("/d/x.m3u8", st, sig, secret)
"""

from __future__ import annotations

import base64
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


# ===== 302 加签改写 =====

def _resource_message(resource_id: str, st: int, client_ip: Optional[str]) -> bytes:
    """构造资源签名消息体：``resource_id\\nst``（bind_ip 时追加 ``\\nclient_ip``）。

    换行符分隔 + client_ip 参与 HMAC，实现「领取与使用 IP 必须一致」。
    """
    parts = [resource_id, str(int(st))]
    if client_ip:
        parts.append(client_ip)
    return "\n".join(parts).encode("utf-8")


def _hmac_hex_message(message: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _blind_ip(client_ip: str, secret: str) -> str:
    """把客户端真实 IP 盲化：返回 ``HMAC-SHA256(secret, "ip:" + client_ip)`` 的十六进制摘要。

    仅用于在签名链接中**隐式**携带 IP 绑定信息——URL 里出现的 ``_ip`` 不再是明文
    客户端 IP，而是该 IP 的密钥哈希令牌。无密钥者无法反推真实 IP（即使是 IPv4 的
    2^32 空间也无法在缺少密钥时离线反查），从而消除「明文 IP 暴露在链接参数中」的
    隐私/安全风险，同时仍可在验签时以「当前 IP 重算令牌是否一致」来保留
    ``ip_mismatch`` 诊断。
    """
    message = f"ip:{client_ip}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def encode_resource_id(path: str, query: str = "") -> str:
    """把「原始请求资源」无状态编码为 base64url 短标识（不落库、重启不失效）。

    Args:
        path: 请求路径（以 ``/`` 开头）。
        query: 原始 query_string（可空）。

    Returns:
        base64url（去填充）字符串；防篡改由外层 HMAC 保证。
    """
    raw = path if not query else f"{path}?{query}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def decode_resource_id(resource_id: str) -> str:
    """解码 resource_id 还原 ``path`` 或 ``path?query``（不含签名参数）。

    Raises:
        ValueError: 非法 base64 / 非 UTF-8 内容。
    """
    pad = "=" * (-len(resource_id) % 4)
    raw = base64.urlsafe_b64decode(resource_id + pad).decode("utf-8")
    if not raw.startswith("/"):
        raise ValueError("资源标识非法")
    return raw


def build_signed_redirect(
    path: str,
    query: str,
    client_ip: str,
    secret: str,
    ttl_seconds: int = 21600,
    base_url: str = "",
    bind_ip: bool = True,
    now: Optional[int] = None,
) -> str:
    """把原始请求改写为系统固定签名链接：``{base_url}/_signed/{rid}?_st&[_ip]&_sig``。

    bind_ip=True 时额外带 ``_ip`` 参数，但**只放盲化令牌**（``_blind_ip(client_ip)``），
    不放明文 IP；令牌用于验签时区分「换 IP 使用」（``ip_mismatch``），且真实 IP 同时
    纳入 HMAC 防篡改。

    Args:
        path: 原始请求路径（以 ``/`` 开头）。
        query: 原始 query_string。
        client_ip: 领取链接的客户端 IP（bind_ip=True 时纳入签名与盲化令牌）。
        secret: 签名密钥（复用 signed_url.secret）。
        ttl_seconds: 有效期秒数。
        base_url: 对外基础地址（可含协议/端口，末尾斜杠会被剥掉）。
        bind_ip: 是否绑定客户端 IP。
        now: 注入时间戳（测试用）。

    Raises:
        ValueError: secret 为空 / path 不以 / 开头 / 有效期非法。
    """
    if not secret:
        raise ValueError("签名密钥不能为空")
    if not path.startswith("/"):
        raise ValueError("签名路径必须以 / 开头")
    if ttl_seconds < 1:
        raise ValueError("有效期必须为正数")

    resource_id = encode_resource_id(path, query)
    expiry = int(now if now is not None else time.time()) + int(ttl_seconds)
    sig = _hmac_hex_message(
        _resource_message(resource_id, expiry, client_ip if bind_ip else None),
        secret,
    )
    prefix = (base_url or "").rstrip("/")
    params = f"_st={expiry}"
    if bind_ip:
        # 盲化：URL 中只放 HMAC 令牌，绝不暴露明文客户端 IP
        params += f"&_ip={_blind_ip(client_ip, secret)}"
    return f"{prefix}/_signed/{resource_id}?{params}&_sig={sig}"


def verify_signed_resource(
    resource_id: str,
    st: Optional[str],
    sig: Optional[str],
    ip_param: Optional[str],
    client_ip: str,
    secret: str,
    bind_ip: bool = True,
    now: Optional[int] = None,
) -> Tuple[bool, str]:
    """校验固定签名链接（``/_signed/{resource_id}``）。

    bind_ip=True 时：``ip_param`` 为 ``_blind_ip(client_ip)`` 盲化令牌（链接签发时写入，
    非明文 IP），须与「当前客户端 IP 重算的令牌」一致；同时 HMAC 覆盖真实 IP 防篡改。

    Returns:
        ``(是否通过, 失败原因)``；原因取值：``missing`` / ``expired`` /
        ``invalid``（签名错误或格式非法）/ ``ip_mismatch``（bind_ip 开启且
        领取 IP 与使用 IP 不一致）。
    """
    if not secret:
        return False, "invalid"
    if not resource_id or not st or not sig:
        return False, "missing"

    try:
        expiry = int(st)
    except (TypeError, ValueError):
        return False, "invalid"

    current = int(now if now is not None else time.time())
    if current > expiry:
        return False, "expired"

    if bind_ip:
        if not ip_param:
            return False, "missing"
        # 隐式 IP 绑定：用当前客户端 IP 重算盲化令牌，与链接中的令牌比对；
        # 不一致即「换 IP 使用」，区分 ip_mismatch（不再比对明文 IP）。
        if _blind_ip(client_ip, secret) != ip_param:
            return False, "ip_mismatch"
        expected = _hmac_hex_message(
            _resource_message(resource_id, expiry, client_ip),
            secret,
        )
    else:
        expected = _hmac_hex_message(
            _resource_message(resource_id, expiry, None),
            secret,
        )

    if not hmac.compare_digest(expected, str(sig)):
        return False, "invalid"
    return True, ""
