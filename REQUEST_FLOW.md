# 请求全链路流程图

> 维护说明：本文档与 `main.py` / `proxy_core.py` 当前实现逐段核实对齐（含 HOTLINK_PROTECTION.md 阶段 1-4 全部功能）。
> 改动请求处理链路时请同步更新本文档。最后更新：2026-09-05。

---

## 1. 全链路总览

```mermaid
flowchart TD
    A[客户端请求] --> B["logging_middleware<br/>生成 request_id · 提取真实 IP · 全程计时"]
    B --> C{路径分流<br/>aiohttp 路由表}
    C -->|"/_admin /_admin/static"| D["管理后台<br/>鉴权门禁 + no-cache"]
    C -->|"/_block/:token"| E["封禁确认页<br/>邮件申诉 token 流程"]
    C -->|"其余全部路径"| F["handle_proxy 代理主链路"]

    F --> S1["① 签名 URL 校验（全局开关）"]
    S1 -->|"失败：缺参 / 过期 / HMAC 不匹配"| X1["403 + 路由日志 signature_invalid"]
    S1 -->|"通过：剥离 _st/_sig 防泄漏上游"| S2["② 路由匹配 + 防护检查链（见 §2）"]
    S2 -->|"无匹配规则"| X2["404 route_miss（计入自动封禁）"]
    S2 -->|"防护拦截"| X3["403 + 路由日志（见 §2）"]
    S2 -->|"通过"| S3["③ IP 封禁检查"]
    S3 -->|"在封禁名单"| X4["403 + 路由日志 cache_status=BANNED"]
    S3 -->|"未封禁"| S4["④ 请求去重（同 IP+Method+URL+Range）"]
    S4 -->|"窗口内命中"| X5["直接返回缓存响应（cache_status=DEDUP）"]
    S4 -->|"未命中"| S5["⑤ 上游转发（流式 / 标准，见 §3）"]
    S5 --> R["响应返回客户端"]

    R -.->|非 /_ 路径| T["middleware 收尾<br/>访问日志 · auto_ban 计数<br/>route-miss 404 / 状态码累计 → 自动封禁 + 邮件"]
```

要点：**每个阶段失败都会短路**——立即写一条路由日志（带对应 `result_status`）并返回错误页，不继续执行后续阶段。

---

## 2. 防护检查链（select_route 内部）

检查按「**先拦来源、再拦客户端**」排序；各检查**留空即不启用、零开销**。

```mermaid
flowchart TD
    A[进入 select_route] --> B["规则匹配<br/>request_host + 路径前缀最长匹配 · priority 排序"]
    B -->|无候选| F1["404 未匹配路由<br/>route_miss=True"]
    B --> C["访问控制<br/>路由组级 + 规则级 IP/地区黑白名单<br/>地区判定走 GeoIP（在线源 + MMDB + 结果缓存）"]
    C -->|命中黑名单 / 不在白名单| F2["403 访问控制拦截"]
    C --> D["Referer 白名单（阶段 2）<br/>域名匹配，支持 *.suffix 通配<br/>空 Referer 按规则级 allow/deny 策略"]
    D -->|未在白名单 / 空 × deny| F3["403 盗链拦截<br/>result_status=hotlink_blocked"]
    D --> E["UA 黑名单 → 白名单（3.1 / 022）<br/>逗号分隔子串 · 大小写不敏感"]
    E -->|"命中黑名单；或白名单启用后未命中 / 无 UA"| F4["403 UA 拦截<br/>result_status=ua_blocked"]
    E --> G["IP 封禁检查<br/>手动封禁 + 自动封禁名单"]
    G -->|已封禁| F5["403 已封禁 IP<br/>cache_status=BANNED"]
    G --> H["放行 → 构建 target_url 进入转发"]
```

设计细节：

- **UA 黑先白后**：同入黑白名单时显式拒绝优先。
- **黑名单对无 UA 放行**（不误伤部分本地播放器）；**白名单启用后无 UA 一并拦截**（防止不发 UA 绕过白名单）。
- 签名 URL 校验在 `select_route` **之前**（handle_proxy 入口），因此无 route_decision，日志用 error_message 前缀区分。

---

## 3. 上游转发双通道

`use_streaming = 规则级 enable_streaming 且 全局 streaming.enabled`

```mermaid
flowchart TD
    A["转发决策"] -->|流式（视频/大文件）| B1["① 单 IP 并发槽位<br/>per-IP 计数 · max_concurrent_per_ip"]
    B1 -->|超限| FB["429 拒绝<br/>Retry-After: 5 + 路由日志"]
    B1 -->|未超限| B2["② chunked 流式写客户端<br/>write_timeout 断开慢客户端<br/>客户端断连安全退出"]
    B2 --> B3["③ finally 收尾<br/>真实字节统计 · record_bytes 进自动封禁窗口<br/>（超 max_bytes 自动封禁）· 路由日志 bytes_transferred"]
    B3 --> R1["200 / 206 流式完成"]

    A -->|标准| C1["① 跟随重定向<br/>≤ max_redirects · ip_result_cache 命中直接回（HIT_REDIRECT）"]
    C1 --> C2["② 响应体处理<br/>大文件自动升级为流式通道"]
    C2 --> C3["③ 收尾<br/>字节补记 · 存入去重缓存（dedup.store）· 路由日志"]
    C3 --> R2["200 标准完成"]

    B1 -.任意环节抛异常.-> X["记录路由日志 proxy_error<br/>500 分类错误页（不暴露内部细节）"]
    C1 -.-> X
```

槽位释放由 `try/finally` 保证，传输结束才释放；`record_bytes` 把本次传输字节计入自动封禁窗口，窗口滑动重置。

---

## 4. 出口结果对照表

所有出口（含成功）都会写一条 `route_logs` 记录，供日志页筛选与盗链监控看板聚合。

| HTTP | result_status | cache_status | 触发条件 |
|------|---------------|--------------|----------|
| 403 | `signature_invalid` | `BLOCKED` | 签名开关开启：`_st/_sig` 缺失、过期或 HMAC 不匹配 |
| 404 | `no_route` | — | request_host + 路径前缀无匹配规则（计入自动封禁 route_miss） |
| 403 | `hotlink_blocked` | `BLOCKED` | Referer 未在白名单；空 Referer 且策略为 deny |
| 403 | `ua_blocked` | `BLOCKED` | UA 命中黑名单；或白名单启用后未命中 / 无 UA |
| 403 | `proxy_error`* | `BLOCKED` | 规则/路由组 IP·地区黑白名单拦截（原因在 error_message） |
| 403 | `proxy_error`* | `BANNED` | IP 在封禁名单（手动封禁或自动封禁已生效） |
| 原状态 | `forwarded` | `DEDUP` | 窗口内同 IP+Method+URL+Range 命中去重缓存 |
| 429 | `proxy_error`* | — | 流式单 IP 并发超限（响应头 `Retry-After: 5`） |
| 透传 | `forwarded` / `forwarded_client_error` / `upstream_error` | `BYPASS` / `HIT_REDIRECT` / `HIT_STREAMING` | 上游正常响应；≥400 分别归为上游 4xx / 上游 5xx |
| 500 | `proxy_error` | — | 转发链路异常（页面仅展示分类原因，不暴露内部细节） |
| 后续 403 | — | — | 流量封禁：窗口内累计字节超 `auto_ban.max_bytes`，自动封禁该 IP（可邮件告警） |

\* `proxy_error` 行的区别靠 `error_message` 字段区分。

---

## 5. 关键入口代码索引

| 环节 | 位置 |
|------|------|
| 路由注册与分流 | `main.py` `_setup_routes`（`/{path:.*}` 兜底进 `handle_proxy`） |
| logging_middleware | `main.py` `_setup_middleware` |
| 签名 URL 校验 + 参数剥离 | `main.py` `handle_proxy` 开头；核心在 `signed_url.py` |
| 防护检查链 | `proxy_core.py` `select_route`（访问控制 → `_check_referer` → `_check_ua_blacklist` → `_check_ua_whitelist`） |
| IP 封禁 / 去重 / 转发决策 | `main.py` `handle_proxy` 中段 |
| 并发槽位 / 流式收尾 | `main.py` `_send_streaming_response` / `_do_send_streaming_response` |
| 路由日志状态推断 | `main.py` `_infer_route_log_result_status` |
