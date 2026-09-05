# 302 加签改写（Signed Redirect Rewrite）实现规划

> 状态：**已实现（未提交，待手动验证）**
> 目标：把"最终跳转权"收回到系统内——客户端只见系统固定签名链接，永远拿不到
> 裸的上游/CDN 地址；签名与领取 IP 绑定，链接转移即失效。

## 0. 背景与动机

当前链路（Emby 场景）：

```
播放器 → 本代理 → 上游302(ttd.xxx) → 又一层代理 → CDN
                ↑
    follow_redirects=false 时，上游 302 的 Location（裸链）直接回给客户端
```

问题：
1. 客户端可见裸的上游/CDN 地址 → 可被嗅探、盗链、直连绕过本系统全部管控；
2. 该地址若长期有效（固定 URL），盗链成本为零；
3. `follow_redirects=true` 时虽然跟随了，但最终 URL 仍可能泄漏，行为不受控。

改写后链路：

```
第一次：播放器 → 本代理 → 上游302 → 【改写】返回 base_url/_signed/{资源id}?_st&_sig
第二次：播放器 → 本代理 /_signed/（验签 + IP 一致性校验）→ 内部跟随/代理 → 媒体流
```

## 1. 总体设计

### 1.1 签名链接形态：固定端点 + 资源id（v2 修订 ①）

改写后的 Location 不再携带原始业务路径，统一为系统**固定签名链接**：

```
{public_base_url}/_signed/{resource_id}?_st={ts}&_sig={hmac}
```

- `public_base_url`：管理员后台配置的对外可达地址（如 `https://media.example.com`），
  未配置时回退请求 Host 头；
- `resource_id`：原始请求资源标识，**无状态编码**：
  `resource_id = base64url(original_path + "?" + original_query)`
  - 不落库、重启不失效、天然多 worker 安全；
  - 不可伪造：整个 URL 由 `_sig = HMAC-SHA256(resource_id + _st + client_ip, secret)`
    保护，篡改 resource_id 即验签失败（密钥只有系统持有）；
- 原始路径较长（Emby /play/... 约 400+ 字符 → base64 后 ~550），
  总 URL 长度约 700 字符，在主流播放器/服务器 2K 限制内；文档记录该权衡。

### 1.2 客户端 IP 绑定（v2 修订 ②，v3 隐式化 ③）

- **签发时**：取当前请求的客户端 IP（经 `extract_client_ip` +
  `trusted_proxy_networks` 解析，防 XFF 伪造），纳入 HMAC 材料，并生成**盲化令牌**
  `_ip = HMAC-SHA256(secret, "ip:" + client_ip)` 写入链接参数；
  **URL 中绝不出现明文客户端 IP**（彻底消除「明文 IP 暴露在链接参数」的隐私/安全风险）；
- **使用时**：`/_signed/` 入口以「当前客户端 IP 重算盲化令牌」与链接 `_ip` 比对
  —— 不一致直接 403（reason=`ip_mismatch`），再验 HMAC（覆盖真实 IP，防篡改）；
- 效果：链接只能在领取它的那个客户端/网络使用，转分享即失效；
- **隐式化要点**：盲化令牌为密钥哈希（无密钥不可离线反推真实 IP，即使 IPv4 2^32 空间亦然），
  因此 `ip_mismatch` 诊断能力完全保留，而真实 IP 不再外泄；
- **已知代价**：手机端 Wi-Fi/流量切换会导致 IP 变化、播放中断后续播失败
  （播放器会重新向 Emby 拿地址，重新走完整链路，可自愈但体验有损）。
  设配置开关 `redirect_signing_bind_ip`（默认**开**，符合"必须一致"要求），
  极端场景可后台关闭放行；
- 时序：ts 校验沿用现有 `verify_signed_url` 的 TTL 逻辑。

### 1.3 防死循环：专用端点天然免疫

`/_signed/{resource_id}` 是独立路由（注册在 admin 路由之后、handle_proxy 通配之前）：

- 处理器内验签 → base64 解码还原 original path/query → **直接调用代理核心管道**
  （路由匹配 → 上游 302 → 强制内部跟随 → 流式返回）；
- 该处理器**永不执行出口改写** → 无二次 302，无需 v1 的 `_mode=redirect` 机制，
  防循环由结构保证（上游持续 302 超过 max_redirects 仍由现有 310 兜底）。

### 1.4 出口改写挂点（共 3 处，全部在返回客户端之前）

| 挂点 | 位置 | 说明 |
|------|------|------|
| HIT_REDIRECT | proxy_core ip_cache 命中分支 | 缓存的 redirect_url 不外发，改写为签名链接 |
| 流式透传 | follow_redirects=false，上游 3xx 原样返回前 | Location 头改写 |
| standard 透传 | handle_request 标准路径 3xx 返回前 | 同上 |

改写函数放 `signed_url.py`：`build_signed_redirect(original_path, original_query,
client_ip, config) -> str`，统一拼 base_url、resource_id、_st/_sig。
原始 path/query 从 aiohttp request 的 `rel_url` 取（挂点处透传）。

### 1.5 配置设计（迁移 024，后台可改）

`system_settings` 新增 4 列（带 LEGACY_SYSTEM_SETTINGS_COLUMNS 幂等补齐）：

| 列 | 类型/默认 | 说明 |
|----|-----------|------|
| `redirect_signing_enabled` | INTEGER, 0 | 总开关（独立于 signed_url_enabled：入口强签 vs 出口改写，语义不同） |
| `redirect_signing_ttl_seconds` | INTEGER, 21600 | 签名链接有效期，默认 6h（须覆盖完整观看会话，播放器 Range 复用同一 URL） |
| `redirect_signing_bind_ip` | INTEGER, 1 | IP 绑定开关（默认开 = 必须一致；关闭则不校验 IP） |
| `public_base_url` | TEXT, '' | 对外可达基础地址，空则回退请求 Host 头 |

- 签名密钥复用 `signed_url_secret`（单一密钥，轮换一次全部生效）；
- 后台「系统设置」新增卡片 + form-note 使用说明（base_url 填写指引、IP 绑定副作用）。

### 1.6 内部管道复用

实现采用 **handle_proxy 请求级重入**（非 HTTP 重放）：`/_signed/` 处理器验签→解码后，
把还原出的真实 path/query 覆盖到当前请求（`_signed_path/_signed_query/_signed_raw_path`），
并置 `_signed_reentry`，再调用 handle_proxy 跑完整管道；handle_proxy 内
`dataclasses.replace(rule, follow_redirects=True)` 强制内部跟随上游 302，专用端点永不外发 3xx。

**规则解耦 + 双模式（id 快照缓存）**：签发 3xx 加签链接的同时，把当时的 `RouteDecision`
（含规则快照，强制 follow）、上游 302 的原始状态码与解析后的绝对 Location 一并按
`resource_id` 登记进 `ProxyRequestHandler._signed_cache`，`kind` 由规则
`follow_redirects` 决定。`/_signed/{id}` 领取时按 kind 分流：

- **A 模式 `redirect_return`**（规则 `follow_redirects=False`，默认）：直接回显签发时缓存的
  上游 302（Location=解析后的绝对 CDN 地址），**客户端自行跟随直连 CDN**——媒体流量不经过
  本服务器，与加签前裸链语义一致，服务器零媒体带宽；无服务器侧重入故无循环。
- **B 模式 `proxy_stream`**（规则 `follow_redirects=True`，或 A 的 Location 缺失等降级）：
  用决策快照（或快照缺失时重新规则匹配）**内部代理穿流**——媒体流量经过本服务器。

#### 1.6.1 逐跳链路（代码索引版）

**① 签发时（第一次请求，建快照）**

1. 客户端 `GET /play/xxx` → `handle_proxy`（`main.py:846`）
2. `select_route` 命中规则，读出 `rule.follow_redirects`（`main.py:904`）
   - **关键（2026-09-05 修复）**：首次代理请求（非 `/_signed` 重入）固定
     `force_external_redirect=True`（`main.py:1028`），使 `RedirectHandler` 的
     `follow_redirects=(False if force_external_redirect else rule.follow_redirects)`
     （`proxy_core.py:1886` / `:2321`）**强制为 False**——无论规则 `follow_redirects` 取值，
     上游 3xx 都原样返回并命中四处改写点（`:1914`/`:2136`/`:2304`/`:2399`）生成签名链接。
     修复前 B 模式规则（`follow_redirects=True`）首次请求会内部跟随上游直出 200，完全跳过加签流程。
     （仅 `/_signed` 重入 `_signed_reentry` 命中快照时 `force_external_redirect=False`，允许内部跟随。）
   - **关键（2026-09-05 二次修复）**：改写成功后同步 `redirect_info.redirect_url = 改写后的签名链接`
     （四处改写点均用 `replace(redirect_info, redirect_url=rewritten)`：`proxy_core.py:1914`/`:2136`/`:2304`/`:2399`），
     使路由日志 `redirect_location` 如实记录**实际返回给客户端的签名链接**，而非上游裸 CDN 地址。
     注意：流式实时改写（`:2136`）用独立 `log_redirect_info` 变量、不原地改 `redirect_info`，
     以免污染 `ip_cache.put_redirect`（`:2167`）误存签名链接、破坏后续重入重签。
3. 代理打上游 → 上游返回 `302`，`Location = 裸 CDN 地址`
4. `_build_rewritten_redirect`（`proxy_core.py:557`）改写 `Location` 为 `302 /_signed/{id}`，
   同时 `_remember_signed_redirect`（`proxy_core.py:488`）登记快照：
   ```python
   kind = "redirect_return" if not rule.follow_redirects else "proxy_stream"
   self._signed_cache[resource_id] = {
       "decision": replace(rule, follow_redirects=True),  # 强制跟随快照
       "kind": kind,
       "location": urljoin(原公共入口, 上游302 Location),  # 相对→绝对
       "status": 上游302状态码,
       "created": now,
   }
   ```
5. 客户端收到 `302 Location=/_signed/{id}`

**② 领取时（第二次请求，按快照分流）**

1. 客户端 `GET /_signed/{id}?_st&_sig&_ip` → `handle_signed_resource`（`main.py:749`）
2. `verify_signed_resource` 验签（`_ip` 盲化令牌：当前 IP 重算令牌比对，不一致 → `ip_mismatch`）→ 失败直接 `403`
3. `decode_resource_id` 还原 `path?query`（`base64url`，无状态、重启安全）
4. `get_signed_redirect_entry(resource_id)` 按 id 换取签发时快照（`main.py:819`）

- **A 模式（`kind=redirect_return`）→ 走 CDN 流量**
  ```python
  if entry.get("kind") == "redirect_return" and entry.get("location"):
      return web.Response(status=entry["status"],
                          headers={"Location": entry["location"]})
  ```
  → 直接回显签发时缓存的上游 `302`（绝对 CDN 地址），客户端自行跟随直连 CDN 拉流，
  **服务器零媒体带宽**；日志记 `cache_status="SIGNED_ECHO"`、`transport_mode="redirect"`。
- **B 模式（`kind=proxy_stream`）→ 走本地服务器流量**
  ```python
  decision = entry.get("decision")
  request["_signed_route_decision"] = decision
  return await self.handle_proxy(request)
  ```
  → `handle_proxy` 内 `main.py:899` 命中 `_signed_route_decision`，**跳过 `select_route`**
  （链路口志「路由:签名快照」），用强制跟随的 decision 快照**内部代理穿流**，
  媒体经服务器回传；**永不外发 3xx**（防循环）。
  - **日志准确性（2026-09-05 修复）**：客户端实际拿到的是 `200`（服务器代理穿流），并无外部重定向。
    但 `redirect_handler` 内部跟随上游会把 `redirect_count=1` / `redirect_location=上游URL` 写进 `redirect_info`，
    这只是服务器内部跳板，**不应在路由日志/链路日志里显示为"重定向"**——
    `_build_route_log_payload`（`main.py:694-695`）对 `request["_signed_reentry"]` 强制把
    `redirect_count`/`redirect_location` 归零；标准路径的 `重定向:{N}` 链路口志（`main.py:1087-1088`）
    也加 `not _signed_reentry` 守卫，避免与紧随的 `代理:200` 自相矛盾。
    （内部上游跳板详情仍可在链路口志「上游耗时」/`上游首包较慢` 看到，诊断能力不丢。）

**关键设计点**：重入绕开规则匹配（凭 id 换快照，规则被删/改不影响已签发链接）；
防循环（A 回显加签前裸链、B 永不外发 3xx）；无状态/重启安全
（`resource_id=base64url(path?query)`，`decision` 快照内存缓存 TTL 默认 21600s、上限 1 万）；
降级（快照缺失/过期回退 `select_route` 重新匹配，「新签发」仍需规则在场）；
验签含 `_ip` 盲化令牌绑定（`bind_ip` 可配，URL 不含明文 IP）防冒领。

## 2. 实施步骤（按依赖顺序）

| 步骤 | 内容 | 产出 |
|------|------|------|
| S1 | config.py `SignedRedirectConfig`（enabled/ttl/bind_ip/base_url）+ 迁移 024 + LEGACY 补齐 + config_store load/update/get API（部分更新语义） | 配置层 |
| S2 | signed_url.py：`encode_resource_id(path, query)` / `decode_resource_id(id)` / `build_signed_redirect(..., client_ip)`（HMAC 材料含 IP） | 签发工具 |
| S3 | signed_url.py：`verify_signed_resource(id, st, sig, client_ip)`（含 IP 一致性，返回 reason: missing/expired/invalid/ip_mismatch） | 校验工具 |
| S4 | proxy_core 三个 3xx 出口接改写开关（配置与原始 path/query 经参数透传） | 出口改写 |
| S5 | main.py：注册 `/_signed/{resource_id}` 路由 + handle_proxy 主体提取为 `_proxy_flow` + _signed 处理器（验签→还原→force_follow） | 入口闭环 |
| S6 | admin_console API（GET/PUT /_admin/api/redirect-signing）+ 前端卡片/表单/说明 | 后台 |
| S7 | `_test_redirect_signing.py`：resource_id 编解码往返/签名格式/验签通过/篡改 id 拒绝/过期拒绝/**IP 不一致拒绝（bind_ip 开）**/IP 变化放行（关）/防循环（_signed 内部无二次 302）/HIT_REDIRECT 改写/迁移 024 旧库补齐 | 测试 |
| S8 | HOTLINK_PROTECTION.md 状态表 + REQUEST_FLOW.md 链路图更新 | 文档 |

## 3. 风险与兼容性

| 风险 | 评估/对策 |
|------|-----------|
| IP 漂移（移动网络切换） | 播放中断后续播需重新取址（播放器自动重走链路，可自愈）；提供 bind_ip 开关兜底 |
| XFF 伪造绕过 IP 校验 | 复用 trusted_proxy_networks 白名单，非可信直连不采信 XFF；反代必须在本机/可信网段 |
| URL 长度 | 原始 path+query base64 后 ~700 字符总长，主流组件 2K 限制内；异常超长 path 跳过改写直接放行（记日志） |
| 播放器兼容 | 仍是一次 302 → 200 流；固定端点 URL 更短更规范，兼容性优于 v1 |
| ip_cache 兼容 | HIT_REDIRECT 命中的 redirect_url 仅作改写输入，缓存语义不变 |
| 重启/多 worker | 链接有效性不依赖内存状态（验签无状态）；但 id 快照为进程内缓存，重启后缺失 → 回退规则重入，规则仍在则不受影响 |
| 规则变更（删/改） | A 模式领取只依赖缓存快照与 302 Location，不重新匹配规则 → 已签发链接不受影响；B 模式快照缺失时回退规则匹配，规则失效则 404（需重新取址） |
| 与其他防护层叠加 | 验签在最前，与 Referer/UA/并发/流量封禁正交，无冲突 |
| 性能 | 每次改写/验签各 1 次 HMAC（微秒级）；base64 编解码可忽略 |

## 4. 待确认项（实施前）

1. **TTL 默认值**：默认 6h（1~72h 可调），是否合适；
2. **public_base_url**：部署环境对外地址（协议+域名+端口）；不提供则先以 Host 回退上线观察；
3. **bind_ip 默认开**：已按"必须一致"设为默认开启，确认接受移动网络切换需重新取址的代价。

## 5. 验收标准

- 开启后：客户端收到的一切 3xx Location 均为 `{base_url}/_signed/{id}?_st&[_ip]&_sig` 形态（`_ip` 为盲化令牌，URL 不含明文 IP）；
- 验签通过 → 按快照 `kind` 双模式出流：A=回显上游 302（CDN 直连，服务器零媒体带宽）；B=内部代理穿流；
  日志链路分别含 `签名重入:换取302(..)->CDN直连` / `签名重入:缓存命中(内部代理)`；
- **规则解耦**：签发后删除/停用规则，A 模式签名链接领取仍 302→CDN（不依赖规则）；
- 篡改 resource_id / 过期 / **换 IP 使用** → 403 + `signature_invalid` 落日志（区分 ip_mismatch 原因）；
- 关闭开关后行为与现状完全一致（零改动回退）。

## 6. 实现清单（已完成，未提交）

| 步骤 | 文件 | 说明 |
|------|------|------|
| S1 | config.py / config_store.py / migrations/024 | `SignedRedirectConfig`；迁移 024 四列；LEGACY_SYSTEM_SETTINGS_COLUMNS 补齐；load/get/update（部分更新） |
| S2-S3 | signed_url.py | `encode_resource_id/decode_resource_id`、`build_signed_redirect`（含 `_ip` 盲化令牌）、`verify_signed_resource`（missing/expired/invalid/ip_mismatch；`_ip` 以当前 IP 重算令牌比对，区分 ip_mismatch） |
| S4 | proxy_core.py | `_build_rewritten_redirect`；四处 3xx 出口改写（HIT_REDIRECT×2、流式/standard 302 透传）；签发时登记 `_signed_cache`（resource_id→{决策快照(强制 follow), kind=redirect_return/proxy_stream, location(绝对化), status, created}，TTL=签发 TTL、上限 1 万，开关关闭/热更时清空） |
| S5 | main.py | `/_signed/{resource_id}` 路由 + `handle_signed_resource`（验签→解码→**按 kind 双模式领取**：A 回显缓存 302→CDN 直连 / B 置 `_signed_route_decision`）；handle_proxy 重入覆盖 path/query、跳过入口签名；命中快照跳过 select_route，缺失回退常规匹配；`replace` 强制 follow_redirects |
| S6 | admin_console.py + 前端 | `GET/PUT /_admin/api/redirect-signing`；安全页「302 加签改写」卡片 + 表单说明 |
| S7 | _diagnostics/_test_redirect_signing.py | 6 组场景全过（编解码/签发验签/配置持久化/出口改写/端点 IP 绑定/id 双模式快照语义）；E2E：A 领取 302→客户端直连 CDN 200 + B 内部代理 200 |

> 注意：`_ip` 现仅含**盲化令牌**（HMAC(secret, "ip:"+client_ip) 的十六进制摘要，非明文 IP），
> 用于验签时以「当前 IP 重算令牌」区分「换 IP 使用」（ip_mismatch）；真实 IP 仍纳入主 HMAC 防篡改。
> 需在反代层正确传递客户端真实 IP（trusted_proxy_networks 可信网段内），否则 IP 绑定误伤。
