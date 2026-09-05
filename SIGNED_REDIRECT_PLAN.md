# 302 加签改写（Signed Redirect Rewrite）实现规划

> 状态：**规划 v2（已吸收两点修订：固定签名链接+资源id、客户端 IP 绑定）**
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

### 1.2 客户端 IP 绑定（v2 修订 ②）

- **签发时**：取当前请求的客户端 IP（经 `extract_client_ip` +
  `trusted_proxy_networks` 解析，防 XFF 伪造）纳入 HMAC 材料；
- **使用时**：`/_signed/` 入口以同样的 IP 解析逻辑取当前 IP，参与重算 HMAC；
  **IP 不一致 → 403**，`result_status=signature_invalid` 落路由日志；
- 效果：链接只能在领取它的那个客户端/网络使用，转分享即失效；
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

`/_signed/` 处理器还原 path/query 后**不重放 HTTP 请求**，而是提取
handle_proxy 的主体为可复用方法（如 `_proxy_flow(request, path, query, *,
force_follow=True, allow_rewrite=False)`），原 handle_proxy 与 _signed 共用；
`force_follow=True` 覆盖 rule.follow_redirects，`allow_rewrite=False` 关闭改写。

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
| 重启/多 worker | resource_id 无状态编码，不依赖内存映射，重启后已发链接仍可用（TTL 内） |
| 与其他防护层叠加 | 验签在最前，与 Referer/UA/并发/流量封禁正交，无冲突 |
| 性能 | 每次改写/验签各 1 次 HMAC（微秒级）；base64 编解码可忽略 |

## 4. 待确认项（实施前）

1. **TTL 默认值**：默认 6h（1~72h 可调），是否合适；
2. **public_base_url**：部署环境对外地址（协议+域名+端口）；不提供则先以 Host 回退上线观察；
3. **bind_ip 默认开**：已按"必须一致"设为默认开启，确认接受移动网络切换需重新取址的代价。

## 5. 验收标准

- 开启后：客户端收到的一切 3xx Location 均为 `{base_url}/_signed/{id}?_st&_sig` 形态；
- 验签通过 → 内部跟随上游 302 → 200 媒体流，日志链路含 `签名:通过`，全程无裸链外泄；
- 篡改 resource_id / 过期 / **换 IP 使用** → 403 + `signature_invalid` 落日志（区分 ip_mismatch 原因）；
- 关闭开关后行为与现状完全一致（零改动回退）。
