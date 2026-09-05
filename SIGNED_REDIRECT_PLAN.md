# 302 加签改写（Signed Redirect Rewrite）实现规划

> 状态：**规划中（待确认后实施）**
> 目标：把"最终跳转权"收回到系统内——客户端永远拿不到裸的上游/CDN 地址，
> 盗链者抓到的固定 URL 过期即失效，所有字节都经过系统管控（并发/流量/封禁）。

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
3. `follow_redirects=true` 时虽然跟随了，但最终 URL 仍出现在 redirect_chain 与日志，
   且客户端一旦拿到，行为不受控。

提案（用户设想，已评估可行）：

```
第一次请求：播放器 → 本代理 → 上游302 → 【改写 Location】返回 本系统加签链接
第二次请求：播放器 → 本代理（验签）→ 内部强制跟随/代理 → 媒体流
```

## 1. 总体设计

### 1.1 加签对象：签「原始客户端路径」（推荐）

- 改写后的 Location = `{public_base_url|请求Host}{原path}?{原query}&_st=..&_sig=..&_mode=redirect`
- 第二跳重入后**重跑完整管道**：路由匹配 → 请求上游拿新 302 → 内部强制跟随 → 流式返回。
- 优点：完全复用现有 `signed_url.py` 的 sign/verify/strip 基础设施与 handle_proxy
  入口校验；上游时效签名地址每次重新获取，天然新鲜。
- 不采用「签上游目标 URL」方案：需要 target 白名单防开放代理，复杂度高，暂缓。

### 1.2 防死循环：`_mode=redirect` 纳入签名

- 出口改写时附加 `_mode=redirect`，与 path 一起参与 HMAC 计算；
- handle_proxy 验签通过后：
  - 剥离 `_st/_sig`（现有逻辑）；
  - 识别 `_mode=redirect` → 设置 `request["_signed_reentry"] = True`；
  - 该请求在 select_route 后**临时覆盖 rule.follow_redirects 为 True**（内部跟随），
    且**不再执行出口改写**（改写逻辑前置判断 `_signed_reentry`）。
- 上游持续 302 超过 max_redirects → 现有 310 错误流兜底，无无限循环。

### 1.3 出口改写挂点（共 3 处，全部在返回客户端之前）

| 挂点 | 位置 | 说明 |
|------|------|------|
| HIT_REDIRECT | proxy_core ip_cache 命中分支 | 缓存的 redirect_url 不外发，改写为加签链接 |
| 流式透传 | follow_redirects=false，上游 3xx 原样返回前 | 过滤后的 Location 头改写 |
| standard 透传 | handle_request 标准路径 3xx 返回前 | 同上 |

改写函数建议放 `signed_url.py`：`build_signed_redirect(request, config) -> str`，
统一处理 public_base_url 回退 Host、query 拼接、签名参数。

### 1.4 配置设计（迁移 024，后台可改）

`system_settings` 新增 3 列（带 LEGACY_SYSTEM_SETTINGS_COLUMNS 幂等补齐）：

| 列 | 类型/默认 | 说明 |
|----|-----------|------|
| `redirect_signing_enabled` | INTEGER, 0 | 总开关（独立于 signed_url_enabled，语义不同：入口强签 vs 出口改写） |
| `redirect_signing_ttl_seconds` | INTEGER, 21600 | 改写链接有效期，默认 6h（须覆盖完整观看会话，播放器 Range 复用同一 URL） |
| `public_base_url` | TEXT, '' | 客户端可达的基础地址（反代/HTTPS 场景必填），空则回退请求 Host 头 |

- 签名密钥复用 `signed_url_secret`（单一密钥，轮换一次全部生效）；
- 后台「系统设置」新增配置卡片 + 使用说明（沿用 form-note 说明模式）。

### 1.5 TTL 与观看时长的关系（关键权衡）

播放器起播后 2~3 小时内会用**同一 URL** 发 Range 请求：
- TTL 内：正常播放；
- TTL 过期：Range 请求 403 → 播放器报错。缓解：播放器通常在失败后重新向 Emby
  拿播放地址（重走完整链路拿新签名），实际影响有限；
- 默认 6h 覆盖绝大多数单片场景，可调（1~72h）。

## 2. 实施步骤（按依赖顺序）

| 步骤 | 内容 | 产出 |
|------|------|------|
| S1 | config.py 新增 `SignedRedirectConfig` + yaml 解析 + 迁移 024 + LEGACY 补齐 + config_store load/update/get API（部分更新语义） | 配置层 |
| S2 | signed_url.py 新增 `build_signed_redirect()`（拼 _mode=redirect、纳入签名、base url 回退） | 签发工具 |
| S3 | proxy_core 三个 3xx 出口接改写开关（route_decision/配置经参数传入，保持 handler 无全局态） | 出口改写 |
| S4 | main.py handle_proxy 验签识别 `_mode=redirect` → `_signed_reentry` 标记 + 强制 follow + 改写跳过 | 入口闭环 |
| S5 | admin_console API + 前端卡片/表单/说明 | 后台 |
| S6 | `_test_redirect_signing.py`：签发格式/验签通过/篡改拒绝/过期拒绝/防循环（重入不再改写）/HIT_REDIRECT 改写/旧库补齐 | 测试 |
| S7 | HOTLINK_PROTECTION.md 状态表 + REQUEST_FLOW.md 链路图更新 | 文档 |

## 3. 风险与兼容性

| 风险 | 评估/对策 |
|------|-----------|
| 播放器兼容 | 改写后仍是"一次 302 → 200 流"，标准播放器兼容；个别只跟一次跳转的播放器无影响（跳转次数不变，只是目标变成本系统） |
| ip_cache 兼容 | HIT_REDIRECT 命中的 redirect_url 直接作为改写输入，语义不变；`_signed_reentry` 请求命中 redirect 缓存时需跳过改写（S4 标记传到 handler） |
| 反代场景 Location 拼错 | public_base_url 必填提示写入前端说明；未配置回退 Host 头并在日志提示 |
| 与盗链防护其他层叠加 | 验签在 handle_proxy 最前，与 Referer/UA/并发/流量封禁正交叠加，无冲突 |
| 性能 | 每次改写 = 1 次 HMAC（微秒级）；重入多一次路由匹配，可忽略 |

## 4. 待确认项（实施前）

1. **开关粒度**：默认按「全局开关」实施（简单、够用）；若需要按规则粒度控制，
   迁移 024 需在 forward_rules 加列，改动量 +30%；
2. **TTL 默认值**：默认 6h，可接受范围 1~72h；
3. **public_base_url**：部署环境的对外可达地址（域名/端口/协议），实施时需要提供
   或先用 Host 回退模式上线观察。

## 5. 验收标准

- 开启后：客户端收到的一切 3xx Location 均指向本系统且带有效签名；
- 无签名/过期/篡改的重入请求 → 403 + `result_status=signature_invalid`；
- 合法重入 → 内部跟随上游 302 → 200 媒体流，日志链路含 `签名:通过`、无二次改写；
- 关闭开关后行为与现状完全一致（零改动回退）。
