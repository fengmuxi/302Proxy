# 开发状态与维护须知（DEVELOPMENT_STATUS）

> 本文由 `IMPLEMENTATION_PLAN.md` / `MISSING_FEATURES_ANALYSIS.md` / `PHASE1_COMPLETION.md` / `PHASE2_SUMMARY.md` / `PHASE3_SUMMARY.md` 合并整理而来。
> **读者对象**：维护者 / 后续 Agent。面向用户的特性与版本说明见 `README.md` 的「功能特性」「更新日志」。
> **当前状态**：规划功能（除明确取消/暂缓项）均已落地，**代码均未提交**，待评审后按职责分批提交。

---

## 一、实现状态总览

| 阶段 | 项 | 状态 | 对应版本 |
|------|----|------|----------|
| P0-1.1 | 自动化测试套件（pytest + CI） | ❌ 取消（用户决定不维护，临时脚本本地验证） | — |
| P0-1.2 | 后台登录防爆破（失败锁定 + 退避） | ✅ 已实现 | v5.1.0 |
| P0-1.3 | 管理操作审计日志 + 审计页 | ✅ 已实现 | v5.1.0 |
| P1-2.1 | 主动速率限制（令牌桶 → 429 + Retry-After） | ✅ 已实现 | v5.2.0 |
| P1-2.2 | 上游健康检查 + 多目标负载均衡 | ✅ 已实现 | v5.2.0 |
| P1-2.3 | API Key 细粒度权限（scope / IP / 速率） | ✅ 已实现 | v5.2.0 |
| P1-2.4 | Webhook / IM 告警（飞书 / 钉钉 / Slack / Generic） | ✅ 已实现 | v5.2.0 |
| P2-3.2 | CORS 处理（全局 + 规则级覆盖、预检 204） | ✅ 已实现 | v5.3.0 |
| P2-3.3 | 每规则自定义请求头 + 上游 TLS / mTLS | ✅ 已实现 | v5.3.0 |
| P2-3.4 | Prometheus 指标导出（`/metrics`，可选依赖） | ✅ 已实现 | v5.3.0 |
| P2-3.5 | 配置版本历史 / 单模块导出导入 / 回滚 | ✅ 已实现 | v5.3.0 |
| P2-3.1 | WebSocket / HTTP Upgrade 隧道 | ⏸ 暂缓（媒体代理无场景） | — |
| P2-4.1 | 组级默认 + 规则继承（10 字段哨兵语义） | ✅ 已实现（本会话） | v5.4.0 |
| P2-4.2 | 三段式开关 UI（替代三态下拉） | ✅ 已实现（本会话） | v5.4.0 |
| 修复 | 系统设置页整页下拉空白（upgradeSelect 包含块） | ✅ 已修复（本会话） | v5.4.0 |

---

## 二、数据库 schema 落地机制（关键不变量）

- **双机制幂等补列**：新表走基表 `CREATE TABLE IF NOT EXISTS` 块；新 `system_settings` 列必须**同时**写进「基表 CREATE 语句」与 `LEGACY_SYSTEM_SETTINGS_COLUMNS` 元组；旧库补列靠 `_ensure_column`（`config_store`）。
- **迁移文件仅占位**：yoyo 迁移（`migrations/0NN_*.py`）只在**全新库**执行；已有库由基表 + LEGACY 元组 + `_ensure_column` 保证补齐。**切勿在迁移里再写 `ALTER TABLE ... ADD COLUMN`**，否则对已有库触发 duplicate column。
- `_initialize_schema` 末尾对关键新增列额外 `_ensure_column` 一次，堵 `_run_migrations` fresh 分支提前 return 跳过补列的洞（如 `route_groups.rule_defaults`）。
- 读取一律 `"col" in row.keys()` 防御 + 默认值。
- **新增表/列落地机制清单**：新表 CREATE 必须写进基表块；新 `system_settings` 列必须同时加进基表 CREATE 和 LEGACY 元组；迁移文件里再写 `ALTER` 会 duplicate column。

## 三、配置加载优先级

`CLI -p/--host > config.yaml（server/ssl/logging 段显式键） > system_settings`。yaml 只覆盖显式写出的键，空段/缺段不覆盖；覆盖结果回写 `system_settings` 保持后台 UI 一致。其余运行时配置（streaming / 封禁 / 邮箱 / geoip 等）以数据库为准，后台「系统设置」可改。

## 四、签名 URL / 302 加签不变量

- 同 IP 同链接**不会**回放旧签名串：整串每次重新签发；`ip_result_cache.put_redirect` 必须存上游裸地址（非签名链接），命中走 `_build_rewritten_redirect` 重新签发。
- `_signed_cache` 存的是上游 302 决策快照（key = resource_id），**不是签名链接本身**。
- `force_external_redirect` 三态：首次代理请求强制 False 跟随后改写生成签名链接；`/_signed/` 重入允许内部跟随；缓存分支在加签开启 + 强制外部重定向时改走现签，封堵「原始地址命中流式缓存绕过签名」的洞。

## 五、前置 nginx 反代部署约束

- **绝不能开 `proxy_cache`**：签名链接每客户端专属（`_st`+`_sig`+`_ip`），缓存会错发给别客户端 → 403 / 泄露。应用已按上游能力精确下发 `Accept-Ranges`/`Content-Range`/`Cache-Control`，nginx 不得用 `add_header Cache-Control immutable` 覆盖。
- **Host 头是签名 `base_url` 回退来源**：`base_url = cfg.base_url or f"{scheme}://{headers.get('Host')}"`，用的是原始 Host 头。前置 nginx 必须 `proxy_set_header Host $host;` 透传真实公网 Host；若坚持 `Host 127.0.0.1` 则须在后台把「对外基础地址」(base_url) 配成公网 HTTPS。
- **真实客户端 IP**：`extract_client_ip` 仅在 `trust_forward_headers=True` 且来源落在 `trusted_proxy_networks` 时才解析 `X-Forwarded-For`（从右取首个不可信），防伪造。前置须发 `X-Real-IP` + `X-Forwarded-For` 并把 nginx IP 加入信任网段；`trust_upstream_ip_headers` 保持 False。
- B 模式（proxy_stream 本地代理）才需流式透传优化（`proxy_buffering off` + Range/If-Range 透传 + 长超时）。

## 六、组级默认 + 规则继承不变量（P2-4.1）

- 哨兵：数值/开关 `-1` = 继承组；字符串列（referer_whitelist / ua_blacklist / ua_whitelist / referer_policy）空串 `""` = 继承（组也未配置时等价于「未启用」，后向兼容）；`"-"` = 组有默认时单条规则强制停用（`normalized_*` 方法过滤）。
- `inherit_fields` 标记**优先于** payload 携带的值：`update_rule` 合并的是「已解析有效值 + inherit_fields」，否则 toggle 等局部更新会固化继承为显式值。
- 解析点两处：`load_runtime_config` 尾部 + 单条路径 `_serialize_rule_resolved`（get/create/update 返回值）；`inherit_fields` 保留给 UI。
- `forward_rules` 的 INSERT/UPDATE SQL 有多处副本（`_insert_rule`、update_signed 内嵌 UPDATE、update_rule）：改列时必须全局 grep 核对「列清单 = 值元组 = 占位符数」三处一致。
- 流式转发在 handler 内 `response.prepare(request)`，返回中间件后改头无效：CORS 等响应头必须在构造 `StreamResponse` 时合并（`request["_cors_headers"]` 机制）。
- 新增顶层路由（如 `/metrics`）必须注册在 catch-all `/{path:.*}` `handle_proxy` **之前**（aiohttp 按注册顺序匹配）。
- Prometheus：prometheus_client 为可选依赖（缺库时 no-op，`/metrics` 返回 503）；`requirements.txt` 已加。

## 七、前端接线约定

- 新增页面：admin.html nav `data-page` + `#page-xxx` 两处；modules.js `VALID_PAGES`/`labels`/`activatePage` 三处；admin.js import + `bindXxx()` + init 调用 + PAGE_INDEX/COMMAND_INDEX/命令派发。同一段代码极易被重复插入（HTML 与 JS 都踩过），改完必须做重复检测：HTML 用 HTMLParser 查重复 id，JS 用 `grep '^export \(async \)\?function' | uniq -d`。
- ESM 语法校验：`node --check` 对含 import 的 .js 按 CJS 解析会误报，需复制为 `.mjs` 再 check；git bash 的 `/tmp` ≠ node 看到的 `D:\tmp`，副本放项目内。
- 自定义下拉（upgradeSelect）：全局 `scroll` 监听（捕获阶段）必须放过 `.cs-pop` 自身滚动；弹层收起时机含页面/弹窗滚动、点击外部（须跳过 `.cs-trigger`/`.cs-pop`）、Esc、选中选项；`.cs-pop` 滚动条颜色用 `--text-3`/hover `--text-2`。
- **包含块陷阱（本会话修复）**：隐藏型绝对定位元素（opacity:0 的原生 select）必须挂在一个 `position:relative` 祖先里（upgradeSelect 已挪进 `span.cs`），否则包含块 = 文档根，`.page`/`.app` 的 overflow 裁剪失效 → 整页下拉空白 / 双层滚动。

## 八、测试策略

- **不单独维护自动化测试套件**（用户拍板）。临时调试脚本仅作本地开发期验证，不入库、不接 CI。验证方式：临时脚本 + 后台日志/审计页核对。
- 改动后至少做：Python `py_compile` 全量；JS `node --check`（.mjs 副本）；前端 HTMLParser 重复 id + JS 重复 export 检测。

## 九、提交拆分约定（待评审）

按职责分离，每个功能独立一笔提交；验证后再提交（用户惯例）。历史建议拆分：

1. 登录防爆破（后端 + 前端）
2. 审计日志（后端 + 前端）
3. 限流
4. 多上游 + 健康
5. API Key 细粒度
6. Webhook 告警
7. CORS
8. 自定义头 / TLS
9. Prometheus
10. 配置历史 / 回滚
11. 组级默认 + 规则继承
12. 三段式开关 UI + 整页空白修复
13. 文档整理（本批）

> 各批次验证脚本在 `E:/Temp/`（本地调试用，不入库）。所有 `.py` 须编译通过，JS 须 `node --check` + 重复导出/重复 id 检测通过。
