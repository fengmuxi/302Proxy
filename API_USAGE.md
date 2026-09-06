# API 密钥使用说明

通过 API 密钥在脚本 / 第三方系统中调用本代理的管理接口（`/_admin/api/*`），无需浏览器登录会话。

## 1. 签发密钥

管理后台侧边栏「**系统运维**」→「**API 密钥**」（独立菜单，已从安全与封禁页拆出）→ 点「+ 签发密钥」：

- **名称**：仅用于辨识用途（≤64 字符）
- **只读模式**：开启后仅允许 GET/HEAD 请求，且无法访问密钥管理本身
- **有效期**：0 = 永久，最长 3650 天

> ⚠️ 密钥明文（`n302_` + 32 位十六进制）**只在签发弹窗中显示一次**，系统仅保存 SHA256 哈希，关闭弹窗后无法找回，请立即复制保存。

## 2. 调用方式

密钥通过以下任一请求头携带（二选一）：

```
Authorization: Bearer n302_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
X-API-Key: n302_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 查询示例（GET）

```bash
# 查看仪表盘聚合数据
curl -H "Authorization: Bearer n302_xxxx" https://your-host/_admin/api/bootstrap

# 查看封禁名单
curl -H "Authorization: Bearer n302_xxxx" https://your-host/_admin/api/banned-ips

# 查看路由日志
curl -H "Authorization: Bearer n302_xxxx" "https://your-host/_admin/api/logs?page=1&limit=50"

# X-API-Key 头等价
curl -H "X-API-Key: n302_xxxx" https://your-host/_admin/api/route-groups
```

### 写入示例（仅读写密钥）

```bash
# 手动封禁 IP
curl -X POST -H "Authorization: Bearer n302_xxxx" \
  -H "Content-Type: application/json" \
  -d '{"ip":"1.2.3.4","reason":"滥用","permanent":true}' \
  https://your-host/_admin/api/banned-ips

# 解封
curl -X DELETE -H "Authorization: Bearer n302_xxxx" \
  https://your-host/_admin/api/banned-ips/1.2.3.4
```

## 3. 权限与限制

| 规则 | 说明 |
| --- | --- |
| 仅限 `/_admin/api/*` | HTML 页面、登录接口、静态资源不接受密钥 |
| 只读密钥 | 仅 GET/HEAD/OPTIONS；写操作返回 `403` |
| 密钥不可管理密钥 | 带密钥访问 `/_admin/api/keys*` 一律 `403`（防权限自增殖），密钥管理只能用浏览器会话 |
| 停用 / 过期密钥 | 所有调用返回 `401` |
| 计数节流 | `use_count` / `last_used_at` 按 60 秒内存节流落库，连续调用只累计一次（防写放大，计数为近似值） |

## 4. 常用端点速查

| 方法 | 端点 | 说明 |
| --- | --- | --- |
| GET | `/_admin/api/bootstrap` | 仪表盘聚合数据（路由组 / 规则 / 统计） |
| GET | `/_admin/api/route-groups` | 路由组列表 |
| GET | `/_admin/api/banned-ips` | 封禁名单 |
| POST | `/_admin/api/banned-ips` | 手动封禁（读写密钥） |
| DELETE | `/_admin/api/banned-ips/{ip}` | 解封（读写密钥） |
| GET | `/_admin/api/logs` | 请求日志（`page` / `limit` / `keyword` 等筛选） |
| GET | `/_admin/api/hotlink/stats` | 盗链监控统计 |
| GET | `/_admin/api/app-logs` | 应用日志文件列表 |
| GET | `/_admin/api/app-logs/content` | 应用日志内容（`file` / `tail_lines`） |

完整接口清单见管理后台各页面实际调用（浏览器 F12 网络面板可对照）。

## 5. 安全建议

- 每个脚本 / 集成方签发独立密钥，按需选只读模式，便于单独吊销
- 设置合理有效期，定期轮换（删除旧密钥 → 签发新密钥）
- 密钥等同管理员凭据，不要提交到代码仓库或日志中

## 6. 在线 API 文档（自动维护）

后台「**API 文档**」页（侧边栏 → 系统运维 → API 文档）汇总了全部 `/_admin/api/*` 接口：

- **自动同步**：文档直接读取路由表，新增接口会自动出现，无需手动登记（未登记的会标「待补充说明」）
- **HEAD 已去重**：所有 GET 接口天然支持 HEAD 请求（aiohttp 的 `add_get` 默认 `allow_head=True` 会为同一 handler 自动注册 HEAD 路由），两者语义相同仅响应体有无之分，故文档只列 GET 不重复展示 HEAD；需要时直接对 GET 路径发 HEAD 即可
- **用法说明**：每个接口附「用法」区块（怎么用、注意事项、与其他接口的关联），开发者可快速上手
- **在线试调用**：粘贴你的 API 密钥后，点任意接口的「试一试」即可用该密钥发起真实请求，实时查看 HTTP 状态码与响应体
- **连通自测**：页头「测试连通」按钮用密钥打一个只读接口，快速验证密钥有效性
- 密钥仅在浏览器本地（localStorage）暂存，方便反复试调用；只读密钥只能试 GET 接口

> 开发者拿到自己的 API 密钥后，直接进「API 文档」页即可自助浏览全部接口并试调用，无需再翻阅本文档。

