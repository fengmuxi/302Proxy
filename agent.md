# Agent 工作规范

## 项目概述
这是一个带 302 重定向处理的 Python HTTP 反向代理服务器，基于 aiohttp 构建。核心功能包括：自动跟踪 HTTP 重定向、大媒体文件流式传输、基于 path_prefix 的分组转发、IP 定位与访问控制、后台管理界面。

## 技术栈
- 后端：Python 3.11+, aiohttp, SQLite3
- 前端：原生 HTML/CSS/JS（无框架）
- 配置：YAML + SQLite 持久化

## 代码修改原则
1. **最小变更**：只修改与需求直接相关的代码，不动无关逻辑
2. **前后端同步**：涉及 UI 的字段变更必须同时更新前端（admin.html/admin.js/admin.css）和后端（config.py/config_store.py/proxy_core.py/main.py）
3. **数据库兼容**：表结构变更需考虑已有数据迁移，新增列应允许 NULL 或有默认值
4. **风格一致**：保持现有代码的命名和格式风格

## 核心模块职责
| 文件 | 职责 |
|------|------|
| main.py | 服务器入口、路由注册、全局中间件、日志记录 |
| proxy_core.py | 请求代理核心：路由选择、URL 构造、传输模式、访问控制检查 |
| config.py | 数据模型、配置解析、工具函数 |
| config_store.py | SQLite 数据库操作：CRUD、DDL 迁移 |
| admin_console.py | 后台 API 路由处理 |
| geo_service.py | IP 归属地查询（在线+离线 MMDB） |
| ip_ban_manager.py | IP 封禁管理（支持临时封禁过期） |

## 关键业务规则
- 一个路由组（相同 path_prefix + request_host）只能有一条默认规则；设置新的默认规则必须先清除同组现有默认规则
- 访问控制检查顺序：前缀 IP 白名单 → 前缀 IP 黑名单 → 前缀地区白名单 → 前缀地区黑名单 → 规则匹配 → 规则 IP 白名单 → 规则 IP 黑名单 → 规则地区白名单 → 规则地区黑名单；任一环节拦截即返回 403
- 正则重写逻辑在 URL 构造时优先于 strip_prefix 执行
- IP 封禁默认全局（path_prefix 为空），支持按前缀封禁
- 未匹配到任何规则的请求直接返回 404，不触发封禁检查

## 前端约定
- 状态列中的标签（启用/禁用、strip_prefix、follow_redirect、streaming）均为可点击切换
- 正则表达式匹配列显示 pattern（蓝底）和 replacement（灰底），过长用省略号，hover 显示完整内容
- 徽标颜色：地区白名单=绿色，IP 黑名单=红色，地区黑名单=黄色，IP 白名单=蓝色
- 前端过滤为纯客户端内存过滤，不请求后端
- CSS tooltip 必须用 `[title]:not([title=''])` 避免空提示

## 调试注意
- 静态文件（JS/CSS）有浏览器缓存，前端修改后需强制刷新（Ctrl+Shift+R）
- 后端代码修改后需重启服务才能生效
- 测试正则重写时，注意 `^/prefix/.*` 会丢弃文件名，如需保留应使用 `^/prefix/`
