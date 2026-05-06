# 带 302 重定向处理的 HTTP 反向代理

## 运行时新增能力

- 本地 SQLite 配置中心，持久化存储在 `data/proxy_config.db`
- 内置后台 `/_admin`，支持本地转发规则增删改查、定位源配置和功能开关
- 支持后台简易登录，账号密码从 `config.yaml` 的 `admin_auth` 读取
- 基于 `path_prefix` 的分组转发，支持分组开关、地区逗号分隔过滤和默认线路回退
- 在线 IP 定位支持多源配置，按权重轮询，单次失败回退，离线 MMDB 兜底
- 双定位策略：在线接口优先，本地 MMDB 次之
- 定位失败时优雅降级到默认转发规则

一个高性能的Python反向代理服务器，能够自动处理并转发302重定向的外网链接地址流量，支持大媒体文件流式传输。

## 功能特性

- **自动处理302重定向**: 透明地跟随所有类型的HTTP重定向（301, 302, 303, 307, 308）
- **高性能异步架构**: 基于aiohttp构建，支持高并发请求处理
- **大媒体文件流式传输**: 支持视频、音频等大文件的流式代理，内存占用低
- **灵活的代理规则**: 支持基于路径前缀的代理规则配置
- **完整的请求/响应头处理**: 正确过滤和转发HTTP头
- **Range请求支持**: 支持断点续传和分段下载
- **SSL/TLS支持**: 可选的HTTPS代理支持
- **重试机制**: 自动重试失败的请求
- **统计监控**: 内置请求统计和健康检查端点
- **可配置超时**: 支持全局和每规则的超时设置

## 项目结构

```
nginx302_proxy/
├── main.py                      # 主程序入口
├── config.py                    # 配置管理模块
├── config_store.py              # 配置存储模块（SQLite）
├── proxy_core.py                # 代理核心逻辑
├── admin_console.py             # 后台管理控制台
├── geo_service.py               # IP 定位服务
├── offline_geoip_sync.py        # 离线 IP 库同步
├── config.yaml.template         # 配置文件模版
├── requirements.txt             # 项目依赖
└── static/                      # 前端资源
    ├── admin.html               # 后台页面
    ├── admin.css                # 后台样式
    └── admin.js                 # 后台脚本
```

## 安装部署

### 环境要求

- Python 3.8+
- pip包管理器

### 安装步骤

1. 克隆或下载项目到本地

2. 创建虚拟环境（推荐）
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. 安装依赖
```bash
pip install -r requirements.txt
```

4. 配置代理规则
编辑 `config.yaml` 文件，配置您的代理规则。服务启动时会读取 `config.yaml` 中的 `server.host` / `server.port`，其余配置写入本地数据库供后台管理使用。

5. 启动服务
```bash
python main.py
```

## 配置说明

### 配置文件结构 (config.yaml)

```yaml
server:
  host: "0.0.0.0"          # 监听地址
  port: 8080               # 监听端口（启动时读取）
  workers: 1               # 工作进程数
  keepalive_timeout: 75    # 保持连接超时（秒）
  max_connections: 1000    # 最大连接数
  max_connections_per_host: 100  # 每主机最大连接数

ssl:
  enabled: false           # 是否启用SSL
  cert_file: null          # SSL证书文件路径
  key_file: null           # SSL私钥文件路径

logging:
  level: "INFO"            # 日志级别：DEBUG, INFO, WARNING, ERROR
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file_path: ./log/nginx302_proxy.log
  max_size: 10MB           # 日志文件最大大小（支持 B/KB/MB/GB）
  backup_count: 5          # 保留的日志文件数量

streaming:
  enabled: true                    # 是否启用流式传输
  chunk_size: 64KB                 # 流式传输块大小（支持 B/KB/MB/GB）
  large_file_threshold: 10MB       # 大文件阈值（支持 B/KB/MB/GB）
  stream_timeout: 3600             # 流式传输超时（秒）
  read_timeout: 300                # 读取超时（秒）
  write_timeout: 300               # 写入超时（秒）
  buffer_size: 64KB                # 缓冲区大小（支持 B/KB/MB/GB）
  enable_range_support: true       # 是否支持 Range 请求
  max_request_body_size: 0         # 最大请求体大小（0 表示无限制，支持 B/KB/MB/GB）

admin_auth:
  enabled: true
  username: "admin"
  password: "123456"
  session_ttl_hours: 12
  cookie_name: "proxy_admin_session"

proxy_rules:
  - path_prefix: "/api"           # 路径前缀
    target_url: "https://api.example.com"  # 目标服务器URL
    strip_prefix: false           # 是否移除路径前缀
    timeout: 30                   # 请求超时（秒）
    max_redirects: 10             # 最大重定向次数
    follow_redirects: true        # 是否自动跟随重定向（规则级别）
    retry_times: 3                # 重试次数
    enable_streaming: true        # 是否启用流式传输

default_timeout: 30        # 默认超时时间
max_redirects: 10          # 默认最大重定向次数
follow_redirects: true     # 是否跟随重定向（全局默认值）
trust_forward_headers: true # 是否添加转发头
```

### 配置参数详解

#### 字节单位说明

配置文件中所有表示大小的参数都支持以下单位格式：

| 单位 | 说明 | 示例 |
|------|------|------|
| B | 字节 | `65536` 或 `64KB` |
| KB | 千字节 (1024 B) | `64KB` = 65,536 字节 |
| MB | 兆字节 (1024 KB) | `10MB` = 10,485,760 字节 |
| GB | 吉字节 (1024 MB) | `2GB` = 2,147,483,648 字节 |
| TB | 太字节 (1024 GB) | `1TB` = 1,099,511,627,776 字节 |

**使用示例：**
```yaml
# 以下格式都被支持
max_size: 10MB      # 推荐：易读
max_size: 10 MB     # 支持空格
max_size: 10.5MB    # 支持小数
max_size: 10485760  # 兼容：纯字节数
```

#### 服务器配置 (server)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host | string | "0.0.0.0" | 服务器监听地址 |
| port | int | 8080 | 服务器监听端口（启动时读取） |
| workers | int | 1 | 工作进程数 |
| keepalive_timeout | int | 75 | HTTP Keep-Alive超时时间 |
| max_connections | int | 1000 | 最大并发连接数 |
| max_connections_per_host | int | 100 | 每个目标主机的最大连接数 |

#### 流式传输配置 (streaming)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | true | 是否启用流式传输模式 |
| chunk_size | int | 64KB | 流式传输块大小（支持 B/KB/MB/GB） |
| large_file_threshold | int | 10MB | 大文件阈值（支持 B/KB/MB/GB） |
| stream_timeout | int | 3600 | 流式传输总超时时间（1 小时） |
| read_timeout | int | 300 | 读取超时时间（5 分钟） |
| write_timeout | int | 300 | 写入超时时间（5 分钟） |
| buffer_size | int | 64KB | 缓冲区大小（支持 B/KB/MB/GB） |
| enable_range_support | bool | true | 支持 Range 请求（断点续传） |
| max_request_body_size | int | 0 | 最大请求体大小（0=无限制，支持 B/KB/MB/GB） |

#### 后台登录配置 (admin_auth)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | false | 是否启用后台登录 |
| username | string | admin | 后台登录账号 |
| password | string | 空 | 后台登录密码 |
| session_ttl_hours | int | 12 | 登录会话有效时长（小时） |
| cookie_name | string | proxy_admin_session | 登录 Cookie 名称 |

#### 代理规则 (proxy_rules)

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| path_prefix | string | 是 | 匹配的URL路径前缀 |
| target_url | string | 是 | 目标服务器基础URL |
| strip_prefix | bool | 否 | 是否在转发时移除路径前缀 |
| timeout | int | 否 | 该规则的超时时间 |
| max_redirects | int | 否 | 该规则的最大重定向次数 |
| follow_redirects | bool | 否 | 是否自动跟随重定向（规则级别） |
| retry_times | int | 否 | 失败重试次数 |
| enable_streaming | bool | 否 | 是否为该规则启用流式传输 |

## 使用示例

### 基本使用

1. 启动代理服务器
```bash
python main.py -c config.yaml
```

2. 发送请求通过代理
```bash
curl http://localhost:8080/api/users
```

### 命令行参数

```bash
python main.py --help

可选参数:
  -c, --config      配置文件路径（默认：config.yaml）
  -p, --port        覆盖配置文件中的端口
  --host            覆盖配置文件中的主机地址
  -v, --verbose     启用详细日志输出
  --no-streaming    禁用流式传输模式
```

### 示例场景

#### 场景1: 代理视频流

```yaml
proxy_rules:
  - path_prefix: "/video"
    target_url: "https://video.example.com"
    enable_streaming: true
    timeout: 120
```

请求 `http://localhost:8080/video/movie.mp4` 将以流式方式代理视频文件。

#### 场景2: 代理音频流

```yaml
proxy_rules:
  - path_prefix: "/audio"
    target_url: "https://audio.example.com"
    enable_streaming: true
    timeout: 120
```

#### 场景3: 代理API服务

```yaml
proxy_rules:
  - path_prefix: "/api/v1"
    target_url: "https://api.github.com"
    enable_streaming: false
```

#### 场景4: 静态资源代理

```yaml
proxy_rules:
  - path_prefix: "/static"
    target_url: "https://cdn.example.com/assets"
    strip_prefix: true
    enable_streaming: true
```

## 流式传输特性

### 支持的媒体类型

代理服务器自动识别以下内容类型并启用流式传输：

- `video/*` - 所有视频格式
- `audio/*` - 所有音频格式
- `application/octet-stream` - 二进制流
- `application/x-mpegurl` - HLS播放列表
- `application/vnd.apple.mpegurl` - Apple HLS
- `application/dash+xml` - DASH流
- `multipart/*` - 分段内容

### 断点续传支持

代理服务器支持HTTP Range请求，允许客户端：

- 分段下载大文件
- 实现断点续传
- 支持视频拖动播放

```bash
# 分段下载示例
curl -H "Range: bytes=0-1023" http://localhost:8080/video/movie.mp4
```

### 内存优化

流式传输模式下：

- 数据分块传输，不一次性加载整个文件
- 默认块大小64KB，可根据需求调整
- 支持并发流式传输，内存占用稳定

## API端点

### 健康检查

```bash
GET /_health

响应:
{
  "status": "healthy"
}
```

### 统计信息

```bash
GET /_stats

响应:
{
  "total_requests": 1000,
  "redirected_requests": 150,
  "failed_requests": 5,
  "total_redirects": 300,
  "streaming_requests": 200,
  "bytes_transferred": 1073741824,
  "uptime_seconds": 3600.5,
  "requests_per_second": 0.28,
  "mb_transferred": 1024.0
}
```

## 重定向处理

代理服务器支持以下HTTP重定向状态码：

| 状态码 | 类型 | 处理方式 |
|--------|------|----------|
| 301 | 永久重定向 | 跟随重定向 |
| 302 | 临时重定向 | 跟随重定向 |
| 303 | See Other | 转换为GET请求后跟随 |
| 307 | 临时重定向 | 保持原方法跟随 |
| 308 | 永久重定向 | 保持原方法跟随 |

### 重定向响应头

当请求涉及重定向时，响应会包含额外的头信息：

- `X-Redirect-Count`: 重定向次数
- `X-Original-URL`: 原始请求URL
- `X-Final-URL`: 最终目标URL

## 运行测试

### 单元测试

#### 安装测试依赖

```bash
pip install -r tests/requirements.txt
```

#### 运行所有测试

```bash
pytest tests/ -v
```

### 运行特定测试文件

```bash
pytest tests/test_proxy.py -v
pytest tests/test_integration.py -v
pytest tests/test_performance.py -v
```

### 测试覆盖范围

- **test_proxy.py**: 核心代理逻辑单元测试
  - 配置解析测试
  - 重定向处理测试
  - 流式传输测试
  - 请求头过滤测试
  - URL构建测试

- **test_integration.py**: 集成测试
  - 端到端代理请求测试
  - 健康检查和统计端点测试
  - 各种重定向场景测试
  - 流式传输集成测试

- **test_performance.py**: 性能测试
  - 并发请求测试
  - 统计模块线程安全测试
  - 高负载场景测试
  - 流式传输性能测试
  - 内存效率测试

## 性能优化建议

1. **流式传输配置**: 对于大文件代理，确保启用流式传输模式
2. **块大小调整**: 根据网络条件调整`chunk_size`（默认64KB）
3. **连接池配置**: 根据实际负载调整 `max_connections`
4. **超时设置**: 为不同类型的请求配置合适的超时时间
5. **日志级别**: 生产环境建议使用 INFO 或 WARNING 级别
6. **SSL优化**: 启用SSL会增加CPU开销，请根据需求配置

## 安全注意事项

1. **信任转发头**: `trust_forward_headers` 会添加 `X-Forwarded-*` 头，确保在可信网络中使用
2. **SSL证书**: 生产环境建议使用有效的SSL证书
3. **访问控制**: 建议在代理前添加认证层或网络访问控制
4. **请求体大小**: 可通过 `max_request_body_size` 限制请求体大小

## 故障排除

### 常见问题

1. **连接超时**
   - 检查目标服务器是否可达
   - 增加超时时间配置

2. **重定向循环**
   - 检查 `max_redirects` 配置
   - 查看日志中的重定向链

3. **流式传输中断**
   - 检查 `stream_timeout` 配置
   - 查看网络稳定性

4. **内存占用过高**
   - 确认流式传输已启用
   - 减小 `chunk_size` 配置

### 日志调试

启用详细日志：
```bash
python main.py -v
```

查看日志文件：
```bash
tail -f ./log/nginx302_proxy.log
```

### 监控命令

```bash
# 查看系统统计
curl http://localhost:8080/_stats | jq

# 健康检查
curl http://localhost:8080/_health
```

## 性能基准

在标准配置下（i7 CPU, 16GB RAM, SSD）：

- 并发连接数：1000+
- 流式传输延迟：<100ms
- 内存占用：~200MB（基础）

## 许可证

MIT 许可证

## 贡献

欢迎提交 Issue 和 Pull Request。

## 更新日志

### v4.0.0
- **移除本地缓存模块**：移除流式缓存、LRU淘汰、预加载等功能
- 简化架构，减少依赖和配置复杂度
- 保留在线IP定位缓存（内存级，非文件缓存）
- 保留核心代理功能：302重定向、流式传输、Range请求

### v3.0.0
- 重新设计流式缓存机制
- 新增边传输边缓存功能，支持流式媒体实时缓存
- 完整支持 HTTP Range 请求缓存
- 新增详细的缓存操作日志追踪

### v2.0.0
- 新增缓存系统（LRU淘汰、预加载、断点续传）
- 新增缓存管理API
- 优化日志输出为中文
- 改进错误处理机制

### v1.0.0
- 初始版本
- 支持302重定向处理
- 支持流式传输
- 支持Range请求
