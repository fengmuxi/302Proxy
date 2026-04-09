# HTTP Reverse Proxy with 302 Redirect Handler

一个高性能的Python反向代理服务器，能够自动处理并转发302重定向的外网链接地址流量，支持大媒体文件流式传输和智能缓存。

## 功能特性

- **自动处理302重定向**: 透明地跟随所有类型的HTTP重定向（301, 302, 303, 307, 308）
- **高性能异步架构**: 基于aiohttp构建，支持高并发请求处理
- **大媒体文件流式传输**: 支持视频、音频等大文件的流式代理，内存占用低
- **智能缓存系统**: LRU缓存淘汰、预加载、断点续传、缓存校验
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
├── proxy_core.py                # 代理核心逻辑
├── cache_manager.py             # 基础缓存管理模块
├── streaming_cache_manager.py   # 流式缓存管理模块（新增）
├── test_cache.py                # 缓存测试脚本（新增）
├── config.yaml                  # 配置文件示例
├── requirements.txt             # 项目依赖
└── tests/                       # 单元测试
    ├── test_proxy.py
    ├── test_integration.py
    ├── test_performance.py
    └── requirements.txt
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
编辑 `config.yaml` 文件，配置您的代理规则。

5. 启动服务
```bash
python main.py
```

## 配置说明

### 配置文件结构 (config.yaml)

```yaml
server:
  host: "0.0.0.0"          # 监听地址
  port: 8080               # 监听端口
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

cache:
  enabled: true                    # 是否启用缓存
  cache_dir: "./cache"             # 缓存目录
  max_size: 10GB                   # 最大缓存大小（支持 B/KB/MB/GB）
  max_entries: 1000                # 最大缓存条目数
  default_ttl: 86400               # 默认 TTL（秒）
  chunk_size: 64KB                 # 下载块大小（支持 B/KB/MB/GB）
  enable_preload: true             # 启用预加载
  preload_concurrency: 3           # 预加载并发数
  enable_validation: true          # 启用缓存校验
  validation_interval: 3600        # 校验间隔（秒）

proxy_rules:
  - path_prefix: "/api"           # 路径前缀
    target_url: "https://api.example.com"  # 目标服务器URL
    strip_prefix: false           # 是否移除路径前缀
    timeout: 30                   # 请求超时（秒）
    max_redirects: 10             # 最大重定向次数
    retry_times: 3                # 重试次数
    enable_streaming: true        # 是否启用流式传输

default_timeout: 30        # 默认超时时间
max_redirects: 10          # 默认最大重定向次数
follow_redirects: true     # 是否跟随重定向
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
| port | int | 8080 | 服务器监听端口 |
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

#### 缓存配置 (cache)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| enabled | bool | false | 是否启用缓存 |
| cache_dir | string | "./cache" | 缓存文件存储目录 |
| max_size | int | 10GB | 最大缓存空间（支持 B/KB/MB/GB） |
| max_entries | int | 1000 | 最大缓存条目数 |
| default_ttl | int | 86400 | 缓存默认有效期（24 小时） |
| chunk_size | int | 64KB | 下载块大小（支持 B/KB/MB/GB） |
| enable_preload | bool | true | 是否启用预加载功能 |
| preload_concurrency | int | 3 | 预加载并发数 |
| enable_validation | bool | true | 是否启用缓存校验 |
| validation_interval | int | 3600 | 缓存校验间隔（1 小时） |

#### 代理规则 (proxy_rules)

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| path_prefix | string | 是 | 匹配的URL路径前缀 |
| target_url | string | 是 | 目标服务器基础URL |
| strip_prefix | bool | 否 | 是否在转发时移除路径前缀 |
| timeout | int | 否 | 该规则的超时时间 |
| max_redirects | int | 否 | 该规则的最大重定向次数 |
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
  --no-cache        禁用缓存
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

#### 场景5: 带缓存的媒体代理

```yaml
cache:
  enabled: true
  max_size: 10737418240  # 10GB
  default_ttl: 86400     # 24小时

proxy_rules:
  - path_prefix: "/media"
    target_url: "https://media.example.com"
    enable_streaming: true
```

首次请求会下载并缓存，后续请求直接从缓存提供。

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

## 缓存系统

### 流式缓存机制

本项目采用全新的流式缓存机制，专门针对大媒体文件传输场景进行了优化：

#### 核心特性

1. **边传输边缓存**: 在流式传输过程中实时缓存数据，无需等待完整下载
2. **Range 请求支持**: 完整支持 HTTP Range 请求，可从缓存中读取任意范围的数据
3. **智能缓存判断**: 自动识别媒体类型，仅缓存适合的内容
4. **内存高效**: 使用流式读写，避免一次性加载大文件到内存

#### 缓存流程

```
客户端请求 → 检查缓存 → 缓存命中?
                ↓ 是              ↓ 否
           从缓存读取      从源站流式传输
                ↓              ↓
           返回数据      边传输边缓存
                              ↓
                         返回数据
```

#### 缓存状态标识

响应头中包含 `X-Cache-Status` 标识：

- `HIT`: 缓存命中，数据来自缓存
- `MISS`: 缓存未命中，数据已缓存并返回
- `BYPASS`: 跳过缓存，数据不缓存

#### 详细日志追踪

缓存系统在关键节点提供详细日志：

```
[缓存查询] 时间: 2026-03-18 10:30:45 | 操作: 查询 | 键: abc123... | URL: http://... | 结果: 未命中
[流式缓存] 时间: 2026-03-18 10:30:45 | 操作: 开始缓存 | 键: abc123... | URL: http://... | 预期大小: 100.50 MB
[流式缓存] 时间: 2026-03-18 10:31:20 | 操作: 缓存完成 | 键: abc123... | 大小: 100.50 MB | 结果: 成功
[缓存命中] 时间: 2026-03-18 10:32:00 | 操作: 流式读取 | 键: abc123... | 范围: 0-1048575 | 结果: 成功
```

### 核心功能

#### 1. LRU缓存淘汰

- 自动淘汰最近最少使用的缓存条目
- 支持按大小和数量双重限制
- 保证热点数据保留在缓存中

#### 2. 预加载机制

```bash
# 预加载URL列表
curl -X POST http://localhost:8080/_cache/preload \
  -H "Content-Type: application/json" \
  -d '{
    "urls": [
      "http://example.com/video1.mp4",
      "http://example.com/video2.mp4"
    ],
    "priority": 1
  }'
```

#### 3. 断点续传

- 下载中断后自动从断点继续
- 支持临时文件管理
- 网络异常自动重试

#### 4. 缓存校验

- 使用ETag/Last-Modified验证缓存有效性
- 定期校验缓存内容
- 自动更新过期缓存

### 缓存管理API

#### 查看缓存统计

```bash
curl http://localhost:8080/_cache/stats
```

响应示例：
```json
{
  "total_requests": 1000,
  "cache_hits": 800,
  "cache_misses": 200,
  "hit_rate": "80.00%",
  "bytes_served_from_cache": 8589934592,
  "mb_served_from_cache": 8192.00,
  "bytes_downloaded": 2147483648,
  "mb_downloaded": 2048.00,
  "evicted_entries": 10,
  "preload_requests": 50,
  "preload_hits": 45
}
```

#### 查看缓存详情

```bash
curl http://localhost:8080/_cache/info
```

#### 清理所有缓存

```bash
curl -X POST http://localhost:8080/_cache/clear
```

#### 删除单个缓存条目

```bash
curl -X DELETE http://localhost:8080/_cache/entry \
  -H "Content-Type: application/json" \
  -d '{"url": "http://example.com/video.mp4"}'
```

### 缓存命中率优化

1. **合理设置TTL**: 根据内容更新频率调整
2. **预加载热门内容**: 提前缓存用户可能访问的内容
3. **监控淘汰率**: 根据淘汰情况调整缓存大小
4. **启用缓存校验**: 确保缓存内容有效性

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

### 缓存统计

```bash
GET /_cache/stats
```

### 缓存详情

```bash
GET /_cache/info
```

### 清理缓存

```bash
POST /_cache/clear
```

### 预加载

```bash
POST /_cache/preload
```

### 删除缓存条目

```bash
DELETE /_cache/entry
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
- `X-Cache-Status`: 缓存状态（HIT/MISS）

## 运行测试

### 流式缓存测试

运行流式缓存机制测试脚本：

```bash
python test_cache.py
```

测试内容包括：
1. **基本缓存功能测试**: 验证缓存命中和未命中场景
2. **Range 请求缓存测试**: 验证 Range 请求的缓存支持
3. **缓存统计 API 测试**: 验证缓存统计接口
4. **缓存信息 API 测试**: 验证缓存详情接口
5. **多个 Range 请求测试**: 验证不同范围的并发请求
6. **缓存清理测试**: 验证缓存清理功能

测试报告将保存到 `cache_test_report.json` 文件。

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
2. **缓存配置**: 启用缓存并设置合理大小，提高命中率
3. **块大小调整**: 根据网络条件调整`chunk_size`（默认64KB）
4. **连接池配置**: 根据实际负载调整 `max_connections`
5. **超时设置**: 为不同类型的请求配置合适的超时时间
6. **日志级别**: 生产环境建议使用 INFO 或 WARNING 级别
7. **SSL优化**: 启用SSL会增加CPU开销，请根据需求配置
8. **预加载策略**: 根据用户行为预测预加载内容

## 安全注意事项

1. **信任转发头**: `trust_forward_headers` 会添加 `X-Forwarded-*` 头，确保在可信网络中使用
2. **SSL证书**: 生产环境建议使用有效的SSL证书
3. **访问控制**: 建议在代理前添加认证层或网络访问控制
4. **请求体大小**: 可通过 `max_request_body_size` 限制请求体大小
5. **缓存安全**: 定期清理缓存，防止缓存污染
6. **资源限制**: 设置合理的缓存大小限制，防止磁盘占满

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

5. **缓存命中率低**
   - 增加缓存大小
   - 调整TTL设置
   - 启用预加载功能

6. **磁盘空间不足**
   - 清理缓存目录
   - 减小 `max_size` 配置
   - 定期执行缓存清理

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
# 查看缓存状态
curl http://localhost:8080/_cache/stats | jq

# 查看系统统计
curl http://localhost:8080/_stats | jq

# 健康检查
curl http://localhost:8080/_health
```

## 性能基准

在标准配置下（i7 CPU, 16GB RAM, SSD）：

- 并发连接数：1000+
- 缓存命中率：80-95%（取决于配置）
- 流式传输延迟：<100ms
- 内存占用：~200MB（基础）+ 缓存占用

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request。

## 更新日志

### v3.0.0
- **重大更新**: 重新设计流式缓存机制
- 新增边传输边缓存功能，支持流式媒体实时缓存
- 完整支持 HTTP Range 请求缓存
- 新增详细的缓存操作日志追踪
- 新增缓存统计指标：Range 请求数、流式缓存写入数
- 优化缓存键生成策略
- 新增流式缓存测试套件
- 改进缓存与流式传输的集成

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
