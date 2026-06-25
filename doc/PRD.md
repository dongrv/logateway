# logateway 产品需求文档（PRD）V3.0

---

## 文档信息

| 属性 | 内容 |
|------|------|
| 版本 | V3.0 |
| 日期 | 2026-06-24 |
| 状态 | 已实现 |
| 语言 | Go 1.21+ |

---

## 1. 产品概述

### 1.1 产品定义

logateway 是一个**高可用、高并发、配置驱动**的 HTTP 消息网关，作为所有日志类数据上报的统一入口。客户端通过 HTTP POST 提交消息，网关完成鉴权、校验、限流后异步投递至下游消息中间件（Redis / Kafka 等）。

### 1.2 核心价值

- **统一接入**：替代分散的 PHP 接入层，一个网关服务所有项目的日志上报
- **配置驱动**：通过 YAML 定义项目、下游、处理器链，热加载无需重启
- **数据不丢**：Channel + WAL 磁盘 + 自动重放 + 熔断恢复，四层保障
- **可观测**：Prometheus 指标、结构化日志、健康检查全覆盖

---

## 2. 系统架构

### 2.1 请求链路

```
客户端 POST /api/v1/log/upload
  │
  ▼
┌─ HTTP Server (Gin) ────────────────────────────────┐
│ ❶ RequestID 注入                                    │
│ ❷ Metrics 记录                                      │
│ ❸ 全局 QPS 限流                                     │
└────────────────────┬────────────────────────────────┘
                     ▼
┌─ Auth Middleware ──────────────────────────────────┐
│ ❹ AppKey + HMAC-SHA256 签名验证                     │
│ ❺ Timestamp 时间窗口校验 (±300s)                     │
│ ❻ Nonce 防重放（内存 LRU 缓存）                      │
└────────────────────┬────────────────────────────────┘
                     ▼
┌─ Project Resolution ───────────────────────────────┐
│ ❼ 解析 JSON 提取 project 字段                        │
│ ❽ 查找项目配置（鉴权、限流、Sink 列表）                │
└────────────────────┬────────────────────────────────┘
                     ▼
┌─ Rate Limit (项目级) ──────────────────────────────┐
│ ❾ 令牌桶限流                                        │
└────────────────────┬────────────────────────────────┘
                     ▼
┌─ Upload Handler ───────────────────────────────────┐
│ ❿ Body 大小校验 → JSON 解析 → 构建 Message          │
│ ⓫ 提交到 ants 协程池                                 │
│ ⓬ 返回 200（同步快速返回）                            │
└────────────────────┬────────────────────────────────┘
                     ▼ (ants goroutine)
┌─ Dispatcher ───────────────────────────────────────┐
│ ⓭ Pipeline 处理器链执行（过滤/脱敏/附加字段）          │
│ ⓮ 投递到各 Sink WorkerPool                          │
└────────────────────┬────────────────────────────────┘
                     ▼
┌─ WorkerPool (per Sink) ────────────────────────────┐
│ ⓯ Channel 缓冲（有界队列）                            │
│ ⓰ Worker goroutine × N                             │
│ ⓱ Retry × 3（指数退避: 100ms/200ms/400ms）          │
│ ⓲ Circuit Breaker（连续 10 次失败 → 熔断打开）       │
│ ⓳ WAL 磁盘兜底（channel 满 或 熔断时）                │
└────────────────────┬────────────────────────────────┘
                     ▼
┌─ Sink ────────────────────────────────────────────┐
│ Redis: LPUSH / XADD                                │
│ Kafka: WriteMessages (Hash 分区, 压缩可选)          │
│ 自定义: 实现 Sink 接口                                │
└────────────────────────────────────────────────────┘
```

### 2.2 后台自治机制

```
┌─ WAL Auto-Replay ──────────────────────────────────┐
│ 每 5s 扫描已封存 segment                             │
│   → 逐条 Dispatch 重新投递                            │
│   → 成功则删除 segment 文件                           │
│   → 跳过 active segment（正在写入）                    │
└────────────────────────────────────────────────────┘

┌─ Circuit Recovery ─────────────────────────────────┐
│ 熔断打开后每 15s 探测下游 HealthCheck                  │
│   → 恢复则关闭熔断，正常投递                           │
│   → 配合 WAL 重放，堆积消息自动回放                     │
└────────────────────────────────────────────────────┘
```

---

## 3. 数据模型

### 3.1 客户端请求

```json
{
  "project": "actilogs",
  "router": "CH=Behavior&Opt=AddLogs",
  "data": { "UID": 123, "action": "click" }
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `project` | string | 是 | 项目标识，对应配置文件中的 `projects[].name` |
| `router` | string | 否 | 路由信息，透传给消费端 |
| `data` | object | 否 | 业务数据，经过 pipeline 处理后落库 |

### 3.2 服务端响应

```json
{
  "code": 0,
  "message": "success",
  "request_id": "b3f1a2c4-...",
  "trace_id": "b3f1a2c4-..."
}
```

### 3.3 内部消息体（Message）

| 字段 | 类型 | 来源 |
|------|------|------|
| `RequestID` | string | 客户端请求头 `X-Request-Id` 或服务端生成 UUID |
| `TraceID` | string | 客户端请求头 `X-Trace-Id` 或等于 RequestID |
| `Project` | string | 请求体 `project` |
| `Router` | string | 请求体 `router` |
| `Data` | json.RawMessage | 请求体 `data`，可能经 pipeline 修改 |
| `Env` | string | 配置文件 `server.env` |
| `Timestamp` | time.Time | 服务端接收时间 |

Message 使用 `sync.Pool` 对象池复用，降低 GC 压力。

### 3.4 投递信封（Envelope）

写入 Redis/Kafka 的最终格式：

```json
{
  "_gateway_meta": {
    "request_id": "b3f1a2c4-...",
    "trace_id": "b3f1a2c4-...",
    "received_at": "2026-06-24T10:00:00Z",
    "env": "production"
  },
  "project": "actilogs",
  "router": "CH=Behavior",
  "data": { "UID": 123, "action": "click" }
}
```

### 3.5 WAL 磁盘条目（Entry）

```json
{
  "seq": 0,
  "project": "actilogs",
  "router": "Test",
  "data": { "k": "v" },
  "request_id": "...",
  "trace_id": "...",
  "timestamp": "2026-06-24T10:00:00Z",
  "env": "production"
}
```

---

## 4. 鉴权机制

### 4.1 请求头

| Header | 必填 | 说明 |
|--------|------|------|
| `X-App-Key` | 是 | 应用标识 |
| `X-Timestamp` | 是 | Unix 秒时间戳 |
| `X-Nonce` | 是 | 随机字符串（16-64 字符） |
| `X-Signature` | 是 | HMAC-SHA256 签名（Hex 编码，64 字符） |
| `X-Request-Id` | 否 | 请求追踪 ID |
| `X-Trace-Id` | 否 | 全链路追踪 ID |

### 4.2 签名算法

```
Signature = Hex(HMAC-SHA256(Secret, Body + Timestamp + Nonce))
```

- `Secret`：与 AppKey 对应的预共享密钥
- `Body`：原始请求体 JSON 字符串（原始字节）
- `Timestamp`：`X-Timestamp` 头部的值（字符串拼接）
- `Nonce`：`X-Nonce` 头部的值
- 拼接顺序：`Body + Timestamp + Nonce`，无分隔符

### 4.3 验证流程

```
❶ 检查 X-App-Key 是否存在 → 查询对应 Secret
❷ 验证 |当前时间 - X-Timestamp| ≤ 300s
❸ 检查 X-Nonce 是否已在缓存中（防重放）
❹ 按相同算法重算签名 → 对比 X-Signature
❺ 全部通过 → 缓存 Nonce（TTL 5分钟，LRU 淘汰）
```

### 4.4 Nonce 缓存

- 存储：内存 `map[string]time.Time`
- 容量上限：`nonce_cache_size`（默认 100,000）
- 淘汰策略：超出上限时随机淘汰一条（简单 LRU）
- 过期机制：5 分钟窗口外的请求已被步骤 ❷ 拒绝

---

## 5. 限流机制

### 5.1 双层限流

| 层级 | 算法 | 作用范围 | 关系 |
|------|------|----------|------|
| 全局限流 | 令牌桶 `golang.org/x/time/rate` | 整个网关 | AND |
| 项目限流 | 令牌桶（按项目独立） | 单个 project | AND |

请求必须**同时通过**两层限流，实际上限为两者较小值。

### 5.2 参数

```
Limiter = rate.NewLimiter(rate.Limit(QPS), QPS × 2)
```

- `rate`：每秒生成令牌数 = QPS
- `burst`：突发容量 = 2 × QPS

---

## 6. Sink（投递后端）

### 6.1 Sink 接口

```
type Sink interface {
    Send(ctx context.Context, msg *Message) error
    Name() string
    HealthCheck() error
    Close() error
}
```

编译期注册工厂：

```
reg.Register("redis", RedisSinkFactory)
reg.Register("kafka", KafkaSinkFactory)
```

### 6.2 Redis Sink

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `addr` | `127.0.0.1:6379` | 地址 |
| `password` | — | 密码 |
| `db` | 0 | 数据库编号 |
| `pool_size` | 100 | 连接池大小 |
| `min_idle_conns` | 10 | 最小空闲连接 |
| `dial_timeout` | 5s | 连接超时 |
| `read_timeout` | 3s | 读超时 |
| `write_timeout` | 3s | 写超时 |
| `key` | — | 队列 Key |
| `type` | list | list（LPUSH）/ stream（XADD） |
| `max_len` | 0 | Stream 最大长度 |

### 6.3 Kafka Sink

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `brokers` | — | Broker 列表 |
| `topic` | — | 主题 |
| `partition_key` | — | 分区键字段名（从 data 中提取） |
| `compression` | — | snappy / gzip / lz4 / zstd |
| `batch_size` | 100 | 批量大小 |
| `batch_timeout` | 100ms | 批量间隔 |

分区策略：优先 `partition_key` 字段 → 备选 `UID`/`uid`/`user_id` → 兜底 `RequestID`。

---

## 7. WorkerPool（异步投递引擎）

### 7.1 核心参数

```
┌─ Submit(msg) ──────────────────────────────────────┐
│  try write wp.ch ──→ success (return nil)           │
│  channel full:                                       │
│    drop:     msg 丢弃 + return error                  │
│    block:    阻塞 submitTimeout(100ms) → 超时丢弃     │
│    fallback: 写 WAL 磁盘 + return nil（异步兜底）     │
└─────────────────────────────────────────────────────┘
         │ (msg in channel)
         ▼
┌─ worker goroutine × N ─────────────────────────────┐
│  for msg := range wp.ch:                             │
│    if circuit_open → handleRejected (WAL/drop)       │
│    retry × 3:                                        │
│      attempt 0: Send(msg)                            │
│      attempt 1: backoff 100ms → Send                 │
│      attempt 2: backoff 200ms → Send                 │
│    all failed → recordFailure → handleRejected       │
│    success     → resetFailures → ReleaseMessage      │
└─────────────────────────────────────────────────────┘
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `workers` | 16 | Worker goroutine 数量 |
| `channel_size` | 16384 | Channel 缓冲容量 |
| `max_fails` | 10 | 连续失败触发熔断的阈值 |
| `submit_timeout` | 100ms | block 模式最大阻塞时间 |

### 7.2 熔断自动恢复

```
熔断打开 ──→ 启动后台探测 goroutine
              │
              每 15s: sink.HealthCheck()
              │
              ├── 失败 → 继续等待
              │
              └── 成功 → resetFailures() 关闭熔断
                         → 探测 goroutine 退出
                         → 熔断可再次触发（recoveryOnce 重置）
```

- 探测用 `HealthCheck()`，不消耗真实消息
- Shutdown 时关闭探测 goroutine

---

## 8. WAL（磁盘 Write-Ahead Log）

### 8.1 设计目标

当 Channel 满（fallback 模式）或熔断打开时，消息写入磁盘 segment 文件确保不丢失。

### 8.2 段文件管理

```
data/wal/
  wal-000001.log    ← 已封存（等待重放）
  wal-000002.log    ← active（正在写入）
```

- 单段上限：`max_segment_bytes`（默认 64MB）
- 段数上限：`max_segments`（默认 10），超出自动清除最旧段
- 写入模式：追加（`O_APPEND`），每行一条 JSON
- 同步策略：`sync_interval > 0` 时定期 fsync；= 0 时每条写入后立即 fsync

### 8.3 自动重放

```
每 5s:
  扫描 data/wal/*.log（排除 active segment）
  按序号升序:
    逐条读取 Entry → fn(entry) 回调重新投递
    整段成功 → 删除 segment 文件
    任一条失败 → 停止，保留 segment，下周期重试
```

- 重放回调内部走 `Dispatcher.Dispatch`，与正常请求走相同路径
- 若重放时 channel 满了，fallback 会写回 WAL 的 active segment（不会死循环——active 不会被重放扫描到）

### 8.4 启动恢复

网关启动时，`replayWAL` 一次性读取所有 segment 文件，逐条投递并删除。

---

## 9. Pipeline（处理器链）

### 9.1 接口

```
type Processor interface {
    Process(msg *Message) (*Message, error)
    Name() string
}
```

处理器按配置顺序链式执行。任一步失败则中止，消息丢弃。

### 9.2 内置处理器

| 处理器 | 类型名 | 配置 | 说明 |
|--------|--------|------|------|
| FieldFilter | `field_filter` | `mode: include/exclude`, `fields: [...]` | 字段白名单/黑名单过滤 |
| FieldAdd | `field_add` | `fields: {key: value, ...}` | 附加固定字段到 data |
| FieldRedact | `field_redact` | `fields: {key: mask, ...}` | 敏感字段脱敏替换 |

---

## 10. 配置体系

### 10.1 配置源

- 本地 YAML 文件（`configs/gateway.yaml`）
- 通过 `fsnotify` 监听文件变化，500ms 防抖后热加载
- `POST /admin/config/reload` 手动触发热加载
- 配置存储：`atomic.Value` 整体替换，读无锁，新旧隔离

### 10.2 三层 Sink 配置合并

项目 Sink 配置按以下优先级合并（浅合并，后覆盖前）：

```
Project Sink Config  (最高)
  ↓ 合并
Sink Instance Config (sink_instances 命名实例)
  ↓ 合并
Global Sink Defaults  (sinks.redis / sinks.kafka)
```

### 10.3 完整配置结构

```yaml
server:
  listen_addr: ":8080"        # 监听地址
  read_timeout: 3s            # 请求读取超时
  write_timeout: 5s           # 响应写入超时
  idle_timeout: 120s          # 空闲连接超时
  max_body_bytes: 1048576     # 全局最大 Body（字节）
  global_rate_limit: 20000    # 全局 QPS 上限
  ants_pool_size: 10000       # HTTP 处理协程池大小
  backpressure: fallback      # 反压策略: drop / block / fallback
  env: "production"           # 环境标识，注入 _gateway_meta.env

auth:
  enabled: true               # 是否启用鉴权
  timestamp_window: 300       # 时间戳允许偏差（秒）
  nonce_ttl_seconds: 300      # Nonce 缓存 TTL（秒）
  nonce_cache_size: 100000    # Nonce 最大缓存数

sinks:                        # 全局 Sink 默认值
  redis:
    addr: "127.0.0.1:6379"
    password: ""
    db: 0
    pool_size: 100
    min_idle_conns: 10
    dial_timeout: 5s
    read_timeout: 3s
    write_timeout: 3s
  kafka:
    brokers: ["kafka1:9092"]
    batch_size: 100
    batch_timeout: 100ms

sink_instances:               # 命名实例（多项目共享连接）
  redis_hp:
    type: redis
    config:
      addr: "redis-prod:6379"
      pool_size: 200

projects:                     # 项目列表
  - name: actilogs            # 项目标识
    enabled: true
    sinks:                    # Sink 列表（多路投递）
      - type: redis           # 直接定义
        workers: 32           # Worker 数
        channel_size: 65536   # Channel 容量
        config:
          key: "queue:actilogs"
          type: list
      - instance: redis_hp    # 引用命名实例
        config:
          key: "queue:backup" # 覆写 key
    rate_limit: 5000          # 项目级 QPS
    max_body_bytes: 524288    # 项目级 Body 限制
    auth_required: true       # 是否要求鉴权
    pipelines:                # 处理器链
      - type: field_filter
        config:
          mode: exclude
          fields: ["internal_token"]
      - type: field_add
        config:
          fields:
            env: "production"

wal:
  enabled: true               # 是否启用 WAL
  dir: "data/wal"             # 段文件目录
  max_segment_bytes: 67108864 # 单段最大字节（64MB）
  max_segments: 10            # 保留段数
  sync_interval: 100ms        # fsync 间隔（0=每条 sync）

pipeline:
  max_depth: 10               # JSON 嵌套深度限制

log:
  level: "info"               # 日志级别: debug / info / warn / error
  console:
    enabled: true
    format: "text"            # text / json
  file:
    enabled: true
    dir: "logs"               # 文件目录（自动创建 error/ warn/ 子目录）
    levels: ["error", "warn"] # 文件记录的级别
    max_age: 7                # 保留天数

metrics:
  enabled: true
  path: "/metrics"            # Prometheus 指标路径

pprof:
  enabled: true               # 是否启用 pprof（生产建议开启）
  path: "/debug/pprof"        # pprof 路径
```

---

## 11. API 接口

### 11.1 消息上报

```
POST /api/v1/log/upload
Content-Type: application/json
```

| 状态码 | code | 说明 |
|--------|------|------|
| 200 | 0 | 成功 |
| 400 | 400 | JSON 解析失败 / project 为空 |
| 401 | 401 | 签名错误 / Nonce 重放 / 时间戳超窗口 |
| 404 | 404 | 未知 project |
| 413 | 413 | Body 超过大小限制 |
| 429 | 429 | 触发限流（全局或项目级） |
| 503 | 503 | 协程池满，服务繁忙 |

### 11.2 管理接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查（返回 `{"status":"ok"}`） |
| GET | `/ready` | 就绪探针（返回 `{"ready":true}`） |
| GET | `/metrics` | Prometheus 指标 |
| POST | `/admin/config/reload` | 手动触发热重载 |
| GET | `/admin/pools` | Channel 使用率（`{"project": 0.45, ...}`） |
| GET | `/debug/pprof/` | Go pprof 性能分析（可配置路径，需 `pprof.enabled: true`） |

---

## 12. Prometheus 指标

| 指标 | 类型 | 标签 | 说明 |
|------|------|------|------|
| `gateway_http_requests_total` | Counter | project, method, status | HTTP 请求计数 |
| `gateway_http_request_duration_seconds` | Histogram | project, method, status | 请求延迟 |
| `gateway_sink_deliveries_total` | Counter | sink, status | Sink 投递计数 |
| `gateway_sink_retries_total` | Counter | sink | 重试计数 |
| `gateway_circuit_state` | Gauge | sink | 熔断状态（0=关, 1=开） |
| `gateway_channel_usage_ratio` | Gauge | sink | Channel 使用率（0-1） |
| `gateway_ratelimit_rejects_total` | Counter | level, project | 限流拒绝计数 |
| `gateway_pool_goroutines` | Gauge | — | ants 池活跃协程数 |
| `gateway_pool_capacity` | Gauge | — | ants 池总容量 |

---

## 13. 日志系统

### 13.1 日志分级

| 级别 | 输出 | 说明 |
|------|------|------|
| ERROR | 控制台 + `logs/error/YYYY-MM-DD.log` | 服务异常、投递失败、panic 恢复 |
| WARN | 控制台 + `logs/warn/YYYY-MM-DD.log` | 降级行为、配置警告、重试 |
| INFO | 控制台 | 生命周期事件（启动/关闭/重载/恢复） |
| DEBUG | 控制台 | 详细诊断信息（需 `level: debug`） |

### 13.2 日志格式

使用 Go 标准 `log` 包，格式：`[LEVEL] message key=value ...`

```
[INFO] gateway starting on :8080 (bp=fallback wal=true)
[WARN] sink blogs-redis-0 message rejected: reason=channel_full request_id=... bp=2
[ERROR] circuit breaker opened for sink blogs-redis-0
[INFO] circuit breaker auto-recovered for sink blogs-redis-0
[INFO] WAL replay segment data/wal/wal-000003.log: 1523 entries replayed, deleting
```

### 13.3 日切文件

- 每天一个文件，按 UTC 日期命名
- 懒创建：有日志写入时才创建目录和文件
- `[ERROR]` 和 `[WARN]` 前缀匹配写入对应文件
- 控制台输出所有级别（不过滤）

---

## 14. 部署

### 14.1 构建

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o gateway ./cmd/gateway/
```

### 14.2 Dockerfile

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o gateway ./cmd/gateway/

FROM alpine:3.20
COPY --from=builder /build/gateway /app/
COPY configs/ /app/configs/
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["./gateway"]
```

### 14.3 Kubernetes 关键配置

```yaml
containers:
  - name: gateway
    env:
      - name: GOMAXPROCS
        valueFrom:
          resourceFieldRef:
            resource: limits.cpu
      - name: GOMEMLIMIT
        value: "1700MiB"
    terminationGracePeriodSeconds: 35
    livenessProbe:
      httpGet: { path: /health, port: 8080 }
    readinessProbe:
      httpGet: { path: /ready, port: 8080 }
```

### 14.4 优雅关闭

```
SIGTERM →
  ❶ 关闭 HTTP Server（停止接收新请求）
  ❷ dispatcher.Shutdown():
     - 标记 WorkerPool.closed = true
     - cancel context（唤醒阻塞在 retry backoff 的 worker）
     - close(wp.ch)（worker 清空剩余消息后退出）
     - 等待 worker goroutine 全部退出（最长 timeout）
  ❸ WAL.Close()（刷盘 + 关闭文件）
  ❹ pool.Release()（ants 协程池释放）
  ❺ logging.Close()（日志 writer 刷盘）
  ❻ config.Close()（停止 fsnotify watcher）
```

---

## 15. 项目结构

```
cmd/gateway/main.go              # 入口（仅 14 行）
internal/
  server/                         # HTTP 路由、中间件、Handler
    server.go                     # Gateway 生命周期
    middleware.go                 # RequestID / Logging / ProjectResolution
  auth/                           # HMAC-SHA256 鉴权中间件
    auth.go
  config/                         # YAML 配置加载、热更新
    config.go                     # Manager + fsnotify + atomic.Value
  message/                        # Message / Envelope / sync.Pool
    message.go
  metrics/                        # Prometheus 指标定义（leaf package）
    metrics.go
  observability/                  # 健康检查、MetricsMiddleware
    observability.go
  pipeline/                       # 处理器链接口 + 内置处理器
    pipeline.go                   # Chain
    processors.go                 # FieldFilter / FieldAdd / FieldRedact
  project/                        # 项目路由、三层 Sink 配置合并
    dispatcher.go                 # Dispatcher
  ratelimit/                      # 全局 + 项目级限流
    ratelimit.go
  sink/                           # Sink 接口 + 实现
    sink.go                       # Sink interface, Registry
    worker.go                     # WorkerPool, Submit, 熔断, 恢复
    redis_sink.go                 # Redis Sink (LPUSH / XADD)
    kafka_sink.go                 # Kafka Sink
  wal/                            # 磁盘 WAL
    wal.go                        # Writer, ReadAll, StartReplay
  logging/                        # 日切日志
    logging.go                    # Setup, levelFilter
    filewriter.go                 # dailyWriter
configs/gateway.yaml              # 默认配置
examples/                         # 场景示例
```

---

## 16. 技术选型

| 组件 | 库 | 说明 |
|------|-----|------|
| HTTP 路由 | `gin-gonic/gin` | 高性能、成熟中间件生态 |
| 协程池 | `panjf2000/ants/v2` | 控制并发 goroutine 数量 |
| 限流 | `golang.org/x/time/rate` | 令牌桶，Go 官方扩展库 |
| 配置监听 | `fsnotify/fsnotify` | 文件变化通知 |
| YAML 解析 | `gopkg.in/yaml.v3` | 标准 YAML |
| Redis 客户端 | `redis/go-redis/v9` | 连接池、集群/哨兵 |
| Kafka 客户端 | `segmentio/kafka-go` | 纯 Go，零依赖 |
| Prometheus | `prometheus/client_golang` | 指标暴露 |
| UUID | `google/uuid` | RequestID 生成 |
| 配置管理 | `atomic.Value`（标准库） | 无锁并发读 |
| 性能分析 | `net/http/pprof`（标准库） | CPU/内存/goroutine 分析 |

---

## 17. 数据可靠性总结

| 保障层 | 机制 | 触发条件 |
|--------|------|----------|
| ❶ 正常投递 | 16 Worker × Retry × 3 | 下游正常 |
| ❷ Channel 溢出 | WAL 磁盘兜底 | Channel 满 + fallback |
| ❸ 下游宕机 | 熔断 + WAL | 连续 10 次失败 |
| ❹ 熔断恢复 | 15s HealthCheck 探测 | 下游恢复 |
| ❺ WAL 重放 | 5s 扫已封存 segment | 后台自动 |
| ❻ 启动恢复 | replayWAL | 进程重启 |
| ❼ 优雅关闭 | 排空 channel + WAL 刷盘 | SIGTERM |

**全链路不丢消息**（`backpressure: fallback` + `wal.enabled: true` 时）。
