# logateway — 通用 HTTP 消息网关

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)
[![DeepSeek](https://img.shields.io/badge/Powered%20by-DeepSeek%20V4%20Pro-536DFE)](https://deepseek.com)

logateway 是一个高可用、高并发、可观测、可扩展的集中式 HTTP 消息网关，用于替代传统 PHP 日志接入层，作为所有日志类数据上报的统一入口。

---

## 目录

- [架构概览](#架构概览)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [API 接口](#api-接口)
- [鉴权机制](#鉴权机制)
- [投递后端](#投递后端)
- [数据可靠性](#数据可靠性)
- [部署指南](#部署指南)
- [运维指南](#运维指南)
- [开发指南](#开发指南)
- [监控与报警](#监控与报警)
- [常见问题](#常见问题)

---

## 架构概览

```
客户端 POST {project, router, data}
  │
  ▼
Gin HTTP Server
  ├── 全局限流
  ├── HMAC-SHA256 鉴权 + Nonce 防重放
  ├── 项目解析 → 三层配置合并
  └── 异步投递 → 返回 200
         │
         ▼
  Worker Pool (可配置 workers + channel_size)
    ├── Retry ×3（指数退避）
    ├── Circuit Breaker（连续失败自动熔断）
    ├── WAL 磁盘兜底（backpressure=fallback）
    └── Sink
          ├── Redis (List / Stream)
          ├── Kafka
          └── 自定义
```

---

## 快速开始

```bash
go build -o gateway ./cmd/gateway/
./gateway -config configs/gateway.yaml
```

```bash
# 健康检查
curl http://localhost:8080/health

# 发送消息
curl -X POST http://localhost:8080/api/v1/log/upload \
  -H "Content-Type: application/json" \
  -H "X-App-Key: test-app-key" \
  -H "X-Timestamp: $(date +%s)" \
  -H "X-Nonce: $(date +%s%N)" \
  -H "X-Signature: $(echo -n '{"project":"actilogs","router":"Test","data":{"k":"v"}}'$(date +%s)$(date +%s%N) | openssl dgst -sha256 -hmac 'test-secret' | awk '{print $NF}')" \
  -d '{"project":"actilogs","router":"Test","data":{"k":"v"}}'

# Prometheus 指标
curl http://localhost:8080/metrics
```

---

## 配置说明

### server

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `listen_addr` | string | `:8080` | 监听地址 |
| `read_timeout` | duration | `3s` | 请求读取超时 |
| `write_timeout` | duration | `5s` | 响应写入超时 |
| `max_body_bytes` | int | `1048576` | 全局最大 Body |
| `global_rate_limit` | int | `20000` | 全局 QPS 上限 |
| `ants_pool_size` | int | `10000` | HTTP 处理协程池 |
| `backpressure` | string | `drop` | 反压策略: `drop` / `block` / `fallback` |
| `env` | string | - | 环境标识，注入 `_gateway_meta.env` |

### auth

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `enabled` | bool | `true` | 是否启用鉴权 |
| `timestamp_window` | int | `300` | 时间戳允许偏差（秒） |
| `nonce_ttl_seconds` | int | `300` | Nonce 缓存 TTL |
| `nonce_cache_size` | int | `100000` | Nonce 最大缓存数 |

### projects

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 项目标识 |
| `enabled` | bool | 是否启用 |
| `rate_limit` | int | 项目级 QPS |
| `max_body_bytes` | int | 项目级 Body 限制 |
| `auth_required` | bool | 是否要求鉴权 |
| `pipelines` | array | 处理器链 |

### sinks（项目级，三层合并）

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | `redis` / `kafka` |
| `instance` | string | 引用 `sink_instances` 命名实例 |
| `workers` | int | Worker goroutine 数（默认 16） |
| `channel_size` | int | Channel 缓冲大小（默认 16384） |
| `config` | map | Sink 连接参数 |

**项目 config 支持以下 key（合并后传递给 sink 工厂）：**

Redis: `addr`, `password`, `db`, `pool_size`, `min_idle_conns`, `dial_timeout`, `read_timeout`, `write_timeout`, `key`, `type`, `max_len`

Kafka: `brokers`, `topic`, `partition_key`, `compression`, `batch_size`, `batch_timeout`

### sinks（全局默认值）

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `redis.addr` | string | `127.0.0.1:6379` | Redis 地址 |
| `redis.password` | string | - | Redis 密码 |
| `redis.db` | int | `0` | Redis DB 编号 |
| `redis.pool_size` | int | `100` | 连接池大小 |
| `redis.min_idle_conns` | int | `10` | 最小空闲连接 |
| `redis.dial_timeout` | duration | `5s` | 连接超时 |
| `redis.read_timeout` | duration | `3s` | 读取超时 |
| `redis.write_timeout` | duration | `3s` | 写入超时 |
| `kafka.brokers` | []string | - | Broker 列表 |
| `kafka.batch_size` | int | `100` | 批量大小 |
| `kafka.batch_timeout` | duration | `100ms` | 批量间隔 |

### wal

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `enabled` | bool | `false` | 是否启用 |
| `dir` | string | `data/wal` | 段文件目录 |
| `max_segment_bytes` | int | `67108864` | 单段最大字节 |
| `max_segments` | int | `10` | 保留段数 |
| `sync_interval` | duration | `100ms` | fsync 间隔（0=每条 sync） |

### 配置示例

```yaml
server:
  listen_addr: ":8080"
  backpressure: fallback
  env: "production"
  ants_pool_size: 10000
  global_rate_limit: 20000

auth:
  enabled: true
  timestamp_window: 300

sinks:
  redis:
    addr: "127.0.0.1:6379"
    db: 0
    pool_size: 100
    min_idle_conns: 10

sink_instances:
  redis_hp:
    type: redis
    config:
      addr: "redis-prod:6379"
      pool_size: 200
      workers: 32
      channel_size: 65536

projects:
  - name: actilogs
    enabled: true
    sinks:
      - type: redis
        workers: 16
        channel_size: 16384
        config:
          key: "queue:actilogs"
          type: list
    rate_limit: 5000
    auth_required: true
    pipelines:
      - type: field_filter
        config:
          mode: exclude
          fields: ["internal_token"]

wal:
  enabled: true
  dir: "data/wal"
  sync_interval: 0

log:
  level: "info"
  format: "json"

metrics:
  enabled: true
  path: "/metrics"
```

---

## API 接口

### POST /api/v1/log/upload

请求头：

| Header | 必填 | 说明 |
|--------|------|------|
| `Content-Type` | 是 | `application/json` |
| `X-App-Key` | 鉴权时 | 应用标识 |
| `X-Timestamp` | 鉴权时 | Unix 秒时间戳 |
| `X-Nonce` | 鉴权时 | 随机字符串（16-64字符） |
| `X-Signature` | 鉴权时 | HMAC-SHA256（Hex） |
| `X-Request-Id` | 否 | 请求追踪 ID |
| `X-Trace-Id` | 否 | 全链路追踪 ID |

请求体（snake_case）：

```json
{
  "project": "actilogs",
  "router": "CH=Behavior",
  "data": {"UID": 123, "action": "click"}
}
```

成功响应 200：

```json
{
  "code": 0,
  "message": "success",
  "request_id": "b3f1a2c4-...",
  "trace_id": "b3f1a2c4-..."
}
```

错误码：

| HTTP | code | 说明 |
|------|------|------|
| 400 | 400 | JSON 解析失败 / project 为空 |
| 401 | 401 | 签名错误 / Nonce 重放 / 时间戳超窗口 |
| 404 | 404 | 未知 project |
| 413 | 413 | Body 超限 |
| 429 | 429 | 限流 |
| 503 | 503 | 协程池满 |

### 管理接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 依赖健康状态 |
| GET | `/ready` | 就绪探针 |
| GET | `/metrics` | Prometheus 指标 |
| POST | `/admin/config/reload` | 触发热重载 |
| GET | `/admin/pools` | Channel 使用率 |

---

## 鉴权机制

```
Signature = Hex(HMAC-SHA256(Secret, Body + Timestamp + Nonce))
```

- `Secret`: 与 AppKey 对应的预共享密钥
- `Body`: 原始请求体 JSON 字符串
- `Timestamp`: `X-Timestamp` 头部的值
- `Nonce`: `X-Nonce` 头部的值
- 拼接顺序: `Body + Timestamp + Nonce`，无分隔符

鉴权流程：检查 AppKey → 验证时间戳窗口（±300s）→ 检查 Nonce 是否重放 → 重算签名对比。

客户端示例见 `examples/` 目录或运行 `go test -v ./internal/server/ -run TestUpload`。

---

## 投递后端

### Redis

支持 `list`（LPUSH）和 `stream`（XADD）两种模式。投递格式：

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
  "data": {"UID": 123, "action": "click"}
}
```

- `_gateway_meta` — 网关元数据
- `project` / `router` — 客户端上报原始值
- `data` — 客户端业务数据（可能经过 pipeline 加工）

### Kafka

基于 `segmentio/kafka-go`，支持 Hash 分区、snappy/gzip/lz4/zstd 压缩。

### 自定义 Sink

实现 `sink.Sink` 接口并注册：

```go
type Sink interface {
    Send(ctx context.Context, msg *message.Message) error
    Name() string
    HealthCheck() error
    Close() error
}
reg.Register("my-sink", MySinkFactory)
```

---

## 数据可靠性

### 三层保障

| 层级 | 机制 | 场景 |
|------|------|------|
| Channel 缓冲 | 16384 缓冲 + 16 workers | 瞬时尖峰 |
| WAL 磁盘兜底 | `backpressure=fallback`，写 `data/wal/` | Channel 溢出 |
| 启动重放 | `replayWAL` 读取段文件重新投递 | 进程崩溃恢复 |

### 反压策略

| 策略 | channel 满时 | 数据安全 |
|------|-------------|---------|
| `drop` | 丢弃 | ✗ |
| `block` | 阻塞 100ms | ✗ (超时后丢弃) |
| `fallback` | 写磁盘 WAL | ✓ (重启重放) |

### 优雅关闭

收到 SIGTERM 后：停止接收请求 → WAL 排空 channel（for-range 阻塞排空）→ 投递进行中消息（独立 context）→ 关闭 Redis/Kafka 连接。

### 配置热重载限制

`/admin/config/reload` 可热更新鉴权参数、限流阈值。**项目增删、sink 连接变更**需要滚动重启（会先排空 channel 再停服）。

---

## 部署指南

### 二进制

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o gateway ./cmd/gateway/
./gateway -config /etc/logateway/gateway.yaml
```

### Docker

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

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: logateway
  template:
    spec:
      terminationGracePeriodSeconds: 35
      containers:
        - name: gateway
          image: logateway:latest
          ports:
            - containerPort: 8080
          env:
            - name: GOMAXPROCS
              valueFrom:
                resourceFieldRef:
                  resource: limits.cpu
            - name: GOMEMLIMIT
              value: "1700MiB"
          resources:
            requests: { cpu: "500m", memory: "512Mi" }
            limits: { cpu: "2000m", memory: "2Gi" }
          livenessProbe:
            httpGet: { path: /health, port: 8080 }
          readinessProbe:
            httpGet: { path: /ready, port: 8080 }
```

---

## 运维指南

### 健康检查

- `/health` — 所有 Sink PING 状态（每 5s 异步探测缓存）
- `/ready` — 网关是否可接收流量（关闭中返回 false）
- `/metrics` — Prometheus 指标

### 日志

结构化 JSON，输出到 stdout：

```json
{"timestamp":"...","level":"info","message":"request","request_id":"...","trace_id":"...","error":"method=POST path=/api/v1/log/upload status=200 duration=2.3ms"}
```

### 常用命令

```bash
curl -X POST http://localhost:8080/admin/config/reload  # 热重载
curl http://localhost:8080/admin/pools                   # channel 使用率
curl http://localhost:8080/metrics | grep gateway_        # 指标
kill -TERM $(pidof gateway)                               # 优雅关闭
```

---

## 开发指南

### 项目布局

```
cmd/gateway/main.go          # 入口（仅 14 行）
internal/
  server/                    # HTTP 路由、中间件、Handler
  auth/                      # HMAC 鉴权中间件
  config/                    # YAML 配置、热加载、fsnotify
  message/                   # Message/Envelope、sync.Pool
  metrics/                   # Prometheus 指标定义
  observability/             # 健康检查、MetricsMiddleware
  pipeline/                  # 处理器链 + 内置处理器
  project/                   # 项目路由、三层配置合并
  ratelimit/                 # 全局+项目限流
  sink/                      # Sink 接口、Redis/Kafka、WorkerPool
  wal/                       # 磁盘 WAL 读写
configs/gateway.yaml         # 默认配置
examples/                    # 场景配置示例
```

### 添加 Sink

1. 实现 `sink.Sink` 接口
2. 创建 Factory 函数
3. 在 `server.go` 中 `reg.Register("my-sink", MySinkFactory)`

### 运行测试

```bash
go test -short ./internal/server/   # 快速（无需 Redis）
go test ./internal/server/          # 完整（需 Redis）
go vet ./...
```

---

## 监控与报警

### Prometheus 指标

| 指标 | 类型 | 标签 |
|------|------|------|
| `gateway_http_requests_total` | Counter | project, method, status |
| `gateway_http_request_duration_seconds` | Histogram | project, method, status |
| `gateway_sink_deliveries_total` | Counter | sink, status |
| `gateway_sink_retries_total` | Counter | sink |
| `gateway_circuit_state` | Gauge | sink (0=closed, 1=open) |
| `gateway_channel_usage_ratio` | Gauge | sink |
| `gateway_ratelimit_rejects_total` | Counter | level, project |
| `gateway_pool_goroutines` | Gauge | - |
| `gateway_pool_capacity` | Gauge | - |

### 报警建议

| 条件 | 说明 |
|------|------|
| `gateway_http_requests_total{status=~"5.."} > 1%` | 服务异常 |
| `gateway_sink_deliveries_total{status="failure"} > 0.1%` | 下游异常 |
| `gateway_channel_usage_ratio > 0.8` | 即将溢出 |
| `gateway_circuit_state == 1` | 熔断打开 |

---

## 常见问题

### 消息会丢吗？

默认配置 `backpressure: fallback` + `wal.enabled: true` 确保不丢失：channel 满 → 写磁盘 → 重启重放。优雅关闭排空 channel。

### 怎么新增项目？

YAML 添加 project → `POST /admin/config/reload` 热重载（限流/鉴权生效）。如果项目包含新 sink 连接配置，**需滚动重启**。

### Nonce 会撑爆内存吗？

上限 100,000 条，超出逐条淘汰。每条有效期为时间戳窗口（5分钟），超时请求本身被拒绝。

### 怎么压测调优？

1. 提高 `workers`（默认 16）和 `channel_size`（默认 16384）
2. 开启 `backpressure: fallback` + WAL
3. 观察 `gateway_channel_usage_ratio`，持续 > 0.5 则继续扩容

### reload 会中断处理吗？

不会。`atomic.Value` 整体替换，进行中的请求使用旧配置完成。但 worker pool 不更新——sink 连接变更需重启。

---

## 路线图

- [x] Gin HTTP 接入、Redis/Kafka Sink
- [x] HMAC-SHA256 鉴权、Nonce 防重放
- [x] ants 协程池、全局+项目限流
- [x] 熔断器、重试、优雅关闭（排空 channel）
- [x] Prometheus 指标暴露
- [x] 配置文件热监听（fsnotify）
- [x] Pipeline 处理器（field_filter / field_add / field_redact）
- [x] 磁盘 WAL 兜底 + 重启重放
- [x] 命名 Sink 实例 + 三层配置合并
- [ ] 压力测试报告

---

MIT License
