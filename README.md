# logateway — 通用 HTTP 消息网关

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

logateway 是一个高可用、高并发、可观测、可扩展的集中式 HTTP 消息网关，用于替代传统 PHP 日志接入层，作为所有日志类数据上报的统一入口。

---

## 目录

- [架构概览](#架构概览)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [API 接口](#api-接口)
- [鉴权机制](#鉴权机制)
- [投递后端](#投递后端)
- [部署指南](#部署指南)
  - [二进制部署](#二进制部署)
  - [Docker 部署](#docker-部署)
  - [Kubernetes 部署](#kubernetes-部署)
- [运维指南](#运维指南)
- [开发指南](#开发指南)
- [监控与报警](#监控与报警)
- [常见问题](#常见问题)

---

## 架构概览

```
客户端
  │  POST {Project, Router, Data}
  ▼
Gin HTTP Server
  ├── 全局限流 (token bucket)
  ├── Header 鉴权 (HMAC-SHA256 + Nonce 防重放)
  ├── 解析 Project → 获取对应配置 (atomic.Value 无锁热缓存)
  ├── 参数校验 (JSON 结构 / 大小限制)
  └── 构建消息，提交至 ants 协程池 → 返回 200
           │
           ▼
    异步 Sink Workers (按项目隔离)
      ├── 指数退避重试 (最多 3 次)
      ├── 熔断器 (连续 10 次失败自动熔断)
      └── 投递至队列中间件
            ├── Redis (List / Stream)
            ├── Kafka (开发中)
            └── 其他自定义 Sink
```

### 模块划分

| 模块 | 路径 | 职责 |
|------|------|------|
| 入口 | `cmd/gateway/` | 初始化组件、启动 Gin Engine、优雅关闭 |
| 配置 | `internal/config/` | YAML 加载、热重载、atomic.Value 无锁存储 |
| 鉴权 | `internal/auth/` | HMAC-SHA256 签名验证、Nonce 防重放缓存 |
| 限流 | `internal/ratelimit/` | 全局 + 项目级令牌桶限流 |
| 消息 | `internal/message/` | Message/Envelope 结构体、sync.Pool 对象池 |
| 投递 | `internal/sink/` | Sink 接口、插件注册、WorkerPool、熔断重试 |
| 路由 | `internal/project/` | 项目解析、多路投递分发 |
| 处理器 | `internal/pipeline/` | 可插拔消息处理器链 |
| 可观测 | `internal/observability/` | 健康检查、结构化 JSON 日志 |

---

## 快速开始

### 前置条件

- Go 1.21+
- Redis（可选，用于 Redis Sink）

### 构建

```bash
# 克隆项目
git clone <repo-url> logateway
cd logateway

# 构建
go build -o gateway ./cmd/gateway/

# 交叉编译 Linux amd64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o gateway ./cmd/gateway/
```

### 运行

```bash
# 使用默认配置文件
./gateway -config configs/gateway.yaml

# 指定配置文件
./gateway -config /etc/logateway/config.yaml
```

### 验证

```bash
# 健康检查
curl http://localhost:8080/health

# 就绪检查
curl http://localhost:8080/ready

# 发送测试请求（鉴权关闭时）
curl -X POST http://localhost:8080/api/v1/log/upload \
  -H "Content-Type: application/json" \
  -d '{"Project":"actilogs","Router":"CH=Behavior&Opt=AddLogs","Data":{"UID":123,"action":"click"}}'
```

### 配置热重载

```bash
# 修改配置文件后触发热重载
curl -X POST http://localhost:8080/admin/config/reload

# 查看各项目 channel 使用率
curl http://localhost:8080/admin/pools
```

---

## 配置说明

配置文件使用 YAML 格式，默认路径 `configs/gateway.yaml`。启动时通过 `-config` 参数指定。

### 完整配置项

#### server

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `listen_addr` | string | `:8080` | HTTP 监听地址 |
| `read_timeout` | duration | `3s` | 请求读取超时 |
| `write_timeout` | duration | `5s` | 响应写入超时 |
| `max_body_bytes` | int | `1048576` (1MB) | 全局最大请求体字节数 |
| `max_conns_per_ip` | int | `100` | 单 IP 最大并发连接数 |
| `global_rate_limit` | int | `20000` | 全局限流 QPS |
| `ants_pool_size` | int | `10000` | 协程池大小 |

#### auth

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `true` | 是否启用鉴权 |
| `timestamp_window` | int | `300` | 时间戳允许偏差（秒） |
| `nonce_ttl_seconds` | int | `300` | Nonce 缓存有效期（秒） |
| `nonce_cache_size` | int | `100000` | Nonce 缓存最大条目数 |

#### projects（数组）

每个项目一个条目：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | string | - | 项目标识（必填） |
| `enabled` | bool | `true` | 是否启用 |
| `sinks` | array | - | 投递后端列表（见下方） |
| `rate_limit` | int | - | 项目级 QPS 限制 |
| `max_body_bytes` | int | - | 项目级请求体大小限制 |
| `auth_required` | bool | `true` | 是否需要鉴权 |
| `pipelines` | array | - | 处理器链配置（可选） |

**sinks 条目：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | string | 投递类型：`redis` 或 `kafka` |
| `config` | map | 投递配置，见下方各 Sink 说明 |

#### sinks.redis

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `addr` | string | `127.0.0.1:6379` | Redis 地址 |
| `password` | string | - | Redis 密码 |
| `db` | int | `0` | Redis 数据库编号 |
| `pool_size` | int | `100` | 连接池大小 |
| `min_idle_conns` | int | `10` | 最小空闲连接数 |
| `dial_timeout` | duration | `5s` | 连接超时 |
| `read_timeout` | duration | `3s` | 读取超时 |
| `write_timeout` | duration | `3s` | 写入超时 |

**项目级 Redis sink config 覆盖：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `key` | string | Redis Key 名称 |
| `type` | string | `list`（LPUSH）或 `stream`（XADD） |
| `max_len` | int | Stream 最大长度（仅 stream 模式） |

#### sinks.kafka

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `brokers` | []string | - | Kafka Broker 地址列表 |
| `batch_size` | int | `100` | 批量发送大小 |
| `batch_timeout` | duration | `100ms` | 批量发送间隔 |

**项目级 Kafka sink config 覆盖：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `topic` | string | 目标 Topic |
| `partition_key` | string | 分区键字段名（如 `UID`） |
| `compression` | string | 压缩算法：`snappy`、`gzip`、`lz4` |

#### log

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `level` | string | `info` | 日志级别：`debug` / `info` / `warn` / `error` |
| `format` | string | `json` | 日志格式：`json` / `text` |

#### metrics

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `true` | 是否启用 Metrics |
| `path` | string | `/metrics` | Metrics 暴露路径 |


#### server.backpressure

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `backpressure` | string | `drop` | Channel 满时策略: `drop`（丢弃）/ `block`（阻塞等待）/ `fallback`（写磁盘 WAL）|

#### wal

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `false` | 是否启用 WAL 磁盘持久化 |
| `dir` | string | `data/wal` | WAL 段文件目录 |
| `max_segment_bytes` | int | `67108864` | 单个段文件最大字节数（64MB）|
| `max_segments` | int | `10` | 最多保留段文件数 |
| `sync_interval` | duration | `100ms` | 定期 fsync 间隔（0=每次写入都 sync）|

#### sink_instances（可选）

| 字段 | 类型 | 说明 |
|------|------|------|
| `<name>.type` | string | Sink 类型: `redis` / `kafka` |
| `<name>.config` | map | Sink 连接参数（字段与全局 `sinks` 相同，逐 key 覆写）|

### 配置示例

<details>
<summary>点击展开完整示例</summary>

```yaml
server:
  listen_addr: ":8080"
  read_timeout: 3s
  write_timeout: 5s
  max_body_bytes: 1048576
  global_rate_limit: 20000
  ants_pool_size: 10000

auth:
  enabled: true
  timestamp_window: 300
  nonce_ttl_seconds: 300
  nonce_cache_size: 100000

projects:
  - name: actilogs
    enabled: true
    sinks:
      - type: redis
        config:
          key: "queue:actilogs"
          type: list
          max_len: 1000000
    rate_limit: 5000
    max_body_bytes: 524288
    auth_required: true

  - name: track
    enabled: true
    sinks:
      - type: kafka
        config:
          topic: "track_logs"
          partition_key: "UID"
          compression: snappy
    rate_limit: 3000
    auth_required: false

sinks:
  redis:
    addr: "redis-cluster:6379"
    password: "${REDIS_PASSWORD}"
    db: 0
    pool_size: 100
    min_idle_conns: 10
    dial_timeout: 5s
    read_timeout: 3s
    write_timeout: 3s

  kafka:
    brokers:
      - "kafka1:9092"
      - "kafka2:9092"
    batch_size: 100
    batch_timeout: 100ms

log:
  level: "info"
  format: "json"

metrics:
  enabled: true
  path: "/metrics"
```

</details>

---

### Sink 配置模式（三层合并）

每个项目的 Sink 最终配置由**三层浅合并**确定：
1. **全局默认值**（`sinks.<type>`）
2. **命名实例**（`sink_instances.<name>`，如果项目指定了 `instance`）
3. **项目覆写**（项目 `config` 中的字段逐 key 覆盖上层）

详细示例参考 `configs/gateway.yaml`，支持四种模式：内联、实例引用、双路投递、实例+覆写。


## API 接口

### 日志上报

**`POST /api/v1/log/upload`**

请求头：

| Header | 必填 | 说明 |
|--------|------|------|
| `Content-Type` | 是 | 固定 `application/json` |
| `X-App-Key` | 鉴权时必填 | 应用标识 |
| `X-Timestamp` | 鉴权时必填 | Unix 秒级时间戳 |
| `X-Nonce` | 鉴权时必填 | 一次性随机字符串（16-64 字符） |
| `X-Signature` | 鉴权时必填 | HMAC-SHA256 签名（Hex 编码） |
| `X-Request-Id` | 否 | 请求追踪 ID（不传则自动生成 UUID） |
| `X-Trace-Id` | 否 | 全链路追踪 ID（不传则复用 RequestID） |

请求体（JSON）：

```json
{
  "Project": "actilogs",
  "Router": "CH=Behavior&Opt=AddLogs",
  "Data": {
    "UID": 12345,
    "BID": 678,
    "action": "click",
    "timestamp": 1718500000
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `Project` | string | 是 | 项目标识，需在配置中定义 |
| `Router` | string | 否 | 路由信息，透传给消费端 |
| `Data` | object | 否 | 业务数据，可以是任意 JSON 结构 |

成功响应 `200`：

```json
{
  "Code": 0,
  "Message": "success",
  "RequestId": "b3f1a2c4-...",
  "TraceId": "b3f1a2c4-..."
}
```

错误响应：

| HTTP 状态码 | Code | 说明 |
|-------------|------|------|
| 400 | 400 | 请求体解析失败 / Project 为空 |
| 401 | 401 | 鉴权失败（签名错误 / Nonce 重放 / 时间戳超窗口） |
| 404 | 404 | 未知 Project |
| 413 | 413 | 请求体超过大小限制 |
| 429 | 429 | 触发限流（全局或项目级） |
| 503 | 503 | 服务繁忙（协程池满） |

### 管理接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 依赖健康状态，返回各组件详情 |
| `GET` | `/ready` | 就绪探针，检查是否可接收流量 |
| `GET` | `/metrics` | Prometheus 指标 |
| `POST` | `/admin/config/reload` | 手动触发热重载 |
| `GET` | `/admin/pools` | 查看各项目 channel 使用率 |

---

## 鉴权机制

### 签名算法

```
Signature = Hex(HMAC-SHA256(Secret, Body + Timestamp + Nonce))
```

**参数说明：**

- `Secret`：与 AppKey 对应的密钥
- `Body`：原始请求体 JSON 字符串
- `Timestamp`：`X-Timestamp` 头部的字符串值
- `Nonce`：`X-Nonce` 头部的字符串值

**注意：** 拼接顺序必须严格按照 `Body + Timestamp + Nonce`，中间无分隔符。

### 鉴权流程

1. 检查 `X-App-Key` 存在且已注册，获取对应 Secret
2. 验证 `X-Timestamp` 与服务器时间的偏差在 `±timestamp_window` 秒内（默认 5 分钟）
3. 检查 `X-Nonce` 是否已被使用（内存缓存，默认 10 万条，简单淘汰）
4. 按相同算法重新计算签名，与 `X-Signature` 进行常数时间比较
5. 确认 AppKey 有权访问该 Project

### 客户端签名示例

<details>
<summary>Go</summary>

```go
package main

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "strconv"
    "time"
)

func main() {
    appKey := "test-app-key"
    secret := "test-secret-key-change-in-production"
    body := `{"Project":"actilogs","Router":"Test","Data":{"k":"v"}}`
    timestamp := strconv.FormatInt(time.Now().Unix(), 10)
    nonce := "random-nonce-" + strconv.FormatInt(time.Now().UnixNano(), 10)

    data := body + timestamp + nonce
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(data))
    signature := hex.EncodeToString(mac.Sum(nil))

    fmt.Printf("X-App-Key: %s\n", appKey)
    fmt.Printf("X-Timestamp: %s\n", timestamp)
    fmt.Printf("X-Nonce: %s\n", nonce)
    fmt.Printf("X-Signature: %s\n", signature)
}
```

</details>

<details>
<summary>PHP</summary>

```php
<?php
$appKey = 'test-app-key';
$secret = 'test-secret-key-change-in-production';
$body = '{"Project":"actilogs","Router":"Test","Data":{"k":"v"}}';
$timestamp = (string)time();
$nonce = 'random-nonce-' . uniqid();

$data = $body . $timestamp . $nonce;
$signature = hash_hmac('sha256', $data, $secret);

echo "X-App-Key: $appKey\n";
echo "X-Timestamp: $timestamp\n";
echo "X-Nonce: $nonce\n";
echo "X-Signature: $signature\n";
```

</details>

<details>
<summary>cURL</summary>

```bash
#!/bin/bash
APP_KEY="test-app-key"
SECRET="test-secret-key-change-in-production"
BODY='{"Project":"actilogs","Router":"Test","Data":{"k":"v"}}'
TIMESTAMP=$(date +%s)
NONCE="nonce-$(date +%s%N)"

SIGNATURE=$(echo -n "${BODY}${TIMESTAMP}${NONCE}" | openssl dgst -sha256 -hmac "${SECRET}" | awk '{print $NF}')

curl -X POST http://localhost:8080/api/v1/log/upload \
  -H "Content-Type: application/json" \
  -H "X-App-Key: ${APP_KEY}" \
  -H "X-Timestamp: ${TIMESTAMP}" \
  -H "X-Nonce: ${NONCE}" \
  -H "X-Signature: ${SIGNATURE}" \
  -d "${BODY}"
```

</details>

---

## 投递后端

### Redis

支持两种投递模式：

| 模式 | Redis 命令 | 适用场景 |
|------|-----------|----------|
| `list` | `LPUSH` | PHP 通过 `BRPOP` / 定时任务消费，兼容现有架构 |
| `stream` | `XADD` | 消费组模式，支持 ACK、重试、多消费者 |

**投递消息格式：**

```json
{
  "_gateway_meta": {
    "request_id": "b3f1a2c4-...",
    "trace_id": "b3f1a2c4-...",
    "received_at": "2026-06-16T10:00:00Z"
  },
  "Project": "actilogs",
  "Router": "CH=Behavior&Opt=AddLogs",
  "Data": {"UID": 123, "action": "click"}
}
```

PHP 消费端可从 `Data` 字段获取原始业务数据，从 `_gateway_meta` 获取链路追踪信息。

### Kafka（开发中）

当前 Kafka Sink 为桩实现，后续将集成 `segmentio/kafka-go` 支持：
- 异步生产者，幂等写入
- 按配置分区策略（基于 `partition_key` 字段）
- 压缩支持（snappy / gzip / lz4）

### 自定义 Sink

实现 `sink.Sink` 接口并注册即可扩展：

```go
type Sink interface {
    Send(ctx context.Context, msg *message.Message) error
    Name() string
    HealthCheck() error
    Close() error
}

// 注册
reg.Register("my-sink", MySinkFactory)
```

---

## 部署指南

### 二进制部署

```bash
# 1. 构建
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o gateway ./cmd/gateway/

# 2. 准备目录结构
mkdir -p /opt/logateway/configs /opt/logateway/logs

# 3. 上传文件
scp gateway user@server:/opt/logateway/
scp configs/gateway.yaml user@server:/opt/logateway/configs/

# 4. 创建 systemd 服务
cat > /etc/systemd/system/logateway.service << 'EOF'
[Unit]
Description=logateway HTTP Message Gateway
After=network.target

[Service]
Type=simple
User=nobody
WorkingDirectory=/opt/logateway
ExecStart=/opt/logateway/gateway -config /opt/logateway/configs/gateway.yaml
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

# 环境变量
Environment=GOMAXPROCS=4
Environment=GOMEMLIMIT=1800MiB

[Install]
WantedBy=multi-user.target
EOF

# 5. 启动
systemctl daemon-reload
systemctl enable logateway
systemctl start logateway
systemctl status logateway
```

### Docker 部署

**Dockerfile：**

```dockerfile
# 多阶段构建
FROM golang:1.21-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gateway ./cmd/gateway/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -H -h /app appuser
WORKDIR /app
COPY --from=builder /build/gateway .
COPY --from=builder /build/configs/gateway.yaml ./configs/
USER appuser
EXPOSE 8080
ENTRYPOINT ["./gateway", "-config", "configs/gateway.yaml"]
```

```bash
# 构建镜像
docker build -t logateway:latest .

# 运行
docker run -d \
  --name logateway \
  -p 8080:8080 \
  -v /path/to/config.yaml:/app/configs/gateway.yaml:ro \
  --restart unless-stopped \
  logateway:latest

# 查看日志
docker logs -f logateway
```

### Kubernetes 部署

**Deployment：**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logateway
  labels:
    app: logateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: logateway
  template:
    metadata:
      labels:
        app: logateway
    spec:
      terminationGracePeriodSeconds: 35
      containers:
        - name: gateway
          image: logateway:latest
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: GOMAXPROCS
              valueFrom:
                resourceFieldRef:
                  resource: limits.cpu
            - name: GOMEMLIMIT
              value: "1700MiB"
          resources:
            requests:
              cpu: 500m
              memory: 512Mi
            limits:
              cpu: 2000m
              memory: 2Gi
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: config
              mountPath: /app/configs
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: logateway-config

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: logateway-config
data:
  gateway.yaml: |
    # 此处放置完整配置
    server:
      listen_addr: ":8080"
      # ...
```

**Service：**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: logateway
spec:
  selector:
    app: logateway
  ports:
    - port: 8080
      targetPort: 8080
      name: http
  type: ClusterIP
```

**HPA（水平自动伸缩）：**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: logateway
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: logateway
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

### 环境变量参考

| 变量 | 说明 | 推荐值 |
|------|------|--------|
| `GOMAXPROCS` | Go 最大并行数 | 容器 CPU Limit 值 |
| `GOMEMLIMIT` | Go GC 内存软限制 | 容器 Memory Limit 的 80-90% |

---

## 运维指南

### 优雅关闭流程

收到 `SIGTERM` 或 `SIGINT` 后：

1. **标记不健康** — Readiness probe 返回失败，K8s 停止路由流量
2. **停止接收新请求** — HTTP Server 调用 `Shutdown()`
3. **等待进行中请求** — 最长 30 秒
4. **排空内存 Channel** — WorkerPool 处理完已入队消息
5. **关闭 Sink 连接** — Redis 连接池、Kafka Producer 正常关闭

若 30 秒内未完成，强制退出。

### 健康检查

- **`/health`**：返回所有注册组件的健康状态。每 5 秒异步探测并缓存结果。
- **`/ready`**：返回网关是否准备好接收流量。关闭过程中返回 false。

```json
{
  "status": true,
  "timestamp": "2026-06-16T10:00:00Z",
  "components": {
    "redis-sink-actilogs": true,
    "redis-sink-track": true
  }
}
```

### 日志

所有日志输出到 stdout，JSON 格式，每行一条记录：

```json
{"timestamp":"2026-06-16T10:00:00.000Z","level":"info","message":"request","request_id":"abc-123","trace_id":"abc-123","project":"","error":"method=POST path=/api/v1/log/upload status=200 duration=2.3ms"}
```

可通过 `jq` 格式化查看：

```bash
tail -f /var/log/logateway/app.log | jq .
```

### 常用运维命令

```bash
# 触发热重载
curl -X POST http://localhost:8080/admin/config/reload

# 查看 channel 积压情况
curl http://localhost:8080/admin/pools

# 查看健康状态
curl http://localhost:8080/health | jq .

# 手动触发关闭
kill -TERM $(pidof gateway)
```

---

## 开发指南

### 项目布局

```
logateway/
├── cmd/gateway/            # 入口，依赖组装
│   └── main.go
├── configs/                # 配置文件
│   └── gateway.yaml
├── internal/
│   ├── auth/               # 鉴权中间件
│   ├── config/             # 配置管理
│   ├── message/            # 消息模型与对象池
│   ├── observability/      # 健康检查、日志
│   ├── pipeline/           # 处理器链
│   ├── project/            # 项目路由分发
│   ├── ratelimit/          # 限流器
│   └── sink/               # 投递后端
│       ├── sink.go         # 接口与注册表
│       ├── redis_sink.go   # Redis 实现
│       ├── kafka_sink.go   # Kafka 实现
│       └── worker.go       # WorkerPool、熔断、重试
├── go.mod
├── go.sum
└── README.md
```

### 添加新 Sink

1. 实现 `sink.Sink` 接口：

```go
type MySink struct{ name string }

func (s *MySink) Send(ctx context.Context, msg *message.Message) error { ... }
func (s *MySink) Name() string { return s.name }
func (s *MySink) HealthCheck() error { return nil }
func (s *MySink) Close() error { return nil }
```

2. 创建工厂函数并注册：

```go
func MySinkFactory(name string, cfg map[string]interface{}) (sink.Sink, error) {
    return &MySink{name: name}, nil
}

// 在 main.go 中注册
reg.Register("my-sink", MySinkFactory)
```

3. 在项目配置中引用：

```yaml
projects:
  - name: my-project
    sinks:
      - type: my-sink
        config:
          key: "value"
```

### 添加 Pipeline 处理器

```go
type FieldFilter struct {
    include []string
}

func (f *FieldFilter) Process(msg *message.Message) (*message.Message, error) {
    // 过滤 Data 字段
    var data map[string]interface{}
    json.Unmarshal(msg.Data, &data)
    filtered := make(map[string]interface{})
    for _, field := range f.include {
        if v, ok := data[field]; ok {
            filtered[field] = v
        }
    }
    msg.Data, _ = json.Marshal(filtered)
    return msg, nil
}

func (f *FieldFilter) Name() string { return "field_filter" }
```

### 本地开发

```bash
# 运行测试
go test ./...

# 代码检查
go vet ./...

# 启动开发服务器（debug 日志）
go run ./cmd/gateway/ -config configs/gateway.yaml
```

---

## 监控与报警

### 关键指标

| 指标 | 报警条件 | 说明 |
|------|---------|------|
| HTTP 5xx 错误率 | > 1% | 服务异常 |
| Sink 投递失败 | > 0.1% | 下游中间件异常 |
| Channel 使用率 | > 80% | 内存队列即将满，需扩容 |
| 限流拒绝数 | 突增 | 流量超预期 |
| 熔断器打开 | 任意 Sink 熔断 | 下游不可用 |
| 实例 CPU | > 80% | 需扩容 |
| 实例内存 | > 80% | 需扩容或排查泄漏 |

### Prometheus 指标规划

未来将在 `gateway_` 命名空间下暴露以下指标：

```
gateway_http_requests_total{project,status}
gateway_http_request_duration_seconds{project,status,quantile}
gateway_sink_deliveries_total{sink,status}
gateway_sink_retries_total{sink}
gateway_circuit_state{sink}          # 0=closed, 1=open
gateway_channel_usage_ratio{sink}
gateway_ratelimit_rejects_total{project}
gateway_goroutines_active
```

---

## 性能基准

| 场景 | 配置 | 结果 |
|------|------|------|
| 单实例 QPS | 2C4G, Redis Sink | 5000+ QPS（待实测） |
| P99 延迟 | 网关内部处理 | < 50ms（待实测） |
| 内存消耗 | 5000 QPS 稳态 | 稳定无泄漏（待实测） |
| 二进制体积 | 编译优化 | ~15MB |

---

## 常见问题

### Q: 如何关闭鉴权？

设置 `auth.enabled: false`，或对特定项目设置 `auth_required: false`。

### Q: 如何新增一个项目？

在配置文件的 `projects` 列表中添加新条目，然后执行热重载：

```bash
curl -X POST http://localhost:8080/admin/config/reload
```

### Q: 消息会丢失吗？

异步模式下（默认），如果进程崩溃，内存队列中的未投递消息会丢失。对于不
可丢失的重要业务：
- 可设置 channel 满时写磁盘 WAL（后续版本支持）
- 或多实例部署 + 客户端重试

优雅关闭流程会尽力排空队列。

### Q: Nonce 缓存会无限增长吗？

不会。默认最多缓存 100,000 条，达到上限后逐条淘汰。每个 Nonce 有效期为
时间戳窗口（默认 5 分钟），超过窗口的请求本身会被拒绝。生产环境中可考虑
使用 Redis 集中存储 Nonce。

### Q: 如何实现 TLS/HTTPS？

Gin 原生支持 TLS。在 `main.go` 中将 `srv.ListenAndServe()` 替换为
`srv.ListenAndServeTLS(certFile, keyFile)`，或前置 Nginx/Ingress 做 TLS
终止（推荐）。

### Q: 支持 gzip 压缩吗？

Gin 默认处理 gzip 请求体。可添加 `gin-contrib/gzip` 中间件启用响应压缩。

---

## 路线图

- [x] Gin HTTP 接入、静态配置、Redis Sink
- [x] HMAC-SHA256 鉴权、Nonce 防重放
- [x] 协程池（ants）、全局/项目限流
- [x] 熔断器、重试、优雅关闭
- [x] 结构化 JSON 日志、健康检查
- [x] Prometheus 指标暴露
- [x] Kafka Sink 完整实现（segmentio/kafka-go）
- [x] 配置文件热监听（fsnotify watcher）
- [x] 内置 Pipeline 处理器（字段过滤、脱敏、添加）
- [x] 磁盘 WAL 兜底
- [ ] 压力测试报告

---

## 开源协议

MIT License. 详见 [LICENSE](./LICENSE)。
