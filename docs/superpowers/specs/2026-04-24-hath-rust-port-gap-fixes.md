# H@H Rust Port — 协议差距修复设计

## Context

对比 Java HentaiAtHome 1.6.5 源码，Rust 端口存在 8 个功能性差距和 UB 风险。
本文档逐一描述 Java 行为、Rust 差距、修复方案。

---

## P0-1: 缓存未命中代理回源

### Java 行为

```
请求到达 → 查本地缓存
  ├─ 命中 → 校验尺寸/keystamp → 返回文件
  └─ 未命中 → RPC srfetch(fileindex, xres, fileid)
       ├─ 服务器返回源 URL 列表
       └─ HTTPResponseProcessorProxy(urls)
            ├─ 并发连接上游（同一个源 server）
            ├─ 边下载边写 TCP 响应
            └─ 下载完成 → 校验 SHA-1 → 导入缓存
```

### Rust 差距

`server.rs` 的 `FileServe` 分支：
- 缓存命中：正常 serve（ok）
- 缓存未命中：**直接 `not_found_response()`**（404）

`ProxyFileDownloader` 和 `RpcClient::static_range_fetch()` 都已实现但从未调用。

### Rust 修复方案

```
server.rs FileServe 分支:
  cache_path.exists() && size_correct
    → file_response (现有路径，不改)

  cache_path 不存在：
    1. 从 additional 中取 fileindex / xres
    2. 调 rpc_client.static_range_fetch(fileindex, xres, fileid)
       → 解析响应 lines 为 Vec<Url>
    3. 构造 ProxyFileDownloader::new(fileid, &sources, &config)
       → 内部 try_source() 逐个尝试上游
    4. 构建流式响应:
       - content-type: hv_file.mime_type()
       - content-length: hv_file.size
       - body: 循环 wait_for_data() → fill_buffer() → yield chunk
    5. ProxyFileDownloader 成功后内部导入缓存
```

**需修改的文件**：

| 文件 | 改动 |
|------|------|
| `src/server.rs` | `HathService` 的 `Response` 类型从 `Full<Bytes>` 改为 `StreamBody`；FileServe 分支加 srfetch→proxy 路径 |
| `src/response.rs` | 新增 `proxy_streaming_response()` 构建流式 body |
| `src/proxy_downloader.rs` | 可能需要微调（当前用 `std::sync::Mutex` + blocking I/O，需要确认在 async 上下文中正常工作） |

**流式 body 实现选择**：
- 用 `hyper::body::Body::channel()` (channel-based streaming body) 从 proxy_downloader 的 temp file 读取
- 或者用 `http_body_util::StreamBody<impl Stream<Item = Result<Frame<Bytes>>>>` 

**风险**：`ProxyFileDownloader` 内部用 `std::fs::File` 做阻塞 I/O，而它从 tokio task 调用。需要改为 `tokio::task::spawn_blocking` 包裹，或改用 `tokio::fs`。

---

## P0-2: 服务器启动时序

### Java 行为

```java
// HentaiAtHomeClient.run() — 同步
startConnectionListener();        // bind + listen，阻塞直到 TLS 就绪
// 如果上面抛异常 → 停止启动
notifyStart();                    // client_start RPC
if (startResult != OK) → stop    // 停止启动
else → allowNormalConnections = true
```

### Rust 差距

```rust
// client.rs
tokio::spawn(start_server);      // fire-and-forget，不等待
rpc_client.client_start();       // 不管 server 是否成功 bind
allow_connections.store(true);   // 不管 client_start 是否 OK
// 如果 client_start 返回 FAIL_CID_IN_USE / FAIL_OTHER_CLIENT_CONNECTED:
//   只 log error，不 return，继续运行
```

**具体风险场景**：
1. cert 下载失败 → spawn 里 log error，主流程照常 `client_start` → 服务器探活失败
2. 端口被占用 → bind 失败 → log error → 主流程不知情
3. P12 解析失败 → TLS 不可用 → 同上
4. `client_start` 返回 `FAIL_CID_IN_USE` → 仅 log，不退出，继续 allow connections（另一个客户端已经在线 → 冲突）

### Rust 修复方案

```
client::run 流程改为:

1. 创建 oneshot channel (tx, rx)
2. 修改 start_server: 完成 bind 后立即 send(Ok(port))，失败 send(Err)
3. 用 tokio::spawn 启动 server task
4. rx.await → 等待 bind 成功
   ├─ Ok(port) → 继续
   └─ Err(e) → return Err(e)  // 停止启动
5. rpc_client.client_start().await
   ├─ Ok → 继续
   └─ Err / FAIL_* → return Err(Fatal)  // 停止启动
6. allow_connections.store(true)
```

**需修改的文件**：

| 文件 | 改动 |
|------|------|
| `src/server.rs` | `start_server` 签名加 `ready_tx: Option<tokio::sync::oneshot::Sender<Result<u16>>>`，bind 后 send |
| `src/client.rs` | 加 oneshot，等待 ready，client_start 失败处理 |

---

## P1-1: 带宽限制与连接数控制

### Java 行为

**带宽限制**（HTTPBandwidthMonitor）：
- 所有非本地连接的响应写出时，每发出一个 chunk 调 `bwm.waitForQuota(chunk.len())`
- `bwm` 按 `throttle_bytes` 初始化（server 下发）

**连接数控制**（maxConnections）：
- Accept 循环里原子计数 `openConnections.incrementAndGet()`
- 超过 `maxConnections` → `openConnections.decrementAndGet()` + 拒绝连接
- 每 10 秒检查：连续超过 10 秒 → 调 RPC `overload` 通知服务器
- 连接关闭时 `openConnections.decrementAndGet()`

### Rust 差距

- `BandwidthMonitor` 已实现，但**只用在下行**（`FileDownloader`），响应写出完全未接入
- `max_connections()` 方法存在但**从未调用**
- `Stats::open_connections` 字段存在但**从未写入**
- Accept 循环中无连接计数、无上限拒绝

### Rust 修复方案

**1. 连接计数 + 上限**
```rust
// server.rs accept 循环
let current = state.stats.open_connections.load(Ordering::Relaxed);
if !is_local && !is_rpc && current >= state.config.max_connections() {
    // 通知服务器 overload（间隔至少 5 分钟）
    state.stats.set_open_connections(current); // 保留当前值
    drop(stream); // 拒绝连接
    continue;
}
state.stats.open_connections.fetch_add(1, Ordering::Relaxed);
// ... 在 serve_connection 完成后
state.stats.open_connections.fetch_sub(1, Ordering::Relaxed);
```

**2. 响应带宽限制**
- 在 `AppState` 中新增 `serve_limiter: Arc<BandwidthMonitor>`
- 在 `client.rs` 中初始化：
  ```rust
  let serve_limiter = Arc::new(BandwidthMonitor::new(config.throttle_bytes));
  ```
- 在 `file_response` 和 `proxy_streaming_response` 中，写出每个 chunk 前调 `limiter.wait_for_quota(chunk.len())`

**3. Overload 通知**
- 在 `Stats` 中加 `last_overload_notification: AtomicI64`
- 拒绝连接时检查间隔（≥ 5 分钟），调 `rpc_client.overload()`

**需修改的文件**：

| 文件 | 改动 |
|------|------|
| `src/server.rs` | Accept 循环加连接计数/拒绝；HathService/AppState 加 serve_limiter |
| `src/response.rs` | file_response 加带宽限速 |
| `src/client.rs` | 初始化 serve_limiter |
| `src/stats.rs` | 加 last_overload_notification 字段 |

---

## P1-2: server_stat RPC 格式

### Java 行为

```java
// 所有 action 通用格式 (client_login, still_alive, ...):
// "clientbuild=178&act={act}&add={add}&cid={cid}&acttime={time}&actkey={key}"

// server_stat 特殊格式:
// "clientbuild=178&act=server_stat"
// 只有 2 个参数，无 cid/acttime/actkey
```

原因：`server_stat` 是第一个调用的 RPC，用来校时。此时还没有 `server_time_delta`，所以不带签名。

### Rust 差距

所有 action 走同一个 `make_rpc_query`，全部签名。`server_stat` URL 变成带有 `cid=`/`acttime=`/`actkey=` 的完整格式。

### Rust 修复方案

最小改动：给 `Action` 加方法区分是否需要完整签名。

```rust
impl Action {
    /// server_stat 不带签名，其他都带
    fn needs_signing(self) -> bool {
        !matches!(self, Self::ServerStat)
    }
}
```

`make_rpc_query` 改为：
```rust
pub fn make_rpc_query(act: Action, add: &str, config: &Config) -> String {
    if !act.needs_signing() {
        return format!("clientbuild={}&act={}", CLIENT_BUILD, act);
    }
    // 原有完整签名逻辑
    ...
}
```

**需修改的文件**：

| 文件 | 改动 |
|------|------|
| `src/rpc.rs` | `Action` 加 `needs_signing()`，`make_rpc_query` 分支 |

---

## P1-3: servercmd 真实现

### Java 行为 vs Rust 差距

| Command | Java 行为 | Rust 现状 | 差距 |
|---------|----------|----------|------|
| `still_alive` | 返回 `"I feel FANTASTIC..."` | 同 Java | ✅ 正确 |
| `threaded_proxy_test` | `"OK:{currentThreads}-{maxThreads}"` | `"OK:0-0"` 硬编码 | ❌ 返回假值 |
| `speed_test` | 返回指定大小的随机数据 | 返回空文本 | ❌ 无数据 |
| `refresh_settings` | RPC 拿回 settings → **应用到配置** | 调 RPC 但不应用结果 | ❌ 丢弃响应 |
| `start_downloader` | 启动 gallery downloader | 空返回 | ⚠️ 下载器未实现（Phase 1 范围外） |
| `refresh_certs` | 重新下载证书 | 空返回 | ❌ 无动作 |
| `unknown` | 返回 `"INVALID_COMMAND"` | 同 Java | ✅ 正确 |

### Rust 修复方案

**threaded_proxy_test**：
```rust
"threaded_proxy_test" => {
    let open = state.stats.open_connections.load(Ordering::Relaxed);
    let max = state.config.max_connections();
    response::text_response(StatusCode::OK, &format!("OK:{}-{}", open, max))
}
```

**speed_test**：
`handle_server_command` 加 `additional: &str` 参数。请求 URL 格式为 `/servercmd/speed_test/{testsize}/{time}/{key}`，其中 `testsize` 是 `additional` 的第一个字段（也是唯一字段）。
```rust
"speed_test" => {
    let size: usize = additional.parse().unwrap_or(0);
    if size > 0 && size <= 10_485_760 {
        response::speedtest_response(size)
    } else {
        response::text_response(StatusCode::OK, "")
    }
}
```
speed_test 上限 10MB，与 Java 一致。

**refresh_settings**：
```rust
"refresh_settings" => {
    match state.rpc_client.client_settings().await {
        Ok(sr) if sr.status == ResponseStatus::Ok => {
            state.config.write().unwrap().apply_server_settings(&sr.lines);
            response::text_response(StatusCode::OK, "")
        }
        _ => response::text_response(StatusCode::OK, ""),
    }
}
```

**refresh_certs**：
```rust
"refresh_certs" => {
    // 1. 重新下载 PKCS12
    // 2. 解析新证书
    // 3. 通过 Arc<RwLock<TlsAcceptor>> 热替换（需预埋基础设施）
    response::text_response(StatusCode::OK, "")
}
```

证书热替换需要：`AppState` 中加入 `tls_acceptor: Arc<RwLock<TlsAcceptor>>`，accept 循环从 RwLock 读当前 acceptor。初始实现可先完成证书下载 + 替换 acceptor，后续 accept 连接自动用新证书。

**speed_test 补充说明**：当前 `handle_server_command` 只接收 `command: &str`，没有传递完整的请求 additional。需要扩展函数签名以接收 `&str` additional。

**需修改的文件**：

| 文件 | 改动 |
|------|------|
| `src/server.rs` | `handle_server_command` 签名加 `additional` 参数；实现 4 个 command |
| `src/server.rs` | `AppState` 加 `tls_acceptor: Arc<RwLock<TlsAcceptor>>`，accept 循环读锁 |
| `src/client.rs` | 初始化 tls_acceptor |

---

## P1-4: Arc 裸指针修改 Config (UB)

### 问题

```rust
let config_ptr = Arc::as_ptr(&config) as *mut Config;
unsafe { &mut *config_ptr }.apply_server_settings(&login_resp.lines);
```

同一时刻 `RpcClient` 持有 `Arc<Config>` 的 clone。Rust 的 `&T` / `&mut T` 别名规则不允许这种访问。即使当前单线程看似安全，编译器优化可能假设不存在别名可变引用。

### Rust 修复方案

**方案：`Arc<RwLock<Config>>`**

启动阶段用写锁修改，之后全部读锁访问。同时消除 Config 内部冗余的 `RwLock` 字段。

```rust
// client.rs
let config = Arc::new(RwLock::new(config));

// 启动阶段：写锁修改
config.write().unwrap().apply_server_settings(&login_resp.lines);

// 后续：读锁访问
let host = config.read().unwrap().client_host;
let port = config.read().unwrap().client_port;
```

**Config 内部清理**：`rpc_current` 和 `rpc_last_failed` 两个 `RwLock` 字段改为普通 `Option<String>`，因为外层 `RwLock` 已提供写同步。`get_rpc_host()` / `mark_rpc_server_failure()` / `clear_rpc_server_failure()` 签名从 `&self` 改为需要外层锁已经持有时调用，或改为 `&mut self`。

**涉及文件（最大改动范围）**：

| 文件 | 改动 |
|------|------|
| `src/config.rs` | 移除内部 RwLock 字段；`get_rpc_host` / `mark_rpc_server_failure` / `clear_rpc_server_failure` 改签名 |
| `src/client.rs` | `Arc::new(RwLock::new(config))`，`config.write().unwrap()...` |
| `src/rpc_client.rs` | `config: Arc<RwLock<Config>>`，读访问加 `.read().unwrap()` |
| `src/server.rs` | `AppState::config` 类型变更，读访问加 `.read().unwrap()` |
| `src/cache/mod.rs` | `CacheHandler::config` 类型变更 |
| `src/cache/pruner.rs` | `CachePruner::config` 类型变更 |

---

## 附加-1: CachePruner 未启动

### 问题

`CachePruner` 已实现但从未实例化和启动。`prune()` 函数内部仍是 stub。

### 修复方案

1. 在 `client.rs` 启动阶段 spawn `CachePruner::run()`：
   ```rust
   let pruner = CachePruner::new(cache.clone(), config.clone(), shutdown.clone());
   tokio::spawn(async move { pruner.run().await });
   ```
2. 实现 `prune()` 逻辑：遍历 cache 目录，统计总大小，超过 `disklimit_bytes` 时从最旧的文件开始删除直到低于阈值。

---

## 附加-2: verify_cache SHA-1 校验

### 问题

`CacheHandler::full_rescan` 只校验文件大小，不校验 SHA-1。Java 版在 `verify_cache=true` 时完整 hash 校验。

### 修复方案

在 `full_rescan` 中，收集目录文件时：
```rust
if config.verify_cache {
    let data = std::fs::read(&path)?;
    let hash = utils::sha1_string(&data);
    if hash != expected_hash {
        // 删除损坏文件
        continue;
    }
}
```
注意：SHA-1 校验对大文件慢，可加进度日志。对 `use_less_memory` 模式用 `BufReader` 分块读。

---

## 附加-3: 证书过期检查

### 问题

PKCS12 证书有有效期（通常 1 年），Java 版定期检查并自动刷新。

### 修复方案

在 `server.rs` `start_server` 中，从 `keychain.certs()` 解析 `not_after` 日期。
在 24h 周期任务中检查：距过期 < 7 天时，调 `refresh_certs` 流程。

---

## 实现顺序

```
1. P0-2  服务器启动时序        ← 最小改动，防止静默启动失败
2. P1-4  Arc<RwLock<Config>>  ← 基础重构，后续 P1-3/P0-2 依赖它
3. P1-2  server_stat 格式      ← 最小改动，影响初始握手
4. P1-3  servercmd 实现        ← 依赖 P1-4
5. P1-1  带宽+连接数           ← 新功能
6. P0-1  代理回源              ← 最大改动
7. 附加-1 CachePruner 启动
8. 附加-2 SHA-1 校验
9. 附加-3 证书过期
```

理由：先修最强的依赖（P0-2 启动时序保证不静默失败，P1-4 重构为后续提供安全基础），再修协议兼容的（P1-2/P1-3），最后加独立新功能（P1-1/P0-1 工作量大）。
