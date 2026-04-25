# H@H Rust Port — 协议差距修复设计

## Context

对比 Java HentaiAtHome 1.6.5 源码 (`/Users/yukko/Code/HentaiAtHome/HentaiAtHome_1.6.5_src`)，Rust 端口存在功能性差距。本文档逐一描述 Java 行为、Rust 差距、修复方案。

**Java 关键源文件对应关系**：
- `HTTPResponse.java` → 请求解析、servercmd 分发、/t/ speedtest
- `HTTPSession.java` → HTTP 响应头构建、body streaming
- `HTTPResponseProcessor.java` → 抽象基类，默认 Content-Type = `text/html; charset=iso-8859-1`
- `HTTPResponseProcessorText.java` → 文本响应（servercmd、错误页）
- `HTTPResponseProcessorSpeedtest.java` → 速度测试（继承默认 Content-Type，8192 字节随机 buffer，分 1460 字节 TCP chunk 发送）
- `Settings.java` → 所有常量（MAX_KEY_TIME_DRIFT=300, CONTENT_TYPE_DEFAULT 等）

---

## ✅ 已完成

### P1-2: server_stat RPC 格式 (已提交)

Java: `server_stat` 不签名（`clientbuild=178&act=server_stat`），其他 action 完整签名。
修复：`Action::needs_signing()` + `make_rpc_query` 分支出未签名格式。

### ArcSwap Config 重构 (已提交，替代 P1-4)

原方案 P1-4 使用 `Arc<RwLock<Config>>`，实际实现改用 `Arc<ArcSwap<Config>>`。
- 所有 `config` 字段类型从 `Arc<Config>` 改为 `Arc<ArcSwap<Config>>`
- 读取：`config.load()` 返回 `Guard<Arc<Config>>`
- 写入：`config.rcu(|current| { let mut new = (**current).clone(); ... ; Arc::new(new) })`
- 涉及文件：config.rs, rpc_client.rs, server.rs, cache/mod.rs, cache/pruner.rs, client.rs
- Config 内部 `RwLock` 字段（`rpc_current`, `rpc_last_failed`）改为普通 `Option<String>`，通过 rcu 修改

### P1-3: servercmd 基本骨架 (未提交)

已实现 `handle_server_command` 的 7 种命令分发，但 Java 详细对比后发现若干差距需要修正。

---

## P1-3 子项: servercmd 响应兼容性修正

以下差距通过对 Java 1.6.5 `HTTPResponse.java` 逐行对比确认。

### 子项 3a: speed_test servercmd — additional 格式修正

**差距**：
- Java `additional` 是 key=value 格式（`testsize=1000000`），通过 `Tools.parseAdditional()` 解析
- Rust 错误地将整个 `additional` 字符串当原始整数解析 → 永远为 0
- Java 默认 testsize = 1,000,000；无上限

**修复**：
```rust
"speed_test" => {
    let add_table = utils::parse_additional(additional);
    let testsize: usize = add_table.get("testsize")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    response::speedtest_response(testsize)
}
```

### 子项 3b: speed_test — Content-Type 修正

**差距**：
- Java `HTTPResponseProcessorSpeedtest` 不重写 `getContentType()`，继承基类返回 `"text/html; charset=iso-8859-1"`
- Rust 使用 `"application/octet-stream"`

**修复**：`speedtest_response()` 将 content_type 从 `"application/octet-stream"` 改为 `"text/html; charset=iso-8859-1"`

### 子项 3c: threaded_proxy_test — 真实现

**Java 行为**（`HTTPResponse.processThreadedProxyTest()`）：
1. 从 additional 解析：hostname, protocol, port, testsize, testcount, testtime, testkey
2. 创建 `testcount` 个并发的 `FileDownloader`，每个请求：
   - URL: `{protocol}://{hostname}:{port}/t/{testsize}/{testtime}/{testkey}/{random_int}`
   - timeout: connect=10000ms, read=60000ms
3. 等待所有完成
4. `successfulTests` += 每个 `contentLength >= testsize` 的请求
5. `totalTimeMillis` += 每个成功的下载时间
6. 返回 `new HTTPResponseProcessorText("OK:" + successfulTests + "-" + totalTimeMillis)`

**Rust 差距**：返回硬编码 `"OK:0-0"`，未做任何出站测试

**修复**：
```rust
"threaded_proxy_test" => {
    let add_table = utils::parse_additional(additional);
    let hostname = add_table.get("hostname").map(|s| s.as_str()).unwrap_or("");
    let protocol = add_table.get("protocol").map(|s| s.as_str()).unwrap_or("http");
    let port: u16 = add_table.get("port").and_then(|v| v.parse().ok()).unwrap_or(80);
    let testsize: u64 = add_table.get("testsize").and_then(|v| v.parse().ok()).unwrap_or(1000000);
    let testcount: u32 = add_table.get("testcount").and_then(|v| v.parse().ok()).unwrap_or(1);
    let testtime = add_table.get("testtime").map(|s| s.as_str()).unwrap_or("0");
    let testkey = add_table.get("testkey").map(|s| s.as_str()).unwrap_or("");

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| HathError::Network(e.to_string()))?;

    let mut tasks = Vec::new();
    for _ in 0..testcount {
        let random: u32 = rand::rng().next_u32() & 0x7FFFFFFF;  // positive int
        let url = format!("{}://{}:{}/t/{}/{}/{}/{}",
            protocol, hostname, port, testsize, testtime, testkey, random);
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let start = Instant::now();
            match client.get(&url).send().await {
                Ok(resp) => {
                    let len = resp.content_length().unwrap_or(0);
                    let elapsed = start.elapsed().as_millis() as u64;
                    if len >= testsize { Some(elapsed) } else { None }
                }
                Err(_) => None,
            }
        }));
    }

    let mut successful = 0u32;
    let mut total_ms = 0u64;
    for task in tasks {
        if let Ok(Some(elapsed)) = task.await {
            successful += 1;
            total_ms += elapsed;
        }
    }

    response::text_response(StatusCode::OK,
        &format!("OK:{}-{}", successful, total_ms))
}
```

注意：用 `reqwest`（已在 Cargo.toml 中），不需要额外依赖。

### 子项 3d: refresh_certs — 响应体修正

**差距**：
- Java `client.setCertRefresh()` 只设置 boolean 标志 → 返回 `new HTTPResponseProcessorText("")`（空 body，200 OK）
- Rust 返回 `"OK"` / `"FAIL"` 文本 body

**修复**：
```rust
"refresh_certs" => {
    let cfg = state.config.load_full();
    let state_clone = state.clone();
    tokio::spawn(async move {
        match build_tls_acceptor(&cfg, true).await {
            Ok(new_acceptor) => {
                state_clone.tls_acceptor.store(Some(Arc::new(new_acceptor)));
                tracing::info!("Certificate refreshed successfully");
            }
            Err(e) => {
                tracing::error!("Failed to refresh certificate: {}", e);
            }
        }
    });
    response::text_response(StatusCode::OK, "")
}
```

---

## 响应头兼容性

对比 Java `HTTPSession.java` 第 127-141 行逐字段确认。

### 原则

Java 响应头构建逻辑：
```
Date / Server / Connection: close / Content-Type    ← 始终存在
Cache-Control + Content-Length                      ← 仅当 contentLength > 0
```

Java **不**给空 body 响应加 `Cache-Control` 和 `Content-Length`。

### 响应头修正

**`ok_response()`** — Cache-Control + Content-Length 改为条件性（仅 len > 0）

**`text_response()`** — 同样条件性添加 Cache-Control + Content-Length（仅 len > 0）。
之前 `text_response` 从不加 Cache-Control，即使 body 有内容也不加——导致 `still_alive`/`INVALID_COMMAND` 等响应缺 Cache-Control。

**`redirect_response()`** — 去掉 `Content-Length: 0`（Java 空 body 不设 Content-Length）

### 状态码

| 场景 | Java | Rust 修复前 | Rust 修复后 |
|------|------|------------|------------|
| `/t/` URL < 5 段 | 400 | 403 | 400 |
| `/t/` 过期/无效 key | 403 | 403 | 403 ✅ |
| 403 原因短语 | "Permission Denied" | "Forbidden" | ⚠️ 无改动（hyper 默认值，客户端不依赖原因短语） |

区分方式：`RequestType::SpeedTest` 增加 `forbidden: bool` 字段：
- `forbidden=true` → 403（key 无效）
- `forbidden=false`（且 `valid=false`）→ 400（URL 格式错误）

### 其他已验证一致

| 项目 | Java | Rust | 状态 |
|------|-----|------|------|
| `still_alive` 响应文本 | "I feel FANTASTIC and I'm still alive" | 同 | ✅ |
| `refresh_settings` body | 空 | 空 | ✅ |
| `start_downloader` body | 空 | 空 | ✅ |
| `INVALID_COMMAND` | "INVALID_COMMAND" | 同 | ✅ |
| MAX_KEY_TIME_DRIFT | 300s | 300s | ✅ |
| Server header | "Genetic Lifeform and Distributed Open Server 1.6.5" | 同 | ✅ |
| Date header 格式 | `EEE, dd MMM yyyy HH:mm:ss GMT` | chrono `"%a, %d %b %Y %H:%M:%S GMT"` | ✅ |
| Speedtest SHA-1 公式 | `hentai@home-speedtest-{size}-{time}-{cid}-{key}` | 同 | ✅ |
| Servercmd SHA-1 公式 | `hentai@home-servercmd-{cmd}-{add}-{cid}-{time}-{key}` | 同 | ✅ |

---

## P0-2: 服务器启动时序

（内容不变，已实现）

---

## P1-1: 带宽限制与连接数控制

（内容不变，尚未实现）

---

## P0-1: 缓存未命中代理回源

（内容不变，尚未实现）

---

## 附加项

### 附加-1: CachePruner 启动

（内容不变，尚未实现）

### 附加-2: verify_cache SHA-1 校验

（内容不变，尚未实现）

### 附加-3: 证书过期检查

（内容不变，尚未实现）

---

## 当前实现状态总览

| 项目 | 状态 | 说明 |
|------|------|------|
| P1-2 server_stat 格式 | ✅ 已提交 | Action::needs_signing() |
| ArcSwap Config 重构 | ✅ 已提交 | 替代原 P1-4 方案 |
| P1-3 servercmd 基本骨架 | 🔧 已实现未提交 | 需 3a/3b/3c/3d 修正 |
| P1-3 子项 3a speed_test additional | 🔧 已实现未提交 | parse_additional + testsize 提取 |
| P1-3 子项 3b speed_test Content-Type | 🔧 已实现未提交 | text/html; charset=iso-8859-1 |
| P1-3 子项 3c threaded_proxy_test | ❌ 未实现 | 需真出站 HTTP 测试 |
| P1-3 子项 3d refresh_certs 响应体 | 🔧 已实现未提交 | 空 body + async spawn |
| 响应头 Cache-Control 条件化 | 🔧 已实现未提交 | ok_response / text_response |
| 响应头 Content-Length 条件化 | 🔧 已实现未提交 | 同上 + redirect_response |
| /t/ URL 400 vs 403 | 🔧 已实现未提交 | SpeedTest.forbidden 字段 |
| P0-2 启动时序 | ✅ 已提交 | oneshot channel |
| P1-1 带宽+连接数 | ❌ 未实现 | |
| P0-1 代理回源 | ❌ 未实现 | |
| 附加-1 CachePruner | ❌ 未实现 | |
| 附加-2 SHA-1 校验 | ❌ 未实现 | |
| 附加-3 证书过期 | ❌ 未实现 | |

---

## 实现顺序

```
 1. [DONE]  P0-2  服务器启动时序
 2. [DONE]  ArcSwap Config 重构
 3. [DONE]  P1-2  server_stat RPC 格式
 4. [NOW]   P1-3  完成所有 servercmd + 响应头兼容修正 → 提交
 5.          P1-1  带宽限制 + 连接数控制
 6.          P0-1  缓存未命中代理回源
 7.          附加-1 CachePruner 启动
 8.          附加-2 SHA-1 校验
 9.          附加-3 证书过期检查
```
