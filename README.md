# Kalshi WebSocket Speed Test

A Rust program to benchmark and test the speed/latency of the Kalshi WebSocket API.

## Overview

`kalshi-ws-bench` is a command-line tool that measures various performance characteristics of the Kalshi trading WebSocket API:

| Test | What it measures |
|------|-----------------|
| **connect** | TCP + TLS + WebSocket handshake time (5 attempts → min/max/mean/p99) |
| **subscribe** | Time from sending a `subscribe` command to receiving the first data frame |
| **throughput** | Number of messages received and KB/s over a configurable time window |
| **roundtrip** | WebSocket ping → pong round-trip latency (10 pings → statistics) |
| **multi** | N concurrent connections opened in parallel, individual + aggregate stats |

## Prerequisites

- [Rust toolchain ≥ 1.70](https://rustup.rs/) (`rustc`, `cargo`)
- OpenSSL development libraries (used by `native-tls`)
  - **Ubuntu/Debian:** `sudo apt install libssl-dev pkg-config`
  - **macOS:** included with Xcode Command Line Tools
  - **Windows:** install [vcpkg](https://github.com/microsoft/vcpkg) and `vcpkg install openssl`

## Build

```bash
# Debug build
cargo build

# Optimised release build (recommended for benchmarking)
cargo build --release
```

The binary is placed at `target/release/kalshi-ws-bench` (or `target/debug/…` for debug builds).

## Usage

```
kalshi-ws-bench [OPTIONS]

Options:
  -e, --endpoint <ENDPOINT>          API endpoint: 'prod' or 'demo' [default: demo]
  -k, --api-key <API_KEY>            Kalshi API key ID (required for private channels)
  -p, --private-key-path <PATH>      Path to RSA private key PEM file (required for private channels)
  -m, --market-ticker <TICKER>       Market ticker to subscribe to [default: AAAA-11]
  -d, --duration <SECS>              Duration in seconds for the throughput test [default: 10]
  -n, --connections <N>              Number of concurrent connections for multi-connection test [default: 5]
  -t, --test <TEST>                  Which test to run [default: all]
                                     Options: all, connect, subscribe, throughput, roundtrip, multi
  -h, --help                         Print help
  -V, --version                      Print version
```

### Examples

**Run all tests against the demo endpoint (default):**
```bash
./target/release/kalshi-ws-bench
```

**Run against production with a specific market ticker:**
```bash
./target/release/kalshi-ws-bench --endpoint prod --market-ticker INXD-23DEC31-B5000
```

**Run only the connection-speed test:**
```bash
./target/release/kalshi-ws-bench --test connect
```

**Run the throughput test for 30 seconds:**
```bash
./target/release/kalshi-ws-bench --test throughput --duration 30 --market-ticker INXD-23DEC31-B5000
```

**Test 20 concurrent connections:**
```bash
./target/release/kalshi-ws-bench --test multi --connections 20
```

**Authenticate with an API key and private key (enables private channels):**
```bash
./target/release/kalshi-ws-bench \
  --endpoint prod \
  --api-key your-api-key-id \
  --private-key-path /path/to/private_key.pem \
  --market-ticker INXD-23DEC31-B5000
```

## Tests Explained

### 1. Connection Speed Test (`--test connect`)

Connects to the WebSocket endpoint 5 times and measures the full handshake time (TCP connection + TLS negotiation + WebSocket upgrade). The connection is closed immediately after the handshake completes. Reports min, max, mean, and p99 latency.

### 2. Subscription Latency Test (`--test subscribe`)

Establishes a connection, sends a `ticker` channel subscription command, and measures how long it takes to receive the first message from the server. This measures the end-to-end latency of the subscribe flow including server processing time.

> **Note:** If the specified market ticker is not currently active, no data will be published and the test will time out (30 s). Use an active market ticker for a meaningful result.

### 3. Message Throughput Test (`--test throughput`)

Subscribes to the `ticker` channel and counts how many messages arrive during the configured `--duration` window. Reports total message count, elapsed time, messages per second, and data rate in KB/s.

### 4. Round-Trip Latency Test (`--test roundtrip`)

Sends 10 WebSocket `PING` frames and measures the time until the corresponding `PONG` frames are received. This is the lowest-level measure of network round-trip time to the Kalshi server.

> **Note:** Not all servers respond to WebSocket ping frames at the application level. If no pong responses are received this test will report a warning.

### 5. Multiple Concurrent Connections Test (`--test multi`)

Opens `--connections` WebSocket connections simultaneously using Tokio tasks. Measures each individual handshake latency plus the total wall-clock time for all connections to complete. Useful for testing connection-pool scenarios.

## API Keys and Private Channels

Public channels (`ticker`, `trade`, `market_lifecycle_v2`, `multivariate`) do not require authentication.

For private channels (`orderbook_delta`, `fill`, `market_positions`, etc.) you need:

1. A Kalshi API key ID (`--api-key`)
2. The corresponding RSA private key in PEM format (`--private-key-path`)

The tool accepts both PKCS#8 (`-----BEGIN PRIVATE KEY-----`) and PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`) formats.

Authentication uses RSASSA-PKCS1-v1_5 with SHA-256. The signed payload is:
```
{timestamp_ms}GET/trade-api/ws/v2
```
The headers `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, and `KALSHI-ACCESS-TIMESTAMP` are injected into the WebSocket HTTP upgrade request.

## Example Output

```
╔══════════════════════════════════════════════════════════════╗
║      Kalshi WebSocket Speed Test & Benchmark v0.1.0         ║
╚══════════════════════════════════════════════════════════════╝
  Endpoint : wss://demo-api.kalshi.co/trade-api/ws/v2
  Market   : AAAA-11
  Auth     : Disabled — public channels only

────────────────────────────────────────────────────────────────
  🔌  Connection Speed Test
────────────────────────────────────────────────────────────────
  Endpoint  : wss://demo-api.kalshi.co/trade-api/ws/v2
  Attempts  : 5
  Attempt  1 : 142.53 ms
  Attempt  2 : 138.21 ms
  Attempt  3 : 135.87 ms
  Attempt  4 : 141.04 ms
  Attempt  5 : 139.66 ms

  Connection handshake latency
    Count:                             5
    Min:                               135.87 ms
    Max:                               142.53 ms
    Mean:                              139.46 ms
    P99:                               142.53 ms

────────────────────────────────────────────────────────────────
  🏓  Round-Trip Latency Test  (Ping / Pong)
────────────────────────────────────────────────────────────────
  Pings : 10
  Ping  1 : 141.22 ms
  Ping  2 : 139.88 ms
  ...

────────────────────────────────────────────────────────────────
  📋  Summary
────────────────────────────────────────────────────────────────
  Connection mean:                       139.46 ms
  Connection p99:                        142.53 ms
  Round-trip mean:                       140.31 ms
  Round-trip p99:                        141.22 ms
```

## API Reference

- [Kalshi WebSocket API documentation](https://trading-api.readme.io/reference/websocket)
- Production endpoint: `wss://trading-api.kalshi.com/trade-api/ws/v2`
- Demo/Sandbox endpoint: `wss://demo-api.kalshi.co/trade-api/ws/v2`
