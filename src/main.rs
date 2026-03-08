//! Kalshi WebSocket Speed Test & Benchmark
//!
//! Measures connection latency, subscription latency, message throughput,
//! round-trip ping/pong latency, and concurrent-connection performance
//! against the Kalshi trading WebSocket API.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use colored::Colorize;
use futures_util::{SinkExt, StreamExt};
use rsa::{
    pkcs1::DecodeRsaPrivateKey,
    pkcs1v15::SigningKey,
    pkcs8::DecodePrivateKey,
    signature::{Signer, SignatureEncoding},
    RsaPrivateKey,
};
use sha2::Sha256;
use std::time::{Duration, Instant};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

// ─── Constants ───────────────────────────────────────────────────────────────

const PROD_ENDPOINT: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const DEMO_ENDPOINT: &str = "wss://demo-api.kalshi.co/trade-api/ws/v2";

/// Placeholder ticker used when none is specified on the CLI
const DEFAULT_MARKET_TICKER: &str = "AAAA-11";
const DEFAULT_DURATION_SECS: u64 = 10;
const DEFAULT_CONNECTIONS: usize = 5;

/// How long to wait for a single connection / first-message before giving up
const TEST_TIMEOUT_SECS: u64 = 30;
/// How many connection attempts are made by the connect-speed test
const CONNECT_ATTEMPTS: usize = 5;
/// How many ping/pong round-trips to measure
const PING_COUNT: usize = 10;
/// Per-ping timeout (seconds)
const PING_TIMEOUT_SECS: u64 = 5;

// ─── Type alias ──────────────────────────────────────────────────────────────

/// Concrete WebSocket stream type returned by `connect_async` with native-TLS
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ─── CLI ─────────────────────────────────────────────────────────────────────

/// Which API endpoint to target
#[derive(Debug, Clone, ValueEnum)]
enum EndpointChoice {
    /// Production: wss://api.elections.kalshi.com/trade-api/ws/v2
    Prod,
    /// Demo/Sandbox: wss://demo-api.kalshi.co/trade-api/ws/v2
    Demo,
}

/// Which benchmark suite to run
#[derive(Debug, Clone, ValueEnum)]
enum TestChoice {
    /// Run every test in sequence
    All,
    /// Only the connection-speed test
    Connect,
    /// Only the subscription-latency test
    Subscribe,
    /// Only the message-throughput test
    Throughput,
    /// Only the ping/pong round-trip test
    Roundtrip,
    /// Only the concurrent-connections test
    Multi,
}

#[derive(Debug, Parser)]
#[command(
    name = "kalshi-ws-bench",
    about = "Benchmark and test the speed/latency of the Kalshi WebSocket API",
    version
)]
struct Cli {
    /// API endpoint: 'prod' or 'demo'
    #[arg(short = 'e', long, default_value = "demo")]
    endpoint: EndpointChoice,

    /// Kalshi API key ID (required for private channels)
    #[arg(short = 'k', long)]
    api_key: Option<String>,

    /// Path to RSA private key PEM file (required for private channels)
    #[arg(short = 'p', long)]
    private_key_path: Option<String>,

    /// Market ticker to subscribe to (e.g. AAAA-11)
    #[arg(short = 'm', long, default_value = DEFAULT_MARKET_TICKER)]
    market_ticker: String,

    /// Duration in seconds for the throughput test
    #[arg(short = 'd', long, default_value_t = DEFAULT_DURATION_SECS)]
    duration: u64,

    /// Number of concurrent connections for the multi-connection test
    #[arg(short = 'n', long, default_value_t = DEFAULT_CONNECTIONS)]
    connections: usize,

    /// Which test to run
    #[arg(short = 't', long, default_value = "all")]
    test: TestChoice,
}

// ─── Runtime config ──────────────────────────────────────────────────────────

/// Fully resolved runtime configuration (built from `Cli`)
struct Config {
    endpoint: String,
    api_key: Option<String>,
    /// Loaded RSA private key, present only when `--private-key-path` was given
    private_key: Option<RsaPrivateKey>,
    market_ticker: String,
    duration: Duration,
    connections: usize,
    test: TestChoice,
}

// ─── Statistics ──────────────────────────────────────────────────────────────

/// Summary statistics computed from a sample of latency measurements
struct Stats {
    min: Duration,
    max: Duration,
    mean: Duration,
    /// 99th-percentile latency
    p99: Duration,
    count: usize,
}

impl Stats {
    /// Compute statistics from an (unsorted) vector of durations.
    /// Returns `None` if the vector is empty.
    fn compute(mut latencies: Vec<Duration>) -> Option<Self> {
        if latencies.is_empty() {
            return None;
        }
        latencies.sort_unstable();
        let count = latencies.len();
        let min = latencies[0];
        let max = latencies[count - 1];
        let sum: Duration = latencies.iter().sum();
        let mean = sum / count as u32;
        // Ceiling index so that at least 99 % of values are ≤ p99
        let p99_idx = ((count as f64 * 0.99).ceil() as usize)
            .saturating_sub(1)
            .min(count - 1);
        let p99 = latencies[p99_idx];
        Some(Stats {
            min,
            max,
            mean,
            p99,
            count,
        })
    }
}

// ─── Display helpers ─────────────────────────────────────────────────────────

/// Format a `Duration` as a human-readable string (µs / ms / s)
fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    if micros < 1_000 {
        format!("{} µs", micros)
    } else if micros < 1_000_000 {
        format!("{:.2} ms", d.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3} s", d.as_secs_f64())
    }
}

/// Print a bold, blue-bordered section header
fn print_header(title: &str) {
    println!();
    println!("{}", "─".repeat(64).bright_blue());
    println!("  {}", title.bold());
    println!("{}", "─".repeat(64).bright_blue());
}

/// Print a single key-value result row
fn print_result(label: &str, value: &str) {
    println!("  {:<38} {}", label, value.cyan());
}

/// Print a full `Stats` block with min / max / mean / p99
fn print_stats(label: &str, stats: &Stats) {
    println!("  {}", label.bold().underline());
    println!("    {:<34} {}", "Count:", stats.count.to_string().cyan());
    println!(
        "    {:<34} {}",
        "Min:",
        format_duration(stats.min).green()
    );
    println!(
        "    {:<34} {}",
        "Max:",
        format_duration(stats.max).yellow()
    );
    println!(
        "    {:<34} {}",
        "Mean:",
        format_duration(stats.mean).cyan()
    );
    println!(
        "    {:<34} {}",
        "P99:",
        format_duration(stats.p99).yellow()
    );
}

// ─── Auth helpers ─────────────────────────────────────────────────────────────

/// Load an RSA private key from a PEM file.
/// Attempts PKCS#8 format first, then falls back to PKCS#1 (traditional) format.
fn load_private_key(path: &str) -> Result<RsaPrivateKey> {
    let pem = std::fs::read_to_string(path)
        .with_context(|| format!("Cannot read private key file '{}'", path))?;

    // Try PKCS#8 PEM (-----BEGIN PRIVATE KEY-----)
    RsaPrivateKey::from_pkcs8_pem(&pem)
        // Fall back to PKCS#1 PEM (-----BEGIN RSA PRIVATE KEY-----)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
        .context("Failed to parse RSA private key (tried PKCS#8 and PKCS#1 formats)")
}

/// Build the three Kalshi authentication headers required for private channels.
///
/// The signature is: RSASSA-PKCS1-v1_5(SHA-256, `{timestamp}GET/trade-api/ws/v2`)
fn create_auth_headers(api_key: &str, private_key: &RsaPrivateKey) -> Result<Vec<(String, String)>> {
    let timestamp = Utc::now().timestamp_millis().to_string();
    // Kalshi signature payload: timestamp + HTTP-method + path
    let message = format!("{}GET/trade-api/ws/v2", timestamp);

    let signing_key = SigningKey::<Sha256>::new(private_key.clone());
    let signature = signing_key.sign(message.as_bytes());
    let encoded = BASE64.encode(signature.to_bytes().as_ref());

    Ok(vec![
        ("KALSHI-ACCESS-KEY".to_string(), api_key.to_string()),
        ("KALSHI-ACCESS-SIGNATURE".to_string(), encoded),
        ("KALSHI-ACCESS-TIMESTAMP".to_string(), timestamp),
    ])
}

// ─── WebSocket connection helper ─────────────────────────────────────────────

/// Build a JSON subscription command for the given channel and market ticker.
fn build_subscribe_msg(channel: &str, market_ticker: &str) -> String {
    serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": [channel],
            "market_tickers": [market_ticker]
        }
    })
    .to_string()
}

/// Connect to the WebSocket endpoint, optionally injecting Kalshi auth headers.
///
/// Returns the open `WsStream` together with the wall-clock duration of the
/// entire TCP + TLS + WebSocket handshake.
async fn connect_timed(
    endpoint: &str,
    auth_headers: Option<&[(String, String)]>,
) -> Result<(WsStream, Duration)> {
    let start = Instant::now();

    // Build an HTTP upgrade request from the WSS URL
    let mut request = endpoint
        .into_client_request()
        .context("Failed to build WebSocket upgrade request")?;

    // Inject optional authentication headers into the HTTP handshake
    if let Some(headers) = auth_headers {
        let header_map = request.headers_mut();
        for (key, value) in headers {
            let name = http::header::HeaderName::from_bytes(key.as_bytes())
                .with_context(|| format!("Invalid header name '{}'", key))?;
            let val = http::header::HeaderValue::from_str(value)
                .with_context(|| format!("Invalid value for header '{}'", key))?;
            header_map.insert(name, val);
        }
    }

    let (ws_stream, _response) = connect_async(request)
        .await
        .context("WebSocket connect failed")?;

    Ok((ws_stream, start.elapsed()))
}

// ─── Test 1 – Connection speed ────────────────────────────────────────────────

/// Measure the wall-clock time to complete the TCP + TLS + WebSocket handshake.
/// Repeats `CONNECT_ATTEMPTS` times so we can report statistics.
async fn run_connect_test(config: &Config) -> Result<Vec<Duration>> {
    print_header("🔌  Connection Speed Test");
    println!("  Endpoint  : {}", config.endpoint.yellow());
    println!("  Attempts  : {}", CONNECT_ATTEMPTS);

    let mut latencies = Vec::new();

    for attempt in 1..=CONNECT_ATTEMPTS {
        let auth_headers = match (config.api_key.as_deref(), config.private_key.as_ref()) {
            (Some(k), Some(pk)) => Some(create_auth_headers(k, pk)?),
            _ => None,
        };

        let result = tokio::time::timeout(
            Duration::from_secs(TEST_TIMEOUT_SECS),
            connect_timed(&config.endpoint, auth_headers.as_deref()),
        )
        .await;

        match result {
            Ok(Ok((mut ws, elapsed))) => {
                // Close immediately – we only care about handshake time
                let _ = ws.send(Message::Close(None)).await;
                latencies.push(elapsed);
                println!(
                    "  Attempt {:2} : {}",
                    attempt,
                    format_duration(elapsed).green()
                );
            }
            Ok(Err(e)) => {
                println!("  Attempt {:2} : {} — {}", attempt, "FAILED".red(), e);
            }
            Err(_) => {
                println!("  Attempt {:2} : {}", attempt, "TIMEOUT".red());
            }
        }
    }

    if let Some(stats) = Stats::compute(latencies.clone()) {
        println!();
        print_stats("Connection handshake latency", &stats);
    } else {
        println!("  {}", "No successful connections.".red());
    }

    Ok(latencies)
}

// ─── Test 2 – Subscription latency ───────────────────────────────────────────

/// Measure the time from sending a `subscribe` command to receiving the first
/// data message from the server on the `ticker` channel.
async fn run_subscribe_test(config: &Config) -> Result<Option<Duration>> {
    print_header("📨  Subscription Latency Test");
    println!("  Channel : ticker");
    println!("  Market  : {}", config.market_ticker.yellow());

    let auth_headers = match (config.api_key.as_deref(), config.private_key.as_ref()) {
        (Some(k), Some(pk)) => Some(create_auth_headers(k, pk)?),
        _ => None,
    };

    let (mut ws, connect_time) = tokio::time::timeout(
        Duration::from_secs(TEST_TIMEOUT_SECS),
        connect_timed(&config.endpoint, auth_headers.as_deref()),
    )
    .await
    .context("Connection timed out")??;

    println!("  Connected in : {}", format_duration(connect_time).green());

    // Start the clock, then send the subscription command
    let subscribe_msg = build_subscribe_msg("ticker", &config.market_ticker);
    let start = Instant::now();
    ws.send(Message::Text(subscribe_msg.into()))
        .await
        .context("Failed to send subscribe message")?;

    // Wait for the first text or binary data frame
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(TEST_TIMEOUT_SECS);

    let first_msg_latency = loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(_)))) | Ok(Some(Ok(Message::Binary(_)))) => {
                break Some(start.elapsed());
            }
            // Ignore control frames (ping/pong/close)
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => {
                eprintln!("  WebSocket error: {}", e);
                break None;
            }
            // Stream ended
            Ok(None) => break None,
            // Deadline exceeded
            Err(_) => {
                println!(
                    "  {} Timed out waiting for first message.",
                    "⚠".yellow()
                );
                println!(
                    "  Note: '{}' may not be an active market ticker.",
                    config.market_ticker
                );
                break None;
            }
        }
    };

    let _ = ws.send(Message::Close(None)).await;

    match first_msg_latency {
        Some(lat) => {
            println!();
            print_result(
                "Subscribe → first message:",
                &format_duration(lat),
            );
            Ok(Some(lat))
        }
        None => {
            println!("  {}", "No data message received.".yellow());
            Ok(None)
        }
    }
}

// ─── Test 3 – Message throughput ─────────────────────────────────────────────

/// Subscribe to the `ticker` channel and count how many messages arrive during
/// the configured duration window.
async fn run_throughput_test(config: &Config) -> Result<(usize, f64)> {
    print_header("📊  Message Throughput Test");
    println!("  Duration : {}s", config.duration.as_secs());
    println!("  Channel  : ticker  |  Market: {}", config.market_ticker.yellow());

    let auth_headers = match (config.api_key.as_deref(), config.private_key.as_ref()) {
        (Some(k), Some(pk)) => Some(create_auth_headers(k, pk)?),
        _ => None,
    };

    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(TEST_TIMEOUT_SECS),
        connect_timed(&config.endpoint, auth_headers.as_deref()),
    )
    .await
    .context("Connection timed out")??;

    let subscribe_msg = build_subscribe_msg("ticker", &config.market_ticker);
    ws.send(Message::Text(subscribe_msg.into()))
        .await
        .context("Failed to send subscribe message")?;

    let mut message_count: usize = 0;
    let mut byte_count: usize = 0;
    let wall_start = Instant::now();

    // Use a sleep future as a deadline; use select! so we can also read messages
    let timer = tokio::time::sleep(config.duration);
    tokio::pin!(timer);

    loop {
        tokio::select! {
            biased;
            // Stop when the duration window closes
            _ = &mut timer => break,
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        message_count += 1;
                        byte_count += text.len();
                    }
                    Some(Ok(Message::Binary(data))) => {
                        message_count += 1;
                        byte_count += data.len();
                    }
                    Some(Ok(_)) => {} // control frames – ignore
                    Some(Err(e)) => {
                        eprintln!("  WebSocket error: {}", e);
                        break;
                    }
                    None => break, // stream closed
                }
            }
        }
    }

    let elapsed = wall_start.elapsed();
    let _ = ws.send(Message::Close(None)).await;

    let msg_per_sec = message_count as f64 / elapsed.as_secs_f64();
    let kbps = (byte_count as f64 / elapsed.as_secs_f64()) / 1_024.0;

    println!();
    print_result("Total messages received:", &message_count.to_string());
    print_result("Elapsed time:", &format_duration(elapsed));
    print_result("Throughput:", &format!("{:.2} msg/s", msg_per_sec));
    print_result("Data rate:", &format!("{:.2} KB/s", kbps));

    if message_count == 0 {
        println!(
            "  {} No messages received — '{}' may be an inactive ticker.",
            "⚠".yellow(),
            config.market_ticker
        );
    }

    Ok((message_count, msg_per_sec))
}

// ─── Test 4 – Round-trip latency (ping / pong) ────────────────────────────────

/// Send `PING_COUNT` WebSocket ping frames and measure the time until the
/// corresponding pong frames arrive.
async fn run_roundtrip_test(config: &Config) -> Result<Vec<Duration>> {
    print_header("🏓  Round-Trip Latency Test  (Ping / Pong)");
    println!("  Pings : {}", PING_COUNT);

    let auth_headers = match (config.api_key.as_deref(), config.private_key.as_ref()) {
        (Some(k), Some(pk)) => Some(create_auth_headers(k, pk)?),
        _ => None,
    };

    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(TEST_TIMEOUT_SECS),
        connect_timed(&config.endpoint, auth_headers.as_deref()),
    )
    .await
    .context("Connection timed out")??;

    let mut latencies = Vec::new();

    for i in 1..=PING_COUNT {
        let ping_payload = format!("kalshi-bench-{}", i).into_bytes();
        let start = Instant::now();

        // Send the ping frame
        if let Err(e) = ws.send(Message::Ping(ping_payload.into())).await {
            println!("  Ping {:2} : {} — {}", i, "send error".red(), e);
            break;
        }

        // Wait for a pong, skipping any data frames that arrive first
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(PING_TIMEOUT_SECS);

        let got_pong = loop {
            match tokio::time::timeout_at(deadline, ws.next()).await {
                Ok(Some(Ok(Message::Pong(_)))) => break true,
                Ok(Some(Ok(_))) => continue, // skip data / other control frames
                Ok(Some(Err(e))) => {
                    eprintln!("  WebSocket error: {}", e);
                    break false;
                }
                Ok(None) => break false, // stream closed
                Err(_) => break false,   // timeout
            }
        };

        if got_pong {
            let rtt = start.elapsed();
            latencies.push(rtt);
            println!("  Ping {:2} : {}", i, format_duration(rtt).green());
        } else {
            println!("  Ping {:2} : {}", i, "no pong / timeout".yellow());
        }

        // Brief pause between pings so we don't flood the server
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = ws.send(Message::Close(None)).await;

    println!();
    if let Some(stats) = Stats::compute(latencies.clone()) {
        print_stats("Ping / Pong round-trip latency", &stats);
    } else {
        println!(
            "  {} No pong responses received — the server may not support WebSocket ping frames.",
            "⚠".yellow()
        );
    }

    Ok(latencies)
}

// ─── Test 5 – Concurrent connections ─────────────────────────────────────────

/// Open `config.connections` WebSocket connections concurrently and measure
/// each individual handshake latency, plus the total wall-clock time.
async fn run_multi_connection_test(config: &Config) -> Result<Vec<Duration>> {
    print_header("🔗  Multiple Concurrent Connections Test");
    println!("  Connections : {}", config.connections);
    println!("  Endpoint    : {}", config.endpoint.yellow());

    let wall_start = Instant::now();

    // Spawn all connections in parallel
    let mut tasks = Vec::with_capacity(config.connections);
    for i in 0..config.connections {
        let endpoint = config.endpoint.clone();
        let api_key = config.api_key.clone();
        let private_key = config.private_key.clone();

        tasks.push(tokio::spawn(async move {
            // Build auth headers inside the spawned task (each needs its own timestamp)
            let auth_headers = match (api_key.as_deref(), private_key.as_ref()) {
                (Some(k), Some(pk)) => create_auth_headers(k, pk).ok(),
                _ => None,
            };

            let result = tokio::time::timeout(
                Duration::from_secs(TEST_TIMEOUT_SECS),
                connect_timed(&endpoint, auth_headers.as_deref()),
            )
            .await;

            match result {
                Ok(Ok((mut ws, elapsed))) => {
                    let _ = ws.send(Message::Close(None)).await;
                    (i + 1, Ok(elapsed))
                }
                Ok(Err(e)) => (i + 1, Err(format!("{}", e))),
                Err(_) => (i + 1, Err("timeout".to_string())),
            }
        }));
    }

    // Collect results as tasks complete
    let mut latencies = Vec::new();
    let mut failures: usize = 0;

    for task in tasks {
        match task.await {
            Ok((conn_id, Ok(elapsed))) => {
                println!(
                    "  Connection {:3} : {}",
                    conn_id,
                    format_duration(elapsed).green()
                );
                latencies.push(elapsed);
            }
            Ok((conn_id, Err(e))) => {
                println!("  Connection {:3} : {} — {}", conn_id, "FAILED".red(), e);
                failures += 1;
            }
            Err(e) => {
                eprintln!("  Task panicked: {}", e);
                failures += 1;
            }
        }
    }

    let total_wall = wall_start.elapsed();

    println!();
    print_result(
        "Total wall-clock time:",
        &format_duration(total_wall),
    );
    print_result(
        "Successful connections:",
        &format!("{}/{}", latencies.len(), config.connections),
    );
    if failures > 0 {
        print_result("Failed connections:", &failures.to_string());
    }

    if let Some(stats) = Stats::compute(latencies.clone()) {
        println!();
        print_stats("Individual connection latency", &stats);
    }

    Ok(latencies)
}

// ─── Summary ─────────────────────────────────────────────────────────────────

/// Print a combined one-page summary of all test results.
fn print_summary(
    connect_lats: Option<&Vec<Duration>>,
    subscribe_lat: Option<Option<Duration>>,
    throughput: Option<(usize, f64)>,
    roundtrip_lats: Option<&Vec<Duration>>,
    multi_lats: Option<&Vec<Duration>>,
) {
    print_header("📋  Summary");

    if let Some(lats) = connect_lats {
        if let Some(s) = Stats::compute(lats.clone()) {
            print_result("Connection mean:", &format_duration(s.mean));
            print_result("Connection p99:", &format_duration(s.p99));
        }
    }

    if let Some(Some(lat)) = subscribe_lat {
        print_result("Subscribe → first message:", &format_duration(lat));
    }

    if let Some((count, rate)) = throughput {
        print_result("Messages received:", &count.to_string());
        print_result("Throughput:", &format!("{:.2} msg/s", rate));
    }

    if let Some(lats) = roundtrip_lats {
        if let Some(s) = Stats::compute(lats.clone()) {
            print_result("Round-trip mean:", &format_duration(s.mean));
            print_result("Round-trip p99:", &format_duration(s.p99));
        }
    }

    if let Some(lats) = multi_lats {
        if let Some(s) = Stats::compute(lats.clone()) {
            print_result(
                &format!("Multi-conn ({} conns) mean:", lats.len()),
                &format_duration(s.mean),
            );
        }
    }

    println!();
    println!("{}", "─".repeat(64).bright_blue());
    println!();
}

// ─── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Resolve the WSS endpoint URL
    let endpoint = match cli.endpoint {
        EndpointChoice::Prod => PROD_ENDPOINT.to_string(),
        EndpointChoice::Demo => DEMO_ENDPOINT.to_string(),
    };

    // Load the RSA private key if a path was provided
    let private_key = if let Some(ref path) = cli.private_key_path {
        Some(load_private_key(path)?)
    } else {
        None
    };

    // Warn about mismatched auth arguments
    if cli.api_key.is_some() && private_key.is_none() {
        eprintln!(
            "{} --api-key provided without --private-key-path; auth headers will be skipped.",
            "⚠ Warning:".yellow()
        );
    }
    if cli.api_key.is_none() && private_key.is_some() {
        eprintln!(
            "{} --private-key-path provided without --api-key; auth headers will be skipped.",
            "⚠ Warning:".yellow()
        );
    }

    let config = Config {
        endpoint,
        api_key: cli.api_key,
        private_key,
        market_ticker: cli.market_ticker,
        duration: Duration::from_secs(cli.duration),
        connections: cli.connections,
        test: cli.test,
    };

    // ── Banner ──────────────────────────────────────────────────────────────
    println!();
    println!(
        "{}",
        "╔══════════════════════════════════════════════════════════════╗"
            .bright_blue()
    );
    println!(
        "{}",
        "║      Kalshi WebSocket Speed Test & Benchmark v0.1.0         ║"
            .bright_blue()
    );
    println!(
        "{}",
        "╚══════════════════════════════════════════════════════════════╝"
            .bright_blue()
    );
    println!("  Endpoint : {}", config.endpoint.yellow());
    println!("  Market   : {}", config.market_ticker.yellow());
    if config.api_key.is_some() && config.private_key.is_some() {
        println!(
            "  Auth     : {}",
            "Enabled (API key + RSA signature)".green()
        );
    } else {
        println!(
            "  Auth     : {}",
            "Disabled — public channels only".yellow()
        );
    }

    // ── Dispatch ────────────────────────────────────────────────────────────
    let run_all = matches!(config.test, TestChoice::All);

    let mut connect_results: Option<Vec<Duration>> = None;
    let mut subscribe_result: Option<Option<Duration>> = None;
    let mut throughput_result: Option<(usize, f64)> = None;
    let mut roundtrip_results: Option<Vec<Duration>> = None;
    let mut multi_results: Option<Vec<Duration>> = None;

    if run_all || matches!(config.test, TestChoice::Connect) {
        match run_connect_test(&config).await {
            Ok(lats) => connect_results = Some(lats),
            Err(e) => eprintln!("  {} Connect test: {}", "Error:".red(), e),
        }
    }

    if run_all || matches!(config.test, TestChoice::Subscribe) {
        match run_subscribe_test(&config).await {
            Ok(lat) => subscribe_result = Some(lat),
            Err(e) => eprintln!("  {} Subscribe test: {}", "Error:".red(), e),
        }
    }

    if run_all || matches!(config.test, TestChoice::Throughput) {
        match run_throughput_test(&config).await {
            Ok(r) => throughput_result = Some(r),
            Err(e) => eprintln!("  {} Throughput test: {}", "Error:".red(), e),
        }
    }

    if run_all || matches!(config.test, TestChoice::Roundtrip) {
        match run_roundtrip_test(&config).await {
            Ok(lats) => roundtrip_results = Some(lats),
            Err(e) => eprintln!("  {} Roundtrip test: {}", "Error:".red(), e),
        }
    }

    if run_all || matches!(config.test, TestChoice::Multi) {
        match run_multi_connection_test(&config).await {
            Ok(lats) => multi_results = Some(lats),
            Err(e) => eprintln!("  {} Multi-connection test: {}", "Error:".red(), e),
        }
    }

    print_summary(
        connect_results.as_ref(),
        subscribe_result,
        throughput_result,
        roundtrip_results.as_ref(),
        multi_results.as_ref(),
    );

    Ok(())
}
