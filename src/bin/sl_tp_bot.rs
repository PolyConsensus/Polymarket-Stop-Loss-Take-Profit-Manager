//! SL/TP Position Manager — Polymarket Stop-Loss / Take-Profit Bot
//!
//! Scans wallet for open Polymarket positions, lets you set SL/TP prices via TUI,
//! monitors real-time orderbook prices via WebSocket, and executes sell orders
//! when SL or TP triggers are hit.
//!
//! No Binance WS, no ML, no BTC price — just position management.
//!
//! Usage:
//!   DRY_RUN=true cargo run --release --bin sl_tp_bot
//!
//! Keybindings:
//!   q       - Quit
//!   ↑/↓,j/k - Navigate positions
//!   s       - Set stop-loss for selected position
//!   t       - Set take-profit for selected position
//!   x       - Clear SL/TP for selected position
//!   Enter   - Confirm input
//!   Esc     - Cancel input
//!   d       - Toggle dry_run mode
//!   r       - Force refresh positions from API

use anyhow::{Result, Context};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use once_cell::sync::Lazy;
use rusqlite::{Connection, params};
use std::sync::Mutex;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Frame, Terminal,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::collections::VecDeque;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// Polymarket SDK
use alloy::primitives::Address;
use alloy::signers::Signer as _;
use alloy_signer_local::PrivateKeySigner;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::clob::types::{
    AssetType, BalanceAllowanceRequest, OrderBookSummaryRequestBuilder, OrderType, 
    Side as OrderSide, SignatureType, UpdateBalanceAllowanceRequest,
};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;

const CLOB_ENDPOINT: &str = "https://clob.polymarket.com";
const POLYGON_CHAIN_ID: u64 = 137;

// ============================================================================
// GLOBAL HTTP CLIENT
// ============================================================================

static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(60))
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(Duration::from_secs(120))
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to create HTTP client")
});

// ============================================================================
// ERROR LOGGING
// ============================================================================

fn log_error(msg: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("sl_tp_errors.log")
    {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let _ = writeln!(f, "[{}] {}", timestamp, msg);
    }
}

// ============================================================================
// DATABASE — SQLite for persisting SL/TP settings and trade log
// ============================================================================

struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    fn new(path: &str) -> Result<Self> {
        // Create directory if needed
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path)
            .context("Failed to open SQLite database")?;

        // WAL mode for concurrent reads
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

        // SL/TP settings table — persists across restarts
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS sl_tp_settings (
                token_id TEXT PRIMARY KEY,
                market_title TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                sl_price TEXT,
                tp_price TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        ")?;

        // Trade/sell log — historical record of all executions
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS trade_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT (datetime('now')),
                token_id TEXT NOT NULL,
                market_title TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                trigger_type TEXT NOT NULL,
                shares TEXT NOT NULL,
                entry_price TEXT NOT NULL DEFAULT '0',
                sell_price TEXT NOT NULL DEFAULT '0',
                fill_price TEXT,
                pnl_pct TEXT,
                dry_run INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'pending'
            );
            CREATE INDEX IF NOT EXISTS idx_trade_log_ts ON trade_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_trade_log_token ON trade_log(token_id);
        ")?;

        // Position snapshots — periodic record of all positions
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS position_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL DEFAULT (datetime('now')),
                token_id TEXT NOT NULL,
                market_title TEXT NOT NULL DEFAULT '',
                outcome TEXT NOT NULL DEFAULT '',
                shares TEXT NOT NULL,
                entry_price TEXT NOT NULL DEFAULT '0',
                current_bid TEXT,
                current_ask TEXT,
                sl_price TEXT,
                tp_price TEXT,
                status TEXT NOT NULL DEFAULT 'active'
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON position_snapshots(timestamp);
        ")?;

        // PolyConsensus entry snapshots — persists entry consensus data
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS consensus_entry (
                condition_id TEXT PRIMARY KEY,
                consensus_pct INTEGER NOT NULL DEFAULT 0,
                yes_count INTEGER NOT NULL DEFAULT 0,
                no_count INTEGER NOT NULL DEFAULT 0,
                total_value REAL NOT NULL DEFAULT 0,
                market_prob REAL NOT NULL DEFAULT 0,
                ml_signal TEXT NOT NULL DEFAULT '',
                ml_edge REAL NOT NULL DEFAULT 0,
                ml_confidence TEXT NOT NULL DEFAULT '',
                ml_risk REAL NOT NULL DEFAULT 0,
                top_trader_count INTEGER NOT NULL DEFAULT 0,
                active_trader_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        ")?;

        eprintln!("[OK] Database initialized: {}", path);
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Save or update SL/TP for a position
    fn save_sl_tp(&self, token_id: &str, market_title: &str, outcome: &str, sl: Option<Decimal>, tp: Option<Decimal>) {
        let conn = self.conn.lock().unwrap();
        let sl_str = sl.map(|v| v.to_string());
        let tp_str = tp.map(|v| v.to_string());
        let _ = conn.execute(
            "INSERT INTO sl_tp_settings (token_id, market_title, outcome, sl_price, tp_price, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
             ON CONFLICT(token_id) DO UPDATE SET
                market_title = ?2, outcome = ?3, sl_price = ?4, tp_price = ?5, updated_at = datetime('now')",
            params![token_id, market_title, outcome, sl_str, tp_str],
        );
    }

    /// Clear SL/TP for a position
    fn clear_sl_tp(&self, token_id: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "DELETE FROM sl_tp_settings WHERE token_id = ?1",
            params![token_id],
        );
    }

    /// Load all saved SL/TP settings
    fn load_sl_tp_settings(&self) -> Vec<(String, Option<Decimal>, Option<Decimal>)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare("SELECT token_id, sl_price, tp_price FROM sl_tp_settings") {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |row| {
            let token_id: String = row.get(0)?;
            let sl_str: Option<String> = row.get(1)?;
            let tp_str: Option<String> = row.get(2)?;
            Ok((token_id, sl_str, tp_str))
        });
        match rows {
            Ok(mapped) => mapped
                .filter_map(|r| r.ok())
                .map(|(tid, sl_str, tp_str)| {
                    let sl = sl_str.and_then(|s| s.parse::<Decimal>().ok());
                    let tp = tp_str.and_then(|s| s.parse::<Decimal>().ok());
                    (tid, sl, tp)
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Log a trade/sell execution
    fn log_trade(
        &self,
        token_id: &str,
        market_title: &str,
        outcome: &str,
        trigger_type: &str,
        shares: Decimal,
        entry_price: Decimal,
        sell_price: Decimal,
        fill_price: Option<Decimal>,
        pnl_pct: Option<Decimal>,
        dry_run: bool,
        status: &str,
    ) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "INSERT INTO trade_log (token_id, market_title, outcome, trigger_type, shares, entry_price, sell_price, fill_price, pnl_pct, dry_run, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                token_id,
                market_title,
                outcome,
                trigger_type,
                shares.to_string(),
                entry_price.to_string(),
                sell_price.to_string(),
                fill_price.map(|v| v.to_string()),
                pnl_pct.map(|v| v.to_string()),
                dry_run as i32,
                status,
            ],
        );
    }

    /// Save a position snapshot (called periodically)
    fn save_position_snapshot(&self, positions: &[TrackedPosition]) {
        let conn = self.conn.lock().unwrap();
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(_) => return,
        };
        for pos in positions {
            let _ = tx.execute(
                "INSERT INTO position_snapshots (token_id, market_title, outcome, shares, entry_price, current_bid, current_ask, sl_price, tp_price, status)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    pos.token_id,
                    pos.market_title,
                    pos.outcome,
                    pos.shares.to_string(),
                    pos.entry_price.to_string(),
                    pos.current_bid.map(|v| v.to_string()),
                    pos.current_ask.map(|v| v.to_string()),
                    pos.sl_price.map(|v| v.to_string()),
                    pos.tp_price.map(|v| v.to_string()),
                    format!("{}", pos.status),
                ],
            );
        }
        let _ = tx.commit();
    }

    /// Save consensus entry snapshot (only first time — INSERT OR IGNORE)
    fn save_consensus_entry(&self, condition_id: &str, snap: &ConsensusSnapshot) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "INSERT OR IGNORE INTO consensus_entry 
             (condition_id, consensus_pct, yes_count, no_count, total_value, market_prob,
              ml_signal, ml_edge, ml_confidence, ml_risk, top_trader_count, active_trader_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                condition_id,
                snap.consensus_pct,
                snap.yes_count,
                snap.no_count,
                snap.total_value,
                snap.market_prob,
                snap.ml_signal,
                snap.ml_edge,
                snap.ml_confidence,
                snap.ml_risk,
                snap.top_trader_count,
                snap.active_trader_count,
            ],
        );
    }

    /// Load consensus entry snapshot from DB
    fn load_consensus_entry(&self, condition_id: &str) -> Option<ConsensusSnapshot> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT consensus_pct, yes_count, no_count, total_value, market_prob,
                    ml_signal, ml_edge, ml_confidence, ml_risk, top_trader_count, active_trader_count
             FROM consensus_entry WHERE condition_id = ?1"
        ).ok()?;
        stmt.query_row(params![condition_id], |row| {
            Ok(ConsensusSnapshot {
                consensus_pct: row.get(0)?,
                yes_count: row.get(1)?,
                no_count: row.get(2)?,
                total_value: row.get(3)?,
                market_prob: row.get(4)?,
                ml_signal: row.get(5)?,
                ml_edge: row.get(6)?,
                ml_confidence: row.get(7)?,
                ml_risk: row.get(8)?,
                top_trader_count: row.get(9)?,
                active_trader_count: row.get(10)?,
                timestamp: Instant::now(),
            })
        }).ok()
    }
}

// ============================================================================
// EXECUTOR — Polymarket SDK with sell capability
// ============================================================================

struct Executor {
    client: ClobClient<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    #[allow(dead_code)]
    funder_address: String,
}

impl Executor {
    async fn new() -> Result<Self> {
        let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .or_else(|_| std::env::var("PM_PRIVATE_KEY"))
            .context("POLYMARKET_PRIVATE_KEY or PM_PRIVATE_KEY not set")?;
        let funder = std::env::var("POLYMARKET_FUNDER")
            .or_else(|_| std::env::var("PM_FUNDER"))
            .context("POLYMARKET_FUNDER or PM_FUNDER not set")?;
        let sig_type: u8 = std::env::var("POLYMARKET_SIGNATURE_TYPE")
            .or_else(|_| std::env::var("POLYMARKET_SIG_TYPE"))
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1);

        let pk = private_key.trim().strip_prefix("0x").unwrap_or(private_key.trim());
        let signer = PrivateKeySigner::from_str(pk)
            .map_err(|e| anyhow::anyhow!("Invalid private key: {}", e))?
            .with_chain_id(Some(POLYGON_CHAIN_ID));

        let signature_type = match sig_type {
            0 => SignatureType::Eoa,
            1 => SignatureType::Proxy,
            2 => SignatureType::GnosisSafe,
            _ => SignatureType::Proxy,
        };

        let unauth = ClobClient::new(CLOB_ENDPOINT, ClobConfig::default())
            .map_err(|e| anyhow::anyhow!("Failed to create CLOB client: {:?}", e))?;

        let funder_addr: Address = funder.parse()
            .map_err(|e| anyhow::anyhow!("Invalid funder address: {:?}", e))?;

        let client = unauth
            .authentication_builder(&signer)
            .signature_type(signature_type)
            .funder(funder_addr)
            .authenticate()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to authenticate: {:?}", e))?;

        // Warm up connection pool
        let _ = HTTP_CLIENT.get(format!("{}/time", CLOB_ENDPOINT)).send().await;
        eprintln!("[OK] Executor initialized");

        Ok(Self {
            client,
            signer,
            funder_address: funder,
        })
    }

    /// Sell shares of a conditional token using a market order (FOK).
    /// The SDK walks the orderbook bids to find the crossing price automatically.
    /// Returns fill price on success, None if order wasn't filled.
    async fn sell(&self, token_id: &str, size: Decimal, price: Decimal) -> Result<Option<Decimal>> {
        let start = Instant::now();

        // Query orderbook for market parameters (min_order_size, tick_size, best bid)
        let ob_req = OrderBookSummaryRequestBuilder::default()
            .token_id(token_id.to_string())
            .build()
            .expect("Failed to build OrderBookSummaryRequest");
        let (ob_min_size, ob_tick_decimals, best_bid) = match self.client.order_book(&ob_req).await {
            Ok(ob) => {
                let tick_dec: Decimal = ob.tick_size.into();
                let best = ob.bids.first().map(|b| b.price);
                log_error(&format!(
                    "Orderbook: min_size={} tick_size={} neg_risk={} bids={} asks={} best_bid={:?}",
                    ob.min_order_size, tick_dec, ob.neg_risk, ob.bids.len(), ob.asks.len(), best
                ));
                (Some(ob.min_order_size), tick_dec.scale(), best)
            }
            Err(e) => {
                log_error(&format!("Warning: failed to query orderbook: {:?}", e));
                // Fallback: query tick_size separately
                let td = match self.client.tick_size(token_id).await {
                    Ok(ts) => ts.minimum_tick_size.as_decimal().scale(),
                    Err(_) => 3u32,
                };
                (None, td, None)
            }
        };

        // Use best bid price if available (ensures we match existing buy orders)
        let raw_price = best_bid.unwrap_or(price);
        let sell_price = raw_price.trunc_with_scale(ob_tick_decimals);
        // Ensure size has at most 2 decimal places (LOT_SIZE_SCALE)
        let sell_size = size.trunc_with_scale(2);

        log_error(&format!(
            "Sell prep: input_price={} best_bid={:?} sell_price={} tick_decimals={} size={} min={:?}",
            price, best_bid, sell_price, ob_tick_decimals, sell_size, ob_min_size
        ));

        // Ensure conditional token allowance is synced with CLOB
        let mut allowance_req = UpdateBalanceAllowanceRequest::default();
        allowance_req.asset_type = AssetType::Conditional;
        allowance_req.token_id = Some(token_id.to_string());
        if let Err(e) = self.client.update_balance_allowance(&allowance_req).await {
            log_error(&format!("Warning: update_balance_allowance failed: {:?}", e));
        }

        // Build GTC limit sell order
        let signable = self.client
            .limit_order()
            .token_id(token_id.to_string())
            .side(OrderSide::Sell)
            .price(sell_price)
            .size(sell_size)
            .order_type(OrderType::GTC)
            .build()
            .await
            .map_err(|e| {
                log_error(&format!("Failed to build sell order: {:?}", e));
                anyhow::anyhow!("Failed to build sell order: {:?}", e)
            })?;

        let signed = self.client
            .sign(&self.signer, signable)
            .await
            .map_err(|e| {
                log_error(&format!("Failed to sign sell order: {:?}", e));
                anyhow::anyhow!("Failed to sign sell order: {:?}", e)
            })?;

        let responses = self.client
            .post_order(signed)
            .await
            .map_err(|e| {
                log_error(&format!("Failed to post sell order: {:?}", e));
                anyhow::anyhow!("Failed to post sell order: {:?}", e)
            })?;

        let elapsed = start.elapsed();

        if let Some(r) = responses.first() {
            log_error(&format!(
                "Sell GTC: token={}.. price={} size={} elapsed={}ms success={} status={:?} making={} taking={} order_id={} error={:?}",
                &token_id[..16.min(token_id.len())],
                sell_price, sell_size, elapsed.as_millis(),
                r.success, r.status, r.making_amount, r.taking_amount, r.order_id,
                r.error_msg
            ));

            // Polymarket returns Some("") on success — treat empty string as no error
            let has_error = r.error_msg.as_ref().map_or(false, |s| !s.is_empty());
            if r.success && !has_error {
                if r.taking_amount > dec!(0) {
                    // Immediately filled
                    let fill_price = r.taking_amount / r.making_amount;
                    log_error(&format!("Sell FILLED immediately: fill_price={:.4} shares={} usdc={}", fill_price, r.making_amount, r.taking_amount));
                    return Ok(Some(fill_price));
                } else if !r.order_id.is_empty() {
                    // Order placed on book, treat as sold at our limit price
                    log_error(&format!("Sell order PLACED on book: order_id={} price={}", r.order_id, sell_price));
                    return Ok(Some(sell_price));
                }
            }
        }

        Ok(None)
    }

    /// Query conditional token balance via CLOB API
    #[allow(dead_code)]
    async fn query_token_balance(&self, token_id: &str) -> Result<Decimal> {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Conditional;
        req.token_id = Some(token_id.to_string());
        let resp = self.client
            .balance_allowance(&req)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query token balance: {:?}", e))?;
        Ok(resp.balance)
    }

    /// Query USDC balance via CLOB API
    /// API returns raw atomic units (6 decimals), so divide by 10^6
    async fn query_usdc_balance(&self) -> Result<Decimal> {
        let mut req = BalanceAllowanceRequest::default();
        req.asset_type = AssetType::Collateral;
        let resp = self.client
            .balance_allowance(&req)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query USDC balance: {:?}", e))?;
        let usdc_scale = Decimal::from(1_000_000u64);
        Ok(resp.balance / usdc_scale)
    }
}

// ============================================================================
// DATA API — Fetch open positions from Polymarket
// ============================================================================

/// Response from https://data-api.polymarket.com/positions
#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
struct ApiPosition {
    /// The condition_id or market identifier
    #[serde(default)]
    market: String,
    /// Outcome token asset_id
    #[serde(default)]
    asset: String,
    /// Side of the position (long/short)
    #[serde(default)]
    side: String,
    /// Number of shares held
    #[serde(default)]
    size: f64,
    /// Average entry price
    #[serde(default, rename = "avgPrice")]
    avg_price: f64,
    /// Current market price
    #[serde(default, rename = "curPrice")]
    cur_price: f64,
    /// Realized PnL
    #[serde(default, rename = "realizedPnl")]
    realized_pnl: f64,
    /// Unrealized PnL
    #[serde(default, rename = "unrealizedPnl")]
    unrealized_pnl: f64,
    /// Market title / question
    #[serde(default)]
    title: String,
    /// Outcome name (Yes/No, Up/Down, etc.)
    #[serde(default)]
    outcome: String,
    /// Condition ID for market lookup
    #[serde(default, rename = "conditionId")]
    condition_id: String,
    /// The CLOB token ID for this outcome
    #[serde(default, rename = "proxyWalletAddress")]
    proxy_wallet_address: String,
}

/// Simpler raw response structure — the data-api can return varied formats
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
struct RawPosition {
    #[serde(default)]
    asset: String,
    #[serde(default)]
    market: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    size: serde_json::Value,
    #[serde(default, rename = "avgPrice")]
    avg_price: serde_json::Value,
    #[serde(default, rename = "curPrice")]
    cur_price: serde_json::Value,
    #[serde(default)]
    title: String,
    #[serde(default)]
    outcome: String,
    #[serde(default, rename = "conditionId")]
    condition_id: String,
    // Additional fields we might see
    #[serde(default, rename = "cashPnl")]
    cash_pnl: serde_json::Value,
    #[serde(default, rename = "percentPnl")]
    percent_pnl: serde_json::Value,
}

fn parse_decimal_value(v: &serde_json::Value) -> Decimal {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Decimal::from_f64_retain(f).unwrap_or_default()
            } else {
                Decimal::ZERO
            }
        }
        serde_json::Value::String(s) => s.parse::<Decimal>().unwrap_or_default(),
        _ => Decimal::ZERO,
    }
}

/// Fetch all open positions from Polymarket data API
async fn fetch_positions(funder_address: &str) -> Result<Vec<RawPosition>> {
    let url = format!(
        "https://data-api.polymarket.com/positions?user={}",
        funder_address
    );

    let resp = HTTP_CLIENT
        .get(&url)
        .send()
        .await
        .context("Failed to fetch positions from data API")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        log_error(&format!("Data API returned {}: {}", status, &body[..200.min(body.len())]));
        anyhow::bail!("Data API returned {}", status);
    }

    let positions: Vec<RawPosition> = resp
        .json()
        .await
        .context("Failed to parse positions JSON")?;

    Ok(positions)
}

// ============================================================================
// GAMMA API — Fetch market metadata (title, outcomes)
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
struct GammaMarket {
    question: Option<String>,
    outcomes: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    slug: Option<String>,
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
}

/// Orderbook metadata returned from CLOB
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct OrderbookMeta {
    min_order_size: Decimal,
    tick_size: Decimal,
    neg_risk: bool,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
}

/// PolyConsensus API response structures
#[derive(Debug, Clone, Deserialize)]
struct ConsensusApiResponse {
    market: Option<ConsensusMarket>,
    #[serde(rename = "mlSignal")]
    ml_signal: Option<ConsensusMlSignal>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConsensusMarket {
    #[serde(rename = "consensusPct", default)]
    consensus_pct: i32,
    #[serde(rename = "topTraderCount", default)]
    top_trader_count: i32,
    #[serde(rename = "activeTraderCount", default)]
    active_trader_count: i32,
    #[serde(rename = "totalValue", default)]
    total_value: f64,
    #[serde(default)]
    outcomes: Vec<ConsensusOutcome>,
    #[serde(rename = "topTraders", default)]
    top_traders: Vec<ConsensusTrader>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConsensusOutcome {
    #[allow(dead_code)]
    name: String,
    probability: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct ConsensusTrader {
    #[serde(default)]
    outcome: String,
    #[serde(rename = "isArbitrage", default)]
    is_arbitrage: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ConsensusMlSignal {
    #[serde(default)]
    signal: String,
    #[allow(dead_code)]
    #[serde(default)]
    predicted_probability: f64,
    #[serde(default)]
    edge: f64,
    #[allow(dead_code)]
    #[serde(default)]
    confidence_score: f64,
    #[serde(default)]
    confidence_label: String,
    #[serde(default)]
    risk_score: f64,
}

/// Snapshot of PolyConsensus data — taken at entry and updated live
#[derive(Debug, Clone)]
struct ConsensusSnapshot {
    consensus_pct: i32,
    yes_count: i32,
    no_count: i32,
    total_value: f64,
    market_prob: f64,
    ml_signal: String,
    ml_edge: f64,
    ml_confidence: String,
    ml_risk: f64,
    top_trader_count: i32,
    active_trader_count: i32,
    #[allow(dead_code)]
    timestamp: Instant,
}

/// Full consensus data for a position: entry snapshot + current live data
#[derive(Debug, Clone)]
struct ConsensusData {
    entry: ConsensusSnapshot,
    current: ConsensusSnapshot,
}

/// Fetch orderbook metadata (min_order_size, tick_size, neg_risk, best bid/ask) from CLOB
async fn fetch_orderbook_meta(token_id: &str) -> Result<OrderbookMeta> {
    let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);
    let resp = HTTP_CLIENT.get(&url).send().await
        .context("Failed to fetch orderbook")?;
    if !resp.status().is_success() {
        let status = resp.status();
        anyhow::bail!("Orderbook API returned {}", status);
    }
    let body: serde_json::Value = resp.json().await
        .context("Failed to parse orderbook JSON")?;

    let min_order_size = body.get("min_order_size")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(dec!(5)); // default 5 if not found

    let tick_size = body.get("tick_size")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .unwrap_or(dec!(0.01)); // default 0.01 if not found

    let neg_risk = body.get("neg_risk")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract best bid/ask from the orderbook
    let best_bid = body.get("bids")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.last()) // bids sorted ascending, best bid is last
        .and_then(|b| b.get("price"))
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok());

    let best_ask = body.get("asks")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.last()) // asks sorted descending, best ask is last
        .and_then(|b| b.get("price"))
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok());

    Ok(OrderbookMeta {
        min_order_size,
        tick_size,
        neg_risk,
        best_bid,
        best_ask,
    })
}

/// Fetch PolyConsensus smart market data for a condition_id
async fn fetch_consensus_data(condition_id: &str) -> Result<ConsensusSnapshot> {
    let url = format!("https://polyconsensus.com/api/smart-markets/{}", condition_id);
    let resp = HTTP_CLIENT.get(&url).send().await
        .context("Failed to fetch PolyConsensus data")?;
    if !resp.status().is_success() {
        anyhow::bail!("PolyConsensus API returned {}", resp.status());
    }
    let api: ConsensusApiResponse = resp.json().await
        .context("Failed to parse PolyConsensus response")?;

    let market = api.market.unwrap_or(ConsensusMarket {
        consensus_pct: 0,
        top_trader_count: 0,
        active_trader_count: 0,
        total_value: 0.0,
        outcomes: vec![],
        top_traders: vec![],
    });

    let yes_count = market.top_traders.iter()
        .filter(|t| t.outcome == "Yes" && !t.is_arbitrage)
        .count() as i32;
    let no_count = market.top_traders.iter()
        .filter(|t| t.outcome == "No" && !t.is_arbitrage)
        .count() as i32;

    let market_prob = market.outcomes.first()
        .map(|o| o.probability)
        .unwrap_or(0.0);

    let ml = api.ml_signal.unwrap_or(ConsensusMlSignal {
        signal: "N/A".to_string(),
        predicted_probability: 0.0,
        edge: 0.0,
        confidence_score: 0.0,
        confidence_label: "none".to_string(),
        risk_score: 0.0,
    });

    Ok(ConsensusSnapshot {
        consensus_pct: market.consensus_pct,
        yes_count,
        no_count,
        total_value: market.total_value,
        market_prob,
        ml_signal: ml.signal,
        ml_edge: ml.edge,
        ml_confidence: ml.confidence_label,
        ml_risk: ml.risk_score,
        top_trader_count: market.top_trader_count,
        active_trader_count: market.active_trader_count,
        timestamp: Instant::now(),
    })
}

/// Fetch market info by condition_id from gamma API
#[allow(dead_code)]
async fn fetch_market_info(condition_id: &str) -> Result<GammaMarket> {
    let url = format!(
        "https://gamma-api.polymarket.com/markets?condition_id={}",
        condition_id
    );
    let resp = HTTP_CLIENT.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("Gamma API returned {}", resp.status());
    }
    let markets: Vec<GammaMarket> = resp.json().await?;
    markets.into_iter().next().context("No market found for condition_id")
}

// ============================================================================
// ORDERBOOK WS — Price monitoring
// ============================================================================

#[derive(Debug, Deserialize)]
struct OrderbookMsg {
    event_type: Option<String>,
    asset_id: Option<String>,
    bids: Option<Vec<OrderLevel>>,
    asks: Option<Vec<OrderLevel>>,
    price_changes: Option<Vec<PriceChange>>,
    #[allow(dead_code)]
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PriceChange {
    asset_id: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
    #[allow(dead_code)]
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrderLevel {
    price: String,
    #[allow(dead_code)]
    size: Option<String>,
}

// ============================================================================
// APP STATE
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum PositionStatus {
    Active,
    SLTriggered,
    TPTriggered,
    Selling,
    Sold,
    Error(String),
}

impl std::fmt::Display for PositionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionStatus::Active => write!(f, "Active"),
            PositionStatus::SLTriggered => write!(f, "SL Hit"),
            PositionStatus::TPTriggered => write!(f, "TP Hit"),
            PositionStatus::Selling => write!(f, "Selling..."),
            PositionStatus::Sold => write!(f, "SOLD"),
            PositionStatus::Error(e) => write!(f, "ERR: {}", e),
        }
    }
}

#[derive(Debug, Clone)]
struct TrackedPosition {
    /// CLOB token ID (the asset_id for the outcome token)  
    token_id: String,
    /// Condition ID (market identifier)
    #[allow(dead_code)]
    condition_id: String,
    /// Human-readable market title
    market_title: String,
    /// Outcome name (Yes/No, Up/Down, etc.)
    outcome: String,
    /// Number of shares held
    shares: Decimal,
    /// Average entry price
    entry_price: Decimal,
    /// Current best bid price (from WS)
    current_bid: Option<Decimal>,
    /// Current best ask price (from WS)
    current_ask: Option<Decimal>,
    /// Stop-loss price (sell when bid <= this)
    sl_price: Option<Decimal>,
    /// Take-profit price (sell when bid >= this)
    tp_price: Option<Decimal>,
    /// Position status
    status: PositionStatus,
    /// Last WS update time
    last_price_update: Option<Instant>,
    /// PnL when sold
    exit_pnl: Option<Decimal>,
    /// Minimum order size from CLOB orderbook (e.g. 5 shares)
    min_order_size: Option<Decimal>,
    /// Tick size from CLOB orderbook (e.g. 0.001)
    tick_size: Option<Decimal>,
    /// Whether this is a neg_risk market
    neg_risk: Option<bool>,
    /// PolyConsensus data: entry snapshot + current
    consensus: Option<ConsensusData>,
    /// Consecutive checks bid was below SL (anti-manipulation)
    sl_trigger_count: u32,
    /// Consecutive checks bid was above TP (anti-manipulation)
    tp_trigger_count: u32,
}

impl TrackedPosition {
    /// Calculate unrealized PnL percentage based on current bid vs entry price
    fn unrealized_pnl_pct(&self) -> Option<f64> {
        let bid = self.current_bid?;
        if self.entry_price == Decimal::ZERO {
            return None;
        }
        let pnl = (bid - self.entry_price) / self.entry_price * dec!(100);
        pnl.to_f64()
    }
    
    /// Short display of market title (truncated)
    fn short_title(&self, max_len: usize) -> String {
        if self.market_title.len() <= max_len {
            self.market_title.clone()
        } else {
            format!("{}...", &self.market_title[..max_len.saturating_sub(3)])
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum InputMode {
    Normal,
    EditingSL,
    EditingTP,
}

struct AppState {
    /// All tracked positions
    positions: Vec<TrackedPosition>,
    /// Currently selected position index
    selected_index: usize,
    /// Input mode FSM
    input_mode: InputMode,
    /// Input buffer for typing prices
    input_buffer: String,
    /// Log messages
    logs: VecDeque<String>,
    /// Dry run mode
    dry_run: bool,
    /// USDC balance
    usdc_balance: Decimal,
    /// Funder address
    funder_address: String,
    /// Session start time
    start_time: Instant,
    /// Last position scan time
    last_scan: Option<Instant>,
    /// WS connection status
    ws_connected: bool,
    /// WS message count
    ws_msg_count: u64,
    /// Number of active SL/TP triggers
    active_triggers: usize,
    /// Total sells executed this session
    sells_executed: u32,
    /// Table state for scrolling
    table_state: TableState,
    /// SQLite database for persistence
    db: Arc<Database>,
}

impl AppState {
    fn new(dry_run: bool, funder_address: String, db: Arc<Database>) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        
        Self {
            positions: Vec::new(),
            selected_index: 0,
            input_mode: InputMode::Normal,
            input_buffer: String::new(),
            logs: VecDeque::with_capacity(100),
            dry_run,
            usdc_balance: Decimal::ZERO,
            funder_address,
            start_time: Instant::now(),
            last_scan: None,
            ws_connected: false,
            ws_msg_count: 0,
            active_triggers: 0,
            sells_executed: 0,
            table_state,
            db,
        }
    }

    fn add_log(&mut self, msg: String) {
        let ts = chrono::Local::now().format("%H:%M:%S");
        self.logs.push_back(format!("[{}] {}", ts, msg));
        if self.logs.len() > 100 {
            self.logs.pop_front();
        }
    }

    fn selected_position(&self) -> Option<&TrackedPosition> {
        self.positions.get(self.selected_index)
    }

    #[allow(dead_code)]
    fn selected_position_mut(&mut self) -> Option<&mut TrackedPosition> {
        self.positions.get_mut(self.selected_index)
    }

    #[allow(dead_code)]
    fn move_selection_up(&mut self) {
        if !self.positions.is_empty() {
            if self.selected_index > 0 {
                self.selected_index -= 1;
            } else {
                self.selected_index = self.positions.len() - 1;
            }
            self.table_state.select(Some(self.selected_index));
        }
    }

    fn move_selection_down(&mut self) {
        if !self.positions.is_empty() {
            if self.selected_index < self.positions.len() - 1 {
                self.selected_index += 1;
            } else {
                self.selected_index = 0;
            }
            self.table_state.select(Some(self.selected_index));
        }
    }

    /// Count positions that have SL or TP set and are active
    fn count_active_triggers(&self) -> usize {
        self.positions.iter().filter(|p| {
            p.status == PositionStatus::Active && (p.sl_price.is_some() || p.tp_price.is_some())
        }).count()
    }

    fn uptime(&self) -> String {
        let elapsed = self.start_time.elapsed();
        let secs = elapsed.as_secs();
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        let s = secs % 60;
        if h > 0 {
            format!("{}h{}m{}s", h, m, s)
        } else if m > 0 {
            format!("{}m{}s", m, s)
        } else {
            format!("{}s", s)
        }
    }
}

// ============================================================================
// POSITION SCANNER TASK
// ============================================================================

async fn run_position_scanner(
    state: Arc<RwLock<AppState>>,
    executor: Arc<Executor>,
    mut force_scan_rx: tokio::sync::mpsc::Receiver<()>,
) {
    let scan_interval = Duration::from_secs(30);

    loop {
        // Wait for either timer or force signal
        tokio::select! {
            _ = tokio::time::sleep(scan_interval) => {}
            Some(()) = force_scan_rx.recv() => {
                let mut s = state.write().await;
                s.add_log("[OK] Force scan triggered".to_string());
            }
        }

        let funder = {
            state.read().await.funder_address.clone()
        };

        // Fetch positions from data API
        match fetch_positions(&funder).await {
            Ok(raw_positions) => {
                // Filter to positions with size > 0
                let active: Vec<&RawPosition> = raw_positions.iter()
                    .filter(|p| {
                        let size = parse_decimal_value(&p.size);
                        size > Decimal::ZERO
                    })
                    .collect();

                let mut s = state.write().await;
                s.last_scan = Some(Instant::now());

                // Merge with existing tracked positions (preserve SL/TP settings)
                let mut new_positions: Vec<TrackedPosition> = Vec::new();

                for raw in &active {
                    let token_id = raw.asset.clone();
                    let size = parse_decimal_value(&raw.size);
                    let avg_price = parse_decimal_value(&raw.avg_price);
                    let cur_price = parse_decimal_value(&raw.cur_price);

                    // Check if we already track this position
                    let existing = s.positions.iter().find(|p| p.token_id == token_id);

                    if let Some(existing) = existing {
                        // Preserve SL/TP and status, update shares/price
                        let mut updated = existing.clone();
                        updated.shares = size;
                        updated.entry_price = if avg_price > Decimal::ZERO { avg_price } else { updated.entry_price };
                        // Update title/outcome if we got new data
                        if !raw.title.is_empty() {
                            updated.market_title = raw.title.clone();
                        }
                        if !raw.outcome.is_empty() {
                            updated.outcome = raw.outcome.clone();
                        }
                        // Don't overwrite WS prices with API prices (WS is more current)
                        if updated.current_bid.is_none() && cur_price > Decimal::ZERO {
                            updated.current_bid = Some(cur_price);
                        }
                        // Reactivate positions on re-entry (user bought back after SL/TP sell)
                        if size > Decimal::ZERO && matches!(updated.status, 
                            PositionStatus::Sold | PositionStatus::SLTriggered | PositionStatus::TPTriggered | PositionStatus::Error(_)) 
                        {
                            let old_status = format!("{:?}", updated.status);
                            updated.status = PositionStatus::Active;
                            updated.exit_pnl = None;
                            updated.current_bid = if cur_price > Decimal::ZERO { Some(cur_price) } else { None };
                            updated.current_ask = None;
                            updated.sl_price = None;
                            updated.tp_price = None;
                            s.add_log(format!("[OK] Re-entry detected: {} — reactivated from {}", updated.market_title, old_status));
                        }
                        new_positions.push(updated);
                    } else {
                        // New position discovered
                        let title = if raw.title.is_empty() {
                            format!("Market {}", &raw.market[..12.min(raw.market.len())])
                        } else {
                            raw.title.clone()
                        };
                        let outcome = if raw.outcome.is_empty() {
                            "Unknown".to_string()
                        } else {
                            raw.outcome.clone()
                        };

                        new_positions.push(TrackedPosition {
                            token_id: token_id.clone(),
                            condition_id: raw.condition_id.clone(),
                            market_title: title,
                            outcome,
                            shares: size,
                            entry_price: avg_price,
                            current_bid: if cur_price > Decimal::ZERO { Some(cur_price) } else { None },
                            current_ask: None,
                            sl_price: None,
                            tp_price: None,
                            status: PositionStatus::Active,
                            last_price_update: None,
                            exit_pnl: None,
                            min_order_size: None,
                            tick_size: None,
                            neg_risk: None,
                            consensus: None,
                            sl_trigger_count: 0,
                            tp_trigger_count: 0,
                        });

                        // Restore SL/TP from DB for new positions
                        let saved = s.db.load_sl_tp_settings();
                        if let Some(last) = new_positions.last_mut() {
                            for (saved_tid, sl, tp) in &saved {
                                if *saved_tid == token_id {
                                    last.sl_price = *sl;
                                    last.tp_price = *tp;
                                    break;
                                }
                            }
                        }
                    }
                }

                // Keep sold positions for display (but don't track new ones that are gone)
                for old_pos in &s.positions {
                    if matches!(old_pos.status, PositionStatus::Sold) {
                        if !new_positions.iter().any(|p| p.token_id == old_pos.token_id) {
                            new_positions.push(old_pos.clone());
                        }
                    }
                }

                let new_count = new_positions.len();
                let prev_count = s.positions.len();
                s.positions = new_positions;

                // Fix selected index if out of bounds
                if s.selected_index >= s.positions.len() && !s.positions.is_empty() {
                    s.selected_index = s.positions.len() - 1;
                }
                let is_empty = s.positions.is_empty();
                let sel = s.selected_index;
                s.table_state.select(if is_empty { None } else { Some(sel) });
                s.active_triggers = s.count_active_triggers();

                if new_count != prev_count {
                    s.add_log(format!("[OK] Scan: {} positions found", new_count));
                }

                // Collect token_ids that need orderbook meta (don't have it yet)
                let tokens_needing_meta: Vec<String> = s.positions.iter()
                    .filter(|p| p.min_order_size.is_none() && !matches!(p.status, PositionStatus::Sold))
                    .map(|p| p.token_id.clone())
                    .collect();

                drop(s); // Release lock before network calls

                // Fetch orderbook metadata for positions that don't have it yet
                for tid in &tokens_needing_meta {
                    match fetch_orderbook_meta(tid).await {
                        Ok(meta) => {
                            let mut s = state.write().await;
                            if let Some(pos) = s.positions.iter_mut().find(|p| p.token_id == *tid) {
                                pos.min_order_size = Some(meta.min_order_size);
                                pos.tick_size = Some(meta.tick_size);
                                pos.neg_risk = Some(meta.neg_risk);
                            }
                        }
                        Err(e) => {
                            log_error(&format!("Failed to fetch orderbook meta for {}.. : {}", &tid[..16.min(tid.len())], e));
                        }
                    }
                }

                // Fetch PolyConsensus data for active positions (every 60s, matching API cache)
                let should_fetch_consensus = {
                    let s = state.read().await;
                    s.positions.first()
                        .and_then(|p| p.consensus.as_ref())
                        .map(|cd| cd.current.timestamp.elapsed() >= Duration::from_secs(55))
                        .unwrap_or(true) // fetch if no consensus data yet
                };
                if should_fetch_consensus {
                    let s = state.read().await;
                    let positions_for_consensus: Vec<(String, String)> = s.positions.iter()
                        .filter(|p| p.status == PositionStatus::Active && !p.condition_id.is_empty())
                        .map(|p| (p.token_id.clone(), p.condition_id.clone()))
                        .collect();
                    drop(s);

                    for (token_id, cond_id) in &positions_for_consensus {
                        match fetch_consensus_data(cond_id).await {
                            Ok(current_snap) => {
                                let mut s = state.write().await;
                                // Check if position already has consensus data
                                let needs_init = s.positions.iter()
                                    .find(|p| p.token_id == *token_id)
                                    .map(|p| p.consensus.is_none())
                                    .unwrap_or(false);

                                if needs_init {
                                    // First time — load entry from DB or use current as entry
                                    let entry = s.db.load_consensus_entry(cond_id)
                                        .unwrap_or_else(|| current_snap.clone());
                                    s.db.save_consensus_entry(cond_id, &entry);
                                    if let Some(pos) = s.positions.iter_mut().find(|p| p.token_id == *token_id) {
                                        pos.consensus = Some(ConsensusData {
                                            entry,
                                            current: current_snap,
                                        });
                                    }
                                } else {
                                    // Update current snapshot only
                                    if let Some(pos) = s.positions.iter_mut().find(|p| p.token_id == *token_id) {
                                        if let Some(data) = &mut pos.consensus {
                                            data.current = current_snap;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                log_error(&format!("PolyConsensus fetch failed for {}.. : {}", &cond_id[..16.min(cond_id.len())], e));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] Position scan failed: {}", e));
            }
        }

        // Also update USDC balance
        match executor.query_usdc_balance().await {
            Ok(balance) => {
                let mut s = state.write().await;
                s.usdc_balance = balance;
            }
            Err(e) => {
                log_error(&format!("USDC balance query failed: {}", e));
            }
        }

        // Save position snapshot to DB
        {
            let s = state.read().await;
            let active_positions: Vec<TrackedPosition> = s.positions.iter()
                .filter(|p| p.status == PositionStatus::Active)
                .cloned()
                .collect();
            if !active_positions.is_empty() {
                s.db.save_position_snapshot(&active_positions);
            }
        }
    }
}

// ============================================================================
// ORDERBOOK WS TASK — Subscribe to all position token IDs
// ============================================================================

async fn run_orderbook_ws(state: Arc<RwLock<AppState>>, ws_msg_count_atomic: Arc<AtomicU64>) {
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    const STALE_TIMEOUT: u64 = 15;

    loop {
        // Collect token IDs to subscribe to
        let token_ids: Vec<String> = {
            let s = state.read().await;
            s.positions.iter()
                .filter(|p| p.status == PositionStatus::Active)
                .map(|p| p.token_id.clone())
                .collect()
        };

        if token_ids.is_empty() {
            {
                let mut s = state.write().await;
                s.ws_connected = false;
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
            continue;
        }

        {
            let mut s = state.write().await;
            s.add_log(format!("Connecting to Polymarket WS ({} tokens)...", token_ids.len()));
        }

        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                {
                    let mut s = state.write().await;
                    s.ws_connected = true;
                    s.add_log("[OK] Polymarket WS connected".to_string());
                }

                let (mut write, mut read) = ws_stream.split();

                // Subscribe to all token IDs
                let sub_msg = serde_json::json!({
                    "assets_ids": token_ids,
                    "type": "market"
                });

                use futures_util::SinkExt;
                if write.send(Message::Text(sub_msg.to_string().into())).await.is_err() {
                    let mut s = state.write().await;
                    s.add_log("[X] Failed to subscribe to WS".to_string());
                    continue;
                }

                // Track current token set to detect changes
                let subscribed_tokens = token_ids.clone();
                let mut msg_since_token_check: u32 = 0;

                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(STALE_TIMEOUT),
                        read.next(),
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            let mut s = state.write().await;
                            s.add_log("WS stream ended".to_string());
                            break;
                        }
                        Err(_) => {
                            let mut s = state.write().await;
                            s.add_log(format!("[!] WS stale ({}s), reconnecting...", STALE_TIMEOUT));
                            break;
                        }
                    };

                    if let Ok(Message::Text(text)) = msg {
                        // Increment msg count without a lock
                        ws_msg_count_atomic.fetch_add(1, Ordering::Relaxed);

                        if let Ok(ob) = serde_json::from_str::<OrderbookMsg>(&text) {
                            // Handle price_change events
                            if ob.event_type.as_deref() == Some("price_change") {
                                if let Some(changes) = &ob.price_changes {
                                    let mut s = state.write().await;
                                    for change in changes {
                                        if let Some(asset_id) = &change.asset_id {
                                            let best_bid = change.best_bid.as_ref()
                                                .and_then(|p| p.parse::<Decimal>().ok());
                                            let best_ask = change.best_ask.as_ref()
                                                .and_then(|p| p.parse::<Decimal>().ok());

                                            // Update matching position
                                            for pos in s.positions.iter_mut() {
                                                if pos.token_id == *asset_id {
                                                    if best_bid.is_some() {
                                                        pos.current_bid = best_bid;
                                                    }
                                                    if best_ask.is_some() {
                                                        pos.current_ask = best_ask;
                                                    }
                                                    pos.last_price_update = Some(Instant::now());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // Handle full book snapshot
                            else if ob.event_type.as_deref() == Some("book") {
                                if let Some(asset_id) = &ob.asset_id {
                                    let best_bid = ob.bids.as_ref()
                                        .and_then(|bids| {
                                            bids.iter()
                                                .filter_map(|l| l.price.parse::<Decimal>().ok())
                                                .max()
                                        });
                                    let best_ask = ob.asks.as_ref()
                                        .and_then(|asks| {
                                            asks.iter()
                                                .filter_map(|l| l.price.parse::<Decimal>().ok())
                                                .min()
                                        });

                                    let mut s = state.write().await;
                                    for pos in s.positions.iter_mut() {
                                        if pos.token_id == *asset_id {
                                            if best_bid.is_some() {
                                                pos.current_bid = best_bid;
                                            }
                                            if best_ask.is_some() {
                                                pos.current_ask = best_ask;
                                            }
                                            pos.last_price_update = Some(Instant::now());
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Check if token set changed (only every 100 messages to reduce lock contention)
                    msg_since_token_check += 1;
                    if msg_since_token_check >= 100 {
                        msg_since_token_check = 0;
                        let s = state.read().await;
                        let current_tokens: Vec<&String> = s.positions.iter()
                            .filter(|p| p.status == PositionStatus::Active)
                            .map(|p| &p.token_id)
                            .collect();
                        let needs_resub = current_tokens.len() != subscribed_tokens.len()
                            || current_tokens.iter().any(|t| !subscribed_tokens.contains(t));
                        if needs_resub {
                            break; // Reconnect with new token set
                        }
                    }
                }

                {
                    let mut s = state.write().await;
                    s.ws_connected = false;
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] WS connect failed: {}", e));
                s.ws_connected = false;
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ============================================================================
// SL/TP CHECKER TASK — Monitors prices and triggers sells
// ============================================================================

async fn run_sltp_checker(
    state: Arc<RwLock<AppState>>,
    executor: Arc<Executor>,
    db: Arc<Database>,
) {
    let check_interval = Duration::from_millis(250);

    loop {
        tokio::time::sleep(check_interval).await;

        // Collect positions that need action
        // Tuple: (idx, token_id, market_title, outcome, status_str, shares, entry_price, bid_price, is_tp)
        let mut sl_increments: Vec<(usize, bool)> = Vec::new();
        let mut tp_increments: Vec<(usize, bool)> = Vec::new();
        let triggers: Vec<(usize, String, String, String, String, Decimal, Decimal, Decimal, bool)> = {
            let s = state.read().await;
            let mut result = Vec::new();

            for (i, pos) in s.positions.iter().enumerate() {
                if pos.status != PositionStatus::Active {
                    continue;
                }

                let bid = match pos.current_bid {
                    Some(b) => b,
                    None => continue,
                };

                // Anti-manipulation: require CONFIRMATIONS_REQUIRED consecutive checks
                // Check interval is 250ms, so 12 checks = 3 seconds of sustained price
                const CONFIRMATIONS_REQUIRED: u32 = 12;

                // Check SL: sell when bid <= sl_price
                if let Some(sl) = pos.sl_price {
                    if bid <= sl {
                        // Will increment counter after this read lock is released
                        let count = pos.sl_trigger_count + 1;
                        if count >= CONFIRMATIONS_REQUIRED {
                            result.push((i, pos.token_id.clone(), pos.market_title.clone(), pos.outcome.clone(), format!("{}", pos.status), pos.shares, pos.entry_price, bid, false));
                            continue;
                        }
                        // Not confirmed yet — will increment below
                        sl_increments.push((i, true));
                        continue; // Don't check TP while SL is pending
                    }
                }

                // Check TP: sell when bid >= tp_price
                if let Some(tp) = pos.tp_price {
                    if bid >= tp {
                        let count = pos.tp_trigger_count + 1;
                        if count >= CONFIRMATIONS_REQUIRED {
                            result.push((i, pos.token_id.clone(), pos.market_title.clone(), pos.outcome.clone(), format!("{}", pos.status), pos.shares, pos.entry_price, bid, true));
                            continue;
                        }
                        tp_increments.push((i, true));
                        continue;
                    }
                }
            }

            result
        };

        // Update confirmation counters (requires write lock)
        if !sl_increments.is_empty() || !tp_increments.is_empty() || !triggers.is_empty() {
            let mut s = state.write().await;
            let mut counter_logs: Vec<String> = Vec::new();
            // Increment counters for positions approaching trigger
            for (idx, _) in &sl_increments {
                if let Some(pos) = s.positions.get_mut(*idx) {
                    pos.sl_trigger_count += 1;
                    if pos.sl_trigger_count == 1 {
                        let title = pos.short_title(25);
                        let bid = pos.current_bid.unwrap_or_default();
                        counter_logs.push(format!(
                            "[!] SL breach detected for {} @ bid={:.2}¢ — confirming...",
                            title, bid * dec!(100)
                        ));
                    }
                }
            }
            for (idx, _) in &tp_increments {
                if let Some(pos) = s.positions.get_mut(*idx) {
                    pos.tp_trigger_count += 1;
                    if pos.tp_trigger_count == 1 {
                        let title = pos.short_title(25);
                        let bid = pos.current_bid.unwrap_or_default();
                        counter_logs.push(format!(
                            "[!] TP breach detected for {} @ bid={:.2}¢ — confirming...",
                            title, bid * dec!(100)
                        ));
                    }
                }
            }
            // Reset counters for confirmed triggers (they'll be processed below)
            for (idx, ..) in &triggers {
                if let Some(pos) = s.positions.get_mut(*idx) {
                    pos.sl_trigger_count = 0;
                    pos.tp_trigger_count = 0;
                }
            }
            // Reset counters for positions where price recovered (not in any increment/trigger list)
            let triggered_indices: Vec<usize> = sl_increments.iter().map(|(i,_)| *i)
                .chain(tp_increments.iter().map(|(i,_)| *i))
                .chain(triggers.iter().map(|(i,..)| *i))
                .collect();
            let mut recovered_logs: Vec<String> = Vec::new();
            for (i, pos) in s.positions.iter_mut().enumerate() {
                if !triggered_indices.contains(&i) {
                    if pos.sl_trigger_count > 0 {
                        let title = pos.short_title(25);
                        recovered_logs.push(format!(
                            "[OK] SL spike filtered for {} — price recovered after {} checks",
                            title, pos.sl_trigger_count
                        ));
                        pos.sl_trigger_count = 0;
                    }
                    if pos.tp_trigger_count > 0 {
                        pos.tp_trigger_count = 0;
                    }
                }
            }
            for log in recovered_logs {
                s.add_log(log);
            }
            for log in counter_logs {
                s.add_log(log);
            }
        }

        // Process triggers
        for (idx, token_id, market_title, outcome, _status_str, shares, entry_price, bid_price, is_tp) in triggers {
            let trigger_type = if is_tp { "TP" } else { "SL" };

            // Mark as triggered
            {
                let mut s = state.write().await;
                if let Some(pos) = s.positions.get_mut(idx) {
                    pos.status = if is_tp {
                        PositionStatus::TPTriggered
                    } else {
                        PositionStatus::SLTriggered
                    };
                    let title = pos.short_title(30);
                    s.add_log(format!(
                        "[!] {} triggered for {} @ bid={:.2}¢ | shares={}",
                        trigger_type, title, bid_price * dec!(100), shares
                    ));
                }
            }

            // Check dry_run
            let dry_run = {
                state.read().await.dry_run
            };

            if dry_run {
                let mut s = state.write().await;
                if let Some(pos) = s.positions.get_mut(idx) {
                    let entry = pos.entry_price;
                    let pnl = if entry > Decimal::ZERO {
                        (bid_price - entry) / entry * dec!(100)
                    } else {
                        Decimal::ZERO
                    };
                    let title = pos.short_title(20);
                    pos.status = PositionStatus::Sold;
                    pos.exit_pnl = Some(pnl);
                    // Clear in-memory SL/TP so it never re-triggers
                    pos.sl_price = None;
                    pos.tp_price = None;
                    s.sells_executed += 1;
                    s.add_log(format!(
                        ">>>╢ DRY_RUN: Would sell {} shares of [{}] @ {:.2}¢ (PnL: {:.1}%)",
                        shares, title, bid_price * dec!(100), pnl
                    ));
                }
                // Log dry run trade to DB
                db.log_trade(
                    &token_id, &market_title, &outcome, trigger_type,
                    shares, entry_price, bid_price, None, 
                    if entry_price > Decimal::ZERO { Some((bid_price - entry_price) / entry_price * dec!(100)) } else { None },
                    true, "dry_run",
                );
                db.clear_sl_tp(&token_id);
                continue;
            }

            // Execute sell
            {
                let mut s = state.write().await;
                if let Some(pos) = s.positions.get_mut(idx) {
                    pos.status = PositionStatus::Selling;
                }
            }

            // Sell at the bid price (aggressive — take the current bid)
            let sell_price = bid_price;
            match executor.sell(&token_id, shares, sell_price).await {
                Ok(Some(fill_price)) => {
                    let mut s = state.write().await;
                    if let Some(pos) = s.positions.get_mut(idx) {
                        let entry = pos.entry_price;
                        let pnl = if entry > Decimal::ZERO {
                            (fill_price - entry) / entry * dec!(100)
                        } else {
                            Decimal::ZERO
                        };
                        let title = pos.short_title(20);
                        pos.status = PositionStatus::Sold;
                        pos.exit_pnl = Some(pnl);
                        s.sells_executed += 1;
                        s.add_log(format!(
                            "[OK] >>>╢ SOLD {} shares of [{}] @ {:.2}¢ (PnL: {:.1}%)",
                            shares, title, fill_price * dec!(100), pnl
                        ));
                    }
                    // Clear in-memory SL/TP
                    if let Some(pos) = s.positions.get_mut(idx) {
                        pos.sl_price = None;
                        pos.tp_price = None;
                    }
                    // Log successful sell to DB
                    let pnl = if entry_price > Decimal::ZERO { Some((fill_price - entry_price) / entry_price * dec!(100)) } else { None };
                    db.log_trade(&token_id, &market_title, &outcome, trigger_type, shares, entry_price, sell_price, Some(fill_price), pnl, false, "filled");
                    db.clear_sl_tp(&token_id);
                }
                Ok(None) => {
                    {
                        let mut s = state.write().await;
                        if let Some(pos) = s.positions.get_mut(idx) {
                            let title = pos.short_title(20);
                            s.add_log(format!("[!] Sell failed for {}, retrying...", title));
                        }
                    }
                    
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    
                    match executor.sell(&token_id, shares, sell_price).await {
                        Ok(Some(fill_price)) => {
                            let mut s = state.write().await;
                            if let Some(pos) = s.positions.get_mut(idx) {
                                let entry = pos.entry_price;
                                let pnl = if entry > Decimal::ZERO {
                                    (fill_price - entry) / entry * dec!(100)
                                } else {
                                    Decimal::ZERO
                                };
                                pos.status = PositionStatus::Sold;
                                pos.exit_pnl = Some(pnl);
                            }
                            s.sells_executed += 1;
                            s.add_log(format!(
                                "[OK] >>>╢ SOLD (retry) {} shares @ {:.2}¢",
                                shares, fill_price * dec!(100)
                            ));
                            // Log retry sell to DB
                            let pnl = if entry_price > Decimal::ZERO { Some((fill_price - entry_price) / entry_price * dec!(100)) } else { None };
                            db.log_trade(&token_id, &market_title, &outcome, trigger_type, shares, entry_price, sell_price, Some(fill_price), pnl, false, "filled_retry");
                            db.clear_sl_tp(&token_id);
                        }
                        Ok(None) => {
                            let mut s = state.write().await;
                            if let Some(pos) = s.positions.get_mut(idx) {
                                let title = pos.short_title(20);
                                pos.status = PositionStatus::Error("Sell returned no fill".to_string());
                                s.add_log(format!("[X] Sell failed for {} after retry", title));
                            }
                            // Log failed sell to DB
                            db.log_trade(&token_id, &market_title, &outcome, trigger_type, shares, entry_price, sell_price, None, None, false, "failed");
                        }
                        Err(e) => {
                            let mut s = state.write().await;
                            if let Some(pos) = s.positions.get_mut(idx) {
                                let title = pos.short_title(20);
                                pos.status = PositionStatus::Error(format!("{}", e));
                                s.add_log(format!("[X] Sell error for {}: {}", title, e));
                            }
                            // Log error sell to DB
                            db.log_trade(&token_id, &market_title, &outcome, trigger_type, shares, entry_price, sell_price, None, None, false, &format!("error: {}", e));
                        }
                    }
                }
                Err(e) => {
                    let mut s = state.write().await;
                    if let Some(pos) = s.positions.get_mut(idx) {
                        let title = pos.short_title(20);
                        pos.status = PositionStatus::Error(format!("{}", e));
                        s.add_log(format!("[X] Sell error for {}: {}", title, e));
                    }
                    // Log error to DB
                    db.log_trade(&token_id, &market_title, &outcome, trigger_type, shares, entry_price, sell_price, None, None, false, &format!("error: {}", e));
                }
            }
        }

        // Update trigger count
        {
            let mut s = state.write().await;
            s.active_triggers = s.count_active_triggers();
        }
    }
}

// ============================================================================
// TUI RENDERING
// ============================================================================

fn draw_ui(f: &mut Frame, state: &AppState) {
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),  // Header
            Constraint::Length(1),  // Status bar
            Constraint::Min(8),    // Position table
            Constraint::Length(7),  // Consensus detail panel
            Constraint::Length(1),  // Input bar
            Constraint::Length(6),  // Logs
            Constraint::Length(1),  // Footer
        ])
        .split(f.area());

    // ========== HEADER ==========
    let header = Paragraph::new(Line::from(vec![
        Span::styled(" SL/TP Manager ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw("| "),
        Span::styled(
            if state.dry_run { "DRY RUN" } else { "LIVE" },
            Style::default()
                .fg(if state.dry_run { Color::Yellow } else { Color::Red })
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" | "),
        Span::raw("USDC: "),
        Span::styled(
            format!("${:.2}", state.usdc_balance),
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" | "),
        Span::raw("Positions: "),
        Span::styled(
            format!("{}", state.positions.iter().filter(|p| p.status == PositionStatus::Active).count()),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" | "),
        Span::raw("SL/TP: "),
        Span::styled(
            format!("{}", state.active_triggers),
            Style::default().fg(if state.active_triggers > 0 { Color::Green } else { Color::DarkGray }),
        ),
        Span::raw(" | "),
        Span::raw("Sells: "),
        Span::styled(
            format!("{}", state.sells_executed),
            Style::default().fg(Color::White),
        ),
    ]));
    f.render_widget(header, main_chunks[0]);

    // ========== STATUS BAR ==========
    let ws_status = if state.ws_connected { "WS: Connected" } else { "WS: Disconnected" };
    let ws_color = if state.ws_connected { Color::Green } else { Color::Red };
    let scan_info = state.last_scan
        .map(|t| format!("Last scan: {}s ago", t.elapsed().as_secs()))
        .unwrap_or_else(|| "Scanning...".to_string());
    
    let status_bar = Paragraph::new(Line::from(vec![
        Span::styled(format!(" {} ", ws_status), Style::default().fg(ws_color)),
        Span::raw("| "),
        Span::styled(format!("Msgs: {} ", state.ws_msg_count), Style::default().fg(Color::DarkGray)),
        Span::raw("| "),
        Span::styled(scan_info, Style::default().fg(Color::DarkGray)),
        Span::raw(" | "),
        Span::styled(format!("Uptime: {}", state.uptime()), Style::default().fg(Color::DarkGray)),
    ]));
    f.render_widget(status_bar, main_chunks[1]);

    // ========== POSITION TABLE ==========
    let header_cells = [
        "#", "Market", "Outcome", "Shares", "Entry", "Bid", "Ask", 
        "SL", "TP", "PnL%", "Vol$K", "Traders", "Status",
    ]
    .iter()
    .map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)));
    let header_row = Row::new(header_cells).height(1);

    let rows: Vec<Row> = state
        .positions
        .iter()
        .enumerate()
        .map(|(i, pos)| {
            let is_selected = i == state.selected_index;
            let base_style = if is_selected {
                Style::default().bg(Color::DarkGray)
            } else {
                Style::default()
            };

            let pnl_str = pos.unrealized_pnl_pct()
                .map(|p| format!("{:.1}%", p))
                .unwrap_or_else(|| "--".to_string());
            let pnl_color = pos.unrealized_pnl_pct()
                .map(|p| if p >= 0.0 { Color::Green } else { Color::Red })
                .unwrap_or(Color::DarkGray);

            let status_color = match &pos.status {
                PositionStatus::Active => Color::Green,
                PositionStatus::SLTriggered => Color::Red,
                PositionStatus::TPTriggered => Color::Cyan,
                PositionStatus::Selling => Color::Yellow,
                PositionStatus::Sold => Color::Magenta,
                PositionStatus::Error(_) => Color::Red,
            };

            let bid_str = pos.current_bid
                .map(|b| format!("{:.1}¢", b * dec!(100)))
                .unwrap_or_else(|| "--".to_string());
            let ask_str = pos.current_ask
                .map(|a| format!("{:.1}¢", a * dec!(100)))
                .unwrap_or_else(|| "--".to_string());
            
            let sl_str = pos.sl_price
                .map(|p| format!("{:.1}¢", p * dec!(100)))
                .unwrap_or_else(|| "--".to_string());
            let tp_str = pos.tp_price
                .map(|p| format!("{:.1}¢", p * dec!(100)))
                .unwrap_or_else(|| "--".to_string());

            let sl_color = if pos.sl_price.is_some() { Color::Red } else { Color::DarkGray };
            let tp_color = if pos.tp_price.is_some() { Color::Green } else { Color::DarkGray };

            let (vol_str, vol_color, traders_str, traders_color) = if let Some(cd) = &pos.consensus {
                let vol_pct = if cd.entry.total_value > 0.0 {
                    (cd.current.total_value - cd.entry.total_value) / cd.entry.total_value * 100.0
                } else { 0.0 };
                let vol_color = if vol_pct > 0.0 { Color::Green } else if vol_pct < 0.0 { Color::Red } else { Color::Cyan };

                let entry_traders = cd.entry.yes_count + cd.entry.no_count;
                let current_traders = cd.current.yes_count + cd.current.no_count;
                let traders_pct = if entry_traders > 0 {
                    (current_traders - entry_traders) as f64 / entry_traders as f64 * 100.0
                } else { 0.0 };
                let traders_color = if traders_pct > 0.0 { Color::Green } else if traders_pct < 0.0 { Color::Red } else { Color::White };

                (
                    format!("${:.0}K{:+.0}%", cd.current.total_value / 1000.0, vol_pct),
                    vol_color,
                    format!("{}Y/{}N{:+.0}%", cd.current.yes_count, cd.current.no_count, traders_pct),
                    traders_color,
                )
            } else {
                ("--".to_string(), Color::DarkGray, "--".to_string(), Color::DarkGray)
            };

            Row::new(vec![
                Cell::from(format!("{}", i + 1)).style(base_style),
                Cell::from(pos.short_title(28)).style(base_style),
                Cell::from(pos.outcome.clone()).style(base_style),
                Cell::from(format!("{:.1}", pos.shares)).style(base_style),
                Cell::from(format!("{:.1}¢", pos.entry_price * dec!(100))).style(base_style),
                Cell::from(bid_str).style(base_style),
                Cell::from(ask_str).style(base_style),
                Cell::from(sl_str).style(base_style.fg(sl_color)),
                Cell::from(tp_str).style(base_style.fg(tp_color)),
                Cell::from(pnl_str).style(base_style.fg(pnl_color)),
                Cell::from(vol_str).style(base_style.fg(vol_color)),
                Cell::from(traders_str).style(base_style.fg(traders_color)),
                Cell::from(format!("{}", pos.status)).style(base_style.fg(status_color)),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(3),   // #
            Constraint::Min(20),     // Market
            Constraint::Length(8),   // Outcome
            Constraint::Length(8),   // Shares
            Constraint::Length(8),   // Entry
            Constraint::Length(8),   // Bid
            Constraint::Length(8),   // Ask
            Constraint::Length(8),   // SL
            Constraint::Length(8),   // TP
            Constraint::Length(8),   // PnL%
            Constraint::Length(12),  // Vol$K
            Constraint::Length(12),  // Traders
            Constraint::Length(8),   // Status
        ],
    )
    .header(header_row)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Positions ")
            .border_style(Style::default().fg(Color::Yellow)),
    )
    .row_highlight_style(Style::default().add_modifier(Modifier::BOLD))
    .highlight_symbol("► ");

    // We need a mutable table_state for rendering, but we're in an immutable context.
    // Clone the table state for rendering.
    let mut table_state = state.table_state.clone();
    f.render_stateful_widget(table, main_chunks[2], &mut table_state);

    // ========== CONSENSUS DETAIL PANEL ==========
    let consensus_content = if let Some(pos) = state.selected_position() {
        if let Some(cd) = &pos.consensus {
            let price_delta = if pos.entry_price > Decimal::ZERO {
                let bid = pos.current_bid.unwrap_or(pos.entry_price);
                let pct = ((bid - pos.entry_price) / pos.entry_price * dec!(100))
                    .to_f64().unwrap_or(0.0);
                format!("{:+.1}%", pct)
            } else {
                "--".to_string()
            };
            let cons_delta = cd.current.consensus_pct - cd.entry.consensus_pct;
            let cons_arrow = if cons_delta > 0 { "\u{2191}" } else if cons_delta < 0 { "\u{2193}" } else { "=" };
            let vol_delta = cd.current.total_value - cd.entry.total_value;
            vec![
                Line::from(vec![
                    Span::styled(" Price:     ", Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("Entry {:.1}\u{00a2}", pos.entry_price * dec!(100)), Style::default().fg(Color::White)),
                    Span::raw(" \u{2192} "),
                    Span::styled(
                        format!("Now {:.1}\u{00a2}", pos.current_bid.unwrap_or(pos.entry_price) * dec!(100)),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(format!(" ({})", price_delta), Style::default().fg(
                        if price_delta.starts_with('+') { Color::Green } else if price_delta.starts_with('-') { Color::Red } else { Color::DarkGray }
                    )),
                ]),
                Line::from(vec![
                    Span::styled(" Consensus: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("Entry {}Y/{}N ({}%)", cd.entry.yes_count, cd.entry.no_count, cd.entry.consensus_pct),
                        Style::default().fg(Color::White),
                    ),
                    Span::raw(" \u{2192} "),
                    Span::styled(
                        format!("Now {}Y/{}N ({}%)", cd.current.yes_count, cd.current.no_count, cd.current.consensus_pct),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(
                        format!(" [{}{:+}%]", cons_arrow, cons_delta),
                        Style::default().fg(if cons_delta > 0 { Color::Green } else if cons_delta < 0 { Color::Red } else { Color::DarkGray }),
                    ),
                ]),
                Line::from(vec![
                    Span::styled(" Volume:    ", Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("Entry ${:.0}K", cd.entry.total_value / 1000.0), Style::default().fg(Color::White)),
                    Span::raw(" \u{2192} "),
                    Span::styled(format!("Now ${:.0}K", cd.current.total_value / 1000.0), Style::default().fg(Color::Cyan)),
                    Span::styled(format!(" ({:+.0})", vol_delta), Style::default().fg(
                        if vol_delta > 0.0 { Color::Green } else if vol_delta < 0.0 { Color::Red } else { Color::DarkGray }
                    )),
                ]),
                Line::from(vec![
                    Span::styled(" ML Signal: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        cd.current.ml_signal.clone(),
                        Style::default().fg(
                            if cd.current.ml_signal.contains("BUY") { Color::Green }
                            else if cd.current.ml_signal.contains("SELL") { Color::Red }
                            else { Color::Yellow }
                        ).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!(" (edge {:+.1}%, confidence: {}, risk: {:.1})",
                            cd.current.ml_edge * 100.0, cd.current.ml_confidence, cd.current.ml_risk),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]),
                Line::from(vec![
                    Span::styled(" Traders:   ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{} smart money wallets ({} active)",
                            cd.current.top_trader_count, cd.current.active_trader_count),
                        Style::default().fg(Color::White),
                    ),
                ]),
            ]
        } else {
            vec![Line::from(Span::styled(
                " Loading PolyConsensus data...",
                Style::default().fg(Color::DarkGray),
            ))]
        }
    } else {
        vec![Line::from(Span::styled(
            " Select a position to see consensus data",
            Style::default().fg(Color::DarkGray),
        ))]
    };

    let consensus_panel = Paragraph::new(consensus_content).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" PolyConsensus ")
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(consensus_panel, main_chunks[3]);

    // ========== INPUT BAR ==========
    let input_line = match &state.input_mode {
        InputMode::Normal => {
            let sel_info = state.selected_position()
                .map(|p| format!(" | Selected: {} ({})", p.short_title(25), p.outcome))
                .unwrap_or_default();
            Line::from(vec![
                Span::styled(" NORMAL", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::styled(sel_info, Style::default().fg(Color::DarkGray)),
            ])
        }
        InputMode::EditingSL => {
            let label = state.selected_position()
                .map(|p| format!("Set SL for [{}]: ", p.short_title(20)))
                .unwrap_or_else(|| "Set SL: ".to_string());
            Line::from(vec![
                Span::styled(" SET SL ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                Span::raw(label),
                Span::styled(
                    format!("{}▌", state.input_buffer),
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                ),
                Span::styled("  (Enter=confirm, Esc=cancel, price in cents e.g. 45 = 0.45)", Style::default().fg(Color::DarkGray)),
            ])
        }
        InputMode::EditingTP => {
            let label = state.selected_position()
                .map(|p| format!("Set TP for [{}]: ", p.short_title(20)))
                .unwrap_or_else(|| "Set TP: ".to_string());
            Line::from(vec![
                Span::styled(" SET TP ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::raw(label),
                Span::styled(
                    format!("{}▌", state.input_buffer),
                    Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
                ),
                Span::styled("  (Enter=confirm, Esc=cancel, price in cents e.g. 75 = 0.75)", Style::default().fg(Color::DarkGray)),
            ])
        }
    };
    let input_bar = Paragraph::new(input_line);
    f.render_widget(input_bar, main_chunks[4]);

    // ========== LOGS ==========
    let log_lines: Vec<Line> = state
        .logs
        .iter()
        .rev()
        .take(7)
        .rev()
        .map(|l| {
            let color = if l.contains("[OK]") {
                Color::Green
            } else if l.contains("[X]") {
                Color::Red
            } else if l.contains("[!]") || l.contains(">>>") {
                Color::Yellow
            } else {
                Color::DarkGray
            };
            Line::from(Span::styled(l.clone(), Style::default().fg(color)))
        })
        .collect();

    let logs_widget = Paragraph::new(log_lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Logs ")
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(logs_widget, main_chunks[5]);

    // ========== FOOTER ==========
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::Yellow)),
        Span::raw(":Quit "),
        Span::styled("↑↓/jk", Style::default().fg(Color::Yellow)),
        Span::raw(":Nav "),
        Span::styled("s", Style::default().fg(Color::Yellow)),
        Span::raw(":SL "),
        Span::styled("t", Style::default().fg(Color::Yellow)),
        Span::raw(":TP "),
        Span::styled("x", Style::default().fg(Color::Yellow)),
        Span::raw(":Clear "),
        Span::styled("f", Style::default().fg(Color::Red)),
        Span::raw(":Sell "),
        Span::styled("d", Style::default().fg(Color::Yellow)),
        Span::raw(":DryRun "),
        Span::styled("r", Style::default().fg(Color::Yellow)),
        Span::raw(":Refresh "),
    ]));
    f.render_widget(footer, main_chunks[6]);
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let dry_run = std::env::var("DRY_RUN")
        .map(|v| v != "false")
        .unwrap_or(true);

    // Initialize executor
    let executor = Arc::new(
        Executor::new()
            .await
            .context("Failed to initialize Polymarket executor")?,
    );

    let funder = std::env::var("POLYMARKET_FUNDER")
        .or_else(|_| std::env::var("PM_FUNDER"))
        .context("POLYMARKET_FUNDER or PM_FUNDER not set")?;

    let sig_type_val: u8 = std::env::var("POLYMARKET_SIGNATURE_TYPE")
        .or_else(|_| std::env::var("POLYMARKET_SIG_TYPE"))
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap_or(1);
    let sig_type_name = match sig_type_val {
        0 => "EOA",
        1 => "Proxy",
        2 => "GnosisSafe (MetaMask)",
        _ => "Unknown",
    };
    eprintln!("[OK] SL/TP Bot starting (dry_run={})", dry_run);
    eprintln!("[OK] Wallet: {} | Sig type: {} ({})", funder, sig_type_val, sig_type_name);

    // Cancel any stale orders from previous runs
    eprintln!("Cancelling any stale orders from previous runs...");
    match executor.client.cancel_all_orders().await {
        Ok(resp) => {
            let count = resp.canceled.len();
            if count > 0 {
                eprintln!("[OK] Cancelled {} stale orders", count);
            } else {
                eprintln!("[OK] No stale orders to cancel");
            }
        }
        Err(e) => {
            eprintln!("[!] Failed to cancel stale orders: {:?}", e);
        }
    }

    // Initial position scan
    eprintln!("Scanning wallet for positions...");
    let initial_positions = match fetch_positions(&funder).await {
        Ok(pos) => {
            eprintln!("[OK] Found {} positions with balance", pos.iter().filter(|p| parse_decimal_value(&p.size) > Decimal::ZERO).count());
            pos
        }
        Err(e) => {
            eprintln!("[!] Initial scan failed ({}), will retry...", e);
            Vec::new()
        }
    };

    // Build initial tracked positions
    let mut tracked: Vec<TrackedPosition> = Vec::new();
    for raw in &initial_positions {
        let size = parse_decimal_value(&raw.size);
        if size <= Decimal::ZERO {
            continue;
        }
        let avg_price = parse_decimal_value(&raw.avg_price);
        let cur_price = parse_decimal_value(&raw.cur_price);
        let title = if raw.title.is_empty() {
            format!("Market {}", &raw.market[..12.min(raw.market.len())])
        } else {
            raw.title.clone()
        };
        let outcome = if raw.outcome.is_empty() {
            "Unknown".to_string()
        } else {
            raw.outcome.clone()
        };

        tracked.push(TrackedPosition {
            token_id: raw.asset.clone(),
            condition_id: raw.condition_id.clone(),
            market_title: title,
            outcome,
            shares: size,
            entry_price: avg_price,
            current_bid: if cur_price > Decimal::ZERO { Some(cur_price) } else { None },
            current_ask: None,
            sl_price: None,
            tp_price: None,
            status: PositionStatus::Active,
            last_price_update: None,
            exit_pnl: None,
            min_order_size: None,
            tick_size: None,
            neg_risk: None,
            consensus: None,
            sl_trigger_count: 0,
            tp_trigger_count: 0,
        });
    }

    // Initialize database
    let db = Arc::new(
        Database::new("data/sl_tp_data.db")
            .context("Failed to initialize SQLite database")?
    );
    eprintln!("[OK] Database ready");

    // Restore saved SL/TP settings from DB
    let saved_settings = db.load_sl_tp_settings();
    let mut restored = 0usize;
    for (token_id, sl, tp) in &saved_settings {
        for pos in tracked.iter_mut() {
            if pos.token_id == *token_id {
                pos.sl_price = *sl;
                pos.tp_price = *tp;
                restored += 1;
            }
        }
    }
    if restored > 0 {
        eprintln!("[OK] Restored SL/TP for {} positions from DB", restored);
    }

    // Query USDC balance
    let usdc = executor.query_usdc_balance().await.unwrap_or_default();

    // Create app state
    let state = Arc::new(RwLock::new(AppState::new(dry_run, funder, Arc::clone(&db))));
    {
        let mut s = state.write().await;
        s.positions = tracked;
        s.usdc_balance = usdc;
        s.last_scan = Some(Instant::now());
        if !s.positions.is_empty() {
            s.table_state.select(Some(0));
        }
        let pos_count = s.positions.len();
        s.add_log(format!("[OK] Loaded {} positions, USDC: ${:.2}", pos_count, usdc));
    }

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Channel for force-scan signal
    let (force_scan_tx, force_scan_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Channel for force-sell signal: (idx, token_id, shares, bid_price, market_title, outcome, entry_price)
    let (force_sell_tx, mut force_sell_rx) = tokio::sync::mpsc::channel::<(usize, String, Decimal, Decimal, String, String, Decimal)>(4);

    // Spawn background tasks
    tokio::spawn(run_position_scanner(
        Arc::clone(&state),
        Arc::clone(&executor),
        force_scan_rx,
    ));

    let ws_msg_count_atomic = Arc::new(AtomicU64::new(0));
    let ws_handle = tokio::spawn(run_orderbook_ws(Arc::clone(&state), Arc::clone(&ws_msg_count_atomic)));
    let _ws_abort = ws_handle.abort_handle();

    tokio::spawn(run_sltp_checker(
        Arc::clone(&state),
        Arc::clone(&executor),
        Arc::clone(&db),
    ));

    // Spawn force-sell handler task
    {
        let state = Arc::clone(&state);
        let executor = Arc::clone(&executor);
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            while let Some((idx, token_id, shares, bid_price, market_title, outcome, entry_price)) = force_sell_rx.recv().await {
                // Mark as selling
                {
                    let mut s = state.write().await;
                    if let Some(pos) = s.positions.get_mut(idx) {
                        pos.status = PositionStatus::Selling;
                    }
                }

                let sell_price = bid_price;
                match executor.sell(&token_id, shares, sell_price).await {
                    Ok(Some(fill_price)) => {
                        let mut s = state.write().await;
                        if let Some(pos) = s.positions.get_mut(idx) {
                            let entry = pos.entry_price;
                            let pnl = if entry > Decimal::ZERO {
                                (fill_price - entry) / entry * dec!(100)
                            } else {
                                Decimal::ZERO
                            };
                            let title = pos.short_title(20);
                            pos.status = PositionStatus::Sold;
                            pos.exit_pnl = Some(pnl);
                            pos.sl_price = None;
                            pos.tp_price = None;
                            s.sells_executed += 1;
                            s.add_log(format!(
                                "[OK] >>>╢ FORCE SOLD {} shares of [{}] @ {:.2}¢ (PnL: {:.1}%)",
                                shares, title, fill_price * dec!(100), pnl
                            ));
                        }
                        let pnl = if entry_price > Decimal::ZERO { Some((fill_price - entry_price) / entry_price * dec!(100)) } else { None };
                        db.log_trade(&token_id, &market_title, &outcome, "MANUAL", shares, entry_price, sell_price, Some(fill_price), pnl, false, "filled");
                        db.clear_sl_tp(&token_id);
                    }
                    Ok(None) => {
                        let mut s = state.write().await;
                        if let Some(pos) = s.positions.get_mut(idx) {
                            let title = pos.short_title(20);
                            pos.status = PositionStatus::Active;
                            s.add_log(format!("[X] Force sell FAILED for {} (no fill)", title));
                        }
                    }
                    Err(e) => {
                        let mut s = state.write().await;
                        if let Some(pos) = s.positions.get_mut(idx) {
                            let title = pos.short_title(20);
                            pos.status = PositionStatus::Active;
                            s.add_log(format!("[X] Force sell ERROR for {}: {:?}", title, e));
                        }
                    }
                }
            }
        });
    }

    // Spawn a dedicated thread for crossterm event reading to avoid blocking tokio runtime
    let (key_tx, mut key_rx) = tokio::sync::mpsc::channel::<crossterm::event::KeyEvent>(32);
    std::thread::spawn(move || {
        loop {
            if crossterm::event::poll(Duration::from_millis(50)).unwrap_or(false) {
                if let Ok(Event::Key(key)) = event::read() {
                    if key.kind == KeyEventKind::Press {
                        if key_tx.blocking_send(key).is_err() {
                            break; // Main loop dropped, exit thread
                        }
                    }
                }
            }
        }
    });

    // Main TUI loop
    let tick_rate = Duration::from_millis(100);
    let mut draw_interval = tokio::time::interval(tick_rate);

    loop {
        // Sync ws_msg_count from atomic (brief write lock)
        {
            let mut s = state.write().await;
            s.ws_msg_count = ws_msg_count_atomic.load(Ordering::Relaxed);
        }
        // Draw (read lock only)
        {
            let s = state.read().await;
            terminal.draw(|f| draw_ui(f, &s))?;
        }

        // Wait for either a key event or the next draw tick
        let key = tokio::select! {
            k = key_rx.recv() => k,
            _ = draw_interval.tick() => None,
        };

        if let Some(key) = key {
            let mut s = state.write().await;
            match &s.input_mode {
                InputMode::Normal => match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('j') | KeyCode::Down => {
                        s.move_selection_down();
                    }
                    KeyCode::Char('k') | KeyCode::Up => {
                        s.move_selection_up();
                    }
                    KeyCode::Char('s') => {
                        if s.selected_position().is_some() {
                            s.input_mode = InputMode::EditingSL;
                            s.input_buffer.clear();
                        }
                    }
                    KeyCode::Char('t') => {
                        if s.selected_position().is_some() {
                            s.input_mode = InputMode::EditingTP;
                            s.input_buffer.clear();
                        }
                    }
                    KeyCode::Char('x') => {
                        let idx = s.selected_index;
                        if let Some(pos) = s.positions.get_mut(idx) {
                            let title = pos.short_title(20);
                            let token_id = pos.token_id.clone();
                            pos.sl_price = None;
                            pos.tp_price = None;
                            s.add_log(format!("[OK] Cleared SL/TP for {}", title));
                            s.db.clear_sl_tp(&token_id);
                        }
                        s.active_triggers = s.count_active_triggers();
                    }
                    KeyCode::Char('d') => {
                        s.dry_run = !s.dry_run;
                        let mode = if s.dry_run { "DRY RUN" } else { "LIVE" };
                        s.add_log(format!("[!] Mode switched to {}", mode));
                    }
                    KeyCode::Char('r') => {
                        let _ = force_scan_tx.try_send(());
                    }
                    KeyCode::Char('f') => {
                        let idx = s.selected_index;
                        if let Some(pos) = s.positions.get(idx) {
                            if pos.status == PositionStatus::Active && pos.shares > Decimal::ZERO {
                                if let Some(bid) = pos.current_bid {
                                    let info = (idx, pos.token_id.clone(), pos.shares, bid, pos.market_title.clone(), pos.outcome.clone(), pos.entry_price);
                                    let title = pos.short_title(20);
                                    s.add_log(format!("[!] Force selling {} @ bid={:.2}¢...", title, bid * dec!(100)));
                                    let _ = force_sell_tx.try_send(info);
                                } else {
                                    s.add_log("[X] No bid price available yet".to_string());
                                }
                            } else {
                                s.add_log("[X] Position not active or no shares".to_string());
                            }
                        }
                    }
                    _ => {}
                },
                InputMode::EditingSL | InputMode::EditingTP => {
                    let is_sl = s.input_mode == InputMode::EditingSL;
                    match key.code {
                        KeyCode::Esc => {
                            s.input_mode = InputMode::Normal;
                            s.input_buffer.clear();
                        }
                        KeyCode::Enter => {
                            let input = s.input_buffer.clone();
                            if let Ok(cents) = input.parse::<Decimal>() {
                                let price = cents / dec!(100);
                                if price > Decimal::ZERO && price < Decimal::ONE {
                                    let idx = s.selected_index;
                                    let db_info = if let Some(pos) = s.positions.get_mut(idx) {
                                        let title = pos.short_title(20);
                                        let token_id = pos.token_id.clone();
                                        let market_title = pos.market_title.clone();
                                        let outcome = pos.outcome.clone();
                                        if is_sl {
                                            pos.sl_price = Some(price);
                                        } else {
                                            pos.tp_price = Some(price);
                                        }
                                        let sl = pos.sl_price;
                                        let tp = pos.tp_price;
                                        Some((title, token_id, market_title, outcome, sl, tp))
                                    } else {
                                        None
                                    };
                                    if let Some((title, token_id, market_title, outcome, sl, tp)) = db_info {
                                        if is_sl {
                                            s.add_log(format!("[OK] SL set: {:.1}¢ for {}", cents, title));
                                        } else {
                                            s.add_log(format!("[OK] TP set: {:.1}¢ for {}", cents, title));
                                        }
                                        s.db.save_sl_tp(&token_id, &market_title, &outcome, sl, tp);
                                    }
                                    s.active_triggers = s.count_active_triggers();
                                } else {
                                    s.add_log("[X] Price must be between 0¢ and 100¢".to_string());
                                }
                            } else if !input.is_empty() {
                                s.add_log(format!("[X] Invalid price: '{}'", input));
                            }
                            s.input_mode = InputMode::Normal;
                            s.input_buffer.clear();
                        }
                        KeyCode::Backspace => {
                            s.input_buffer.pop();
                        }
                        KeyCode::Char(c) if c.is_ascii_digit() || c == '.' => {
                            s.input_buffer.push(c);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}