//! Hope1h - ML-Enhanced BTC 5-Minute Z-Score Trading Bot
//!
//! Full Terminal UI with real-time diagnostics, ML predictions, orderbook, and trade history.
//! EVENT-DRIVEN: Executes strategy immediately on each tick for minimal latency.
//! 
//! OPTIMIZATIONS APPLIED:
//! - Zero-allocation JSON parsing with memchr for hot path (Binance trades)
//! - Pre-signed orders for instant submission (no sign latency)
//! - RAW HTTP order posting (bypasses SDK for 10x faster execution)
//! - Parallel order building/signing with tokio::join!
//! - HTTP Keep-Alive connection pooling with TCP_NODELAY
//! - Reduced allocations in logging (pre-allocated String capacity)
//! 
//! FUTURE: io_uring/DPDK kernel bypass would require tokio-uring runtime change

/// Market window duration in seconds (5 minutes)
const MARKET_WINDOW_SECS: i64 = 300;
const MARKET_WINDOW_SECS_F64: f64 = 300.0;

use anyhow::{Result, Context};
use chrono::{DateTime, Local, Utc};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use memchr::memmem;  // Zero-allocation byte searching
use once_cell::sync::Lazy;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
    Frame, Terminal,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use rusqlite::{Connection, params};
use serde::Deserialize;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Write, BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};

// Polymarket SDK for live trading
use alloy::primitives::Address;
use alloy::signers::Signer as _;
use alloy_signer_local::PrivateKeySigner;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::clob::types::{OrderStatusType, OrderType, Side as OrderSide, SignatureType, SignedOrder};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;

const CLOB_ENDPOINT: &str = "https://clob.polymarket.com";
const POLYGON_CHAIN_ID: u64 = 137;

// ============================================================================
// GLOBAL HTTP CLIENT - Keep-Alive Pool with TCP_NODELAY (Optimized for latency)
// ============================================================================

static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .tcp_nodelay(true)              // Disable Nagle's algorithm (critical for latency!)
        .tcp_keepalive(Duration::from_secs(60))  // Longer keepalive for persistent connections
        .pool_max_idle_per_host(20)     // Larger connection pool
        .pool_idle_timeout(Duration::from_secs(120))  // Keep connections alive longer
        .timeout(Duration::from_secs(5))  // Shorter timeout - fail fast
        .connect_timeout(Duration::from_secs(3))  // Fast connect timeout
        .http2_adaptive_window(true)    // HTTP/2 adaptive flow control
        .build()
        .expect("Failed to create HTTP client")
});

// ============================================================================
// ERROR LOGGING - Write to file for debugging order failures
// ============================================================================

fn log_error(msg: &str) {
    use std::io::Write;
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/polyfill_errors.log")
    {
        let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let _ = writeln!(f, "[{}] {}", timestamp, msg);
    }
}

// ============================================================================
// ============================================================================
// LIVE ORDER EXECUTOR - Native Rust Polymarket SDK with Pre-Signed Orders
// ============================================================================

static EXECUTOR: Lazy<tokio::sync::OnceCell<Arc<LiveExecutor>>> = Lazy::new(|| tokio::sync::OnceCell::new());

/// Pre-signed orders ready for instant submission
/// Orders are signed at 99c to guarantee fill at best available price
struct PreSignedOrders {
    up_token: String,
    down_token: String,
    up_buy: Option<SignedOrder>,
    down_buy: Option<SignedOrder>,
    size: Decimal,
    last_refresh: Instant,
}

impl PreSignedOrders {
    fn new() -> Self {
        Self {
            up_token: String::new(),
            down_token: String::new(),
            up_buy: None,
            down_buy: None,
            size: dec!(7.5),
            last_refresh: Instant::now(),
        }
    }
}

struct LiveExecutor {
    client: ClobClient<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    pre_signed: tokio::sync::RwLock<PreSignedOrders>,
}

impl LiveExecutor {
    async fn new() -> Result<Self> {
        // Load credentials from environment
        let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .or_else(|_| std::env::var("PM_PRIVATE_KEY"))
            .expect("POLYMARKET_PRIVATE_KEY or PM_PRIVATE_KEY not set");
        let funder = std::env::var("POLYMARKET_FUNDER")
            .or_else(|_| std::env::var("PM_FUNDER"))
            .expect("POLYMARKET_FUNDER or PM_FUNDER not set");
        // Signature type: 0=EOA, 1=PolyProxy(MagicLink), 2=GnosisSafe(MetaMask proxy)
        // For Magic Link email wallets, use sig_type=1 (Proxy)
        let sig_type: u8 = std::env::var("POLYMARKET_SIGNATURE_TYPE")
            .or_else(|_| std::env::var("POLYMARKET_SIG_TYPE"))
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1);

        // Strip 0x prefix if present
        let pk = private_key.trim().strip_prefix("0x").unwrap_or(private_key.trim());
        let signer = PrivateKeySigner::from_str(pk)
            .map_err(|e| anyhow::anyhow!("Invalid private key: {}", e))?
            .with_chain_id(Some(POLYGON_CHAIN_ID));

        // Log signer and funder for debugging
        log_error(&format!("Signer address: {:?}", signer.address()));
        log_error(&format!("Funder address: {}", funder));
        log_error(&format!("Signature type: {}", sig_type));

        let signature_type = match sig_type {
            0 => SignatureType::Eoa,
            1 => SignatureType::Proxy,
            2 => SignatureType::GnosisSafe,
            _ => SignatureType::Proxy,  // Default to Proxy for Magic Link
        };

        // Create unauthenticated client first
        let unauth = ClobClient::new(CLOB_ENDPOINT, ClobConfig::default())
            .map_err(|e| anyhow::anyhow!("Failed to create CLOB client: {:?}", e))?;

        // Authenticate with funder address
        let funder_addr: Address = funder.parse()
            .map_err(|e| anyhow::anyhow!("Invalid funder address: {:?}", e))?;

        // SDK authentication
        let client = unauth
            .authentication_builder(&signer)
            .signature_type(signature_type)
            .funder(funder_addr)
            .authenticate()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to authenticate: {:?}", e))?;
        
        // Warm up the HTTP connection pool
        let _ = HTTP_CLIENT.get(format!("{}/time", CLOB_ENDPOINT)).send().await;
        eprintln!("[OK] Polymarket executor initialized (SDK mode)");

        Ok(Self { 
            client, 
            signer,
            pre_signed: tokio::sync::RwLock::new(PreSignedOrders::new()),
        })
    }

    /// Pre-sign orders for both UP and DOWN tokens at max price (99c)
    /// This eliminates signing latency when a signal triggers
    /// OPTIMIZED: Signs UP and DOWN orders in PARALLEL
    async fn refresh_pre_signed(&self, up_token: &str, down_token: &str, size: Decimal) -> Result<()> {
        // Build BOTH orders in parallel for speed
        let up_token_owned = up_token.to_string();
        let down_token_owned = down_token.to_string();
        
        // Build UP order
        let up_build_future = async {
            self.client
                .limit_order()
                .token_id(up_token_owned)
                .side(OrderSide::Buy)
                .price(dec!(0.99))
                .size(size)
                .order_type(OrderType::GTC)  // GTC for pre-signed orders (they persist)
                .build()
                .await
        };
        
        // Build DOWN order
        let down_build_future = async {
            self.client
                .limit_order()
                .token_id(down_token_owned)
                .side(OrderSide::Buy)
                .price(dec!(0.99))
                .size(size)
                .order_type(OrderType::GTC)  // GTC for pre-signed orders (they persist)
                .build()
                .await
        };
        
        // Execute builds in PARALLEL
        let (up_result, down_result) = tokio::join!(up_build_future, down_build_future);
        
        let up_signable = up_result.map_err(|e| anyhow::anyhow!("Failed to build UP order: {:?}", e))?;
        let down_signable = down_result.map_err(|e| anyhow::anyhow!("Failed to build DOWN order: {:?}", e))?;

        // Sign BOTH in parallel
        let up_sign_future = self.client.sign(&self.signer, up_signable);
        let down_sign_future = self.client.sign(&self.signer, down_signable);
        
        let (up_signed_result, down_signed_result) = tokio::join!(up_sign_future, down_sign_future);
        
        let up_signed = up_signed_result.map_err(|e| anyhow::anyhow!("Failed to sign UP order: {:?}", e))?;
        let down_signed = down_signed_result.map_err(|e| anyhow::anyhow!("Failed to sign DOWN order: {:?}", e))?;

        // Store pre-signed orders
        {
            let mut ps = self.pre_signed.write().await;
            ps.up_token = up_token.to_string();
            ps.down_token = down_token.to_string();
            ps.up_buy = Some(up_signed);
            ps.down_buy = Some(down_signed);
            ps.size = size;
            ps.last_refresh = Instant::now();
        }

        Ok(())
    }

    /// Submit a pre-signed order via SDK
    /// Returns the fill price (making_amount / taking_amount) on success, None on failure
    /// expected_price: orderbook price to use if order is accepted but not immediately filled
    async fn submit_pre_signed(&self, is_up: bool, expected_price: Option<Decimal>) -> Result<Option<Decimal>> {
        let start = Instant::now();
        let signed_order = {
            let mut ps = self.pre_signed.write().await;
            if is_up {
                ps.up_buy.take()
            } else {
                ps.down_buy.take()
            }
        };
        let lock_time = start.elapsed();

        if let Some(signed) = signed_order {
            let post_start = Instant::now();
            let responses = self.client
                .post_order(signed)
                .await
                .map_err(|e| {
                    log_error(&format!("SDK post_order failed: {:?}", e));
                    anyhow::anyhow!("Failed to post pre-signed order: {:?}", e)
                })?;
            let post_time = post_start.elapsed();

            if let Some(r) = responses.first() {
                log_error(&format!("SDK {} order: lock={}us post={}ms success={} making={} taking={}", 
                    if is_up { "UP" } else { "DN" }, lock_time.as_micros(), post_time.as_millis(),
                    r.success, r.making_amount, r.taking_amount));
                
                // CRITICAL FIX: If success=true, order was accepted by Polymarket
                // Even if taking_amount=0 (not immediately filled), the order WILL fill
                // at 99c GTC. Return the expected price.
                if r.success {
                    if r.taking_amount > dec!(0) {
                        // Immediate fill - use actual price
                        let fill_price = r.making_amount / r.taking_amount;
                        return Ok(Some(fill_price));
                    } else {
                        // Order accepted but pending - use expected orderbook price
                        let fill_price = expected_price.unwrap_or(dec!(0.50)); // Default to 50c if no price
                        log_error(&format!("SDK {} order accepted but pending (taking=0), using expected price {:.2}c", 
                            if is_up { "UP" } else { "DN" }, fill_price * dec!(100)));
                        return Ok(Some(fill_price));
                    }
                }
            }
            return Ok(None);
        }

        Ok(None)
    }

    /// Check if we have a pre-signed order available for the given side
    async fn has_pre_signed(&self, is_up: bool) -> bool {
        let ps = self.pre_signed.read().await;
        if is_up { ps.up_buy.is_some() } else { ps.down_buy.is_some() }
    }

    /// Check if pre-signed order size matches the required size
    /// Returns true if pre-signed exists AND size matches
    async fn has_pre_signed_with_size(&self, is_up: bool, required_size: Decimal) -> bool {
        let ps = self.pre_signed.read().await;
        let has_order = if is_up { ps.up_buy.is_some() } else { ps.down_buy.is_some() };
        has_order && ps.size == required_size
    }

    /// Fallback: Buy shares at market price (GTC at 99c guarantees fill)
    /// Returns the fill price (making_amount / taking_amount) on success, None on failure
    /// NOTE: Using GTC instead of FOK because SDK interprets size differently:
    /// - GTC: size = number of shares
    /// - FOK: size = USDC to spend (causes share imbalance!)
    /// expected_price: orderbook price to use if order is accepted but not immediately filled
    async fn buy(&self, token_id: &str, size: Decimal, is_up: bool, expected_price: Option<Decimal>) -> Result<Option<Decimal>> {
        let start = Instant::now();
        let buy_price = dec!(0.99);
        let min_shares = (dec!(1.05) / buy_price).ceil();
        let order_size = std::cmp::max(size, min_shares);

        let signable = self.client
            .limit_order()
            .token_id(token_id.to_string())
            .side(OrderSide::Buy)
            .price(buy_price)
            .size(order_size)
            .order_type(OrderType::GTC)  // GTC so size means SHARES not USDC
            .build()
            .await
            .map_err(|e| {
                log_error(&format!("Failed to create order: {:?}", e));
                anyhow::anyhow!("Failed to create order: {:?}", e)
            })?;
        let build_time = start.elapsed();

        let sign_start = Instant::now();
        let signed = self.client
            .sign(&self.signer, signable)
            .await
            .map_err(|e| {
                log_error(&format!("Failed to sign order: {:?}", e));
                anyhow::anyhow!("Failed to sign order: {:?}", e)
            })?;
        let sign_time = sign_start.elapsed();

        let post_start = Instant::now();
        let responses = self.client
            .post_order(signed)
            .await
            .map_err(|e| {
                log_error(&format!("Failed to post order: {:?}", e));
                anyhow::anyhow!("Failed to post order: {:?}", e)
            })?;
        let post_time = post_start.elapsed();

        if let Some(r) = responses.first() {
            // Log with detailed timing breakdown
            log_error(&format!("Fallback {} order: build={}ms sign={}ms post={}ms total={}ms success={} making={} taking={}", 
                if is_up { "UP" } else { "DN" },
                build_time.as_millis(), sign_time.as_millis(), post_time.as_millis(),
                start.elapsed().as_millis(), r.success, r.making_amount, r.taking_amount));
            
            // CRITICAL FIX: If success=true, order was accepted by Polymarket
            // Even if taking_amount=0, the GTC order at 99c WILL fill
            if r.success {
                if r.taking_amount > dec!(0) {
                    // Immediate fill - use actual price
                    let fill_price = r.making_amount / r.taking_amount;
                    return Ok(Some(fill_price));
                } else {
                    // Order accepted but pending - use expected orderbook price
                    let fill_price = expected_price.unwrap_or(dec!(0.50)); // Default to 50c if no price
                    log_error(&format!("Fallback {} order accepted but pending (taking=0), using expected price {:.2}c", 
                        if is_up { "UP" } else { "DN" }, fill_price * dec!(100)));
                    return Ok(Some(fill_price));
                }
            }
        } else {
            log_error("Fallback order returned empty response");
        }

        Ok(None)
    }
}

// ============================================================================
// TICK-LEVEL ML MODEL - Trained on 100M ticks, predicts 200ms volatility
// ============================================================================

pub struct MLPredictor {
    // Neural network: 13 -> 64 -> 32 -> 1
    w1: Vec<Vec<f64>>,  // 64x13
    b1: Vec<f64>,       // 64
    w2: Vec<Vec<f64>>,  // 32x64
    b2: Vec<f64>,       // 32
    w3: Vec<f64>,       // 32
    b3: f64,
    means: Vec<f64>,    // 13 feature means
    stds: Vec<f64>,     // 13 feature stds
}

impl MLPredictor {
    pub fn load(model_path: &str, norm_path: &str) -> Result<Self> {
        // Load normalization params
        let norm_file = File::open(norm_path)
            .context(format!("Failed to open {}", norm_path))?;
        let mut norm_reader = BufReader::new(norm_file);
        let mut means_line = String::new();
        let mut stds_line = String::new();
        norm_reader.read_line(&mut means_line)?;
        norm_reader.read_line(&mut stds_line)?;
        
        let means: Vec<f64> = means_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        let stds: Vec<f64> = stds_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        // Load model weights
        let model_file = File::open(model_path)
            .context(format!("Failed to open {}", model_path))?;
        let mut reader = BufReader::new(model_file);
        
        // Skip header line (13 64 32 1)
        let mut header = String::new();
        reader.read_line(&mut header)?;
        
        // Read w1 (64 rows x 13 cols)
        let mut w1 = Vec::with_capacity(64);
        for _ in 0..64 {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let row: Vec<f64> = line.trim().split_whitespace()
                .filter_map(|s| s.parse().ok()).collect();
            w1.push(row);
        }
        
        // Read b1 (64)
        let mut b1_line = String::new();
        reader.read_line(&mut b1_line)?;
        let b1: Vec<f64> = b1_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        // Read w2 (32 rows x 64 cols)
        let mut w2 = Vec::with_capacity(32);
        for _ in 0..32 {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let row: Vec<f64> = line.trim().split_whitespace()
                .filter_map(|s| s.parse().ok()).collect();
            w2.push(row);
        }
        
        // Read b2 (32)
        let mut b2_line = String::new();
        reader.read_line(&mut b2_line)?;
        let b2: Vec<f64> = b2_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        // Read w3 (32)
        let mut w3_line = String::new();
        reader.read_line(&mut w3_line)?;
        let w3: Vec<f64> = w3_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        // Read b3
        let mut b3_line = String::new();
        reader.read_line(&mut b3_line)?;
        let b3: f64 = b3_line.trim().parse().unwrap_or(0.0);
        
        eprintln!("Loaded tick-level ML model: {} -> {} -> {} -> 1", 
            means.len(), w1.len(), w2.len());
        
        Ok(Self { w1, b1, w2, b2, w3, b3, means, stds })
    }
    
    pub fn new() -> Self {
        // Fallback to heuristic if model files not found
        Self {
            w1: vec![vec![0.0; 13]; 64],
            b1: vec![0.0; 64],
            w2: vec![vec![0.0; 64]; 32],
            b2: vec![0.0; 32],
            w3: vec![0.0; 32],
            b3: 5.0, // Default prediction ~$5
            means: vec![0.0; 13],
            stds: vec![1.0; 13],
        }
    }

    pub fn predict(&self, features: &TickFeatures) -> f32 {
        let raw = features.to_vec();
        
        // Normalize features
        let x: Vec<f64> = raw.iter().enumerate()
            .map(|(i, &v)| {
                if i < self.means.len() && self.stds[i] > 1e-8 {
                    (v - self.means[i]) / self.stds[i]
                } else {
                    v
                }
            })
            .collect();
        
        // Layer 1: ReLU(W1*x + b1)
        let h1: Vec<f64> = self.w1.iter().enumerate().map(|(i, w)| {
            let sum: f64 = w.iter().zip(x.iter()).map(|(wi, xi)| wi * xi).sum();
            (sum + self.b1.get(i).unwrap_or(&0.0)).max(0.0)
        }).collect();
        
        // Layer 2: ReLU(W2*h1 + b2)
        let h2: Vec<f64> = self.w2.iter().enumerate().map(|(i, w)| {
            let sum: f64 = w.iter().zip(h1.iter()).map(|(wi, hi)| wi * hi).sum();
            (sum + self.b2.get(i).unwrap_or(&0.0)).max(0.0)
        }).collect();
        
        // Output: Linear(W3*h2 + b3) with floor
        // Note: During quiet periods, network outputs negative values that would clamp to 0
        // Add small floor and take absolute value for stability
        let raw_out: f64 = self.w3.iter().zip(h2.iter())
            .map(|(w, h)| w * h).sum::<f64>() + self.b3;
        
        // Apply exponential transform to map any output to positive range
        // This is more stable than ReLU for volatility prediction
        let out = if raw_out > 0.0 {
            raw_out
        } else {
            // For negative outputs, use softplus-like transform to avoid hard cutoff
            (1.0 + raw_out.exp()).ln()  // ln(1 + e^x) ~ x for large x, ~ 0.69 for x=0
        };
        
        out as f32  // Volatility in dollars
    }
}

// ============================================================================
// ONLINE CALIBRATION - EMA-based recalibration from live trade outcomes
// ============================================================================

/// Online calibration using exponential moving averages in probability bins
/// Updates from resolved trade outcomes to adapt to market regime changes
#[derive(Clone, Debug)]
pub struct CalibrationBins {
    // 10 bins: [0.0-0.1], [0.1-0.2], ..., [0.9-1.0]
    bin_emas: [f64; 10],      // EMA of actual outcomes per bin
    bin_counts: [u32; 10],    // Number of samples per bin
    alpha: f64,               // EMA smoothing factor (0.1 = last ~10 dominate)
    total_samples: u32,       // Total resolved trades
    blend_threshold: u32,     // Samples needed before fully blending (50)
}

impl Default for CalibrationBins {
    fn default() -> Self {
        Self {
            // Initialize bins to diagonal (0.05, 0.15, ..., 0.95) = "trust raw model"
            bin_emas: [0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95],
            bin_counts: [0; 10],
            alpha: 0.15,  // Faster adaptation: ~7 samples dominate
            total_samples: 0,
            blend_threshold: 30,  // Start blending after 30 trades (~2-3 hours)
        }
    }
}

impl CalibrationBins {
    /// Update calibration with a resolved trade outcome
    /// raw_prob: The raw sigmoid output when trade was entered (0.0-1.0)
    /// outcome: 1.0 if UP won, 0.0 if DOWN won
    pub fn update(&mut self, raw_prob: f64, outcome: f64) {
        let bin_idx = ((raw_prob * 10.0) as usize).min(9);
        
        // EMA update: new_ema = alpha * outcome + (1 - alpha) * old_ema
        self.bin_emas[bin_idx] = self.alpha * outcome + (1.0 - self.alpha) * self.bin_emas[bin_idx];
        self.bin_counts[bin_idx] += 1;
        self.total_samples += 1;
    }
    
    /// Get live-calibrated probability for a raw prediction
    /// Returns linearly interpolated value from bin EMAs
    pub fn calibrate(&self, raw_prob: f64) -> f64 {
        // Map raw_prob to bin centers and interpolate
        let scaled = raw_prob * 10.0;  // 0.0-10.0
        let bin_idx = (scaled as usize).min(9);
        
        if bin_idx >= 9 {
            return self.bin_emas[9];
        }
        
        // Linear interpolation between adjacent bins
        let frac = scaled - bin_idx as f64;  // Fractional part within bin
        let y0 = self.bin_emas[bin_idx];
        let y1 = self.bin_emas[bin_idx + 1];
        y0 + frac * (y1 - y0)
    }
    
    /// Get blend factor (0.0 = all pretrained, 1.0 = all live)
    pub fn blend_factor(&self) -> f64 {
        (self.total_samples as f64 / self.blend_threshold as f64).min(1.0)
    }
    
    /// Get summary stats for TUI display
    pub fn stats(&self) -> (u32, f64) {
        (self.total_samples, self.blend_factor())
    }
}

// ============================================================================
// LIQUIDATION TRACKER - Tracks BTC liquidations for cascade detection
// ============================================================================

#[derive(Clone, Debug)]
pub struct Liquidation {
    pub timestamp_ms: i64,
    pub side: bool,      // true = long liquidated, false = short liquidated
    pub quantity: f64,   // BTC amount
    pub price: f64,
}

pub struct LiquidationTracker {
    liquidations: VecDeque<Liquidation>,
    lookback_ms: i64,
}

impl LiquidationTracker {
    pub fn new() -> Self {
        Self {
            liquidations: VecDeque::with_capacity(1000),
            lookback_ms: 60_000, // 60 second lookback
        }
    }
    
    pub fn add(&mut self, timestamp_ms: i64, side: bool, quantity: f64, price: f64) {
        self.liquidations.push_back(Liquidation {
            timestamp_ms,
            side,
            quantity,
            price,
        });
        self.prune(timestamp_ms);
    }
    
    fn prune(&mut self, current_ts: i64) {
        let cutoff = current_ts - self.lookback_ms;
        while let Some(front) = self.liquidations.front() {
            if front.timestamp_ms < cutoff {
                self.liquidations.pop_front();
            } else {
                break;
            }
        }
    }
    
    /// Compute liquidation features for direction prediction
    pub fn compute_features(&self, current_ts: i64) -> LiquidationFeatures {
        self.prune_readonly(current_ts);
        
        let recent: Vec<&Liquidation> = self.liquidations.iter()
            .filter(|l| l.timestamp_ms >= current_ts - self.lookback_ms)
            .collect();
        
        if recent.is_empty() {
            return LiquidationFeatures::default();
        }
        
        let long_liqs: Vec<&&Liquidation> = recent.iter().filter(|l| l.side).collect();
        let short_liqs: Vec<&&Liquidation> = recent.iter().filter(|l| !l.side).collect();
        
        let long_volume: f64 = long_liqs.iter().map(|l| l.quantity * l.price).sum();
        let short_volume: f64 = short_liqs.iter().map(|l| l.quantity * l.price).sum();
        let total_volume = long_volume + short_volume;
        
        // Imbalance: positive = more longs liquidated = bearish
        let liq_imbalance = if total_volume > 0.0 {
            (long_volume - short_volume) / total_volume
        } else {
            0.0
        };
        
        // Cascade detection: multiple large liquidations in short time
        let large_liqs: Vec<&&Liquidation> = recent.iter()
            .filter(|l| l.quantity * l.price > 50_000.0)  // >$50k
            .collect();
        
        let cascade_score = if large_liqs.len() >= 3 {
            large_liqs.len() as f64 * (total_volume / 1_000_000.0)
        } else {
            0.0
        };
        
        LiquidationFeatures {
            liq_count_60s: recent.len() as f64,
            liq_volume_60s: total_volume / 1_000_000.0,  // In millions
            liq_imbalance_60s: liq_imbalance,
            liq_cascade_score: cascade_score,
        }
    }
    
    fn prune_readonly(&self, _current_ts: i64) {
        // Read-only version for compute - actual pruning happens in add()
    }
}

#[derive(Clone, Debug, Default)]
pub struct LiquidationFeatures {
    pub liq_count_60s: f64,
    pub liq_volume_60s: f64,
    pub liq_imbalance_60s: f64,
    pub liq_cascade_score: f64,
}

// ============================================================================
// DIRECTION ML MODEL - Predicts P(UP) for 1-hour conditional probability market
// Architecture: 27 -> 64 -> 32 -> 16 -> 1 with isotonic calibration
// Features: Displacement(10) + Microstructure(10) + Reversal(3) + Liquidations(4)
// ============================================================================

pub struct DirectionPredictor {
    // Dynamic layer architecture
    layers: Vec<(Vec<Vec<f64>>, Vec<f64>)>,  // [(weights, biases), ...]
    output_weights: Vec<f64>,
    output_bias: f64,
    n_features: usize,
    means: Vec<f64>,
    stds: Vec<f64>,
    calib_x: Vec<f64>,
    calib_y: Vec<f64>,
}

impl DirectionPredictor {
    pub fn load(model_path: &str, norm_path: &str, calib_path: &str) -> Result<Self> {
        // Load normalization params
        let norm_file = File::open(norm_path)
            .context(format!("Failed to open {}", norm_path))?;
        let mut norm_reader = BufReader::new(norm_file);
        let mut means_line = String::new();
        let mut stds_line = String::new();
        norm_reader.read_line(&mut means_line)?;
        norm_reader.read_line(&mut stds_line)?;
        
        let means: Vec<f64> = means_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        let stds: Vec<f64> = stds_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        // Load isotonic calibration (format: x values on line 1, y values on line 2)
        let calib_file = File::open(calib_path)
            .context(format!("Failed to open {}", calib_path))?;
        let calib_reader = BufReader::new(calib_file);
        let mut calib_x = Vec::new();
        let mut calib_y = Vec::new();
        
        for line in calib_reader.lines() {
            let line = line?;
            let parts: Vec<f64> = line.trim().split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect();
            if parts.len() >= 2 {
                calib_x.push(parts[0]);
                calib_y.push(parts[1]);
            }
        }
        
        // Load model weights - dynamic architecture from header
        let model_file = File::open(model_path)
            .context(format!("Failed to open {}", model_path))?;
        let mut reader = BufReader::new(model_file);
        
        // Parse header: "27 64 32 16 1" -> [27, 64, 32, 16, 1]
        let mut header = String::new();
        reader.read_line(&mut header)?;
        let dims: Vec<usize> = header.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        if dims.len() < 3 {
            anyhow::bail!("Invalid model header: {}", header);
        }
        
        let n_features = dims[0];
        let mut layers = Vec::new();
        
        // Read hidden layers (all except last dim which is output=1)
        for i in 0..dims.len() - 2 {
            let in_dim = dims[i];
            let out_dim = dims[i + 1];
            
            // Read weights (out_dim rows x in_dim cols)
            let mut weights = Vec::with_capacity(out_dim);
            for _ in 0..out_dim {
                let mut line = String::new();
                reader.read_line(&mut line)?;
                let row: Vec<f64> = line.trim().split_whitespace()
                    .filter_map(|s| s.parse().ok()).collect();
                weights.push(row);
            }
            
            // Read biases (out_dim)
            let mut bias_line = String::new();
            reader.read_line(&mut bias_line)?;
            let biases: Vec<f64> = bias_line.trim().split_whitespace()
                .filter_map(|s| s.parse().ok()).collect();
            
            layers.push((weights, biases));
        }
        
        // Read output layer weights (last hidden dim)
        let last_hidden = dims[dims.len() - 2];
        let mut output_line = String::new();
        reader.read_line(&mut output_line)?;
        let output_weights: Vec<f64> = output_line.trim().split_whitespace()
            .filter_map(|s| s.parse().ok()).collect();
        
        // Read output bias
        let mut bias_line = String::new();
        reader.read_line(&mut bias_line)?;
        let output_bias: f64 = bias_line.trim().parse().unwrap_or(0.0);
        
        eprintln!("[OK] Direction model loaded: {} features, {} hidden layers, {} calibration points", 
            n_features, layers.len(), calib_x.len());
        
        Ok(Self { 
            layers, 
            output_weights, 
            output_bias, 
            n_features,
            means, 
            stds, 
            calib_x, 
            calib_y 
        })
    }
    
    pub fn new() -> Self {
        // Fallback to neutral predictions if model files not found
        Self {
            layers: vec![(vec![vec![0.0; 27]; 64], vec![0.0; 64])],
            output_weights: vec![0.0; 64],
            output_bias: 0.0,
            n_features: 27,
            means: vec![0.0; 27],
            stds: vec![1.0; 27],
            calib_x: vec![0.0, 1.0],
            calib_y: vec![0.5, 0.5], // Always 50% if no model
        }
    }
    
    /// Predict P(UP) with isotonic calibration
    pub fn predict(&self, features: &DirectionFeatures) -> f32 {
        let (calibrated, _raw) = self.predict_with_raw(features);
        calibrated
    }
    
    /// Predict with both calibrated and raw outputs
    pub fn predict_with_raw(&self, features: &DirectionFeatures) -> (f32, f32) {
        let raw = features.to_vec();
        
        // Normalize features
        let mut x: Vec<f64> = raw.iter().enumerate()
            .map(|(i, &v)| {
                if i < self.means.len() && i < self.stds.len() && self.stds[i].abs() > 1e-8 {
                    (v - self.means[i]) / self.stds[i]
                } else {
                    v
                }
            })
            .collect();
        
        // Forward pass through hidden layers with ReLU
        for (weights, biases) in &self.layers {
            let mut h: Vec<f64> = Vec::with_capacity(weights.len());
            for (i, w) in weights.iter().enumerate() {
                let sum: f64 = w.iter().zip(x.iter()).map(|(wi, xi)| wi * xi).sum();
                let bias = biases.get(i).unwrap_or(&0.0);
                h.push((sum + bias).max(0.0));  // ReLU
            }
            x = h;
        }
        
        // Output layer: Sigmoid
        let logit: f64 = self.output_weights.iter().zip(x.iter())
            .map(|(w, h)| w * h).sum::<f64>() + self.output_bias;
        let raw_prob = 1.0 / (1.0 + (-logit.clamp(-500.0, 500.0)).exp());
        
        // Apply isotonic calibration
        let calibrated = self.interpolate(raw_prob);
        
        (calibrated as f32, raw_prob as f32)
    }
    
    /// Predict with online calibration blending
    /// Blends pretrained isotonic with live EMA calibration based on sample count
    pub fn predict_with_live_calib(&self, features: &DirectionFeatures, live_calib: &CalibrationBins) -> (f32, f32) {
        let (_pretrained, raw_prob) = self.predict_with_raw(features);
        
        // Get pretrained isotonic calibration
        let pretrained_calib = self.interpolate(raw_prob as f64) as f32;
        
        // Get live EMA calibration
        let live_calib_prob = live_calib.calibrate(raw_prob as f64) as f32;
        
        // Blend based on sample count (0% live at 0 samples, 100% live at 30+ samples)
        let blend = live_calib.blend_factor() as f32;
        let blended = (1.0 - blend) * pretrained_calib + blend * live_calib_prob;
        
        (blended, raw_prob)
    }
    
    fn interpolate(&self, x: f64) -> f64 {
        if self.calib_x.is_empty() || self.calib_y.is_empty() {
            return x;
        }
        if x <= self.calib_x[0] {
            return self.calib_y[0];
        }
        if x >= *self.calib_x.last().unwrap() {
            return *self.calib_y.last().unwrap();
        }
        // Binary search for interval
        for i in 1..self.calib_x.len() {
            if x <= self.calib_x[i] {
                let x0 = self.calib_x[i - 1];
                let x1 = self.calib_x[i];
                let y0 = self.calib_y[i - 1];
                let y1 = self.calib_y[i];
                if (x1 - x0).abs() < 1e-10 {
                    return y0;
                }
                return y0 + (y1 - y0) * (x - x0) / (x1 - x0);
            }
        }
        *self.calib_y.last().unwrap()
    }
}

// Direction prediction features (27 features for 1-hour conditional probability)
// Displacement(10) + Microstructure(10) + Reversal(3) + Liquidations(4)
#[derive(Clone, Debug)]
pub struct DirectionFeatures {
    // Displacement features (10) - price vs reference at window start
    pub displacement_usd: f64,          // current - reference price in USD
    pub displacement_pct: f64,          // displacement as percentage
    pub displacement_speed: f64,        // USD/second rate of displacement
    pub max_displacement_up: f64,       // max upward move from reference
    pub max_displacement_down: f64,     // max downward move from reference
    pub reversal_ratio: f64,            // how much retraced from max
    pub recent_displacement: f64,       // displacement in last 10s
    pub displacement_momentum: f64,     // acceleration of displacement
    pub elapsed_pct: f64,               // % of 1-hour window elapsed
    pub remaining_pct: f64,             // % of 1-hour window remaining
    
    // Microstructure features (10) - tick-level dynamics
    pub price_std: f64,                 // standard deviation of prices
    pub price_range: f64,               // high - low
    pub momentum: f64,                  // sum of returns
    pub volatility: f64,                // std of returns
    pub volume_imbalance: f64,          // (buy - sell) / total
    pub large_trade_imbalance: f64,     // large trade directional bias
    pub tick_rate: f64,                 // ticks per second
    pub tick_acceleration: f64,         // change in tick rate
    pub price_vs_vwap: f64,             // last price vs VWAP
    pub order_flow_toxicity: f64,       // consecutive same-direction trades
    
    // Reversal probability features (3) - likelihood of reverting
    pub reversal_prob: f64,             // estimated P(revert to reference)
    pub normalized_displacement: f64,   // displacement / expected_move
    pub time_adjusted_distance: f64,    // displacement / sqrt(time_remaining)
    
    // Liquidation features (4) - cascade detection
    pub liq_count_60s: f64,             // liquidations in last 60s
    pub liq_volume_60s: f64,            // liquidation volume in millions
    pub liq_imbalance_60s: f64,         // long vs short liquidation bias
    pub liq_cascade_score: f64,         // cascade detection score
}

impl DirectionFeatures {
    fn to_vec(&self) -> Vec<f64> {
        vec![
            // Displacement (10)
            self.displacement_usd,
            self.displacement_pct,
            self.displacement_speed,
            self.max_displacement_up,
            self.max_displacement_down,
            self.reversal_ratio,
            self.recent_displacement,
            self.displacement_momentum,
            self.elapsed_pct,
            self.remaining_pct,
            // Microstructure (10)
            self.price_std,
            self.price_range,
            self.momentum,
            self.volatility,
            self.volume_imbalance,
            self.large_trade_imbalance,
            self.tick_rate,
            self.tick_acceleration,
            self.price_vs_vwap,
            self.order_flow_toxicity,
            // Reversal probability (3)
            self.reversal_prob,
            self.normalized_displacement,
            self.time_adjusted_distance,
            // Liquidations (4)
            self.liq_count_60s,
            self.liq_volume_60s,
            self.liq_imbalance_60s,
            self.liq_cascade_score,
        ]
    }
}

// Direction Feature Engine - computes all 27 features for direction prediction
#[derive(Clone)]
struct DirectionFeatureEngine {
    ticks: VecDeque<Tick>,
    window_ms: u64,
    // Track max displacement for reversal calculation
    max_price_seen: f64,
    min_price_seen: f64,
    // Track recent tick rates for acceleration
    prev_tick_rate: f64,
}

impl DirectionFeatureEngine {
    fn new(window_ms: u64) -> Self {
        Self {
            ticks: VecDeque::with_capacity(10000),
            window_ms,
            max_price_seen: 0.0,
            min_price_seen: f64::MAX,
            prev_tick_rate: 0.0,
        }
    }
    
    fn push_tick(&mut self, timestamp_ms: u64, price: f64, quantity: f64, is_buyer: bool) {
        self.ticks.push_back(Tick {
            timestamp_us: (timestamp_ms * 1000) as i64,
            price,
            quantity,
            is_buyer,
        });
        
        // Track extremes
        self.max_price_seen = self.max_price_seen.max(price);
        if self.min_price_seen == f64::MAX {
            self.min_price_seen = price;
        }
        self.min_price_seen = self.min_price_seen.min(price);
        
        self.prune();
    }
    
    fn prune(&mut self) {
        if self.ticks.is_empty() {
            return;
        }
        let last_ts_ms = self.ticks.back().unwrap().timestamp_us / 1000;
        let cutoff = last_ts_ms - self.window_ms as i64;
        while let Some(front) = self.ticks.front() {
            if front.timestamp_us / 1000 < cutoff {
                self.ticks.pop_front();
            } else {
                break;
            }
        }
    }
    
    /// Reset for new 1-hour window
    fn reset_window(&mut self, initial_price: f64) {
        self.max_price_seen = initial_price;
        self.min_price_seen = initial_price;
    }
    
    /// Compute all 27 features
    /// reference_price: price at start of 1-hour window
    /// window_start_ms: timestamp when 1-hour window started
    /// current_ts_ms: current timestamp
    /// liq_features: liquidation features from LiquidationTracker
    fn compute_features_full(
        &mut self,
        reference_price: f64,
        window_start_ms: i64,
        current_ts_ms: i64,
        liq_features: &LiquidationFeatures,
    ) -> Option<DirectionFeatures> {
        if self.ticks.len() < 10 || reference_price <= 0.0 {
            return None;
        }
        
        let prices: Vec<f64> = self.ticks.iter().map(|t| t.price).collect();
        let qtys: Vec<f64> = self.ticks.iter().map(|t| t.quantity).collect();
        let timestamps: Vec<i64> = self.ticks.iter().map(|t| t.timestamp_us / 1000).collect();
        let is_buyers: Vec<bool> = self.ticks.iter().map(|t| t.is_buyer).collect();
        
        let n = prices.len();
        let last_price = *prices.last().unwrap();
        let first_ts = timestamps[0];
        let last_ts = *timestamps.last().unwrap();
        let duration_ms = (last_ts - first_ts).max(1) as f64;
        
        // ========== DISPLACEMENT FEATURES (10) ==========
        let displacement_usd = last_price - reference_price;
        let displacement_pct = displacement_usd / reference_price * 100.0;
        
        // Displacement speed (USD per second)
        let elapsed_from_window = (current_ts_ms - window_start_ms).max(1) as f64 / 1000.0;
        let displacement_speed = displacement_usd / elapsed_from_window;
        
        // Max displacements from reference
        let max_displacement_up = self.max_price_seen - reference_price;
        let max_displacement_down = reference_price - self.min_price_seen;
        
        // Reversal ratio: how much has it retraced from the extreme?
        let reversal_ratio = if displacement_usd > 0.0 && max_displacement_up > 0.0 {
            1.0 - (displacement_usd / max_displacement_up)
        } else if displacement_usd < 0.0 && max_displacement_down > 0.0 {
            1.0 - (displacement_usd.abs() / max_displacement_down)
        } else {
            0.0
        };
        
        // Recent displacement (last 10 seconds)
        let cutoff_10s = last_ts - 10_000;
        let recent_prices: Vec<f64> = self.ticks.iter()
            .filter(|t| t.timestamp_us / 1000 >= cutoff_10s)
            .map(|t| t.price)
            .collect();
        let recent_displacement = if !recent_prices.is_empty() {
            recent_prices.last().unwrap() - recent_prices[0]
        } else {
            0.0
        };
        
        // Displacement momentum (acceleration)
        let mid_ts = first_ts + (last_ts - first_ts) / 2;
        let first_half: Vec<f64> = self.ticks.iter()
            .filter(|t| t.timestamp_us / 1000 < mid_ts)
            .map(|t| t.price - reference_price)
            .collect();
        let second_half: Vec<f64> = self.ticks.iter()
            .filter(|t| t.timestamp_us / 1000 >= mid_ts)
            .map(|t| t.price - reference_price)
            .collect();
        let displacement_momentum = if !first_half.is_empty() && !second_half.is_empty() {
            let first_avg: f64 = first_half.iter().sum::<f64>() / first_half.len() as f64;
            let second_avg: f64 = second_half.iter().sum::<f64>() / second_half.len() as f64;
            second_avg - first_avg
        } else {
            0.0
        };
        
        // Window timing
        let window_ms: i64 = 15 * 60 * 1000;
        let elapsed_ms = current_ts_ms - window_start_ms;
        let elapsed_pct = (elapsed_ms as f64 / window_ms as f64).min(1.0).max(0.0);
        let remaining_pct = 1.0 - elapsed_pct;
        
        // ========== MICROSTRUCTURE FEATURES (10) ==========
        let mean_price: f64 = prices.iter().sum::<f64>() / n as f64;
        let price_std = (prices.iter().map(|p| (p - mean_price).powi(2)).sum::<f64>() / n as f64).sqrt();
        let price_range = prices.iter().cloned().fold(f64::MIN, f64::max) 
                        - prices.iter().cloned().fold(f64::MAX, f64::min);
        
        // Returns and momentum
        let returns: Vec<f64> = prices.windows(2).map(|w| (w[1] - w[0]) / w[0]).collect();
        let momentum: f64 = returns.iter().sum();
        let volatility = if returns.len() > 1 {
            let mean_ret: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
            (returns.iter().map(|r| (r - mean_ret).powi(2)).sum::<f64>() / returns.len() as f64).sqrt()
        } else {
            0.0
        };
        
        // Volume analysis
        let buy_vol: f64 = self.ticks.iter().filter(|t| t.is_buyer).map(|t| t.quantity).sum();
        let sell_vol: f64 = self.ticks.iter().filter(|t| !t.is_buyer).map(|t| t.quantity).sum();
        let total_vol = buy_vol + sell_vol;
        let volume_imbalance = (buy_vol - sell_vol) / (total_vol + 1e-10);
        
        // Large trade detection
        let median_qty = {
            let mut sorted_qtys = qtys.clone();
            sorted_qtys.sort_by(|a, b| a.partial_cmp(b).unwrap());
            sorted_qtys[sorted_qtys.len() / 2]
        };
        let large_threshold = median_qty * 3.0;
        let large_buys = self.ticks.iter().filter(|t| t.is_buyer && t.quantity > large_threshold).count();
        let large_sells = self.ticks.iter().filter(|t| !t.is_buyer && t.quantity > large_threshold).count();
        let large_trade_imbalance = (large_buys as f64 - large_sells as f64) / (n as f64 + 1.0);
        
        // Tick rate and acceleration
        let tick_rate = n as f64 / (duration_ms / 1000.0).max(0.001);
        let tick_acceleration = tick_rate - self.prev_tick_rate;
        self.prev_tick_rate = tick_rate;
        
        // VWAP
        let vwap: f64 = prices.iter().zip(qtys.iter())
            .map(|(p, q)| p * q).sum::<f64>() / (total_vol + 1e-10);
        let price_vs_vwap = (last_price - vwap) / (vwap + 1e-10);
        
        // Order flow toxicity (consecutive same-direction trades)
        let mut buy_run = 0i32;
        let mut sell_run = 0i32;
        let mut max_buy_run = 0i32;
        let mut max_sell_run = 0i32;
        for &is_buy in &is_buyers {
            if is_buy {
                buy_run += 1;
                sell_run = 0;
                max_buy_run = max_buy_run.max(buy_run);
            } else {
                sell_run += 1;
                buy_run = 0;
                max_sell_run = max_sell_run.max(sell_run);
            }
        }
        let order_flow_toxicity = (max_buy_run - max_sell_run) as f64 / n as f64;
        
        // ========== REVERSAL PROBABILITY FEATURES (3) ==========
        let remaining_sec = (remaining_pct * MARKET_WINDOW_SECS_F64).max(0.1);  // 5min = 300s
        let vol_per_sec = if tick_rate > 0.0 { volatility * tick_rate } else { 0.1 };
        let expected_move = vol_per_sec * remaining_sec.sqrt();
        
        let normalized_displacement = if expected_move > 0.0 {
            displacement_usd.abs() / expected_move
        } else {
            10.0
        };
        
        let time_adjusted_distance = displacement_usd.abs() / (remaining_sec.sqrt() + 0.1);
        
        // Reversal probability: logistic estimate
        let reversal_prob = 1.0 / (1.0 + (2.0 * normalized_displacement - 2.0).exp());
        
        Some(DirectionFeatures {
            // Displacement (10)
            displacement_usd,
            displacement_pct,
            displacement_speed,
            max_displacement_up,
            max_displacement_down,
            reversal_ratio,
            recent_displacement,
            displacement_momentum,
            elapsed_pct,
            remaining_pct,
            // Microstructure (10)
            price_std,
            price_range,
            momentum,
            volatility,
            volume_imbalance,
            large_trade_imbalance,
            tick_rate,
            tick_acceleration,
            price_vs_vwap,
            order_flow_toxicity,
            // Reversal probability (3)
            reversal_prob,
            normalized_displacement,
            time_adjusted_distance,
            // Liquidations (4)
            liq_count_60s: liq_features.liq_count_60s,
            liq_volume_60s: liq_features.liq_volume_60s,
            liq_imbalance_60s: liq_features.liq_imbalance_60s,
            liq_cascade_score: liq_features.liq_cascade_score,
        })
    }
    
    fn tick_count(&self) -> usize {
        self.ticks.len()
    }
}

// ============================================================================
// INTRA-WINDOW ML MODEL - Predicts P(UP wins) for exit decisions
// Architecture: 15 -> 64 -> 32 -> 16 -> 1 with BatchNorm on layers 1-2
// ============================================================================

pub struct IntraWindowPredictor {
    // Layer 1: 15 -> 64
    w1: Vec<Vec<f64>>,  // 64x15
    b1: Vec<f64>,       // 64
    bn1_gamma: Vec<f64>,
    bn1_beta: Vec<f64>,
    bn1_mean: Vec<f64>,
    bn1_var: Vec<f64>,
    
    // Layer 2: 64 -> 32
    w2: Vec<Vec<f64>>,  // 32x64
    b2: Vec<f64>,       // 32
    bn2_gamma: Vec<f64>,
    bn2_beta: Vec<f64>,
    bn2_mean: Vec<f64>,
    bn2_var: Vec<f64>,
    
    // Layer 3: 32 -> 16
    w3: Vec<Vec<f64>>,  // 16x32
    b3: Vec<f64>,       // 16
    
    // Layer 4: 16 -> 1
    w4: Vec<f64>,       // 16
    b4: f64,
    
    // Normalization
    means: Vec<f64>,    // 15 feature means
    stds: Vec<f64>,     // 15 feature stds
}

impl IntraWindowPredictor {
    pub fn load(model_path: &str, norm_path: &str) -> Result<Self> {
        // Load normalization params (CSV format: name,mean,std)
        let norm_file = File::open(norm_path)
            .context(format!("Failed to open {}", norm_path))?;
        let norm_reader = BufReader::new(norm_file);
        
        let mut means = Vec::with_capacity(15);
        let mut stds = Vec::with_capacity(15);
        
        for line in norm_reader.lines() {
            let line = line?;
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 3 {
                means.push(parts[1].parse().unwrap_or(0.0));
                stds.push(parts[2].parse().unwrap_or(1.0));
            }
        }
        
        // Load model weights (one value per line)
        let model_file = File::open(model_path)
            .context(format!("Failed to open {}", model_path))?;
        let model_reader = BufReader::new(model_file);
        
        let values: Vec<f64> = model_reader.lines()
            .filter_map(|l| l.ok())
            .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
            .filter_map(|l| l.trim().parse().ok())
            .collect();
        
        let mut idx = 0;
        
        // Layer 1: W1 (64*15) + B1 (64) + BN1 (64*4)
        let mut w1 = Vec::with_capacity(64);
        for i in 0..64 {
            w1.push(values[idx + i*15..idx + (i+1)*15].to_vec());
        }
        idx += 64 * 15;
        
        let b1 = values[idx..idx+64].to_vec();
        idx += 64;
        let bn1_gamma = values[idx..idx+64].to_vec();
        idx += 64;
        let bn1_beta = values[idx..idx+64].to_vec();
        idx += 64;
        let bn1_mean = values[idx..idx+64].to_vec();
        idx += 64;
        let bn1_var = values[idx..idx+64].to_vec();
        idx += 64;
        
        // Layer 2: W2 (32*64) + B2 (32) + BN2 (32*4)
        let mut w2 = Vec::with_capacity(32);
        for i in 0..32 {
            w2.push(values[idx + i*64..idx + (i+1)*64].to_vec());
        }
        idx += 32 * 64;
        
        let b2 = values[idx..idx+32].to_vec();
        idx += 32;
        let bn2_gamma = values[idx..idx+32].to_vec();
        idx += 32;
        let bn2_beta = values[idx..idx+32].to_vec();
        idx += 32;
        let bn2_mean = values[idx..idx+32].to_vec();
        idx += 32;
        let bn2_var = values[idx..idx+32].to_vec();
        idx += 32;
        
        // Layer 3: W3 (16*32) + B3 (16)
        let mut w3 = Vec::with_capacity(16);
        for i in 0..16 {
            w3.push(values[idx + i*32..idx + (i+1)*32].to_vec());
        }
        idx += 16 * 32;
        
        let b3 = values[idx..idx+16].to_vec();
        idx += 16;
        
        // Layer 4: W4 (16) + B4 (1)
        let w4 = values[idx..idx+16].to_vec();
        idx += 16;
        let b4 = values[idx];
        
        eprintln!("[OK] Loaded intra-window model: 15 -> 64 -> 32 -> 16 -> 1");
        
        Ok(Self { 
            w1, b1, bn1_gamma, bn1_beta, bn1_mean, bn1_var,
            w2, b2, bn2_gamma, bn2_beta, bn2_mean, bn2_var,
            w3, b3, w4, b4, means, stds 
        })
    }
    
    pub fn new() -> Self {
        // Fallback - returns 0.5 (uncertain)
        Self {
            w1: vec![vec![0.0; 15]; 64],
            b1: vec![0.0; 64],
            bn1_gamma: vec![1.0; 64],
            bn1_beta: vec![0.0; 64],
            bn1_mean: vec![0.0; 64],
            bn1_var: vec![1.0; 64],
            w2: vec![vec![0.0; 64]; 32],
            b2: vec![0.0; 32],
            bn2_gamma: vec![1.0; 32],
            bn2_beta: vec![0.0; 32],
            bn2_mean: vec![0.0; 32],
            bn2_var: vec![1.0; 32],
            w3: vec![vec![0.0; 32]; 16],
            b3: vec![0.0; 16],
            w4: vec![0.0; 16],
            b4: 0.0,
            means: vec![0.0; 15],
            stds: vec![1.0; 15],
        }
    }
    
    /// Predict P(UP wins) given intra-window features
    /// Returns probability 0.0-1.0
    pub fn predict(&self, features: &IntraWindowFeatures) -> f64 {
        let raw = features.to_vec();
        let eps = 1e-5;
        
        // Normalize features
        let x: Vec<f64> = raw.iter().enumerate()
            .map(|(i, &v)| {
                if i < self.means.len() && self.stds[i].abs() > eps {
                    (v - self.means[i]) / self.stds[i]
                } else {
                    v
                }
            })
            .collect();
        
        // Layer 1: Linear -> ReLU -> BatchNorm
        let h1_pre: Vec<f64> = self.w1.iter().enumerate().map(|(i, w)| {
            let sum: f64 = w.iter().zip(x.iter()).map(|(wi, xi)| wi * xi).sum();
            sum + self.b1[i]
        }).collect();
        
        let h1_relu: Vec<f64> = h1_pre.iter().map(|&v| v.max(0.0)).collect();
        
        let h1: Vec<f64> = h1_relu.iter().enumerate().map(|(i, &v)| {
            self.bn1_gamma[i] * (v - self.bn1_mean[i]) / (self.bn1_var[i] + eps).sqrt() + self.bn1_beta[i]
        }).collect();
        
        // Layer 2: Linear -> ReLU -> BatchNorm
        let h2_pre: Vec<f64> = self.w2.iter().enumerate().map(|(i, w)| {
            let sum: f64 = w.iter().zip(h1.iter()).map(|(wi, hi)| wi * hi).sum();
            sum + self.b2[i]
        }).collect();
        
        let h2_relu: Vec<f64> = h2_pre.iter().map(|&v| v.max(0.0)).collect();
        
        let h2: Vec<f64> = h2_relu.iter().enumerate().map(|(i, &v)| {
            self.bn2_gamma[i] * (v - self.bn2_mean[i]) / (self.bn2_var[i] + eps).sqrt() + self.bn2_beta[i]
        }).collect();
        
        // Layer 3: Linear -> ReLU
        let h3: Vec<f64> = self.w3.iter().enumerate().map(|(i, w)| {
            let sum: f64 = w.iter().zip(h2.iter()).map(|(wi, hi)| wi * hi).sum();
            (sum + self.b3[i]).max(0.0)
        }).collect();
        
        // Layer 4: Linear -> Sigmoid
        let logit: f64 = self.w4.iter().zip(h3.iter())
            .map(|(w, h)| w * h).sum::<f64>() + self.b4;
        
        // Sigmoid
        1.0 / (1.0 + (-logit).exp())
    }
}

/// Features for intra-window prediction (15 features)
#[derive(Clone, Debug, Default)]
pub struct IntraWindowFeatures {
    pub time_remaining: f64,     // 1.0 -> 0.0 as window progresses
    pub time_elapsed: f64,       // 0.0 -> 1.0
    pub current_return: f64,     // % return from window start
    pub current_momentum: f64,   // 30s momentum %
    pub prev_return_1: f64,      // Previous candle return %
    pub prev_return_2: f64,
    pub prev_return_4: f64,
    pub volatility_4: f64,       // Avg volatility of last 4 candles
    pub volatility_8: f64,       // Avg volatility of last 8 candles
    pub trend: f64,              // Sum of recent returns
    pub rsi: f64,                // 14-period RSI
    pub prev_body: f64,          // Previous candle body %
    pub prev_range: f64,         // Previous candle range %
    pub momentum_x_time: f64,    // momentum * time_elapsed
    pub trend_alignment: f64,    // 1.0 if momentum matches trend, else 0.0
}

impl IntraWindowFeatures {
    pub fn to_vec(&self) -> Vec<f64> {
        vec![
            self.time_remaining,
            self.time_elapsed,
            self.current_return,
            self.current_momentum,
            self.prev_return_1,
            self.prev_return_2,
            self.prev_return_4,
            self.volatility_4,
            self.volatility_8,
            self.trend,
            self.rsi,
            self.prev_body,
            self.prev_range,
            self.momentum_x_time,
            self.trend_alignment,
        ]
    }
}

/// State for tracking intra-window features
pub struct IntraWindowState {
    // Current 5-minute window
    pub window_start_ms: i64,
    pub window_start_price: f64,
    pub window_high: f64,
    pub window_low: f64,
    
    // Entry price for current position (used for return calc)
    pub entry_price: Option<f64>,
    
    // Previous candles (OHLC)
    pub prev_candles: VecDeque<(f64, f64, f64, f64)>, // (open, high, low, close)
    
    // Recent prices for RSI/momentum (last 1000 ticks)
    pub recent_prices: VecDeque<(i64, f64)>, // (timestamp_ms, price)
}

impl IntraWindowState {
    pub fn new() -> Self {
        Self {
            window_start_ms: 0,
            window_start_price: 0.0,
            window_high: 0.0,
            window_low: f64::MAX,
            entry_price: None,
            prev_candles: VecDeque::with_capacity(10),
            recent_prices: VecDeque::with_capacity(1000),
        }
    }
    
    /// Update with new price tick
    pub fn update(&mut self, price: f64, ts_ms: i64) {
        // Add to recent prices
        self.recent_prices.push_back((ts_ms, price));
        if self.recent_prices.len() > 1000 {
            self.recent_prices.pop_front();
        }
        
        // Check if new 5-minute window started
        let window_ms: i64 = (MARKET_WINDOW_SECS as i64) * 1000;
        let current_window = (ts_ms / window_ms) * window_ms;
        
        if current_window > self.window_start_ms {
            // Save previous candle
            if self.window_start_price > 0.0 && self.window_low < f64::MAX {
                self.prev_candles.push_back((
                    self.window_start_price,
                    self.window_high,
                    self.window_low,
                    price, // close = current price
                ));
                if self.prev_candles.len() > 10 {
                    self.prev_candles.pop_front();
                }
            }
            
            // Start new window
            self.window_start_ms = current_window;
            self.window_start_price = price;
            self.window_high = price;
            self.window_low = price;
        } else {
            // Update current window
            self.window_high = self.window_high.max(price);
            self.window_low = self.window_low.min(price);
            if self.window_start_price == 0.0 {
                self.window_start_price = price;
            }
        }
    }
    
    /// Compute RSI from recent prices
    fn compute_rsi(&self, period: usize) -> f64 {
        if self.recent_prices.len() < period + 1 {
            return 50.0;
        }
        
        let prices: Vec<f64> = self.recent_prices.iter().map(|(_, p)| *p).collect();
        let mut gains = 0.0;
        let mut losses = 0.0;
        
        let start = prices.len().saturating_sub(period + 1);
        for i in start + 1..prices.len() {
            let delta = prices[i] - prices[i - 1];
            if delta > 0.0 {
                gains += delta;
            } else {
                losses -= delta;
            }
        }
        
        let avg_gain = gains / period as f64;
        let avg_loss = losses / period as f64;
        
        if avg_loss < 1e-10 {
            return 100.0;
        }
        
        let rs = avg_gain / avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }
    
    /// Compute all features for current state
    pub fn compute_features(&self, current_price: f64, current_ts_ms: i64) -> IntraWindowFeatures {
        let window_ms: i64 = 15 * 60 * 1000;
        let elapsed_ms = current_ts_ms - self.window_start_ms;
        let time_elapsed = (elapsed_ms as f64 / window_ms as f64).min(1.0).max(0.0);
        let time_remaining = 1.0 - time_elapsed;
        
        // Current return
        let current_return = if self.window_start_price > 0.0 {
            (current_price - self.window_start_price) / self.window_start_price * 100.0
        } else {
            0.0
        };
        
        // Momentum (last 30 seconds)
        let current_momentum = {
            let cutoff = current_ts_ms - 30_000;
            let start_price = self.recent_prices.iter()
                .find(|(ts, _)| *ts >= cutoff)
                .map(|(_, p)| *p);
            
            match start_price {
                Some(sp) if sp > 0.0 => (current_price - sp) / sp * 100.0,
                _ => 0.0,
            }
        };
        
        // Previous candle returns
        let (prev_return_1, prev_body, prev_range) = if let Some(&(o, h, l, c)) = self.prev_candles.back() {
            let ret = if o > 0.0 { (c - o) / o * 100.0 } else { 0.0 };
            let body = if o > 0.0 { (c - o).abs() / o * 100.0 } else { 0.0 };
            let range = if o > 0.0 { (h - l) / o * 100.0 } else { 0.0 };
            (ret, body, range)
        } else {
            (0.0, 0.0, 0.0)
        };
        
        let prev_return_2 = self.prev_candles.iter().rev().nth(1)
            .map(|&(o, _, _, c)| if o > 0.0 { (c - o) / o * 100.0 } else { 0.0 })
            .unwrap_or(0.0);
            
        let prev_return_4 = self.prev_candles.iter().rev().nth(3)
            .map(|&(o, _, _, c)| if o > 0.0 { (c - o) / o * 100.0 } else { 0.0 })
            .unwrap_or(0.0);
        
        // Volatility
        let volatility_4 = if self.prev_candles.len() >= 4 {
            let sum: f64 = self.prev_candles.iter().rev().take(4)
                .map(|&(o, h, l, _)| if o > 0.0 { (h - l) / o * 100.0 } else { 0.0 })
                .sum();
            sum / 4.0
        } else {
            0.0
        };
        
        let volatility_8 = if self.prev_candles.len() >= 8 {
            let sum: f64 = self.prev_candles.iter().rev().take(8)
                .map(|&(o, h, l, _)| if o > 0.0 { (h - l) / o * 100.0 } else { 0.0 })
                .sum();
            sum / 8.0
        } else {
            volatility_4
        };
        
        // Trend (sum of last 4 candle returns)
        let trend: f64 = self.prev_candles.iter().rev().take(4)
            .map(|&(o, _, _, c)| if o > 0.0 { (c - o) / o * 100.0 } else { 0.0 })
            .sum();
        
        // RSI
        let rsi = self.compute_rsi(14);
        
        // Interaction features
        let momentum_x_time = current_momentum * time_elapsed;
        let trend_alignment = if (current_return > 0.0 && trend > 0.0) || (current_return < 0.0 && trend < 0.0) {
            1.0
        } else {
            0.0
        };
        
        IntraWindowFeatures {
            time_remaining,
            time_elapsed,
            current_return,
            current_momentum,
            prev_return_1,
            prev_return_2,
            prev_return_4,
            volatility_4,
            volatility_8,
            trend,
            rsi,
            prev_body,
            prev_range,
            momentum_x_time,
            trend_alignment,
        }
    }
    
    /// Calculate rolling volatility (standard deviation of returns) over recent ticks
    /// Returns volatility as a decimal (e.g., 0.001 = 0.1%)
    pub fn compute_volatility(&self, window_secs: i64) -> f64 {
        if self.recent_prices.len() < 20 {
            return 0.001; // Default 0.1% if not enough data
        }
        
        let cutoff = self.recent_prices.back().map(|(ts, _)| ts - window_secs * 1000).unwrap_or(0);
        let prices: Vec<f64> = self.recent_prices.iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, p)| *p)
            .collect();
        
        if prices.len() < 10 {
            return 0.001;
        }
        
        // Calculate returns
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        
        if returns.is_empty() {
            return 0.001;
        }
        
        // Calculate standard deviation
        let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance: f64 = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        variance.sqrt().max(0.0001) // Min 0.01% volatility
    }
    
    /// Calculate Z-score: normalized distance to reference price
    /// Z = (S_t - S_0) / (σ * sqrt(τ))
    /// where τ is time remaining in seconds
    pub fn compute_z_score(&self, current_price: f64, time_remaining_secs: f64) -> f64 {
        if self.window_start_price <= 0.0 || time_remaining_secs <= 0.0 {
            return 0.0;
        }
        
        // Use 60-second rolling volatility
        let sigma = self.compute_volatility(60);
        
        // Annualize: scale by sqrt(time remaining / 1 year in seconds)
        // But for simplicity, use sqrt(tau) directly in seconds, scaled appropriately
        let tau_normalized = (time_remaining_secs / MARKET_WINDOW_SECS_F64).sqrt(); // Normalize to 5-min window
        
        if tau_normalized < 0.001 || sigma < 0.00001 {
            // Near expiry or no volatility - return large Z (clamped)
            let diff = (current_price - self.window_start_price) / self.window_start_price;
            return if diff > 0.0 { 10.0 } else { -10.0 };
        }
        
        let displacement = (current_price - self.window_start_price) / self.window_start_price;
        let z = displacement / (sigma * tau_normalized);
        
        // Clamp Z to reasonable range [-10, 10] to prevent extreme blocking
        z.clamp(-10.0, 10.0)
    }
}

// Tick-level features (50 tick lookback)
#[derive(Clone, Debug)]
pub struct TickFeatures {
    pub price_velocity: f64,       // Recent 5 ticks
    pub price_velocity_5: f64,     // Previous 5 ticks
    pub price_velocity_20: f64,    // Over 20 ticks
    pub acceleration: f64,
    pub tick_rate: f64,            // Ticks per ms
    pub buy_sell_imbalance: f64,
    pub volume_imbalance: f64,
    pub price_range: f64,
    pub price_std: f64,
    pub direction_changes: f64,
    pub large_trade_pct: f64,
    pub avg_trade_size: f64,
    pub max_trade_size: f64,
}

impl TickFeatures {
    fn to_vec(&self) -> Vec<f64> {
        vec![
            self.price_velocity * 1e6,
            self.price_velocity_5 * 1e6,
            self.price_velocity_20 * 1e6,
            self.acceleration * 1e9,
            self.tick_rate,
            self.buy_sell_imbalance,
            self.volume_imbalance,
            self.price_range,
            self.price_std,
            self.direction_changes,
            self.large_trade_pct,
            self.avg_trade_size,
            self.max_trade_size,
        ]
    }
}

// ============================================================================
// TICK-BASED FEATURE ENGINE (50 tick lookback, sub-second precision)
// ============================================================================

const LOOKBACK_TICKS: usize = 50;

#[derive(Clone, Debug)]
struct Tick {
    timestamp_us: i64,  // Microseconds
    price: f64,
    quantity: f64,
    is_buyer: bool,
}

#[derive(Clone)]
struct FeatureEngine {
    ticks: VecDeque<Tick>,
    max_ticks: usize,
}

impl FeatureEngine {
    fn new() -> Self {
        Self {
            ticks: VecDeque::with_capacity(200),
            max_ticks: 200,  // Keep more for analysis
        }
    }

    fn push_tick(&mut self, timestamp_ms: u64, price: f64, quantity: f64, is_buyer: bool) {
        self.ticks.push_back(Tick {
            timestamp_us: (timestamp_ms * 1000) as i64,  // Convert ms to us
            price,
            quantity,
            is_buyer,
        });
        while self.ticks.len() > self.max_ticks {
            self.ticks.pop_front();
        }
    }

    fn compute_features(&self) -> Option<TickFeatures> {
        if self.ticks.len() < LOOKBACK_TICKS {
            return None;
        }
        
        let window: Vec<&Tick> = self.ticks.iter()
            .skip(self.ticks.len() - LOOKBACK_TICKS)
            .collect();
        
        let first = window[0];
        let last = window[LOOKBACK_TICKS - 1];
        
        // Time span
        let time_span_us = (last.timestamp_us - first.timestamp_us).max(1) as f64;
        let time_span_ms = time_span_us / 1000.0;
        
        // Price velocity (recent - last 5 ticks)
        let recent = &window[LOOKBACK_TICKS - 5..];
        let recent_time = (recent.last().unwrap().timestamp_us - recent[0].timestamp_us).max(1) as f64;
        let price_velocity = (recent.last().unwrap().price - recent[0].price) / recent_time;
        
        // Price velocity over previous 5 ticks
        let mid = &window[LOOKBACK_TICKS - 10..LOOKBACK_TICKS - 5];
        let mid_time = (mid.last().unwrap().timestamp_us - mid[0].timestamp_us).max(1) as f64;
        let price_velocity_5 = (mid.last().unwrap().price - mid[0].price) / mid_time;
        
        // Price velocity over 20 ticks
        let w20 = &window[LOOKBACK_TICKS - 20..];
        let w20_time = (w20.last().unwrap().timestamp_us - w20[0].timestamp_us).max(1) as f64;
        let price_velocity_20 = (w20.last().unwrap().price - w20[0].price) / w20_time;
        
        let acceleration = price_velocity - price_velocity_5;
        let tick_rate = LOOKBACK_TICKS as f64 / time_span_ms.max(0.001);
        
        // Buy/sell imbalance
        let buys: usize = window.iter().filter(|t| t.is_buyer).count();
        let sells = LOOKBACK_TICKS - buys;
        let buy_sell_imbalance = (buys as f64 - sells as f64) / LOOKBACK_TICKS as f64;
        
        // Volume imbalance
        let buy_vol: f64 = window.iter().filter(|t| t.is_buyer).map(|t| t.quantity).sum();
        let sell_vol: f64 = window.iter().filter(|t| !t.is_buyer).map(|t| t.quantity).sum();
        let total_vol = buy_vol + sell_vol;
        let volume_imbalance = if total_vol > 0.0 { (buy_vol - sell_vol) / total_vol } else { 0.0 };
        
        // Price range
        let prices: Vec<f64> = window.iter().map(|t| t.price).collect();
        let high = prices.iter().cloned().fold(f64::MIN, f64::max);
        let low = prices.iter().cloned().fold(f64::MAX, f64::min);
        let price_range = high - low;
        
        // Standard deviation
        let mean_price: f64 = prices.iter().sum::<f64>() / prices.len() as f64;
        let variance: f64 = prices.iter().map(|p| (p - mean_price).powi(2)).sum::<f64>() / prices.len() as f64;
        let price_std = variance.sqrt();
        
        // Direction changes (microstructure noise)
        let mut direction_changes = 0;
        for i in 2..prices.len() {
            let prev_dir = (prices[i-1] - prices[i-2]).signum();
            let curr_dir = (prices[i] - prices[i-1]).signum();
            if prev_dir != curr_dir && prev_dir != 0.0 && curr_dir != 0.0 {
                direction_changes += 1;
            }
        }
        
        // Large trades
        let quantities: Vec<f64> = window.iter().map(|t| t.quantity).collect();
        let avg_qty = quantities.iter().sum::<f64>() / quantities.len() as f64;
        let large_trades: f64 = quantities.iter().filter(|&&q| q > avg_qty * 2.0).sum();
        let large_trade_pct = large_trades / total_vol.max(0.0001);
        let max_trade_size = quantities.iter().cloned().fold(0.0, f64::max);
        
        Some(TickFeatures {
            price_velocity,
            price_velocity_5,
            price_velocity_20,
            acceleration,
            tick_rate,
            buy_sell_imbalance,
            volume_imbalance,
            price_range,
            price_std,
            direction_changes: direction_changes as f64 / LOOKBACK_TICKS as f64,
            large_trade_pct,
            avg_trade_size: avg_qty,
            max_trade_size,
        })
    }
    
    fn tick_count(&self) -> usize {
        self.ticks.len()
    }
}

// ============================================================================
// SHARED STATE
// ============================================================================

#[derive(Debug, Clone)]
struct BtcMarket {
    slug: String,
    up_token_id: String,
    down_token_id: String,
    interval_start: i64,
}

#[derive(Debug, Clone, Default)]
struct PriceLevel {
    price: Decimal,
    size: Decimal,
}

#[derive(Debug, Clone, Default)]
struct Orderbook {
    up_best_bid: Option<Decimal>,
    up_best_ask: Option<Decimal>,
    down_best_bid: Option<Decimal>,
    down_best_ask: Option<Decimal>,
    // Top 5 levels for display
    up_bids: Vec<PriceLevel>,
    up_asks: Vec<PriceLevel>,
    down_bids: Vec<PriceLevel>,
    down_asks: Vec<PriceLevel>,
    up_bid_depth: usize,
    up_ask_depth: usize,
    down_bid_depth: usize,
    down_ask_depth: usize,
    last_update: Option<Instant>,
    update_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Side {
    Up,
    Down,
}

#[derive(Debug, Clone)]
struct Position {
    side: Side,
    entry_price: Decimal,
    entry_time: Instant,
    size: Decimal,
    predicted_vol_usd: f32,  // Predicted 200ms volatility in $
    is_safe_entry: bool,     // True if entered via safe entry logic (hold longer)
    is_reversal: bool,       // True if entered via reversal logic
    trigger_exchange: String, // Which exchange triggered this entry (Kraken, Coinbase, Bybit, Bitfinex, Binance)
    direction_prob_raw: f32, // Raw sigmoid output for online calibration (before isotonic)
    direction_prob_cal: f32, // Calibrated probability for analysis
    market_interval_start: i64, // Market interval this position was entered in
    entry_z_score: f64,      // Z-score at entry time for analysis
    displacement_usd: f64,   // Displacement at entry for analysis
    elapsed_pct: f64,        // Elapsed % of 5-min window at entry
    liq_count: f64,          // Liquidation count at entry
}

impl Position {
    fn entry_type(&self) -> EntryType {
        if self.is_reversal { EntryType::Reversal }
        else if self.is_safe_entry { EntryType::SafeEntry }
        else { EntryType::Momentum }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EntryType {
    Momentum,
    SafeEntry,
    Reversal,
}

#[derive(Debug, Clone)]
struct TradeRecord {
    time: DateTime<Local>,
    side: Side,
    entry_price: Decimal,
    exit_price: Decimal,
    pnl: Decimal,
    hold_ms: u64,
    ml_prediction_vol: f32,  // Predicted volatility in $
    size: Decimal,           // Position size in $
    entry_type: EntryType,   // Momentum, SafeEntry, or Reversal
    trigger_exchange: String, // Which exchange triggered this trade
    z_score: f64,            // Z-score at entry time
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
}

/// System health metrics
#[derive(Debug, Clone, Default)]
struct SystemHealth {
    cpu_percent: f32,
    mem_used_mb: u64,
    mem_total_mb: u64,
    disk_used_gb: f32,
    disk_total_gb: f32,
    last_update: Option<Instant>,
}

impl SystemHealth {
    fn mem_percent(&self) -> f32 {
        if self.mem_total_mb > 0 {
            (self.mem_used_mb as f32 / self.mem_total_mb as f32) * 100.0
        } else {
            0.0
        }
    }
    
    fn disk_percent(&self) -> f32 {
        if self.disk_total_gb > 0.0 {
            (self.disk_used_gb / self.disk_total_gb) * 100.0
        } else {
            0.0
        }
    }
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Disconnected
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BlockReason {
    None,
    NoMarket,
    MarketClosing,
    OrderbookStale,
    NotEnoughTicks,
    NoMomentum,
    PriceOutOfRange,
    MLFiltered,
    InPosition,
    MaxTradesReached,
    MaxPositions,
    DirectionCooldown,
}

impl std::fmt::Display for BlockReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockReason::None => write!(f, "Ready to trade"),
            BlockReason::NoMarket => write!(f, "No market discovered"),
            BlockReason::MarketClosing => write!(f, "Market closing (<15s)"),
            BlockReason::OrderbookStale => write!(f, "Orderbook stale (>5s)"),
            BlockReason::NotEnoughTicks => write!(f, "Not enough ticks"),
            BlockReason::NoMomentum => write!(f, "No momentum signal"),
            BlockReason::PriceOutOfRange => write!(f, "Price out of range"),
            BlockReason::MLFiltered => write!(f, "ML filtered (low volatility)"),
            BlockReason::InPosition => write!(f, "Already in position"),
            BlockReason::MaxTradesReached => write!(f, "Max trades reached"),
            BlockReason::MaxPositions => write!(f, "Max positions reached"),
            BlockReason::DirectionCooldown => write!(f, "Direction cooldown active"),
        }
    }
}

struct SharedState {
    // Prices
    btc_price: Decimal,
    btc_price_24h_high: Decimal,
    btc_price_24h_low: Decimal,
    btc_price_history: VecDeque<(Instant, Decimal)>,  // (timestamp, price) for movement filter
    min_price_movement: f32,  // Minimum BTC movement in $ to trade (e.g., $30)
    tick_directions: VecDeque<i8>,
    ticks_per_sec: f32,
    last_tick_time: Option<Instant>,
    
    // Market
    market: Option<BtcMarket>,
    orderbook: Orderbook,
    
    // Positions (stacking - multiple can be open)
    positions: Vec<Position>,
    last_third_position_time: Option<Instant>,  // Cooldown after 3rd position
    
    // Direction cooldown: prevent whipsawing between UP/DOWN
    last_entry_side: Option<Side>,
    last_entry_time: Option<Instant>,
    direction_cooldown_ms: u64,  // How long to block opposite direction (default: 60000ms = 60s)
    
    // Stats
    total_pnl: Decimal,
    trade_count: u32,
    win_count: u32,
    ml_filtered_count: u32,
    momentum_signals: u32,
    trades: VecDeque<TradeRecord>,
    
    // ML - Tick-level predictions (volatility)
    tick_features: Option<TickFeatures>,
    ml_prediction_vol: f32,  // Predicted 200ms volatility in $
    feature_engine_ticks: usize,
    
    // ML - Direction predictions (entry signal)
    direction_features: Option<DirectionFeatures>,
    direction_prob_up: f32,       // P(price goes UP) 0.0-1.0 (blended calibration)
    direction_prob_raw: f32,      // Raw sigmoid output (for online calibration)
    direction_threshold: f32,     // Entry threshold (default 0.55)
    direction_engine_ticks: usize,
    
    // Connection status
    binance_status: ConnectionStatus,
    polymarket_status: ConnectionStatus,
    coinbase_status: ConnectionStatus,  // Coinbase for early signals
    binance_msg_count: u64,
    polymarket_msg_count: u64,
    coinbase_msg_count: u64,
    binance_last_msg: Option<Instant>,
    polymarket_last_msg: Option<Instant>,
    coinbase_last_msg: Option<Instant>,
    
    // Latency tracking (actual network latency in ms)
    binance_latency_ms: u64,   // Time from Binance event to our receipt
    polymarket_latency_ms: u64, // Time from PM event to our receipt
    coinbase_latency_ms: u64,  // Time from Coinbase event to our receipt
    
    // Multi-exchange early signals (faster than Binance 106ms)
    // Kraken: ~23ms, Coinbase: ~52ms, Bybit: ~86ms
    kraken_status: ConnectionStatus,
    kraken_msg_count: u64,
    kraken_last_msg: Option<Instant>,
    kraken_latency_ms: u64,
    kraken_price: Decimal,
    kraken_last_direction: i8,
    
    bybit_status: ConnectionStatus,
    bybit_msg_count: u64,
    bybit_last_msg: Option<Instant>,
    bybit_latency_ms: u64,
    bybit_price: Decimal,
    bybit_last_direction: i8,
    
    // Bitfinex - additional fast exchange (~30ms)
    bitfinex_status: ConnectionStatus,
    bitfinex_msg_count: u64,
    bitfinex_last_msg: Option<Instant>,
    bitfinex_latency_ms: u64,
    bitfinex_price: Decimal,
    bitfinex_last_direction: i8,
    
    coinbase_price: Decimal,
    coinbase_last_direction: i8,  // +1 = up, -1 = down, 0 = neutral
    
    // Combined early signal from fastest exchange - TRIGGERS TRADES!
    early_signal: Option<(Side, Instant, &'static str)>,  // (direction, time, source)
    early_signal_confirmations: u8,  // How many exchanges agree (1-5)
    early_signal_ready: bool,  // True when signal is strong enough to trade
    
    // Diagnostics
    block_reason: BlockReason,
    consecutive_up: usize,
    consecutive_down: usize,
    
    // Config
    dry_run: bool,
    position_size: Decimal,
    min_volatility_usd: f32,  // Minimum predicted volatility to trade (in $)
    tick_threshold: usize,
    min_hold_ms: u64,         // Minimum hold time before allowing exit
    hold_timeout_ms: u64,     // Maximum hold time (forced exit)
    min_z_score: f64,         // Minimum |Z-score| to enter a trade
    max_trades: u32,  // Max trades per 5-minute session (split evenly per side)
    max_stacking: usize,  // Max concurrent open positions (1 = no stacking)
    wallet_balance: Decimal,  // Current USDC balance
    
    // Per-side trade counts (reset on new market)
    up_trade_count: u32,
    down_trade_count: u32,
    
    // CRITICAL: Track actual shares bought per side for debugging
    up_shares_bought: Decimal,
    down_shares_bought: Decimal,
    up_entry_count: u32,   // Entries only (not exits)
    down_entry_count: u32, // Entries only (not exits)
    up_exit_count: u32,    // Exits only
    down_exit_count: u32,  // Exits only
    failed_exit_count: u32, // Track failed exits
    
    // Adaptive volatility threshold
    session_trade_count: u32,         // Trades in current 5-minute session
    session_start: Option<i64>,       // Unix timestamp of session start
    adaptive_threshold_active: bool,  // Whether adaptive is currently active
    base_min_volatility: f32,         // Original base threshold from env
    adaptive_min_threshold: f32,      // Floor - never go below this
    adaptive_max_threshold: f32,      // Ceiling - never go above this
    target_trades_per_market: u32,    // Target trades per 5-minute market
    last_market_trade_count: u32,     // Trades from previous market (for between-market adjust)
    session_max_volatility: f32,      // Max volatility seen this session (for top-tracking)
    volatility_samples: Vec<f32>,     // Recent volatility samples for range detection
    
    // Pre-signed order status
    pre_signed_up_ready: bool,
    pre_signed_down_ready: bool,
    
    // Intra-window ML for exit decisions
    intra_window_state: IntraWindowState,
    intra_window_prediction: f64,  // P(UP wins) 0.0-1.0
    z_score: f64,                  // Z-score: normalized displacement from reference
    
    // Momentum tracking for exit decisions (M = momentum, dM/dt = acceleration)
    prev_momentum: f64,            // Previous tick's momentum (for dM/dt)
    momentum_sign_flips: u32,      // Count of dM/dt sign changes in current position
    
    // Liquidation tracking for direction model
    liquidation_tracker: LiquidationTracker,
    
    // Reversal tracking: bet opposite after big move fails to follow through
    last_big_move_time: Option<Instant>,  // When the big move happened
    last_big_move_side: Option<Side>,     // Direction of the big move (what we'd normally bet)
    last_big_move_pm_price: Option<Decimal>, // PM price of that side when move happened
    reversal_traded: bool,                // Did we already trade the reversal?
    
    // Online calibration for direction model
    calibration_bins: CalibrationBins,    // EMA-based live calibration
    pending_calibration: Vec<(f32, i64, Side)>, // (raw_prob, market_interval, side) waiting for resolution
    last_resolved_market: i64,            // Last market interval where we recorded outcome
    
    // Logs
    logs: VecDeque<String>,
    
    // Start time
    start_time: Instant,
    
    // System health
    system_health: SystemHealth,
}
impl Default for SharedState {
    fn default() -> Self {
        Self {
            btc_price: dec!(0),
            btc_price_24h_high: dec!(0),
            btc_price_24h_low: dec!(999999),
            btc_price_history: VecDeque::new(),
            min_price_movement: 15.0,  // $15 minimum movement in last 30s
            tick_directions: VecDeque::with_capacity(20),
            ticks_per_sec: 0.0,
            last_tick_time: None,
            market: None,
            orderbook: Orderbook::default(),
            positions: Vec::with_capacity(20),
            last_third_position_time: None,
            last_entry_side: None,
            last_entry_time: None,
            direction_cooldown_ms: 0,  // DISABLED - trust Z-score + tick confirmation
            total_pnl: dec!(0),
            trade_count: 0,
            win_count: 0,
            ml_filtered_count: 0,
            momentum_signals: 0,
            trades: VecDeque::with_capacity(50),
            tick_features: None,
            ml_prediction_vol: 0.0,
            feature_engine_ticks: 0,
            direction_features: None,
            direction_prob_up: 0.5,
            direction_prob_raw: 0.5,
            direction_threshold: 0.55,
            direction_engine_ticks: 0,
            binance_status: ConnectionStatus::Disconnected,
            polymarket_status: ConnectionStatus::Disconnected,
            coinbase_status: ConnectionStatus::Disconnected,
            binance_msg_count: 0,
            polymarket_msg_count: 0,
            coinbase_msg_count: 0,
            binance_last_msg: None,
            polymarket_last_msg: None,
            coinbase_last_msg: None,
            binance_latency_ms: 0,
            polymarket_latency_ms: 0,
            coinbase_latency_ms: 0,
            kraken_status: ConnectionStatus::Disconnected,
            kraken_msg_count: 0,
            kraken_last_msg: None,
            kraken_latency_ms: 0,
            kraken_price: dec!(0),
            kraken_last_direction: 0,
            bybit_status: ConnectionStatus::Disconnected,
            bybit_msg_count: 0,
            bybit_last_msg: None,
            bybit_latency_ms: 0,
            bybit_price: dec!(0),
            bybit_last_direction: 0,
            bitfinex_status: ConnectionStatus::Disconnected,
            bitfinex_msg_count: 0,
            bitfinex_last_msg: None,
            bitfinex_latency_ms: 0,
            bitfinex_price: dec!(0),
            bitfinex_last_direction: 0,
            coinbase_price: dec!(0),
            coinbase_last_direction: 0,
            early_signal: None,
            early_signal_confirmations: 0,
            early_signal_ready: false,
            block_reason: BlockReason::NoMarket,
            consecutive_up: 0,
            consecutive_down: 0,
            dry_run: true,
            position_size: dec!(10),  // 10 shares per trade
            min_volatility_usd: 6.5,  // $6.5 minimum predicted volatility (optimized from trade analysis)
            tick_threshold: 4,
            min_hold_ms: 3500,        // 3.5s minimum hold before exit (5-min markets)
            hold_timeout_ms: 15000,   // 15s max hold (forced exit)
            min_z_score: 0.5,         // Lower threshold for more trades
            max_trades: 10,  // 5 per side (5-min markets)
            max_stacking: 5,  // Max concurrent open positions (1 = no stacking)
            wallet_balance: dec!(0),
            up_trade_count: 0,
            down_trade_count: 0,
            up_shares_bought: dec!(0),
            down_shares_bought: dec!(0),
            up_entry_count: 0,
            down_entry_count: 0,
            up_exit_count: 0,
            down_exit_count: 0,
            failed_exit_count: 0,
            session_trade_count: 0,
            session_start: None,
            adaptive_threshold_active: false,
            base_min_volatility: 5.0,
            adaptive_min_threshold: 1.0,
            adaptive_max_threshold: 15.0,
            target_trades_per_market: 50,
            last_market_trade_count: 0,
            session_max_volatility: 0.0,
            volatility_samples: Vec::with_capacity(100),
            pre_signed_up_ready: false,
            pre_signed_down_ready: false,
            logs: VecDeque::with_capacity(100),
            start_time: Instant::now(),
            system_health: SystemHealth::default(),
            intra_window_state: IntraWindowState::new(),
            intra_window_prediction: 0.5,
            z_score: 0.0,
            prev_momentum: 0.0,
            momentum_sign_flips: 0,
            liquidation_tracker: LiquidationTracker::new(),
            last_big_move_time: None,
            last_big_move_side: None,
            last_big_move_pm_price: None,
            reversal_traded: false,
            calibration_bins: CalibrationBins::default(),
            pending_calibration: Vec::with_capacity(20),
            last_resolved_market: 0,
        }
    }
}

impl SharedState {
    /// Add log message - accepts String for compatibility
    fn add_log(&mut self, msg: String) {
        self.add_log_str(&msg);
    }
    
    /// Add log message - zero-allocation version for static strings
    #[inline]
    fn add_log_str(&mut self, msg: &str) {
        // Pre-allocate capacity to avoid reallocation
        let mut log_entry = String::with_capacity(24 + msg.len());
        log_entry.push('[');
        log_entry.push_str(&Local::now().format("%H:%M:%S%.3f").to_string());
        log_entry.push_str("] ");
        log_entry.push_str(msg);
        
        self.logs.push_front(log_entry);
        if self.logs.len() > 100 {
            self.logs.pop_back();
        }
    }
    
    fn time_left_in_market(&self) -> Option<i64> {
        self.market.as_ref().map(|m| {
            let now = Utc::now().timestamp();
            let market_end = m.interval_start + MARKET_WINDOW_SECS; // 5 minutes
            (market_end - now).max(0)
        })
    }
    
    fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// DEPRECATED: Load historical trades from CSV file on startup
#[allow(dead_code)]
fn load_historical_trades(state: &mut SharedState) {
    let log_path = "/data/trades.csv";
    
    if !Path::new(log_path).exists() {
        state.add_log("No historical trades file found".to_string());
        return;
    }
    
    let file = match File::open(log_path) {
        Ok(f) => f,
        Err(e) => {
            state.add_log(format!("Failed to open trades.csv: {}", e));
            return;
        }
    };
    
    let reader = BufReader::new(file);
    let mut loaded_count = 0;
    let mut total_pnl = dec!(0);
    let mut win_count = 0;
    
    for (i, line) in reader.lines().enumerate() {
        // Skip header
        if i == 0 { continue; }
        
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 10 { continue; }
        
        // Parse: timestamp,side,entry_price,exit_price,pnl_pct,hold_ms,ml_prediction_vol,btc_price,market_slug,dry_run
        let time = match chrono::NaiveDateTime::parse_from_str(parts[0], "%Y-%m-%d %H:%M:%S%.3f") {
            Ok(t) => DateTime::<Local>::from_naive_utc_and_offset(t, *Local::now().offset()),
            Err(_) => continue,
        };
        
        let side = if parts[1].contains("Up") { Side::Up } else { Side::Down };
        let entry_price = parts[2].parse::<Decimal>().unwrap_or(dec!(0));
        let exit_price = parts[3].parse::<Decimal>().unwrap_or(dec!(0));
        let pnl_pct = parts[4].parse::<Decimal>().unwrap_or(dec!(0)) / dec!(100); // Convert back from %
        let hold_ms = parts[5].parse::<u64>().unwrap_or(0);
        let ml_prediction_vol = parts[6].parse::<f32>().unwrap_or(0.0);
        let size = dec!(10); // Default size, not stored in old format
        
        let trade = TradeRecord {
            time,
            side,
            entry_price,
            exit_price,
            pnl: pnl_pct,
            hold_ms,
            ml_prediction_vol,
            size,
            entry_type: EntryType::Momentum,  // Historical trades default to Momentum
            trigger_exchange: "Unknown".to_string(),  // Historical trades don't have trigger info
            z_score: 0.0,  // Historical trades don't have z_score
        };
        
        total_pnl += pnl_pct * size;
        if pnl_pct > dec!(0) { win_count += 1; }
        
        state.trades.push_back(trade);
        loaded_count += 1;
        
        // Keep only last 50 trades for display
        if state.trades.len() > 50 {
            state.trades.pop_front();
        }
    }
    
    // Update stats from historical data
    state.trade_count = loaded_count;
    state.win_count = win_count;
    state.total_pnl = total_pnl;
    
    state.add_log(format!("[OK] Loaded {} historical trades (PnL: ${:.2})", loaded_count, total_pnl));
}

/// Collect system health metrics from /proc (Linux)
fn collect_system_health() -> SystemHealth {
    let mut health = SystemHealth::default();
    
    // Read memory info from /proc/meminfo
    if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
        let mut mem_total: u64 = 0;
        let mut mem_available: u64 = 0;
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                mem_total = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            } else if line.starts_with("MemAvailable:") {
                mem_available = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            }
        }
        health.mem_total_mb = mem_total / 1024;  // kB to MB
        health.mem_used_mb = (mem_total - mem_available) / 1024;
    }
    
    // Read CPU usage from /proc/stat (simplified - just load average)
    if let Ok(content) = std::fs::read_to_string("/proc/loadavg") {
        // Format: "0.00 0.01 0.05 1/234 12345" - first value is 1-min load avg
        if let Some(load) = content.split_whitespace().next().and_then(|s| s.parse::<f32>().ok()) {
            // Convert load average to approximate CPU % (assumes single core baseline)
            // For multi-core, this is load per core
            health.cpu_percent = load * 100.0;  // Rough approximation
        }
    }
    
    // Read disk usage using statfs syscall via Command (simpler than raw syscall)
    if let Ok(output) = std::process::Command::new("df")
        .args(["-BG", "/"])
        .output() 
    {
        if let Ok(stdout) = String::from_utf8(output.stdout) {
            // Skip header line, parse second line
            if let Some(line) = stdout.lines().nth(1) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 4 {
                    // Format: Filesystem 1G-blocks Used Available Use% Mounted
                    let total: f32 = parts[1].trim_end_matches('G').parse().unwrap_or(0.0);
                    let used: f32 = parts[2].trim_end_matches('G').parse().unwrap_or(0.0);
                    health.disk_total_gb = total;
                    health.disk_used_gb = used;
                }
            }
        }
    }
    
    health.last_update = Some(Instant::now());
    health
}

// ============================================================================
// ZERO-ALLOCATION JSON PARSING (memchr)
// ============================================================================

/// Fast extraction of a JSON field value using memchr (no allocations in hot path)
/// Returns slice pointing into original bytes
#[inline(always)]
fn extract_json_field<'a>(json: &'a [u8], field: &[u8]) -> Option<&'a str> {
    // Find field pattern like "p":"
    let finder = memmem::Finder::new(field);
    let pos = finder.find(json)?;
    let start = pos + field.len();
    
    // Find closing quote
    let rest = &json[start..];
    let end = memchr::memchr(b'"', rest)?;
    
    // Safety: Binance sends valid UTF-8
    std::str::from_utf8(&rest[..end]).ok()
}

/// Fast extraction of numeric JSON field (no quotes)
#[inline(always)]
fn extract_json_number<'a>(json: &'a [u8], field: &[u8]) -> Option<&'a str> {
    let finder = memmem::Finder::new(field);
    let pos = finder.find(json)?;
    let start = pos + field.len();
    
    let rest = &json[start..];
    // Find end of number (comma, brace, or bracket)
    let end = rest.iter().position(|&b| b == b',' || b == b'}' || b == b']')?;
    
    std::str::from_utf8(&rest[..end]).ok()
}

/// Zero-allocation Binance trade parser
/// Returns (price_str, quantity_str, timestamp) without heap allocation
#[inline(always)]
fn parse_binance_trade_fast(json: &[u8]) -> Option<(&str, &str, u64)> {
    // Patterns for Binance trade JSON: {"p":"12345.67","q":"0.001","T":1234567890123,...}
    static PRICE_PATTERN: &[u8] = b"\"p\":\"";
    static QUANTITY_PATTERN: &[u8] = b"\"q\":\"";
    static TIMESTAMP_PATTERN: &[u8] = b"\"T\":";
    
    let price = extract_json_field(json, PRICE_PATTERN)?;
    let quantity = extract_json_field(json, QUANTITY_PATTERN)?;
    let ts_str = extract_json_number(json, TIMESTAMP_PATTERN)?;
    let timestamp: u64 = ts_str.parse().ok()?;
    
    Some((price, quantity, timestamp))
}

// ============================================================================
// WEBSOCKET HANDLERS
// ============================================================================

#[derive(Debug, Deserialize)]
struct BinanceTrade {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    timestamp: u64,
}

async fn run_binance_ws(
    state: Arc<RwLock<SharedState>>,
    feature_engine: Arc<RwLock<FeatureEngine>>,
    direction_feature_engine: Arc<RwLock<DirectionFeatureEngine>>,
    ml: Arc<MLPredictor>,
    direction_model: Arc<DirectionPredictor>,
    tick_tx: mpsc::Sender<()>,
    intra_predictor: Option<Arc<IntraWindowPredictor>>,
) {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    
    loop {
        {
            let mut s = state.write().await;
            s.binance_status = ConnectionStatus::Connecting;
            s.add_log("Connecting to Binance WebSocket...".to_string());
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                {
                    let mut s = state.write().await;
                    s.binance_status = ConnectionStatus::Connected;
                    s.add_log("[OK] Binance WebSocket connected".to_string());
                }
                
                let (_, mut read) = ws_stream.split();
                let mut last_price: Option<Decimal> = None;
                let mut tick_count_1s = 0u32;
                let mut last_tick_count_time = Instant::now();
                
                // Stale connection detection: 5s timeout (Binance sends ~50+ ticks/sec)
                const BINANCE_TIMEOUT_SECS: u64 = 5;
                
                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(BINANCE_TIMEOUT_SECS),
                        read.next()
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            let mut s = state.write().await;
                            s.add_log("Binance stream ended".to_string());
                            break;
                        }
                        Err(_) => {
                            // Timeout - connection is stale/zombie
                            let mut s = state.write().await;
                            s.add_log(format!("[!] Binance stale (no data {}s), forcing reconnect...", BINANCE_TIMEOUT_SECS));
                            break;
                        }
                    };
                    match msg {
                        Ok(Message::Text(text)) => {
                            // ZERO-ALLOCATION FAST PATH: Use memchr-based parser
                            let bytes = text.as_bytes();
                            if let Some((price_str, qty_str, ts)) = parse_binance_trade_fast(bytes) {
                                if let Ok(price) = Decimal::from_str(price_str) {
                                    let quantity: f64 = qty_str.parse().unwrap_or(0.0);
                                    
                                    // Calculate actual network latency (our time - Binance event time)
                                    let now_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as u64;
                                    let latency_ms = now_ms.saturating_sub(ts);
                                    
                                    let direction = match last_price {
                                        Some(lp) if price > lp => 1i8,
                                        Some(lp) if price < lp => -1i8,
                                        _ => 0i8,
                                    };
                                    last_price = Some(price);
                                    
                                    tick_count_1s += 1;
                                    if last_tick_count_time.elapsed() >= Duration::from_secs(1) {
                                        let mut s = state.write().await;
                                        s.ticks_per_sec = tick_count_1s as f32;
                                        tick_count_1s = 0;
                                        last_tick_count_time = Instant::now();
                                    }
                                    
                                    // Use ts from fast parser
                                    {
                                        let mut fe = feature_engine.write().await;
                                        // Parse is_buyer from Binance trade (if maker is seller, trade was buyer-initiated)
                                        let is_buyer = true; // Binance @trade doesn't include this, assume buyer
                                        fe.push_tick(ts, price.to_f64().unwrap_or(0.0), quantity, is_buyer);
                                        
                                        if let Some(features) = fe.compute_features() {
                                            let prediction = ml.predict(&features);
                                            let mut s = state.write().await;
                                            s.ml_prediction_vol = prediction;
                                            s.feature_engine_ticks = fe.tick_count();
                                            s.tick_features = Some(features);
                                            
                                            // ASYMMETRIC ADAPTIVE THRESHOLD
                                            // Fast up (chase peaks), slow down (stay elevated for filtering)
                                            if prediction > 0.0 {
                                                // Add to rolling window of volatility samples
                                                s.volatility_samples.push(prediction);
                                                if s.volatility_samples.len() > 100 {
                                                    s.volatility_samples.remove(0);
                                                }
                                                
                                                // Track session max with slow decay
                                                if prediction > s.session_max_volatility {
                                                    s.session_max_volatility = prediction;
                                                } else {
                                                    s.session_max_volatility *= 0.9995; // Very slow decay
                                                }
                                                
                                                let current_thr = s.min_volatility_usd;
                                                let min_t = s.adaptive_min_threshold;
                                                let max_t = s.adaptive_max_threshold;
                                                
                                                // ASYMMETRIC MOVEMENT with PROPORTIONAL DECAY:
                                                // - If prediction > threshold: ramp up toward peak
                                                // - If prediction < threshold: decay FASTER when gap is larger
                                                let new_threshold = if prediction > current_thr {
                                                    // Up: move 3% toward 85% of prediction (slow ramp)
                                                    let target = prediction * 0.85;
                                                    current_thr + (target - current_thr) * 0.03
                                                } else {
                                                    // Down: decay proportional to gap
                                                    // If vol is 10% of threshold, decay 3% per tick
                                                    // If vol is 50% of threshold, decay 1% per tick
                                                    // If vol is 90% of threshold, decay 0.2% per tick
                                                    let ratio = prediction / current_thr.max(0.01);
                                                    let decay_rate = if ratio < 0.2 {
                                                        0.97  // 3% decay - very low vol
                                                    } else if ratio < 0.5 {
                                                        0.99  // 1% decay - low vol
                                                    } else if ratio < 0.8 {
                                                        0.995 // 0.5% decay - moderate vol
                                                    } else {
                                                        0.998 // 0.2% decay - close to threshold
                                                    };
                                                    current_thr * decay_rate
                                                };
                                                
                                                let new_threshold = new_threshold.clamp(min_t, max_t);
                                                
                                                // Apply if meaningful change
                                                let change_pct = ((new_threshold - current_thr) / current_thr).abs();
                                                if change_pct > 0.005 {
                                                    s.min_volatility_usd = new_threshold;
                                                    s.adaptive_threshold_active = true;
                                                    
                                                    // Log significant jumps (>20%)
                                                    if change_pct > 0.20 {
                                                        let direction = if new_threshold > current_thr { "^" } else { "v" };
                                                        s.add_log(format!("[THR] ${:.2}->${:.2} {} (vol: ${:.2})", 
                                                            current_thr, new_threshold, direction, prediction));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Direction prediction for entry signals (27-feature conditional probability model)
                                    {
                                        let mut dfe = direction_feature_engine.write().await;
                                        // Infer is_buyer from price direction (approximation)
                                        let is_buyer = direction >= 0;
                                        dfe.push_tick(ts, price.to_f64().unwrap_or(0.0), quantity, is_buyer);
                                        
                                        // Get window state for reference price and timing
                                        let s_read = state.read().await;
                                        let window_start_ms = s_read.intra_window_state.window_start_ms;
                                        let reference_price = s_read.intra_window_state.window_start_price;
                                        let liq_features = s_read.liquidation_tracker.compute_features(ts as i64);
                                        drop(s_read);
                                        
                                        // Reset engine on new window
                                        let window_ms: i64 = 15 * 60 * 1000;
                                        let current_window = (ts as i64 / window_ms) * window_ms;
                                        if current_window > window_start_ms && reference_price > 0.0 {
                                            dfe.reset_window(price.to_f64().unwrap_or(0.0));
                                        }
                                        
                                        if let Some(dir_features) = dfe.compute_features_full(
                                            reference_price,
                                            window_start_ms,
                                            ts as i64,
                                            &liq_features,
                                        ) {
                                            let mut s = state.write().await;
                                            // Use live calibration blended with pretrained isotonic
                                            let (prob_up, raw_prob) = direction_model.predict_with_live_calib(&dir_features, &s.calibration_bins);
                                            s.direction_prob_up = prob_up;
                                            s.direction_prob_raw = raw_prob;
                                            s.direction_engine_ticks = dfe.tick_count();
                                            s.direction_features = Some(dir_features);
                                        }
                                    }
                                    
                                    {
                                        let mut s = state.write().await;
                                        s.btc_price = price;
                                        s.binance_msg_count += 1;
                                        s.binance_last_msg = Some(Instant::now());
                                        s.binance_latency_ms = latency_ms;  // Update latency
                                        s.last_tick_time = Some(Instant::now());
                                        
                                        // Track price history for movement filter (keep last 30s)
                                        let now = Instant::now();
                                        s.btc_price_history.push_back((now, price));
                                        // Remove prices older than 30 seconds
                                        while let Some((ts, _)) = s.btc_price_history.front() {
                                            if now.duration_since(*ts).as_secs() > 30 {
                                                s.btc_price_history.pop_front();
                                            } else {
                                                break;
                                            }
                                        }
                                        
                                        // Track 24h high/low (session high/low actually)
                                        if price > s.btc_price_24h_high {
                                            s.btc_price_24h_high = price;
                                        }
                                        if price < s.btc_price_24h_low {
                                            s.btc_price_24h_low = price;
                                        }
                                        
                                        if direction != 0 {
                                            s.tick_directions.push_back(direction);
                                            if s.tick_directions.len() > 15 {
                                                s.tick_directions.pop_front();
                                            }
                                            // NOTE: consecutive_up/down are computed from tick_directions
                                            // in the entry logic (line ~4850), not here
                                        }
                                        
                                        // Update intra-window state and prediction on every tick
                                        if let Some(ref predictor) = intra_predictor {
                                            let now_ms = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis() as i64;
                                            let btc_f64 = price.to_f64().unwrap_or(0.0);
                                            
                                            // Update state with price
                                            s.intra_window_state.update(btc_f64, now_ms);
                                            
                                            // Get actual market time remaining if available
                                            let time_remaining_secs = s.market.as_ref().map(|m| {
                                                let now = Utc::now().timestamp();
                                                let market_end = m.interval_start + MARKET_WINDOW_SECS;
                                                (market_end - now).max(0) as f64
                                            }).unwrap_or(150.0); // Default to mid-window (half of 300s)
                                            
                                            // Compute features with correct time
                                            let mut features = s.intra_window_state.compute_features(btc_f64, now_ms);
                                            // Override time features with actual market timing
                                            features.time_remaining = time_remaining_secs / MARKET_WINDOW_SECS_F64;
                                            features.time_elapsed = 1.0 - features.time_remaining;
                                            features.momentum_x_time = features.current_momentum * features.time_elapsed;
                                            
                                            s.intra_window_prediction = predictor.predict(&features);
                                        }
                                    }
                                    
                                    // EVENT-DRIVEN: Signal strategy immediately after tick
                                    let _ = tick_tx.try_send(());
                                }
                            }
                        }
                        Err(e) => {
                            let mut s = state.write().await;
                            s.add_log(format!("Binance WS error: {}", e));
                            break;
                        }
                        _ => {}
                    }
                } // end loop
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("Binance connection error: {}", e));
            }
        }
        
        {
            let mut s = state.write().await;
            s.binance_status = ConnectionStatus::Disconnected;
            s.add_log("Binance disconnected, reconnecting in 2s...".to_string());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ============================================================================
// BINANCE FUTURES LIQUIDATION STREAM - Cascade detection for direction model
// ============================================================================

async fn run_liquidation_ws(state: Arc<RwLock<SharedState>>) {
    // Binance Futures liquidation stream: wss://fstream.binance.com/ws/btcusdt@forceOrder
    let url = "wss://fstream.binance.com/ws/btcusdt@forceOrder";
    
    loop {
        {
            let mut s = state.write().await;
            s.add_log("Connecting to Binance Liquidation stream...".to_string());
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                {
                    let mut s = state.write().await;
                    s.add_log("[OK] Binance Liquidation stream connected".to_string());
                }
                
                let (_, mut read) = ws_stream.split();
                
                loop {
                    match tokio::time::timeout(
                        Duration::from_secs(60), // Liquidations are sparse, 60s timeout
                        read.next()
                    ).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Parse liquidation: {"e":"forceOrder","E":timestamp,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"97530.00","ap":"97530.00","X":"FILLED","l":"0.014","z":"0.014","T":1706012345678}}
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                if json.get("e").and_then(|v| v.as_str()) == Some("forceOrder") {
                                    if let Some(order) = json.get("o") {
                                        let side_str = order.get("S").and_then(|v| v.as_str()).unwrap_or("");
                                        let qty_str = order.get("q").and_then(|v| v.as_str()).unwrap_or("0");
                                        let price_str = order.get("p").and_then(|v| v.as_str()).unwrap_or("0");
                                        let ts = order.get("T").and_then(|v| v.as_u64()).unwrap_or(0);
                                        
                                        let qty: f64 = qty_str.parse().unwrap_or(0.0);
                                        let price: f64 = price_str.parse().unwrap_or(0.0);
                                        // SELL = long was liquidated (bearish), BUY = short was liquidated (bullish)
                                        let is_long_liquidated = side_str == "SELL";
                                        
                                        if qty > 0.0 && price > 0.0 {
                                            let mut s = state.write().await;
                                            s.liquidation_tracker.add(ts as i64, is_long_liquidated, qty, price);
                                            
                                            // Log significant liquidations (>$50k)
                                            let usd_value = qty * price;
                                            if usd_value > 50_000.0 {
                                                let side_name = if is_long_liquidated { "LONG" } else { "SHORT" };
                                                s.add_log(format!("[LIQ] {} ${:.0}k liquidated @ ${:.0}", 
                                                    side_name, usd_value / 1000.0, price));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            // Respond with pong - but we don't have write access in this simple loop
                            // The connection will stay alive with built-in keep-alive
                            let _ = data;
                        }
                        Ok(Some(Err(e))) => {
                            let mut s = state.write().await;
                            s.add_log(format!("[X] Liquidation stream error: {}", e));
                            break;
                        }
                        Ok(None) => {
                            break;  // Stream closed
                        }
                        Err(_) => {
                            // Timeout is OK for liquidation stream (sparse events)
                            continue;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] Liquidation stream connect error: {}", e));
            }
        }
        
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

// ============================================================================
// KRAKEN WEBSOCKET - Fastest exchange (~23ms latency) for early signals
// ============================================================================

/// Parse Kraken trade message: ["channelID", [["price","volume","time","side","orderType","misc"],...], "channelName", "pair"]
fn parse_kraken_trade(data: &serde_json::Value) -> Option<(Decimal, i8, u64)> {
    let arr = data.as_array()?;
    if arr.len() < 2 { return None; }
    
    let trades = arr[1].as_array()?;
    if trades.is_empty() { return None; }
    
    let trade = trades.last()?.as_array()?;  // Get most recent trade
    if trade.len() < 4 { return None; }
    
    let price_str = trade[0].as_str()?;
    let time_str = trade[2].as_str()?;
    let side_str = trade[3].as_str()?;
    
    let price = Decimal::from_str(price_str).ok()?;
    let direction: i8 = if side_str == "b" { 1 } else { -1 };  // b=buy (price up), s=sell (price down)
    
    // Kraken time is Unix timestamp with decimal seconds
    let time_f: f64 = time_str.parse().ok()?;
    let time_ms = (time_f * 1000.0) as u64;
    
    Some((price, direction, time_ms))
}

async fn run_kraken_ws(state: Arc<RwLock<SharedState>>) {
    let url = "wss://ws.kraken.com";
    
    loop {
        {
            let mut s = state.write().await;
            s.kraken_status = ConnectionStatus::Connecting;
            s.add_log("Connecting to Kraken WebSocket (fastest: ~23ms)...".to_string());
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                
                // Subscribe to XBT/USD trades
                let sub_msg = r#"{"event":"subscribe","pair":["XBT/USD"],"subscription":{"name":"trade"}}"#;
                if let Err(e) = futures_util::SinkExt::send(&mut write, Message::Text(sub_msg.to_string().into())).await {
                    let mut s = state.write().await;
                    s.add_log(format!("[X] Kraken subscribe failed: {}", e));
                    continue;
                }
                
                {
                    let mut s = state.write().await;
                    s.kraken_status = ConnectionStatus::Connected;
                    s.add_log("[OK] Kraken connected".to_string());
                }
                
                const KRAKEN_TIMEOUT_SECS: u64 = 10;
                let mut consecutive_ups: u8 = 0;
                let mut consecutive_downs: u8 = 0;
                
                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(KRAKEN_TIMEOUT_SECS),
                        read.next()
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => break,
                        Err(_) => {
                            let mut s = state.write().await;
                            s.add_log(format!("[!] Kraken stale {}s, reconnecting...", KRAKEN_TIMEOUT_SECS));
                            break;
                        }
                    };
                    
                    if let Ok(Message::Text(text)) = msg {
                        // Parse as JSON
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            // Only process trade arrays (skip subscription confirmations)
                            if data.is_array() {
                                if let Some((price, direction, trade_time_ms)) = parse_kraken_trade(&data) {
                                    let now_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as u64;
                                    let latency = now_ms.saturating_sub(trade_time_ms);
                                    
                                    // Track consecutive same-direction trades for strong signal
                                    if direction > 0 {
                                        consecutive_ups += 1;
                                        consecutive_downs = 0;
                                    } else if direction < 0 {
                                        consecutive_downs += 1;
                                        consecutive_ups = 0;
                                    }
                                    
                                    let mut s = state.write().await;
                                    s.kraken_msg_count += 1;
                                    s.kraken_last_msg = Some(Instant::now());
                                    s.kraken_latency_ms = latency;
                                    
                                    let old_price = s.kraken_price;
                                    s.kraken_price = price;
                                    s.kraken_last_direction = direction;
                                    
                                    // Generate early signal if strong consecutive movement (3+ same direction)
                                    let signal_threshold: u8 = 3;
                                    if consecutive_ups >= signal_threshold {
                                        let side = Side::Up;
                                        s.early_signal = Some((side, Instant::now(), "Kraken"));
                                        s.early_signal_confirmations = 1;
                                        s.early_signal_ready = false; // Need confirmation from other exchange
                                    } else if consecutive_downs >= signal_threshold {
                                        let side = Side::Down;
                                        s.early_signal = Some((side, Instant::now(), "Kraken"));
                                        s.early_signal_confirmations = 1;
                                        s.early_signal_ready = false; // Need confirmation from other exchange
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] Kraken connect error: {}", e));
            }
        }
        
        {
            let mut s = state.write().await;
            s.kraken_status = ConnectionStatus::Disconnected;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ============================================================================
// COINBASE WEBSOCKET - Second fastest (~52ms latency) for signal confirmation
// ============================================================================

async fn run_coinbase_ws(state: Arc<RwLock<SharedState>>) {
    let url = "wss://ws-feed.exchange.coinbase.com";
    
    loop {
        {
            let mut s = state.write().await;
            s.coinbase_status = ConnectionStatus::Connecting;
            s.add_log("Connecting to Coinbase WebSocket (~52ms)...".to_string());
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                
                // Subscribe to BTC-USD matches (filled trades)
                let sub_msg = r#"{"type":"subscribe","product_ids":["BTC-USD"],"channels":["matches"]}"#;
                if let Err(e) = futures_util::SinkExt::send(&mut write, Message::Text(sub_msg.to_string().into())).await {
                    let mut s = state.write().await;
                    s.add_log(format!("[X] Coinbase subscribe failed: {}", e));
                    continue;
                }
                
                {
                    let mut s = state.write().await;
                    s.coinbase_status = ConnectionStatus::Connected;
                    s.add_log("[OK] Coinbase connected".to_string());
                }
                
                const COINBASE_TIMEOUT_SECS: u64 = 10;
                let mut last_price: Option<Decimal> = None;
                let mut consecutive_ups: u8 = 0;
                let mut consecutive_downs: u8 = 0;
                
                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(COINBASE_TIMEOUT_SECS),
                        read.next()
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => break,
                        Err(_) => {
                            let mut s = state.write().await;
                            s.add_log(format!("[!] Coinbase stale {}s, reconnecting...", COINBASE_TIMEOUT_SECS));
                            break;
                        }
                    };
                    
                    if let Ok(Message::Text(text)) = msg {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            if data.get("type").and_then(|v| v.as_str()) == Some("match") {
                                // Parse price and time
                                if let (Some(price_str), Some(time_str)) = (
                                    data.get("price").and_then(|v| v.as_str()),
                                    data.get("time").and_then(|v| v.as_str())
                                ) {
                                    if let Ok(price) = Decimal::from_str(price_str) {
                                        // Parse ISO timestamp to get latency
                                        let now_ms = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_millis() as u64;
                                        
                                        // Parse Coinbase ISO timestamp (e.g., "2024-01-15T12:30:45.123456Z")
                                        let trade_time_ms = if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(time_str) {
                                            dt.timestamp_millis() as u64
                                        } else {
                                            now_ms // Fallback if parse fails
                                        };
                                        let latency = now_ms.saturating_sub(trade_time_ms);
                                        
                                        // Determine direction from price change
                                        let direction: i8 = match last_price {
                                            Some(lp) if price > lp => 1,
                                            Some(lp) if price < lp => -1,
                                            _ => 0,
                                        };
                                        last_price = Some(price);
                                        
                                        // Track consecutive same-direction trades
                                        if direction > 0 {
                                            consecutive_ups += 1;
                                            consecutive_downs = 0;
                                        } else if direction < 0 {
                                            consecutive_downs += 1;
                                            consecutive_ups = 0;
                                        }
                                        
                                        let mut s = state.write().await;
                                        s.coinbase_msg_count += 1;
                                        s.coinbase_last_msg = Some(Instant::now());
                                        s.coinbase_latency_ms = latency;
                                        s.coinbase_price = price;
                                        s.coinbase_last_direction = direction;
                                        
                                        // Check if Coinbase confirms existing early signal
                                        let signal_threshold: u8 = 2;  // Lower threshold since this is confirmation
                                        if let Some((signal_side, signal_time, source)) = s.early_signal {
                                            // If signal is recent (<500ms) and Coinbase agrees
                                            if signal_time.elapsed() < Duration::from_millis(500) {
                                                let coinbase_agrees = match signal_side {
                                                    Side::Up => consecutive_ups >= signal_threshold,
                                                    Side::Down => consecutive_downs >= signal_threshold,
                                                };
                                                if coinbase_agrees && source != "Coinbase" {
                                                    s.early_signal_confirmations = s.early_signal_confirmations.saturating_add(1).min(5);
                                                    // Mark signal as ready to trade when 2+ exchanges agree
                                                    if s.early_signal_confirmations >= 2 {
                                                        s.early_signal_ready = true;
                                                    }
                                                }
                                            }
                                        } else {
                                            // No existing signal - create one if strong Coinbase movement
                                            if consecutive_ups >= 3 {
                                                s.early_signal = Some((Side::Up, Instant::now(), "Coinbase"));
                                                s.early_signal_confirmations = 1;
                                                s.early_signal_ready = false;
                                            } else if consecutive_downs >= 3 {
                                                s.early_signal = Some((Side::Down, Instant::now(), "Coinbase"));
                                                s.early_signal_confirmations = 1;
                                                s.early_signal_ready = false;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] Coinbase connect error: {}", e));
            }
        }
        
        {
            let mut s = state.write().await;
            s.coinbase_status = ConnectionStatus::Disconnected;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ============================================================================
// BYBIT WEBSOCKET - Third fastest (~86ms latency) for additional confirmation
// ============================================================================

async fn run_bybit_ws(state: Arc<RwLock<SharedState>>) {
    let url = "wss://stream.bybit.com/v5/public/spot";
    
    loop {
        {
            let mut s = state.write().await;
            s.bybit_status = ConnectionStatus::Connecting;
            s.add_log("Connecting to Bybit WebSocket (~86ms)...".to_string());
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                
                // Subscribe to BTC/USDT public trades
                let sub_msg = r#"{"op":"subscribe","args":["publicTrade.BTCUSDT"]}"#;
                if let Err(e) = futures_util::SinkExt::send(&mut write, Message::Text(sub_msg.to_string().into())).await {
                    let mut s = state.write().await;
                    s.add_log(format!("[X] Bybit subscribe failed: {}", e));
                    continue;
                }
                
                {
                    let mut s = state.write().await;
                    s.bybit_status = ConnectionStatus::Connected;
                    s.add_log("[OK] Bybit connected".to_string());
                }
                
                const BYBIT_TIMEOUT_SECS: u64 = 15;
                let mut last_price: Option<Decimal> = None;
                let mut consecutive_ups: u8 = 0;
                let mut consecutive_downs: u8 = 0;
                
                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(BYBIT_TIMEOUT_SECS),
                        read.next()
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => break,
                        Err(_) => {
                            let mut s = state.write().await;
                            s.add_log(format!("[!] Bybit stale {}s, reconnecting...", BYBIT_TIMEOUT_SECS));
                            break;
                        }
                    };
                    
                    if let Ok(Message::Text(text)) = msg {
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            // Bybit trade format: {"topic":"publicTrade.BTCUSDT","data":[{"T":timestamp_ms,"s":"BTCUSDT","S":"Buy/Sell","v":"qty","p":"price",...}]}
                            if let Some(trades) = data.get("data").and_then(|d| d.as_array()) {
                                if let Some(trade) = trades.last() {
                                    if let (Some(price_str), Some(time_ms), Some(side_str)) = (
                                        trade.get("p").and_then(|v| v.as_str()),
                                        trade.get("T").and_then(|v| v.as_u64()),
                                        trade.get("S").and_then(|v| v.as_str())
                                    ) {
                                        if let Ok(price) = Decimal::from_str(price_str) {
                                            let now_ms = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis() as u64;
                                            let latency = now_ms.saturating_sub(time_ms);
                                            
                                            // Bybit provides trade side directly: "Buy" = buyer initiated
                                            let direction: i8 = if side_str == "Buy" { 1 } else { -1 };
                                            
                                            // Track consecutive same-direction trades
                                            if direction > 0 {
                                                consecutive_ups += 1;
                                                consecutive_downs = 0;
                                            } else if direction < 0 {
                                                consecutive_downs += 1;
                                                consecutive_ups = 0;
                                            }
                                            
                                            let mut s = state.write().await;
                                            s.bybit_msg_count += 1;
                                            s.bybit_last_msg = Some(Instant::now());
                                            s.bybit_latency_ms = latency;
                                            s.bybit_price = price;
                                            s.bybit_last_direction = direction;
                                            
                                            // Check if Bybit confirms existing early signal
                                            let signal_threshold: u8 = 2;
                                            if let Some((signal_side, signal_time, source)) = s.early_signal {
                                                // If signal is recent (<500ms) and Bybit agrees
                                                if signal_time.elapsed() < Duration::from_millis(500) {
                                                    let bybit_agrees = match signal_side {
                                                        Side::Up => consecutive_ups >= signal_threshold,
                                                        Side::Down => consecutive_downs >= signal_threshold,
                                                    };
                                                    if bybit_agrees && source != "Bybit" {
                                                        s.early_signal_confirmations = s.early_signal_confirmations.saturating_add(1).min(5);
                                                        // Mark signal as ready to trade when 2+ exchanges agree
                                                        if s.early_signal_confirmations >= 2 {
                                                            s.early_signal_ready = true;
                                                        }
                                                    }
                                                }
                                            } else {
                                                // No existing signal - create one if strong Bybit movement
                                                if consecutive_ups >= 3 {
                                                    s.early_signal = Some((Side::Up, Instant::now(), "Bybit"));
                                                    s.early_signal_confirmations = 1;
                                                    s.early_signal_ready = false;
                                                } else if consecutive_downs >= 3 {
                                                    s.early_signal = Some((Side::Down, Instant::now(), "Bybit"));
                                                    s.early_signal_confirmations = 1;
                                                    s.early_signal_ready = false;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] Bybit connect error: {}", e));
            }
        }
        
        {
            let mut s = state.write().await;
            s.bybit_status = ConnectionStatus::Disconnected;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ============================================================================
// BITFINEX WEBSOCKET - Additional fast exchange (~30ms latency)
// ============================================================================

async fn run_bitfinex_ws(state: Arc<RwLock<SharedState>>) {
    let url = "wss://api-pub.bitfinex.com/ws/2";
    
    loop {
        {
            let mut s = state.write().await;
            s.bitfinex_status = ConnectionStatus::Connecting;
            s.add_log("Connecting to Bitfinex WebSocket (~30ms)...".to_string());
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                let (mut write, mut read) = ws_stream.split();
                
                // Subscribe to BTC/USD trades
                // Bitfinex format: {"event":"subscribe","channel":"trades","symbol":"tBTCUSD"}
                let sub_msg = r#"{"event":"subscribe","channel":"trades","symbol":"tBTCUSD"}"#;
                if let Err(e) = futures_util::SinkExt::send(&mut write, Message::Text(sub_msg.to_string().into())).await {
                    let mut s = state.write().await;
                    s.add_log(format!("[X] Bitfinex subscribe failed: {}", e));
                    continue;
                }
                
                {
                    let mut s = state.write().await;
                    s.bitfinex_status = ConnectionStatus::Connected;
                    s.add_log("[OK] Bitfinex connected".to_string());
                }
                
                const BITFINEX_TIMEOUT_SECS: u64 = 15;
                let mut consecutive_ups: u8 = 0;
                let mut consecutive_downs: u8 = 0;
                
                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(BITFINEX_TIMEOUT_SECS),
                        read.next()
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => break,
                        Err(_) => {
                            let mut s = state.write().await;
                            s.add_log(format!("[!] Bitfinex stale {}s, reconnecting...", BITFINEX_TIMEOUT_SECS));
                            break;
                        }
                    };
                    
                    if let Ok(Message::Text(text)) = msg {
                        // Bitfinex trade format: [CHANNEL_ID,"te",[ID,MTS,AMOUNT,PRICE]]
                        // "te" = trade executed, AMOUNT > 0 = buy, AMOUNT < 0 = sell
                        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                            // Skip subscription confirmations and heartbeats
                            if data.is_array() {
                                let arr = data.as_array().unwrap();
                                // Check for trade execution: [chan_id, "te", [id, mts, amount, price]]
                                if arr.len() >= 3 {
                                    if let Some(msg_type) = arr.get(1).and_then(|v| v.as_str()) {
                                        if msg_type == "te" {
                                            if let Some(trade_arr) = arr.get(2).and_then(|v| v.as_array()) {
                                                if trade_arr.len() >= 4 {
                                                    let mts = trade_arr.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u64;
                                                    let amount = trade_arr.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                    let price_f = trade_arr.get(3).and_then(|v| v.as_f64()).unwrap_or(0.0);
                                                    
                                                    if let Some(price) = Decimal::from_f64_retain(price_f) {
                                                        let now_ms = std::time::SystemTime::now()
                                                            .duration_since(std::time::UNIX_EPOCH)
                                                            .unwrap_or_default()
                                                            .as_millis() as u64;
                                                        let latency = now_ms.saturating_sub(mts);
                                                        
                                                        // Bitfinex: amount > 0 = buy (bullish), amount < 0 = sell (bearish)
                                                        let direction: i8 = if amount > 0.0 { 1 } else { -1 };
                                                        
                                                        // Track consecutive same-direction trades
                                                        if direction > 0 {
                                                            consecutive_ups += 1;
                                                            consecutive_downs = 0;
                                                        } else {
                                                            consecutive_downs += 1;
                                                            consecutive_ups = 0;
                                                        }
                                                        
                                                        let mut s = state.write().await;
                                                        s.bitfinex_msg_count += 1;
                                                        s.bitfinex_last_msg = Some(Instant::now());
                                                        s.bitfinex_latency_ms = latency;
                                                        s.bitfinex_price = price;
                                                        s.bitfinex_last_direction = direction;
                                                        
                                                        // Check if Bitfinex confirms existing early signal OR creates new one
                                                        let signal_threshold: u8 = 3;
                                                        if let Some((signal_side, signal_time, source)) = s.early_signal {
                                                            // If signal is recent (<500ms) and Bitfinex agrees
                                                            if signal_time.elapsed() < Duration::from_millis(500) {
                                                                let bitfinex_agrees = match signal_side {
                                                                    Side::Up => consecutive_ups >= 2,
                                                                    Side::Down => consecutive_downs >= 2,
                                                                };
                                                                if bitfinex_agrees && source != "Bitfinex" {
                                                                    s.early_signal_confirmations = s.early_signal_confirmations.saturating_add(1).min(5);
                                                                    // Mark signal as ready to trade when 2+ exchanges agree
                                                                    if s.early_signal_confirmations >= 2 {
                                                                        s.early_signal_ready = true;
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            // No existing signal - create one if strong Bitfinex movement
                                                            if consecutive_ups >= signal_threshold {
                                                                s.early_signal = Some((Side::Up, Instant::now(), "Bitfinex"));
                                                                s.early_signal_confirmations = 1;
                                                                s.early_signal_ready = false;
                                                            } else if consecutive_downs >= signal_threshold {
                                                                s.early_signal = Some((Side::Down, Instant::now(), "Bitfinex"));
                                                                s.early_signal_confirmations = 1;
                                                                s.early_signal_ready = false;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("[X] Bitfinex connect error: {}", e));
            }
        }
        
        {
            let mut s = state.write().await;
            s.bitfinex_status = ConnectionStatus::Disconnected;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    outcomes: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
}

/// Generate market slug for 5-minute BTC up/down markets
/// Format: btc-updown-5m-{unix_timestamp}
/// Timestamp is aligned to 5-minute intervals (divisible by 300)
fn generate_market_slug() -> (String, i64) {
    let now_utc = Utc::now();
    
    // Calculate interval start (5-minute window aligned to epoch)
    let interval_start = (now_utc.timestamp() / MARKET_WINDOW_SECS) * MARKET_WINDOW_SECS;
    
    let slug = format!("btc-updown-5m-{}", interval_start);
    (slug, interval_start)
}

async fn run_market_discovery(state: Arc<RwLock<SharedState>>) {
    loop {
        let (slug, interval_start) = generate_market_slug();
        let url = format!("https://gamma-api.polymarket.com/markets/slug/{}", slug);

        match HTTP_CLIENT.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(market) = resp.json::<GammaMarket>().await {
                        if let (Some(outcomes_str), Some(token_ids_str)) = (&market.outcomes, &market.clob_token_ids) {
                            let outcomes: Vec<String> = serde_json::from_str(outcomes_str).unwrap_or_default();
                            let token_ids: Vec<String> = serde_json::from_str(token_ids_str).unwrap_or_default();

                            if outcomes.len() >= 2 && token_ids.len() >= 2 {
                                let mut up_token = String::new();
                                let mut down_token = String::new();

                                for (i, outcome) in outcomes.iter().enumerate() {
                                    if outcome.to_lowercase().contains("up") {
                                        up_token = token_ids.get(i).cloned().unwrap_or_default();
                                    } else if outcome.to_lowercase().contains("down") {
                                        down_token = token_ids.get(i).cloned().unwrap_or_default();
                                    }
                                }

                                if !up_token.is_empty() && !down_token.is_empty() {
                                    let mut s = state.write().await;
                                    let is_new = s.market.as_ref().map(|m| m.slug != slug).unwrap_or(true);
                                    if is_new {
                                        s.add_log(format!("[OK] New market: {}", slug));
                                        s.orderbook = Orderbook::default(); // Reset orderbook for new market
                                    }
                                    s.market = Some(BtcMarket { 
                                        slug, 
                                        up_token_id: up_token, 
                                        down_token_id: down_token,
                                        interval_start,
                                    });
                                }
                            }
                        }
                    }
                } else {
                    let mut s = state.write().await;
                    s.add_log(format!("Market API returned {}", resp.status()));
                }
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("Market discovery error: {}", e));
            }
        }
        
        tokio::time::sleep(Duration::from_secs(2)).await;  // Check every 2s for 5-min markets
    }
}

#[derive(Debug, Deserialize)]
struct OrderbookMsg {
    event_type: Option<String>,
    asset_id: Option<String>,
    bids: Option<Vec<OrderLevel>>,
    asks: Option<Vec<OrderLevel>>,
    price_changes: Option<Vec<PriceChange>>,
    timestamp: Option<String>,  // Unix ms timestamp as string
}

#[derive(Debug, Deserialize)]
struct PriceChange {
    asset_id: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
    timestamp: Option<String>,  // Unix ms timestamp as string
}

#[derive(Debug, Deserialize)]
struct OrderLevel {
    price: String,
    size: Option<String>,
}

async fn run_polymarket_ws(state: Arc<RwLock<SharedState>>) {
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    
    loop {
        let (up_token, down_token, market_slug) = {
            let s = state.read().await;
            match &s.market {
                Some(m) => (m.up_token_id.clone(), m.down_token_id.clone(), m.slug.clone()),
                None => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        };
        
        {
            let mut s = state.write().await;
            s.polymarket_status = ConnectionStatus::Connecting;
            s.add_log(format!("Connecting to Polymarket WS for {}...", market_slug));
        }
        
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                {
                    let mut s = state.write().await;
                    s.polymarket_status = ConnectionStatus::Connected;
                    s.add_log("[OK] Polymarket WebSocket connected".to_string());
                }
                
                let (mut write, mut read) = ws_stream.split();
                
                let sub_msg = serde_json::json!({
                    "assets_ids": [&up_token, &down_token],
                    "type": "market"
                });
                
                use futures_util::SinkExt;
                if write.send(Message::Text(sub_msg.to_string().into())).await.is_err() {
                    let mut s = state.write().await;
                    s.add_log("Failed to subscribe to Polymarket".to_string());
                    continue;
                }
                
                {
                    let mut s = state.write().await;
                    s.add_log(format!("Subscribed to UP: {}...", &up_token[..16.min(up_token.len())]));
                    s.add_log(format!("Subscribed to DOWN: {}...", &down_token[..16.min(down_token.len())]));
                }
                
                let current_up = up_token.clone();
                
                // Stale connection detection: 10s timeout
                const POLY_TIMEOUT_SECS: u64 = 10;
                
                loop {
                    let msg = match tokio::time::timeout(
                        Duration::from_secs(POLY_TIMEOUT_SECS),
                        read.next()
                    ).await {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            let mut s = state.write().await;
                            s.add_log("Polymarket stream ended".to_string());
                            break;
                        }
                        Err(_) => {
                            // Timeout - connection is stale/zombie
                            let mut s = state.write().await;
                            s.add_log(format!("[!] Polymarket stale (no data {}s), forcing reconnect...", POLY_TIMEOUT_SECS));
                            break;
                        }
                    };
                    
                    // Check if market changed
                    {
                        let s = state.read().await;
                        if let Some(m) = &s.market {
                            if m.up_token_id != current_up {
                                break;
                            }
                        }
                    }
                    
                    if let Ok(Message::Text(text)) = msg {
                        // Count ALL messages, even if parsing fails
                        {
                            let mut s = state.write().await;
                            s.polymarket_msg_count += 1;
                            s.polymarket_last_msg = Some(Instant::now());
                        }
                        
                        match serde_json::from_str::<OrderbookMsg>(&text) {
                            Err(e) => {
                                // Log parse failures for debugging (only first 100 chars)
                                let preview = if text.len() > 100 { &text[..100] } else { &text };
                                let mut s = state.write().await;
                                if s.orderbook.update_count == 0 && s.polymarket_msg_count % 1000 == 1 {
                                    s.add_log(format!("[DEBUG] Parse fail: {} | {}", e, preview));
                                }
                            }
                            Ok(ob) => {
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            
                            let mut s = state.write().await;
                            
                            // Calculate latency if timestamp available (timestamp is a string)
                            if let Some(ts_str) = &ob.timestamp {
                                if let Ok(ts) = ts_str.parse::<u64>() {
                                    s.polymarket_latency_ms = now_ms.saturating_sub(ts);
                                }
                            }
                            
                            // Handle price_change events (most common)
                            if ob.event_type.as_deref() == Some("price_change") {
                                if let Some(changes) = &ob.price_changes {
                                    for change in changes {
                                        // Use change timestamp if available (timestamp is a string)
                                        if let Some(ts_str) = &change.timestamp {
                                            if let Ok(ts) = ts_str.parse::<u64>() {
                                                s.polymarket_latency_ms = now_ms.saturating_sub(ts);
                                            }
                                        }
                                        
                                        if let Some(asset_id) = &change.asset_id {
                                            let is_up = asset_id == &up_token;
                                            let is_down = asset_id == &down_token;
                                            
                                            if is_up || is_down {
                                                let best_bid = change.best_bid.as_ref()
                                                    .and_then(|p| p.parse::<Decimal>().ok());
                                                let best_ask = change.best_ask.as_ref()
                                                    .and_then(|p| p.parse::<Decimal>().ok());
                                                
                                                if is_up {
                                                    if best_bid.is_some() { s.orderbook.up_best_bid = best_bid; }
                                                    if best_ask.is_some() { s.orderbook.up_best_ask = best_ask; }
                                                } else {
                                                    if best_bid.is_some() { s.orderbook.down_best_bid = best_bid; }
                                                    if best_ask.is_some() { s.orderbook.down_best_ask = best_ask; }
                                                }
                                                s.orderbook.last_update = Some(Instant::now());
                                                s.orderbook.update_count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                            // Handle book events (full orderbook snapshot)
                            else if ob.event_type.as_deref() == Some("book") {
                                if let Some(asset_id) = &ob.asset_id {
                                    let is_up = asset_id == &up_token;
                                    let is_down = asset_id == &down_token;
                                    
                                    if is_up || is_down {
                                        // Parse top 5 bid levels (sorted high to low)
                                        let mut bids: Vec<PriceLevel> = ob.bids.as_ref()
                                            .map(|levels| {
                                                let mut parsed: Vec<PriceLevel> = levels.iter()
                                                    .filter_map(|l| {
                                                        let price = l.price.parse::<Decimal>().ok()?;
                                                        let size = l.size.as_ref()
                                                            .and_then(|s| s.parse::<Decimal>().ok())
                                                            .unwrap_or(Decimal::ZERO);
                                                        Some(PriceLevel { price, size })
                                                    })
                                                    .collect();
                                                parsed.sort_by(|a, b| b.price.cmp(&a.price)); // Descending
                                                parsed.truncate(5);
                                                parsed
                                            })
                                            .unwrap_or_default();
                                        
                                        // Parse top 5 ask levels (sorted low to high)
                                        let mut asks: Vec<PriceLevel> = ob.asks.as_ref()
                                            .map(|levels| {
                                                let mut parsed: Vec<PriceLevel> = levels.iter()
                                                    .filter_map(|l| {
                                                        let price = l.price.parse::<Decimal>().ok()?;
                                                        let size = l.size.as_ref()
                                                            .and_then(|s| s.parse::<Decimal>().ok())
                                                            .unwrap_or(Decimal::ZERO);
                                                        Some(PriceLevel { price, size })
                                                    })
                                                    .collect();
                                                parsed.sort_by(|a, b| a.price.cmp(&b.price)); // Ascending
                                                parsed.truncate(5);
                                                parsed
                                            })
                                            .unwrap_or_default();
                                        
                                        let best_bid = bids.first().map(|l| l.price);
                                        let best_ask = asks.first().map(|l| l.price);
                                        let bid_depth = ob.bids.as_ref().map(|b| b.len()).unwrap_or(0);
                                        let ask_depth = ob.asks.as_ref().map(|a| a.len()).unwrap_or(0);
                                        
                                        if is_up {
                                            s.orderbook.up_best_bid = best_bid;
                                            s.orderbook.up_best_ask = best_ask;
                                            s.orderbook.up_bids = bids;
                                            s.orderbook.up_asks = asks;
                                            s.orderbook.up_bid_depth = bid_depth;
                                            s.orderbook.up_ask_depth = ask_depth;
                                        } else {
                                            s.orderbook.down_best_bid = best_bid;
                                            s.orderbook.down_best_ask = best_ask;
                                            s.orderbook.down_bids = bids;
                                            s.orderbook.down_asks = asks;
                                            s.orderbook.down_bid_depth = bid_depth;
                                            s.orderbook.down_ask_depth = ask_depth;
                                        }
                                        s.orderbook.last_update = Some(Instant::now());
                                        s.orderbook.update_count += 1;
                                    }
                                }
                            }
                        } // end match Ok(ob)
                        } // end match
                    }
                } // end loop
            }
            Err(e) => {
                let mut s = state.write().await;
                s.add_log(format!("Polymarket connection error: {}", e));
            }
        }
        
        {
            let mut s = state.write().await;
            s.polymarket_status = ConnectionStatus::Disconnected;
            s.add_log("Polymarket disconnected, reconnecting...".to_string());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ============================================================================
// ORDER EXECUTION
// ============================================================================

/// Get database path - uses /data on server, ./data locally
fn get_db_path() -> String {
    if std::path::Path::new("/data").exists() {
        "/data/trades.db".to_string()
    } else {
        // Create local data directory if needed
        let _ = std::fs::create_dir_all("data");
        "data/trades.db".to_string()
    }
}

/// Initialize SQLite database with trades table
fn init_database() -> Result<Connection> {
    let db_path = get_db_path();
    // Note: DB path logged at startup, not here to avoid TUI corruption
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            pnl_pct REAL NOT NULL,
            hold_ms INTEGER NOT NULL,
            ml_prediction_vol REAL NOT NULL,
            btc_price REAL NOT NULL,
            market_slug TEXT NOT NULL,
            dry_run INTEGER NOT NULL,
            entry_type TEXT DEFAULT 'Momentum',
            trigger_exchange TEXT DEFAULT 'Binance',
            z_score REAL DEFAULT 0.0,
            direction_prob REAL DEFAULT 0.5,
            direction_raw REAL DEFAULT 0.5,
            displacement_usd REAL DEFAULT 0.0,
            elapsed_pct REAL DEFAULT 0.0,
            liq_count REAL DEFAULT 0.0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )?;
    // Migration for existing DBs - add new columns
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN entry_type TEXT DEFAULT 'Momentum'", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN trigger_exchange TEXT DEFAULT 'Binance'", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN z_score REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN direction_prob REAL DEFAULT 0.5", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN direction_raw REAL DEFAULT 0.5", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN displacement_usd REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN elapsed_pct REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE trades ADD COLUMN liq_count REAL DEFAULT 0.0", []);
    // Create index for faster queries
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)",
        [],
    )?;
    Ok(conn)
}

/// Log trade to SQLite database
fn log_trade_to_db(trade: &TradeRecord, btc_price: Decimal, market_slug: &str, dry_run: bool, 
                   direction_prob: f32, direction_raw: f32, displacement_usd: f64, elapsed_pct: f64, liq_count: f64) {
    let db_path = get_db_path();
    let conn = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(_) => {
            // Don't eprintln - corrupts TUI. Error logged to internal state.
            return;
        }
    };
    
    let result = conn.execute(
        "INSERT INTO trades (timestamp, side, entry_price, exit_price, pnl_pct, hold_ms, ml_prediction_vol, btc_price, market_slug, dry_run, entry_type, trigger_exchange, z_score, direction_prob, direction_raw, displacement_usd, elapsed_pct, liq_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
        params![
            trade.time.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            format!("{:?}", trade.side),
            trade.entry_price.to_f64().unwrap_or(0.0),
            trade.exit_price.to_f64().unwrap_or(0.0),
            (trade.pnl * dec!(100)).to_f64().unwrap_or(0.0),
            trade.hold_ms,
            trade.ml_prediction_vol,
            btc_price.to_f64().unwrap_or(0.0),
            market_slug,
            if dry_run { 1 } else { 0 },
            format!("{:?}", trade.entry_type),
            &trade.trigger_exchange,
            trade.z_score,
            direction_prob,
            direction_raw,
            displacement_usd,
            elapsed_pct,
            liq_count,
        ],
    );
    
    if let Err(e) = result {
        // Only log errors, not successes - eprintln corrupts TUI
        let _ = e; // Suppress warning, error already logged to internal state
    }
}

/// Load historical trades from database on startup
fn load_trades_from_db(state: &mut SharedState) {
    let db_path = get_db_path();
    let conn = match Connection::open(&db_path) {
        Ok(c) => c,
        Err(_) => {
            state.add_log(format!("No trades database found at {} - starting fresh", db_path));
            return;
        }
    };
    
    let mut stmt = match conn.prepare(
        "SELECT timestamp, side, entry_price, exit_price, pnl_pct, hold_ms, ml_prediction_vol 
         FROM trades ORDER BY timestamp DESC LIMIT 1000"
    ) {
        Ok(s) => s,
        Err(e) => {
            state.add_log(format!("Failed to query trades: {}", e));
            return;
        }
    };
    
    let mut loaded_count = 0;
    let mut total_pnl = dec!(0);
    
    let trade_iter = stmt.query_map([], |row| {
        let timestamp: String = row.get(0)?;
        let side_str: String = row.get(1)?;
        let entry_price: f64 = row.get(2)?;
        let exit_price: f64 = row.get(3)?;
        let pnl_pct: f64 = row.get(4)?;
        let hold_ms: u64 = row.get(5)?;
        let ml_vol: f64 = row.get(6)?;
        
        Ok((timestamp, side_str, entry_price, exit_price, pnl_pct, hold_ms, ml_vol))
    });
    
    if let Ok(trades) = trade_iter {
        for trade_result in trades {
            if let Ok((timestamp, side_str, entry_price, exit_price, pnl_pct, hold_ms, ml_vol)) = trade_result {
                let side = if side_str == "Up" { Side::Up } else { Side::Down };
                let time = DateTime::parse_from_str(&format!("{} +0000", timestamp), "%Y-%m-%d %H:%M:%S%.3f %z")
                    .map(|dt| dt.with_timezone(&Local))
                    .unwrap_or_else(|_| Local::now());
                
                // pnl_pct is percentage (e.g., 3.0 = 3%), convert to decimal and multiply by size
                let pnl = Decimal::from_f64_retain(pnl_pct / 100.0).unwrap_or(dec!(0));
                let size = dec!(10);  // Position size in shares
                total_pnl += pnl * size;  // Convert to USD
                
                // Track stats
                if pnl > dec!(0) {
                    state.win_count += 1;
                }
                state.trade_count += 1;
                
                state.trades.push_front(TradeRecord {
                    time,
                    side,
                    entry_price: Decimal::from_f64_retain(entry_price).unwrap_or(dec!(0)),
                    exit_price: Decimal::from_f64_retain(exit_price).unwrap_or(dec!(0)),
                    pnl,
                    hold_ms,
                    ml_prediction_vol: ml_vol as f32,
                    size,  // Use same size as PnL calculation
                    entry_type: EntryType::Momentum,  // Historical trades default to Momentum
                    trigger_exchange: "Unknown".to_string(),  // Historical trades don't have trigger info
                    z_score: 0.0,  // Historical trades don't have z_score
                });
                loaded_count += 1;
            }
        }
    }
    
    state.total_pnl = total_pnl;
    if loaded_count > 0 {
        state.add_log(format!("[OK] Loaded {} trades from database (PnL: ${:.2})", loaded_count, total_pnl));
    } else {
        state.add_log("Starting with empty trade history".to_string());
    }
}

// DEPRECATED: CSV logging removed, using DB only
#[allow(dead_code)]
fn log_trade_to_csv(trade: &TradeRecord, btc_price: Decimal, market_slug: &str, dry_run: bool) {
    let log_path = "/data/trades.csv";
    let file_exists = Path::new(log_path).exists();
    
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path);
    
    if let Ok(mut file) = file {
        // Write header if new file
        if !file_exists {
            let _ = writeln!(file, "timestamp,side,entry_price,exit_price,pnl_pct,hold_ms,ml_prediction_vol,btc_price,market_slug,dry_run");
        }
        
        let _ = writeln!(file, "{},{:?},{:.4},{:.4},{:.4},{},{:.2},{:.2},{},{}",
            trade.time.format("%Y-%m-%d %H:%M:%S%.3f"),
            trade.side,
            trade.entry_price,
            trade.exit_price,
            trade.pnl * dec!(100),
            trade.hold_ms,
            trade.ml_prediction_vol,
            btc_price,
            market_slug,
            dry_run
        );
    }
}

/// Execute order using pre-signed orders (instant) or fallback to regular order
/// is_up: true for UP token, false for DOWN token
/// expected_price: orderbook price to record if order is accepted but not immediately filled
/// Returns the fill price on success, None on failure
async fn execute_order_async(is_up: bool, token_id: &str, size: Decimal, expected_price: Option<Decimal>) -> Result<Option<Decimal>> {
    let executor = EXECUTOR.get().ok_or_else(|| anyhow::anyhow!("Executor not initialized"))?;
    
    // Try pre-signed first (instant submission) - BUT only if size matches!
    // Pre-signed orders are created with a specific size. If position size differs
    // (due to dynamic sizing: 1x=10, 1.5x=15, 2x=20), we must use fallback.
    if executor.has_pre_signed_with_size(is_up, size).await {
        return executor.submit_pre_signed(is_up, expected_price).await;
    }
    
    // Fallback to regular order (slower, requires build+sign)
    // This ensures correct size even if pre-signed was created with different size
    executor.buy(token_id, size, is_up, expected_price).await
}

/// Execute order with retry logic - critical for ensuring exits complete
/// Retries up to max_retries times with 100ms delay between attempts
/// expected_price: orderbook price to record if order is accepted but not immediately filled
async fn execute_order_with_retry(is_up: bool, token_id: &str, size: Decimal, max_retries: u32, expected_price: Option<Decimal>) -> Result<Option<Decimal>> {
    for attempt in 0..=max_retries {
        match execute_order_async(is_up, token_id, size, expected_price).await {
            Ok(Some(price)) => return Ok(Some(price)),
            Ok(None) => {
                if attempt < max_retries {
                    // Brief delay before retry
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            Err(e) => {
                if attempt < max_retries {
                    log_error(&format!("Order attempt {} failed: {:?}, retrying...", attempt + 1, e));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(e);
            }
        }
    }
    Ok(None)
}

/// Refresh pre-signed orders for both UP and DOWN tokens
async fn refresh_pre_signed_orders(up_token: &str, down_token: &str, size: Decimal) {
    if let Some(executor) = EXECUTOR.get() {
        if let Err(e) = executor.refresh_pre_signed(up_token, down_token, size).await {
            // Don't eprintln - corrupts TUI
            let _ = e;
        }
    }
}

/// Fetch wallet USDC balance from Polygon chain
async fn fetch_wallet_balance() -> Result<Decimal> {
    let funder = std::env::var("POLYMARKET_FUNDER")
        .or_else(|_| std::env::var("PM_FUNDER"))
        .unwrap_or_default();
    
    if funder.is_empty() {
        return Ok(dec!(0));
    }
    
    // USDC contract on Polygon (6 decimals)
    let usdc_contract = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
    
    // Build balanceOf(address) call data
    // Function selector: 0x70a08231
    // Pad address to 32 bytes
    let funder_clean = funder.trim_start_matches("0x");
    let call_data = format!("0x70a08231000000000000000000000000{}", funder_clean);
    
    // Use public Polygon RPC
    let rpc_url = "https://polygon-rpc.com";
    
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": usdc_contract,
            "data": call_data
        }, "latest"],
        "id": 1
    });
    
    let resp = HTTP_CLIENT
        .post(rpc_url)
        .json(&body)
        .send()
        .await?;
    
    if resp.status().is_success() {
        let json: serde_json::Value = resp.json().await?;
        if let Some(result) = json.get("result").and_then(|r| r.as_str()) {
            // Parse hex result (USDC has 6 decimals)
            let hex_str = result.trim_start_matches("0x");
            if let Ok(balance_raw) = u128::from_str_radix(hex_str, 16) {
                // Convert from 6 decimals to Decimal
                let balance = Decimal::from(balance_raw) / dec!(1_000_000);
                return Ok(balance);
            }
        }
    }
    
    Ok(dec!(0))
}

/// Execute order (async, called from strategy loop)
/// Returns the fill price on success, None on failure
/// In dry_run mode, returns the expected_price (simulates perfect fill)
/// Use retries > 0 for critical orders like exits that MUST complete
async fn execute_order(is_up: bool, token_id: &str, size: Decimal, dry_run: bool, expected_price: Option<Decimal>) -> Result<Option<Decimal>> {
    if dry_run {
        return Ok(expected_price);  // Simulate fill at expected orderbook price
    }
    execute_order_async(is_up, token_id, size, expected_price).await
}

/// Execute order with mandatory retries - use for exits that MUST complete
async fn execute_order_must_fill(is_up: bool, token_id: &str, size: Decimal, dry_run: bool, expected_price: Option<Decimal>) -> Result<Option<Decimal>> {
    if dry_run {
        return Ok(expected_price);
    }
    // Retry up to 3 times for exits - they MUST complete
    execute_order_with_retry(is_up, token_id, size, 3, expected_price).await
}

// ============================================================================
// TRADING STRATEGY
// ============================================================================

async fn run_strategy(
    state: Arc<RwLock<SharedState>>,
    mut tick_rx: mpsc::Receiver<()>,
    intra_predictor: Option<Arc<IntraWindowPredictor>>,
) {
    let mut last_pre_sign_refresh = Instant::now();
    let mut last_market_slug: Option<String> = None;
    let mut last_reconcile_check = Instant::now();
    
    // Event-driven: wait for tick signals instead of polling
    let mut tick_count: u64 = 0;
    while tick_rx.recv().await.is_some() {
        tick_count += 1;
        
        let mut s = state.write().await;
        
        // Very first debug log
        if tick_count == 1 {
            s.add_log("[DBG-STRATEGY] First tick received!".to_string());
        }
        
        // ====================================================================
        // SHARE RECONCILIATION CHECK - Every 60 seconds
        // Alert if UP and DOWN shares are imbalanced
        // ====================================================================
        if last_reconcile_check.elapsed() > Duration::from_secs(60) {
            let up_total = s.up_shares_bought;
            let down_total = s.down_shares_bought;
            let diff = (up_total - down_total).abs();
            if diff > dec!(5) {
                s.add_log(format!("[!!!] SHARE IMBALANCE DETECTED: UP={} DOWN={} diff={}", 
                    up_total, down_total, diff));
            }
            last_reconcile_check = Instant::now();
        }
        
        // Check market
        let market = match &s.market {
            Some(m) => m.clone(),
            None => {
                s.block_reason = BlockReason::NoMarket;
                continue;
            }
        };
        
        // ====================================================================
        // SAFETY NET: Clear any positions from a DIFFERENT market interval
        // This catches edge cases where market changed but slug_changed block
        // didn't run (e.g., during MarketClosing continue)
        // ====================================================================
        let stale_positions: Vec<_> = s.positions.iter()
            .filter(|p| p.market_interval_start != market.interval_start)
            .cloned()
            .collect();
        if !stale_positions.is_empty() {
            let stale_up: Decimal = stale_positions.iter()
                .filter(|p| p.side == Side::Up)
                .map(|p| p.size)
                .sum();
            let stale_down: Decimal = stale_positions.iter()
                .filter(|p| p.side == Side::Down)
                .map(|p| p.size)
                .sum();
            s.add_log(format!("[!!!] STALE {} positions from interval {} (current: {}) - UP:{} DOWN:{} - CLEARING",
                stale_positions.len(), 
                stale_positions.first().map(|p| p.market_interval_start).unwrap_or(0),
                market.interval_start,
                stale_up, stale_down));
            
            // FIX: Track cleared positions to keep share counts balanced
            // These were entries without exits, so add to opposite side to balance
            s.down_shares_bought += stale_up;   // UP entries balance with DOWN
            s.up_shares_bought += stale_down;   // DOWN entries balance with UP
            
            s.positions.retain(|p| p.market_interval_start == market.interval_start);
        }
        
        // DEBUG: Log that we have a market
        if s.logs.is_empty() {
            s.add_log(format!("[DBG] Got market: {}", market.slug));
        }
        
        // Check if market is closing within 15 seconds - NO NEW ENTRIES
        // (5-min markets need tighter closing window)
        let now = Utc::now().timestamp();
        let market_end = market.interval_start + MARKET_WINDOW_SECS; // 5 min = 300 seconds
        let seconds_until_close = market_end - now;
        
        if seconds_until_close <= 15 {
            s.block_reason = BlockReason::MarketClosing;
            
            // FORCE EXIT all positions when market closing in <5 seconds
            if seconds_until_close <= 5 && !s.positions.is_empty() {
                let positions_count = s.positions.len();
                s.add_log(format!("[!] Market closing in {}s - FORCE EXITING {} positions", 
                    seconds_until_close, positions_count));
                
                // Exit all positions immediately - but only remove if exit succeeds!
                let mut positions_to_exit: Vec<_> = s.positions.drain(..).collect();
                let mut failed_positions = Vec::new();
                
                for pos in positions_to_exit.drain(..) {
                    let exit_price = match pos.side {
                        Side::Up => s.orderbook.down_best_ask,
                        Side::Down => s.orderbook.up_best_ask,
                    };
                    
                    let dry_run = s.dry_run;
                    let is_up_exit = pos.side == Side::Down;  // Exit DOWN = buy UP
                    let token_id = match pos.side {
                        Side::Up => market.down_token_id.clone(),
                        Side::Down => market.up_token_id.clone(),
                    };
                    let pos_size = pos.size;
                    drop(s);  // Release lock for network call
                    
                    // CRITICAL: Use must_fill with retries - force exits MUST complete
                    let actual_exit = execute_order_must_fill(is_up_exit, &token_id, pos_size, dry_run, exit_price).await;
                    
                    s = state.write().await;
                    
                    match actual_exit {
                        Ok(Some(exit_p)) => {
                            // Success - calculate PnL
                            let pnl_pct = dec!(1) - exit_p - pos.entry_price;
                            let pnl_usd = pnl_pct * pos.size;
                            s.total_pnl += pnl_usd;
                            s.trade_count += 1;
                            if pnl_usd > dec!(0) { s.win_count += 1; }
                            
                            // CRITICAL: Track shares bought for FORCE EXIT (same as regular exit)
                            match pos.side {
                                Side::Up => {
                                    s.down_shares_bought += pos.size;  // Exit UP = buy DOWN
                                    s.up_exit_count += 1;
                                },
                                Side::Down => {
                                    s.up_shares_bought += pos.size;    // Exit DOWN = buy UP
                                    s.down_exit_count += 1;
                                },
                            }
                            
                            s.add_log(format!("[!] FORCE EXIT {:?} @ {:.2}c | PnL: {:.2}% | Market closing",
                                pos.side, exit_p * dec!(100), pnl_pct * dec!(100)));
                        },
                        Ok(None) | Err(_) => {
                            // Failed - keep position, will try again next tick
                            s.add_log(format!("[!!!] FORCE EXIT FAILED {:?} - will retry", pos.side));
                            failed_positions.push(pos);
                        }
                    }
                }
                
                // Put failed positions back
                s.positions.extend(failed_positions);
            }
            continue;
        }
        
        // === DEBUG: We passed the MarketClosing check! ===
        if s.logs.len() < 5 {  // Only log first few times
            s.add_log(format!("[DBG-REACH] Passed MarketClosing, secs_left={}", seconds_until_close));
        }
        
        // Detect new market (5-minute session) and reset trade counts
        let current_slug = Some(market.slug.clone());
        let slug_changed = current_slug != last_market_slug;
        
        // Debug: ALWAYS log on FIRST market detection
        if last_market_slug.is_none() {
            s.add_log(format!("[!!!DBG!!!] FIRST: changed={}, current={}", 
                slug_changed, market.slug));
        }
        
        if slug_changed {
            s.add_log(format!("[!!!DBG!!!] ENTERING slug_changed block for {}", market.slug));
            
            // ====================================================================
            // CRITICAL: Clear orphaned positions from previous market
            // These are positions that failed to exit before market closed
            // They can NEVER be exited (wrong token IDs) so we must remove them
            // ====================================================================
            let orphan_count = s.positions.len();
            if orphan_count > 0 {
                // Calculate total orphaned shares for accurate logging
                let orphan_up: Decimal = s.positions.iter()
                    .filter(|p| p.side == Side::Up)
                    .map(|p| p.size)
                    .sum();
                let orphan_down: Decimal = s.positions.iter()
                    .filter(|p| p.side == Side::Down)
                    .map(|p| p.size)
                    .sum();
                    
                s.add_log(format!("[!!!] ORPHANED {} positions from old market! UP:{} DOWN:{} - CLEARING",
                    orphan_count, orphan_up, orphan_down));
                
                // FIX: Track cleared positions to keep share counts balanced
                // These were entries without exits, so add to opposite side to balance
                s.down_shares_bought += orphan_up;   // UP entries balance with DOWN
                s.up_shares_bought += orphan_down;   // DOWN entries balance with UP
                    
                // These shares were bought but never sold, so we have the tokens
                // but no exit. The market resolved so we either won or lost based
                // on the outcome. For accounting, track as "unexited" separately.
                s.positions.clear();
            }
            
            // === ONLINE CALIBRATION: Resolve pending samples from previous market ===
            // Determine if UP or DOWN won in the previous market
            let window_start = s.intra_window_state.window_start_price;
            let window_close = s.btc_price.to_f64().unwrap_or(0.0);
            let pending_count = s.pending_calibration.len();
            
            // CRITICAL: Reset intra_window_state for new market (fixes Z-score bug)
            let current_btc = s.btc_price.to_f64().unwrap_or(0.0);
            s.intra_window_state = IntraWindowState::new();
            s.intra_window_state.window_start_price = current_btc;
            s.z_score = 0.0;  // Reset displayed Z-score
            
            // Debug: Log calibration state on every market transition
            s.add_log(format!("[CAL-DBG] Market change: pending={}, start={:.2}, close={:.2}", 
                pending_count, window_start, window_close));
            
            if window_start > 0.0 && pending_count > 0 {
                let up_won = window_close > window_start;
                
                // Drain pending calibration samples first to avoid borrow conflicts
                let prev_market_interval = s.last_resolved_market;
                let pending: Vec<_> = s.pending_calibration.drain(..).collect();
                let mut updated_count = 0;
                
                s.add_log(format!("[CAL-DBG] Processing {} pending, prev_interval={}, UP_won={}", pending.len(), prev_market_interval, up_won));
                
                for (raw_prob, market_interval, trade_side) in pending {
                    // Only process trades from the just-closed market
                    // (market_interval > prev_market_interval ensures we don't double-count)
                    if market_interval > prev_market_interval {
                        // FIX: Adjust probability and outcome based on WHICH SIDE we traded
                        // raw_prob is P(UP), we need P(our_side_wins)
                        // If we traded UP: prob_our_side = raw_prob, outcome = 1 if UP won
                        // If we traded DOWN: prob_our_side = 1 - raw_prob, outcome = 1 if DOWN won
                        let (prob_for_our_side, outcome) = match trade_side {
                            Side::Up => (raw_prob as f64, if up_won { 1.0 } else { 0.0 }),
                            Side::Down => (1.0 - raw_prob as f64, if !up_won { 1.0 } else { 0.0 }),
                        };
                        s.calibration_bins.update(prob_for_our_side, outcome);
                        updated_count += 1;
                    }
                }
                
                if updated_count > 0 {
                    let (samples, blend) = s.calibration_bins.stats();
                    let direction = if up_won { "UP" } else { "DOWN" };
                    s.add_log(format!("[CAL] {} won | +{} samples | total: {} | blend: {:.0}%", 
                        direction, updated_count, samples, blend * 100.0));
                }
                
                // Update last resolved market
                if last_market_slug.is_some() {
                    // Use current market's interval minus 300 as proxy for old market
                    s.last_resolved_market = market.interval_start.saturating_sub(MARKET_WINDOW_SECS);
                }
            }
            
            // Store last market trade count for adaptive adjustment
            let prev_trades = s.up_trade_count + s.down_trade_count;
            s.last_market_trade_count = prev_trades;
            
            // Reset volatility tracking for new market (keep some history)
            if s.volatility_samples.len() > 30 {
                // Keep last 30 samples to bootstrap new market
                let keep_from = s.volatility_samples.len() - 30;
                s.volatility_samples = s.volatility_samples.split_off(keep_from);
            }
            // Decay session max so it can adapt to new market
            s.session_max_volatility *= 0.7;
            
            s.up_trade_count = 0;
            s.down_trade_count = 0;
            
            // Reset session tracking for new market
            s.session_trade_count = 0;
            s.session_start = Some(market.interval_start);
            
            // Fetch wallet balance and calculate max trades with 10% buffer
            drop(s);  // Release lock during network call
            match fetch_wallet_balance().await {
                Ok(balance) => {
                    let mut s = state.write().await;
                    s.wallet_balance = balance;
                    
                    // Calculate max trades per side with 10% buffer
                    let usable_balance = balance * dec!(0.90);  // 10% buffer
                    let cost_per_trade = s.position_size * dec!(0.50);  // Each trade costs position_size in USDC
                    let total_trades = (usable_balance / cost_per_trade).floor().to_u32().unwrap_or(0);
                    let max_per_side = total_trades / 2;  // Equal for both sides
                    s.max_trades = max_per_side * 2;  // Store total (will be divided by 2 when checking)
                    
                    s.add_log(format!("New session: {} | Balance: ${:.2} | Max trades: {}/side", 
                        market.slug, balance, max_per_side));
                }
                Err(e) => {
                    let mut s = state.write().await;
                    s.add_log(format!("New session: {} (balance fetch failed: {})", market.slug, e));
                }
            }
            s = state.write().await;
            last_market_slug = current_slug;
            // Force pre-signed refresh on new market
            last_pre_sign_refresh = Instant::now() - Duration::from_secs(100);
        }
        
        // Refresh pre-signed orders every 15 seconds or when consumed (fresher = better fills)
        if !s.dry_run && last_pre_sign_refresh.elapsed() > Duration::from_secs(15) {
            let up_token = market.up_token_id.clone();
            let down_token = market.down_token_id.clone();
            let size = s.position_size;
            drop(s);  // Release lock during network call
            refresh_pre_signed_orders(&up_token, &down_token, size).await;
            s = state.write().await;
            s.pre_signed_up_ready = true;
            s.pre_signed_down_ready = true;
            last_pre_sign_refresh = Instant::now();
        }
        
        // MANDATORY TIMEOUT EXIT - happens BEFORE orderbook check!
        // Positions held > 30s MUST exit regardless of orderbook state
        let max_hold_emergency: u64 = 30_000;
        let mut emergency_exits: Vec<Position> = Vec::new();
        let mut emergency_indices: Vec<usize> = Vec::new();
        
        // First, identify positions to exit (don't remove yet!)
        for (idx, pos) in s.positions.iter().enumerate() {
            if pos.entry_time.elapsed().as_millis() as u64 >= max_hold_emergency {
                emergency_exits.push(pos.clone());
                emergency_indices.push(idx);
            }
        }
        
        // Process emergency exits - only remove if exit succeeds
        let mut successful_exits: Vec<usize> = Vec::new();
        for (i, pos) in emergency_exits.into_iter().enumerate() {
            let idx = emergency_indices[i];
            let hold_time = pos.entry_time.elapsed().as_millis() as u64;
            let exit_price = match pos.side {
                Side::Up => s.orderbook.down_best_ask.unwrap_or(dec!(0.50)),
                Side::Down => s.orderbook.up_best_ask.unwrap_or(dec!(0.50)),
            };
            let (exit_token, exit_is_up) = match pos.side {
                Side::Up => (market.down_token_id.clone(), false),
                Side::Down => (market.up_token_id.clone(), true),
            };
            let pos_size = pos.size;
            let pos_side = pos.side.clone();
            let is_dry = s.dry_run;
            
            s.add_log(format!("[!!!] EMERGENCY EXIT {:?} @ ~{:.2}c | held {}ms > 30s limit", 
                pos_side, exit_price * dec!(100), hold_time));
            
            drop(s);  // Release lock before network call
            // CRITICAL: Use must_fill - emergency exits MUST complete to avoid orphans
            let exit_result = execute_order_must_fill(exit_is_up, &exit_token, pos_size, is_dry, Some(exit_price)).await.unwrap_or(None);
            s = state.write().await;
            
            // CRITICAL: Only count as success if we got a fill price back
            match exit_result {
                Some(actual_exit_price) => {
                    let pnl = dec!(1) - actual_exit_price - pos.entry_price;
                    
                    // Track shares
                    match pos_side {
                        Side::Up => { s.down_shares_bought += pos_size; s.up_exit_count += 1; },
                        Side::Down => { s.up_shares_bought += pos_size; s.down_exit_count += 1; },
                    }
                    
                    s.total_pnl += pnl * pos_size;
                    s.trade_count += 1;
                    if pnl > dec!(0) { s.win_count += 1; }
                    
                    let trade = TradeRecord {
                        time: Local::now(),
                        side: pos_side.clone(),
                        entry_price: pos.entry_price,
                        exit_price: actual_exit_price,
                        pnl,
                        hold_ms: hold_time,
                        ml_prediction_vol: pos.predicted_vol_usd,
                        size: pos_size,
                        entry_type: pos.entry_type(),
                        trigger_exchange: "EMERGENCY".to_string(),
                        z_score: pos.entry_z_score,
                    };
                    
                    log_trade_to_db(&trade, s.btc_price, &market.slug, s.dry_run,
                        pos.direction_prob_cal, pos.direction_prob_raw, pos.displacement_usd, pos.elapsed_pct, pos.liq_count);
                    s.trades.push_front(trade);
                    if s.trades.len() > 20 { s.trades.pop_back(); }
                    
                    successful_exits.push(idx);
                    s.add_log(format!("[OK] EMERGENCY EXIT SUCCESS {:?} @ {:.2}c", pos_side, actual_exit_price * dec!(100)));
                },
                None => {
                    // Exit failed - DON'T remove position, will retry
                    s.failed_exit_count += 1;
                    s.add_log(format!("[!!!] EMERGENCY EXIT FAILED {:?} - keeping position, will retry", pos_side));
                }
            }
        }
        
        // Remove only successfully exited positions (in reverse order to preserve indices)
        for idx in successful_exits.into_iter().rev() {
            if idx < s.positions.len() {
                s.positions.remove(idx);
            }
        }
        
        // Check orderbook freshness - only blocks NEW entries, not exits (handled above)
        if s.orderbook.last_update.map(|t| t.elapsed() > Duration::from_secs(5)).unwrap_or(true) {
            s.block_reason = BlockReason::OrderbookStale;
            continue;
        }
        
        // Orderbook sanity check: UP_ask + DOWN_bid should equal ~$1 (they're complements)
        // If not, the orderbook data is stale/inconsistent - clear and wait for fresh data
        if let (Some(up_ask), Some(down_bid)) = (s.orderbook.up_best_ask, s.orderbook.down_best_bid) {
            let sum = up_ask + down_bid;
            if sum < dec!(0.95) || sum > dec!(1.05) {
                s.add_log(format!("[!] Orderbook sanity fail: UP_ask({:.1}c) + DOWN_bid({:.1}c) = {:.2} != $1, clearing", 
                    up_ask * dec!(100), down_bid * dec!(100), sum));
                s.orderbook.up_best_ask = None;
                s.orderbook.up_best_bid = None;
                s.orderbook.down_best_ask = None;
                s.orderbook.down_best_bid = None;
                s.block_reason = BlockReason::OrderbookStale;
                continue;
            }
        }
        
        // Position management - Momentum-based exit framework
        // Exit conditions:
        // 1. |M| < eps_exit → momentum faded, no edge
        // 2. hold_time > k * τ → time-based exit scaled to remaining time
        // 3. dM/dt sign flip → momentum acceleration reversed
        let _timeout = s.hold_timeout_ms;
        let market_close_buffer: i64 = 30;  // Exit 30s before market close
        let mut positions_to_exit: Vec<usize> = Vec::new();
        
        // Exit parameters
        let eps_exit: f64 = 0.2;           // Minimum |Z| to maintain position (momentum threshold)
        let k_time_mult: f64 = 0.5;        // Exit if hold_time_secs > k * time_remaining
        
        // Get market timing info
        let now_ts = Utc::now().timestamp();
        let market_end = market.interval_start + MARKET_WINDOW_SECS;
        let seconds_to_close = market_end - now_ts;
        let time_remaining_secs = seconds_to_close.max(0) as f64;
        let now_ms = now_ts * 1000;
        let btc_price = s.btc_price.to_f64().unwrap_or(0.0);
        
        // Update intra-window state and compute Z-score
        s.intra_window_state.update(btc_price, now_ms);
        let z_score = s.intra_window_state.compute_z_score(btc_price, time_remaining_secs);
        s.z_score = z_score;  // Store for display
        
        // Compute current momentum (30s price change %)
        let current_momentum = s.intra_window_state.compute_features(btc_price, now_ms).current_momentum;
        
        // Track dM/dt (momentum derivative) - did momentum sign flip?
        let prev_m = s.prev_momentum;
        let dm_dt_sign_flip = prev_m.abs() > 0.001 && current_momentum.abs() > 0.001 &&
                              ((prev_m > 0.0 && current_momentum < 0.0) || 
                               (prev_m < 0.0 && current_momentum > 0.0));
        s.prev_momentum = current_momentum;  // Update for next tick
        
        // ML prediction (kept for display/logging, but not used for entry)
        let _ml_prediction = if let Some(ref predictor) = intra_predictor {
            let mut features = s.intra_window_state.compute_features(btc_price, now_ms);
            features.time_remaining = time_remaining_secs / MARKET_WINDOW_SECS_F64;
            features.time_elapsed = 1.0 - features.time_remaining;
            features.momentum_x_time = features.current_momentum * features.time_elapsed;
            let pred = predictor.predict(&features);
            s.intra_window_prediction = pred;
            Some(pred)
        } else {
            None
        };
        
        // Evaluate each position for exit
        let mut exit_logs: Vec<String> = Vec::new();
        for (idx, pos) in s.positions.iter().enumerate() {
            let hold_time = pos.entry_time.elapsed().as_millis() as u64;
            
            // Calculate current PnL for this position
            let current_exit_price = match pos.side {
                Side::Up => s.orderbook.down_best_ask,
                Side::Down => s.orderbook.up_best_ask,
            };
            let current_pnl_pct = current_exit_price.map(|exit_p| {
                (dec!(1) - exit_p - pos.entry_price) * dec!(100)
            });
            
            // ============================================================================
            // MOMENTUM-BASED EXIT FRAMEWORK
            // ============================================================================
            // Exit conditions:
            //   1. |M| < eps_exit → momentum faded, no edge left
            //   2. hold_time > k * τ → time-based exit scaled to time remaining
            //   3. dM/dt sign flip → momentum turning against us
            //   4. Market closing → must exit before resolution
            //
            // Note: M = Z-score (normalized displacement), τ = time remaining
            // ============================================================================
            
            let hold_time_secs = hold_time as f64 / 1000.0;
            let time_pct = 1.0 - (time_remaining_secs / MARKET_WINDOW_SECS_F64);
            
            // Check if momentum is WITH our position or AGAINST
            let momentum_with_us = match pos.side {
                Side::Up => z_score > 0.0,
                Side::Down => z_score < 0.0,
            };
            
            // MINIMUM HOLD TIME - don't exit early regardless of other signals
            let min_hold = s.min_hold_ms;
            let met_min_hold = hold_time >= min_hold;
            
            // Exit condition 1: |M| < eps_exit (momentum faded) - only after min hold
            let momentum_faded = met_min_hold && z_score.abs() < eps_exit;
            
            // Exit condition 2: hold_time > k * τ (time-based) - only after min hold
            // Only applies when we have reasonable time left (avoid div by small τ)
            let time_exit = met_min_hold && time_remaining_secs > 60.0 && hold_time_secs > k_time_mult * time_remaining_secs;
            
            // Exit condition 3: dM/dt sign flip AND momentum now against us - only after min hold
            // This catches reversals before they become catastrophic
            let momentum_reversal = met_min_hold && dm_dt_sign_flip && !momentum_with_us;
            
            // Simple hold timeout exit (MAXIMUM hold time - forced exit)
            let timeout = s.hold_timeout_ms;
            let timeout_exit = hold_time >= timeout;
            
            // Combine exit conditions
            let should_exit = 
                // 0. Market closing - MUST exit before resolution
                seconds_to_close <= market_close_buffer ||
                // NEW: Simple timeout exit (CRITICAL - this is what working bots use)
                timeout_exit ||
                // 1. Momentum faded - no edge
                (momentum_faded && !momentum_with_us) ||
                // 2. Time-based exit
                time_exit ||
                // 3. Momentum reversal detected
                momentum_reversal;
            
            if should_exit {
                let reason = if seconds_to_close <= market_close_buffer {
                    "MKT_CLOSE"
                } else if timeout_exit {
                    "TIMEOUT"
                } else if momentum_faded {
                    "M_FADED"
                } else if time_exit {
                    "T_SCALE"
                } else if momentum_reversal {
                    "dM/dt_FLIP"
                } else {
                    "UNKNOWN"
                };
                
                if !positions_to_exit.contains(&idx) {
                    let pnl_str = current_pnl_pct.map(|p| format!("{:+.1}%", p)).unwrap_or("--".to_string());
                    exit_logs.push(format!("[EXIT:{}] {:?} | Z={:.2} M={:.3}% | PnL={} | {:.1}s | t={:.0}%",
                        reason, pos.side, z_score, current_momentum, pnl_str, hold_time_secs, time_pct * 100.0));
                    positions_to_exit.push(idx);
                }
            }
        }
        
        // Log exits after iteration is done
        for log_msg in exit_logs {
            s.add_log(log_msg);
        }
        
        // Exit positions in reverse order (to preserve indices)
        for &idx in positions_to_exit.iter().rev() {
            // CRITICAL: Re-check index is still valid after potential modifications
            if idx >= s.positions.len() {
                let pos_len = s.positions.len();
                s.add_log(format!("[!] RACE: idx {} >= positions.len() {}, skipping", idx, pos_len));
                continue;
            }
            
            let pos = s.positions[idx].clone();
            let hold_time = pos.entry_time.elapsed().as_millis() as u64;
            
            let exit_price = match pos.side {
                Side::Up => s.orderbook.down_best_ask,
                Side::Down => s.orderbook.up_best_ask,
            };
            
            if let Some(exit_p) = exit_price {
                // Exit by buying opposite token
                let (exit_token, exit_is_up) = match pos.side {
                    Side::Up => (market.down_token_id.clone(), false),
                    Side::Down => (market.up_token_id.clone(), true),
                };
                
                let pos_size = pos.size;
                let pos_side = pos.side.clone();
                let is_dry = s.dry_run;
                let up_t_exit = market.up_token_id.clone();
                let down_t_exit = market.down_token_id.clone();
                
                // Log BEFORE attempting exit
                let pos_len = s.positions.len();
                s.add_log(format!("[>>] Attempting EXIT {:?} @ ~{:.2}c | idx={} | positions={}", 
                    pos_side, exit_p * dec!(100), idx, pos_len));
                
                drop(s);  // Release lock before network call!
                // CRITICAL: Use must_fill - exits MUST complete to avoid orphans
                let exit_result = execute_order_must_fill(exit_is_up, &exit_token, pos_size, is_dry, Some(exit_p)).await.unwrap_or(None);
                
                // CRITICAL: Background refresh pre-signed orders after exit too
                if !is_dry {
                    let up_t2 = up_t_exit.clone();
                    let down_t2 = down_t_exit.clone();
                    tokio::spawn(async move {
                        refresh_pre_signed_orders(&up_t2, &down_t2, pos_size).await;
                    });
                }
                
                s = state.write().await;  // Re-acquire lock
                
                // Use actual fill price if available, otherwise use orderbook estimate
                let actual_exit_price = match exit_result {
                    Some(fill_price) => fill_price,
                    None => {
                        s.failed_exit_count += 1;
                        let failed_count = s.failed_exit_count;
                        s.add_log(format!("[!!!] EXIT FAILED {:?} @ ~{:.2}c | Failed exits: {} | Will retry", 
                            pos_side, exit_p * dec!(100), failed_count));
                        // Don't remove position if exit failed - will retry next tick
                        continue;
                    }
                };
                
                // Calculate PnL using ACTUAL fill prices
                // Exit cost = actual_exit_price (what we paid for opposite token)
                // Profit = (1 - exit_cost) - entry_cost = 1 - actual_exit_price - entry_price
                let pnl = dec!(1) - actual_exit_price - pos.entry_price;
                
                // Track shares bought for exit (opposite side)
                match pos_side {
                    Side::Up => {
                        s.down_shares_bought += pos_size;  // Exit UP = buy DOWN
                        s.up_exit_count += 1;
                    },
                    Side::Down => {
                        s.up_shares_bought += pos_size;    // Exit DOWN = buy UP
                        s.down_exit_count += 1;
                    },
                }
                
                s.total_pnl += pnl * pos_size;
                s.trade_count += 1;
                if pnl > dec!(0) { s.win_count += 1; }
                
                let trade = TradeRecord {
                    time: Local::now(),
                    side: pos_side.clone(),
                    entry_price: pos.entry_price,
                    exit_price: actual_exit_price,
                    pnl,
                    hold_ms: hold_time,
                    ml_prediction_vol: pos.predicted_vol_usd,
                    size: pos.size,
                    entry_type: pos.entry_type(),
                    trigger_exchange: pos.trigger_exchange.clone(),
                    z_score: pos.entry_z_score,  // Z-score at entry time
                };
                
                // Log with share totals for debugging - show ACTUAL fill price and trigger exchange
                let up_shares = s.up_shares_bought;
                let down_shares = s.down_shares_bought;
                s.add_log(format!("[X] EXIT {:?} @ {:.2}c | {} | PnL: {:.2}% | UP:{} DOWN:{}", 
                    trade.side, actual_exit_price * dec!(100), trade.trigger_exchange, pnl * dec!(100), 
                    up_shares, down_shares));
                
                // Log trade to database with all ML features
                log_trade_to_db(&trade, s.btc_price, &market.slug, s.dry_run,
                    pos.direction_prob_cal, pos.direction_prob_raw, pos.displacement_usd, pos.elapsed_pct, pos.liq_count);
                
                s.trades.push_front(trade);
                if s.trades.len() > 20 { s.trades.pop_back(); }
                
                // CRITICAL: Find and remove by matching position, not by index
                // Index may have changed after lock was released
                let remove_idx = s.positions.iter().position(|p| {
                    p.side == pos_side && 
                    p.entry_price == pos.entry_price && 
                    p.entry_time == pos.entry_time
                });
                
                if let Some(ri) = remove_idx {
                    s.positions.remove(ri);
                } else {
                    s.add_log(format!("[!!!] CRITICAL: Could not find position to remove! {:?}", pos_side));
                }
            }
        }
        
        // ============================================================================
        // ENTRY LOGIC - Z-SCORE BASED CONDITIONAL PROBABILITY FRAMEWORK
        // ============================================================================
        // Mathematical basis: P(UP wins) = Φ(Z) where Z = (S - S0) / (σ√τ)
        // 
        // Key insight: Once displacement is established, probability drift is MONOTONIC
        // Signal flips are noise - trust the math, not the fluctuations
        //
        // Entry windows:
        //   EARLY (first 20% = 3 min): Enter when |Z| > 0.5 (displacement establishing)
        //   LATE  (last 30% = 4.5 min): Enter when |Z| > 0.8 (ride probability collapse)
        //   MIDDLE: More conservative, need |Z| > 1.0
        // ============================================================================
        
        let mut trigger_exchange: String = "Z-Score".to_string();
        
        // Clear any stale early signal state
        if s.early_signal_ready {
            s.early_signal = None;
            s.early_signal_ready = false;
            s.early_signal_confirmations = 0;
        }
        
        // Get timing info
        let elapsed_pct = 1.0 - (time_remaining_secs / MARKET_WINDOW_SECS_F64);  // 0.0 = start, 1.0 = end
        let in_early_window = elapsed_pct < 0.20;   // First 1 minute
        let in_late_window = elapsed_pct > 0.70;    // Last 1.5 minutes
        let _in_middle_window = !in_early_window && !in_late_window;
        
        // Z-score entry threshold - use configured MIN_Z_SCORE
        // ALWAYS require tick confirmation to avoid entering against momentum
        // This prevents entries where Z says DOWN but ticks are UP (recovery)
        let z_entry_threshold = s.min_z_score;  // Configurable via MIN_Z_SCORE env var
        
        // Update consecutive tracking for display (based on recent ticks)
        let ticks: Vec<i8> = s.tick_directions.iter().cloned().collect();
        if ticks.len() >= 5 {
            let recent = &ticks[ticks.len() - 5..];
            s.consecutive_up = recent.iter().filter(|&&d| d > 0).count();
            s.consecutive_down = recent.iter().filter(|&&d| d < 0).count();
        }
        
        // Tick momentum confirmation - REQUIRED for all entries
        // Must have at least 3 consecutive ticks in the same direction as Z-score
        let tick_confirms_up = s.consecutive_up >= 3;
        let tick_confirms_down = s.consecutive_down >= 3;
        
        // Z-SCORE ENTRY SIGNAL
        // Z > 0 means BTC above reference → bet UP (but only if ticks confirm!)
        // Z < 0 means BTC below reference → bet DOWN (but only if ticks confirm!)
        let current_z = z_score;  // Already computed above
        
        // CRITICAL: Both Z-score AND tick momentum must agree
        // This prevents entering DOWN when Z is negative but ticks are UP (potential recovery)
        let (has_z_signal, z_side) = if current_z >= z_entry_threshold && tick_confirms_up {
            // Z says UP AND ticks confirm UP → strong UP signal
            (true, Some(Side::Up))
        } else if current_z <= -z_entry_threshold && tick_confirms_down {
            // Z says DOWN AND ticks confirm DOWN → strong DOWN signal
            (true, Some(Side::Down))
        } else {
            // Z and ticks disagree, or no clear signal → NO TRADE
            (false, None)
        };
        
        // Log Z-score entry attempts periodically
        if s.binance_msg_count % 500 == 0 && current_z.abs() > 0.3 {
            let window = if in_early_window { "EARLY" } else if in_late_window { "LATE" } else { "MID" };
            let tick_dir = if tick_confirms_up { "UP" } else if tick_confirms_down { "DOWN" } else { "MIXED" };
            s.add_log(format!("[Z] {} | Z={:.2} | ticks={} | signal={}", 
                window, current_z, tick_dir, has_z_signal));
        }
        
        // REVERSAL LOGIC DISABLED
        let is_reversal_entry = false;
        let reversal_side: Option<Side> = None;
        
        // Clear old reversal tracking state
        if let Some(move_time) = s.last_big_move_time {
            if move_time.elapsed().as_millis() as u64 > 20_000 {
                s.last_big_move_time = None;
                s.last_big_move_side = None;
                s.last_big_move_pm_price = None;
                s.reversal_traded = false;
            }
        }
        
        if !has_z_signal && !is_reversal_entry {
            s.block_reason = BlockReason::NoMomentum;
            continue;
        }
        
        // We have a Z-score entry signal!
        s.momentum_signals += 1;
        
        // Use reversal side if this is a reversal entry, otherwise use Z-score direction
        let side = if is_reversal_entry {
            trigger_exchange = "Reversal".to_string();
            reversal_side.unwrap()
        } else {
            z_side.unwrap()
        };
        
        // Track for potential future reversal (only for momentum signals, not reversals)
        if has_z_signal && !is_reversal_entry {
            let pm_price = match side {
                Side::Up => s.orderbook.up_best_ask.or(s.orderbook.up_best_bid),
                Side::Down => s.orderbook.down_best_ask.or(s.orderbook.down_best_bid),
            };
            s.last_big_move_time = Some(Instant::now());
            s.last_big_move_side = Some(side);
            s.last_big_move_pm_price = pm_price;
            s.reversal_traded = false;
        }
        
        // Determine entry type for this potential trade
        let pending_entry_type = if is_reversal_entry {
            EntryType::Reversal
        } else {
            // SafeEntry will be determined later after more checks, default to Momentum for now
            EntryType::Momentum
        };
        
        // Check max stacking limit: allow up to 5 positions per entry type
        // (5 Momentum + 5 SafeEntry + 5 Reversal can be open simultaneously = max 15)
        let max_per_type = 5usize;
        let mom_count = s.positions.iter().filter(|p| p.entry_type() == EntryType::Momentum).count();
        let rev_count = s.positions.iter().filter(|p| p.entry_type() == EntryType::Reversal).count();
        
        if !is_reversal_entry {
            // For momentum entries, check momentum limit
            if mom_count >= max_per_type {
                s.block_reason = BlockReason::MaxPositions;
                continue;
            }
        }
        if is_reversal_entry && rev_count >= max_per_type {
            s.block_reason = BlockReason::MaxPositions;
            continue;
        }
        
        // PRICE CONFIRMATION FILTER: Don't stack if price moved against us
        // If we have existing positions in this direction, only add more if price is still favorable
        let same_side_positions: Vec<_> = s.positions.iter()
            .filter(|p| p.side == side)
            .collect();
        
        if !same_side_positions.is_empty() {
            // Get current price for this side
            let current_price = match side {
                Side::Up => s.orderbook.up_best_ask.unwrap_or(dec!(0.5)),
                Side::Down => s.orderbook.down_best_ask.unwrap_or(dec!(0.5)),
            };
            
            // Get best (lowest) entry price among existing positions
            let best_entry = same_side_positions.iter()
                .map(|p| p.entry_price)
                .min()
                .unwrap_or(current_price);
            
            // If current price is worse (higher) than our best entry by >5c, don't stack
            // This means the market moved against us
            if current_price > best_entry + dec!(0.05) {
                s.block_reason = BlockReason::PriceOutOfRange;
                if s.binance_msg_count % 200 == 0 {
                    s.add_log(format!("[X] {:?} stack blocked: price {:.0}c > entry {:.0}c + 5c", 
                        side, current_price * dec!(100), best_entry * dec!(100)));
                }
                continue;
            }
        }
        
        // Check 7.5s cooldown after 3rd position
        if let Some(last_third) = s.last_third_position_time {
            if last_third.elapsed().as_millis() < 7500 {
                continue;  // Still in cooldown
            } else {
                s.last_third_position_time = None;  // Clear cooldown
            }
        }
        
        // DIRECTION COOLDOWN: Prevent whipsawing between UP/DOWN
        // If we recently entered one direction, block opposite direction for X seconds
        if let (Some(last_side), Some(last_time)) = (s.last_entry_side, s.last_entry_time) {
            if last_side != side {
                let elapsed_ms = last_time.elapsed().as_millis() as u64;
                if elapsed_ms < s.direction_cooldown_ms {
                    let remaining = (s.direction_cooldown_ms - elapsed_ms) / 1000;
                    s.block_reason = BlockReason::DirectionCooldown;
                    if s.binance_msg_count % 500 == 0 {
                        s.add_log(format!("[X] {:?} blocked: direction cooldown ({}s left after {:?})", 
                            side, remaining, last_side));
                    }
                    continue;
                }
            }
        }
        
        // Check max trades per side limit
        let max_per_side = s.max_trades / 2; // Split evenly between sides
        let side_count = match side {
            Side::Up => s.up_trade_count,
            Side::Down => s.down_trade_count,
        };
        if max_per_side > 0 && side_count >= max_per_side {
            s.block_reason = BlockReason::MaxTradesReached;
            s.add_log(format!("[X] {:?} blocked: max {} trades reached for this side", side, max_per_side));
            continue;
        }
        
        // For entry, we can either:
        // 1. Buy the token directly (hit the ask)
        // 2. Sell the opposite token (hit the opposite bid) - this is equivalent
        // Use whichever gives a better price
        let (direct_ask, opposite_bid) = match side {
            Side::Up => (s.orderbook.up_best_ask, s.orderbook.down_best_bid),
            Side::Down => (s.orderbook.down_best_ask, s.orderbook.up_best_bid),
        };
        
        // If we have opposite bid, implied price = 1 - opposite_bid
        let implied_price = opposite_bid.map(|b| dec!(1) - b);
        
        // Pick the better (lower) price
        let entry_price = match (direct_ask, implied_price) {
            (Some(ask), Some(imp)) => Some(ask.min(imp)),
            (Some(ask), None) => Some(ask),
            (None, Some(imp)) => Some(imp),
            (None, None) => None,
        };
        
        let entry_price = match entry_price {
            Some(p) => p,
            None => {
                s.block_reason = BlockReason::PriceOutOfRange;
                s.add_log(format!("[X] {:?} signal blocked: no price available", side));
                continue;
            }
        };
        
        // PRICE RANGE FILTER: Only trade between 5c and 95c
        // Avoid extreme prices where market has already decided
        if entry_price < dec!(0.05) || entry_price > dec!(0.95) {
            s.block_reason = BlockReason::PriceOutOfRange;
            s.add_log(format!("[X] {:?} blocked: price {:.0}c outside 5-95c range", side, entry_price * dec!(100)));
            continue;
        }
        
        // MINIMUM PRICE MOVEMENT FILTER: Only trade when BTC is moving
        // Calculate price range in last 30 seconds
        let min_movement = s.min_price_movement;
        let msg_count = s.binance_msg_count;
        let (min_price, max_price) = s.btc_price_history.iter()
            .fold((Decimal::MAX, Decimal::MIN), |(min, max), (_, p)| {
                (min.min(*p), max.max(*p))
            });
        let price_range = if max_price > min_price { 
            (max_price - min_price).to_f32().unwrap_or(0.0) 
        } else { 
            0.0 
        };
        
        // SAFE ENTRY: When PM price is strongly one-sided (>=70c), enter that side if:
        //   - BTC is flat (not fighting the direction), OR
        //   - BTC is moving WITH that direction (Z confirms)
        // The PM price will drift toward 100c as resolution approaches.
        let time_left_secs = s.time_left_in_market().unwrap_or(MARKET_WINDOW_SECS) as f64;
        let in_late_window = time_left_secs < 60.0;  // Last 1 minute
        let btc_is_flat = price_range < min_movement;
        let current_z = s.z_score;
        
        // Check if PM price strongly favors one side (>= 70c)
        let (up_price, down_price) = (
            s.orderbook.up_best_ask.or(s.orderbook.up_best_bid).unwrap_or(dec!(0.5)),
            s.orderbook.down_best_ask.or(s.orderbook.down_best_bid).unwrap_or(dec!(0.5)),
        );
        
        // Safe entry: DISABLED - analysis shows Momentum outperforms SafeEntry
        // SafeEntry had 51.4% win rate vs Momentum 52.6% and 2x lower avg PnL
        let is_safe_entry = false;
        
        // BTC flat filter DISABLED - Z-score alone is sufficient for entry decisions
        // Previously blocked entries when BTC wasn't moving, but this was too conservative
        let _ = btc_is_flat;  // Suppress unused warning
        
        // Volatility filter - BYPASS for safe entries (PM price will drift to 100c)
        let ml_pred = s.ml_prediction_vol;
        let min_vol = s.min_volatility_usd;
        if ml_pred < min_vol && !is_safe_entry {
            s.ml_filtered_count += 1;
            s.block_reason = BlockReason::MLFiltered;
            s.add_log(format!("[X] Vol filtered {:?} Z={:.2} (vol: ${:.2} < ${:.2})", 
                side, current_z, ml_pred, min_vol));
            continue;
        }
        
        // Log safe entries
        if is_safe_entry {
            // Check if we already have max SafeEntry positions
            let safe_count = s.positions.iter().filter(|p| p.entry_type() == EntryType::SafeEntry).count();
            if safe_count >= max_per_type {
                s.block_reason = BlockReason::MaxPositions;
                continue;
            }
            let pm_price = match side { Side::Up => up_price, Side::Down => down_price };
            let reason = if btc_is_flat { "flat" } else { "momentum" };
            s.add_log(format!("[SAFE] {} {:?} @ PM {:.0}c | Z={:.2} | {:.0}s left", 
                reason, side, pm_price * dec!(100), current_z, time_left_secs));
        }
        
        s.block_reason = BlockReason::None;
        
        let token_id = match side {
            Side::Up => market.up_token_id.clone(),
            Side::Down => market.down_token_id.clone(),
        };
        
        // ====================================================================
        // DYNAMIC POSITION SIZING based on trade analysis (500 trades):
        // 
        // KEY FINDINGS:
        // - Entry 30-50c: 78.9% win rate (123 trades)
        // - |Z| 0.5-1.5: ~74% win rate (sweet spot)
        // - |Z| > 2.0: only 49.3% win rate (coin flip!) → BLOCK
        // - Vol $5-7: 66.2% win rate (baseline)
        // - Vol $7-10: 85.7% win rate (42 trades)
        //
        // SIZING TIERS:
        // - HIGH CONFIDENCE (2x): Entry 30-50c AND |Z| 0.5-1.5 AND Vol >= 5
        // - MEDIUM CONFIDENCE (1.5x): Entry 30-50c OR (|Z| 0.5-1.5 AND Vol >= 5)
        // - LOW CONFIDENCE (1x): Everything else that passes filters
        // - BLOCK: |Z| > 1.5 (only 37% win rate from analysis)
        // ====================================================================
        let base_size = s.position_size;
        let abs_z = current_z.abs();
        
        // CRITICAL: Block entries where |Z| > 1.5 (only 37% win rate from analysis)
        if abs_z > 1.5 {
            s.block_reason = BlockReason::MLFiltered;
            if s.binance_msg_count % 500 == 0 {
                s.add_log(format!("[X] Z-block {:?} |Z|={:.2} > 1.5 (37% WR)", side, abs_z));
            }
            continue;
        }
        
        // Confidence signals
        let entry_is_sweet_spot = entry_price >= dec!(0.30) && entry_price <= dec!(0.50);  // 78.9% WR
        let z_is_optimal = abs_z >= 0.5 && abs_z <= 1.5;  // ~74% WR
        let vol_is_good = ml_pred >= 5.0;  // Baseline filter
        
        let pos_size = if entry_is_sweet_spot && z_is_optimal && vol_is_good {
            // HIGH CONFIDENCE: All 3 signals align → 2x
            base_size * dec!(2)
        } else if entry_is_sweet_spot || (z_is_optimal && vol_is_good) {
            // MEDIUM CONFIDENCE: Entry sweet spot OR (Z optimal + vol good) → 1.5x
            base_size * dec!(1.5)
        } else {
            // LOW CONFIDENCE: Passed filters but not optimal → 1x
            base_size
        };
        
        let is_dry = s.dry_run;
        let is_up = matches!(side, Side::Up);
        let expected_price = entry_price;  // Save expected price for logging
        let was_safe_entry = is_safe_entry;  // Save for Position creation
        let was_reversal_entry = is_reversal_entry;  // Save for Position creation
        let saved_trigger_exchange = trigger_exchange.clone();  // Save trigger exchange for Position
        let saved_direction_prob_raw = s.direction_prob_raw;  // Save for online calibration
        let saved_market_interval = market.interval_start;  // Save for outcome tracking
        // Save tokens for background pre-sign refresh
        let up_token_refresh = market.up_token_id.clone();
        let down_token_refresh = market.down_token_id.clone();
        
        // Mark reversal as traded before dropping lock
        if is_reversal_entry {
            s.reversal_traded = true;
        }
        
        drop(s);  // Release lock before network call!
        
        // CRITICAL: Check dry_run BEFORE making any order calls!
        let order_result = if is_dry {
            // Dry run - simulate fill at expected price, NO real order
            Some(expected_price)
        } else {
            // Live mode - use retry logic for entries, pass expected_price for fill tracking
            execute_order_with_retry(is_up, &token_id, pos_size, 1, Some(expected_price)).await.unwrap_or(None)
        };
        
        // CRITICAL LATENCY OPTIMIZATION: Immediately trigger background pre-sign refresh
        // This ensures next trade has a fresh pre-signed order ready
        if !is_dry {
            let up_t = up_token_refresh.clone();
            let down_t = down_token_refresh.clone();
            tokio::spawn(async move {
                refresh_pre_signed_orders(&up_t, &down_t, pos_size).await;
            });
        }
        
        s = state.write().await;  // Re-acquire lock
        
        // Use actual fill price if available
        if let Some(actual_entry_price) = order_result {
            // Track shares bought for entry
            match side {
                Side::Up => {
                    s.up_shares_bought += pos_size;
                    s.up_entry_count += 1;
                    s.up_trade_count += 1;
                },
                Side::Down => {
                    s.down_shares_bought += pos_size;
                    s.down_entry_count += 1;
                    s.down_trade_count += 1;
                },
            }
            
            // Increment session trade count for adaptive threshold
            s.session_trade_count += 1;
            
            // Reset intra-window state for new position to track from entry
            s.intra_window_state = IntraWindowState::new();
            s.intra_window_state.entry_price = Some(actual_entry_price.to_f64().unwrap_or(0.0));
            s.intra_window_prediction = 0.5;  // Reset prediction
            
            // (Volatility-range-tracking adaptive logic is in Binance tick handler)
            
            let new_position_count = s.positions.len() + 1;
            let pos_size_dec = pos_size;  // Use dynamic position size calculated earlier
            // Log entry with trigger exchange
            let up_shares = s.up_shares_bought;
            let down_shares = s.down_shares_bought;
            let size_mult = if pos_size > base_size * dec!(1.5) { " [2x]" } else if pos_size > base_size { " [1.5x]" } else { "" };
            let entry_type = if was_reversal_entry { " [REV]" } else if was_safe_entry { " [SAFE 30s]" } else { "" };
            let entry_z = s.z_score;  // Capture z_score at entry
            s.add_log(format!(">>> ENTER {:?} @ {:.2}c | {} | ML: ${:.2} | Z={:.2} | UP:{} DOWN:{}{}{}", 
                side, actual_entry_price * dec!(100), saved_trigger_exchange, ml_pred, entry_z, up_shares, down_shares, entry_type, size_mult));
            
            // Get direction features for database logging
            let (saved_dir_cal, saved_disp, saved_elapsed, saved_liq) = {
                let dir_prob_calibrated = s.direction_prob_up;
                let displacement = s.direction_features.as_ref().map(|f| f.displacement_usd).unwrap_or(0.0);
                let elapsed = s.direction_features.as_ref().map(|f| f.elapsed_pct).unwrap_or(0.0);
                let liq = s.direction_features.as_ref().map(|f| f.liq_count_60s).unwrap_or(0.0);
                (dir_prob_calibrated, displacement, elapsed, liq)
            };
            
            s.positions.push(Position {
                side,
                entry_price: actual_entry_price,  // Use ACTUAL fill price
                entry_time: Instant::now(),
                size: pos_size_dec,
                predicted_vol_usd: ml_pred,
                is_safe_entry: was_safe_entry,
                is_reversal: was_reversal_entry,
                trigger_exchange: saved_trigger_exchange,
                direction_prob_raw: saved_direction_prob_raw,  // For online calibration
                direction_prob_cal: saved_dir_cal,             // Calibrated for DB logging
                market_interval_start: saved_market_interval,  // Track which market
                entry_z_score: entry_z,  // Z-score at entry for analysis
                displacement_usd: saved_disp,
                elapsed_pct: saved_elapsed,
                liq_count: saved_liq,
            });
            
            // Track pending calibration sample (include side for correct outcome mapping)
            s.pending_calibration.push((saved_direction_prob_raw, saved_market_interval, side));
            
            // Update direction cooldown tracking
            s.last_entry_side = Some(side);
            s.last_entry_time = Some(Instant::now());
            
            // Trigger cooldown when hitting 3rd position
            if s.positions.len() == 3 {
                s.last_third_position_time = Some(Instant::now());
                s.add_log("[!] 3rd position - 7.5s cooldown started".to_string());
            }
            s.tick_directions.clear();
        } else {
            s.add_log(format!("[!!!] ENTRY FAILED {:?} @ ~{:.2}c", side, expected_price * dec!(100)));
        }
    }
}

// ============================================================================
// TUI RENDERING
// ============================================================================

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, secs)
}

fn draw_ui(f: &mut Frame, state: &SharedState) {
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),   // Header line 1
            Constraint::Length(1),   // Header line 2 (session stats)
            Constraint::Length(11),  // Middle section (BTC + Position + Trade History)
            Constraint::Length(16),  // Bottom section (Orderbook + Connections + PnL)
            Constraint::Min(6),      // Logs
            Constraint::Length(1),   // Footer
        ])
        .split(f.area());

    // ========== HEADER LINE 1 ==========
    let time_left = state.time_left_in_market().map(|s| format!("{}s", s)).unwrap_or_else(|| "--".to_string());
    let market_slug = state.market.as_ref().map(|m| m.slug.clone()).unwrap_or_else(|| "None".to_string());
    
    let header1 = Paragraph::new(Line::from(vec![
        Span::styled(" Hope1h ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw("| "),
        Span::raw("Strategy ML "),
        Span::raw("| "),
        Span::raw("BTC: "),
        Span::styled(format!("${:.2}", state.btc_price), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        Span::raw(" | "),
        Span::styled(if state.dry_run { "DRY RUN" } else { "LIVE" }, 
            Style::default().fg(if state.dry_run { Color::Yellow } else { Color::Red }).add_modifier(Modifier::BOLD)),
    ]));
    f.render_widget(header1, main_chunks[0]);

    // ========== HEADER LINE 2 (Session Stats) ==========
    let win_rate = if state.trade_count > 0 { (state.win_count as f32 / state.trade_count as f32) * 100.0 } else { 0.0 };
    let max_per_side = state.max_trades / 2;
    let header2 = Paragraph::new(Line::from(vec![
        Span::styled(format!("Session: {} ", format_duration(state.uptime())), Style::default().fg(Color::White)),
        Span::raw("| "),
        Span::raw("Balance: "),
        Span::styled(format!("${:.2} ", state.wallet_balance), Style::default().fg(Color::Cyan)),
        Span::raw("| "),
        Span::raw("Trades: "),
        Span::styled(format!("{} ", state.trade_count), Style::default().fg(Color::White)),
        Span::styled(format!("(^{}/v{}/{}) ", state.up_trade_count, state.down_trade_count, max_per_side), Style::default().fg(Color::DarkGray)),
        Span::raw("| "),
        Span::raw("Win Rate: "),
        Span::styled(format!("{:.1}%", win_rate), Style::default().fg(if win_rate >= 50.0 { Color::Green } else { Color::Red })),
        Span::raw(" | "),
        Span::styled("CPU:", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{:.0}%", state.system_health.cpu_percent), 
            Style::default().fg(if state.system_health.cpu_percent < 80.0 { Color::Green } else { Color::Red })),
        Span::raw(" "),
        Span::styled("Mem:", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{:.0}%", state.system_health.mem_percent()), 
            Style::default().fg(if state.system_health.mem_percent() < 80.0 { Color::Green } else { Color::Yellow })),
        Span::raw(" "),
        Span::styled("Disk:", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{:.0}%", state.system_health.disk_percent()), 
            Style::default().fg(if state.system_health.disk_percent() < 90.0 { Color::Green } else { Color::Red })),
    ]));
    f.render_widget(header2, main_chunks[1]);

    // ========== MIDDLE SECTION (BTC Price + Position + Trade History) ==========
    let mid_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(25),  // BTC Price
            Constraint::Length(32),  // Position
            Constraint::Min(30),     // Trade History
        ])
        .split(main_chunks[2]);

    // ----- BTC Price Panel (Yellow border) -----
    let ticks_display: String = state.tick_directions.iter().map(|d| if *d > 0 { '+' } else { '-' }).collect();
    let last_5: Vec<i8> = state.tick_directions.iter().rev().take(5).cloned().collect();
    let net: i32 = last_5.iter().map(|&d| d as i32).sum();
    let signal_text = if net >= state.tick_threshold as i32 { "UP" } 
                      else if net <= -(state.tick_threshold as i32) { "DOWN" } 
                      else { "NEUTRAL" };
    let signal_color = if signal_text == "UP" { Color::Green } else if signal_text == "DOWN" { Color::Red } else { Color::White };
    
    // Colorful tick display
    let tick_colored: Vec<Span> = state.tick_directions.iter().rev().take(12).collect::<Vec<_>>().into_iter().rev().map(|d| {
        if *d > 0 { Span::styled("+", Style::default().fg(Color::Green)) } 
        else { Span::styled("-", Style::default().fg(Color::Red)) }
    }).collect();
    
    let btc_text = vec![
        Line::from(vec![
            Span::styled(format!("${:.2}", state.btc_price), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(""),
        Line::from(std::iter::once(Span::styled("Tick", Style::default().fg(Color::Cyan))).chain(std::iter::once(Span::styled(format!("{:>3} ", state.tick_directions.len()), Style::default().fg(Color::Yellow)))).chain(tick_colored).collect::<Vec<_>>()),
        Line::from(vec![
            Span::raw("Signal: [ "),
            Span::styled(format!("{:^7}", signal_text), Style::default().fg(signal_color).add_modifier(Modifier::BOLD)),
            Span::raw(" ]"),
        ]),
        Line::from(vec![
            Span::raw("Count: "),
            Span::styled(format!("{}", state.tick_directions.len()), Style::default().fg(Color::Cyan)),
            Span::raw("        "),
            Span::styled(format!("{:^7}", signal_text), Style::default().fg(signal_color)),
        ]),
    ];
    let btc_block = Paragraph::new(btc_text)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Yellow)).title(Span::styled(" BTC Price ", Style::default().fg(Color::Yellow))));
    f.render_widget(btc_block, mid_chunks[0]);

    // ----- Position Panel (Cyan border) -----
    let ob = &state.orderbook;
    let up_entry = ob.up_best_ask.map(|a| a.min(ob.down_best_bid.map(|b| dec!(1) - b).unwrap_or(a)));
    let down_entry = ob.down_best_ask.map(|a| a.min(ob.up_best_bid.map(|b| dec!(1) - b).unwrap_or(a)));
    
    let pos_lines = if !state.positions.is_empty() {
        let num_positions = state.positions.len();
        let up_count = state.positions.iter().filter(|p| p.side == Side::Up).count();
        let down_count = state.positions.iter().filter(|p| p.side == Side::Down).count();
        let oldest = state.positions.iter().map(|p| p.entry_time.elapsed().as_millis() as u64).max().unwrap_or(0);
        let total_size: Decimal = state.positions.iter().map(|p| p.size).sum();
        
        // Count by entry type
        let mom_count = state.positions.iter().filter(|p| p.entry_type() == EntryType::Momentum).count();
        let safe_count = state.positions.iter().filter(|p| p.entry_type() == EntryType::SafeEntry).count();
        let rev_count = state.positions.iter().filter(|p| p.entry_type() == EntryType::Reversal).count();
        
        vec![
            Line::from(vec![
                Span::styled(format!("{} Open ", num_positions), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::styled(format!("(^{}/v{})", up_count, down_count), Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled(format!("M:{} ", mom_count), Style::default().fg(Color::White)),
                Span::styled(format!("S:{} ", safe_count), Style::default().fg(Color::Cyan)),
                Span::styled(format!("R:{}", rev_count), Style::default().fg(Color::Magenta)),
            ]),
            Line::from(vec![Span::raw(format!("Oldest: {}ms", oldest))]),
            Line::from(vec![Span::raw(format!("Total Size: {} shares", total_size))]),
            Line::from(vec![Span::raw(format!("Timeout: {}ms", state.hold_timeout_ms))]),
        ]
    } else {
        vec![
            Line::from(Span::raw("No Positions")),
            Line::from(""),
            Line::from(vec![
                Span::raw("Entry Range: "),
                Span::styled(format!("{:.0}c", up_entry.unwrap_or(dec!(0)) * dec!(100)), Style::default().fg(Color::Green)),
                Span::raw(" - "),
                Span::styled(format!("{:.0}c", down_entry.unwrap_or(dec!(0)) * dec!(100)), Style::default().fg(Color::Red)),
            ]),
            Line::from(vec![Span::raw(format!("Hold Time: {}ms", state.hold_timeout_ms))]),
            Line::from(vec![Span::raw(format!("Size: {} shares", state.position_size))]),
            Line::from(vec![Span::raw(format!("Max Trades: up {}/dn {} of {}", state.up_trade_count, state.down_trade_count, state.max_trades / 2))]),
        ]
    };
    let pos_block = Paragraph::new(pos_lines)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Magenta)).title(Span::styled(" Positions ", Style::default().fg(Color::Magenta))));
    f.render_widget(pos_block, mid_chunks[1]);

    // ----- Trade History Panel (Green border) -----
    let trade_rows: Vec<Row> = state.trades.iter().take(8).map(|t| {
        let pnl_style = if t.pnl > dec!(0) { Style::default().fg(Color::Green) } else { Style::default().fg(Color::Red) };
        let side_style = if t.side == Side::Up { Style::default().fg(Color::Green) } else { Style::default().fg(Color::Red) };
        Row::new(vec![
            Cell::from(t.time.format("%H:%M").to_string()),
            Cell::from(format!("{:?}", t.side)).style(side_style),
            Cell::from(format!("{:.0}c", t.entry_price * dec!(100))),
            Cell::from(format!("{:.0}c", t.exit_price * dec!(100))),
            Cell::from(format!("${:.2}", t.pnl * t.size)).style(pnl_style),
        ])
    }).collect();

    let trades_table = Table::new(trade_rows, [
        Constraint::Length(6),
        Constraint::Length(5),
        Constraint::Length(6),
        Constraint::Length(6),
        Constraint::Length(7),
    ])
    .header(Row::new(vec!["Time", "Side", "Entry", "Exit", "PnL"]).style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan)))
    .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Cyan)).title(Span::styled(" Trade History ", Style::default().fg(Color::Cyan))));
    f.render_widget(trades_table, mid_chunks[2]);

    // ========== BOTTOM SECTION (Orderbook + Connections + PnL Stats) ==========
    let bot_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(24),  // Orderbook (wider for depth)
            Constraint::Length(43),  // Connections + Calibration display
            Constraint::Min(25),     // PnL Statistics
        ])
        .split(main_chunks[3]);

    // ----- Orderbook Panel (Magenta border) - Enhanced with depth -----
    let ob_age = ob.last_update.map(|t| t.elapsed().as_millis()).unwrap_or(9999);
    
    // Build orderbook depth display
    let mut ob_lines = Vec::new();
    
    // Header
    ob_lines.push(Line::from(vec![
        Span::styled("═══ UP ═══", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
    ]));
    ob_lines.push(Line::from(vec![
        Span::styled("  BID", Style::default().fg(Color::DarkGray)),
        Span::raw("      "),
        Span::styled("ASK  ", Style::default().fg(Color::DarkGray)),
    ]));
    
    // UP orderbook - show up to 3 levels
    for i in 0..3 {
        let bid = ob.up_bids.get(i);
        let ask = ob.up_asks.get(i);
        let bid_str = bid.map(|l| format!("{:>3}c {:>4}", (l.price * dec!(100)).round(), l.size.round()))
            .unwrap_or_else(|| "   -    -".to_string());
        let ask_str = ask.map(|l| format!("{:>4} {:>3}c", l.size.round(), (l.price * dec!(100)).round()))
            .unwrap_or_else(|| "   -    -".to_string());
        ob_lines.push(Line::from(vec![
            Span::styled(bid_str, Style::default().fg(Color::Green)),
            Span::raw(" "),
            Span::styled(ask_str, Style::default().fg(Color::Red)),
        ]));
    }
    
    ob_lines.push(Line::from(""));
    
    // DOWN section
    ob_lines.push(Line::from(vec![
        Span::styled("══ DOWN ══", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
    ]));
    ob_lines.push(Line::from(vec![
        Span::styled("  BID", Style::default().fg(Color::DarkGray)),
        Span::raw("      "),
        Span::styled("ASK  ", Style::default().fg(Color::DarkGray)),
    ]));
    
    // DOWN orderbook - show up to 3 levels
    for i in 0..3 {
        let bid = ob.down_bids.get(i);
        let ask = ob.down_asks.get(i);
        let bid_str = bid.map(|l| format!("{:>3}c {:>4}", (l.price * dec!(100)).round(), l.size.round()))
            .unwrap_or_else(|| "   -    -".to_string());
        let ask_str = ask.map(|l| format!("{:>4} {:>3}c", l.size.round(), (l.price * dec!(100)).round()))
            .unwrap_or_else(|| "   -    -".to_string());
        ob_lines.push(Line::from(vec![
            Span::styled(bid_str, Style::default().fg(Color::Green)),
            Span::raw(" "),
            Span::styled(ask_str, Style::default().fg(Color::Red)),
        ]));
    }
    
    ob_lines.push(Line::from(""));
    ob_lines.push(Line::from(vec![
        Span::styled("Age: ", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{}ms", ob_age), Style::default().fg(if ob_age < 1000 { Color::Green } else { Color::Yellow })),
        Span::raw(format!(" #{}", ob.update_count)),
    ]));
    
    let ob_block = Paragraph::new(ob_lines)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Magenta)).title(Span::styled(" Orderbook ", Style::default().fg(Color::Magenta))));
    f.render_widget(ob_block, bot_chunks[0]);

    // ----- Connections Panel (Yellow border) -----
    let binance_age = state.binance_last_msg.map(|t| t.elapsed().as_millis()).unwrap_or(9999);
    let poly_age = state.polymarket_last_msg.map(|t| t.elapsed().as_millis()).unwrap_or(9999);
    let binance_ok = matches!(state.binance_status, ConnectionStatus::Connected);
    let poly_ok = matches!(state.polymarket_status, ConnectionStatus::Connected);
    
    let conn_lines = vec![
        Line::from(vec![
            Span::styled(if binance_ok { "[OK]" } else { "[--]" }, Style::default().fg(if binance_ok { Color::Green } else { Color::Red })),
            Span::raw(" Binance   "),
            Span::styled(format!("{}ms", state.binance_latency_ms), Style::default().fg(
                if state.binance_latency_ms < 80 { Color::Green } 
                else if state.binance_latency_ms < 150 { Color::Yellow } 
                else { Color::Red }
            )),
            Span::raw(format!(" age:{}ms", binance_age)),
        ]),
        Line::from(vec![
            Span::styled(if poly_ok { "[OK]" } else { "[--]" }, Style::default().fg(if poly_ok { Color::Green } else { Color::Red })),
            Span::raw(" Polymarket "),
            Span::styled(format!("{}ms", state.polymarket_latency_ms), Style::default().fg(
                if state.polymarket_latency_ms < 50 { Color::Green }
                else if state.polymarket_latency_ms < 200 { Color::Yellow }
                else { Color::Red }
            )),
            Span::raw(format!(" age:{}ms", poly_age)),
        ]),

        Line::from(vec![
            Span::raw("ML Dir: "),
            Span::styled(format!("P(↑)={:.1}%", state.direction_prob_up * 100.0), 
                Style::default().fg(if state.direction_prob_up >= state.direction_threshold { 
                    Color::Green 
                } else if state.direction_prob_up <= (1.0 - state.direction_threshold) { 
                    Color::Red 
                } else { 
                    Color::Yellow 
                })),
            Span::raw(format!(" [thr: {}%]", (state.direction_threshold * 100.0) as u32)),
            Span::raw(format!(" | Vol: ${:.2}", state.ml_prediction_vol)),
            if state.ml_prediction_vol >= state.min_volatility_usd {
                Span::styled(" ✓", Style::default().fg(Color::Green))
            } else {
                Span::styled(" ✗", Style::default().fg(Color::Yellow))
            },
        ]),
        Line::from(vec![
            Span::raw("ML Exit: "),
            Span::styled(format!("P(UP)={:.0}%", state.intra_window_prediction * 100.0), 
                Style::default().fg(if state.intra_window_prediction > 0.5 { Color::Green } else { Color::Red })),
            Span::raw(" | Z="),
            Span::styled(format!("{:.2}", state.z_score), 
                Style::default().fg(if state.z_score.abs() > 1.2 { Color::Magenta } else if state.z_score > 0.0 { Color::Green } else { Color::Red })),
        ]),
        Line::from(vec![
            // Online calibration status - compact format
            {
                let (samples, blend) = state.calibration_bins.stats();
                Span::styled(format!("Cal: {}/{:.0}%", samples, blend * 100.0), 
                    Style::default().fg(if samples >= 30 { Color::Green } else { Color::Yellow }))
            },
            Span::raw(format!(" | VolHi: ${:.2}", state.session_max_volatility)),
        ]),
    ];
    let conn_block = Paragraph::new(conn_lines)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Yellow)).title(Span::styled(" Connections ", Style::default().fg(Color::Yellow))));
    f.render_widget(conn_block, bot_chunks[1]);

    // ----- PnL Statistics Panel (Yellow border) -----
    // Calculate current 5-min window start time  
    let now = Local::now();
    let now_epoch = now.timestamp();
    let window_start_epoch = (now_epoch / MARKET_WINDOW_SECS) * MARKET_WINDOW_SECS;
    let window_start = DateTime::from_timestamp(window_start_epoch, 0)
        .map(|dt| dt.with_timezone(&Local))
        .unwrap_or(now);
    
    // Calculate PnL for current 5-min window only
    let window_pnl: Decimal = state.trades.iter()
        .filter(|t| t.time >= window_start)
        .map(|t| t.pnl * t.size)
        .sum();
    let window_trades: usize = state.trades.iter().filter(|t| t.time >= window_start).count();
    let window_wins: usize = state.trades.iter().filter(|t| t.time >= window_start && t.pnl > dec!(0)).count();
    
    // Calculate all stats from trades deque for consistency (last 20 trades shown)
    let gross_profit: Decimal = state.trades.iter().filter(|t| t.pnl > dec!(0)).map(|t| t.pnl * t.size).sum();
    let gross_loss: Decimal = state.trades.iter().filter(|t| t.pnl < dec!(0)).map(|t| (t.pnl * t.size).abs()).sum();
    let win_count_visible = state.trades.iter().filter(|t| t.pnl > dec!(0)).count() as u32;
    let loss_count_visible = state.trades.iter().filter(|t| t.pnl < dec!(0)).count() as u32;
    let profit_factor = if gross_loss > dec!(0) { (gross_profit / gross_loss).to_f64().unwrap_or(0.0) } else { 0.0 };
    let avg_win: Decimal = if win_count_visible > 0 { gross_profit / Decimal::from(win_count_visible) } else { dec!(0) };
    let avg_loss: Decimal = if loss_count_visible > 0 { gross_loss / Decimal::from(loss_count_visible) } else { dec!(0) };
    let max_win: Decimal = state.trades.iter().map(|t| t.pnl * t.size).max().unwrap_or(dec!(0));
    let max_loss: Decimal = state.trades.iter().map(|t| t.pnl * t.size).min().unwrap_or(dec!(0));
    
    let pnl_lines = vec![
        Line::from(vec![
            Span::raw("5m Window: "),
            Span::styled(format!("${:.2}", window_pnl), 
                Style::default().fg(if window_pnl >= dec!(0) { Color::Green } else { Color::Red }).add_modifier(Modifier::BOLD)),
            Span::styled(format!(" ({}/{} W)", window_wins, window_trades), Style::default().fg(Color::DarkGray)),
            Span::raw("  Total: "),
            Span::styled(format!("${:.2}", state.total_pnl), 
                Style::default().fg(if state.total_pnl >= dec!(0) { Color::Green } else { Color::Red }).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::raw("Gross Profit: "),
            Span::styled(format!("${:.2}", gross_profit), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::raw("Gross Loss:   "),
            Span::styled(format!("${:.2}", gross_loss), Style::default().fg(Color::Red)),
        ]),
        Line::from(vec![
            Span::raw("Profit Factor: "),
            Span::styled(format!("{:.2}", profit_factor), Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::raw("Avg Win: "),
            Span::styled(format!("${:.2}", avg_win), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::raw("Max Win: "),
            Span::styled(format!("${:.2}", max_win), Style::default().fg(Color::Green)),
            Span::raw("  Max Loss: "),
            Span::styled(format!("${:.2}", max_loss), Style::default().fg(Color::Red)),
        ]),
    ];
    let pnl_block = Paragraph::new(pnl_lines)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::Yellow)).title(Span::styled(" PnL Statistics ", Style::default().fg(Color::Yellow))));
    f.render_widget(pnl_block, bot_chunks[2]);

    // ========== LOGS SECTION ==========
    let log_height = main_chunks[4].height.saturating_sub(2) as usize;
    let log_text: Vec<Line> = state.logs.iter().take(log_height).map(|l| {
        let style = if l.contains("[OK]") || l.contains("EXIT") {
            Style::default().fg(Color::Green)
        } else if l.contains("[X]") || l.contains("error") || l.contains("Error") {
            Style::default().fg(Color::Red)
        } else if l.contains(">>> ENTER") || l.contains("ENTRY") {
            Style::default().fg(Color::Cyan)
        } else if l.contains("[!]") || l.contains("WARN") {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        Line::from(Span::styled(l.as_str(), style))
    }).collect();
    let logs = Paragraph::new(log_text)
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)).title(" Logs "));
    f.render_widget(logs, main_chunks[4]);

    // ========== FOOTER ==========
    let market_slug = state.market.as_ref().map(|m| m.slug.clone()).unwrap_or_else(|| "None".to_string());
    let footer = Paragraph::new(Line::from(vec![
        Span::raw(" q:Quit  d:ToggleDry  +/-:Threshold  m:MinVol"),
        Span::raw(" | "),
        Span::styled(market_slug.chars().take(45).collect::<String>(), Style::default().fg(Color::DarkGray)),
    ]))
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, main_chunks[5]);
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    // Load config
    let dry_run = std::env::var("DRY_RUN").map(|v| v != "false").unwrap_or(true);
    let position_size: Decimal = std::env::var("POSITION_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(dec!(10));
    // MIN_VOLATILITY_USD: minimum predicted volatility in $ to trade (optimized: vol>10 has 67% WR)
    let min_volatility_usd: f32 = std::env::var("MIN_VOLATILITY_USD").ok().and_then(|v| v.parse().ok()).unwrap_or(5.0);
    let tick_threshold: usize = std::env::var("TICK_THRESHOLD").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let min_hold_ms: u64 = std::env::var("MIN_HOLD_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(3500);  // 3.5s min hold (5-min markets)
    let hold_timeout_ms: u64 = std::env::var("HOLD_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(15000);  // 15s max hold
    let min_z_score: f64 = std::env::var("MIN_Z_SCORE").ok().and_then(|v| v.parse().ok()).unwrap_or(0.5);  // Lower threshold for more trades
    let max_trades: u32 = std::env::var("MAX_TRADES").ok().and_then(|v| v.parse().ok()).unwrap_or(10);  // 5 per side for 5-min markets
    
    // Adaptive threshold config
    let target_trades_per_market: u32 = std::env::var("TARGET_TRADES_PER_MARKET").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    let adaptive_min_threshold: f32 = std::env::var("ADAPTIVE_MIN_THRESHOLD").ok().and_then(|v| v.parse().ok()).unwrap_or(1.0);
    let adaptive_max_threshold: f32 = std::env::var("ADAPTIVE_MAX_THRESHOLD").ok().and_then(|v| v.parse().ok()).unwrap_or(15.0);

    // ALWAYS initialize live executor (even in dry_run mode) so it's ready when toggling to live
    // This fixes the bug where starting in DRY_RUN=true then pressing 'd' would fail to execute orders
    match LiveExecutor::new().await {
        Ok(exec) => {
            EXECUTOR.set(Arc::new(exec)).ok();
            eprintln!("[OK] Live trading executor ready (dry_run={})", dry_run);
        }
        Err(e) => {
            eprintln!("[X] Failed to initialize executor: {}. Live trading disabled.", e);
        }
    }

    // Load trained tick-level ML model
    let ml = match MLPredictor::load("/data/tick_model.txt", "/data/tick_model_norm.txt") {
        Ok(m) => {
            eprintln!("[OK] Loaded trained tick-level ML model");
            Arc::new(m)
        }
        Err(e) => {
            eprintln!("[!] Could not load ML model ({}), using fallback", e);
            Arc::new(MLPredictor::new())
        }
    };
    
    // Load direction prediction model (primary entry signal)
    let direction_model = match DirectionPredictor::load(
        "models/direction_model.txt",
        "models/direction_model_norm.txt",
        "models/direction_model_calib.txt"
    ) {
        Ok(m) => {
            eprintln!("[OK] Loaded direction model for entry signals");
            Arc::new(m)
        }
        Err(e) => {
            eprintln!("[!] Could not load direction model ({}), using fallback", e);
            Arc::new(DirectionPredictor::new())
        }
    };

    // Load intra-window exit predictor
    let intra_predictor: Option<Arc<IntraWindowPredictor>> = match IntraWindowPredictor::load(
        "/data/intrawindow_model.txt",
        "/data/intrawindow_norm.txt"
    ) {
        Ok(p) => {
            eprintln!("[OK] Loaded intra-window exit predictor (15 features, 4 layers)");
            Some(Arc::new(p))
        }
        Err(e) => {
            eprintln!("[!] Could not load intra-window model ({}), using fixed timeout", e);
            None
        }
    };

    let state = Arc::new(RwLock::new(SharedState {
        dry_run,
        position_size,
        min_volatility_usd,
        base_min_volatility: min_volatility_usd,  // Store for adaptive threshold
        tick_threshold,
        min_hold_ms,
        hold_timeout_ms,
        min_z_score,
        max_trades,
        // Adaptive threshold config
        target_trades_per_market,
        adaptive_min_threshold,
        adaptive_max_threshold,
        ..Default::default()
    }));
    
    // Initialize database
    if let Err(e) = init_database() {
        eprintln!("[!] Failed to initialize database: {}", e);
    }
    
    // Load historical trades from database (falls back to CSV if no DB)
    {
        let mut s = state.write().await;
        load_trades_from_db(&mut s);
    }
    
    let feature_engine = Arc::new(RwLock::new(FeatureEngine::new()));
    let direction_feature_engine = Arc::new(RwLock::new(DirectionFeatureEngine::new(5000))); // 5s window

    // EVENT-DRIVEN: Channel for tick signals (Binance -> Strategy)
    // Capacity of 1 means if strategy is slow, we skip redundant signals
    let (tick_tx, tick_rx) = mpsc::channel::<()>(1);

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Spawn background tasks with AbortHandles for restart capability
    let binance_handle = tokio::spawn(run_binance_ws(
        Arc::clone(&state), 
        Arc::clone(&feature_engine), 
        Arc::clone(&direction_feature_engine),
        Arc::clone(&ml), 
        Arc::clone(&direction_model),
        tick_tx.clone(), 
        intra_predictor.clone()
    ));
    let mut binance_abort = binance_handle.abort_handle();
    
    // Liquidation stream for cascade detection (direction model feature)
    tokio::spawn(run_liquidation_ws(Arc::clone(&state)));
    
    tokio::spawn(run_market_discovery(Arc::clone(&state)));
    
    let poly_handle = tokio::spawn(run_polymarket_ws(Arc::clone(&state)));
    let mut poly_abort = poly_handle.abort_handle();
    
    // Multi-exchange early signal feeds DISABLED - data showed they underperform Binance:
    // Binance: +0.79% avg | Kraken: -0.12% | Bybit: -0.60% | Reversal: -0.13%
    // tokio::spawn(run_kraken_ws(Arc::clone(&state)));
    // tokio::spawn(run_bitfinex_ws(Arc::clone(&state)));
    // tokio::spawn(run_coinbase_ws(Arc::clone(&state)));
    // tokio::spawn(run_bybit_ws(Arc::clone(&state)));
    
    tokio::spawn(run_strategy(Arc::clone(&state), tick_rx, intra_predictor.clone()));

    // Main loop
    let tick_rate = Duration::from_millis(100);
    let mut last_tick = Instant::now();
    let mut last_health_check = Instant::now();
    const STALE_TIMEOUT_SECS: u64 = 10;  // Restart task if no data for 10s

    loop {
        // Update system health every 5 seconds
        if last_health_check.elapsed() > Duration::from_secs(5) {
            let health = collect_system_health();
            let mut s = state.write().await;
            s.system_health = health;
            last_health_check = Instant::now();
        }
        
        // Draw and check for stale connections
        let (binance_stale, poly_stale) = {
            let s = state.read().await;
            terminal.draw(|f| draw_ui(f, &s))?;
            
            let b_stale = s.binance_last_msg
                .map(|t| t.elapsed().as_secs() > STALE_TIMEOUT_SECS)
                .unwrap_or(false) && s.binance_status == ConnectionStatus::Connected;
            let p_stale = s.polymarket_last_msg
                .map(|t| t.elapsed().as_secs() > STALE_TIMEOUT_SECS)
                .unwrap_or(false) && s.polymarket_status == ConnectionStatus::Connected;
            (b_stale, p_stale)
        };
        
        // Restart stale Binance task
        if binance_stale {
            {
                let mut s = state.write().await;
                s.add_log(format!("[!] Binance stale >{}s, restarting...", STALE_TIMEOUT_SECS));
                s.binance_status = ConnectionStatus::Disconnected;
                s.binance_last_msg = Some(std::time::Instant::now());  // Reset to prevent immediate re-trigger
            }
            binance_abort.abort();
            let new_handle = tokio::spawn(run_binance_ws(
                Arc::clone(&state), 
                Arc::clone(&feature_engine), 
                Arc::clone(&direction_feature_engine),
                Arc::clone(&ml), 
                Arc::clone(&direction_model),
                tick_tx.clone(), 
                intra_predictor.clone()
            ));
            binance_abort = new_handle.abort_handle();
        }
        
        // Restart stale Polymarket task
        if poly_stale {
            {
                let mut s = state.write().await;
                s.add_log(format!("[!] Polymarket stale >{}s, restarting...", STALE_TIMEOUT_SECS));
                s.polymarket_status = ConnectionStatus::Disconnected;
                s.polymarket_last_msg = Some(std::time::Instant::now());  // Reset to prevent immediate re-trigger
            }
            poly_abort.abort();
            let new_handle = tokio::spawn(run_polymarket_ws(Arc::clone(&state)));
            poly_abort = new_handle.abort_handle();
        }

        // Handle input
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('d') => {
                            let mut s = state.write().await;
                            s.dry_run = !s.dry_run;
                            let dry_run = s.dry_run;
                            let mode_str = if dry_run { "DRY RUN" } else { "LIVE" };
                            s.add_log(format!("[!] Mode switched to {}", mode_str));
                            
                            // If switching to LIVE mode, immediately refresh pre-signed orders
                            if !dry_run {
                                if let Some(market) = &s.market {
                                    let up_token = market.up_token_id.clone();
                                    let down_token = market.down_token_id.clone();
                                    let size = s.position_size;
                                    s.add_log("[!] Refreshing pre-signed orders...".to_string());
                                    drop(s);  // Release lock before network call
                                    refresh_pre_signed_orders(&up_token, &down_token, size).await;
                                    let mut s2 = state.write().await;
                                    s2.pre_signed_up_ready = true;
                                    s2.pre_signed_down_ready = true;
                                    s2.add_log("[OK] Pre-signed orders ready".to_string());
                                }
                            }
                        }
                        KeyCode::Char('+') | KeyCode::Char('=') => {
                            let mut s = state.write().await;
                            s.tick_threshold = (s.tick_threshold + 1).min(10);
                            let t = s.tick_threshold;
                            s.add_log(format!("Tick threshold: {}", t));
                        }
                        KeyCode::Char('-') => {
                            let mut s = state.write().await;
                            s.tick_threshold = s.tick_threshold.saturating_sub(1).max(1);
                            let t = s.tick_threshold;
                            s.add_log(format!("Tick threshold: {}", t));
                        }
                        KeyCode::Char('m') => {
                            let mut s = state.write().await;
                            // Cycle through min volatility values (in $)
                            s.min_volatility_usd = match s.min_volatility_usd as u32 {
                                0..=2 => 5.0,    // $0-2 -> $5
                                3..=6 => 10.0,   // $3-6 -> $10
                                7..=12 => 15.0,  // $7-12 -> $15
                                13..=20 => 0.0,  // $13-20 -> $0 (disabled)
                                _ => 5.0,
                            };
                            let m = s.min_volatility_usd;
                            s.add_log(format!("Min volatility: ${:.2}", m));
                        }
                        _ => {}
                    }
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;

    Ok(())
}
