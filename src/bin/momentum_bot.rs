// ============================================================================
// MOMENTUM ML BOT - High-frequency tick-level trading
// ============================================================================
//
// Architecture:
//   Binance WS (500+ ticks/sec) → TickProcessor → Entry/Exit Models → Polymarket
//
// Speed targets:
//   - Tick processing: <1ms
//   - Decision: <5ms  
//   - Order submission: <50ms (pre-signed)
//   - Total latency: <100ms
//
// Key features:
//   - Microsecond timestamp tracking
//   - Zero-allocation hot path
//   - Pre-signed order caching
//   - Conditional probability framework
//
// ============================================================================

mod momentum_strategy;

use momentum_strategy::{
    MarketState, TickMomentum, EntryModel, ExitModel, 
    PositionSizer, Side, EntrySignal, ExitSignal, ExitReason
};

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

// ============================================================================
// FAST TICK PARSER - Zero-allocation hot path
// ============================================================================

/// Parse Binance trade message without serde (hot path optimization)
/// Format: {"e":"trade","E":1234567890123,"s":"BTCUSDT","t":123456789,"p":"91234.56","q":"0.001","b":123,"a":456,"T":1234567890123,"m":true,"M":true}
#[inline(always)]
pub fn parse_binance_trade_fast(json: &[u8]) -> Option<(i64, f64, f64, bool)> {
    // Use memchr for fast byte searching
    use memchr::memmem;
    
    // Find "T": (trade timestamp in ms)
    let ts_start = memmem::find(json, b"\"T\":")?;
    let ts_slice = &json[ts_start + 4..];
    let ts_end = ts_slice.iter().position(|&b| b == b',')?;
    let ts: i64 = std::str::from_utf8(&ts_slice[..ts_end]).ok()?.parse().ok()?;
    
    // Find "p":" (price)
    let p_start = memmem::find(json, b"\"p\":\"")?;
    let p_slice = &json[p_start + 5..];
    let p_end = p_slice.iter().position(|&b| b == b'"')?;
    let price: f64 = std::str::from_utf8(&p_slice[..p_end]).ok()?.parse().ok()?;
    
    // Find "q":" (quantity)
    let q_start = memmem::find(json, b"\"q\":\"")?;
    let q_slice = &json[q_start + 5..];
    let q_end = q_slice.iter().position(|&b| b == b'"')?;
    let qty: f64 = std::str::from_utf8(&q_slice[..q_end]).ok()?.parse().ok()?;
    
    // Find "m": (is buyer maker = seller aggressive = is_sell)
    let m_start = memmem::find(json, b"\"m\":")?;
    let m_slice = &json[m_start + 4..];
    let is_sell = m_slice.starts_with(b"true");
    let is_buy = !is_sell;
    
    Some((ts * 1000, price, qty, is_buy))  // Convert to microseconds
}

// ============================================================================
// SHARED STATE
// ============================================================================

#[derive(Clone)]
pub struct SharedState {
    // Market state
    pub market: MarketState,
    // Current Polymarket window info
    pub window_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub up_price: f64,
    pub down_price: f64,
    // Position
    pub position: Option<Position>,
    // Stats
    pub total_trades: u32,
    pub winning_trades: u32,
    pub pnl_cents: i64,
    // Control
    pub dry_run: bool,
    pub position_size: f64,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub side: Side,
    pub entry_z: f64,
    pub entry_price: f64,
    pub entry_ts_ms: i64,
    pub shares: f64,
}

// ============================================================================
// MAIN BOT STRUCTURE
// ============================================================================

pub struct MomentumBot {
    state: Arc<RwLock<SharedState>>,
    tick_momentum: TickMomentum,
    entry_model: EntryModel,
    exit_model: ExitModel,
    sizer: PositionSizer,
    // Timing
    last_tick_us: i64,
    tick_count: u64,
    // Pre-signed orders (to be integrated with Polymarket SDK)
    // up_buy_order: Option<SignedOrder>,
    // down_buy_order: Option<SignedOrder>,
}

impl MomentumBot {
    pub fn new(
        market: MarketState,
        dry_run: bool,
        position_size: f64,
    ) -> Self {
        let state = SharedState {
            market,
            window_id: String::new(),
            up_token_id: String::new(),
            down_token_id: String::new(),
            up_price: 0.50,
            down_price: 0.50,
            position: None,
            total_trades: 0,
            winning_trades: 0,
            pnl_cents: 0,
            dry_run,
            position_size,
        };
        
        Self {
            state: Arc::new(RwLock::new(state)),
            tick_momentum: TickMomentum::new(500),  // Keep last 500 ticks
            entry_model: EntryModel::new(),
            exit_model: ExitModel::new(),
            sizer: PositionSizer::new(position_size),
            last_tick_us: 0,
            tick_count: 0,
        }
    }
    
    /// Process a single tick - this is the hot path
    #[inline]
    pub async fn process_tick(&mut self, ts_us: i64, price: f64, qty: f64, is_buy: bool) {
        // Record tick latency
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let tick_latency_us = now_us - ts_us;
        
        // Update tick momentum (lock-free)
        self.tick_momentum.push(ts_us, price, qty, is_buy);
        self.tick_count += 1;
        self.last_tick_us = ts_us;
        
        // Quick read to check if we need to act
        let needs_action = {
            let state = self.state.read().await;
            
            // Update market state
            let market = &state.market;
            if market.reference_price == 0.0 {
                return;  // Not initialized yet
            }
            
            // Every 10 ticks, check for signals
            self.tick_count % 10 == 0
        };
        
        if !needs_action {
            return;
        }
        
        // Check entry/exit signals
        self.check_signals(price, ts_us / 1000).await;
    }
    
    async fn check_signals(&mut self, current_price: f64, ts_ms: i64) {
        let mut state = self.state.write().await;
        
        // Update market state
        state.market.current_price = current_price;
        state.market.current_ts_ms = ts_ms;
        
        // Check if we have a position
        if let Some(ref position) = state.position {
            // Check exit signal
            let exit_signal = self.exit_model.signal(
                &state.market,
                position.side,
                position.entry_z,
                &self.tick_momentum,
            );
            
            if exit_signal.should_exit {
                self.execute_exit(&mut state, exit_signal).await;
            }
        } else {
            // Check entry signal
            if let Some(entry_signal) = self.entry_model.signal(&state.market, &self.tick_momentum) {
                self.execute_entry(&mut state, entry_signal).await;
            }
        }
    }
    
    async fn execute_entry(&self, state: &mut SharedState, signal: EntrySignal) {
        let market_price = match signal.side {
            Side::Up => state.up_price,
            Side::Down => state.down_price,
        };
        
        // Calculate edge
        let true_prob = match signal.side {
            Side::Up => state.market.true_prob_up(),
            Side::Down => state.market.true_prob_down(),
        };
        let edge = true_prob - market_price;
        
        // Calculate position size
        let size = self.sizer.size_tiered(edge, 1000.0);  // $1000 bankroll placeholder
        
        if size <= 0.0 {
            return;  // No trade
        }
        
        // Log entry
        let side_str = match signal.side {
            Side::Up => "UP",
            Side::Down => "DOWN",
        };
        
        println!(">>>╢ ENTRY {} | Z={:.3} | Edge={:.1}% | Size=${:.2} | Conf={:.2}",
            side_str,
            signal.z_score,
            edge * 100.0,
            size,
            signal.confidence
        );
        
        // Create position
        state.position = Some(Position {
            side: signal.side,
            entry_z: signal.z_score,
            entry_price: market_price,
            entry_ts_ms: state.market.current_ts_ms,
            shares: size / market_price,
        });
        
        state.total_trades += 1;
        
        if !state.dry_run {
            // TODO: Execute actual order via pre-signed order
            // self.submit_order(signal.side, size).await;
        }
    }
    
    async fn execute_exit(&self, state: &mut SharedState, signal: ExitSignal) {
        let position = match state.position.take() {
            Some(p) => p,
            None => return,
        };
        
        // Calculate PnL
        let exit_price = match position.side {
            Side::Up => state.up_price,
            Side::Down => state.down_price,
        };
        
        let pnl = (exit_price - position.entry_price) * position.shares;
        let pnl_cents = (pnl * 100.0) as i64;
        
        state.pnl_cents += pnl_cents;
        if pnl > 0.0 {
            state.winning_trades += 1;
        }
        
        let side_str = match position.side {
            Side::Up => "UP",
            Side::Down => "DOWN",
        };
        
        let reason_str = match signal.reason {
            ExitReason::Hold => "HOLD",
            ExitReason::TakeProfit => "TP",
            ExitReason::StopLoss => "SL",
            ExitReason::TimeExpiry => "TIME",
            ExitReason::MomentumReversal => "REVERSAL",
        };
        
        println!("<<<╢ EXIT {} | Reason={} | PnL={:+}¢ | Total={:+}¢",
            side_str,
            reason_str,
            pnl_cents,
            state.pnl_cents
        );
        
        if !state.dry_run {
            // TODO: Execute actual sell order
        }
    }
    
    /// Update Polymarket prices
    pub async fn update_prices(&self, up_price: f64, down_price: f64) {
        let mut state = self.state.write().await;
        state.up_price = up_price;
        state.down_price = down_price;
    }
    
    /// Update market window (new 1-hour period)
    pub async fn update_window(
        &self,
        window_id: String,
        up_token_id: String,
        down_token_id: String,
        reference_price: f64,
        window_start_ms: i64,
    ) {
        let mut state = self.state.write().await;
        state.window_id = window_id;
        state.up_token_id = up_token_id;
        state.down_token_id = down_token_id;
        state.market = MarketState::new(reference_price, window_start_ms);
        state.position = None;  // Clear position on new window
    }
    
    /// Get current stats
    pub async fn stats(&self) -> (u32, u32, i64, f64) {
        let state = self.state.read().await;
        let win_rate = if state.total_trades > 0 {
            state.winning_trades as f64 / state.total_trades as f64
        } else {
            0.0
        };
        (state.total_trades, state.winning_trades, state.pnl_cents, win_rate)
    }
}

// ============================================================================
// FEATURE ENGINEERING FOR ML MODELS
// ============================================================================

/// Features for entry/exit ML models
/// These should be trained on tick-level data
#[derive(Clone, Debug)]
pub struct TickFeatures {
    // Core conditional probability features
    pub z_score: f64,                 // Normalized displacement
    pub remaining_pct: f64,           // Time remaining
    pub true_prob_up: f64,            // Theoretical P(up)
    
    // Tick momentum features (last 500ms)
    pub price_change_500ms: f64,      // Price change
    pub volume_imbalance_500ms: f64,  // Buy vs sell volume
    pub tick_rate_500ms: f64,         // Ticks per second
    pub consecutive_buys: i32,        // Max consecutive buy ticks
    pub consecutive_sells: i32,       // Max consecutive sell ticks
    
    // Volatility features
    pub realized_vol_1min: f64,       // Recent realized volatility
    pub vol_of_vol: f64,              // Volatility of volatility
    
    // Order flow features
    pub vwap_distance: f64,           // Distance from VWAP
    pub trade_intensity: f64,         // Trades per second normalized
    
    // Polymarket specific
    pub up_price: f64,                // Current UP market price
    pub down_price: f64,              // Current DOWN market price
    pub spread: f64,                  // Bid-ask spread estimate
    pub edge_up: f64,                 // True prob - market price (UP)
    pub edge_down: f64,               // True prob - market price (DOWN)
}

impl TickFeatures {
    pub fn compute(
        market: &MarketState,
        tick_momentum: &TickMomentum,
        up_price: f64,
        down_price: f64,
    ) -> Self {
        let z = market.z_score();
        let true_up = market.true_prob_up();
        let true_down = 1.0 - true_up;
        
        Self {
            z_score: z,
            remaining_pct: market.remaining_pct(),
            true_prob_up: true_up,
            
            price_change_500ms: 0.0,    // TODO: compute from tick_momentum
            volume_imbalance_500ms: 0.0,
            tick_rate_500ms: 0.0,
            consecutive_buys: 0,
            consecutive_sells: 0,
            
            realized_vol_1min: market.volatility,
            vol_of_vol: 0.0,
            
            vwap_distance: 0.0,
            trade_intensity: 0.0,
            
            up_price,
            down_price,
            spread: (up_price + down_price - 1.0).abs(),
            edge_up: true_up - up_price,
            edge_down: true_down - down_price,
        }
    }
    
    /// Convert to array for ML model input
    pub fn to_array(&self) -> [f64; 17] {
        [
            self.z_score,
            self.remaining_pct,
            self.true_prob_up,
            self.price_change_500ms,
            self.volume_imbalance_500ms,
            self.tick_rate_500ms,
            self.consecutive_buys as f64,
            self.consecutive_sells as f64,
            self.realized_vol_1min,
            self.vol_of_vol,
            self.vwap_distance,
            self.trade_intensity,
            self.up_price,
            self.down_price,
            self.spread,
            self.edge_up,
            self.edge_down,
        ]
    }
}

// ============================================================================
// TRAINING DATA COLLECTION
// ============================================================================

/// Collect training data for ML models
#[derive(Clone, Debug)]
pub struct TrainingExample {
    pub features: TickFeatures,
    pub timestamp_us: i64,
    
    // Labels (computed after the fact)
    pub price_5s_later: Option<f64>,   // BTC price 5s later
    pub price_30s_later: Option<f64>,  // BTC price 30s later
    pub up_price_5s_later: Option<f64>,
    pub down_price_5s_later: Option<f64>,
    
    // Did a spike happen in next 10 seconds?
    pub spike_up_10s: Option<bool>,
    pub spike_down_10s: Option<bool>,
    
    // Entry model target: P(profitable entry)
    pub entry_profitable: Option<bool>,
    
    // Exit model target: P(continuation)
    pub continuation_30s: Option<bool>,
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

#[tokio::main]
async fn main() {
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║         MOMENTUM ML BOT v0.1.0                               ║");
    println!("║         Tick-level conditional probability trading           ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    
    // Parse environment
    let dry_run = std::env::var("DRY_RUN")
        .map(|v| v != "false")
        .unwrap_or(true);
    
    let position_size: f64 = std::env::var("POSITION_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10.0);
    
    println!("[*] Mode: {}", if dry_run { "DRY RUN" } else { "LIVE" });
    println!("[*] Position size: ${:.2}", position_size);
    
    // Initialize market state (placeholder - will be set when window starts)
    let market = MarketState::new(0.0, 0);
    
    // Create bot
    let mut bot = MomentumBot::new(market, dry_run, position_size);
    
    // TODO: Connect to Binance WebSocket
    // TODO: Connect to Polymarket for prices
    // TODO: Main loop processing ticks
    
    println!("[*] Bot initialized. Connect to data feeds to start trading.");
    
    // Placeholder main loop
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
