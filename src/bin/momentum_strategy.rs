// ============================================================================
// MOMENTUM STRATEGY - Based on Conditional Probability + Time Decay
// ============================================================================
//
// Mathematical Framework:
//   Z_t = (S_t - S_0) / (σ√τ)     # Normalized displacement  
//   P_t = Φ(Z_t)                   # True probability UP wins
//
// Key Insights:
//   - |Z_t| > 1.2 → "irreversible regime" - mean reversion negligible
//   - Probability drift is monotonic once established
//   - Signal flips are noise
//   - Time decay causes prices to move even without BTC movement
//
// Entry Logic:
//   - Detect early displacement in first 3 minutes
//   - Confirm with tick-level momentum spike
//   - Enter in direction of displacement
//
// Exit Logic:
//   - Hold if Z_t continues trending
//   - Exit only on catastrophic reversal OR resolution
//
// ============================================================================

use std::collections::VecDeque;
use std::f64::consts::PI;

/// Standard normal CDF (Φ function)
fn phi(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Error function approximation (Abramowitz and Stegun)
fn erf(x: f64) -> f64 {
    let a1 =  0.254829592;
    let a2 = -0.284496736;
    let a3 =  1.421413741;
    let a4 = -1.453152027;
    let a5 =  1.061405429;
    let p  =  0.3275911;
    
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();
    sign * y
}

/// Core market state - tracks the conditional probability framework
#[derive(Clone, Debug)]
pub struct MarketState {
    /// Reference price at window start (S_0)
    pub reference_price: f64,
    /// Current BTC price (S_t)
    pub current_price: f64,
    /// Window start timestamp (ms)
    pub window_start_ms: i64,
    /// Current timestamp (ms)
    pub current_ts_ms: i64,
    /// Estimated short-term volatility (σ)
    pub volatility: f64,
    /// Window duration (1 hour = 3_600_000 ms)
    pub window_duration_ms: i64,
}

impl MarketState {
    pub fn new(reference_price: f64, window_start_ms: i64) -> Self {
        Self {
            reference_price,
            current_price: reference_price,
            window_start_ms,
            current_ts_ms: window_start_ms,
            volatility: 50.0,  // Default ~$50 per √1hr, calibrate from data
            window_duration_ms: 60 * 60 * 1000,
        }
    }
    
    /// Time remaining in minutes
    pub fn tau_minutes(&self) -> f64 {
        let elapsed = self.current_ts_ms - self.window_start_ms;
        let remaining = self.window_duration_ms - elapsed;
        (remaining as f64 / 60_000.0).max(0.01)  // Avoid division by zero
    }
    
    /// Normalized displacement: Z_t = (S_t - S_0) / (σ√τ)
    pub fn z_score(&self) -> f64 {
        let tau = self.tau_minutes();
        let displacement = self.current_price - self.reference_price;
        displacement / (self.volatility * tau.sqrt())
    }
    
    /// True conditional probability: P(UP wins) = Φ(Z_t)
    pub fn true_prob_up(&self) -> f64 {
        phi(self.z_score())
    }
    
    /// True conditional probability: P(DOWN wins) = 1 - Φ(Z_t)
    pub fn true_prob_down(&self) -> f64 {
        1.0 - self.true_prob_up()
    }
    
    /// Check if in "irreversible regime" (|Z| > threshold)
    pub fn is_irreversible(&self, z_threshold: f64) -> bool {
        self.z_score().abs() > z_threshold
    }
    
    /// Predicted winning side based on current state
    pub fn predicted_side(&self) -> Option<Side> {
        let z = self.z_score();
        if z > 0.5 {
            Some(Side::Up)
        } else if z < -0.5 {
            Some(Side::Down)
        } else {
            None  // Too close to call
        }
    }
    
    /// Remaining time as percentage (1.0 = full window, 0.0 = expired)
    pub fn remaining_pct(&self) -> f64 {
        let elapsed = self.current_ts_ms - self.window_start_ms;
        1.0 - (elapsed as f64 / self.window_duration_ms as f64).min(1.0)
    }
    
    /// Elapsed time as percentage
    pub fn elapsed_pct(&self) -> f64 {
        1.0 - self.remaining_pct()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Side {
    Up,
    Down,
}

// ============================================================================
// TICK-LEVEL MOMENTUM DETECTOR
// Detects momentum spikes BEFORE they fully develop
// ============================================================================

#[derive(Clone, Debug)]
pub struct TickMomentum {
    /// Recent ticks: (timestamp_us, price, quantity, is_buy)
    ticks: VecDeque<(i64, f64, f64, bool)>,
    /// Maximum ticks to keep
    max_ticks: usize,
    /// Price at last check
    last_check_price: f64,
    /// Timestamp of last check
    last_check_ts: i64,
}

impl TickMomentum {
    pub fn new(max_ticks: usize) -> Self {
        Self {
            ticks: VecDeque::with_capacity(max_ticks),
            max_ticks,
            last_check_price: 0.0,
            last_check_ts: 0,
        }
    }
    
    pub fn push(&mut self, ts_us: i64, price: f64, qty: f64, is_buy: bool) {
        self.ticks.push_back((ts_us, price, qty, is_buy));
        if self.ticks.len() > self.max_ticks {
            self.ticks.pop_front();
        }
    }
    
    /// Detect momentum spike - returns (has_spike, direction, confidence)
    /// Direction: +1 = up spike, -1 = down spike, 0 = no spike
    pub fn detect_spike(&mut self, lookback_ms: i64) -> (bool, i8, f64) {
        if self.ticks.len() < 10 {
            return (false, 0, 0.0);
        }
        
        let now_us = self.ticks.back().map(|t| t.0).unwrap_or(0);
        let cutoff_us = now_us - (lookback_ms * 1000);
        
        // Get recent ticks
        let recent: Vec<_> = self.ticks.iter()
            .filter(|t| t.0 >= cutoff_us)
            .collect();
        
        if recent.len() < 5 {
            return (false, 0, 0.0);
        }
        
        // Calculate metrics
        let first_price = recent.first().map(|t| t.1).unwrap_or(0.0);
        let last_price = recent.last().map(|t| t.1).unwrap_or(0.0);
        let price_change = last_price - first_price;
        
        // Volume imbalance
        let buy_vol: f64 = recent.iter().filter(|t| t.3).map(|t| t.2).sum();
        let sell_vol: f64 = recent.iter().filter(|t| !t.3).map(|t| t.2).sum();
        let total_vol = buy_vol + sell_vol;
        let vol_imbalance = if total_vol > 0.0 {
            (buy_vol - sell_vol) / total_vol
        } else {
            0.0
        };
        
        // Tick rate (ticks per second)
        let duration_s = (recent.last().unwrap().0 - recent.first().unwrap().0) as f64 / 1_000_000.0;
        let tick_rate = if duration_s > 0.0 { recent.len() as f64 / duration_s } else { 0.0 };
        
        // Consecutive same-direction ticks
        let mut up_run = 0i32;
        let mut down_run = 0i32;
        let mut max_up_run = 0i32;
        let mut max_down_run = 0i32;
        
        for t in &recent {
            if t.3 {  // is_buy
                up_run += 1;
                down_run = 0;
                max_up_run = max_up_run.max(up_run);
            } else {
                down_run += 1;
                up_run = 0;
                max_down_run = max_down_run.max(down_run);
            }
        }
        
        // Spike detection thresholds
        let price_threshold = 5.0;  // $5 move
        let vol_imbalance_threshold = 0.3;  // 30% imbalance
        let tick_rate_threshold = 20.0;  // 20 ticks/sec (high activity)
        let run_threshold = 5;  // 5 consecutive same direction
        
        // Combine signals
        let up_signals = (
            (price_change > price_threshold) as i32 +
            (vol_imbalance > vol_imbalance_threshold) as i32 +
            (max_up_run as i32 > run_threshold) as i32 +
            (tick_rate > tick_rate_threshold) as i32
        );
        
        let down_signals = (
            (price_change < -price_threshold) as i32 +
            (vol_imbalance < -vol_imbalance_threshold) as i32 +
            (max_down_run as i32 > run_threshold) as i32 +
            (tick_rate > tick_rate_threshold) as i32
        );
        
        // Need at least 2 signals to confirm
        if up_signals >= 2 {
            let confidence = up_signals as f64 / 4.0;
            return (true, 1, confidence);
        }
        
        if down_signals >= 2 {
            let confidence = down_signals as f64 / 4.0;
            return (true, -1, confidence);
        }
        
        (false, 0, 0.0)
    }
    
    /// Calculate current momentum direction (-1 to +1)
    pub fn momentum(&self, lookback_ms: i64) -> f64 {
        if self.ticks.len() < 5 {
            return 0.0;
        }
        
        let now_us = self.ticks.back().map(|t| t.0).unwrap_or(0);
        let cutoff_us = now_us - (lookback_ms * 1000);
        
        let recent: Vec<_> = self.ticks.iter()
            .filter(|t| t.0 >= cutoff_us)
            .collect();
        
        if recent.len() < 2 {
            return 0.0;
        }
        
        // Calculate returns
        let mut returns = Vec::new();
        for window in recent.windows(2) {
            if window.len() == 2 {
                let ret = (window[1].1 - window[0].1) / window[0].1;
                returns.push(ret);
            }
        }
        
        if returns.is_empty() {
            return 0.0;
        }
        
        // Sum of returns (momentum)
        let momentum: f64 = returns.iter().sum();
        
        // Normalize to -1 to +1
        (momentum * 10000.0).tanh()
    }
}

// ============================================================================
// ENTRY MODEL - Predicts momentum spike before it happens
// ============================================================================

#[derive(Clone, Debug)]
pub struct EntrySignal {
    pub side: Side,
    pub confidence: f64,
    pub z_score: f64,
    pub momentum: f64,
    pub remaining_pct: f64,
}

pub struct EntryModel {
    /// Minimum |Z| to consider entry
    min_z_threshold: f64,
    /// Maximum |Z| (too late, already priced in)
    max_z_threshold: f64,
    /// Minimum confidence from tick momentum
    min_confidence: f64,
    /// Window for early entry (first N% of window)
    early_window_pct: f64,
}

impl EntryModel {
    pub fn new() -> Self {
        Self {
            min_z_threshold: 0.3,   // Need some displacement to enter
            max_z_threshold: 2.0,   // Above this, already priced in
            min_confidence: 0.4,    // Minimum spike confidence
            early_window_pct: 0.3,  // First 30% of window = ~4.5 minutes
        }
    }
    
    /// Generate entry signal
    pub fn signal(
        &self,
        market: &MarketState,
        tick_momentum: &TickMomentum,
    ) -> Option<EntrySignal> {
        let z = market.z_score();
        let z_abs = z.abs();
        let remaining = market.remaining_pct();
        let elapsed = market.elapsed_pct();
        
        // Check if in valid entry window
        // Best entries: early window with established displacement
        // OR late window with strong Z (riding the probability collapse)
        
        let in_early_window = elapsed < self.early_window_pct;
        let in_late_window = remaining < 0.3;  // Last 4.5 minutes
        
        // Early entry: detect displacement forming
        if in_early_window {
            // Need tick momentum spike + some Z movement
            let (has_spike, direction, confidence) = tick_momentum.clone().detect_spike(500);
            
            if has_spike && confidence >= self.min_confidence {
                let side = if direction > 0 { Side::Up } else { Side::Down };
                
                // Check Z is moving in same direction
                let z_confirms = (z > 0.1 && direction > 0) || (z < -0.1 && direction < 0);
                
                if z_confirms || z_abs < 0.3 {  // Enter if Z confirms OR Z is neutral
                    return Some(EntrySignal {
                        side,
                        confidence,
                        z_score: z,
                        momentum: tick_momentum.momentum(500),
                        remaining_pct: remaining,
                    });
                }
            }
        }
        
        // Late entry: ride the probability collapse
        if in_late_window && z_abs > 0.8 && z_abs < self.max_z_threshold {
            let side = if z > 0.0 { Side::Up } else { Side::Down };
            
            // Calculate edge: true_prob vs market (assume market is lagging)
            let true_prob = market.true_prob_up();
            let edge = if side == Side::Up {
                true_prob - 0.5  // Simplified; in production use actual market price
            } else {
                0.5 - true_prob
            };
            
            if edge > 0.05 {  // 5% edge minimum
                return Some(EntrySignal {
                    side,
                    confidence: edge.min(1.0),
                    z_score: z,
                    momentum: tick_momentum.momentum(500),
                    remaining_pct: remaining,
                });
            }
        }
        
        None
    }
}

// ============================================================================
// EXIT MODEL - Predicts if direction will continue
// ============================================================================

#[derive(Clone, Debug)]
pub struct ExitSignal {
    pub should_exit: bool,
    pub reason: ExitReason,
    pub continuation_prob: f64,
}

#[derive(Clone, Debug)]
pub enum ExitReason {
    Hold,              // Keep position
    TakeProfit,        // Z strongly in our favor
    StopLoss,          // Z reversed against us
    TimeExpiry,        // Near resolution, lock in
    MomentumReversal,  // Tick momentum reversed
}

pub struct ExitModel {
    /// Z threshold for take profit
    take_profit_z: f64,
    /// Z threshold for stop loss (reversal)
    stop_loss_z: f64,
    /// Time threshold for expiry exit (remaining %)
    expiry_threshold: f64,
}

impl ExitModel {
    pub fn new() -> Self {
        Self {
            take_profit_z: 1.5,     // Exit when strongly in favor
            stop_loss_z: -0.5,      // Exit if Z reverses against us
            expiry_threshold: 0.05, // Last 45 seconds
        }
    }
    
    /// Generate exit signal for a position
    pub fn signal(
        &self,
        market: &MarketState,
        position_side: Side,
        entry_z: f64,
        tick_momentum: &TickMomentum,
    ) -> ExitSignal {
        let current_z = market.z_score();
        let remaining = market.remaining_pct();
        
        // Calculate Z relative to our position
        let z_for_us = match position_side {
            Side::Up => current_z,
            Side::Down => -current_z,
        };
        
        // Time expiry - near resolution, let it ride
        if remaining < self.expiry_threshold {
            return ExitSignal {
                should_exit: false,
                reason: ExitReason::TimeExpiry,
                continuation_prob: 0.95,  // Very likely to continue
            };
        }
        
        // Take profit - strongly in our favor
        if z_for_us > self.take_profit_z {
            return ExitSignal {
                should_exit: false,  // Actually HOLD when strongly winning
                reason: ExitReason::TakeProfit,
                continuation_prob: 0.9,
            };
        }
        
        // Stop loss - Z reversed against us significantly
        if z_for_us < self.stop_loss_z {
            // Check if momentum confirms reversal
            let momentum = tick_momentum.momentum(1000);
            let momentum_against = match position_side {
                Side::Up => momentum < -0.3,
                Side::Down => momentum > 0.3,
            };
            
            if momentum_against {
                return ExitSignal {
                    should_exit: true,
                    reason: ExitReason::StopLoss,
                    continuation_prob: 0.3,
                };
            }
        }
        
        // Check for momentum reversal
        let (has_spike, direction, confidence) = tick_momentum.clone().detect_spike(500);
        if has_spike && confidence > 0.5 {
            let spike_against = match position_side {
                Side::Up => direction < 0,
                Side::Down => direction > 0,
            };
            
            if spike_against {
                return ExitSignal {
                    should_exit: true,
                    reason: ExitReason::MomentumReversal,
                    continuation_prob: 0.4,
                };
            }
        }
        
        // Default: HOLD
        // Calculate continuation probability based on Z and time
        let irreversible = market.is_irreversible(1.2);
        let continuation_prob = if irreversible && z_for_us > 0.0 {
            0.85  // Strongly trending in our favor
        } else if z_for_us > 0.0 {
            0.6 + z_for_us.min(0.3)  // Slight edge
        } else {
            0.5 - z_for_us.abs().min(0.2)  // Slight disadvantage
        };
        
        ExitSignal {
            should_exit: false,
            reason: ExitReason::Hold,
            continuation_prob,
        }
    }
}

// ============================================================================
// POSITION SIZING - Kelly-based with caps
// ============================================================================

pub struct PositionSizer {
    /// Kelly fraction (0.1-0.25 recommended)
    kelly_fraction: f64,
    /// Maximum position as % of bankroll
    max_position_pct: f64,
    /// Base position size in dollars
    base_size: f64,
}

impl PositionSizer {
    pub fn new(base_size: f64) -> Self {
        Self {
            kelly_fraction: 0.15,
            max_position_pct: 0.02,  // 2% max per trade
            base_size,
        }
    }
    
    /// Calculate optimal position size
    /// edge: our estimated edge (true_prob - market_prob)
    /// market_price: current market price for this side
    /// remaining_pct: time remaining in window
    pub fn size(
        &self,
        edge: f64,
        market_price: f64,
        remaining_pct: f64,
        bankroll: f64,
    ) -> f64 {
        if edge <= 0.02 {
            return 0.0;  // No trade under 2% edge
        }
        
        // Kelly: f* = (q - p) / (p * (1 - p))
        let q = market_price + edge;  // Our true probability
        let p = market_price;
        
        let kelly = if p > 0.0 && p < 1.0 {
            (q - p) / (p * (1.0 - p))
        } else {
            0.0
        };
        
        // Apply fractional Kelly
        let fractional_kelly = kelly * self.kelly_fraction;
        
        // Time-weighted: increase size as time decreases (more certainty)
        let time_multiplier = 1.0 + (1.0 - remaining_pct) * 0.5;  // Up to 1.5x at end
        
        // Calculate size
        let kelly_size = fractional_kelly * bankroll * time_multiplier;
        
        // Apply caps
        let max_size = bankroll * self.max_position_pct;
        let capped_size = kelly_size.min(max_size).max(0.0);
        
        // Discretize to nearest base_size
        let units = (capped_size / self.base_size).floor();
        units * self.base_size
    }
    
    /// Simple tier-based sizing (more robust)
    pub fn size_tiered(&self, edge: f64, bankroll: f64) -> f64 {
        let base = self.base_size;
        
        match edge {
            e if e < 0.02 => 0.0,           // No trade
            e if e < 0.04 => base * 0.5,    // Small
            e if e < 0.07 => base * 1.0,    // Normal
            e if e < 0.10 => base * 1.5,    // Large
            _ => base * 2.0,                 // Max
        }.min(bankroll * self.max_position_pct)
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_phi() {
        assert!((phi(0.0) - 0.5).abs() < 0.001);
        assert!((phi(1.96) - 0.975).abs() < 0.01);
        assert!((phi(-1.96) - 0.025).abs() < 0.01);
    }
    
    #[test]
    fn test_market_state() {
        let mut market = MarketState::new(90000.0, 0);
        market.current_price = 90100.0;  // $100 up
        market.current_ts_ms = 5 * 60 * 1000;  // 5 minutes in
        
        // Z should be positive
        assert!(market.z_score() > 0.0);
        
        // Prob up should be > 0.5
        assert!(market.true_prob_up() > 0.5);
        
        // Not yet irreversible
        assert!(!market.is_irreversible(1.2));
    }
    
    #[test]
    fn test_time_decay() {
        let mut market = MarketState::new(90000.0, 0);
        market.current_price = 89900.0;  // $100 down
        
        // At 5 minutes
        market.current_ts_ms = 5 * 60 * 1000;
        let z_early = market.z_score();
        let p_early = market.true_prob_up();
        
        // At 14 minutes
        market.current_ts_ms = 14 * 60 * 1000;
        let z_late = market.z_score();
        let p_late = market.true_prob_up();
        
        // Z should be more extreme later (same price, less time)
        assert!(z_late.abs() > z_early.abs());
        
        // Probability should be more extreme later
        assert!(p_late < p_early);  // DOWN is winning, so P(up) decreases
    }
}
