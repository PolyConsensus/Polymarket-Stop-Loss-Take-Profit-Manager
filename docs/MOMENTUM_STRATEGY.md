# Momentum ML Strategy

## Overview

This strategy is designed specifically for Polymarket's 15-minute BTC prediction markets, based on the **conditional probability framework**:

```
P(UP wins) = Φ(Z_t)

where Z_t = (S_t - S_0) / (σ√τ)
```

- `S_t` = Current BTC price
- `S_0` = Reference price at window start
- `σ` = Short-term volatility (~$50/√min)
- `τ` = Time remaining (minutes)
- `Φ` = Standard normal CDF

## Key Insights

1. **Time Decay**: Probability collapses even without price movement as τ → 0
2. **Irreversible Regime**: When |Z| > 1.2, mean reversion is negligible
3. **Signal Flips are Noise**: Once direction established, probability drift is monotonic
4. **Speed Matters**: Big moves happen in <20ms, you have ~500ms to act

## Two ML Models

### Entry Model
**Purpose**: Detect momentum spike BEFORE it fully develops

**Target**: `P(BTC moves >$20 in next 10 seconds)`

**Features**:
- Z-score (current displacement)
- Time remaining %
- True probability (from Φ)
- Tick momentum (price change, volume imbalance, tick rate)
- Consecutive buy/sell runs
- Realized volatility

**Entry Conditions**:
1. Early window (first 3 min): Tick spike + Z confirming
2. Late window (last 5 min): Strong Z + edge over market

### Exit Model
**Purpose**: Predict if current direction will continue

**Target**: `P(price continues same direction for 30s)`

**Exit Conditions**:
1. **Hold**: If continuation probability > 60%
2. **Stop Loss**: Z reversed against us AND momentum confirms
3. **Take Profit**: At resolution (hold to 99%+ convergence)
4. **Reversal**: Strong spike against position

## Architecture

```
Binance WS (500+ ticks/sec)
       ↓
parse_binance_trade_fast() [<1ms, zero-allocation]
       ↓
TickMomentum buffer (500 ticks)
       ↓
Every 10 ticks: check_signals()
       ↓
Entry Model → Should we enter?
Exit Model  → Should we exit?
       ↓
Pre-signed order submission [<50ms]
```

## Position Sizing

Based on Kelly criterion with caps:

```rust
// Kelly: f* = (q - p) / (p(1-p))
// Fractional: f = 0.15 × f*
// Cap: max 2% per trade

// Tiered sizing:
| Edge    | Size      |
|---------|-----------|
| < 2%    | No trade  |
| 2-4%    | 0.5× base |
| 4-7%    | 1.0× base |
| 7-10%   | 1.5× base |
| > 10%   | 2.0× base |
```

## Data Collection

```bash
# Collect tick data
python scripts/collect_training_data.py

# After collection, compute labels (must be done offline)
python scripts/collect_training_data.py label features.parquet ticks.parquet labeled.parquet
```

## Model Training

```bash
# Train both models
python scripts/train_models.py

# Train entry model only
python scripts/train_models.py entry

# Train exit model only
python scripts/train_models.py exit
```

Output:
- `models/entry_model.txt` + `models/entry_model_norm.txt`
- `models/exit_model.txt` + `models/exit_model_norm.txt`

## Running the Bot

```bash
# Build
cargo build --release --bin momentum_bot

# Dry run
DRY_RUN=true ./target/release/momentum_bot

# Live
DRY_RUN=false POSITION_SIZE=10 ./target/release/momentum_bot
```

## Performance Targets

| Metric | Target |
|--------|--------|
| Tick processing | <1ms |
| Signal decision | <5ms |
| Order submission | <50ms |
| Total latency | <100ms |

## Mathematical Framework

### Why Prices Move Without BTC Moving

As τ → 0 with fixed displacement:
```
Z_t = (S_t - S_0) / (σ√τ) → ±∞
P_t = Φ(Z_t) → 0 or 1
```

This is **deterministic probability collapse**, not noise.

### Irreversible Regime

When |Z| > Z* (~1.2):
- Recovery probability < 12%
- Signal flips are noise
- Hold position to resolution

### Rate of Probability Decay

```
∂P/∂τ ∝ (S_0 - S_t) / (σ τ^{3/2})
```

Accelerates as τ → 0 (gamma explosion analog).

## Files

```
src/bin/
├── momentum_strategy.rs   # Core math: MarketState, Entry/Exit models
├── momentum_bot.rs        # Main bot with tick processing

scripts/
├── collect_training_data.py  # Tick data collection
├── train_models.py           # Model training

models/
├── entry_model.txt           # Spike prediction model
├── entry_model_norm.txt      # Entry normalization
├── exit_model.txt            # Continuation prediction model
├── exit_model_norm.txt       # Exit normalization
```
