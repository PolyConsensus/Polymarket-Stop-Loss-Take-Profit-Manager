# Hope1h - ML-Enhanced BTC 1-Hour Z-Score Trading Bot

## Architecture

Low-latency Rust bot for BTC **1-hour** up/down prediction markets on Polymarket. Uses Z-score strategy with multi-exchange signals. EVENT-DRIVEN design executes immediately on each Binance tick (~50/sec).

```
Binance WS → parse_binance_trade_fast() → IntraWindowState (Z-score)
          → Multi-exchange signals (Kraken/Coinbase/Bybit/Bitfinex)
          → Strategy filter → Pre-signed orders → Polymarket
```

### Core Components ([src/bin/ml_bot_tui.rs](src/bin/ml_bot_tui.rs))
- **`LiveExecutor`** - Polymarket SDK with pre-signed order caching
- **`IntraWindowState`** - Z-score calculation: normalized displacement from reference price
- **`FeatureEngine`** - 50-tick sliding window computing microstructure features
- **`SharedState`** - Central `RwLock<T>` state shared across async tasks
- **Market Discovery** - Auto-finds `btc-updown-1h-{timestamp}` markets every 5s

### Z-Score Strategy
Z-score = (current_price - reference_price) / (volatility × √time_remaining)
- Measures how far price has moved relative to expected volatility
- Higher |Z| = stronger directional conviction for entry

## Critical Performance Patterns

### 1. Zero-Allocation Hot Path (MUST follow)
Binance tick parsing uses `memchr` byte searching—**NO serde/JSON in hot path**:
```rust
static PRICE_PATTERN: &[u8] = b"\"p\":\"";
fn extract_json_field<'a>(json: &'a [u8], field: &[u8]) -> Option<&'a str>
```

### 2. Lock Discipline
**Release `RwLock` before ANY network call** to avoid blocking tick handler:
```rust
let data = { state.read().await.clone() };  // Lock released at brace
let result = http_call(&data).await;        // Safe to await now
```

### 3. Pre-Signed Orders
Orders pre-signed at 99¢, refreshed every 30s. **Build & sign BOTH sides in parallel**:
```rust
let (up_result, down_result) = tokio::join!(up_build_future, down_build_future);
```

## Build & Run

```bash
cargo build --release --bin ml_bot_tui   # LTO + codegen-units=1
DRY_RUN=true ./target/release/ml_bot_tui # Safe test mode (default)
./start_bot.sh                            # Deploys in tmux session
```

## Environment Variables

| Variable | Default | Notes |
|----------|---------|-------|
| `DRY_RUN` | `true` | `false` enables live orders |
| `POSITION_SIZE` | `30` | Shares per trade |
| `MIN_VOLATILITY_USD` | `5.0` | ML filter threshold |
| `TICK_THRESHOLD` | `4` | Net ticks required for entry |
| `HOLD_TIMEOUT_MS` | `7500` | Position exit timeout |
| `MAX_TRADES` | `20` | Max trades per 1-hour session |
| `POLYMARKET_PRIVATE_KEY` | - | Also accepts `PM_PRIVATE_KEY` |
| `POLYMARKET_FUNDER` | - | Proxy wallet, also `PM_FUNDER` |

## Database

Trades automatically saved to SQLite (`data/trades.db` locally):
- All trade details: side, entry/exit price, PnL, hold time, Z-score
- Indexed by timestamp for fast queries
- Auto-migrates schema for new columns

## Coding Conventions

- **Financial math**: Always `rust_decimal::Decimal`, never `f64` for money
- **Parallel async**: `tokio::join!` for independent operations
- **Log prefixes**: `[OK]` success, `[X]` error, `[!]` warning, `>>>╢` trade action
- **TUI style**: ratatui with `Color::Yellow` borders, `Color::DarkGray` logs
- **Stale detection**: 10s timeout triggers WS reconnect

## TUI Keybindings

| Key | Action |
|-----|--------|
| `q` | Quit |
| `d` | Toggle dry_run mode |
| `+/-` | Adjust tick threshold |
| `m` | Cycle MIN_VOLATILITY_USD (5→10→15→0) |
