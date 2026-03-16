# Polymarket Stop-Loss / Take-Profit Manager

A terminal-based (TUI) position manager for [Polymarket](https://polymarket.com) that lets you set **stop-loss** and **take-profit** triggers on your open positions with automatic order execution.

Also includes ML-enhanced trading bots for BTC hourly prediction markets.

![Rust](https://img.shields.io/badge/Rust-000000?style=flat&logo=rust&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Features

### SL/TP Position Manager (`sl_tp_bot`)
- **Auto-scans** your wallet for all open Polymarket positions
- **Real-time orderbook** monitoring via WebSocket for live prices
- **Stop-Loss / Take-Profit** — set price triggers per position, auto-sells when hit
- **Force Sell** — instantly sell any position at best bid
- **TUI Interface** — navigate positions, set triggers, view logs in terminal
- **SQLite persistence** — SL/TP settings survive restarts
- **Dry-run mode** — test safely without executing real orders (default)

### ML Trading Bot (`ml_bot_tui`)
- **Z-score strategy** for BTC 1-hour up/down prediction markets
- **Multi-exchange signals** from Binance, Kraken, Coinbase, Bybit, Bitfinex
- **Zero-allocation hot path** — `memchr` byte parsing for Binance ticks (~50/sec)
- **Pre-signed order caching** for low-latency execution
- **Auto market discovery** — finds active BTC hourly markets automatically

### Momentum Bot (`momentum_bot`)
- Momentum-based strategy using price velocity and acceleration signals

## Prerequisites

- **Rust** (1.70+ recommended)
- **Polymarket account** with a funded proxy wallet
- Your **private key** and **funder (proxy) wallet address**

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/PolyConsensus/Polymarket-Stop-Loss-Take-Profit-Manager.git
cd Polymarket-Stop-Loss-Take-Profit-Manager
```

### 2. Create a `.env` file

```bash
POLYMARKET_PRIVATE_KEY=your_private_key_here
POLYMARKET_FUNDER=your_proxy_wallet_address_here
```

> **Never commit your `.env` file.** It's already in `.gitignore`.

### 3. Build

```bash
cargo build --release --bin sl_tp_bot    # SL/TP Manager
cargo build --release --bin ml_bot_tui   # ML Trading Bot
cargo build --release --bin momentum_bot # Momentum Bot
```

### 4. Run

```bash
# SL/TP Manager (safe mode — no real orders)
DRY_RUN=true ./target/release/sl_tp_bot

# ML Trading Bot (safe mode)
DRY_RUN=true ./target/release/ml_bot_tui

# Or use the start script for tmux deployment:
./start_bot.sh
```

## Keybindings — SL/TP Manager

| Key | Action |
|-----|--------|
| `q` | Quit |
| `↑`/`↓` or `j`/`k` | Navigate positions |
| `s` | Set stop-loss for selected position |
| `t` | Set take-profit for selected position |
| `x` | Clear SL/TP for selected position |
| `f` | Force sell selected position at best bid |
| `d` | Toggle dry-run mode |
| `r` | Force refresh positions from API |
| `Enter` | Confirm input |
| `Esc` | Cancel input |

## Keybindings — ML Bot

| Key | Action |
|-----|--------|
| `q` | Quit |
| `d` | Toggle dry-run mode |
| `+`/`-` | Adjust tick threshold |
| `m` | Cycle MIN_VOLATILITY_USD (5→10→15→0) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLYMARKET_PRIVATE_KEY` | — | Your wallet private key (also accepts `PM_PRIVATE_KEY`) |
| `POLYMARKET_FUNDER` | — | Your proxy wallet address (also accepts `PM_FUNDER`) |
| `DRY_RUN` | `true` | Set to `false` to enable live order execution |
| `POSITION_SIZE` | `30` | Shares per trade (ML bot) |
| `MIN_VOLATILITY_USD` | `5.0` | ML filter threshold |
| `TICK_THRESHOLD` | `4` | Net ticks required for entry (ML bot) |
| `HOLD_TIMEOUT_MS` | `7500` | Position exit timeout (ML bot) |
| `MAX_TRADES` | `20` | Max trades per 1-hour session (ML bot) |

## Architecture

```
sl_tp_bot:
  Polymarket Data API → fetch positions → TUI display
  Orderbook WS → real-time price updates → SL/TP trigger check → sell orders

ml_bot_tui:
  Binance WS → parse_binance_trade_fast() → IntraWindowState (Z-score)
            → Multi-exchange signals (Kraken/Coinbase/Bybit/Bitfinex)
            → Strategy filter → Pre-signed orders → Polymarket
```

### Key Design Decisions
- **`rust_decimal::Decimal`** for all financial math — never `f64` for money
- **`tokio::join!`** for parallel async operations
- **Lock discipline** — release `RwLock` before any network call
- **SQLite** for trade history and SL/TP persistence

## Database

Trades and SL/TP settings are saved to SQLite (`data/sl_tp_data.db`):
- Position details, entry prices, SL/TP levels
- Trade execution history with timestamps
- Auto-migrates schema on startup

## Disclaimer

This software is provided as-is for educational and personal use. Trading on prediction markets involves financial risk. Always test in dry-run mode first. The authors are not responsible for any financial losses incurred through the use of this software.

## License

MIT
