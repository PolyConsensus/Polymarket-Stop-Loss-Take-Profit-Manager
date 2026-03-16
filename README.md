# Polymarket Stop-Loss / Take-Profit Manager

A terminal-based (TUI) position manager for [Polymarket](https://polymarket.com) that automatically monitors your open positions and executes sell orders when your **stop-loss** or **take-profit** price triggers are hit.

![Rust](https://img.shields.io/badge/Rust-000000?style=flat&logo=rust&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Features

- **Auto-scans** your wallet for all open Polymarket positions
- **Real-time orderbook** monitoring via WebSocket for live mid-prices
- **Stop-Loss** — set a floor price; auto-sells if price drops below it
- **Take-Profit** — set a ceiling price; auto-sells if price rises above it
- **Force Sell** — instantly sell any position at the best bid
- **TUI Interface** — navigate positions, set triggers, and view logs in the terminal
- **SQLite persistence** — SL/TP settings and trade history survive restarts
- **Dry-run mode** — enabled by default, test safely without executing real orders

## Prerequisites

- **Rust** 1.70+
- A **Polymarket** account with a funded proxy wallet
- Your **private key** and **funder (proxy wallet) address**

## Setup

### 1. Clone

```bash
git clone https://github.com/PolyConsensus/Polymarket-Stop-Loss-Take-Profit-Manager.git
cd Polymarket-Stop-Loss-Take-Profit-Manager
```

### 2. Create a `.env` file

```bash
POLYMARKET_PRIVATE_KEY=your_private_key_here
POLYMARKET_FUNDER=your_proxy_wallet_address_here
```

> **Never commit your `.env` file.** It is already in `.gitignore`.

### 3. Build

```bash
cargo build --release --bin sl_tp_bot
```

### 4. Run

```bash
# Dry-run mode (default) — no real orders
./target/release/sl_tp_bot

# Live mode — real orders will be placed
DRY_RUN=false ./target/release/sl_tp_bot
```

## Keybindings

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

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLYMARKET_PRIVATE_KEY` | — | Your wallet private key (also accepts `PM_PRIVATE_KEY`) |
| `POLYMARKET_FUNDER` | — | Your proxy wallet address (also accepts `PM_FUNDER`) |
| `DRY_RUN` | `true` | Set to `false` to enable live order execution |

## How It Works

```
1. Connects to Polymarket Data API → fetches your open positions
2. Subscribes to orderbook WebSocket → real-time price updates
3. Checks each position against your SL/TP triggers every tick
4. When triggered → sells at best bid via Polymarket CLOB API
```

- All financial math uses `rust_decimal::Decimal` — never floating point
- SL/TP settings are persisted in SQLite (`data/sl_tp_data.db`)
- Positions auto-refresh every 60 seconds

## Disclaimer

This software is provided as-is for educational and personal use. Trading on prediction markets involves financial risk. Always test in dry-run mode first. The authors are not responsible for any financial losses incurred through the use of this software.

## License

MIT
