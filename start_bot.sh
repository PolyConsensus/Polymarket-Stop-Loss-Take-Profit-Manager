#!/bin/bash
# Start the ML bot in a tmux session
# Usage: ./start_bot.sh

# Change to your project directory
cd "$(dirname "$0")"

# Load environment variables from .env
source .env
export POLYMARKET_PRIVATE_KEY POLYMARKET_FUNDER

# Kill existing session if running
tmux kill-session -t hope1h_bot 2>/dev/null

# Start in a new tmux session
tmux new-session -d -s hope1h_bot -x 120 -y 40 \
  "RUST_BACKTRACE=1 DRY_RUN=true POSITION_SIZE=30 \
   POLYMARKET_PRIVATE_KEY=$POLYMARKET_PRIVATE_KEY \
   POLYMARKET_FUNDER=$POLYMARKET_FUNDER \
   ./target/release/ml_bot_tui 2>&1 | tee bot_output.log; \
   echo 'Bot exited at $(date)' >> bot_crash.log"

echo 'Bot started in tmux session: hope1h_bot'
echo 'Attach with: tmux attach -t hope1h_bot'
