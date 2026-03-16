#!/usr/bin/env python3
"""
Tick-level training data collector for Entry/Exit ML models.

Collects:
1. Binance BTC ticks (price, qty, is_buy, timestamp_us)
2. Polymarket prices every 100ms
3. Labels computed after the fact:
   - spike_up_10s: Did BTC spike up >$20 in next 10s?
   - spike_down_10s: Did BTC spike down >$20 in next 10s?
   - continuation_30s: Did price continue in same direction for 30s?
   - entry_profitable: Would entering here have been profitable?

Output: Parquet files with features and labels for training.
"""

import asyncio
import json
import time
import os
from datetime import datetime, timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List, Deque
import numpy as np
import pandas as pd
import websockets
from scipy.stats import norm

# ============================================================================
# CONFIGURATION
# ============================================================================

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
OUTPUT_DIR = "./training_data"
SAVE_INTERVAL_SECONDS = 300  # Save every 5 minutes

# Feature computation windows (milliseconds)
WINDOW_500MS = 500
WINDOW_1S = 1000
WINDOW_5S = 5000
WINDOW_30S = 30000

# Spike detection thresholds
SPIKE_THRESHOLD_USD = 20.0  # $20 move = spike
CONTINUATION_THRESHOLD = 0.5  # 50% of initial move = continuation

# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class Tick:
    """Single Binance trade tick"""
    timestamp_us: int  # Microseconds since epoch
    price: float
    quantity: float
    is_buy: bool
    
@dataclass 
class WindowState:
    """15-minute Polymarket window state"""
    reference_price: float
    window_start_ms: int
    window_duration_ms: int = 15 * 60 * 1000
    volatility: float = 50.0  # σ in $/√min
    
    def tau_minutes(self, current_ms: int) -> float:
        """Time remaining in minutes"""
        elapsed = current_ms - self.window_start_ms
        remaining = self.window_duration_ms - elapsed
        return max(remaining / 60000.0, 0.01)
    
    def z_score(self, current_price: float, current_ms: int) -> float:
        """Normalized displacement: Z = (S - S0) / (σ√τ)"""
        tau = self.tau_minutes(current_ms)
        displacement = current_price - self.reference_price
        return displacement / (self.volatility * np.sqrt(tau))
    
    def true_prob_up(self, current_price: float, current_ms: int) -> float:
        """P(UP wins) = Φ(Z)"""
        z = self.z_score(current_price, current_ms)
        return norm.cdf(z)
    
    def remaining_pct(self, current_ms: int) -> float:
        """Time remaining as percentage (1.0 = full, 0.0 = expired)"""
        elapsed = current_ms - self.window_start_ms
        return 1.0 - min(elapsed / self.window_duration_ms, 1.0)

@dataclass
class FeatureRow:
    """Single training example with features"""
    timestamp_us: int
    
    # Core features
    z_score: float
    remaining_pct: float
    true_prob_up: float
    
    # Tick momentum (500ms)
    price_change_500ms: float
    volume_imbalance_500ms: float
    tick_rate_500ms: float
    consecutive_buys_500ms: int
    consecutive_sells_500ms: int
    
    # Tick momentum (1s)
    price_change_1s: float
    volume_imbalance_1s: float
    tick_rate_1s: float
    
    # Tick momentum (5s)
    price_change_5s: float
    volume_imbalance_5s: float
    
    # Volatility
    realized_vol_1min: float
    vol_of_vol: float
    
    # Raw
    btc_price: float
    reference_price: float
    
    # Labels (to be filled later)
    price_5s_later: Optional[float] = None
    price_30s_later: Optional[float] = None
    spike_up_10s: Optional[bool] = None
    spike_down_10s: Optional[bool] = None
    continuation_30s: Optional[bool] = None

# ============================================================================
# FEATURE COMPUTATION
# ============================================================================

class FeatureEngine:
    """Computes features from tick stream"""
    
    def __init__(self, max_ticks: int = 10000):
        self.ticks: Deque[Tick] = deque(maxlen=max_ticks)
        self.returns_1min: Deque[float] = deque(maxlen=1000)
        
    def push_tick(self, tick: Tick):
        """Add new tick"""
        self.ticks.append(tick)
        
        # Compute return for vol calculation
        if len(self.ticks) >= 2:
            prev = self.ticks[-2]
            ret = (tick.price - prev.price) / prev.price
            self.returns_1min.append(ret)
    
    def get_ticks_in_window(self, window_ms: int) -> List[Tick]:
        """Get ticks within last window_ms milliseconds"""
        if not self.ticks:
            return []
        
        cutoff_us = self.ticks[-1].timestamp_us - (window_ms * 1000)
        return [t for t in self.ticks if t.timestamp_us >= cutoff_us]
    
    def compute_momentum_features(self, window_ms: int) -> dict:
        """Compute momentum features for a window"""
        ticks = self.get_ticks_in_window(window_ms)
        
        if len(ticks) < 2:
            return {
                'price_change': 0.0,
                'volume_imbalance': 0.0,
                'tick_rate': 0.0,
                'consecutive_buys': 0,
                'consecutive_sells': 0,
            }
        
        # Price change
        price_change = ticks[-1].price - ticks[0].price
        
        # Volume imbalance
        buy_vol = sum(t.quantity for t in ticks if t.is_buy)
        sell_vol = sum(t.quantity for t in ticks if not t.is_buy)
        total_vol = buy_vol + sell_vol
        vol_imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0
        
        # Tick rate
        duration_s = (ticks[-1].timestamp_us - ticks[0].timestamp_us) / 1_000_000
        tick_rate = len(ticks) / duration_s if duration_s > 0 else 0.0
        
        # Consecutive runs
        max_buy_run = 0
        max_sell_run = 0
        buy_run = 0
        sell_run = 0
        
        for t in ticks:
            if t.is_buy:
                buy_run += 1
                sell_run = 0
                max_buy_run = max(max_buy_run, buy_run)
            else:
                sell_run += 1
                buy_run = 0
                max_sell_run = max(max_sell_run, sell_run)
        
        return {
            'price_change': price_change,
            'volume_imbalance': vol_imbalance,
            'tick_rate': tick_rate,
            'consecutive_buys': max_buy_run,
            'consecutive_sells': max_sell_run,
        }
    
    def compute_volatility(self) -> tuple:
        """Compute realized volatility and vol-of-vol"""
        if len(self.returns_1min) < 10:
            return 50.0, 0.0
        
        returns = np.array(self.returns_1min)
        
        # Realized vol (annualized, then scaled to $/√min)
        vol = np.std(returns) * np.sqrt(len(returns))
        
        # Vol of vol (rolling std of vol)
        if len(returns) >= 100:
            window = 20
            rolling_vol = pd.Series(returns).rolling(window).std()
            vol_of_vol = rolling_vol.std()
        else:
            vol_of_vol = 0.0
        
        return vol, vol_of_vol
    
    def compute_features(self, window: WindowState) -> Optional[FeatureRow]:
        """Compute full feature row"""
        if len(self.ticks) < 10:
            return None
        
        current_tick = self.ticks[-1]
        current_price = current_tick.price
        current_ms = current_tick.timestamp_us // 1000
        
        # Core features
        z_score = window.z_score(current_price, current_ms)
        remaining_pct = window.remaining_pct(current_ms)
        true_prob_up = window.true_prob_up(current_price, current_ms)
        
        # Momentum features
        m_500ms = self.compute_momentum_features(500)
        m_1s = self.compute_momentum_features(1000)
        m_5s = self.compute_momentum_features(5000)
        
        # Volatility
        vol, vol_of_vol = self.compute_volatility()
        
        return FeatureRow(
            timestamp_us=current_tick.timestamp_us,
            z_score=z_score,
            remaining_pct=remaining_pct,
            true_prob_up=true_prob_up,
            price_change_500ms=m_500ms['price_change'],
            volume_imbalance_500ms=m_500ms['volume_imbalance'],
            tick_rate_500ms=m_500ms['tick_rate'],
            consecutive_buys_500ms=m_500ms['consecutive_buys'],
            consecutive_sells_500ms=m_500ms['consecutive_sells'],
            price_change_1s=m_1s['price_change'],
            volume_imbalance_1s=m_1s['volume_imbalance'],
            tick_rate_1s=m_1s['tick_rate'],
            price_change_5s=m_5s['price_change'],
            volume_imbalance_5s=m_5s['volume_imbalance'],
            realized_vol_1min=vol,
            vol_of_vol=vol_of_vol,
            btc_price=current_price,
            reference_price=window.reference_price,
        )

# ============================================================================
# LABEL COMPUTATION
# ============================================================================

def compute_labels(features: List[FeatureRow], ticks: List[Tick]) -> List[FeatureRow]:
    """
    Compute labels for training examples using future data.
    Must be done AFTER data collection.
    """
    # Build price lookup by timestamp
    price_lookup = {t.timestamp_us: t.price for t in ticks}
    timestamps = sorted(price_lookup.keys())
    
    for row in features:
        ts = row.timestamp_us
        
        # Find price 5s later
        target_5s = ts + 5_000_000
        idx_5s = np.searchsorted(timestamps, target_5s)
        if idx_5s < len(timestamps):
            row.price_5s_later = price_lookup[timestamps[idx_5s]]
        
        # Find price 30s later
        target_30s = ts + 30_000_000
        idx_30s = np.searchsorted(timestamps, target_30s)
        if idx_30s < len(timestamps):
            row.price_30s_later = price_lookup[timestamps[idx_30s]]
        
        # Detect spikes in next 10s
        target_10s = ts + 10_000_000
        window_start = np.searchsorted(timestamps, ts)
        window_end = np.searchsorted(timestamps, target_10s)
        
        if window_end > window_start:
            prices_10s = [price_lookup[timestamps[i]] for i in range(window_start, min(window_end, len(timestamps)))]
            if prices_10s:
                max_price = max(prices_10s)
                min_price = min(prices_10s)
                current = row.btc_price
                
                row.spike_up_10s = (max_price - current) > SPIKE_THRESHOLD_USD
                row.spike_down_10s = (current - min_price) > SPIKE_THRESHOLD_USD
        
        # Continuation in 30s
        if row.price_30s_later is not None:
            initial_dir = np.sign(row.z_score) if abs(row.z_score) > 0.1 else 0
            price_change = row.price_30s_later - row.btc_price
            
            if initial_dir != 0:
                # Did price continue in same direction?
                row.continuation_30s = np.sign(price_change) == initial_dir
            else:
                row.continuation_30s = None
    
    return features

# ============================================================================
# DATA COLLECTOR
# ============================================================================

class DataCollector:
    """Collects tick data and computes features"""
    
    def __init__(self):
        self.engine = FeatureEngine()
        self.raw_ticks: List[Tick] = []
        self.features: List[FeatureRow] = []
        self.current_window: Optional[WindowState] = None
        self.last_save_time = time.time()
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    def process_tick(self, data: dict):
        """Process incoming Binance tick"""
        tick = Tick(
            timestamp_us=data['T'] * 1000,  # Convert ms to us
            price=float(data['p']),
            quantity=float(data['q']),
            is_buy=not data['m'],  # m=true means buyer is maker = seller aggressor
        )
        
        self.raw_ticks.append(tick)
        self.engine.push_tick(tick)
        
        # Create window if needed (simulate 15-min windows)
        if self.current_window is None:
            # Start new window aligned to 15-min boundary
            now_ms = tick.timestamp_us // 1000
            window_start = (now_ms // (15 * 60 * 1000)) * (15 * 60 * 1000)
            self.current_window = WindowState(
                reference_price=tick.price,
                window_start_ms=window_start,
            )
            print(f"[*] New window started: ref={tick.price:.2f}")
        
        # Check if window expired
        now_ms = tick.timestamp_us // 1000
        if self.current_window.remaining_pct(now_ms) <= 0:
            # Start new window
            self.current_window = WindowState(
                reference_price=tick.price,
                window_start_ms=now_ms,
            )
            print(f"[*] New window started: ref={tick.price:.2f}")
        
        # Compute features every 100 ticks
        if len(self.raw_ticks) % 100 == 0:
            features = self.engine.compute_features(self.current_window)
            if features:
                self.features.append(features)
        
        # Save periodically
        if time.time() - self.last_save_time > SAVE_INTERVAL_SECONDS:
            self.save_data()
    
    def save_data(self):
        """Save collected data to parquet"""
        if not self.features:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save features
        df = pd.DataFrame([vars(f) for f in self.features])
        features_path = f"{OUTPUT_DIR}/features_{timestamp}.parquet"
        df.to_parquet(features_path)
        print(f"[*] Saved {len(df)} features to {features_path}")
        
        # Save raw ticks
        ticks_df = pd.DataFrame([
            {'timestamp_us': t.timestamp_us, 'price': t.price, 'quantity': t.quantity, 'is_buy': t.is_buy}
            for t in self.raw_ticks
        ])
        ticks_path = f"{OUTPUT_DIR}/ticks_{timestamp}.parquet"
        ticks_df.to_parquet(ticks_path)
        print(f"[*] Saved {len(ticks_df)} ticks to {ticks_path}")
        
        # Reset
        self.features = []
        self.raw_ticks = []
        self.last_save_time = time.time()

async def collect_data():
    """Main data collection loop"""
    collector = DataCollector()
    
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║         TICK DATA COLLECTOR v0.1.0                            ║")
    print("║         Collecting training data for Entry/Exit models        ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    print(f"[*] Output directory: {OUTPUT_DIR}")
    print(f"[*] Save interval: {SAVE_INTERVAL_SECONDS}s")
    print("[*] Connecting to Binance...")
    
    reconnect_delay = 1
    
    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                print("[OK] Connected to Binance WebSocket")
                reconnect_delay = 1
                
                tick_count = 0
                start_time = time.time()
                
                async for msg in ws:
                    data = json.loads(msg)
                    collector.process_tick(data)
                    
                    tick_count += 1
                    if tick_count % 10000 == 0:
                        elapsed = time.time() - start_time
                        rate = tick_count / elapsed
                        print(f"[*] Processed {tick_count} ticks ({rate:.1f}/sec), {len(collector.features)} features")
        
        except Exception as e:
            print(f"[X] WebSocket error: {e}")
            print(f"[*] Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

def compute_labels_offline(features_file: str, ticks_file: str, output_file: str):
    """Compute labels for a collected dataset (offline processing)"""
    print(f"[*] Loading {features_file}...")
    features_df = pd.read_parquet(features_file)
    
    print(f"[*] Loading {ticks_file}...")
    ticks_df = pd.read_parquet(ticks_file)
    
    # Convert to objects
    features = [FeatureRow(**row) for _, row in features_df.iterrows()]
    ticks = [Tick(**row) for _, row in ticks_df.iterrows()]
    
    print(f"[*] Computing labels for {len(features)} examples...")
    features = compute_labels(features, ticks)
    
    # Save
    df = pd.DataFrame([vars(f) for f in features])
    df.to_parquet(output_file)
    print(f"[OK] Saved labeled data to {output_file}")
    
    # Stats
    if 'spike_up_10s' in df.columns:
        spike_up_pct = df['spike_up_10s'].mean() * 100
        spike_down_pct = df['spike_down_10s'].mean() * 100
        print(f"[*] Spike up rate: {spike_up_pct:.1f}%")
        print(f"[*] Spike down rate: {spike_down_pct:.1f}%")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "label":
        # Offline label computation
        if len(sys.argv) != 5:
            print("Usage: python collect_training_data.py label <features.parquet> <ticks.parquet> <output.parquet>")
            sys.exit(1)
        compute_labels_offline(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        # Live data collection
        asyncio.run(collect_data())
