"""
engine/runtime.py

Main runtime loop.

Responsibilities:
- Orchestrate Track A (live candles)
- Orchestrate Track B (backfill)
- Route candles to strategies
- Forward signals downstream (execution later)

This file MUST:
- Not contain strategy logic
- Not contain indicator math
"""

import logging
from datetime import timedelta

from engine.strategy_router import route_strategies
from data.time_utils import normalize_ist_naive

logger = logging.getLogger(__name__)

# ============================================================
# RUNTIME STATE (shared)
# ============================================================

candles_1m = {}
candles_5m = {}
last_minute_seen = {}
last_cum_volume = {}

# ============================================================
# TICK → 1M
# ============================================================

def process_tick_to_1m(
    tick,
    *,
    candles_1m,
    last_minute_seen,
    on_5m_close
):
    """
    Convert ticks into 1-minute candles.
    Calls `on_5m_close` when a 5m candle completes.
    """
    if "exchange_timestamp" not in tick:
        return

    token = tick["instrument_token"]
    ts = normalize_ist_naive(tick["exchange_timestamp"]).replace(second=0, microsecond=0)
    price = tick["last_price"]

    cum_vol = tick.get("volume_traded")
    if cum_vol is None:
        return

    prev_cum = last_cum_volume.get(token)
    if prev_cum is None:
        delta_vol = 0
    else:
        delta_vol = max(cum_vol - prev_cum, 0)

    last_cum_volume[token] = cum_vol

    candle = candles_1m.setdefault((token, ts), {
        "start": ts,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0
    })

    candle["high"] = max(candle["high"], price)
    candle["low"] = min(candle["low"], price)
    candle["close"] = price
    candle["volume"] += delta_vol

    last = last_minute_seen.get(token)
    if last and ts > last:
        _on_1m_close(token, last, candles_1m, on_5m_close)

    last_minute_seen[token] = ts


def _on_1m_close(token, minute, candles_1m, on_5m_close):
    five_start = minute.replace(
        minute=(minute.minute // 5) * 5
    )

    key = (token, five_start)
    if key in candles_5m:
        return

    parts = [
        candles_1m.get((token, five_start + timedelta(minutes=i)))
        for i in range(5)
    ]

    if any(p is None for p in parts):
        return

    candle_5m = {
        "start": five_start,
        "open": parts[0]["open"],
        "high": max(p["high"] for p in parts),
        "low": min(p["low"] for p in parts),
        "close": parts[-1]["close"],
        "volume": sum(p["volume"] for p in parts)
    }

    candles_5m[key] = candle_5m

    logger.info(
        f"5M CLOSED | token={token} | time={five_start}"
    )

    on_5m_close(token, candle_5m)

# ============================================================
# STRATEGY DISPATCH (NEW)
# ============================================================

def on_5m_candle_close(
    *,
    token,
    candle,
    candles_5m,
    vwap_state,
    opening_range,
    strategy_state,
    strategy_config
):
    """
    Entry point for completed 5-minute candles.
    """

    prev_candle = candles_5m.get(
        (token, candle["start"] - timedelta(minutes=5))
    )

    signals = route_strategies(
        token=token,
        candle=candle,
        prev_candle=prev_candle,
        vwap_state=vwap_state,
        opening_range=opening_range,
        strategy_state=strategy_state,
        strategy_config=strategy_config
    )

    for signal in signals:
        logger.info(
            f"SIGNAL EMITTED | {signal['strategy']} | "
            f"token={signal['token']} | "
            f"{signal['direction']} @ {signal['price']}"
        )

    # IMPORTANT:
    # Execution will be plugged in here in the next step
