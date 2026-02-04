"""
candles.py

Track A: Live ticks → 1-minute → 5-minute candles

Responsibilities:
- Build 1-minute candles from ticks
- Detect minute close
- Aggregate 5-minute candles
- Enforce time integrity (IST only)

This module MUST remain:
- Stateless with respect to strategies
- Free of execution logic
- Free of indicator math

Downstream consumers:
- indicators/
- strategies/
"""

from datetime import timedelta

# ============================================================
# STATE (owned by data layer)
# ============================================================

# (token, minute_start) -> 1m candle
candles_1m = {}

# (token, five_min_start) -> 5m candle
candles_5m = {}

# token -> last seen minute (datetime)
last_minute_seen = {}

# ============================================================
# 1-MIN CANDLE BUILDER
# ============================================================

def process_tick_to_1m(tick):
    """
    Build / update 1-minute candles from live ticks.
    """
    if "exchange_timestamp" not in tick:
        return

    price = tick.get("last_price")
    if price is None:
        return

    token = tick["instrument_token"]

    # IMPORTANT:
    # Kite exchange_timestamp is already IST.
    minute = tick["exchange_timestamp"].replace(second=0, microsecond=0)

    key = (token, minute)

    candle = candles_1m.get(key)
    if candle is None:
        candle = {
            "start": minute,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": tick.get("volume_traded", 0),
        }
        candles_1m[key] = candle
    else:
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] = tick.get("volume_traded", candle["volume"])

    detect_minute_close(token, minute)


# ============================================================
# MINUTE CLOSE DETECTION
# ============================================================

def detect_minute_close(token, current_minute):
    """
    Detect when a 1-minute candle closes and trigger 5-minute aggregation.
    """
    last = last_minute_seen.get(token)

    if last and current_minute > last:
        aggregate_5m(token, last)

    last_minute_seen[token] = current_minute


# ============================================================
# 5-MIN CANDLE AGGREGATION
# ============================================================

def aggregate_5m(token, closed_minute):
    """
    Aggregate five completed 1-minute candles into one 5-minute candle.
    """
    five_start = closed_minute.replace(
        minute=(closed_minute.minute // 5) * 5,
        second=0,
        microsecond=0,
    )

    key = (token, five_start)
    if key in candles_5m:
        return

    minutes = [
        five_start + timedelta(minutes=i)
        for i in range(5)
    ]

    parts = [candles_1m.get((token, m)) for m in minutes]
    if any(p is None for p in parts):
        return  # wait until all 5 exist

    candle = {
        "start": five_start,
        "open": parts[0]["open"],
        "high": max(p["high"] for p in parts),
        "low": min(p["low"] for p in parts),
        "close": parts[-1]["close"],
        "volume": sum(p["volume"] for p in parts),
    }

    # ========================================================
    # HARD SAFETY: Ensure IST candles only
    # ========================================================
    assert candle["start"].hour >= 9, (
        f"Non-IST candle detected: {candle['start']}"
    )

    candles_5m[key] = candle

    return candle
