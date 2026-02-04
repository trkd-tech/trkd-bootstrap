# data/candles.py
"""
Candle aggregation layer

Responsibilities:
- Aggregate 1-minute candles → 5-minute candles
- Ensure exactly-once 5m close
- Emit closed 5m candles via callback

This module must remain indicator- and strategy-agnostic.
"""

from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


def aggregate_5m_from_1m(
    token,
    closed_minute,
    candles_1m,
    candles_5m,
    on_5m_close
):
    """
    Builds a 5-minute candle once all 5 underlying 1-minute candles exist.

    on_5m_close(token, candle_5m) is called exactly once per 5-minute window.
    """

    five_start = closed_minute.replace(
        minute=(closed_minute.minute // 5) * 5,
        second=0,
        microsecond=0
    )

    key = (token, five_start)

    if key in candles_5m:
        return

    minutes = [
        five_start + timedelta(minutes=i)
        for i in range(5)
    ]

    parts = [
        candles_1m.get((token, m))
        for m in minutes
    ]

    if any(p is None for p in parts):
        return  # wait for all 5 candles

    candle = {
        "start": five_start,
        "open": parts[0]["open"],
        "high": max(p["high"] for p in parts),
        "low": min(p["low"] for p in parts),
        "close": parts[-1]["close"],
        "volume": sum(p["volume"] for p in parts),
    }

    candles_5m[key] = candle

    logger.info(
        f"5M CLOSED | token={token} | {five_start} | "
        f"O={candle['open']} H={candle['high']} "
        f"L={candle['low']} C={candle['close']} V={candle['volume']}"
    )

    on_5m_close(token, candle)
