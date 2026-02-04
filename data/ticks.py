# data/ticks.py
"""
Tick ingestion layer (Track A)

Responsibilities:
- Connect to Kite WebSocket
- Receive live ticks
- Aggregate ticks → 1-minute candles
- Detect minute close and trigger downstream aggregation

This module MUST remain strategy-agnostic.
"""

from datetime import timedelta
import logging
from kiteconnect import KiteTicker

from data.time_utils import normalize_ist_naive

logger = logging.getLogger(__name__)

last_cum_volume = {}

def start_kite_ticker(
    api_key,
    access_token,
    tokens,
    on_tick_callback
):
    """
    Starts Kite WebSocket and forwards ticks to callback.

    on_tick_callback(tick) must be non-blocking.
    """

    kws = KiteTicker(
        api_key=api_key,
        access_token=access_token
    )

    def on_connect(ws, response):
        logger.info("Kite WebSocket connected")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_ticks(ws, ticks):
        for tick in ticks:
            try:
                on_tick_callback(tick)
            except Exception:
                logger.exception("Tick processing failed (non-fatal)")

    def on_close(ws, code, reason):
        logger.warning(f"Kite WebSocket closed | {code} | {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close

    kws.connect(threaded=True)


def process_tick_to_1m(
    tick,
    candles_1m,
    last_minute_seen,
    on_minute_close
):
    """
    Aggregates live ticks into 1-minute candles.

    on_minute_close(token, closed_minute) is called exactly once per minute.
    """

    if "exchange_timestamp" not in tick or tick.get("last_price") is None:
        return

    token = tick["instrument_token"]
    ts = normalize_ist_naive(tick["exchange_timestamp"]).replace(second=0, microsecond=0)
    price = tick["last_price"]

    key = (token, ts)

    cum_vol = tick.get("volume_traded")
    if cum_vol is None:
        return

    prev_cum = last_cum_volume.get(token)
    if prev_cum is None:
        delta_vol = 0
    else:
        delta_vol = max(cum_vol - prev_cum, 0)

    last_cum_volume[token] = cum_vol

    candle = candles_1m.get(key)
    if candle is None:
        candles_1m[key] = {
            "start": ts,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": delta_vol
        }
    else:
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] += delta_vol

    _detect_minute_close(
        token,
        ts,
        last_minute_seen,
        on_minute_close
    )


def _detect_minute_close(
    token,
    current_minute,
    last_minute_seen,
    on_minute_close
):
    last = last_minute_seen.get(token)

    if last and current_minute > last:
        on_minute_close(token, last)

    last_minute_seen[token] = current_minute
