# indicators/vwap.py
"""
VWAP Indicator Engine

Responsibilities:
- VWAP backfill from historical candles (Track B)
- Incremental VWAP updates from live candles (Track A)
- Maintain exact cumulative PV / volume state

VWAP is session-based and timeframe-independent
when built from cumulative PV & volume.
"""

import logging
from datetime import datetime, time as dtime

logger = logging.getLogger(__name__)


OR_START = dtime(9, 15)   # VWAP session start


def init_vwap_state(vwap_state, token):
    """
    Ensure VWAP state exists for token.
    """
    vwap_state.setdefault(token, {
        "cum_pv": 0.0,
        "cum_vol": 0,
        "vwap": None
    })


def backfill_vwap_from_candles(
    token,
    candles,
    vwap_state
):
    """
    Backfill VWAP using historical candles.

    candles: list of candle dicts (OHLCV)
    """

    if not candles:
        logger.warning(f"VWAP BACKFILL FAILED | token={token} | no candles")
        return

    cum_pv = 0.0
    cum_vol = 0

    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_pv += tp * c["volume"]
        cum_vol += c["volume"]

    if cum_vol == 0:
        logger.warning(f"VWAP BACKFILL FAILED | token={token} | zero volume")
        return

    vwap_state[token] = {
        "cum_pv": cum_pv,
        "cum_vol": cum_vol,
        "vwap": cum_pv / cum_vol
    }

    logger.info(
        f"VWAP BACKFILL DONE | token={token} | "
        f"VWAP={round(vwap_state[token]['vwap'], 2)} | "
        f"candles={len(candles)}"
    )


def update_vwap_from_candle(
    token,
    candle,
    vwap_state
):
    """
    Incrementally update VWAP using a closed candle.
    """

    init_vwap_state(vwap_state, token)

    # Ignore candles before session start
    if candle["start"].time() < OR_START:
        return

    tp = (candle["high"] + candle["low"] + candle["close"]) / 3
    pv = tp * candle["volume"]

    s = vwap_state[token]
    s["cum_pv"] += pv
    s["cum_vol"] += candle["volume"]

    if s["cum_vol"] > 0:
        s["vwap"] = s["cum_pv"] / s["cum_vol"]

    logger.info(
        f"VWAP UPDATE | token={token} | "
        f"upto={candle['start']} | "
        f"VWAP={round(s['vwap'], 2)}"
    )
