"""
backfill.py

Track B: Historical backfill for indicators.

Responsibilities:
- VWAP backfill (session VWAP up to now)
- Opening Range backfill (09:15–09:45)
- IST safety validation

This module MUST:
- Never place trades
- Never evaluate strategies
- Only prepare indicator state
"""

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ============================================================
# IST HANDLING
# ============================================================

IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)

OR_START = datetime.strptime("09:15", "%H:%M").time()
OR_END   = datetime.strptime("09:45", "%H:%M").time()


def now_ist():
    return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(IST)

# ============================================================
# VWAP BACKFILL
# ============================================================

def backfill_vwap(kite, token, vwap_state):
    """
    Backfill session VWAP using historical 5-minute candles.

    Populates:
    vwap_state[token] = {cum_pv, cum_vol, vwap}

    Safe to call multiple times (idempotent).
    """

    now = now_ist()

    # Market safety
    if now.time() < OR_START:
        logger.warning(
            f"VWAP BACKFILL SKIPPED | token={token} | "
            f"market not started | now={now.time()}"
        )
        return

    today = now.date()

    from_dt = datetime.combine(today, OR_START, tzinfo=IST)

    # 🔒 CRITICAL ASSERT — catches UTC / naive datetime bugs
    assert from_dt.tzinfo == IST, "VWAP backfill datetime is not IST!"

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=now,
        interval="5minute"
    )

    if not candles:
        logger.warning(f"VWAP BACKFILL FAILED | token={token} | no candles")
        return

    cum_pv = 0.0
    cum_vol = 0

    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        vol = c["volume"]

        cum_pv += tp * vol
        cum_vol += vol

    if cum_vol == 0:
        logger.warning(f"VWAP BACKFILL FAILED | token={token} | zero volume")
        return

    # Overwrite state explicitly (restart-safe)
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


# ============================================================
# OPENING RANGE BACKFILL
# ============================================================

def backfill_opening_range(kite, token, opening_range):
    """
    Backfill Opening Range (09:15–09:45) using historical 5-minute candles.

    Populates:
    opening_range[token] = {high, low, finalized}

    Safe to call multiple times.
    """

    if token in opening_range and opening_range[token].get("finalized"):
        return

    today = now_ist().date()

    from_dt = datetime.combine(today, OR_START, tzinfo=IST)
    to_dt   = datetime.combine(today, OR_END, tzinfo=IST)

    # 🔒 IST sanity check
    assert from_dt.tzinfo == IST, "OR backfill datetime is not IST!"

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=to_dt,
        interval="5minute"
    )

    if not candles:
        logger.warning(f"OR BACKFILL FAILED | token={token} | no candles")
        return

    opening_range[token] = {
        "high": max(c["high"] for c in candles),
        "low":  min(c["low"] for c in candles),
        "finalized": True
    }

    logger.info(
        f"OPENING RANGE BACKFILLED | token={token} | "
        f"H={opening_range[token]['high']} "
        f"L={opening_range[token]['low']} | "
        f"candles={len(candles)}"
    )
