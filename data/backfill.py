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

from datetime import datetime, timedelta, timezone

# ============================================================
# IST HANDLING
# ============================================================

IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)

OR_START = datetime.strptime("09:15", "%H:%M").time()
OR_END   = datetime.strptime("09:45", "%H:%M").time()

# ============================================================
# VWAP BACKFILL
# ============================================================

def backfill_vwap(kite, token, vwap_state):
    """
    Backfill session VWAP using historical 5-minute candles.

    Populates:
    vwap_state[token] = {cum_pv, cum_vol, vwap}
    """
    now_ist = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(IST)

    # Market safety
    if now_ist.time() < OR_START:
        print(
            f"VWAP BACKFILL SKIPPED | token={token} | "
            f"market not started | now={now_ist.time()}"
        )
        return

    today = now_ist.date()

    from_dt = datetime.combine(today, OR_START, tzinfo=IST)
    to_dt   = now_ist

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=to_dt,
        interval="5minute"
    )

    if not candles:
        print(f"VWAP BACKFILL FAILED | token={token} | no candles")
        return

    cum_pv = 0.0
    cum_vol = 0

    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        vol = c["volume"]

        cum_pv += tp * vol
        cum_vol += vol

    if cum_vol == 0:
        print(f"VWAP BACKFILL FAILED | token={token} | zero volume")
        return

    vwap_state[token] = {
        "cum_pv": cum_pv,
        "cum_vol": cum_vol,
        "vwap": cum_pv / cum_vol
    }

    print(
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
    """
    if token in opening_range and opening_range[token].get("finalized"):
        return

    today = datetime.utcnow().replace(
        tzinfo=timezone.utc
    ).astimezone(IST).date()

    from_dt = datetime.combine(today, OR_START, tzinfo=IST)
    to_dt   = datetime.combine(today, OR_END, tzinfo=IST)

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=to_dt,
        interval="5minute"
    )

    if not candles:
        print(f"OR BACKFILL FAILED | token={token} | no candles")
        return

    opening_range[token] = {
        "high": max(c["high"] for c in candles),
        "low":  min(c["low"] for c in candles),
        "finalized": True
    }

    print(
        f"OPENING RANGE BACKFILLED | token={token} | "
        f"H={opening_range[token]['high']} "
        f"L={opening_range[token]['low']} | "
        f"candles={len(candles)}"
    )
