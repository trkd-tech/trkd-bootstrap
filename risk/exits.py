"""
risk/exits.py

Centralized exit & risk management engine.

Responsibilities:
- Monitor open positions
- Trigger exits based on:
    - VWAP recross
    - Trailing Stop Loss
    - Time-based exit (hard stop)

This module MUST:
- Never enter trades
- Never compute indicators
- Never talk to Kite APIs
- Only decide WHEN to exit
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================
# EXIT CONFIG DEFAULTS
# ============================================================

DEFAULT_TRAIL_POINTS = {
    "NIFTY": 40,
    "BANKNIFTY": 120
}

HARD_EXIT_TIME = datetime.strptime("15:20", "%H:%M").time()

# ============================================================
# CORE EXIT CHECK
# ============================================================

def evaluate_exits(
    *,
    token,
    candle,
    vwap_state,
    positions,
    token_meta,
    exit_position,
    live_engine=None
):
    # 🔒 Sync with Kite before exits
    if live_engine:
        live_engine.sync()

    for key, pos in list(positions.items()):
        if not pos.get("open"):
            continue

        if pos["token"] != token:
            continue

        # Example: VWAP recross exit
        vwap = vwap_state[token]["vwap"]
        close = candle["close"]

        if pos["direction"] == "LONG" and close < vwap:
            exit_position(
                positions,
                key,
                close,
                reason="VWAP_RECROSS"
            )
    
    """
    Evaluate ALL exit conditions for a token on a closed candle.

    This function is idempotent and safe to call repeatedly.
    """

    pos = positions.get(token)
    if not pos or not pos.get("open"):
        return

    # --------------------------------------------------------
    # 1. VWAP RECROSS EXIT
    # --------------------------------------------------------
    _check_vwap_recross(
        token=token,
        candle=candle,
        vwap_state=vwap_state,
        positions=positions,
        exit_position=exit_position
    )

    # --------------------------------------------------------
    # 2. TRAILING STOP LOSS
    # --------------------------------------------------------
    _check_trailing_sl(
        token=token,
        candle=candle,
        positions=positions,
        token_meta=token_meta,
        exit_position=exit_position
    )

    # --------------------------------------------------------
    # 3. TIME EXIT
    # --------------------------------------------------------
    _check_time_exit(
        token=token,
        candle=candle,
        positions=positions,
        exit_position=exit_position
    )

# ============================================================
# EXIT RULES
# ============================================================

def _check_vwap_recross(
    *,
    token,
    candle,
    vwap_state,
    positions,
    exit_position
):
    pos = positions.get(token)
    if not pos or not pos.get("open"):
        return

    vwap = vwap_state.get(token, {}).get("vwap")
    if not vwap:
        return

    close = candle["close"]

    if pos["direction"] == "LONG" and close < vwap:
        logger.info(
            f"EXIT SIGNAL | VWAP RECROSS | token={token} | close={close} < VWAP={round(vwap,2)}"
        )
        exit_position(positions, token, close, "VWAP_RECROSS")

    elif pos["direction"] == "SHORT" and close > vwap:
        logger.info(
            f"EXIT SIGNAL | VWAP RECROSS | token={token} | close={close} > VWAP={round(vwap,2)}"
        )
        exit_position(positions, token, close, "VWAP_RECROSS")


def _check_trailing_sl(
    *,
    token,
    candle,
    positions,
    token_meta,
    exit_position
):
    pos = positions.get(token)
    if not pos or not pos.get("open"):
        return

    index = token_meta[token]["index"]
    trail_points = DEFAULT_TRAIL_POINTS.get(index)
    if not trail_points:
        return

    close = candle["close"]

    # Initialize best price if missing
    best = pos.setdefault("best_price", pos["entry_price"])

    if pos["direction"] == "LONG":
        pos["best_price"] = max(best, close)
        if pos["best_price"] - close >= trail_points:
            logger.info(
                f"EXIT SIGNAL | TRAIL SL | token={token} | "
                f"best={pos['best_price']} close={close}"
            )
            exit_position(positions, token, close, "TRAIL_SL")

    elif pos["direction"] == "SHORT":
        pos["best_price"] = min(best, close)
        if close - pos["best_price"] >= trail_points:
            logger.info(
                f"EXIT SIGNAL | TRAIL SL | token={token} | "
                f"best={pos['best_price']} close={close}"
            )
            exit_position(positions, token, close, "TRAIL_SL")


def _check_time_exit(
    *,
    token,
    candle,
    positions,
    exit_position
):
    pos = positions.get(token)
    if not pos or not pos.get("open"):
        return

    candle_time = candle["start"].time()

    if candle_time >= HARD_EXIT_TIME:
        logger.info(
            f"EXIT SIGNAL | TIME EXIT | token={token} | time={candle_time}"
        )
        exit_position(
            positions,
            token,
            candle["close"],
            "TIME_EXIT"
        )
