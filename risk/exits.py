# risk/exits.py

"""
Centralized exit & risk management engine.

Responsibilities:
- Monitor open positions
- Decide WHEN to exit based on:
    - VWAP recross
    - Trailing Stop Loss
    - Time-based hard exit

This module MUST:
- Never enter trades
- Never place orders
- Never talk to Kite APIs
- Only emit exit decisions
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================
# CONSTANTS
# ============================================================

HARD_EXIT_TIME = datetime.strptime("15:20", "%H:%M").time()

# ============================================================
# PUBLIC API
# ============================================================

def evaluate_exits(
    *,
    candle,
    vwap_state,
    positions,
    token_meta,
    execution_config,
    exit_paper_position,
    exit_live_position,
    live_engine=None
):
    """
    Evaluate ALL exit conditions for ALL open positions
    on a completed candle.

    Safe to call repeatedly (idempotent).
    """

    # --------------------------------------------------------
    # SAFETY: Sync with Kite before any exit (live only)
    # --------------------------------------------------------
    if live_engine:
        live_engine.sync_positions_from_kite()

    for (strategy, instrument_token), pos in list(positions.items()):
        if not pos.get("open"):
            continue

        index = pos.get("index")
        direction = pos.get("direction")

        if not index or not direction:
            continue

        exec_key = (strategy.upper(), index.upper(), direction.upper())
        exec_cfg = execution_config.get(exec_key, {})

        # ----------------------------------------------------
        # 1. VWAP RECROSS EXIT
        # ----------------------------------------------------
        if _check_vwap_recross(
            candle=candle,
            pos=pos,
            vwap_state=vwap_state,
            instrument_token=instrument_token
        ):
            _exit_position(
                pos=pos,
                instrument_token=instrument_token,
                reason="VWAP_RECROSS",
                exit_paper_position=exit_paper_position,
                exit_live_position=exit_live_position
            )
            continue

        # ----------------------------------------------------
        # 2. TRAILING STOP LOSS
        # ----------------------------------------------------
        if exec_cfg.get("trailing_sl_enabled"):
            if _check_trailing_sl(
                candle=candle,
                pos=pos,
                trail_points=exec_cfg.get("trailing_sl_points", 0)
            ):
                _exit_position(
                    pos=pos,
                    instrument_token=instrument_token,
                    reason="TRAIL_SL",
                    exit_paper_position=exit_paper_position,
                    exit_live_position=exit_live_position
                )
                continue

        # ----------------------------------------------------
        # 3. TIME EXIT (HARD STOP)
        # ----------------------------------------------------
        if candle["start"].time() >= HARD_EXIT_TIME:
            _exit_position(
                pos=pos,
                instrument_token=instrument_token,
                reason="TIME_EXIT",
                exit_paper_position=exit_paper_position,
                exit_live_position=exit_live_position
            )

# ============================================================
# EXIT RULES (PURE LOGIC)
# ============================================================

def _check_vwap_recross(*, candle, pos, vwap_state, instrument_token):
    token = instrument_token
    vwap = vwap_state.get(token, {}).get("vwap")
    if not vwap:
        return False

    close = candle["close"]

    if pos["direction"] == "LONG" and close < vwap:
        logger.info(
            f"EXIT SIGNAL | VWAP RECROSS | {pos['tradingsymbol']} | close={close} < VWAP={round(vwap,2)}"
        )
        return True

    if pos["direction"] == "SHORT" and close > vwap:
        logger.info(
            f"EXIT SIGNAL | VWAP RECROSS | {pos['tradingsymbol']} | close={close} > VWAP={round(vwap,2)}"
        )
        return True

    return False


def _check_trailing_sl(*, candle, pos, trail_points):
    if not trail_points or trail_points <= 0:
        return False

    close = candle["close"]
    best = pos.setdefault("best_price", pos["entry_price"])

    if pos["direction"] == "LONG":
        pos["best_price"] = max(best, close)
        if pos["best_price"] - close >= trail_points:
            logger.info(
                f"EXIT SIGNAL | TRAIL SL | {pos['tradingsymbol']} | "
                f"best={pos['best_price']} close={close}"
            )
            return True

    elif pos["direction"] == "SHORT":
        pos["best_price"] = min(best, close)
        if close - pos["best_price"] >= trail_points:
            logger.info(
                f"EXIT SIGNAL | TRAIL SL | {pos['tradingsymbol']} | "
                f"best={pos['best_price']} close={close}"
            )
            return True

    return False

# ============================================================
# EXECUTION DISPATCH
# ============================================================

def _exit_position(
    *,
    pos,
    instrument_token,
    reason,
    exit_paper_position,
    exit_live_position
):
    """
    Dispatch exit to correct execution engine.
    """

    logger.info(
        f"EXIT TRIGGERED | {pos['strategy']} | {pos['tradingsymbol']} | reason={reason}"
    )

    if pos.get("kite_order_id"):
        # LIVE
        exit_live_position(
            instrument_token=instrument_token,
            qty=pos["qty"],
            reason=reason
        )
    else:
        # PAPER
        exit_paper_position(
            positions=None,  # paper engine already has reference
            token=instrument_token,
            price=pos.get("last_price"),
            reason=reason
        )
