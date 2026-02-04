"""
engine/runtime.py

Central runtime orchestrator.

Responsibilities:
- Receive completed candles (from data layer)
- Route candles to strategies
- Collect strategy signals
- Forward valid signals to execution engine
- Invoke risk exits (if any)

This module MUST:
- Not compute indicators
- Not place real trades
- Not talk to Kite APIs
"""

import logging

from strategies.orb import evaluate_orb
from strategies.vwap_crossover import evaluate_vwap_crossover
from execution.paper import enter_position, has_open_position

logger = logging.getLogger(__name__)

# ============================================================
# RUNTIME STATE SHAPE
# ============================================================
# Shared mutable state passed in from main / bootstrap
#
# vwap_state[token] -> {cum_pv, cum_vol, vwap}
# opening_range[token] -> {high, low, finalized}
# strategy_state[token][strategy] -> strategy-specific counters
# positions[token] -> open position
#
# This file does NOT own state — it only mutates what is passed.
# ============================================================


# ============================================================
# STRATEGY DISPATCH
# ============================================================

def on_5m_candle_close(
    token,
    candle,
    prev_candle,
    *,
    vwap_state,
    opening_range,
    strategy_state,
    positions,
    paper_qty,
    strategies_config
):
    """
    Called exactly once per CLOSED 5-minute candle per token.

    Parameters are injected explicitly to avoid hidden globals.
    """

    signals = []

    # --------------------------------------------------------
    # Strategy: VWAP ORB
    # --------------------------------------------------------
    orb_cfg = strategies_config.get("VWAP_ORB", {})
    if orb_cfg.get("enabled"):
        sig = evaluate_orb(
            token=token,
            candle=candle,
            vwap_state=vwap_state,
            opening_range=opening_range,
            strategy_state=strategy_state,
            config=orb_cfg
        )
        if sig:
            signals.append(sig)

    # --------------------------------------------------------
    # Strategy: VWAP Crossover
    # --------------------------------------------------------
    cross_cfg = strategies_config.get("VWAP_CROSSOVER", {})
    if cross_cfg.get("enabled"):
        sig = evaluate_vwap_crossover(
            token=token,
            candle=candle,
            prev_candle=prev_candle,
            vwap_state=vwap_state,
            strategy_state=strategy_state,
            config=cross_cfg
        )
        if sig:
            signals.append(sig)

    # --------------------------------------------------------
    # EXECUTION (paper)
    # --------------------------------------------------------
    for sig in signals:
        if has_open_position(positions, token):
            logger.info(
                f"SKIP ENTRY | token={token} | "
                f"existing open position"
            )
            continue

        qty = paper_qty.get(token)
        if not qty:
            logger.warning(
                f"NO QTY CONFIGURED | token={token} | skipping entry"
            )
            continue

        enter_position(
            positions=positions,
            token=token,
            signal=sig,
            qty=qty
        )

# ============================================================
# OPTIONAL: LOGGING HOOK
# ============================================================

def log_state_snapshot(
    token,
    *,
    vwap_state,
    opening_range,
    positions
):
    """
    Optional debugging hook.
    Safe to remove in production.
    """
    vwap = vwap_state.get(token, {}).get("vwap")
    orr = opening_range.get(token)
    pos = positions.get(token)

    logger.info(
        f"STATE SNAPSHOT | token={token} | "
        f"VWAP={round(vwap,2) if vwap else None} | "
        f"OR={orr} | "
        f"POS={'OPEN' if pos and pos.get('open') else 'NONE'}"
    )
