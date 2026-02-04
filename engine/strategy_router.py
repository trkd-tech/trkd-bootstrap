"""
engine/strategy_router.py

Central strategy evaluation router.

Responsibilities:
- Invoke enabled strategies
- Enforce candle sequencing safety
- Normalize strategy outputs
- Isolate strategy failures (one strategy must NOT break others)

This module MUST:
- Not place trades
- Not manage positions
- Not update indicators
"""

import logging
from datetime import timedelta

from strategies.orb import evaluate_orb
from strategies.vwap_crossover import evaluate_vwap_crossover

logger = logging.getLogger(__name__)

# ============================================================
# STRATEGY REGISTRY
# ============================================================

STRATEGY_REGISTRY = {
    "VWAP_ORB": evaluate_orb,
    "VWAP_CROSSOVER": evaluate_vwap_crossover
}

# ============================================================
# ROUTER
# ============================================================

def evaluate_strategies(
    token,
    candle,
    candles_5m,
    vwap_state,
    opening_range,
    strategy_state,
    strategies_config
):
    """
    Evaluate all enabled strategies on a completed 5-minute candle.

    Args:
        token: instrument token
        candle: current closed 5-minute candle
        candles_5m: dict[(token, datetime)] -> candle
        vwap_state: shared VWAP state
        opening_range: shared OR state
        strategy_state: shared per-strategy state
        strategies_config: global strategy config

    Returns:
        List of strategy signal dicts
    """

    signals = []

    # Resolve previous 5-minute candle safely
    prev_key = (token, candle["start"] - timedelta(minutes=5))
    prev_candle = candles_5m.get(prev_key)

    for strategy_name, config in strategies_config.items():
        if not config.get("enabled", False):
            continue

        evaluator = STRATEGY_REGISTRY.get(strategy_name)
        if not evaluator:
            logger.warning(f"Strategy not registered: {strategy_name}")
            continue

        try:
            # --- ORB ---
            if strategy_name == "VWAP_ORB":
                signal = evaluator(
                    token=token,
                    candle=candle,
                    vwap_state=vwap_state,
                    opening_range=opening_range,
                    strategy_state=strategy_state,
                    config=config
                )

            # --- VWAP Crossover ---
            elif strategy_name == "VWAP_CROSSOVER":
                signal = evaluator(
                    token=token,
                    candle=candle,
                    prev_candle=prev_candle,
                    vwap_state=vwap_state,
                    strategy_state=strategy_state,
                    config=config
                )

            else:
                continue

            if signal:
                signals.append(signal)

        except Exception:
            logger.exception(
                f"Strategy evaluation failed | "
                f"strategy={strategy_name} | token={token}"
            )

    return signals
