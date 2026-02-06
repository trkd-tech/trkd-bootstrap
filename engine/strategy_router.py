"""
engine/strategy_router.py

Routes completed candles to enabled strategies using
per-strategy configuration loaded at runtime.

Responsibilities:
- Decide WHICH strategies run
- Pass correct inputs + config to each strategy
- Collect and return emitted signals

This module MUST:
- Not place trades
- Not manage positions
- Not compute indicators
"""

import logging

from strategies.orb import evaluate_orb, STRATEGY_NAME as ORB_NAME
from strategies.vwap_crossover import (
    evaluate_vwap_crossover,
    STRATEGY_NAME as VWAP_CROSS_NAME
)

logger = logging.getLogger(__name__)

# ============================================================
# STRATEGY REGISTRY
# ============================================================
# Maps strategy name → evaluation function
# Adding a new strategy = add one line here
# ============================================================

STRATEGY_REGISTRY = {
    ORB_NAME: evaluate_orb,
    VWAP_CROSS_NAME: evaluate_vwap_crossover,
}

# ============================================================
# ROUTER
# ============================================================

def route_strategies(
    *,
    token,
    candle,
    prev_candle,
    vwap_state,
    opening_range,
    token_meta,
    strategy_state,
    strategy_config
):
    """
    Route a completed candle to all enabled strategies.

    Args:
        token: instrument token
        candle: completed 5-minute candle (dict)
        prev_candle: previous 5-minute candle or None
        vwap_state: shared VWAP state
        opening_range: shared OR state
        token_meta: token → {index: str}
        strategy_state: mutable per-strategy state
        strategy_config: dict loaded from STRATEGY_CONFIG sheet

    Returns:
        List[signal_dict]
    """

    signals = []

    if not strategy_config:
        return signals

    for strategy_name, config in strategy_config.items():
        if not config.get("enabled", False):
            continue

        evaluator = STRATEGY_REGISTRY.get(strategy_name)
        if not evaluator:
            logger.warning(
                f"STRATEGY NOT REGISTERED | name={strategy_name}"
            )
            continue

        try:
            # ------------------------------------------------
            # ORB STRATEGY
            # ------------------------------------------------
            if strategy_name == ORB_NAME:
                signal = evaluator(
                    token=token,
                    candle=candle,
                    vwap_state=vwap_state,
                    opening_range=opening_range,
                    token_meta=token_meta,
                    strategy_state=strategy_state,
                    config=config
                )

            # ------------------------------------------------
            # VWAP CROSSOVER STRATEGY
            # ------------------------------------------------
            elif strategy_name == VWAP_CROSS_NAME:
                signal = evaluator(
                    token=token,
                    candle=candle,
                    prev_candle=prev_candle,
                    vwap_state=vwap_state,
                    token_meta=token_meta,
                    strategy_state=strategy_state,
                    config=config
                )

            else:
                # Should never happen due to registry check
                continue

            if signal:
                signals.append(signal)

        except Exception:
            logger.exception(
                f"STRATEGY ERROR | name={strategy_name} | token={token}"
            )

    return signals
