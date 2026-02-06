"""
execution/router.py

Routes strategy signals to PAPER or LIVE execution based on
per-strategy x per-index configuration.
"""

import logging

from db.session import db_session
from db.repository import SignalRepository

logger = logging.getLogger(__name__)


def route_signal(
    signal,
    token_meta,
    execution_config,
    paper_engine,
    live_engine,
    live_trading_enabled
):
    """
    Routes a strategy signal to PAPER or LIVE execution
    based on per-strategy x per-index config.
    """
    strategy = signal.get("strategy")
    token = signal.get("token")
    index = token_meta.get(token, {}).get("index")

    if not strategy or not index:
        return False

    cfg = execution_config.get((strategy, index))
    if not cfg or not cfg.get("enabled"):
        _log_signal(signal, accepted=False, reject_reason="OFF")
        logger.info(f"EXEC ROUTE | strategy={strategy} | index={index} | mode=OFF")
        return False

    mode = cfg.get("mode")
    qty = cfg.get("qty", 1)

    logger.info(f"EXEC ROUTE | strategy={strategy} | index={index} | mode={mode}")

    if mode == "OFF":
        _log_signal(signal, accepted=False, reject_reason="OFF")
        return False

    if mode == "PAPER":
        accepted = paper_engine.enter_position(token=token, signal=signal, qty=qty)
        if accepted:
            _log_signal(signal, accepted=True, reject_reason=None)
        else:
            _log_signal(signal, accepted=False, reject_reason="ALREADY_OPEN")
        return accepted

    if mode == "LIVE":
        if not live_trading_enabled:
            _log_signal(signal, accepted=False, reject_reason="LIVE_BLOCKED")
            logger.warning(f"LIVE BLOCKED BY ENV | {strategy} | {index}")
            return False
        accepted = live_engine.enter_position(token=token, signal=signal, qty=qty)
        if accepted:
            _log_signal(signal, accepted=True, reject_reason=None)
        else:
            _log_signal(signal, accepted=False, reject_reason="LIVE_REJECTED")
        return accepted

    return False


def _log_signal(signal, accepted, reject_reason):
    try:
        with db_session() as session:
            if session is None:
                return
            SignalRepository(session).log_signal(
                strategy=signal["strategy"],
                token=signal["token"],
                direction=signal["direction"],
                price=signal["price"],
                signal_time=signal.get("time"),
                accepted=accepted,
                reject_reason=reject_reason
            )
    except Exception:
        logger.exception("DB signal write failed (non-fatal)")
