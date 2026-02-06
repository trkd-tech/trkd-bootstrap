"""
execution/paper.py

Paper execution engine.

Responsibilities:
- Accept entry signals from strategies
- Track open positions per token
- Enforce one open position per token
- Handle exits (called externally by risk / strategy engine)

This module MUST:
- Not compute indicators
- Not evaluate strategies
- Not talk to Kite APIs
"""

import logging

from db.session import db_session
from db.repository import TradeRepository, PositionRepository
from data.time_utils import normalize_ist_naive, now_ist

logger = logging.getLogger(__name__)

# ============================================================
# POSITION STATE SHAPE
# ============================================================
# positions[token] = {
#     "strategy": str,
#     "direction": "LONG" | "SHORT",
#     "entry_price": float,
#     "entry_time": datetime,
#     "qty": int,
#     "open": bool
# }

# ============================================================
# ENTRY
# ============================================================

def enter_position(
    positions,
    token,
    signal,
    qty
):
    """
    Enter a paper position if none exists.

    signal = {
        "strategy": str,
        "token": token,
        "direction": "LONG" | "SHORT",
        "price": float,
        "time": datetime
    }
    """

    # --- Defensive validation ---
    required_keys = {"strategy", "direction", "price", "time"}
    if not required_keys.issubset(signal):
        logger.error(f"INVALID SIGNAL RECEIVED | token={token} | signal={signal}")
        return False

    signal_time = signal.get("time")
    if signal_time:
        signal_time = normalize_ist_naive(signal_time)
    else:
        signal_time = now_ist()

    # --- One open position per token ---
    if token in positions and positions[token]["open"]:
        return False

    positions[token] = {
        "strategy": signal["strategy"],
        "direction": signal["direction"],
        "entry_price": signal["price"],
        "entry_time": signal["time"],
        "qty": qty,
        "open": True
    }

    trade_id = _build_trade_id(signal["strategy"], token, signal_time)

    _log_trade_entry(signal, token, trade_id, qty, signal_time)
    _log_position_open(signal, token, trade_id, qty, signal_time)

    logger.info(
        f"PAPER ENTRY | {signal['strategy']} | "
        f"token={token} | {signal['direction']} | "
        f"price={signal['price']}"
    )

    return True

# ============================================================
# EXIT
# ============================================================

def exit_position(
    positions,
    token,
    price,
    reason
):
    """
    Exit an open paper position.
    """

    pos = positions.get(token)
    if not pos or not pos["open"]:
        return False

    pnl = (
        (price - pos["entry_price"]) * pos["qty"]
        if pos["direction"] == "LONG"
        else (pos["entry_price"] - price) * pos["qty"]
    )

    pos["open"] = False

    trade_id = _build_trade_id(pos["strategy"], token, pos["entry_time"])

    _log_trade_exit(trade_id, price, reason, pnl)
    _log_position_close(trade_id, price, pnl)

    logger.info(
        f"PAPER EXIT | {pos['strategy']} | "
        f"token={token} | reason={reason} | "
        f"PNL={round(pnl, 2)}"
    )

    return True

# ============================================================
# HELPERS
# ============================================================

def has_open_position(positions, token):
    return token in positions and positions[token]["open"]

def get_open_position(positions, token):
    pos = positions.get(token)
    return pos if pos and pos["open"] else None


def _build_trade_id(strategy, token, dt):
    if dt is None:
        dt = now_ist()
    dt = normalize_ist_naive(dt)
    return f"{strategy}-{token}-{dt.strftime('%Y%m%d')}-{dt.strftime('%H%M')}"


def _log_trade_entry(signal, token, trade_id, qty, entry_time):
    try:
        with db_session() as session:
            if session is None:
                return
            TradeRepository(session).upsert_trade_entry(
                trade_id=trade_id,
                strategy=signal["strategy"],
                token=token,
                index=signal.get("index", "UNKNOWN"),
                direction=signal["direction"],
                qty=qty,
                entry_price=signal["price"],
                entry_time=entry_time
            )
    except Exception:
        logger.exception("DB trade entry write failed (non-fatal)")


def _log_trade_exit(trade_id, exit_price, exit_reason, pnl):
    try:
        with db_session() as session:
            if session is None:
                return
            TradeRepository(session).update_trade_exit(
                trade_id=trade_id,
                exit_price=exit_price,
                exit_reason=exit_reason,
                pnl=pnl
            )
    except Exception:
        logger.exception("DB trade exit write failed (non-fatal)")


def _log_position_open(signal, token, position_id, qty, entry_time):
    try:
        with db_session() as session:
            if session is None:
                return
            PositionRepository(session).upsert_position(
                position_id=position_id,
                token=token,
                strategy=signal["strategy"],
                index=signal.get("index", "UNKNOWN"),
                direction=signal["direction"],
                qty=qty,
                entry_price=signal["price"],
                entry_time=entry_time
            )
    except Exception:
        logger.exception("DB position open write failed (non-fatal)")


def _log_position_close(position_id, exit_price, pnl):
    try:
        with db_session() as session:
            if session is None:
                return
            PositionRepository(session).close_position(
                position_id=position_id,
                exit_price=exit_price,
                pnl=pnl
            )
    except Exception:
        logger.exception("DB position close write failed (non-fatal)")
