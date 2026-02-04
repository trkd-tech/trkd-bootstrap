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
