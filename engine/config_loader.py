"""
engine/config_loader.py

Loads configuration from Google Sheets.

Responsibilities:
- Load STRATEGY_CONFIG (strategy parameters)
- Load STRATEGY_EXECUTION (paper/live/off + qty)
- Load SYSTEM_CONTROL (global runtime switches)
- Parse values into correct Python types
- Cache configs once per IST day

This module MUST:
- Not evaluate strategies
- Not place trades
- Only provide configuration
"""

import logging
from datetime import datetime

from data.time_utils import now_ist

logger = logging.getLogger(__name__)

# ============================================================
# INTERNAL CACHE
# ============================================================

_cached_strategy_config = {}
_last_strategy_loaded_date = None

_cached_execution_config = {}
_last_execution_loaded_date = None

_cached_system_control = {}
_last_system_control_date = None

# ============================================================
# TYPE PARSING
# ============================================================

def parse_value(value):
    """
    Convert string sheet values to proper Python types.
    """
    if value is None:
        return None

    v = str(value).strip()

    # Boolean
    if v.upper() in ("TRUE", "FALSE"):
        return v.upper() == "TRUE"

    # Integer
    if v.isdigit():
        return int(v)

    # Float
    try:
        return float(v)
    except ValueError:
        pass

    # Time (HH:MM)
    try:
        return datetime.strptime(v, "%H:%M").time()
    except ValueError:
        pass

    # Fallback: string
    return v

# ============================================================
# STRATEGY CONFIG
# ============================================================

def load_strategy_config(gspread_client, sheet_id):
    """
    Load STRATEGY_CONFIG sheet.

    Expected columns:
    - strategy_name
    - enabled
    - param
    - value

    Returns:
        dict[strategy_name] = {
            "enabled": bool,
            param_name: parsed_value,
            ...
        }
    """
    sh = gspread_client.open_by_key(sheet_id)
    ws = sh.worksheet("STRATEGY_CONFIG")

    rows = ws.get_all_records()
    config = {}

    for row in rows:
        strategy = row.get("strategy_name")
        param = row.get("param")
        value = row.get("value")
        enabled = row.get("enabled")

        if not strategy or not param:
            continue

        strat_cfg = config.setdefault(strategy, {})

        if param == "enabled":
            strat_cfg["enabled"] = bool(enabled)
        else:
            strat_cfg[param] = parse_value(value)

    for strat, cfg in config.items():
        logger.info(
            f"STRATEGY CONFIG LOADED | {strat} | params={cfg}"
        )

    return config


def get_strategy_config(
    gspread_client,
    sheet_id,
    *,
    force_reload=False
):
    """
    Cached STRATEGY_CONFIG (once per IST day).
    """
    global _cached_strategy_config, _last_strategy_loaded_date

    today = now_ist().date()

    if force_reload or _last_strategy_loaded_date != today:
        _cached_strategy_config = load_strategy_config(
            gspread_client, sheet_id
        )
        _last_strategy_loaded_date = today

    return _cached_strategy_config

# ============================================================
# EXECUTION CONFIG (Paper / Live / Off)
# ============================================================

def _normalize_mode(value):
    if not value:
        return None
    return str(value).strip().upper()


def load_execution_config(gspread_client, sheet_id):
    """
    Load STRATEGY_EXECUTION sheet.

    Expected columns:
    - strategy_name
    - index (NIFTY / BANKNIFTY)
    - mode (PAPER / LIVE / OFF)
    - qty
    - enabled

    Returns:
        dict[(strategy_name, index)] = {
            "mode": str,
            "qty": int,
            "enabled": bool
        }
    """
    sh = gspread_client.open_by_key(sheet_id)
    ws = sh.worksheet("STRATEGY_EXECUTION")

    rows = ws.get_all_records()
    config = {}

    for row in rows:
        strategy = row.get("strategy_name")
        index = row.get("index")
        mode = _normalize_mode(row.get("mode"))
        qty = row.get("qty")
        enabled = row.get("enabled")

        if not strategy or not index:
            continue

        if mode not in {"LIVE", "PAPER", "OFF"}:
            logger.warning(
                f"EXEC CONFIG INVALID MODE | {strategy} | {index} | mode={mode}"
            )
            continue

        try:
            qty = int(qty)
        except (TypeError, ValueError):
            logger.warning(
                f"EXEC CONFIG INVALID QTY | {strategy} | {index} | qty={qty}"
            )
            continue

        if qty <= 0:
            continue

        config[(strategy, index)] = {
            "mode": mode,
            "qty": qty,
            "enabled": bool(enabled)
        }

    for (strategy, index), cfg in config.items():
        logger.info(
            f"EXEC CONFIG LOADED | {strategy} | {index} | "
            f"mode={cfg['mode']} | qty={cfg['qty']} | enabled={cfg['enabled']}"
        )

    return config


def get_execution_config(
    gspread_client,
    sheet_id,
    *,
    force_reload=False
):
    """
    Cached STRATEGY_EXECUTION (once per IST day).
    """
    global _cached_execution_config, _last_execution_loaded_date

    today = now_ist().date()

    if force_reload or _last_execution_loaded_date != today:
        _cached_execution_config = load_execution_config(
            gspread_client, sheet_id
        )
        _last_execution_loaded_date = today

    return _cached_execution_config

# ============================================================
# SYSTEM CONTROL
# ============================================================

def load_system_control(gspread_client, sheet_id):
    """
    Load SYSTEM_CONTROL sheet into key/value dict.

    Accepts flexible column naming.
    """
    sh = gspread_client.open_by_key(sheet_id)
    ws = sh.worksheet("SYSTEM_CONTROL")

    rows = ws.get_all_records()
    control = {}

    key_fields = ("key", "param", "name")
    value_fields = ("value", "enabled", "flag")

    for row in rows:
        key = next(
            (row.get(k) for k in key_fields if row.get(k) is not None),
            None
        )
        value = next(
            (row.get(v) for v in value_fields if row.get(v) is not None),
            None
        )

        if key is None:
            continue

        control[str(key).strip()] = parse_value(value)

    logger.info(f"SYSTEM CONTROL LOADED | keys={list(control.keys())}")
    return control


def get_system_control(
    gspread_client,
    sheet_id,
    *,
    force_reload=False
):
    """
    Cached SYSTEM_CONTROL (once per IST day).
    """
    global _cached_system_control, _last_system_control_date

    today = now_ist().date()

    if force_reload or _last_system_control_date != today:
        _cached_system_control = load_system_control(
            gspread_client, sheet_id
        )
        _last_system_control_date = today

    return _cached_system_control
