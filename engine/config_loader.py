"""
config_loader.py

Loads per-strategy configuration from Google Sheets.

Responsibilities:
- Read STRATEGY_CONFIG sheet
- Parse values into correct types
- Return config dict keyed by strategy name

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
# LOADER
# ============================================================

def load_strategy_config(gspread_client, sheet_id):
    """
    Load per-strategy configuration from Google Sheet.

    Returns:
        dict[strategy_name] = {
            "enabled": bool,
            param_name: value,
            ...
        }
    """

    sh = gspread_client.open_by_key(sheet_id)
    ws = sh.worksheet("STRATEGY_CONFIG")

    rows = ws.get_all_records()

    config = {}

    for row in rows:
        strategy = row.get("strategy_name")
        enabled = row.get("enabled")
        param = row.get("param")
        value = row.get("value")

        if not strategy or not param:
            continue

        strat_cfg = config.setdefault(strategy, {})

        if param == "enabled":
            strat_cfg["enabled"] = bool(enabled)
        else:
            strat_cfg[param] = parse_value(value)

    # Log loaded strategies
    for strat, cfg in config.items():
        logger.info(
            f"STRATEGY CONFIG LOADED | {strat} | params={cfg}"
        )

    return config


_cached_config = {}
_last_loaded_date = None


def get_strategy_config(
    gspread_client,
    sheet_id,
    *,
    force_reload=False
):
    """
    Load strategy config once per IST day unless force_reload is True.
    """
    global _cached_config, _last_loaded_date

    today = now_ist().date()

    if force_reload or _last_loaded_date != today:
        _cached_config = load_strategy_config(gspread_client, sheet_id)
        _last_loaded_date = today

    return _cached_config
