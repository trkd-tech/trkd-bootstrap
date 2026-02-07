# engine/config_loader.py

import logging
from data.time_utils import now_ist

logger = logging.getLogger(__name__)

# ============================================================
# CACHE
# ============================================================

_cached_exec_config = {}
_last_exec_loaded_date = None

# ============================================================
# HELPERS
# ============================================================

def _norm(v):
    return str(v).strip().upper() if v is not None else None


def _parse_bool(v):
    if v is None:
        return False
    return str(v).strip().upper() in ("TRUE", "YES", "1")


def _parse_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _parse_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ============================================================
# LOADER
# ============================================================

def load_execution_config(gspread_client, sheet_id):
    """
    Load execution configuration from STRATEGY_EXECUTION sheet.

    Keyed by:
        (strategy_name, index, direction)

    Example:
        ("VWAP_CROSSOVER", "NIFTY", "LONG")
    """

    sh = gspread_client.open_by_key(sheet_id)
    ws = sh.worksheet("STRATEGY_EXECUTION")
    rows = ws.get_all_records()

    config = {}

    for r in rows:
        strategy = _norm(r.get("strategy_name"))
        index = _norm(r.get("index"))
        direction = _norm(r.get("direction"))
        mode = _norm(r.get("mode"))

        if not all([strategy, index, direction]):
            logger.warning(f"EXEC CONFIG SKIPPED | missing keys | row={r}")
            continue

        if mode not in {"LIVE", "PAPER", "OFF"}:
            logger.warning(
                f"EXEC CONFIG INVALID MODE | {strategy} {index} {direction} | mode={mode}"
            )
            continue

        key = (strategy, index, direction)

        cfg = {
            "mode": mode,
            "enabled": _parse_bool(r.get("enabled")),
            "qty": _parse_int(r.get("qty"), 0),

            # --- Risk controls ---
            "trailing_sl_enabled": _parse_bool(r.get("trailing_sl_enabled")),
            "trailing_sl_points": _parse_int(r.get("trailing_sl_points"), 0),
            "max_daily_loss": _parse_float(r.get("max_daily_loss"), 0.0),

            # --- Options constraints ---
            "min_expiry_days": _parse_int(r.get("min_expiry_days"), 7),
        }

        # --- Hard safety checks ---
        if cfg["qty"] < 0:
            logger.warning(f"EXEC CONFIG INVALID QTY | {key} | qty={cfg['qty']}")
            continue

        if cfg["trailing_sl_points"] < 0:
            logger.warning(f"EXEC CONFIG INVALID TRAIL SL | {key}")
            continue

        config[key] = cfg

        logger.info(
            f"EXEC CONFIG LOADED | {key} | "
            f"mode={cfg['mode']} | qty={cfg['qty']} | "
            f"trail={cfg['trailing_sl_points']} | enabled={cfg['enabled']}"
        )

    return config


# ============================================================
# PUBLIC API
# ============================================================

def get_execution_config(gspread_client, sheet_id, *, force_reload=False):
    """
    Load execution config once per IST day unless force_reload=True.
    """
    global _cached_exec_config, _last_exec_loaded_date

    today = now_ist().date()

    if force_reload or _last_exec_loaded_date != today:
        _cached_exec_config = load_execution_config(gspread_client, sheet_id)
        _last_exec_loaded_date = today

    return _cached_exec_config
