"""
performance/tracker.py

In-memory tracking of strategy signal performance using ATM option prices.

NOTE: This is an in-memory tracker intended for local runtime visibility.
Persist records to a database for durable analytics.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Iterable, Optional

from data.time_utils import now_ist


@dataclass
class SignalRecord:
    strategy: str
    index: str
    direction: str
    option_symbol: str
    entry_ltp: float
    entry_time: datetime
    last_ltp: float
    last_time: datetime
    pnl: float
    qty: int = 1

    def to_dict(self):
        return asdict(self)


records: list[SignalRecord] = []


def record_signal(*, strategy, index, direction, option_symbol, ltp, qty=1):
    now = now_ist()
    record = SignalRecord(
        strategy=strategy,
        index=index,
        direction=direction,
        option_symbol=option_symbol,
        entry_ltp=ltp,
        entry_time=now,
        last_ltp=ltp,
        last_time=now,
        pnl=0.0,
        qty=qty
    )
    records.append(record)
    return record


def update_option_marks(kite_client):
    if not records:
        return

    symbols = {record.option_symbol for record in records}
    ltp_keys = [f"NFO:{symbol}" for symbol in symbols]
    ltp_data = kite_client.ltp(ltp_keys)
    now = now_ist()

    for record in records:
        key = f"NFO:{record.option_symbol}"
        ltp = ltp_data.get(key, {}).get("last_price")
        if ltp is None:
            continue

        direction_mult = 1 if record.direction == "LONG" else -1
        record.last_ltp = ltp
        record.last_time = now
        record.pnl = (ltp - record.entry_ltp) * record.qty * direction_mult


def summarize_performance(
    *,
    period: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None
):
    now = now_ist()
    if period == "custom":
        if not start or not end:
            raise ValueError("custom period requires start and end datetimes")
        from_dt, to_dt = start, end
    elif period == "1d":
        from_dt, to_dt = now - timedelta(days=1), now
    elif period == "1w":
        from_dt, to_dt = now - timedelta(weeks=1), now
    elif period == "1m":
        from_dt, to_dt = now - timedelta(days=30), now
    elif period == "1q":
        from_dt, to_dt = now - timedelta(days=90), now
    elif period == "ytd":
        from_dt = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif period == "1y":
        from_dt, to_dt = now - timedelta(days=365), now
    else:
        raise ValueError(f"Unsupported period: {period}")

    summary = {}
    for record in records:
        if not (from_dt <= record.entry_time <= to_dt):
            continue
        key = (record.strategy, record.index)
        bucket = summary.setdefault(key, {
            "strategy": record.strategy,
            "index": record.index,
            "signals": 0,
            "pnl": 0.0
        })
        bucket["signals"] += 1
        bucket["pnl"] += record.pnl

    return list(summary.values())
