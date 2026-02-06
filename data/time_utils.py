"""
time_utils.py

IST time helpers for consistent Cloud Run behavior.
"""

from datetime import datetime, timedelta, timezone

IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)


def now_ist():
    """
    Current IST time as an aware datetime.
    """
    return datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(IST)


def normalize_ist_naive(dt):
    """
    Normalize a datetime to IST and return a naive IST datetime.
    """
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(IST).replace(tzinfo=None)
