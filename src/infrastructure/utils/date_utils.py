"""Date utility functions."""

from datetime import datetime, timedelta, timezone
from typing import Optional


def to_naive(date: Optional[datetime]) -> Optional[datetime]:
    """Convert datetime to naive (remove timezone).
    
    Args:
        date: Datetime with or without timezone.
        
    Returns:
        Naive datetime or None.
    """
    if date is None:
        return None
    return date.replace(tzinfo=None) if date.tzinfo else date


def get_window_start_date(window_in_days: Optional[int]) -> Optional[datetime]:
    """Calculate the start date of a time window.
    
    Args:
        window_in_days: Number of days to look back from now. If None, returns None.
        
    Returns:
        Start date of the window (now - window_in_days), or None if window_in_days is None.
    """
    if window_in_days is None:
        return None
    
    now = datetime.now(timezone.utc)
    return now - timedelta(days=window_in_days)

