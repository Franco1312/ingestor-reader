"""Date utility functions."""

from datetime import datetime
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

