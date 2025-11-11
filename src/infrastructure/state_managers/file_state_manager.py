"""File-based state manager for incremental updates."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.domain.interfaces import StateManager as StateManagerInterface
from src.infrastructure.utils.date_utils import to_naive


class FileStateManager(StateManagerInterface):
    """File-based state manager for incremental updates."""

    def __init__(self, state_file: str = "state.json"):
        """Initialize file-based state manager.
        
        Args:
            state_file: Path to state file.
        """
        self._state_file = Path(state_file)

    def get_series_last_dates(self, config: Dict[str, Any]) -> Dict[str, datetime]:
        """Get last processed date for each series in config."""
        parse_config = config.get("parse_config", {})
        series_map = parse_config.get("series_map", [])
        
        series_last_dates = {}
        for series_config in series_map:
            series_code = str(series_config.get("internal_series_code", ""))
            if series_code:
                last_date = self.get_last_date(series_code)
                if last_date:
                    series_last_dates[series_code] = last_date
        
        return series_last_dates

    def save_dates_from_data(self, data: List[Dict[str, Any]]) -> None:
        """Save max date for each series from normalized data."""
        if not data:
            return
        
        series_max_dates: Dict[str, datetime] = {}
        for data_point in data:
            series_code = data_point.get("internal_series_code")
            obs_time = data_point.get("obs_time")
            
            if series_code and isinstance(obs_time, datetime):
                series_code = str(series_code)
                obs_time_naive = to_naive(obs_time)
                if obs_time_naive and (series_code not in series_max_dates or obs_time_naive > series_max_dates[series_code]):
                    series_max_dates[series_code] = obs_time_naive
        
        if series_max_dates:
            state = self._load()
            for series_code, max_date in series_max_dates.items():
                state[series_code] = max_date.isoformat()
            self._save(state)

    def get_last_date(self, series_code: str) -> Optional[datetime]:
        """Get last processed date for a series (always naive)."""
        state = self._load()
        date_str = state.get(series_code)
        if not date_str:
            return None
        
        date = datetime.fromisoformat(date_str)
        return to_naive(date)

    def _load(self) -> Dict[str, str]:
        """Load state from file."""
        if not self._state_file.exists():
            return {}
        try:
            with open(self._state_file, encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return {}

    def _save(self, state: Dict[str, str]) -> None:
        """Save state to file."""
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self._state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)


# Alias for backward compatibility
StateManager = FileStateManager

