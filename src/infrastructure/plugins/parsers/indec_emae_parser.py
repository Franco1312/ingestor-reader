"""INDEC EMAE parser plugin for CSV table format."""

import io
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd

from src.domain.interfaces import Parser
from src.infrastructure.utils.date_utils import to_naive

SeriesDataPoint = Dict[str, Union[str, datetime, float, None]]


class IndecEmaeParser(Parser):
    """Parser for INDEC EMAE CSV files.
    
    This parser handles CSV files with table format where:
    - Dates are in a column (indice_tiempo)
    - Multiple value columns exist (emae_original, emae_desestacionalizada, etc.)
    - Each value column becomes a separate series
    """

    def parse(
        self, 
        raw_data: bytes, 
        config: Dict[str, Union[str, int, Dict, List]],
        series_last_dates: Optional[Dict[str, datetime]] = None
    ) -> List[SeriesDataPoint]:
        """Parse raw CSV data according to INDEC EMAE configuration.
        
        Args:
            raw_data: Raw bytes of the CSV file.
            config: Configuration dictionary containing parse_config.
            series_last_dates: Dictionary mapping series_code to last processed date.
            
        Returns:
            List of dictionaries containing parsed series data.
        """
        parse_config = config.get("parse_config", {})
        date_column = parse_config.get("date_column", "indice_tiempo")
        series_map = parse_config.get("series_map", [])
        
        # Load CSV into DataFrame
        csv_file = io.BytesIO(raw_data)
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
        except UnicodeDecodeError:
            csv_file.seek(0)
            df = pd.read_csv(csv_file, encoding='latin-1')
        
        # Parse date column
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        parsed_data: List[SeriesDataPoint] = []
        
        for series_config in series_map:
            series_code = str(series_config.get("internal_series_code", ""))
            value_column = series_config.get("value_column")
            drop_na = series_config.get("drop_na", False)
            last_date = series_last_dates.get(series_code) if series_last_dates else None
            
            if not value_column or value_column not in df.columns:
                continue
            
            series_data = self._extract_series(
                df, 
                date_column, 
                value_column, 
                series_config, 
                last_date, 
                drop_na
            )
            parsed_data.extend(series_data)
        
        return parsed_data
    
    def _extract_series(
        self,
        df: pd.DataFrame,
        date_column: str,
        value_column: str,
        series_config: Dict[str, Union[str, int, bool]],
        last_date: Optional[datetime] = None,
        drop_na: bool = False
    ) -> List[SeriesDataPoint]:
        """Extract data for a single series from the DataFrame.
        
        Args:
            df: DataFrame with the CSV data.
            date_column: Name of the date column.
            value_column: Name of the value column.
            series_config: Configuration for the series to extract.
            last_date: Last processed date for this series (filters dates <= last_date).
            drop_na: Whether to drop rows with NaN values.
            
        Returns:
            List of dictionaries containing series data points.
        """
        series_data: List[SeriesDataPoint] = []
        
        # Filter out rows where date is NaT
        valid_df = df[df[date_column].notna()].copy()
        
        # Filter by last_date if provided
        if last_date:
            last_date_naive = to_naive(last_date)
            valid_df = valid_df[valid_df[date_column] > pd.Timestamp(last_date_naive)]
        
        for _, row in valid_df.iterrows():
            obs_time = row[date_column]
            value = row[value_column]
            
            # Skip if value is NaN and drop_na is True
            if drop_na and pd.isna(value):
                continue
            
            # Convert pandas Timestamp to datetime if needed
            if isinstance(obs_time, pd.Timestamp):
                obs_time = obs_time.to_pydatetime()
            
            # Apply timezone naive conversion
            obs_time = to_naive(obs_time)
            
            # Convert value to float if possible
            if pd.isna(value) or value is None:
                value = None
            elif isinstance(value, (int, float)):
                value = float(value)
            elif isinstance(value, str):
                try:
                    value = float(value.replace(",", ".").strip())
                except (ValueError, AttributeError):
                    value = None
            
            # Skip if value is still None and drop_na is True
            if drop_na and value is None:
                continue
            
            series_data.append({
                "internal_series_code": str(series_config["internal_series_code"]),
                "unit": series_config.get("unit"),
                "frequency": series_config.get("frequency"),
                "obs_time": obs_time,
                "value": value,
            })
        
        return series_data

