"""INDEC EMAE transformer plugin."""

from datetime import UTC, datetime
from typing import Any, Dict, List

from src.domain.interfaces import Transformer


class IndecEmaeTransformer(Transformer):
    """Transformer for INDEC EMAE normalized data.

    This transformer:
    - Ensures unit and frequency come from configuration (not from parser)
    - Adds collection_date (when the ETL was executed)
    - Converts fraction values to percentage for variation series (_via and _vm)
    """

    def transform(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform normalized data by adding metadata from config and converting fractions to percentages.

        Args:
            data: List of normalized data points.
            config: Configuration dictionary containing parse_config.

        Returns:
            List of transformed data points with unit, frequency, collection_date, and converted values.
        """
        # Get series map from config to lookup unit and frequency
        parse_config = config.get("parse_config", {})
        series_map = parse_config.get("series_map", [])

        # Create lookup dict: series_code -> {unit, frequency, value_column}
        series_metadata = {}
        for series_config in series_map:
            series_code = str(series_config.get("internal_series_code", ""))
            if series_code:
                series_metadata[series_code] = {
                    "unit": series_config.get("unit"),
                    "frequency": series_config.get("frequency"),
                    "value_column": series_config.get("value_column"),
                }

        # Get collection date (when ETL is executed)
        collection_date = datetime.now(UTC)

        # Series codes that need fraction to percentage conversion
        variation_series_suffixes = ["_via", "_vm"]
        
        # Transform each data point
        transformed = []
        for data_point in data:
            series_code = data_point.get("internal_series_code")

            # Get unit and frequency from config (not from parser)
            metadata = series_metadata.get(series_code, {})
            unit = metadata.get("unit")
            frequency = metadata.get("frequency")
            value_column = metadata.get("value_column", "")

            # Get original value
            value = data_point["value"]

            # Convert fraction to percentage for variation series
            # Check if value_column ends with _via or _vm, or if unit is pct
            if unit == "pct" and value is not None:
                # Multiply by 100 to convert fraction to percentage
                value = value * 100

            # Create transformed data point
            transformed_point = {
                "obs_time": data_point["obs_time"],
                "internal_series_code": series_code,
                "value": value,
                "unit": unit,  # From config
                "frequency": frequency,  # From config
                "collection_date": collection_date,  # When ETL was executed
            }

            transformed.append(transformed_point)

        return transformed

