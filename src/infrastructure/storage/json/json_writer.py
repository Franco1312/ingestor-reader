"""JSON writer for writing partitioned data."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from src.infrastructure.partitioning.partition_strategy import PartitionStrategy


class JSONWriter:
    """Writes data to JSON format with partitioning."""

    def __init__(self, partition_strategy: PartitionStrategy):
        """Initialize JSONWriter.

        Args:
            partition_strategy: Partition strategy to use for grouping data.
        """
        self._partition_strategy = partition_strategy

    def write_to_json(self, data: List[Dict[str, Any]], base_output_path: str) -> List[str]:
        """Write data to JSON files partitioned by partition strategy.

        Args:
            data: List of data point dictionaries.
            base_output_path: Base directory path for output files.

        Returns:
            List of relative file paths (from base_output_path) of created JSON files.
        """
        if not data:
            return []

        # Group data by partition
        grouped = self._partition_strategy.group_by_partition(data)

        file_paths = []
        base_path = Path(base_output_path)

        for partition_path, partition_data in grouped.items():
            # Create partition directory
            partition_dir = base_path / partition_path
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Generate unique filename for this partition
            json_file = self._generate_json_filename(partition_dir)

            # Serialize data to JSON
            json_data = self._serialize_datetimes(partition_data)

            # Write JSON file
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)

            # Return relative path from base_output_path
            relative_path = os.path.relpath(str(json_file), base_output_path)
            file_paths.append(relative_path)

        return file_paths

    def _generate_json_filename(self, partition_dir: Path) -> Path:
        """Generate a unique filename for JSON file in partition.

        Args:
            partition_dir: Directory path for the partition.

        Returns:
            Path to the JSON file.
        """
        # Simple approach: use "data.json" if single file per partition
        # Future: could split into multiple parts if needed
        return partition_dir / "data.json"

    def _serialize_datetimes(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Serialize datetime objects to ISO format strings for JSON.

        Args:
            data: List of data dictionaries.

        Returns:
            List with datetime objects converted to ISO strings.
        """
        serialized = []
        for item in data:
            serialized_item = {}
            for key, value in item.items():
                if isinstance(value, datetime):
                    serialized_item[key] = value.isoformat()
                elif hasattr(value, "isoformat"):  # Handle pandas Timestamp
                    serialized_item[key] = value.isoformat()
                else:
                    serialized_item[key] = value
            serialized.append(serialized_item)
        return serialized

