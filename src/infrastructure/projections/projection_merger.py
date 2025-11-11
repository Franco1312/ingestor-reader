"""Projection merger for merging staging data with projections."""

import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.infrastructure.projections.staging_manager import StagingManager

logger = logging.getLogger(__name__)


class ProjectionMerger:
    """Merges staging data with existing projections."""

    def __init__(
        self,
        bucket: str,
        s3_client: Any = None,
        aws_region: str = "us-east-1",
        compression: str = "snappy",
        merge_workers: int = 1,
    ):
        """Initialize ProjectionMerger.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
            compression: Compression codec (default: "snappy").
            merge_workers: Number of parallel workers for merging partitions (default: 1, sequential).
        """
        self._bucket = bucket
        self._s3_client = s3_client or boto3.client("s3", region_name=aws_region)
        self._compression = compression
        self._merge_workers = merge_workers

    def merge_partition(self, dataset_id: str, partition_path: str) -> None:
        """Merge a single partition from staging with projections.

        Args:
            dataset_id: Dataset identifier.
            partition_path: Partition path (e.g., "SERIES_1/year=2024/month=01").
        """
        logger.info("Merging partition %s for dataset %s", partition_path, dataset_id)

        staging_key = self._build_staging_file_key(dataset_id, partition_path)
        projections_key = self._build_projections_file_key(dataset_id, partition_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Download files if they exist
            staging_data = self._download_and_read_parquet(staging_key, tmpdir)
            projections_data = self._download_and_read_parquet(projections_key, tmpdir)

            # Merge data
            merged_data = self._merge_data(projections_data, staging_data)

            # Write merged data back to staging
            if merged_data is not None and len(merged_data) > 0:
                output_file = Path(tmpdir) / "merged.parquet"
                self._write_parquet(merged_data, output_file)
                self._upload_to_staging(staging_key, output_file)

        logger.info("Successfully merged partition %s", partition_path)

    def merge_all_partitions(self, dataset_id: str) -> None:
        """Merge all partitions in staging with projections.

        Args:
            dataset_id: Dataset identifier.
        """
        staging_manager = StagingManager(bucket=self._bucket, s3_client=self._s3_client)
        partitions = staging_manager.list_staging_partitions(dataset_id)

        logger.info("Merging %d partition(s) for dataset %s", len(partitions), dataset_id)

        self._merge_all_partitions_parallel(dataset_id, partitions)

        logger.info("Successfully merged all partitions")

    def _merge_all_partitions_parallel(self, dataset_id: str, partitions: List[str]) -> None:
        """Merge all partitions in parallel using ThreadPoolExecutor."""
        logger.info("Merging %d partitions in parallel with %d workers", len(partitions), self._merge_workers)

        completed_count = 0
        total_partitions = len(partitions)

        with ThreadPoolExecutor(max_workers=self._merge_workers) as executor:
            future_to_partition = {
                executor.submit(self.merge_partition, dataset_id, partition_path): partition_path
                for partition_path in partitions
            }

            for future in as_completed(future_to_partition):
                completed_count += 1
                partition_path = future_to_partition[future]
                logger.debug("Completed merging partition %d/%d: %s", completed_count, total_partitions, partition_path)
                try:
                    future.result()
                except Exception as e:
                    logger.error("Failed to merge partition %s: %s", partition_path, e)
                    raise

    def _build_staging_file_key(self, dataset_id: str, partition_path: str) -> str:
        """Build S3 key for staging file.

        Args:
            dataset_id: Dataset identifier.
            partition_path: Partition path.

        Returns:
            S3 key string.
        """
        return f"datasets/{dataset_id}/staging/{partition_path}/data.parquet"

    def _build_projections_file_key(self, dataset_id: str, partition_path: str) -> str:
        """Build S3 key for projections file.

        Args:
            dataset_id: Dataset identifier.
            partition_path: Partition path.

        Returns:
            S3 key string.
        """
        return f"datasets/{dataset_id}/projections/{partition_path}/data.parquet"

    def _download_and_read_parquet(self, s3_key: str, tmpdir: str) -> Optional[pa.Table]:
        """Download parquet file from S3 and read it.

        Args:
            s3_key: S3 object key.
            tmpdir: Temporary directory path.

        Returns:
            PyArrow Table with data, or None if file doesn't exist.
        """
        if not self._s3_object_exists(s3_key):
            return None

        local_file = Path(tmpdir) / Path(s3_key).name
        self._s3_client.download_file(self._bucket, s3_key, str(local_file))

        return pq.read_table(str(local_file))

    def _s3_object_exists(self, s3_key: str) -> bool:
        """Check if S3 object exists.

        Args:
            s3_key: S3 object key.

        Returns:
            True if object exists, False otherwise.
        """
        try:
            self._s3_client.head_object(Bucket=self._bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def _merge_data(
        self, projections_table: Optional[pa.Table], staging_table: Optional[pa.Table]
    ) -> Optional[pa.Table]:
        """Merge projections and staging data, removing duplicates.

        Args:
            projections_table: Existing projections data (can be None).
            staging_table: New staging data (can be None).

        Returns:
            Merged PyArrow Table, or None if both inputs are None.
        """
        if projections_table is None and staging_table is None:
            return None

        if projections_table is None:
            return staging_table

        if staging_table is None:
            return projections_table

        # Combine both tables
        combined = pa.concat_tables([projections_table, staging_table])

        # Remove duplicates based on (obs_time, internal_series_code)
        # Convert to pandas for simpler deduplication
        df = combined.to_pandas()
        # Keep first occurrence (projections data takes precedence)
        merged_df = df.drop_duplicates(subset=["obs_time", "internal_series_code"], keep="first")
        # Convert back to table with original schema
        return pa.Table.from_pandas(merged_df, schema=combined.schema)

    def _write_parquet(self, data: pa.Table, output_file: Path) -> None:
        """Write PyArrow Table to parquet file.

        Args:
            data: PyArrow Table to write.
            output_file: Output file path.
        """
        pq.write_table(data, str(output_file), compression=self._compression)

    def _upload_to_staging(self, s3_key: str, local_file: Path) -> None:
        """Upload file to staging in S3.

        Args:
            s3_key: S3 destination key.
            local_file: Local file path.
        """
        self._s3_client.upload_file(str(local_file), self._bucket, s3_key)
