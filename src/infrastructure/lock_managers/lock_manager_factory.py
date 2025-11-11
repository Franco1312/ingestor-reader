"""Factory for creating LockManager instances."""

from typing import Any, Dict, Optional

from src.domain.interfaces import LockManager as LockManagerInterface
from src.infrastructure.lock_managers.dynamodb_lock_manager import DynamoDBLockManager
from src.infrastructure.state_managers.manager_kinds import LockManagerKind


class LockManagerFactory:
    """Factory for creating LockManager instances based on configuration."""

    @staticmethod
    def create(lock_config: Optional[Dict[str, Any]] = None) -> Optional[LockManagerInterface]:
        """Create LockManager instance from configuration.

        Args:
            lock_config: Configuration dictionary with 'kind' and specific parameters.
                Examples:
                - {"kind": "dynamodb", "table_name": "etl_locks", "region_name": "us-east-1"}
                - None: Returns None (no lock manager)

        Returns:
            LockManager instance or None if not configured.
        """
        if not lock_config:
            return None

        kind_str = lock_config.get("kind")
        if not kind_str:
            raise ValueError("Lock manager config requires 'kind' field")

        try:
            kind = LockManagerKind(kind_str)
        except ValueError as exc:
            raise ValueError(f"Unknown lock manager kind: {kind_str}") from exc

        if kind == LockManagerKind.DYNAMODB:
            table_name = lock_config.get("table_name")
            if not table_name:
                raise ValueError("DynamoDB lock manager requires 'table_name' in config")

            return DynamoDBLockManager(
                table_name=table_name,
                region_name=lock_config.get("region_name"),
                aws_access_key_id=lock_config.get("aws_access_key_id"),
                aws_secret_access_key=lock_config.get("aws_secret_access_key"),
            )

        raise ValueError(f"Unhandled lock manager kind: {kind}")  # pragma: no cover
