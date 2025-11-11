"""Factory for creating StateManager instances."""

from typing import Any, Dict, Optional

from src.domain.interfaces import StateManager as StateManagerInterface
from src.infrastructure.state_managers.file_state_manager import FileStateManager
from src.infrastructure.state_managers.manager_kinds import StateManagerKind
from src.infrastructure.state_managers.s3_state_manager import S3StateManager


class StateManagerFactory:
    """Factory for creating StateManager instances based on configuration."""

    @staticmethod
    def create(state_config: Optional[Dict[str, Any]] = None) -> Optional[StateManagerInterface]:
        """Create StateManager instance from configuration.

        Args:
            state_config: Configuration dictionary with 'kind' and specific parameters.
                Examples:
                - {"kind": "file", "state_file": "state.json"}
                - {"kind": "s3", "bucket": "my-bucket", "key": "state.json"}
                - None: Returns None (no state manager)

        Returns:
            StateManager instance or None if not configured.
        """
        if not state_config:
            return None

        kind_str = state_config.get("kind", StateManagerKind.FILE.value)
        try:
            kind = StateManagerKind(kind_str)
        except ValueError as exc:
            raise ValueError(f"Unknown state manager kind: {kind_str}") from exc

        if kind == StateManagerKind.FILE:
            state_file = state_config.get("state_file", "state.json")
            return FileStateManager(state_file)

        elif kind == StateManagerKind.S3:
            bucket = state_config.get("bucket")
            key = state_config.get("key")
            if not bucket or not key:
                raise ValueError("S3 state manager requires 'bucket' and 'key' in config")
            return S3StateManager(
                bucket=bucket,
                key=key,
                aws_access_key_id=state_config.get("aws_access_key_id"),
                aws_secret_access_key=state_config.get("aws_secret_access_key"),
            )

        # This should never happen with proper enum usage, but kept for defensive programming
        raise ValueError(f"Unhandled state manager kind: {kind}")  # pragma: no cover
