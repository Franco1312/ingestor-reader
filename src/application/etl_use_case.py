"""ETL use case."""

from typing import Optional

from ..domain.interfaces import (
    Extractor,
    Loader,
    LockManager,
    Normalizer,
    Parser,
    StateManager,
    Transformer,
)


class ETLUseCase:
    """Orchestrates the ETL process."""

    def __init__(
        self,
        extractor: Extractor,
        parser: Optional[Parser] = None,
        normalizer: Optional[Normalizer] = None,
        transformer: Optional[Transformer] = None,
        loader: Optional[Loader] = None,
        state_manager: Optional[StateManager] = None,
        lock_manager: Optional[LockManager] = None,
    ):
        """Initialize ETL use case with dependencies.

        Args:
            extractor: Extractor instance.
            parser: Parser instance (optional).
            normalizer: Normalizer instance (optional).
            transformer: Transformer instance (optional).
            loader: Loader instance (optional).
            state_manager: StateManager instance for incremental updates (optional).
            lock_manager: LockManager instance for distributed locking (optional).
        """
        self._extractor = extractor
        self._parser = parser
        self._normalizer = normalizer
        self._transformer = transformer
        self._loader = loader
        self._state_manager = state_manager
        self._lock_manager = lock_manager

    @property
    def extractor(self):
        """Get extractor instance (for testing)."""
        return self._extractor

    @property
    def parser(self):
        """Get parser instance (for testing)."""
        return self._parser

    @property
    def normalizer(self):
        """Get normalizer instance (for testing)."""
        return self._normalizer

    @property
    def transformer(self):
        """Get transformer instance (for testing)."""
        return self._transformer

    @property
    def loader(self):
        """Get loader instance (for testing)."""
        return self._loader

    @property
    def state_manager(self):
        """Get state manager instance (for testing)."""
        return self._state_manager

    @property
    def lock_manager(self):
        """Get lock manager instance (for testing)."""
        return self._lock_manager

    def execute(self, config: Optional[dict] = None):
        """Execute the complete ETL process.

        Args:
            config: Configuration dictionary. May contain 'lock' config with:
                - 'key': Lock key (defaults to dataset_id)
                - 'timeout_seconds': Lock timeout (default: 300)

        Returns:
            Processed data.

        Raises:
            RuntimeError: If lock cannot be acquired.
        """
        config = config or {}
        lock_key = None

        if self._lock_manager:
            lock_config = config.get("lock", {})
            dataset_id = config.get("dataset_id", "default")
            lock_key = lock_config.get("key", f"etl:{dataset_id}")
            timeout_seconds = lock_config.get("timeout_seconds", 300)

            if not self._lock_manager.acquire(lock_key, timeout_seconds):
                raise RuntimeError(
                    f"Could not acquire lock for '{lock_key}'. Another process may be running."
                )

        try:
            return self._execute_etl(config)
        finally:
            if self._lock_manager and lock_key:
                self._lock_manager.release(lock_key)

    def _execute_etl(self, config: dict):
        """Execute ETL steps without lock management."""
        raw_data = self._extractor.extract()

        series_last_dates = (
            self._state_manager.get_series_last_dates(config) if self._state_manager else None
        )

        data = self._parser.parse(raw_data, config, series_last_dates) if self._parser else []

        if self._normalizer:
            data = self._normalizer.normalize(data, config)
            if self._state_manager:
                self._state_manager.save_dates_from_data(data)

        if self._transformer:
            data = self._transformer.transform(data, config)

        if self._loader:
            self._loader.load(data)

        return data
