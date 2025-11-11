"""Test builders for creating test objects."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest.mock import Mock

from src.application.etl_use_case import ETLUseCase
from src.domain.interfaces import LockManager, StateManager as StateManagerInterface
from src.infrastructure.lock_managers.lock_manager_factory import LockManagerFactory
from src.infrastructure.state_managers import StateManager
from src.infrastructure.state_managers.state_manager_factory import StateManagerFactory


class ETLUseCaseBuilder:
    """Builder for ETLUseCase instances."""

    def __init__(self):
        """Initialize builder."""
        self._extractor = None
        self._parser = None
        self._normalizer = None
        self._transformer = None
        self._loader = None
        self._state_manager = None
        self._lock_manager = None

    def with_extractor(self, extractor=None):
        """Add extractor to builder."""
        self._extractor = extractor or self.create_mock_extractor()
        return self

    def with_parser(self, parser=None):
        """Add parser to builder."""
        self._parser = parser or self.create_mock_parser()
        return self

    def with_normalizer(self, normalizer=None):
        """Add normalizer to builder."""
        self._normalizer = normalizer or self.create_mock_normalizer()
        return self

    def with_transformer(self, transformer=None):
        """Add transformer to builder."""
        self._transformer = transformer or self.create_mock_transformer()
        return self

    def with_loader(self, loader=None):
        """Add loader to builder."""
        self._loader = loader or self.create_mock_loader()
        return self

    def with_state_manager(
        self,
        state_manager=None,
        state_file: Optional[str] = None,
        state_config: Optional[Dict[str, Any]] = None,
    ):
        """Add state manager to builder.

        Args:
            state_manager: StateManager instance (optional).
            state_file: Path to state file for file-based manager (optional).
            state_config: Configuration dict for factory (optional).
        """
        if state_manager:
            self._state_manager = state_manager
        elif state_config:
            self._state_manager = StateManagerFactory.create(state_config)
        elif state_file:
            self._state_manager = StateManager(state_file)
        else:
            self._state_manager = StateManager("test_state.json")
        return self

    def with_lock_manager(
        self,
        lock_manager: Optional[LockManager] = None,
        lock_config: Optional[Dict[str, Any]] = None,
    ):
        """Add lock manager to builder.

        Args:
            lock_manager: LockManager instance (optional).
            lock_config: Configuration dict for factory (optional).
        """
        if lock_manager:
            self._lock_manager = lock_manager
        elif lock_config:
            self._lock_manager = LockManagerFactory.create(lock_config)
        else:
            self._lock_manager = self.create_mock_lock_manager()
        return self

    def build(self) -> ETLUseCase:
        """Build ETLUseCase instance."""
        if self._extractor is None:
            self._extractor = self.create_mock_extractor()

        return ETLUseCase(
            extractor=self._extractor,
            parser=self._parser,
            normalizer=self._normalizer,
            transformer=self._transformer,
            loader=self._loader,
            state_manager=self._state_manager,
            lock_manager=self._lock_manager,
        )

    @staticmethod
    def create_mock_extractor():
        """Create a mock extractor."""
        extractor = Mock()
        extractor.extract.return_value = b"test data"
        return extractor

    @staticmethod
    def create_mock_parser():
        """Create a mock parser."""
        parser = Mock()
        parser.parse.return_value = [
            {
                "internal_series_code": "TEST_SERIES",
                "obs_time": datetime(2025, 1, 15),
                "value": 100.5,
            }
        ]
        return parser

    @staticmethod
    def create_mock_normalizer():
        """Create a mock normalizer."""
        normalizer = Mock()
        normalizer.normalize.return_value = [
            {
                "internal_series_code": "TEST_SERIES",
                "obs_time": datetime(2025, 1, 15),
                "value": 100.5,
            }
        ]
        return normalizer

    @staticmethod
    def create_mock_transformer():
        """Create a mock transformer."""
        transformer = Mock()
        transformer.transform.return_value = [{"transformed": True}]

        # Make transform accept data and config
        def transform_side_effect(_data, _config):
            return [{"transformed": True}]

        transformer.transform.side_effect = transform_side_effect
        return transformer

    @staticmethod
    def create_mock_loader():
        """Create a mock loader."""
        return Mock()

    @staticmethod
    def create_mock_lock_manager():
        """Create a mock lock manager."""
        lock_manager = Mock(spec=LockManager)
        lock_manager.acquire.return_value = True
        lock_manager.release.return_value = None
        return lock_manager


class ConfigBuilder:
    """Builder for ETL configuration dictionaries."""

    def __init__(self):
        """Initialize builder."""
        self._config: Dict[str, Any] = {}

    def with_parse_config(self, series_map: List[Dict[str, Any]]):
        """Add parse_config to builder."""
        if "parse_config" not in self._config:
            self._config["parse_config"] = {}
        self._config["parse_config"]["series_map"] = series_map
        return self

    def with_normalize_config(
        self, timezone: str = "UTC", primary_keys: Optional[List[str]] = None
    ):
        """Add normalize config to builder."""
        if "normalize" not in self._config:
            self._config["normalize"] = {}
        self._config["normalize"]["timezone"] = timezone
        if primary_keys is not None:
            self._config["normalize"]["primary_keys"] = primary_keys
        return self

    def with_series(self, internal_series_code: str, **kwargs):
        """Add a series to parse_config."""
        if "parse_config" not in self._config:
            self._config["parse_config"] = {}
        if "series_map" not in self._config["parse_config"]:
            self._config["parse_config"]["series_map"] = []

        series_config = {"internal_series_code": internal_series_code, **kwargs}
        self._config["parse_config"]["series_map"].append(series_config)
        return self

    def build(self) -> Dict[str, Any]:
        """Build configuration dictionary."""
        return self._config.copy()


class DataPointBuilder:
    """Builder for data point dictionaries."""

    def __init__(self):
        """Initialize builder."""
        self._data_point: Dict[str, Any] = {}

    def with_series_code(self, series_code: str):
        """Add internal_series_code to data point."""
        self._data_point["internal_series_code"] = series_code
        return self

    def with_obs_time(self, obs_time: datetime):
        """Add obs_time to data point."""
        self._data_point["obs_time"] = obs_time
        return self

    def with_value(self, value: float):
        """Add value to data point."""
        self._data_point["value"] = value
        return self

    def with_unit(self, unit: str):
        """Add unit to data point."""
        self._data_point["unit"] = unit
        return self

    def with_frequency(self, frequency: str):
        """Add frequency to data point."""
        self._data_point["frequency"] = frequency
        return self

    def build(self) -> Dict[str, Any]:
        """Build data point dictionary."""
        return self._data_point.copy()


class DataPointListBuilder:
    """Builder for lists of data points."""

    def __init__(self):
        """Initialize builder."""
        self._data_points: List[Dict[str, Any]] = []

    def add_data_point(self, data_point: Dict[str, Any]):
        """Add a data point to the list."""
        self._data_points.append(data_point.copy())
        return self

    def add(self, series_code: str, obs_time: datetime, value: float, **kwargs):
        """Add a data point with common fields.

        Args:
            series_code: Internal series code.
            obs_time: Observation time.
            value: Value.
            **kwargs: Additional fields (unit, frequency, etc.).
        """
        data_point = {
            "internal_series_code": series_code,
            "obs_time": obs_time,
            "value": value,
            **kwargs,
        }
        self._data_points.append(data_point)
        return self

    def build(self) -> List[Dict[str, Any]]:
        """Build list of data points."""
        return self._data_points.copy()


class StateManagerBuilder:
    """Builder for StateManager instances."""

    def __init__(self):
        """Initialize builder."""
        self._state_config: Optional[Dict[str, Any]] = None
        self._state_manager: Optional[StateManagerInterface] = None

    def with_file(self, state_file: str = "state.json"):
        """Configure file-based state manager.

        Args:
            state_file: Path to state file.
        """
        self._state_config = {"kind": "file", "state_file": state_file}
        return self

    def with_s3(
        self,
        bucket: str,
        key: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """Configure S3-based state manager.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            aws_access_key_id: AWS access key ID (optional).
            aws_secret_access_key: AWS secret access key (optional).
        """
        self._state_config = {
            "kind": "s3",
            "bucket": bucket,
            "key": key,
        }
        if aws_access_key_id:
            self._state_config["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            self._state_config["aws_secret_access_key"] = aws_secret_access_key
        return self

    def with_config(self, state_config: Dict[str, Any]):
        """Configure state manager with custom config dict.

        Args:
            state_config: Configuration dictionary.
        """
        self._state_config = state_config
        return self

    def with_instance(self, state_manager: StateManagerInterface):
        """Use an existing StateManager instance.

        Args:
            state_manager: StateManager instance.
        """
        self._state_manager = state_manager
        return self

    def build(self) -> Optional[StateManagerInterface]:
        """Build StateManager instance.

        Returns:
            StateManager instance or None if not configured.
        """
        if self._state_manager:
            return self._state_manager

        if self._state_config:
            return StateManagerFactory.create(self._state_config)

        # Default: file-based with default name
        return StateManagerFactory.create({"kind": "file", "state_file": "test_state.json"})
