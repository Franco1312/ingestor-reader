"""Application configuration loader."""

import importlib.util
import logging
import os
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _load_config_module(environment: str) -> Dict[str, Any]:
    """Load configuration module for the given environment.

    Args:
        environment: Environment name (local, staging, production).

    Returns:
        Configuration dictionary from the module's config variable.
    """
    config_dir = Path(__file__).parent.parent.parent / "config"
    config_file = config_dir / f"{environment}.py"

    if not config_file.exists():
        logger.warning("Configuration file not found: %s", config_file)
        return {}

    try:
        spec = importlib.util.spec_from_file_location(
            f"config_{environment}", config_file
        )
        if spec is None or spec.loader is None:
            logger.warning("Failed to create spec for config file: %s", config_file)
            return {}

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "config"):
            logger.warning("Config module %s does not have 'config' attribute", config_file)
            return {}

        return module.config
    except (ImportError, AttributeError, OSError) as e:
        logger.warning("Failed to load configuration from %s: %s", config_file, e)
        return {}


def load_config() -> Dict[str, Any]:
    """Load application configuration based on ENVIRONMENT variable.

    Loads config from config/{environment}.py where environment is
    determined by ENVIRONMENT env var (defaults to 'local').

    Returns:
        Configuration dictionary from the selected environment module.
    """
    environment = os.environ.get("ENVIRONMENT", "local")

    # Validate environment
    valid_environments = {"local", "staging", "production"}
    if environment not in valid_environments:
        logger.warning(
            "Invalid ENVIRONMENT '%s', must be one of %s. Defaulting to 'local'",
            environment,
            valid_environments,
        )
        environment = "local"

    return _load_config_module(environment)

