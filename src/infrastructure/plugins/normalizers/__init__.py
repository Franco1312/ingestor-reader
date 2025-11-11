"""Normalizer plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)


def register_normalizers(registry: PluginRegistry) -> None:
    """Register all normalizer plugins.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    registry.register_normalizer("bcra_infomondia", BcraInfomondiaNormalizer)
