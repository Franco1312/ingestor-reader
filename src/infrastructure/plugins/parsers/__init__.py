"""Parser plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.parsers.bcra_infomondia_parser import BcraInfomondiaParser


def register_parsers(registry: PluginRegistry) -> None:
    """Register all parser plugins.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    registry.register_parser("bcra_infomondia", BcraInfomondiaParser)
