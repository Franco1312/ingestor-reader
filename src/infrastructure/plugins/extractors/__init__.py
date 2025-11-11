"""Extractor plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.extractors.http_extractor import HttpExtractor


def register_extractors(registry: PluginRegistry) -> None:
    """Register all extractor plugins.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    registry.register_extractor("http", HttpExtractor)
