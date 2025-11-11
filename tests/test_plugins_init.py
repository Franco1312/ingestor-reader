"""Tests for plugin initialization."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins import create_plugin_registry, initialize_plugins


class TestPluginInitialization:
    """Tests for plugin initialization functions."""

    def test_initialize_plugins_registers_all(self):
        """Test that initialize_plugins registers all available plugins."""
        registry = PluginRegistry()
        initialize_plugins(registry)

        # Verify extractors are registered
        source_config = {"url": "https://example.com"}
        extractor = registry.get_extractor("http", source_config)
        assert extractor is not None

        # Verify parsers are registered
        parser = registry.get_parser("bcra_infomondia")
        assert parser is not None

        # Verify normalizers are registered
        normalizer = registry.get_normalizer("bcra_infomondia")
        assert normalizer is not None

    def test_create_plugin_registry_returns_initialized(self):
        """Test that create_plugin_registry returns initialized registry."""
        registry = create_plugin_registry()

        assert isinstance(registry, PluginRegistry)

        # Verify plugins are registered
        source_config = {"url": "https://example.com"}
        extractor = registry.get_extractor("http", source_config)
        assert extractor is not None
