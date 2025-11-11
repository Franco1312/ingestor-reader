"""Tests for YAML config loader."""

import tempfile
from pathlib import Path

import pytest
import yaml

from src.infrastructure.config_loader import YamlConfigLoader


class TestYamlConfigLoader:
    """Tests for YamlConfigLoader class."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary config directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def config_loader(self, temp_config_dir):
        """Create a YamlConfigLoader instance."""
        return YamlConfigLoader(str(temp_config_dir))

    def test_load_dataset_config_yml(self, config_loader, temp_config_dir):
        """Test loading config from .yml file."""
        config_data = {
            "dataset_id": "test_dataset",
            "provider": "TEST",
            "source": {"kind": "http", "url": "https://example.com/data.xlsx"},
        }
        
        config_file = temp_config_dir / "test_dataset.yml"
        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f)
        
        result = config_loader.load_dataset_config("test_dataset")
        assert result == config_data

    def test_load_dataset_config_yaml(self, config_loader, temp_config_dir):
        """Test loading config from .yaml file."""
        config_data = {
            "dataset_id": "test_dataset",
            "provider": "TEST",
        }
        
        config_file = temp_config_dir / "test_dataset.yaml"
        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f)
        
        result = config_loader.load_dataset_config("test_dataset")
        assert result == config_data

    def test_load_dataset_config_not_found(self, config_loader):
        """Test loading non-existent config raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            config_loader.load_dataset_config("nonexistent")

    def test_load_dataset_config_empty_file(self, config_loader, temp_config_dir):
        """Test loading empty config file returns empty dict."""
        config_file = temp_config_dir / "empty.yml"
        config_file.touch()
        
        result = config_loader.load_dataset_config("empty")
        assert result == {}

    def test_load_dataset_config_prefers_yml_over_yaml(self, config_loader, temp_config_dir):
        """Test that .yml is preferred over .yaml."""
        yml_data = {"dataset_id": "test", "source": "yml"}
        yaml_data = {"dataset_id": "test", "source": "yaml"}
        
        with open(temp_config_dir / "test.yml", "w", encoding="utf-8") as f:
            yaml.dump(yml_data, f)
        with open(temp_config_dir / "test.yaml", "w", encoding="utf-8") as f:
            yaml.dump(yaml_data, f)
        
        result = config_loader.load_dataset_config("test")
        assert result["source"] == "yml"

