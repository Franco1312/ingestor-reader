"""Tests for CLI."""

from unittest.mock import Mock, patch

import pytest

from src.cli import _execute_etl_pipeline, _handle_error, run_etl


class TestExecuteETLPipeline:
    """Tests for _execute_etl_pipeline function."""

    @patch("src.cli.create_plugin_registry")
    @patch("src.cli.YamlConfigLoader")
    @patch("src.cli.ETLUseCase")
    def test_execute_etl_pipeline_success(self, mock_etl_class, mock_config_loader_class, mock_registry):
        """Test successful ETL pipeline execution."""
        # Setup mocks
        mock_registry_instance = Mock()
        mock_registry.return_value = mock_registry_instance
        mock_registry_instance.get_extractor.return_value = Mock()
        mock_registry_instance.get_parser.return_value = Mock()
        mock_registry_instance.get_normalizer.return_value = Mock()
        
        mock_config_loader = Mock()
        mock_config_loader_class.return_value = mock_config_loader
        mock_config_loader.load_dataset_config.return_value = {
            "dataset_id": "test_dataset",
            "source": {"kind": "http", "url": "https://example.com"},
            "parse": {"plugin": "bcra_infomondia"},
            "normalize": {"plugin": "bcra_infomondia"},
        }
        
        mock_etl_instance = Mock()
        mock_etl_class.return_value = mock_etl_instance
        mock_etl_instance.execute.return_value = [{"data": "test"}]
        
        # Execute
        result = _execute_etl_pipeline("test_dataset")
        
        # Verify
        assert result == 0
        mock_config_loader.load_dataset_config.assert_called_once_with("test_dataset")
        mock_etl_instance.execute.assert_called_once()

    @patch("src.cli.create_plugin_registry")
    @patch("src.cli.YamlConfigLoader")
    def test_execute_etl_pipeline_file_not_found(self, mock_config_loader_class, mock_registry):
        """Test FileNotFoundError is raised when config file not found."""
        mock_config_loader = Mock()
        mock_config_loader_class.return_value = mock_config_loader
        mock_config_loader.load_dataset_config.side_effect = FileNotFoundError("Config not found")
        
        with pytest.raises(FileNotFoundError):
            _execute_etl_pipeline("nonexistent")


class TestHandleError:
    """Tests for _handle_error function."""

    def test_handle_file_not_found_error(self):
        """Test handling FileNotFoundError."""
        error = FileNotFoundError("Config not found")
        result = _handle_error(error)
        
        assert result == 1

    def test_handle_value_error(self):
        """Test handling ValueError."""
        error = ValueError("Invalid config")
        result = _handle_error(error)
        
        assert result == 1

    def test_handle_runtime_error(self):
        """Test handling RuntimeError."""
        error = RuntimeError("Lock failed")
        result = _handle_error(error)
        
        assert result == 1

    def test_handle_keyboard_interrupt(self):
        """Test handling KeyboardInterrupt."""
        error = KeyboardInterrupt()
        result = _handle_error(error)
        
        assert result == 130

    def test_handle_unexpected_error(self):
        """Test handling unexpected error type."""
        error = AttributeError("Unexpected error")
        result = _handle_error(error)
        
        assert result == 1


class TestRunETL:
    """Tests for run_etl function."""

    @patch("src.cli._execute_etl_pipeline")
    def test_run_etl_success(self, mock_execute):
        """Test successful ETL execution."""
        mock_execute.return_value = 0
        
        result = run_etl("test_dataset")
        
        assert result == 0
        mock_execute.assert_called_once_with("test_dataset")

    @patch("src.cli._execute_etl_pipeline")
    @patch("src.cli._handle_error")
    def test_run_etl_handles_error(self, mock_handle, mock_execute):
        """Test error handling in run_etl."""
        mock_execute.side_effect = ValueError("Test error")
        mock_handle.return_value = 1
        
        result = run_etl("test_dataset")
        
        assert result == 1
        mock_handle.assert_called_once()

    @patch("src.cli._execute_etl_pipeline")
    @patch("src.cli._handle_error")
    def test_run_etl_handles_keyboard_interrupt(self, mock_handle, mock_execute):
        """Test KeyboardInterrupt handling."""
        mock_execute.side_effect = KeyboardInterrupt()
        mock_handle.return_value = 130
        
        result = run_etl("test_dataset")
        
        assert result == 130
        mock_handle.assert_called_once()

