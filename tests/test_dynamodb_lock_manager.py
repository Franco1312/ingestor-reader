"""Tests for DynamoDBLockManager."""

from unittest.mock import Mock, patch

from src.infrastructure.lock_managers.dynamodb_lock_manager import DynamoDBLockManager


class TestDynamoDBLockManager:
    """Tests for DynamoDBLockManager class."""

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_init_with_table_name(self, _mock_boto3):
        """Test initialization with table name."""
        manager = DynamoDBLockManager(table_name="test_locks")

        # Verify initialization without accessing private members
        assert hasattr(manager, "acquire")
        assert hasattr(manager, "release")

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_init_with_all_parameters(self, _mock_boto3):
        """Test initialization with all parameters."""
        manager = DynamoDBLockManager(
            table_name="test_locks",
            region_name="us-east-1",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
        )

        # Verify initialization without accessing private members
        assert hasattr(manager, "acquire")
        assert hasattr(manager, "release")

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_acquire_success(self, mock_boto3):
        """Test successful lock acquisition."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks")
        result = manager.acquire("test_key", timeout_seconds=300)

        assert result is True
        mock_client.put_item.assert_called_once()
        call_kwargs = mock_client.put_item.call_args
        assert call_kwargs[1]["TableName"] == "test_locks"
        assert "lock_key" in call_kwargs[1]["Item"]
        assert "expiration_time" in call_kwargs[1]["Item"]
        assert "acquired_at" in call_kwargs[1]["Item"]

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_acquire_failure_already_locked(self, mock_boto3):
        """Test lock acquisition when already locked."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # Create exception class
        class ConditionalCheckFailedException(Exception):
            pass

        # Set up exceptions namespace
        mock_exceptions = Mock()
        mock_exceptions.ConditionalCheckFailedException = ConditionalCheckFailedException
        mock_client.exceptions = mock_exceptions

        # Simulate ConditionalCheckFailedException
        mock_client.put_item.side_effect = ConditionalCheckFailedException()

        manager = DynamoDBLockManager(table_name="test_locks")
        result = manager.acquire("test_key", timeout_seconds=300)

        assert result is False

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_release_success(self, mock_boto3):
        """Test successful lock release."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks")
        manager.release("test_key")

        mock_client.delete_item.assert_called_once()
        call_kwargs = mock_client.delete_item.call_args
        assert call_kwargs[1]["TableName"] == "test_locks"
        assert call_kwargs[1]["Key"]["lock_key"]["S"] == "test_key"

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_release_table_not_found(self, mock_boto3):
        """Test lock release when table doesn't exist."""

        # Create exception class
        class ResourceNotFoundException(Exception):
            pass

        # Create a simple object with the exception as attribute using type()
        exceptions_ns = type(
            "exceptions", (), {"ResourceNotFoundException": ResourceNotFoundException}
        )()

        mock_client = Mock()
        mock_client.exceptions = exceptions_ns
        mock_client.delete_item.side_effect = ResourceNotFoundException()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks")
        # Should not raise, should handle gracefully
        manager.release("test_key")

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_release_client_error(self, mock_boto3):
        """Test lock release when client error occurs."""

        # Create exception class
        class ClientError(Exception):
            pass

        # Create a simple object with the exception as attribute using type()
        exceptions_ns = type("exceptions", (), {"ClientError": ClientError})()

        mock_client = Mock()
        mock_client.exceptions = exceptions_ns
        mock_client.delete_item.side_effect = ClientError()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks")
        # Should not raise, should handle gracefully
        manager.release("test_key")

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_get_client_creates_lazy(self, mock_boto3):
        """Test that client is created lazily on first use."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks")
        # Client not created yet
        mock_boto3.client.assert_not_called()

        # First use creates client
        manager.acquire("test_key")
        mock_boto3.client.assert_called_once_with("dynamodb")

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_get_client_reuses_instance(self, mock_boto3):
        """Test that client instance is reused."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks")

        # First call
        manager.acquire("test_key")
        # Second call should reuse same client
        manager.release("test_key")

        # Client should only be created once
        assert mock_boto3.client.call_count == 1

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_get_client_with_region(self, mock_boto3):
        """Test client creation with region."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(table_name="test_locks", region_name="us-west-2")
        manager.acquire("test_key")

        mock_boto3.client.assert_called_once_with("dynamodb", region_name="us-west-2")

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_get_client_with_credentials(self, mock_boto3):
        """Test client creation with credentials."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(
            table_name="test_locks",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )
        manager.acquire("test_key")

        mock_boto3.client.assert_called_once_with(
            "dynamodb",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )

    @patch("src.infrastructure.lock_managers.dynamodb_lock_manager.boto3")
    def test_get_client_filters_none_values(self, mock_boto3):
        """Test that None values are filtered from client kwargs."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        manager = DynamoDBLockManager(
            table_name="test_locks",
            region_name="us-east-1",
            aws_access_key_id=None,
            aws_secret_access_key=None,
        )
        manager.acquire("test_key")

        # Should only pass region_name, not None values
        mock_boto3.client.assert_called_once_with("dynamodb", region_name="us-east-1")
