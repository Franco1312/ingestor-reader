"""AWS utility functions."""

import os
from typing import Any, Optional

import boto3


def create_s3_client(
    aws_region: str = "us-east-1", s3_client: Optional[Any] = None
) -> Any:
    """Create an S3 client with explicit credentials.

    This function ensures credentials are passed explicitly to avoid issues
    with expired session tokens in the environment.

    Args:
        aws_region: AWS region (default: us-east-1).
        s3_client: Existing S3 client to use (for testing).

    Returns:
        Boto3 S3 client instance.
    """
    if s3_client:
        return s3_client

    # Pass credentials explicitly to avoid issues with expired session tokens
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    client_kwargs = {"region_name": aws_region}
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs["aws_access_key_id"] = aws_access_key_id
        client_kwargs["aws_secret_access_key"] = aws_secret_access_key

    return boto3.client("s3", **client_kwargs)

