"""Manager kind enumerations."""

from enum import Enum


class StateManagerKind(str, Enum):
    """State manager implementation kinds."""

    FILE = "file"
    S3 = "s3"


class LockManagerKind(str, Enum):
    """Lock manager implementation kinds."""

    DYNAMODB = "dynamodb"
