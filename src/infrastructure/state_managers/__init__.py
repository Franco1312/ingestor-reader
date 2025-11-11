"""State manager implementations."""

from src.infrastructure.state_managers.file_state_manager import FileStateManager
from src.infrastructure.state_managers.state_manager_factory import StateManagerFactory

# Alias for backward compatibility
StateManager = FileStateManager

__all__ = [
    "FileStateManager",
    "StateManager",
    "StateManagerFactory",
]

