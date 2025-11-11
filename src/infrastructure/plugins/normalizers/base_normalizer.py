"""Base normalizer plugin."""

from src.domain.interfaces import Normalizer


class BaseNormalizer(Normalizer):
    """Base normalizer implementation."""

    def normalize(self, data, config):
        """Normalize data according to config."""
        pass

