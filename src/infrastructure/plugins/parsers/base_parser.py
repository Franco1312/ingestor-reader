"""Base parser plugin."""

from src.domain.interfaces import Parser


class BaseParser(Parser):
    """Base parser implementation."""

    def parse(self, raw_data, config):
        """Parse raw data according to config."""
        pass

