"""File extractor plugin."""

from src.domain.interfaces import Extractor


class FileExtractor(Extractor):
    """Extract data from file source."""

    def extract(self):
        """Extract data from file source."""
        pass

