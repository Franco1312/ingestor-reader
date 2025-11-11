"""Tests for Excel utilities."""

from src.infrastructure.utils.excel_utils import excel_column_to_index


class TestExcelColumnToIndex:
    """Tests for excel_column_to_index function."""

    def test_single_letter_columns(self):
        """Test single letter columns."""
        assert excel_column_to_index("A") == 0
        assert excel_column_to_index("B") == 1
        assert excel_column_to_index("Z") == 25

    def test_double_letter_columns(self):
        """Test double letter columns."""
        assert excel_column_to_index("AA") == 26
        assert excel_column_to_index("AB") == 27
        assert excel_column_to_index("AZ") == 51

    def test_triple_letter_columns(self):
        """Test triple letter columns."""
        assert excel_column_to_index("AAA") == 702
        assert excel_column_to_index("AAB") == 703

    def test_case_insensitive(self):
        """Test that function is case insensitive."""
        assert excel_column_to_index("a") == 0
        assert excel_column_to_index("Aa") == 26
        assert excel_column_to_index("aA") == 26

    def test_specific_columns_from_config(self):
        """Test specific columns used in config."""
        assert excel_column_to_index("A") == 0
        assert excel_column_to_index("C") == 2
        assert excel_column_to_index("P") == 15
        assert excel_column_to_index("AD") == 29
        assert excel_column_to_index("X") == 23
        assert excel_column_to_index("Z") == 25

