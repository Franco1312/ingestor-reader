"""Excel utility functions for parsers."""


def excel_column_to_index(column: str) -> int:
    """Convert Excel column letter(s) to 0-based index.
    
    Args:
        column: Excel column letter(s) (e.g., 'A', 'Z', 'AA').
        
    Returns:
        0-based column index.
        
    Examples:
        >>> excel_column_to_index('A')
        0
        >>> excel_column_to_index('Z')
        25
        >>> excel_column_to_index('AA')
        26
        >>> excel_column_to_index('AD')
        29
    """
    index = 0
    for char in column:
        index = index * 26 + (ord(char.upper()) - ord('A') + 1)
    return index - 1

