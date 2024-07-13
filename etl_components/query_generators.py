def create_retrieve_query(table: str, columns: list[str]) -> str:
    """Create a retrieve query for this table and columns.

    Args:
    ----
        table: name of table to retrieve from
        columns: dictionary of {column: value, ...} pairs

    Returns:
    -------
        valid retrieve query for this connector

    """
    column_section = ", ".join(columns)
    return f"SELECT id, {column_section} FROM {table}"
