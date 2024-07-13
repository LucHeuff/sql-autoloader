from typing import Callable


def create_insert_query(
    table: str,
    columns: list[str],
    insert_prefix: str,
    insert_postfix: str,
    parameterize_method: Callable[[str], str],
) -> str:
    """Create an insert query for this database connector.

    To be used in combination with for example sqlite3, psycopg, etc.

    Args:
    ----
        table: to be inserted into
        columns: to be inserted
        insert_prefix: allows passing a prefix before INTO
        insert_postfix: allows passing a postfix after VALUES
        parameterize_method: used to convert the column names into
                             the parameterized format for this database connector

    Returns:
    -------
        valid insert query for this connector


    """
    columns_section = ", ".join(columns)
    values_section = ", ".join([parameterize_method(col) for col in columns])
    return f"{insert_prefix} INTO {table} ({columns_section})\nVALUES ({values_section}){insert_postfix}".strip()


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
