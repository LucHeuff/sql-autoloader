import sqlite3
from contextlib import contextmanager
from typing import Callable, Iterator, Self

from etl_components.connector import DBConnector

# ---- Functions for getting SQL queries for the sqlite3 connector


def _get_insert_query(table: str, columns: list[str]) -> str:
    """Get an insert query appropriate for a SQLite database.

    Args:
    ----
        table: to insert into
        columns: to insert values into

    Returns:
    -------
        valid insert query

    """
    columns_section = ", ".join(columns)
    values_section = ", ".join([f":{col}" for col in columns])
    return f"INSERT OR IGNORE INTO {table} ({columns_section}) VALUES ({values_section})"


def _get_retrieve_query(table: str, columns: list[str]) -> str:
    """Get a retrieve query appropriate for a SQLite database.

    Args:
    ----
        table: to retrieve from
        columns: to read values from

    Returns:
    -------
        valid insert query

    """
    columns_section = ", ".join(columns)
    return f"SELECT id as {table}_id, {columns_section} FROM {table}"


def _dict_row(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """Row Factory that converts SQLite outputs into dictionaries.

    Args:
    ----
        cursor: SQLite cursor
        row: data row output

    Returns:
    -------
        data in dictionary format

    """
    r = sqlite3.Row(cursor, row)
    return dict(zip(r.keys(), tuple(r)))


# ---- functions for fetching schema information from the database


def _get_unique_constraints(
    cursor: sqlite3.Cursor,
    table_name: str,
    length_filter: Callable[[int], bool],
) -> list[str]:
    """Get a list of uniqueness constraints on a table, depending on a length filter."""
    # SQLite stores information about unique constraints in the indexes for a table. So getting those first
    indexes_query = f"""SELECT name FROM pragma_index_list('{table_name}') WHERE "unique" = 1 """
    cursor.execute(indexes_query)
    indexes = cursor.fetchall()
    # indexes can be one or more columns long. If they are one, these are a column constraint.
    # if they are more, they are a table constraint.
    unique_columns = []
    for index in indexes:
        index_query = f"SELECT name FROM pragma_index_info('{index["name"]}')"
        cursor.execute(index_query)
        unique_names = cursor.fetchall()
        if length_filter(len(unique_names)):
            unique_columns += [u["name"] for u in unique_names]

    return unique_columns


def _get_tables(cursor: sqlite3.Cursor) -> list[tuple[str, list[str]]]:
    """Get list of tables from SQLite database."""
    query = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
    cursor.execute(query)
    tables = [row["tbl_name"] for row in cursor.fetchall()]
    constraints = [
        _get_unique_constraints(cursor, table, lambda x: x > 1)
        for table in tables
    ]

    return list(zip(tables, constraints))


def _get_columns(cursor: sqlite3.Cursor, table_name: str) -> list[str]:
    """Get columns for this table from SQLite database."""
    columns_query = f"""
    SELECT 
        name, 
        type AS dtype,
        "notnull" AS nullable,
        "dflt_value" AS default_value,
        pk AS primary_key 
    FROM pragma_table_info('{table_name}')
    """
    references_query = f"""
    SELECT 
        "from" as name,
        "table" as to_table,
        "to" as to_column,
        on_delete,
        1 AS foreign_key
    FROM pragma_foreign_key_list('{table_name}')
    """
    # getting column information
    cursor.execute(columns_query)
    columns = cursor.fetchall()

    # getting foreign key information
    cursor.execute(references_query)
    references = cursor.fetchall()

    # getting columns with uniqueness constraints
    # -> recognise column constraint refers to only one name
    unique_columns = _get_unique_constraints(
        cursor, table_name, lambda x: x == 1
    )

    # processing column information into the right formats
    for col in columns:
        col["primary_key"] = bool(col["primary_key"])
        col["nullable"] = not col["nullable"]  # flip 'notnull' boolean
        col["unique"] = col["name"] in unique_columns

        for ref in references:
            if col["name"] == ref["name"]:
                col.update(ref)

    return columns


class SQLiteConnector(DBConnector):
    """Connector for SQLite databases."""

    def __init__(
        self, credentials: str, *, allow_custom_dtypes: bool = False
    ) -> None:
        """Create a SQLiteConnector that connects to the database at the given credentials.

        Args:
        ----
            credentials: filename of sqlite database.
            allow_custom_dtypes: sets `detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES`
                                 in sqlite3 connection.
                                 NOTE: this may give confusing errors due to sqlite3
                                 auto transforming your columns, so define your own
                                 adapters/converters and use at your own risk!

        """
        self.credentials = credentials
        self.allow_custom_dtypes = allow_custom_dtypes

    def __enter__(self) -> Self:
        """Enter context manager for SQLiteConnector by opening a connection.

        Returns
        -------
            instance of SQLiteConnector

        """
        detect_types = (
            sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            if self.allow_custom_dtypes
            else 0
        )
        self.connection = sqlite3.connect(
            self.credentials, detect_types=detect_types
        )
        self.connection.row_factory = _dict_row
        self.connection.autocommit = False
        return self

    def __exit__(self, *exception: object) -> None:
        """Exit context manager by closing connection."""
        self.connection.close()

    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        """Create a new cursor to the database.

        Yields
        ------
           sqlite3.Cursor

        """
        cursor = self.connection.cursor()
        try:
            yield cursor
        except:
            self.connection.rollback()
        finally:
            self.connection.commit()
            cursor.close()

    # ---- query generation methods

    def get_insert_query(self, table: str, columns: list[str]) -> str:
        """Get an insert query appropriate for a SQLite database.

        Args:
        ----
                    table: to insert into
                    columns: to insert values into

        Returns:
        -------
                    valid insert query

        """
        return _get_insert_query(table, columns)

    def get_retrieve_query(self, table: str, columns: list[str]) -> str:
        """Get a retrieve query appropriate for a SQLite database.

        Args:
        ----
            table: to retrieve from
            columns: to read values from

        Returns:
        -------
            valid insert query

        """
        return _get_retrieve_query(table, columns)

    def get_tables(self) -> list[tuple[str, list[str]]]:
        """Retrieve list of tables from the database."""
        with self.cursor() as cursor:
            return _get_tables(cursor)

    def get_columns(self, table_name: str) -> list[str]:
        """Retrieve a list of columns for this table form the database."""
        with self.cursor() as cursor:
            return _get_columns(cursor, table_name)
