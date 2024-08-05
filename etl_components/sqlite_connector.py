import sqlite3
from contextlib import contextmanager
from typing import Iterator

from etl_components.connector import Cursor, DBConnector

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


def _get_tables(cursor: Cursor) -> list[str]:
    """Get list of tables from SQLite database."""
    query = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
    cursor.execute(query)
    return [row["tbl_name"] for row in cursor.fetchall()]


def _get_table_schema(cursor: Cursor, table_name: str) -> str:
    """Get SQL schema for this table from SQLite database."""
    query = f"SELECT sql FROM sqlite_master WHERE tbl_name = '{table_name}'"
    cursor.execute(query)
    return cursor.fetchall()[0]["sql"]


def _get_columns(cursor: Cursor, table_name: str) -> list[str]:
    """Get columns for this table from SQLite database."""
    query = f"SELECT name FROM pragma_table_info('{table_name}')"
    cursor.execute(query)
    return [row["name"] for row in cursor.fetchall()]


def _get_references(cursor: Cursor, table_name: str) -> list[dict[str, str]]:
    """Get references for this table from SQLite database."""
    query = f"SELECT * FROM pragma_foreign_key_list('{table_name}')"
    cursor.execute(query)
    return [
        {"column": row["from"], "table": row["table"], "to": row["to"]}
        for row in cursor.fetchall()
    ]


class SQLiteConnector(DBConnector):
    """Connector for SQLite databases."""

    connection: sqlite3.Connection

    def __init__(self, credentials: str) -> None:
        """Create a SQLiteConnector that connects to the database at the given credentials.

        Args:
        ----
            credentials: filename of sqlite database.

        """
        self.credentials = credentials

    def connect(self) -> sqlite3.Connection:
        """Make a connection to the SQLite database."""
        connection = sqlite3.connect(
            self.credentials,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        connection.row_factory = _dict_row
        return connection

    # Shadowing the context manager for cursor to get the correct cursor type for LSP
    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        """Context manager for sqlite3.cursor."""
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
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

    def get_tables(self) -> list[str]:
        """Retrieve list of tables from the database."""
        with self.cursor() as cursor:
            return _get_tables(cursor)

    def get_table_schema(self, table_name: str) -> str:
        """Retrieve SQL schema for this table from the database."""
        with self.cursor() as cursor:
            return _get_table_schema(cursor, table_name)

    def get_columns(self, table_name: str) -> list[str]:
        """Retrieve a list of columns for this table form the database."""
        with self.cursor() as cursor:
            return _get_columns(cursor, table_name)

    def get_references(self, table_name: str) -> list[dict[str, str]]:
        """Retrieve a list of references for this table from the database."""
        with self.cursor() as cursor:
            return _get_references(cursor, table_name)
