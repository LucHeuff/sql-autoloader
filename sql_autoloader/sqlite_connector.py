import sqlite3
from typing import Self

from sql_autoloader.connector import DBConnector
from sql_autoloader.schema import ReferenceDict, TableDict

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


def _get_retrieve_query(
    table: str, key: str, alias: str, columns: list[str]
) -> str:
    """Get a retrieve query appropriate for a SQLite database.

    Args:
    ----
        table: to retrieve from
        key: name of the primary key
        alias: for the primary key
        columns: to read values from

    Returns:
    -------
        valid retrieve query

    """
    columns_section = ", ".join(columns)
    return f"SELECT {key} as {alias}, {columns_section} FROM {table}"


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


def _fetch_schema(
    cursor: sqlite3.Cursor,
) -> tuple[list[TableDict], list[ReferenceDict]]:
    """Get schema from SQLite database."""
    # fetching table names
    query = "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
    cursor.execute(query)
    tables = [row["tbl_name"] for row in cursor.fetchall()]

    table_dicts = []
    reference_dicts = []

    # fetching columns and references
    for table in tables:
        query = f"""
        SELECT
          name,
          pk AS primary_key,
          "table" as to_table,
          "to" as to_key
        FROM
          pragma_table_info ('{table}') AS info
          LEFT JOIN pragma_foreign_key_list ('{table}') AS foreign_keys ON foreign_keys."from" = info.name
        """
        cursor.execute(query)
        column_info = cursor.fetchall()

        primary_key = []
        foreign_keys = []
        columns = []

        for row in column_info:
            column = row["name"]
            if row["primary_key"]:
                primary_key.append(column)
            elif row["to_table"] is not None:
                foreign_keys.append(column)
                reference_dicts.append(
                    {
                        "from_table": table,
                        "from_key": column,
                        "to_table": row["to_table"],
                        "to_key": row["to_key"],
                    }
                )
            else:
                columns.append(column)

        assert len(primary_key) <= 1, "Cannot have more than 1 primary key"
        primary_key = primary_key[0] if len(primary_key) == 1 else ""
        assert (
            foreign_keys != columns
        ), "Foreign keys and columns cannot be the same."

        table_dict = {
            "name": table,
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys,
        }

        table_dicts.append(table_dict)

    return table_dicts, reference_dicts


class SQLiteConnector(DBConnector):
    """Connector for SQLite databases."""

    cursor: sqlite3.Cursor

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
        """Enter context manager for SQLiteConnector by opening a connection and creating a cursor.

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
        self.cursor = self.connection.cursor()
        self.schema = self.get_schema()
        return self

    def __exit__(
        self, exception: object, value: object, traceback: object
    ) -> None:
        """Exit context manager by closing connection, and rolling back on an exception."""
        if exception:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.cursor.close()
        self.connection.close()

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

    def get_retrieve_query(
        self, table: str, key: str, alias: str, columns: list[str]
    ) -> str:
        """Get a retrieve query appropriate for a SQLite database.

        Args:
        ----
            table: to retrieve from
            key: name of the primary key
            alias: for the primary key
            columns: to read values from

        Returns:
        -------
            valid insert query

        """
        return _get_retrieve_query(table, key, alias, columns)

    def fetch_schema(self) -> tuple[list[TableDict], list[ReferenceDict]]:
        """Retrieve schema from the database."""
        return _fetch_schema(self.cursor)
