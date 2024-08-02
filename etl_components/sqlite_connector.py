import sqlite3

from etl_components.connector import Cursor, DBConnector
from etl_components.schema import Reference, Schema, Table

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


def _get_schema(cursor: Cursor) -> Schema:
    query = "SELECT * FROM sqlite_master WHERE type = 'table'"
    cursor.execute(query)
    table_info = cursor.fetchall()

    tables = []

    for info in table_info:
        table = info["tbl_name"]
        # Fetching column
        query = f"SELECT name FROM pragma_table_info('{table}')"
        cursor.execute(query)
        column_info = cursor.fetchall()
        columns = [col["name"] for col in column_info]
        # fetching references
        query = f"SELECT * FROM pragma_foreign_key_list('{table}')"
        cursor.execute(query)
        reference_info = cursor.fetchall()
        references = [
            Reference(ref["from"], ref["table"], ref["to"])
            for ref in reference_info
        ]
        tables.append(Table(table, info["sql"], columns, references))

    # filling in referred_by
    for table in tables:
        referred_by = [
            other_table.name
            for other_table in tables
            if table.name in other_table.refers_to
        ]
        table.referred_by = referred_by

    return Schema(tables)


class SQLiteConnector(DBConnector):
    """Connector for SQLite databases."""

    connector = sqlite3  # type: ignore

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

    def get_schema(self) -> Schema:
        """Retrieve schema from the database."""
        with self.cursor() as cursor:
            return _get_schema(cursor)
