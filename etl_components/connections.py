import sqlite3
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Union

import psycopg
from dotenv import dotenv_values
from psycopg.rows import dict_row

config = dotenv_values(".env")

SQLITE_INSERT_FORMAT = """
    INSERT INTO <table> (<column1>, <column2>, ...)
    VALUES (:<column1_source>, :<column2_source>, ...)
"""
SQLITE_VALUES_PATTERN = r":(\w+)"

POSTGRES_INSERT_FORMAT = """
    INSERT INTO <table> (<column1>, <column2>, ...)
    VALUES (%(<column1_source>)s, %(<column2_source>)s, ...)
"""
POSTGRES_VALUES_PATTERN = r"\((\w+)\)s"

# SQLite and PostgreSQL use the same format for retrieval
RETRIEVE_FORMAT = """
    SELECT id as <table>_id, <column1> as <alias1>, <column2> FROM <table>
"""
COMPARE_FORMAT = """
    SELECT 
        <table>.<column> as <alias>,
        <table>.<column>,
        <column>,
        ...
    FROM <table>
        JOIN <other_table> ON <other_table>.<table>_id = <table>.id
        JOIN ...
    ...
"""


@dataclass
class CursorFormats:
    """Stores formats and patterns of SQL queries."""

    insert_format: str
    values_pattern: str
    retrieve_format: str
    compare_format: str


sqlite_formats = CursorFormats(
    SQLITE_INSERT_FORMAT, SQLITE_VALUES_PATTERN, RETRIEVE_FORMAT, COMPARE_FORMAT
)

postgres_formats = CursorFormats(
    POSTGRES_INSERT_FORMAT,
    POSTGRES_VALUES_PATTERN,
    RETRIEVE_FORMAT,
    COMPARE_FORMAT,
)

Cursor = sqlite3.Cursor | psycopg.Cursor


def get_cursor_formats(cursor: Cursor) -> CursorFormats:
    """Get formats for this cursor.

    Args:
    ----
        cursor: active cursor

    Returns:
    -------
        CursorFormat

    """
    if isinstance(cursor, sqlite3.Cursor):
        return sqlite_formats
    if isinstance(cursor, psycopg.Cursor):
        return postgres_formats


class RollbackCausedError(Exception):
    """Exception raised when the PostgresCursor catches and runs a rollback."""

    pass


def _dict_row(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict:
    """Row Factory that converts SQLite output into dictionaries.

    Args:
    ----
        cursor: SQLite cursor
        row: data row output

    Returns:
    -------
        data in dictionary format.

    """
    r = sqlite3.Row(cursor, row)
    return dict(zip(r.keys(), tuple(r)))


class SQLiteCursor:
    """Context manager for handling connections with PostgreSQL server.

    Assumes the following variables are set in .env:
        SQLITE_DB: filename to sqlite DB. If not available, defaults to in memory db.
    """

    def __init__(self, db: Union[str, None] = None) -> None:
        """Create a connection to SQLite database.

        Args:
        ----
            db: filename of SQLite database. If None, creates database in memory. (default: None)

        """
        db = ":memory:" if db is None else db
        self.connection = sqlite3.connect(db)
        self.connection.row_factory = _dict_row

    def __enter__(self) -> sqlite3.Cursor:
        """Enter by creating a cursor to interact with the SQLite database.

        Returns
        -------
            sqlite3 cursor.

        """
        return self.connection.cursor()

    def __exit__(
        self, exception: Exception, message: str, traceback: TracebackType
    ) -> None:
        """Exit by commiting and closing the connection, or rolling back on exception.

        Args:
        ----
            exception: raised exception in the context manager
            message: message the exception is raised with
            traceback: traceback for the exception

        Raises:
        ------
            RollbackCausedError: when an exception occurs within the context.

        """
        if exception:
            self.connection.rollback()
            self.connection.close()
            raise RollbackCausedError(message)
        self.connection.commit()
        self.connection.close()


class PostgresCursor:
    """Context manager for handling connections with PostgreSQL server.

    Assumes the following variables are set in .env:
        HOST: database host ip to PostgreSQL server
        PORT: port to which PostgreSQL server listens (usually 5432)
        DB: name of database to connect to
        USER: username that has right on database
        PASSWORD: to authenticate user

    Attributes
    ----------
        connection: connection with the server, using dict_row row factory.

    """

    def __init__(self) -> None:
        """Create a connection with the PostgreSQL server."""
        host = config["HOST"]
        port = config["PORT"]
        database = config["DB"]
        user = config["USER"]
        password = config["PASSWORD"]

        credentials = f"dbname={database} user={user} password={password} host={host} port={port}"
        self.connection = psycopg.connect(credentials, row_factory=dict_row)

    def __enter__(self) -> psycopg.Cursor:
        """Enter by creating a cursor to interact with the PostgreSQL server.

        Returns
        -------
            psycopg cursor.

        """
        return self.connection.cursor()

    def __exit__(
        self, exception: Exception, message: str, traceback: TracebackType
    ) -> None:
        """Exit by commiting and closing the connection, or rolling back on exception.

        Args:
        ----
            exception: raised exception in the context manager
            message: message the exception is raised with
            traceback: traceback for the exception

        Raises:
        ------
            RollbackCausedError: when an exception occurs within the context.

        """
        if exception:
            self.connection.rollback()
            self.connection.close()
            raise RollbackCausedError(message)
        self.connection.commit()
        self.connection.close()
