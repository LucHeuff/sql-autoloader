from abc import ABC
from typing import Protocol


class Cursor(Protocol):
    """A cursor to interact with the database."""

    def execute(self, *args: tuple, **kwargs: dict) -> None:
        """Execute a query."""
        ...

    def executemany(self, *args: tuple, **kwargs: dict) -> None:
        """Execute a query for many rows."""
        ...

    def fetchall(self) -> list[dict]:
        """Return results from query."""
        ...


class Connection(Protocol):
    """A connection to the database."""

    def cursor(self) -> Cursor:
        """Create a cursor."""
        ...

    def commit(self) -> None:
        """Commit a transaction."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    def rollback(self) -> None:
        """Roll back the transaction."""
        ...


class DBConnector(ABC):
    """Abstract base class for connector with a database."""

    connection: Connection

    def __enter__(self) -> Cursor:
        """Enter context manager by creating a cursor to interact with the database.

        Returns
        -------
            cursor to interact with the database.

        """
        return self.connection.cursor()

    def __exit__(self, *exception: tuple) -> None:
        """Exit context manager by committing or rolling back on exception, and closing the connection.

        Args:
        ----
            exception: raised exception while inside the context manager

        """
        if exception:
            self.connection.rollback()
            self.connection.close()
        else:
            self.connection.commit()
            self.connection.close()

    # TODO add functions for insert, retrieve, insert_and_retrieve and compare
