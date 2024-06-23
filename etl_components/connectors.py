from abc import ABC, abstractmethod
from typing import Protocol

import polars as pl


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
    schema: dict

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

    @abstractmethod
    def parameterize_value(self, value: str) -> str:
        """Convert a value to a parameter using the syntax for this connector.

        Args:
        ----
            value: the name of the columns from the DataFrame to parameterize

        Returns:
        -------
            paramterized value

        """
        pass

    # TODO think of convention in dictionary that is convenient
    @abstractmethod
    def get_schema(self) -> dict:
        """Retrieve schema (tables and their columns) from the database."""
        ...

    def update_schema(self) -> None:
        """Update schema from database manually.

        Allows you to tell the Connector to update the schema if that has
        changed after the Connector was created.
        """
        self.schema = self.get_schema()

    # TODO write function to convert database schema to nicely formatted string.
    def print_schema(self) -> None:
        """Print the current database schema."""
        pass

    # TODO add functions for insert, retrieve, insert_and_retrieve and compare
    def insert(
        self, table: str, columns: dict[str, str], data: pl.DataFrame
    ) -> None:
        """Insert data into database.

        Args:
        ----
            table: name of the table to insert into
            columns: dictionary linking column names in data with column names in dataframe
                     Example {column_db1: column_df2, ...}
            data: DataFrame containing the data that needs to be inserted.

        """
        query_parts = parse_insert(
            table, columns, self.schema, list(data.columns)
        )
        query = create_insert_query(query_parts, self.parameterize_value)
        with self as cursor:
            cursor.executemany(query, data.to_dicts())

    def retrieve_ids(
        self, table: str, columns: list[tuple], data: pl.DataFrame
    ) -> pl.DataFrame:
        pass

    def insert_and_retrieve_ids(
        self, table: str, columns: list[tuple], data: pl.DataFrame
    ) -> pl.DataFrame:
        pass

    def compare(
        self, columns: list[tuple], data: pl.DataFrame, *, exact: bool = True
    ):
        pass
