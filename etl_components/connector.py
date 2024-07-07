from abc import ABC, abstractmethod
from typing import Protocol

import polars as pl

from etl_components.dataframe_operations import merge_ids
from etl_components.parsers import parse_input
from etl_components.schema import Schema


class MissingIDsOnJoinError(Exception):
    """Used when joining upon retrieving from the database results in missing ids."""

    pass


class Cursor(Protocol):
    """A cursor to interact with the database."""

    def execute(self, query: str) -> None:
        """Execute a query."""
        ...

    def executemany(self, query: str, data: list[dict]) -> None:
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
    schema: Schema

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

    @abstractmethod
    def get_schema(self) -> Schema:
        """Retrieve schema (tables and their columns) from the database."""
        ...

    def update_schema(self) -> None:
        """Update schema from database manually.

        Allows you to tell the Connector to update the schema if that has
        changed after the Connector was created.
        """
        self.schema = self.get_schema()

    def print_schema(self) -> None:
        """Print the current database schema."""
        print(str(self.schema))  # noqa: T201

    @abstractmethod
    def create_insert_query(self, table: str, columns: dict[str, str]) -> str:
        """Create an insert query for this table and columns.

        Args:
        ----
            table: name of table to insert to
            columns: dictionary of {column: value, ...} pairs

        Returns:
        -------
            valid insert query for this connector

        """
        pass

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
        parse_input(table, columns, self.schema, list(data.columns))
        query = self.create_insert_query(table, columns)
        with self as cursor:
            cursor.executemany(query, data.to_dicts())

    @abstractmethod
    def create_retrieve_query(self, table: str, columns: dict[str, str]) -> str:
        """Create a retrieve query for this table and columns.

        Args:
        ----
            table: name of table to retrieve from
            columns: dictionary of {column: value, ...} pairs

        Returns:
        -------
            valid retrieve query for this connector

        """
        # TODO can this be a generic function outside of specific connector implementation?
        pass

    def retrieve_ids(
        self,
        table: str,
        columns: dict[str, str],
        data: pl.DataFrame,
        *,
        replace: bool = True,
        allow_duplication: bool = False,
    ) -> pl.DataFrame:
        """Retrieve ids from the database and join them to data.

        Args:
        ----
            table: table to retrieve ids from
            columns: dictionary linking column names in data with column names in dataframe
                     Example {column_db1: column_df2, ...}
            data: DataFrame containing the data for which ids need to be retrieved and joined
            replace: whether non-id columns from provided list are to be dropped after joining
            allow_duplication: if rows are allowed to be duplicated when merging ids

        Returns:
        -------
            data with ids from database added, or replacing original columns


        Raises:
        ------
            MissingIDsOnJoinError: if joining results in missing ids

        data = merge_ids(data, db_fetch, allow_duplication=allow_duplication)

        if replace:
            non_id_columns = [
                val for val in columns.values() if "_id" not in val
            ]
            data = data.drop(non_id_columns)

        return data

    def insert_and_retrieve_ids(
        self, table: str, columns: list[tuple], data: pl.DataFrame
    ) -> pl.DataFrame:
        pass

    def compare(
        self, columns: list[tuple], data: pl.DataFrame, *, exact: bool = True
    ):
        pass
