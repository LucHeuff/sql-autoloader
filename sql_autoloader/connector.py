import logging
from abc import ABC, abstractmethod
from typing import Protocol, Self

import polars as pl

from sql_autoloader.dataframe_operations import (
    compare,
    get_rows,
    has_nulls,
    merge_ids,
)
from sql_autoloader.schema import ReferenceDict, Schema, TableDict

logger = logging.getLogger(__name__)


# --- Utility functions
def invert(d: dict[str, str]) -> dict[str, str]:
    """Inverts the keys and values of a dictionary."""
    return {v: k for (k, v) in d.items()}


def preprocess(data: pl.DataFrame, columns: dict[str, str] | None) -> pl.DataFrame:
    """Check rename columns and remove duplicate rows.

    Args:
    ----
        data: pl.DataFrame
        columns: (Optional) dictionary of {old_name: new_name}

    Returns
    -------
        renamed pl.DataFrame

    """
    columns = {} if columns is None else columns
    return data.rename(columns).unique()


def postprocess(data: pl.DataFrame, columns: dict[str, str] | None) -> pl.DataFrame:
    """Undoes column renaming if required.

    Args:
    ----
        data: pl.DataFrame
        columns: (Optional) dictionary of {old_name: new_name}

    Returns
    -------
        original pl.DataFrame

    """
    if columns is not None:
        return data.rename(invert(columns))
    return data


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

    def close(self) -> None:
        """Close the cursor."""


class DBConnector(ABC):
    """Abstract base class for connector with a database."""

    credentials: str
    schema: Schema
    cursor: Cursor

    # ---- Context managers to connect conveniently to the database

    @abstractmethod
    def __enter__(self) -> Self:
        """Enter DBConnector context manager."""
        ...

    @abstractmethod
    def __exit__(self, *exception: object) -> None:
        """Exit DBConnector context manager."""

    # ---- Methods related to generating queries

    @abstractmethod
    def get_insert_query(self, table: str, columns: list[str]) -> str:
        """Get an insert query for this Connector.

        Args:
        ----
            table: to insert into
            columns: to insert values into

        Returns
        -------
            Valid insert query for this Connector

        """
        ...

    @abstractmethod
    def get_retrieve_query(
        self, table: str, key: str, alias: str, columns: list[str]
    ) -> str:
        """Get a retrieve query for this Connector.

        Args:
        ----
            table: to retrieve from
            key: name of the primary key
            alias: for the primary key
            columns: to read values from

        Returns
        -------
            Valid retrieve query for this Connector

        """
        ...

    # ---- Methods related to the Schema

    @abstractmethod
    def fetch_schema(self) -> tuple[list[TableDict], list[ReferenceDict]]:
        """Retrieve schema from the database."""

    def get_schema(self) -> Schema:
        """Retrieve schema from the database."""
        return Schema(self.fetch_schema)

    def schema_is_empty(self) -> bool:
        """Check whether database schema is empty."""
        return self.schema.is_empty

    def update_schema(self) -> None:
        """Update schema from database manually.

        Allows you to tell the Connector to update the schema if that has
        changed after the Connector was created.
        """
        self.schema = self.get_schema()

    def print_schema(self) -> None:
        """Print the current database schema."""
        print(str(self.schema))  # noqa: T201

    # ---- Database interaction methods

    def insert(
        self,
        data: pl.DataFrame,
        *,
        table: str,
        columns: dict[str, str] | None = None,
    ) -> None:
        """Insert data into database.

        Args:
        ----
            data: pl.DataFrame containing the data that needs to be inserted.
            table: name of the table to insert into
            columns: (Optional) dictionary linking column names in data
                     to column names in dataframe.
                     Example {data_name: db_name, ...}
                     If left empty, will assume that column names to insert
                     from data match column names in the database

        """
        self.schema.check_schema_not_empty()
        data = preprocess(data, columns)
        common_columns = self.schema.parse_insert(table, data.columns)
        assert len(common_columns) > 0, "No common columns were found."

        query = self.get_insert_query(table, common_columns)

        log_message = "Inserting %s into %s using query:\n%s"
        logger.debug(log_message, common_columns, table, query)

        # Executing query
        self.cursor.executemany(query, get_rows(data, common_columns))

        # postprocessing because processing happens in place.
        data = postprocess(data, columns)

    def retrieve_ids(
        self,
        data: pl.DataFrame,
        *,
        table: str,
        alias: str,
        columns: dict[str, str] | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
    ) -> pl.DataFrame:
        """Retrieve ids from the database and join them to data.

        Args:
        ----
            data: pl.DataFrame for which ids need to be retrieved and joined
            table: table to retrieve ids from
            alias: of the primary key of table
            columns: (Optional) dictionary linking column names in data
                     with column names in dataframe.
                     Example {data_name: db_name, ...}
                     If left empty, will assume that column names to retrieve ids on
                     from data match column names in the database
            replace: whether non-id columns from provided list are to be
                     dropped after joining.
            allow_duplication: if rows are allowed to be duplicated when merging ids

        Returns
        -------
            data with ids from database added, or replacing original columns

        """
        self.schema.check_schema_not_empty()
        data = preprocess(data, columns)

        primary_key, common_columns = self.schema.parse_retrieve(
            table, alias, data.columns
        )

        query = self.get_retrieve_query(table, primary_key, alias, common_columns)
        log_message = "Retrieving %s from %s using query:\n%s"
        logger.debug(log_message, common_columns, table, query)

        # Executing query
        self.cursor.execute(query)
        db_fetch = self.cursor.fetchall()

        data = merge_ids(data, db_fetch, alias, allow_duplication=allow_duplication)

        if replace:
            # Use table schema to determine which non_id columns can be dropped.
            data.drop(self.schema.get_columns(table))

        return postprocess(data, columns)

    def insert_and_retrieve_ids(
        self,
        data: pl.DataFrame,
        *,
        table: str,
        alias: str,
        columns: dict[str, str] | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
    ) -> pl.DataFrame:
        """Insert data into database and retrieve ids to join them to data.

            data: pl.DataFrame for which ids need to be retrieved and joined
            table: table to retrieve ids from
            alias: of the primary key of table
            columns: (Optional) dictionary linking column names in data
                     to column names in dataframe.
                     Example {data_name: db_name, ...}
                     If left empty, will assume that column names to retrieve ids on
                     from data match column names in the database
            replace: whether non-id columns from provided list are to be
                     dropped after joining.
            allow_duplication: if rows are allowed to be duplicated when merging ids

        Returns
        -------
            data with ids from database added, or replacing original columns


        """
        self.insert(data, table=table, columns=columns)
        return self.retrieve_ids(
            data,
            table=table,
            alias=alias,
            columns=columns,
            replace=replace,
            allow_duplication=allow_duplication,
        )

    def compare(
        self,
        data: pl.DataFrame,
        *,
        query: str | None = None,
        columns: dict[str, str] | None = None,
        where: str | None = None,
        exact: bool = True,
    ) -> None:
        """Compare data in the database against data in a dataframe.

        Args:
        ----
            data: pl.DataFrame containing data to be compared to
            query: valid SQL query to retrieve data to compare to.
            columns: (Optional) dictionary linking column names in data
                     to column names in dataframe.
                     Example {data_name: db_name, ...}
            where: (Optional) SQL WHERE clause to filter comparison data with.
            NOTE: Always prefix the tables for columns you are conditioning on.
            exact: (Optional) whether all the rows in data must match all
                   the rows retrieved from the database. If False, only checks
                   if rows from data appear in rows from query.
                   Setting exact to false will also remove rows with missings
                   from data.


        """
        self.schema.check_schema_not_empty()
        data = preprocess(data, columns)

        if query is None:
            query = self.schema.get_compare_query(data.columns, where=where)

        logger.debug("Comparing using query:\n%s", query)

        self.cursor.execute(query)
        db_rows = self.cursor.fetchall()

        assert len(db_rows) > 0, "Compare query yielded no results."
        # the following check is not always valid if the data contain missings
        if not has_nulls(data):
            assert len(db_rows) >= len(data), (
                f"Compare query yielded fewer rows ({len(db_rows)}) than data. ({len(data)})"
            )

        compare(data, db_rows, exact=exact)

        data = postprocess(data, columns)

    def load(
        self,
        data: pl.DataFrame,
        *,
        columns: dict[str, str] | None = None,
        compare: bool = True,
        compare_query: str | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
        where: str | None = None,
        exact: bool = True,
    ) -> pl.DataFrame:
        """Automatically load data into the database.

        Args:
        ----
            data: pl.DataFrame containing data to be inserted into the database.
            columns: (Optional) mapping of columns in data to columns in database.
                     Dictionary of format {data_name: db_name}.
                     If the same column name appears multiple times in the database,
                     prefix the column name with the desired table,
                     eg. <table>.<column_name>
            compare: Whether a comparison needs to be preformed after loading.
                     This can sometimes be flaky, so this allows you to turn it off.
            compare_query: (Optional) valid SQL query to retrieve data
                           from the database to compare against.
                   Ignored if compare == False,
                   automatically generated when compare == True and none is provided.
                   This might break if you have a complicated database model.
            replace: (Optional) whether columns can be replaced when retrieving ids.
                     If False, id columns are concatenated.
            allow_duplication: (Optional) whether to allow rows to be duplicated
                                when joining on ids from the database.
            where: (Optional) SQL WHERE clause to filter comparison data with.
               NOTE: always prefix the tables for columns you are conditioning on.
            exact: (Optional) whether all the rows in data must match all rows
                   retrieved from the database in comparison.
                   If False, only checks if rows from data appear in rows from query.
                   Setting exact=False will also remove rows with missings from data.

        Returns
        -------
            pl.DataFrame including foreign keys

        """
        self.schema.check_schema_not_empty()
        data = preprocess(data, columns)

        orig_data = data.clone()

        logger.debug("Loading data using columns %s", data.columns)
        load_instructions = self.schema.get_load_instructions(data.columns)

        logger.debug(
            "Tables to insert and retrieve: %s",
            load_instructions.insert_and_retrieve_tables,
        )
        logger.debug("Tables to insert: %s", load_instructions.insert_tables)

        logger.debug("Inserting and retrieving tables...")
        for params in load_instructions.insert_and_retrieve:
            data = self.insert_and_retrieve_ids(
                data,
                **params,
                replace=replace,
                allow_duplication=allow_duplication,
            )

        logger.debug("Inserting tables...")
        for params in load_instructions.insert:
            self.insert(data, **params)

        if compare:
            logger.debug("Comparing...")
            self.compare(orig_data, query=compare_query, where=where, exact=exact)

        return postprocess(data, columns)
