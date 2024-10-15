import logging
from abc import ABC, abstractmethod
from copy import copy
from typing import Any, Protocol, Self

from etl_components.dataframe import DataFrame, get_dataframe
from etl_components.schema import ReferenceDict, Schema, TableDict


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

        Returns:
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

        Returns:
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
        data,  # noqa: ANN001
        *,
        table: str,
        columns: dict[str, str] | None = None,
    ) -> None:
        """Insert data into database.

        Args:
        ----
            data: DataFrame containing the data that needs to be inserted.
            table: name of the table to insert into
            columns: (Optional) dictionary linking column names in data with column names in dataframe
                     Example {data_name: db_name, ...}
                     If left empty, will assume that column names to insert
                     from data match column names in the database

        """
        dataframe = get_dataframe(data)
        if columns is not None:
            dataframe.rename(columns)

        common_columns = self.schema.parse_insert(table, dataframe.columns)
        query = self.get_insert_query(table, common_columns)

        logging.debug(
            "Inserting %s into %s using query:\n%s",
            common_columns,
            table,
            query,
        )

        # Executing query
        self.cursor.executemany(query, dataframe.rows(common_columns))

        if columns is not None:
            dataframe.undo_rename(columns)

    def retrieve_ids(
        self,
        data,  # noqa: ANN001
        *,
        table: str,
        alias: str,
        columns: dict[str, str] | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
    ) -> Any:  # noqa: ANN401
        """Retrieve ids from the database and join them to data.

        Args:
        ----
            data: DataFrame containing the data for which ids need to be retrieved and joined
            table: table to retrieve ids from
            alias: of the primary key of table
            columns: (Optional) dictionary linking column names in data with column names in dataframe
                     Example {data_name: db_name, ...}
                     If left empty, will assume that column names to retrieve ids on
                     from data match column names in the database
            replace: whether non-id columns from provided list are to be dropped after joining
            allow_duplication: if rows are allowed to be duplicated when merging ids

        Returns:
        -------
            data with ids from database added, or replacing original columns

        """
        dataframe = get_dataframe(data)
        if columns is not None:
            dataframe.rename(columns)

        key, common_columns = self.schema.parse_retrieve(
            table, alias, dataframe.columns
        )

        query = self.get_retrieve_query(table, key, alias, common_columns)
        logging.debug(
            "Retrieving %s from %s using query:\n%s",
            common_columns,
            table,
            query,
        )
        # Executing query
        self.cursor.execute(query)
        db_fetch = self.cursor.fetchall()

        dataframe.merge_ids(db_fetch, allow_duplication=allow_duplication)

        if replace:
            # Use table schema to determine which non_id columns can be dropped.
            dataframe.drop(self.schema.get_columns(table))
        elif not replace and columns is not None:
            # making sure to reverse the naming of columns if they are not replaced
            dataframe.undo_rename(columns)

        if isinstance(data, DataFrame):
            return dataframe

        return dataframe.data

    def insert_and_retrieve_ids(
        self,
        data,  # noqa: ANN001
        *,
        table: str,
        alias: str,
        columns: dict[str, str] | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
    ) -> Any:  # noqa: ANN401
        """Insert data into database and retrieve ids to join them to data.

            data: DataFrame containing the data for which ids need to be retrieved and joined
            table: table to retrieve ids from
            alias: of the primary key of table
            columns: (Optional) dictionary linking column names in data with column names in dataframe
                     Example {data_name: db_name, ...}
                     If left empty, will assume that column names to retrieve ids on
                     from data match column names in the database
            replace: whether non-id columns from provided list are to be dropped after joining
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
        data,  # noqa: ANN001
        *,
        query: str | None = None,
        columns: dict[str, str] | None = None,
        where: str | None = None,
        exact: bool = True,
    ) -> None:
        """Compare data in the database against data in a dataframe.

        Args:
        ----
            data: DataFrame containing data to be compared to
            query: valid SQL query to retrieve data to compare to.
            columns: (Optional) dictionary linking column names in data with column names in dataframe
                     Example {data_name: db_name, ...}
            where: (Optional) SQL WHERE clause to filter comparison data with.
                   NOTE: please always prefix the tables for columns you are conditioning on.
            exact: (Optional) whether all the rows in data must match all
                   the rows retrieved from the database. If False, only checks
                   if rows from data appear in rows from query.

        """
        dataframe = get_dataframe(data)
        if columns is not None:
            dataframe.rename(columns)

        if query is None:
            query = self.schema.get_compare_query(
                dataframe.columns, where=where
            )

        logging.debug("Comparing using query:\n%s", query)

        self.cursor.execute(query)
        db_rows = self.cursor.fetchall()

        assert len(db_rows) > 0, "Compare query yielded no results."
        assert len(db_rows) >= len(
            dataframe
        ), "Compare query yielded fewer rows than data."

        dataframe.compare(db_rows, exact=exact)

    def load(
        self,
        data,  # noqa: ANN001
        *,
        columns: dict[str, str] | None = None,
        compare: bool = True,
        compare_query: str | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
        where: str | None = None,
        exact: bool = True,
    ) -> Any:  # noqa: ANN401
        """Automatically load data into the database.

        Args:
        ----
            data: DataFrame containing data to be inserted into the database.
            columns: (Optional) translation of columns in data to column names in database.
                     Dictionary of format {data_name: db_name}.
                     If the same column name appears multiple times in the database,
                     prefix the column name with the desired table, eg. <table>.<column_name>
            compare: Whether a comparison needs to be preformed after loading has completed.
                     This tends to be flaky, so this allows you to turn it off.
            compare_query: (Optional) valid SQL query to retrieve data from the database to compare against.
                            Ignored if compare == False, automatically generated when compare == True and none is provided.
                            This might break if you have a complicated database model.
            replace: (Optional) whether columns can be replaced when retrieving ids.
                     If False, id columns are concatenated.
            allow_duplication: (Optional) whether joining on ids from the database can result in
                                rows being duplicated.
            where: (Optional) SQL WHERE clause to filter comparison data with.
                   NOTE: please always prefix the tables for columns you are conditioning on.
            exact: (Optional) whether all the rows in data must match all
                   the rows retrieved from the database in comparison. If False, only checks
                   if rows from data appear in rows from query.

        Returns:
        -------
            dataframe in original format (pandas or polars) with id columns

        """
        dataframe = get_dataframe(data)
        if columns is not None:
            dataframe.rename(columns)

        orig_dataframe = copy(dataframe)

        logging.debug("Loading data using columns %s", dataframe.columns)
        load_instructions = self.schema.get_load_instructions(dataframe.columns)

        logging.debug(
            "Tables to insert and retrieve: %s",
            load_instructions.insert_and_retrieve_tables,
        )
        logging.debug("Tables to insert: %s", load_instructions.insert_tables)

        logging.debug("Inserting and retrieving tables...")
        for params in load_instructions.insert_and_retrieve:
            dataframe = self.insert_and_retrieve_ids(
                dataframe,
                **params,
                replace=replace,
                allow_duplication=allow_duplication,
            )

        logging.debug("Inserting tables...")
        for params in load_instructions.insert:
            self.insert(dataframe, **params)

        if compare:
            logging.debug("Comparing...")
            self.compare(
                orig_dataframe, query=compare_query, where=where, exact=exact
            )

        return dataframe.data
