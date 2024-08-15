import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from copy import copy
from typing import Any, Iterator, Protocol, Self

from etl_components.dataframe import DataFrame, get_dataframe
from etl_components.schema import Schema

logger = logging.getLogger(__name__)


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

    credentials: str
    schema: Schema

    # ---- function for connecting to the database

    @abstractmethod
    def connect(self) -> Connection:
        """Make a connection to the database."""
        ...

    # ---- Context managers

    def __enter__(self) -> Self:
        """Enter context manager by creating a connection with the database."""
        self.connection = self.connect()
        self.schema = self.get_schema()
        return self

    def __exit__(self, *exception: object) -> None:
        """Exit context manager by committing or rolling back on exception, and closing the connection.

        Args:
        ----
            exception: raised exception while inside the context manager

        """
        if exception:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()

    @contextmanager
    def cursor(self) -> Iterator[Cursor]:
        """Context manager for cursor."""
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    # ---- methods related to generating queries

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
    def get_retrieve_query(self, table: str, columns: list[str]) -> str:
        """Get a retrieve query for this Connector.

        Args:
        ----
            table: to retrieve from
            columns: to read values from

        Returns:
        -------
            Valid retrieve query for this Connector

        """
        ...

    # ---- methods related to the Schema
    @abstractmethod
    def get_tables(self) -> list[tuple[str, list[str]]]:
        """Retrieve list of table names from the database."""

    @abstractmethod
    def get_columns(self, table_name: str) -> list[dict]:
        """Retrieve a list of columns for this table from the database."""

    def get_schema(self) -> Schema:
        """Retrieve schema from the database."""
        return Schema(
            self.get_tables,
            self.get_columns,
        )

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

        common_columns = self.schema.parse_input(table, dataframe.columns)
        query = self.get_insert_query(table, common_columns)

        logger.debug(
            "Inserting %s into %s using query:\n\t%s",
            common_columns,
            table,
            query,
        )

        # Executing query
        with self.cursor() as cursor:
            cursor.executemany(
                query,
                dataframe.rows(common_columns),
            )

    def retrieve_ids(  # noqa ANN201
        self,
        data,  # noqa: ANN001
        *,
        table: str,
        columns: dict[str, str] | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
    ):
        """Retrieve ids from the database and join them to data.

        Args:
        ----
            data: DataFrame containing the data for which ids need to be retrieved and joined
            table: table to retrieve ids from
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

        common_columns = self.schema.parse_input(table, dataframe.columns)
        query = self.get_retrieve_query(table, common_columns)
        logger.debug(
            "Retrieving %s from %s using query:\n\t%s",
            common_columns,
            table,
            query,
        )
        # Executing query
        with self.cursor() as cursor:
            cursor.execute(query)
            db_fetch = cursor.fetchall()

        dataframe.merge_ids(db_fetch, allow_duplication=allow_duplication)

        if replace:
            # Use table schema to determine which non_id columns can be dropped.
            non_id_columns = self.schema.get_non_id_columns(table)
            dataframe.drop(non_id_columns)
        elif not replace and columns is not None:
            # making sure to reverse the naming of columns if they are not replaced
            reverse_columns = {v: k for (k, v) in columns.items()}
            dataframe.rename(reverse_columns)

        if isinstance(data, DataFrame):
            return dataframe

        return dataframe.data

    def insert_and_retrieve_ids(
        self,
        data,  # noqa: ANN001
        *,
        table: str,
        columns: dict[str, str] | None = None,
        replace: bool = True,
        allow_duplication: bool = False,
    ) -> Any:  # noqa: ANN401
        """Insert data into database and retrieve ids to join them to data.

            data: DataFrame containing the data for which ids need to be retrieved and joined
            table: table to retrieve ids from
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
            columns=columns,
            replace=replace,
            allow_duplication=allow_duplication,
        )

    def compare(
        self,
        data,  # noqa: ANN001
        *,
        columns: dict[str, str] | None = None,
        query: str | None = None,
        where: str | None = None,
        exact: bool = True,
    ) -> None:
        """Compare data in the database against data in a dataframe.

        Args:
        ----
            data: DataFrame containing data to be compared to
            columns: (Optional) dictionary linking column names in data with column names in dataframe
                     Example {data_name: db_name, ...}
            query: (Optional) valid SQL query to retrieve data to compare to.
                   If left empty, will try to automatically generate a compare query.
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

        logger.debug("Comparing using query:\n%s", query)

        with self.cursor() as cursor:
            cursor.execute(query)
            db_rows = cursor.fetchall()

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
        replace: bool = True,
        allow_duplication: bool = False,
        where: str | None = None,
        exact: bool = True,
    ) -> None:
        """Automatically load data into the database.

        Args:
        ----
            data: DataFrame containing data to be inserted into the database.
            columns: (Optional) translation of columns in data to column names in database.
                     Dictionary of format {data_name: db_name}.
                     If the same column name appears multiple times in the database,
                     prefix the column name with the desired table, eg. <table>.<column_name>
            replace: (Optional) whether columns can be replaced when retrieving ids.
                     If False, id columns are concatenated.
            allow_duplication: (Optional) whether joining on ids from the database can result in
                                rows being duplicated.
            where: (Optional) SQL WHERE clause to filter comparison data with.
                   NOTE: please always prefix the tables for columns you are conditioning on.
            exact: (Optional) whether all the rows in data must match all
                   the rows retrieved from the database in comparison. If False, only checks
                   if rows from data appear in rows from query.

        """
        dataframe = get_dataframe(data)
        if columns is not None:
            dataframe.rename(columns)

        orig_dataframe = copy(dataframe)

        logging.debug("Loading data using columns %s", dataframe.columns)

        (
            insert_and_retrieve,
            insert,
        ) = self.schema.get_insert_and_retrieve_tables(dataframe.columns)

        logging.debug("Tables to insert and retrieve: %s", insert_and_retrieve)
        logging.debug("Tables to insert: %s", insert)

        def get_column_map(table: str) -> dict[str, str] | None:
            """Translate columns prefixed with this table to their name in the schema."""
            if columns is None:
                return None
            prefix = f"{table}."
            return {
                col: col.replace(prefix, "") for col in columns if prefix in col
            }

        logger.debug("Inserting and retrieving tables...")
        for table in insert_and_retrieve:
            dataframe = self.insert_and_retrieve_ids(
                dataframe,
                columns=get_column_map(table),
                table=table,
                replace=replace,
                allow_duplication=allow_duplication,
            )

        logger.debug("Inserting tables...")
        for table in insert:
            self.insert(dataframe, table=table, columns=get_column_map(table))

        logger.debug("Comparing...")
        self.compare(orig_dataframe, where=where, exact=exact)

        return dataframe.data
