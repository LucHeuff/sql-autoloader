from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Iterator, Protocol, Self

from etl_components.dataframe import get_dataframe
from etl_components.parsers import parse_input
from etl_components.schema import Schema


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
    def get_tables(self) -> list[str]:
        """Retrieve list of table names from the database."""

    @abstractmethod
    def get_table_schema(self, table_name: str) -> str:
        """Retrieve SQL schema for this table from the database."""

    @abstractmethod
    def get_columns(self, table_name: str) -> list[str]:
        """Retrieve a list of columns for this table from the database."""

    @abstractmethod
    def get_references(self, table_name: str) -> list[dict[str, str]]:
        """Retrieve a list of references for this table from the database.

        Reference should be a dictionary of format
        {
            "column": <name of reference column in current table>,
            "table: <table that is referred to>,
            "to": <column that is referred to>
        }

        """

    def get_schema(self) -> Schema:
        """Retrieve schema from the database."""
        return Schema(
            self.get_tables,
            self.get_table_schema,
            self.get_columns,
            self.get_references,
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

        common_columns = parse_input(table, dataframe.columns, self.schema)
        query = self.get_insert_query(table, common_columns)
        # Executing query
        with self.cursor() as cursor:
            cursor.executemany(
                query,
                dataframe.rows(common_columns),
            )

    def retrieve_ids(  # noqa ANN201
        self,
        data,  # noqa: ANN001
        table: str,
        columns: dict[str, str] | None = None,
        *,
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

        common_columns = parse_input(table, dataframe.columns, self.schema)
        query = self.get_retrieve_query(table, common_columns)
        # Executing query
        with self.cursor() as cursor:
            cursor.execute(query)
            db_fetch = cursor.fetchall()

        dataframe.merge_ids(db_fetch, allow_duplication=allow_duplication)

        if replace:
            # Use table schema to determine which non_id columns can be dropped.
            schema_columns = self.schema(table).columns
            non_id_columns = [col for col in schema_columns if "_id" not in col]
            dataframe.drop(non_id_columns)
        elif not replace and columns is not None:
            # making sure to reverse the naming of columns if they are not replaced
            reverse_columns = {v: k for (k, v) in columns.items()}
            dataframe.rename(reverse_columns)

        return dataframe.data

    def insert_and_retrieve_ids(  # noqa ANN201
        self,
        data,  # noqa: ANN001
        table: str,
        columns: dict[str, str] | None = None,
        *,
        replace: bool = True,
        allow_duplication: bool = False,
    ):
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
        self.insert(data, table, columns)
        return self.retrieve_ids(
            data,
            table,
            columns,
            replace=replace,
            allow_duplication=allow_duplication,
        )

    def compare(
        self,
        data,  # noqa: ANN001
        query: str,
        columns: dict[str, str] | None = None,
        *,
        exact: bool = False,
    ) -> None:
        """Compare data in the database against data in a dataframe.

        Args:
        ----
            data: DataFrame containing data to be compared to
            query: valid SQL query to retrieve data to compare to
            columns: (Optional) dictionary linking column names in data with column names in dataframe
                     Example {data_name: db_name, ...}
            exact: (Optional) whether all the rows in data must match all
                   the rows retrieved from the database. If False, only checks
                   if rows from data appear in rows from query.

        """
        dataframe = get_dataframe(data)
        if columns is not None:
            dataframe.rename(columns)
        data_rows = dataframe.rows()

        with self.cursor() as cursor:
            cursor.execute(query)
            db_rows = cursor.fetchall()

        # TODO These assertions check if the compare query ran well enough,
        # this should not be necessary when automated.

        assert len(db_rows) > 0, "Compare query yielded no results."
        assert len(db_rows) >= len(
            data_rows
        ), "Compare query yielded fewer rows than data."

        # TODO data_rows and db_rows should both be lists of dicts.
        # comparing now checks whether each row in on of the lists appears in
        # another of the lists.
        # This means that I can't debug where things go missings though,
        # so I may want to write more complicated error messages here instead
        # of simple asserts.

        assert all(
            row in db_rows for row in data_rows
        ), "Some rows in data do not appear in database."
        if exact:
            assert all(
                row in data_rows for row in db_rows
            ), "Not all rows in data appear in database."
