from typing import Self

import polars as pl
import psycopg
from psycopg.rows import dict_row

from sql_autoloader.connector import DBConnector
from sql_autoloader.schema import ReferenceDict, TableDict

# ---- Functions for getting SQL queries from the psycopg connector


def _get_insert_query(table: str, columns: list[str]) -> str:
    """Get an insert query appropriate for a PostgreSQL database.

    Args:
    ----
        table: to insert into
        columns: to insert values into

    Returns:
    -------
       valid insert query

    """
    columns_section = ", ".join(columns)
    values_section = ", ".join([f"%({col})s" for col in columns])
    return f"INSERT INTO {table} ({columns_section}) VALUES ({values_section}) ON CONFLICT DO NOTHING"


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


def _fetch_schema(
    cursor: psycopg.Cursor,
) -> tuple[list[TableDict], list[ReferenceDict]]:
    """Get schema from PostgreSQL database."""
    tables_query = """
    SELECT
      c.table_name as table,
      c.column_name as column,
      tc.constraint_type as constraint
    FROM
      information_schema.columns as c
      LEFT JOIN information_schema.key_column_usage as kc ON kc.table_name = c.table_name
      AND kc.column_name = c.column_name
      LEFT JOIN information_schema.table_constraints as tc ON kc.table_name = tc.table_name
      AND kc.constraint_name = tc.constraint_name
    WHERE
      c.table_schema = 'public'
    ORDER BY
      c.table_name
    """
    cursor.execute(tables_query)
    tables_data = pl.DataFrame(cursor.fetchall())

    # --- Constructing TableDicts
    tables = tables_data["table"].unique().to_list()
    table_dicts = []

    for table in tables:
        primary_key = []
        foreign_keys = []
        columns = []

        # Postgres returns a separate row for each constraint, so a column can be both FOREIGN KEY and UNIQUE.
        # I only want to add a column once, so I need to keep track of columns that were already seen.
        seen = []
        for row in (
            tables_data.filter(pl.col("table") == table)
            .select(["column", "constraint"])
            .rows(named=True)
        ):
            column, constraint = row["column"], row["constraint"]
            # Skip this column if it was already seen.
            if column in seen:
                continue
            if constraint == "PRIMARY KEY":
                primary_key.append(column)
            elif constraint == "FOREIGN KEY":
                foreign_keys.append(column)
            else:
                columns.append(column)
            seen.append(column)

        assert len(primary_key) <= 1, "Cannot have more than 1 primary key."
        primary_key = primary_key[0] if len(primary_key) == 1 else ""
        assert (
            foreign_keys != columns
        ), "Foreign keys and columns cannot be the same."

        table_dict: TableDict = {
            "name": table,
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys,
        }

        table_dicts.append(table_dict)

    # --- Constructing ReferenceDicts
    # and what they are referring to
    reference_dicts = []

    for table in tables:
        # query taken from https://stackoverflow.com/questions/72679453/postgresql-sql-script-to-get-a-list-of-all-foreign-key-references-to-a-table
        # I have no idea what most of this does.
        reference_query = f"""
        SELECT (select  r.relname from pg_class r where r.oid = c.confrelid) as to_table,
               a.attname as to_key,
               (select r.relname from pg_class r where r.oid = c.conrelid) as from_table,
               UNNEST((select array_agg(attname) from pg_attribute where attrelid = c.conrelid and array[attnum] <@ c.conkey)) as from_key
        FROM pg_constraint c join pg_attribute a on c.confrelid=a.attrelid and a.attnum = ANY(confkey)
        WHERE c.confrelid = (select oid from pg_class where relname = '{table}')
        AND c.confrelid != c.conrelid;
        """
        cursor.execute(reference_query)  # type: ignore
        rows = cursor.fetchall()
        for row in rows:
            reference_dicts.append(row)  # noqa: PERF402

    return table_dicts, reference_dicts


class PostgresConnector(DBConnector):
    """Connector for PostgreSQL databases."""

    cursor: psycopg.Cursor

    def __init__(self, credentials: str) -> None:
        """Create a PostgresConnector that connects to the database with the given credentials.

        Args:
        ----
            credentials: valid connection string to the PostgreSQL database.
                         e.g. postgresql://<username>:<password>@<host>:<port>/<db_name>

        """
        self.credentials = credentials

    def __enter__(self) -> Self:
        """Enter context manager for PostgresConnector by opening a connection and creating a cursor.

        Returns
        -------
           instance of PostgresConnector

        """
        self.connection = psycopg.connect(
            self.credentials,
            row_factory=dict_row,  # type: ignore
        )
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

    # --- query generation methods

    def get_insert_query(self, table: str, columns: list[str]) -> str:
        """Get an insert query appropriate for a PostgreSQL database.

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
        """Get a retrieve query appropriate for a PostgreSQL database.

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
