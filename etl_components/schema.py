from dataclasses import dataclass
from typing import Callable


class SchemaError(Exception):
    """Raised when something goes wrong when using the schema."""


GetTablesFunction = Callable[[], list[str]]
GetTableSchema = Callable[[str], str]
GetColumnsFunction = Callable[[str], list[str]]
GetReferencesFunction = Callable[[str], list[dict[str, str]]]

# TODO check for all these classes, and methods in Schema, whether I need all of them.

# TODO add docstrings to public functions


@dataclass
class InsertTables:
    """Contains list of tables that should either be inserted and retrieved, or only inserted."""

    insert_and_retrieve: list[str]
    insert: list[str]


@dataclass
class Reference:
    """Describes a reference to another table."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    def __str__(self) -> str:
        """Get a JOIN string for this reference."""
        return f"\tJOIN {self.to_table} ON {self.from_table}.{self.from_column} = {self.to_table}.{self.to_column}"


@dataclass(frozen=False)
class Table:
    """Describes a table with a name and a list of columns."""

    name: str
    sql: str
    columns: list[str]
    references: list[Reference]
    refers_to: list[str]
    referred_by: list[str]

    def __str__(self) -> str:
        """Human readable representation of the table."""
        return self.sql


@dataclass
class Schema:
    """Describes a database Schema consisting of a list of tables."""

    tables: list[Table]
    column_table_mapping: dict[str, list[str]]

    def __init__(
        self,
        get_tables: GetTablesFunction,
        get_table_schema: GetTableSchema,
        get_columns: GetColumnsFunction,
        get_references: GetReferencesFunction,
    ) -> None:
        """Initialize schema."""
        table_names = get_tables()
        tables = [
            Table(
                table,
                get_table_schema(table),
                get_columns(table),
                [Reference(**ref) for ref in get_references(table)],
                [],
                [],
            )
            for table in table_names
        ]
        # Setting refers_to
        for table in tables:
            table.refers_to = [ref.to_table for ref in table.references]
        # Setting referred_by
        for table in tables:
            table.referred_by = [
                other_table.name
                for other_table in tables
                if table.name in other_table.refers_to
            ]
        self.tables = tables
        # setting column_table_mapping
        # first getting unique column names in the schema
        columns = {column for table in tables for column in table.columns}
        self.column_table_mapping = {
            column: [table.name for table in tables if column in table.columns]
            for column in columns
        }

    # ---- Private methods

    @property
    def _table_names(self) -> list[str]:
        """Get a list of names of tables in the schema."""
        return [table.name for table in self.tables]

    def _get_table(self, table_name: str) -> Table:
        """Retrieve the Table that belongs to provided table_name. Raises an error of schema does not exist."""
        if table_name not in self._table_names:
            message = f"'{table_name}' does not appear in schema."
            raise SchemaError(message)
        return self.tables[self._table_names.index(table_name)]

    def _get_table_by_column(self, column_name: str) -> str:
        """Retrieve the name of the table that contains this column name.

        Args:
        ----
            column_name: column for which the table is to be found.
                either just <column_name>, or <table>.<column_name>
                if the column name can appear in multiple tables.

        Raises:
        ------
            SchemaError if:
                - column doesn't exist in schema
                - column doesn't exist in table
                - column appears in multiple tables

        Returns:
        -------
            Name of the table

        """
        # Catch case where the table is prefixed by the user
        if "." in column_name:
            table, column = column_name.split(".")
            if not column in self._get_table(table).columns:
                message = f"On {column_name}: {column} does not appear in {table} schema:\n{self._get_table(table)}"
                raise SchemaError(message)
            return self._get_table(table).name

        if not column_name in self.column_table_mapping:
            message = f"No column by name of '{column_name}' appears anywhere in the schema."
            raise SchemaError(message)

        tables = self.column_table_mapping[column_name]
        if len(tables) > 1:
            options = " or ".join(
                [f"'{table}.{column_name}'" for table in tables]
            )
            message = f"{column_name} is ambiguous, appears in tables {tables}.\nPlease prefix the correct table, e.g. {options}"
            raise SchemaError(message)
        return self._get_table(tables[0]).name

    def _get_table_refers_to(self, table_name: str) -> list[str]:
        """Get a list of tables that this table refers to."""
        return self._get_table(table_name).refers_to

    def _get_table_referred_by(self, table_name: str) -> list[str]:
        """Get a list of tables that this table is referred by."""
        return self._get_table(table_name).referred_by

    def _get_tables_from_columns(self, columns: list[str]) -> list[str]:
        """Get a list of all columns that need to be inserted to based on a list of columns."""

        # recursive function to retrieve all the tables that refer to a specific table
        # This needs to be recursive in case one of the tables is referred to by a third table itself.
        def tail_references(
            table_name: str, tail: list[str] | None = None
        ) -> list[str]:
            """Get a list of tables that refer to this table, recursively."""
            tail = [] if tail is None else tail
            referred_by = self._get_table_referred_by(table_name)
            if not referred_by:  # end condition on a table that is not referred to by any other table
                return tail
            tail += referred_by
            return [
                ref
                for table in referred_by
                for ref in tail_references(table, tail)
            ]

        # 1. Identify which tables need to be inserted into directly from the columns
        tables = {self._get_table_by_column(col) for col in columns}
        # 2. find all tables that refer to these tables, recursively
        tables |= {ref for table in tables for ref in tail_references(table)}
        # 3. Recursively looking for references finds everything, but not all columns may be available.
        # So only include the tables for which all tables to which they themselves refer are in the list of tables
        insert_tables = {
            table
            for table in tables
            if all(
                refers_to in tables
                for refers_to in self._get_table_refers_to(table)
            )
        }
        # Catching case where the combination of columns that is requested cannot be meaningfully inserted or retrieved
        if len(insert_tables) == 0:
            message = f""" This combination of columns cannot be inserted or retrieved:\n\t {columns}
            This usually means that the columns lead to a set of tables that results in an incomplete set of references.
            """
            raise SchemaError(message)
        return list(insert_tables)

    # ---- Public methods

    def get_columns(self, table_name: str) -> list[str]:
        """Get a list of column names for this table."""
        return self._get_table(table_name).columns

    def get_insert_and_retrieve_tables(
        self, columns: list[str]
    ) -> InsertTables:
        """Get lists of tables that need to be inserted and retrieved, or only inserted, based on columns.

        Args:
        ----
            columns: that are to be inserted

        Returns:
        -------
            InsertTables with
                insert_and_retrieve: tables that need to be inserted and retrieved
                insert: tables that only need to be inserted

        """
        insert_tables = self._get_tables_from_columns(columns)
        # NOTE: ordering of these tables might matter,
        # will get issues when a table tries to insert that depends on another
        # table that has not been inserted yet.
        # Sorting by the number of tables a table refers to should solve that?
        insert_and_retrieve = sorted(
            [
                table
                for table in insert_tables
                if "id" in self.get_columns(table)
            ],
            key=lambda t: len(self._get_table(t).refers_to),
            reverse=False,
        )
        insert = [
            table
            for table in insert_tables
            if not "id" in self.get_columns(table)
        ]
        return InsertTables(insert_and_retrieve, insert)

    def get_compare_query(
        self, columns: list[str], where: str | None = None
    ) -> str:
        """Get compare query for the listed columns.

        Args:
        ----
            columns: list of columns to include in the compare query
            where: (Optional) WHERE clause to filter comparison results by

        Returns:
        -------
            valid compare query

        """
        # parsin WHERE section if it exists
        if where is None:
            where = ""
        else:
            # making sure not to duplicate WHERE
            where = f"\n{where}" if "WHERE" in where else f"\nWHERE {where}"

        tables = self._get_tables_from_columns(columns)
        references = [
            ref for table in tables for ref in self._get_table(table).references
        ]
        # using the difference in the from_tables to the to_tables to figure out
        # which table I should use in the first FROM clause
        from_table = {ref.from_table for ref in references} - {
            ref.to_table for ref in references
        }
        # this should only contain one items
        assert (
            len(from_table) == 1
        ), "Too many from_tables that do not appear in to_tables"
        columns_section = ", ".join(columns)
        join_section = "\n".join([str(ref) for ref in references])
        return f"SELECT {columns_section}\nFROM {from_table.pop()}\n{join_section}{where}"

    def parse_input(self, table_name: str, columns: list[str]) -> list[str]:
        """Parse input values for insert or retrieve query, and return columns that table and data have in common.

        Checks whether table exists in the database,
        and whether any of columns exist for that table.

        Args:
        ----
            table_name: name of table to be inserted into
            columns: list of columns in dataframe

        Raises:
        ------
            SchemaError: when no columns exist for that table

        Returns:
        -------
            list of columns that table and data have in common.

        """
        table = self._get_table(table_name)

        if not any(col in table.columns for col in columns):
            message = f"None of [{columns}] exist in {table_name}. Table scheme is:\n{table}"
            raise SchemaError(message)

        common = set(columns) & set(table.columns)
        return list(common)

    def __str__(self) -> str:
        """Human readable representation of schema."""
        return "\n\n".join(list(map(str, self.tables)))
