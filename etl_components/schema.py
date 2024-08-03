from dataclasses import dataclass
from typing import Callable


class SchemaError(Exception):
    """Raised when something goes wrong when using the schema."""


GetTablesFunction = Callable[[], list[str]]
GetTableSchema = Callable[[str], str]
GetColumnsFunction = Callable[[str], list[str]]
GetReferencesFunction = Callable[[str], list[dict[str, str]]]


@dataclass
class Reference:
    """Describes a reference to another table."""

    column: str
    table: str
    to: str


@dataclass(frozen=False)
class Table:
    """Describes a table with a name and a list of columns."""

    name: str
    sql: str
    columns: list[str]
    references: list[Reference]
    refers_to: list[str]
    referred_by: list[str]

    def get_reference(self, table: str) -> str:
        """Get the reference of self to table."""
        if not table in self.refers_to:
            message = f"{self.name} does not refer to {table}"
            raise SchemaError(message)
        ref = self.references[self.refers_to.index(table)]
        # TODO maybe this is too strict and I might want more leeway
        return (
            f"JOIN {table} ON {self.name}.{ref.column} = {ref.table}.{ref.to}"
        )

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
            table.refers_to = [ref.table for ref in table.references]
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

    @property
    def table_names(self) -> list[str]:
        """Get a list of names of tables in the schema.

        Returns
        -------
            list of table names


        """
        return [table.name for table in self.tables]

    def get_table_by_column(self, column_name: str) -> Table:
        """Retrieve the Table that contains this column name.

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
            Appropriate table


        """
        # Catch case where the table is prefixed by the user
        if "." in column_name:
            table, column = column_name.split(".")
            if not column in self(table).columns:
                message = f"On {column_name}: {column} does not appear in {table} schema:\n{self(table)}"
                raise SchemaError(message)
            return self(table)

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
        return self(tables[0])

    def __call__(self, table_name: str) -> Table:
        """Retrieve the Table that belongs to provided table_name.

        Args:
        ----
            table_name: of the desired table

        Raises:
        ------
            SchemaError: if table_name does not appear in schema.

        Returns:
        -------
            Table with the provided table_name


        """
        if table_name not in self.table_names:
            message = f"'{table_name}' does not appear in schema."
            raise SchemaError(message)
        return self.tables[self.table_names.index(table_name)]

    def __str__(self) -> str:
        """Human readable representation of schema."""
        return "\n\n".join(list(map(str, self.tables)))
