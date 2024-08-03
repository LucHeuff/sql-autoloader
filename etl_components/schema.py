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

    def get_reference(self, table: str) -> Reference:
        """Get the reference of self to table."""
        if not table in self.refers_to:
            message = f"{self.name} does not refer to {table}"
            raise SchemaError(message)
        return self.references[self.refers_to.index(table)]

    def __str__(self) -> str:
        """Human readable representation of the table."""
        return self.sql


@dataclass
class Schema:
    """Describes a database Schema consisting of a list of tables."""

    tables: list[Table]

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

    @property
    def table_names(self) -> list[str]:
        """Get a list of names of tables in the schema.

        Returns
        -------
            list of table names


        """
        return [table.name for table in self.tables]

    def __call__(self, table_name: str) -> Table:
        """Retrieve the Table that belongs to provided table_name.

        Args:
        ----
            table_name: of the desired table

        Returns:
        -------
            Table with the provided table_name

        Raises:
        ------
            SchemaError: if table_name does not appear in schema.

        """
        if table_name not in self.table_names:
            message = f"'{table_name}' does not appear in schema."
            raise SchemaError(message)
        return self.tables[self.table_names.index(table_name)]

    def __str__(self) -> str:
        """Human readable representation of schema."""
        return "\n\n".join(list(map(str, self.tables)))
