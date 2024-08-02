from dataclasses import dataclass


class SchemaError(Exception):
    """Raised when something goes wrong when using the schema."""


@dataclass
class Reference:
    """Describes a reference to another table."""

    column: str
    to_table: str
    to_column: str


@dataclass
class Table:
    """Describes a table with a name and a list of columns."""

    name: str
    sql: str
    columns: list[str]
    references: list[Reference]
    referred_by: list[str] | None = None

    @property
    def refers_to(self) -> list[str]:
        """Get a list of tables that this table refers to."""
        return [reference.to_table for reference in self.references]

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
