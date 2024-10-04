from functools import cached_property
from typing import Annotated, Callable, Self, TypedDict

import networkx as nx
from more_itertools import unique
from pydantic import (
    BaseModel,
    StringConstraints,
    model_validator,
)

from etl_components.exceptions import (
    AliasDoesNotExistError,
    ColumnsDoNotExistError,
    EmptyColumnListError,
    InvalidReferenceError,
    InvalidTableError,
    NoPrimaryKeyError,
    TableDoesNotExistError,
)


class Table(BaseModel):
    """Describes a table within a Schema."""

    name: str
    columns: list[str]
    primary_key: str = ""
    foreign_keys: list[str] = []

    @model_validator(mode="after")
    def verify_not_empty(self) -> Self:
        """Validate that the Table is not empty (no columns, primary or foreign keys)."""
        if (
            len(self.columns) == 0
            and not self.primary_key
            and not self.foreign_keys
        ):
            message = f"{self!r} seems to be empty, what is it for?"
            raise InvalidTableError(message)
        return self

    @property
    def columns_and_foreign_keys(self) -> list[str]:
        """Return both the columns and the foreign keys for this table."""
        return self.columns + self.foreign_keys

    @property
    def has_primary_key(self) -> bool:
        """Return whether the table has a primary key."""
        return bool(self.primary_key)

    def __str__(self) -> str:
        """Return human readable representation of the table."""
        pk = [] if not self.has_primary_key else [self.primary_key]
        cols = "\n\t".join([*pk, *self.foreign_keys, *self.columns])
        return f"Table {self.name} (\n\t{cols}\n)"


# Pydantic constraint for non empty string and more readable code
non_empty_string = StringConstraints(min_length=1, strip_whitespace=True)


class Reference(BaseModel):
    """Describes a reference between two tables."""

    # Making sure the strings are not empty
    from_table: Annotated[str, non_empty_string]
    from_key: Annotated[str, non_empty_string]
    to_table: Annotated[str, non_empty_string]
    to_key: Annotated[str, non_empty_string]

    @model_validator(mode="after")
    def verify_no_self_reference(self) -> Self:
        """Verify that the reference is not to itself."""
        if self.from_table == self.to_table:
            message = f"{self!r} is a reference to self."
            raise InvalidReferenceError(message)
        return self

    def __str__(self) -> str:
        """Return partial SQL string to join the two tables together."""
        return f"ON {self.from_table}.{self.from_key} = {self.to_table}.{self.to_key}"


class TableDict(TypedDict):
    """Type indicator for GetTablesFunction output."""

    name: str
    columns: list[str]
    primary_key: str
    foreign_keys: list[str]


class ReferenceDict(TypedDict):
    """Type indicator for GetReferencesFunction output."""

    from_table: str
    from_key: str
    to_table: str
    to_key: str


GetSchemaFunction = Callable[[], tuple[list[TableDict], list[ReferenceDict]]]


class Schema:
    """Describes the database schema and contains functions to interact with it."""

    graph: nx.DiGraph

    def __init__(self, get_schema: GetSchemaFunction) -> None:
        """Initialize schema."""
        # creating the DAG representing the database schema
        self.graph = nx.DiGraph()

        table_dicts, reference_dicts = get_schema()

        for table_dict in table_dicts:
            table = Table(**table_dict)
            self.graph.add_node(table.name, table=table)
        for reference_dict in reference_dicts:
            reference = Reference(**reference_dict)
            self.graph.add_edge(
                reference.from_table, reference.to_table, reference=reference
            )

    # Private methods

    def _get_table(self, table_name: str) -> Table:
        """Retrieve Table with this name.

        Args:
        ----
            table_name: of the desired table

        Returns:
        -------
           corresponding Table object

        Raises:
        ------
            TableDoesNotExistError: if table does not exist in schema.

        """
        if not table_name in self.graph.nodes:
            message = f"table '{table_name}' does not appear in schema."
            raise TableDoesNotExistError(message)
        return self.graph.nodes[table_name]["table"]

    # ---- Public methods

    def get_columns(self, table_name: str) -> list[str]:
        """Get a list of columns that are not primary or foreign keys for this table.

        Args:
        ----
            table_name: name of the desired table

        Returns:
        -------
           list of columns that are not primary or foreign keys

        """
        if not table_name in self.graph.nodes:
            message = f"table {table_name} does not appear in the schema."
            raise SchemaError(message)
        return self.graph.nodes[table_name]["table"].columns

    def get_compare_query(
        self,
        columns: list[str],
        *,
        where: str | None = None,
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

    def get_insert_and_retrieve_tables(
        self, columns: list[str]
    ) -> tuple[list[str], list[str]]:
        """Get lists of tables that need to be inserted and retrieved, or only inserted, based on columns.

        Args:
        ----
            columns: that are to be inserted

        Returns:
        -------
            insert_and_retrieve, insert
            with:
                insert_and_retrieve: tables that need to be inserted and retrieved
                insert: tables that only need to be inserted

        """

    def _parse_columns(self, table: Table, columns: list[str]) -> None:
        """Check if columns list is not empty and that columns exist in table.

        Args:
        ----
            table: under consideration
            columns: list of columns that are to be inserted or retrieved with

        Raises:
        ------
            EmptyColumnListError: when list is empty
            ColumnsDoNotExistError: when none of the columns exist in the table.

        """
        if len(columns) == 0:
            message = "Provided list of columns cannot be empty"
            raise EmptyColumnListError(message)

        if not any(col in table.columns_and_foreign_keys for col in columns):
            message = f"None of {columns} exist in {table.name}. Table schema is:\n{table}"
            raise ColumnsDoNotExistError(message)

    def parse_insert(self, table_name: str, columns: list[str]) -> list[str]:
        """Parse input values for insert or retrieve query, and return columns that table and data have in common.

        Checks whether table exists in the database,
        and whether any of columns exist for that table.

        Args:
        ----
            table_name: name of table to be inserted into
            columns: list of columns in dataframe

        Returns:
        -------
            list of columns that table and data have in common.

        """
        table = self._get_table(table_name)

        self._parse_columns(table, columns)

        common = set(columns) & set(table.columns_and_foreign_keys)
        return list(common)

    def parse_retrieve(
        self, table_name: str, alias: str, columns: list[str]
    ) -> tuple[str, list[str]]:
        """Parse input values for insert or retrieve query, and return columns that table and data have in common.

        Checks whether table exists in the database,
        and whether any of columns exist for that table.

        Args:
        ----
            table_name: name of table to be inserted into
            alias: of the primary key of the table
            columns: list of columns in dataframe

        Raises:
        ------
            NoPrimaryKeyError: when the table does not have a primary key
            AliasDoesNotExistError: when the alias does not appear in the schema

        Returns:
        -------
            list of columns that table and data have in common.

        """
        table = self._get_table(table_name)

        if not table.has_primary_key:
            message = f"Table {table_name} does not have a primary key. It does not make sense to retrieve ids from it."
            raise NoPrimaryKeyError(message)

        # checking if alias appears in the schema
        edges = self.graph.edges(table_name)
        references = [
            self.graph.get_edge_data(*edge)["reference"] for edge in edges
        ]
        if not alias in unique(
            [reference.from_key for reference in references]
        ):
            message = f"Alias '{alias}' does not appear anywhere in the schema for table {table_name}."
            raise AliasDoesNotExistError(message)

        self._parse_columns(table, columns)

        common = set(columns) & set(table.columns_and_foreign_keys)
        return table.primary_key, list(common)

    # Properties and dunder methods

    @cached_property
    def _column_table_mapping(self) -> dict[str, list[str]]:
        """Get reverse mapping from columns to tables."""
        mapping = {}
        for table_name in self.graph.nodes:
            table = self._get_table(table_name)
            for col in table.columns:
                if col not in mapping:
                    mapping[col] = [table_name]
                else:
                    mapping[col] += [table_name]
        return mapping

    def __str__(self) -> str:
        """Return schema as a string."""
        return "\n".join(
            str(self.graph.nodes[table]["table"]) for table in self.graph.nodes
        )
