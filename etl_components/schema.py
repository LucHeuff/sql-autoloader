from dataclasses import dataclass
from typing import Callable


class SchemaError(Exception):
    """Raised when something goes wrong when using Schema."""


class ColumnError(Exception):
    """Raised when something goes wrong when using a Column."""


GetTablesFunction = Callable[[], list[tuple[str, list[str]]]]
GetColumnsFunction = Callable[[str], list[dict]]


@dataclass(frozen=True)
class Column:
    """Describes a column in a table."""

    table: str
    name: str
    dtype: str
    primary_key: bool
    nullable: bool
    unique: bool
    default_value: str | None
    foreign_key: bool | None = None
    to_table: str | None = None
    to_column: str | None = None
    on_delete: str | None = None

    def __post_init__(self) -> None:
        """Perform field validation."""
        if self.primary_key and self.foreign_key:
            message = f"On column {self.name}: column cannot be both a primary and a foreign key."
            raise ColumnError(message)

        if self.foreign_key and (
            self.to_table is None or self.to_column is None
        ):
            message = f"On column {self.name}: column says it is a foreign key, but does not refer to another table and column."
            raise ColumnError(message)

    def get_reference(self) -> str:
        """Get a JOIN string for this reference."""
        if not self.foreign_key:
            message = f"{self.name} is not a foreign key."
            raise ColumnError(message)
        return f"JOIN {self.to_table} ON {self.table}.{self.name} = {self.to_table}.{self.to_column}"

    def __str__(self) -> str:
        """Get SQL schema representation of this column."""
        base = f"{self.name} {self.dtype} "
        if self.primary_key:
            return base + "PRIMARY KEY"
        if self.foreign_key:
            on_delete = f" ON DELETE {self.on_delete}" if self.on_delete else ""
            return (
                base
                + f"REFERENCES {self.to_table} ({self.to_column})"
                + on_delete
            )
        if self.default_value is not None:
            return base + f"DEFAULT {self.default_value}"
        return (
            base + "UNIQUE " * self.unique + "NOT NULL" * (not self.nullable)
        ).strip()


@dataclass(frozen=True)
class Table:
    """Describes a table with a name and a list of columns."""

    name: str
    columns: list[Column]
    constraint: list[str]

    @property
    def column_names(self) -> list[str]:
        """Get a list of column names for this table."""
        return [col.name for col in self.columns]

    @property
    def has_primary_key(self) -> bool:
        """Return whether this table contains a primary key."""
        return any(col.primary_key for col in self.columns)

    @property
    def has_foreign_key(self) -> bool:
        """Return whether this table contains a foreign key."""
        return any(col.foreign_key for col in self.columns)

    def get_non_id_column_names(self) -> list[str]:
        """Get a list of columns that are not primary or foreign keys."""
        return [
            col.name
            for col in self.columns
            if not (col.primary_key or col.foreign_key)
        ]

    def __repr__(self) -> str:
        """Programmer readable representation of the table."""
        cols = [col.name for col in self.columns]
        return f"Table(name={self.name}, columns={cols}, constraint={self.constraint})"

    def __str__(self) -> str:
        """Human readable representation of the table."""
        column_section = ",\n\t".join(str(col) for col in self.columns)
        if self.constraint:
            unique = f",\n\tUNIQUE ({', '.join(self.constraint)})"
            column_section += unique
        return f"CREATE TABLE {self.name} (\n\t{column_section}\n)"


@dataclass
class Schema:
    """Describes a database Schema consisting of a list of tables."""

    tables: list[Table]
    table_rank: dict[str, int]
    column_name_to_table_mapping: dict[str, list[Table]]
    refers_to: dict[str, list[Table]]
    referred_by: dict[str, list[Table]]

    def __init__(
        self,
        get_tables: GetTablesFunction,
        get_columns: GetColumnsFunction,
    ) -> None:
        """Initialize schema."""

        def _columns(table: str) -> list[Column]:
            return [Column(table=table, **col) for col in get_columns(table)]

        self.tables = [
            Table(table, _columns(table), constraint)
            for (table, constraint) in get_tables()
        ]
        # Setting refers_to

        self.refers_to = {
            table.name: self._get_unique_tables(
                [
                    self(col.to_table)
                    for col in table.columns
                    if col.to_table is not None
                ]
            )
            for table in self.tables
        }
        self.referred_by = {
            table.name: self._get_unique_tables(
                [
                    other_table
                    for other_table in self.tables
                    if table in self.refers_to[other_table.name]
                ]
            )
            for table in self.tables
        }
        self.table_rank = {
            table.name: self._get_table_rank(table) for table in self.tables
        }

        # setting column_table_mapping
        # first getting unique column names in the schema
        columns = {
            column.name for table in self.tables for column in table.columns
        }
        self.column_name_to_table_mapping = {
            column: [
                table for table in self.tables if column in table.column_names
            ]
            for column in columns
        }

    # ---- Private methods

    @property
    def table_names(self) -> list[str]:
        """Get a list of names of tables in the schema."""
        return [table.name for table in self.tables]

    def __call__(self, table_name: str) -> Table:
        """Retrieve the Table that belongs to provided table_name. Raises an error of schema does not exist."""
        if table_name not in self.table_names:
            message = f"'{table_name}' does not appear in schema."
            raise SchemaError(message)
        return self.tables[self.table_names.index(table_name)]

    def _get_table_rank(self, table: Table, rank: int = 0) -> int:
        """Get table rank by recursively counting how many tables are referred to."""
        if not self.refers_to[table.name]:
            # if not table.refers_to:
            return rank
        return max(
            self._get_table_rank(ref_table, rank + 1)
            for ref_table in self.refers_to[table.name]
        )

    def _get_table_by_column(self, column_name: str) -> Table:
        """Retrieve the table that contains this column name.

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
            table_name, column = column_name.split(".")
            table = self(table_name)
            if not column in table.column_names:
                message = f"On {column_name}: {column} does not appear in {table} schema:\n{table}"
                raise SchemaError(message)
            return table

        if not column_name in self.column_name_to_table_mapping:
            message = f"No column by name of '{column_name}' appears anywhere in the schema."
            raise SchemaError(message)

        tables = self.column_name_to_table_mapping[column_name]
        if len(tables) > 1:
            options = " or ".join(
                [f"'{table.name}.{column_name}'" for table in tables]
            )
            message = f"{column_name} is ambiguous, appears in tables {tables}.\nPlease prefix the correct table, e.g. {options}"
            raise SchemaError(message)
        return tables[0]

    def _get_unique_tables(self, tables: list[Table]) -> list[Table]:
        """Get a list of unique tables.

        Tables are not hashable, so can't just do a set.
        """
        unique_tables = []
        for table in tables:
            if not table in unique_tables:
                unique_tables.append(table)
        return unique_tables

    def _get_tables_from_columns(self, columns: list[str]) -> list[Table]:
        """Get a list of all tables that need to be inserted to based on a list of columns.

        This function proceeds in 4 steps:
            1. Get all tables that contain the name of one of [columns]
            2. Get all the tables that refer to the tables in 1.
            3. Sort the tables by 'rank': the number of tables they or their references refer to
            4. Downselect to tables for which all non-id columns appear in [columns] and for which
               all tables to which they refer appear in the downselected list of tables.
               This list should still be sorted by rank.
        """
        # recursive function to retrieve all the tables that refer to a specific table
        # This needs to be recursive in case one of the tables is referred to by a third table itself.

        def tail_references(
            table: Table, tail: list[Table] | None = None
        ) -> list[Table]:
            """Get a list of tables that refer to this table, recursively."""
            tail = [] if tail is None else tail
            referred_by = self.referred_by[table.name]
            if not referred_by:
                # end condition on a table that is not referred by another table
                return tail
            tail += referred_by
            return [
                ref_table
                for table in referred_by
                for ref_table in tail_references(table, tail)
            ]

        # 1. Identify which tables need to be inserted into directly from the columns
        root_tables = [self._get_table_by_column(col) for col in columns]

        # 2. find all tables that refer to these tables, recursively
        reference_tables = [
            ref for table in root_tables for ref in tail_references(table)
        ]

        # 3. order tables by 'rank', which indicates how many tables this table (indirectly) refers to.
        # reduce to unique since root_tables and reference_tables might hold some of the same tables.
        tables = self._get_unique_tables(
            sorted(
                root_tables + reference_tables,
                key=lambda t: self.table_rank[t.name],
            )
        )

        # 4. include tables for which all non-id columns are present, in [columns],
        # AND for which all tables they refer to are in insert_tables already.
        insert_tables = []
        for table in tables:
            # tables might have multiple copies of the same table, so
            # only add a table if it is not already present in insert_tables
            if all(
                # Note that this check ignores any id columns present in [columns]
                # checking against column, or column prefixed with table name
                col in columns or f"{table.name}.{col}" in columns
                for col in table.get_non_id_column_names()
            ) and all(
                ref_table in insert_tables
                for ref_table in self.refers_to[table.name]
            ):
                insert_tables.append(table)
        if not insert_tables:
            message = f"Combination of columns {columns} cannot be meaningfully inserted.\nThis usually means that table references cannot be inserted correctly from this combination of columns.\nEither insert these manually using insert() or insert_and_retrieve_ids(), or check if your database schema is correct."
            raise SchemaError(message)
        return insert_tables

    # ---- Public methods

    def get_non_id_columns(self, table_name: str) -> list[str]:
        """Get a list of columns that are not primary or foreign keys for this table.

        Args:
        ----
            table_name: name of the desired table

        Returns:
        -------
           list of columns that are not primary or foreign keys

        """
        return self(table_name).get_non_id_column_names()

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
        # NOTE: ordering of these tables matters, but _get_tables_from_columns already sorted them.
        insert_tables = self._get_tables_from_columns(columns)

        insert_and_retrieve = [
            table.name for table in insert_tables if table.has_primary_key
        ]
        insert = [
            table.name for table in insert_tables if not table.has_primary_key
        ]
        return insert_and_retrieve, insert

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
        # parsing WHERE section if it exists
        if where is None:
            where = ""
        else:
            # making sure not to duplicate WHERE
            where = f"\n{where}" if "WHERE" in where else f"\nWHERE {where}"

        tables = self._get_tables_from_columns(columns)
        references = [
            col for table in tables for col in table.columns if col.foreign_key
        ]
        # using the difference in the from_tables to the to_tables to figure out
        # which table I should use in the first FROM clause
        from_table = {col.table for col in references} - {
            col.to_table for col in references
        }
        # from_table should only contain one item
        assert (
            len(from_table) == 1
        ), "Too many from_tables that do not appear in to_tables"

        # storing a list of column objects that should be selected
        column_objects = [
            col
            for table in tables
            for col in table.columns
            if col.name in columns or f"{col.table}.{col.name}" in columns
        ]

        # prefixing table name to make sure there are no naming collisions
        columns_section = ", ".join(
            f"{col.table}.{col.name}" for col in column_objects
        )
        join_section = "\n".join([ref.get_reference() for ref in references])
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
        table = self(table_name)

        # start by getting rid of table prefix in columns, if they appear

        if not any(col in table.column_names for col in columns):
            message = f"None of {columns} exist in {table_name}. Table schema is:\n{table}"
            raise SchemaError(message)

        common = set(columns) & set(table.column_names)
        return list(common)

    def __str__(self) -> str:
        """Human readable representation of schema."""
        return "\n\n".join(list(map(str, self.tables)))
