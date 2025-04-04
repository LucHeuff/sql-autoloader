# ruff: noqa: E501
from collections.abc import Callable
from functools import cached_property
from typing import Annotated, Self, TypedDict

import networkx as nx
from more_itertools import (
    collapse,
    unique,
    unique_everseen,
    unique_justseen,
    windowed,
)
from pydantic import (
    BaseModel,
    StringConstraints,
    model_validator,
)

from sql_autoloader.exceptions import (
    AliasDoesNotExistError,
    AmbiguousAliasesError,
    ColumnIsAmbiguousError,
    ColumnsDoNotExistOnTableError,
    EmptyColumnListError,
    EmptySchemaError,
    InvalidReferenceError,
    InvalidTableError,
    IsolatedSubgraphsError,
    IsolatedTablesError,
    NoPrimaryKeyError,
    NoSuchColumnForTableError,
    NoSuchColumnInSchemaError,
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
        """Validate that the Table is not empty.

        (no columns or foreign keys).
        """
        if len(self.columns) == 0 and len(self.foreign_keys) == 0:
            message = f"{self!r} seems to be empty, what is it for?"
            raise InvalidTableError(message)
        return self

    def get_common_columns(self, columns: list[str]) -> list[str]:
        """Return a set of columns common to this table and the list of columns.

        Args:
        ----
                    columns: list of columns of interest

        Returns
        -------
                    columns that the list and this table have in common.

        """
        return list(set(columns) & set(self.columns_and_foreign_keys))

    def get_prefixed_columns(self, columns: list[str]) -> list[tuple[str, str]]:
        """Get prefixed version of each column that apears in this table.

        Args:
        ----
            columns: list of columns that may appear in this table,
                     which are allowed to have a prefix.

        Returns
        -------
           list of tuples of prefixed columns in format (prefixed, original)

        """
        column_to_prefix_map = {v: k for (k, v) in self.prefix_column_map.items()}
        prefix_columns = []
        for col in columns:
            if col not in self:
                continue
            if col in self.prefix_column_map:
                prefix_columns.append((col, col))
            else:
                prefix_columns.append((column_to_prefix_map[col], col))
        return prefix_columns

    @cached_property
    def columns_and_foreign_keys(self) -> list[str]:
        """Return both the columns and the foreign keys for this table."""
        return self.foreign_keys + self.columns

    @property
    def has_primary_key(self) -> bool:
        """Return whether the table has a primary key."""
        return bool(self.primary_key)

    @property
    def is_linking(self) -> bool:
        """Return whether this table is a linking table.

        Linking tables are tables where all columns are primary or foreign keys
        """
        return len(self.columns) == 0

    @cached_property
    def prefix_column_map(self) -> dict[str, str]:
        """Return mapping from prefixes to columns for this table."""
        return {f"{self.name}.{col}": col for col in self.columns_and_foreign_keys}

    def __contains__(self, column: str) -> bool:
        """Return whether column exist for this table.

        Accepts the column name as well as prefixed column name (<table>.<column>).

        Args:
        ----
            column: name of column to check

        Returns
        -------
           boolean indicating if the column exists for this table.

        """
        return (
            column in self.columns_and_foreign_keys
            or column in self.prefix_column_map
        )

    def __str__(self) -> str:
        """Return human readable representation of the table."""
        pk = [] if not self.has_primary_key else [self.primary_key]
        cols = "\n\t".join([*pk, *self.columns_and_foreign_keys])
        return f"Table {self.name} (\n\t{cols}\n)"


# Pydantic constraint for non empty string, for more readable code
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
        return (
            f"ON {self.from_table}.{self.from_key} = {self.to_table}.{self.to_key}"
        )


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


class InsertAndRetrieveDict(TypedDict):
    """Type indicator for params used in connector.insert_and_retrieve_ids()."""

    table: str
    alias: str
    columns: dict[str, str] | None


class InsertDict(TypedDict):
    """Type indicator for params used in connector.insert()."""

    table: str
    columns: dict[str, str] | None


class LoadInstructions(BaseModel):
    """Model to neatly encapsulate instructions for connector.load()."""

    insert_and_retrieve: list[InsertAndRetrieveDict]
    insert: list[InsertDict]

    @property
    def insert_and_retrieve_tables(self) -> list[str]:
        """Get a list of tables being inserted and retrieved."""
        return [d["table"] for d in self.insert_and_retrieve]

    @property
    def insert_tables(self) -> list[str]:
        """Get a list of tables being inserted."""
        return [d["table"] for d in self.insert]

    def __repr__(self) -> str:
        """Return more readable repr for object."""
        insert_and_retrieve_repr = "\n".join(
            repr(d) for d in self.insert_and_retrieve
        )
        insert_repr = "\n".join(repr(d) for d in self.insert)
        return f"LoadInstructions(\ninsert_and_retrieve:\n{insert_and_retrieve_repr}\ninsert:\n{insert_repr})"


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
            # add_edge will also add tables that don't appear in table_dict.
            # This leads to an unclear error somewhere down the line,
            # which I'd like to avoid.
            if reference.to_table not in self.graph.nodes:
                reference_example = f"On table '{reference.from_table}':\n\t{reference.from_key} REFERENCES {reference.to_table} ({reference.to_key})"
                message = f"Table {reference.to_table} does not appear in list of tables. Is the following reference correct?\n{reference_example}"
                raise TableDoesNotExistError(message)

            self.graph.add_edge(
                reference.to_table, reference.from_table, reference=reference
            )

        # The resulting graph has to be a Directed Acyclic Graph
        # for my algorithms to work.
        # As far as I am aware, all valid SQL database structures should be DAGs
        assert nx.is_directed_acyclic_graph(self.graph), (
            "Provided schema is not a DAG."
        )

    # ---- Private methods

    def _get_table(self, table_name: str) -> Table:
        """Retrieve Table with this name.

        Args:
        ----
            table_name: of the desired table

        Returns
        -------
           corresponding Table object

        Raises
        ------
            TableDoesNotExistError: if table does not exist in schema.

        """
        if table_name not in self.graph.nodes:
            message = f"table '{table_name}' does not appear in schema."
            raise TableDoesNotExistError(message)
        return self.graph.nodes[table_name]["table"]

    def _get_table_name_by_column(self, column_name: str) -> str:
        """Retrieve the table name beloning to this column.

        Args:
        ----
            column_name: bare column name, or with table prefix if ambiguous

        Returns
        -------
           name of the table that belongs to this column.

        Raises
        ------
            NoSuchColumnForTableError: if prefixed table doesn't have this column.
            ColumnIsAmbiguousError: if the column name appears on multiple columns.
            NoSuchColumnInSchemaError: if column does not appear in the schema.

        """
        # Splitting off table prefix if it exists.
        if "." in column_name:
            table_name, _ = column_name.split(".")
            table = self._get_table(table_name)
            if column_name not in table:
                message = f"Columns '{column_name}' does not exist for {table_name}."
                raise NoSuchColumnForTableError(message)
            return table.name

        if column_name not in self._column_table_mapping:
            message = f"No column with name '{column_name}' appears anywhere in the schema.\nNote, sql-autoloader does not allow directly setting primary keys."
            raise NoSuchColumnInSchemaError(message)

        tables = self._column_table_mapping[column_name]
        if len(tables) > 1:
            message = f"Column name '{column_name}' is ambiguous, as it appears on tables '{tables}'.\nPlease prefix the column name with the correct table using the format <table>.<column>."
            raise ColumnIsAmbiguousError(message)

        return tables[0]

    def _get_table_prefix_map(
        self, table_name: str, columns: list[str]
    ) -> dict[str, str]:
        """Get a dictionary mapping prefixed columns to bare column names for this table.

        Args:
        ----
            table_name: name of table to get mapping for
            columns: columns that might need to be remapped

        Returns
        -------
            mapping of prefixed columns to bare column names.

        """
        table = self._get_table(table_name)
        return {
            prefix: column
            for (prefix, column) in table.prefix_column_map.items()
            if prefix in columns
        }

    def _get_relevant_tables(self, columns: list[str]) -> list[str]:
        """Get a list of tables that are relevant to this set of columns.

        Also Searches through the graph to find relevant linking tables.

        Args:
        ----
            columns: list of columns to be loaded.

        Returns
        -------
           list of tables to load.

        """
        # First getting a list of tables straight from the column names
        tables = list(unique(self._get_table_name_by_column(col) for col in columns))

        # Finding linking tables, which are assumed to be many-to-many tables
        # consisting of only primary or foreign keys, since any tables represented by
        # one of the columns is now already in our list.

        # iterating through topological sort makes sure that linking tables are
        # not discarded before all its predecessors are in tables
        for node in self._topological_sort:
            # if the node is already in tables, skip it
            if node in tables:
                continue
            if not self._get_table(node).is_linking:
                continue

            predecessors = list(self.graph.predecessors(node))
            if all(predecessor in tables for predecessor in predecessors):
                tables.append(node)

        # NOTE: I'm not entirely sure if this assumption is all that useful
        subgraph = self.graph.subgraph(tables)
        assert nx.isomorphism.DiGraphMatcher(
            self.graph, subgraph
        ).subgraph_is_isomorphic(), "Selected tables do not form a valid subgraph"

        return tables

    def _parse_columns(self, table: Table, columns: list[str]) -> list[str]:
        """Check if columns list is not empty and that columns exist in table, then return common columns.

        Args:
        ----
            table: under consideration
            columns: list of columns that are to be inserted or retrieved with

        Raises
        ------
            EmptyColumnListError: when list is empty
            ColumnsDoNotExistError: when none of the columns exist in the table.

        """
        if len(columns) == 0:
            message = "Provided list of columns cannot be empty"
            raise EmptyColumnListError(message)

        if not any(col in table for col in columns):
            message = f"None of '{columns}' exist in table '{table.name}'. Table schema is:\n{table}"
            raise ColumnsDoNotExistOnTableError(message)

        return table.get_common_columns(columns)

    # ---- Public methods

    def check_schema_not_empty(self) -> None:
        """Check if the schema is not empty."""
        if self.is_empty:
            message = "Database does not contain any tables."
            raise EmptySchemaError(message)

    def get_columns(self, table_name: str) -> list[str]:
        """Get a list of columns that are not primary or foreign keys for this table.

        Args:
        ----
            table_name: name of the desired table

        Returns
        -------
           list of columns that are not primary or foreign keys

        """
        return self._get_table(table_name).columns

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

        Returns
        -------
            valid compare query

        """
        # parsing where clause
        where_clause = f"\n{where}" if where is not None else ""

        # Find the tables belonging to these columns
        # retrieve the subgraph for this set of tables and the topological ordering
        tables = self._get_relevant_tables(columns)
        subgraph = nx.subgraph(self.graph, tables)

        # I cannot deal with comparing when isolated tables are involved, so throwing back to the user.
        # This is only an issue when I'm trying to load to multiple tables
        if len(tables) > 1 and nx.number_of_isolates(subgraph) > 0:
            isolated = [
                node for node in subgraph.nodes if nx.is_isolate(subgraph, node)
            ]
            message = f"Automatic compare query generation cannot handle any isolated tables, but '{isolated}' do not link to any other table when considering '{tables}'.\nEither provide a compare query yourself, make sure the data you are loading all relate to one another, or disable comparison if you do not care."
            raise IsolatedTablesError(message)

        # I also cannot deal with isolated subgraphs
        if (
            len(tables) > 1
            and len(iso_subgraps := list(nx.weakly_connected_components(subgraph)))
            > 1
        ):
            message = f"Automatic compare query generation cannot handle isolated subgraphs, but found weakly connected components: '{iso_subgraps}'"
            raise IsolatedSubgraphsError(message)

        # --- Building the SELECT clause
        # e.g <table>.<column> as <alias>
        select_columns = collapse(
            [
                pair
                for table in tables
                for pair in self._get_table(table).get_prefixed_columns(columns)
            ],
            base_type=tuple,
        )
        select_aliases = [
            f'{prefixed} as "{original}"' for (prefixed, original) in select_columns
        ]

        select_clause = f"SELECT\n{',\n'.join(select_aliases)}"

        # If only writing to a single table, no need to try and figure out joins
        if len(tables) == 1:
            # I do need to add from which table selection is happening
            join_clause = f"FROM {next(iter(tables))}"
        else:
            # --- Building the JOIN clause
            # I want to be able to ignore the edge direction, so I also need an undirected graph.
            undirected = subgraph.to_undirected()

            # ---- Replace below here?
            path = nx.dag_longest_path(subgraph)

            assert len(path) > 0, "only found empty base path."

            # There is no guarantee that all tables have been visited by the path.
            # The missing tables will be added to the path iteratively,
            # by splicing them in the path as a loop, which makes sure that the transistions
            # are still valid.
            for table in tables:
                # Checking if the table was not already added in a previous loop iteration
                if table not in path:
                    # this results in a dictionary with the path from table to all other tables
                    table_paths = {
                        target: _path
                        for (target, _path) in nx.shortest_path(
                            undirected, table
                        ).items()
                        if target in path
                    }
                    assert len(table_paths) > 0, "No valid node-paths found."
                    # fetching the target table for the shortest path in which the most missing tables appear.
                    target = sorted(
                        table_paths,
                        key=lambda t: sum(
                            node not in path for node in table_paths[t]
                        ),
                        reverse=True,
                    )[0]
                    # finding where in the path this target (first) appears. Moving one to the left so the new path being added appears after the target.
                    index = path.index(target) + 1
                    # we want to walk along the edges to the missing table, and back to the target table in a loop
                    # this makes sure that the edges are all still valid.
                    # nx.shortest_path results in paths from table to target, so need to reverse that first
                    loop = list(reversed(table_paths[target])) + table_paths[target]
                    # splicing the loop into the path
                    path[index:index] = loop
                    # removing consecutive duplicate nodes we introduced by adding the loop
                    path = list(unique_justseen(path))

            # making sure the loop above resulted in a valid path
            assert nx.is_path(undirected, path), (
                "Adding missing tables resulted in an invalid path."
            )

            # retrieving references based on the path, and removing duplicates
            references = list(
                unique_everseen(
                    undirected.get_edge_data(u, v)["reference"]
                    for (u, v) in windowed(path, 2)
                )
            )
            # removing duplicate tables from path
            join_tables = list(unique_everseen(path))

            # constructing the join clause
            join_lines = [
                f"LEFT JOIN {table} {ref}"
                for (table, ref) in zip(join_tables[1:], references, strict=False)
            ]
            join_clause = f"\nFROM {join_tables[0]}\n{'\n'.join(join_lines)}"

        return select_clause + join_clause + where_clause

    def get_load_instructions(self, columns: list[str]) -> LoadInstructions:
        """Get lists of tables that need to be inserted and retrieved, or only inserted, based on columns.

        Args:
        ----
            columns: that are to be inserted

        Returns
        -------
            insert_and_retrieve, insert
            with:
                insert_and_retrieve: tables that need to be inserted and retrieved
                insert: tables that only need to be inserted

        """
        # Find all tables that can be inserted using columns
        tables = self._get_relevant_tables(columns)

        # Find which order the tables should be inserted
        subgraph = nx.subgraph(self.graph, tables)
        order = nx.topological_sort(subgraph)

        insert_and_retrieve = []
        insert = []

        for table in order:
            # Find the prefix_column_map to rename columns
            prefix_map = self._get_table_prefix_map(table, columns)
            params = {"table": table, "columns": prefix_map}

            # insertion and retrieval only needs to be performed if the table has a primary key
            # and is referred to in the current subgraph

            # -> Look for successors of this node in the subgraph
            successors = list(subgraph.successors(table))

            if self._get_table(table).has_primary_key and len(successors) > 0:
                # Find the alias with which should be retrieved

                # Two differen list comprehensions because both the edge data and the dictionaries are theoretically allowed to be empty
                # Could do this in one but that would make it even less readable
                edge_attributes = [
                    attr
                    for child in successors
                    if (attr := self.graph.get_edge_data(table, child)) is not None
                ]
                assert len(edge_attributes) > 0, (
                    f"No attributes on edges for table '{table}' and successors '{successors}'."
                )

                # Find how the reference refers to the id, if a reference was found
                aliases = [
                    ref.from_key
                    for attr in edge_attributes
                    if (ref := attr.get("reference", None)) is not None
                ]
                assert len(aliases) > 0, (
                    f"No aliases were found, despite table '{table}' having a primary key and successors '{successors}'."
                )

                # It is possible that tables use different aliases. Currently, this code cannot handle that situation
                # so raises an exception
                if len(list(unique(aliases))) > 1:
                    message = f"Table '{table}' is referred to by multiple aliases: '{aliases}', which alias to use is ambiguous. Either use a consistent alias or insert data manually."
                    raise AmbiguousAliasesError(message)
                params.update(alias=aliases[0])

                insert_and_retrieve.append(params)
            else:
                insert.append(params)

        return LoadInstructions(
            insert_and_retrieve=insert_and_retrieve, insert=insert
        )

    def parse_insert(self, table_name: str, columns: list[str]) -> list[str]:
        """Parse input values for insert or retrieve query, and return columns that table and data have in common.

        Checks whether table exists in the database,
        and whether any of columns exist for that table.

        Args:
        ----
            table_name: name of table to be inserted into
            columns: list of columns in dataframe

        Returns
        -------
            list of columns that table and data have in common.

        """
        table = self._get_table(table_name)

        return self._parse_columns(table, columns)

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

        Raises
        ------
            NoPrimaryKeyError: when the table does not have a primary key
            AliasDoesNotExistError: when the alias does not appear in the schema

        Returns
        -------
            list of columns that table and data have in common.

        """
        table = self._get_table(table_name)

        if not table.has_primary_key:
            message = f"Table '{table_name}' does not have a primary key. It does not make sense to retrieve ids from it."
            raise NoPrimaryKeyError(message)

        # checking if alias appears in the schema
        edges = self.graph.edges(table_name)
        assert len(edges) > 0, (
            f"Table '{table_name}' has a primary key but is not connected to any edges."
        )

        references = [self.graph.get_edge_data(*edge)["reference"] for edge in edges]
        if alias not in unique([reference.from_key for reference in references]):
            message = f"Alias '{alias}' does not appear anywhere in the schema for table '{table_name}'."
            raise AliasDoesNotExistError(message)

        return table.primary_key, self._parse_columns(table, columns)

    # ---- Properties and dunder methods

    @cached_property
    def _column_table_mapping(self) -> dict[str, list[str]]:
        """Get reverse mapping from columns to tables."""
        mapping = {}
        for table_name in self.graph.nodes:
            table = self._get_table(table_name)
            for col in table.columns_and_foreign_keys:
                if col not in mapping:
                    mapping[col] = [table_name]
                else:
                    mapping[col] += [table_name]
        return mapping

    @cached_property
    def _topological_sort(self) -> list[str]:
        """Get topological sort for the entire graph."""
        return list(nx.topological_sort(self.graph))

    @property
    def is_empty(self) -> bool:
        """Returns whether the graph is empty."""
        return len(self.graph.nodes) == 0

    def __str__(self) -> str:
        """Return schema as a string."""
        return "\n".join(
            str(self.graph.nodes[table]["table"]) for table in self.graph.nodes
        )
