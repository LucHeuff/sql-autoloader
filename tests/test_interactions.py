import re
import string
from dataclasses import dataclass
from typing import Callable

import hypothesis.strategies as st
import pandas as pd
import pytest
from hypothesis import given
from hypothesis.strategies import DrawFn, composite

from etl_components.connections import (
    CursorFormat,
    postgres_formats,
    sqlite_formats,
)
from etl_components.interactions import (
    InvalidCompareQueryError,
    InvalidInsertQueryError,
    InvalidRetrieveQueryError,
    WrongDatasetPassedError,
    check_compare_query,
    check_insert_query,
    check_retrieve_query,
    get_columns_from_insert,
    get_columns_from_retrieve,
    get_table_from_insert,
    get_table_from_retrieve,
    get_values_from_insert,
    parse_compare_query,
    parse_insert_query,
    parse_retrieve_query,
)

# Nomenclature
# - a generator is a sampler that is meant to be reused in multiple strategies
# - a strategy is a bespoke sampler for a specific test

pattern_fn = Callable[[str], str]

# ---- Generators


@composite
def table_name_generator(draw: DrawFn) -> str:
    """Generate a random table name.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        table name as string

    """
    return draw(
        st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=10)
    )


@composite
def column_name_generator(draw: DrawFn) -> str:
    """Generate a random column name.

    Args:
    ----
        draw: hypothesis draw function
        size: number of columns to generate

    Returns:
    -------
        column name as string

    """
    return draw(
        st.text(alphabet=string.ascii_lowercase + "_", min_size=4, max_size=10)
    )


@composite
def gap_generator(draw: DrawFn) -> str:
    """Generate a random whitespace gap.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        gap as a atring

    """
    return draw(st.text(string.whitespace, min_size=1, max_size=3))


def postgres_value_pattern(column: str) -> str:
    """Convert column to PostgreSQL value format.

    Args:
    ----
        column: column name.

    Returns:
    -------
       column name in PostgreSQL format.

    """
    return f"%({column})s"


def sqlite_value_pattern(column: str) -> str:
    """Convert column to SQLite value format.

    Args:
    ----
        column: column name.

    Returns:
    -------
       column name in SQLite format.

    """
    return f":{column}"


@dataclass
class FormatPair:
    """Encapsulates cursor_formats_generator() output."""

    cursor_format: CursorFormat
    pattern_function: pattern_fn


POSTGRES_FORMAT_PAIR = FormatPair(postgres_formats, postgres_value_pattern)
SQLITE_FORMAT_PAIR = FormatPair(sqlite_formats, sqlite_value_pattern)

POSTGRES_WRONG_PAIR = FormatPair(postgres_formats, sqlite_value_pattern)
SQLITE_WRONG_PAIR = FormatPair(sqlite_formats, postgres_value_pattern)


@composite
def format_pair_generator(
    draw: DrawFn, *, wrong_format_pair: bool = False
) -> FormatPair:
    """Generate a random pattern function.

    Args:
    ----
        draw: hypothesis draw function
        wrong_format_pair: if the format pair should not match

    Returns:
    -------
        GeneratedCursor

    """
    options = [POSTGRES_FORMAT_PAIR, SQLITE_FORMAT_PAIR]
    if wrong_format_pair:
        options = [POSTGRES_WRONG_PAIR, SQLITE_WRONG_PAIR]
    return draw(st.sampled_from(options))


@composite
def insert_query_generator(
    draw: DrawFn,
    table: str,
    columns: list[str],
    values: list[str],
    values_pattern_function: pattern_fn,
) -> str:
    """Generate a valid insert query.

    Args:
    ----
        draw: hypothesis draw function
        table: table name
        columns: column names
        values: value names
        values_pattern_function: that transforms values to correct format

    Returns:
    -------
        insert query as a string

    """
    w = draw(st.lists(gap_generator(), min_size=6, max_size=6))
    end_noise = draw(
        st.text(alphabet=string.ascii_uppercase, min_size=0, max_size=10)
    )
    column_names = ", ".join(columns)
    value_names = ", ".join(values_pattern_function(val) for val in values)
    return f"{w[0]}INSERT INTO{w[1]}{table}{w[2]}({column_names}){w[3]}VALUES{w[4]}({value_names}){w[5]}{end_noise}"


@composite
def retrieve_query_generator(
    draw: DrawFn, table: str, columns: list[str]
) -> str:
    """Generate a valid retrieve query.

    Args:
    ----
        draw: hypothesis draw function
        table: table name
        columns: column names (optionally with aliases)

    Returns:
    -------
        retrieve query as string

    """
    w = draw(st.lists(gap_generator(), min_size=5, max_size=5))
    column_names = ", ".join(columns)
    return f"{w[0]}SELECT{w[1]}id as {table}_id, {column_names}{w[2]}FROM{w[3]}{table}"


@composite
def compare_query_generator(draw: DrawFn, columns: list[str]) -> str:
    """Generate a valid compare query.

    Args:
    ----
        draw: hypothesis draw function
        columns: columns to be compared

    Returns:
    -------
       compare query as string

    """
    n = len(columns)
    w = draw(st.lists(gap_generator(), min_size=5, max_size=5))
    tables = draw(
        st.lists(table_name_generator(), min_size=n, max_size=n, unique=True)
    )
    dotted = draw(st.lists(st.booleans(), min_size=n, max_size=n))

    select_section = f"{w[0]}SELECT{w[1]}"
    dotted_columns = [
        f"{table}.{column}" if dot else f"{column}"
        for (table, column, dot) in zip(columns, tables, dotted)
    ]
    columns_section = ", ".join(dotted_columns)
    from_section = f"{w[2]}FROM {tables[0]}{w[3]}"
    joins = [
        f"\tJOIN {table2} ON {table2}.{table1}_id = {table1}.id"
        for (table1, table2) in zip(tables[:-1], tables[1:])
    ]
    joins_section = "\n".join(joins) + f"{w[4]}"

    return select_section + columns_section + from_section + joins_section


# ---- Strategies


@dataclass
class InsertQueryComponents:
    """Encapsulates insert_query_strategy() outputs."""

    query: str
    table: str
    columns: list[str]
    values: list[str]
    cursor_format: CursorFormat


@composite
def insert_query_strategy(
    draw: DrawFn, *, wrong_format_pair: bool = False
) -> InsertQueryComponents:
    """Generate random valid insert query.

    Args:
    ----
        draw: hypothesis draw function
        wrong_format_pair: when the cursor and values format should not match

    Returns:
    -------
        InsertQueryComponents

    """
    table = draw(table_name_generator())
    num_columns = draw(st.integers(min_value=1, max_value=5))
    columns = draw(
        st.lists(
            column_name_generator(),
            min_size=num_columns,
            max_size=num_columns,
            unique=True,
        )
    )
    values = draw(
        st.lists(
            column_name_generator(),
            min_size=num_columns,
            max_size=num_columns,
            unique=True,
        )
    )
    format_pair = draw(
        format_pair_generator(wrong_format_pair=wrong_format_pair)
    )
    query = draw(
        insert_query_generator(
            table, columns, values, format_pair.pattern_function
        )
    )
    return InsertQueryComponents(
        query, table, columns, values, format_pair.cursor_format
    )


@composite
def invalid_insert_query_strategy(
    draw: DrawFn,
) -> tuple[list[str], CursorFormat]:
    """Generate random invalid insert query.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        list of invalid insert queries, CursorFormat


    """
    random_text = draw(st.text(min_size=2, max_size=10))

    valid_components = draw(insert_query_strategy())

    no_table_query = valid_components.query.replace(valid_components.table, "")
    no_values_query = valid_components.query.split("VALUES")[0]
    lowercase_query = valid_components.query.lower()

    return [
        random_text,
        no_table_query,
        no_values_query,
        lowercase_query,
    ], valid_components.cursor_format


@dataclass
class ParseInsertQueryComponents:
    """Encapsulates parse_insert_strategy() outputs."""

    query: str
    table: str
    columns: list[str]
    values: list[str]
    data: pd.DataFrame
    cursor_format: CursorFormat


@composite
def parse_insert_strategy(draw: DrawFn) -> ParseInsertQueryComponents:
    """Generate examples for parse_insert_query().

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
       ParseInsertQueryComponents

    """
    query_components = draw(insert_query_strategy())
    data = pd.DataFrame({col: [1, 2, 3] for col in query_components.values})
    return ParseInsertQueryComponents(
        **query_components.__dict__,
        data=data,
    )


# ---- Retrieve query strategies


@dataclass
class RetrieveQueryComponents:
    """Encapsulates output from retrieve_query_strategy()."""

    query: str
    table: str
    columns: list[str]
    cursor_format: CursorFormat


@composite
def retrieve_query_strategy(draw: DrawFn) -> RetrieveQueryComponents:
    """Generate a random valid retrieve query.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        RetrieveQueryComponents

    """
    table = draw(table_name_generator())
    num_columns = draw(st.integers(min_value=1, max_value=5))
    # generate whether each column has an alias
    has_alias = draw(
        st.lists(st.booleans(), min_size=num_columns, max_size=num_columns)
    )
    # generating columns and aliases
    columns = draw(
        st.lists(
            column_name_generator(),
            min_size=num_columns,
            max_size=num_columns,
            unique=True,
        )
    )
    aliases = draw(
        st.lists(
            column_name_generator(),
            min_size=num_columns,
            max_size=num_columns,
            unique=True,
        )
    )
    # weaving aliases into SQL string
    columns_with_aliases = [
        f"{col} as {al}" if has else col
        for (col, al, has) in zip(columns, aliases, has_alias)
    ]
    # weaving out which name is at the end
    effective_columns = [
        al if has else col
        for (col, al, has) in zip(columns, aliases, has_alias)
    ]
    format_pair = draw(format_pair_generator())
    query = draw(retrieve_query_generator(table, columns_with_aliases))
    return RetrieveQueryComponents(
        query, table, effective_columns, format_pair.cursor_format
    )


@composite
def invalid_retrieve_query_strategy(
    draw: DrawFn,
) -> tuple[list[str], CursorFormat]:
    """Generate random invalid retrieve queries.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
       list of invalid retrieve queries, CursorFormat

    """
    random_text = draw(st.text(min_size=2, max_size=10))
    components = draw(retrieve_query_strategy())

    no_table_query = components.query.replace(components.table, "")
    no_id_query = components.query.replace("id as", "")
    lowercase_query = components.query.lower()

    return [
        random_text,
        no_table_query,
        no_id_query,
        lowercase_query,
    ], components.cursor_format


@dataclass
class ParseRetrieveQueryComponents:
    """Encapsulates parse_retrieve_strategy() outputs."""

    query: str
    table: str
    columns: list[str]
    data: pd.DataFrame
    cursor_format: CursorFormat


@composite
def parse_retrieve_strategy(draw: DrawFn) -> ParseRetrieveQueryComponents:
    """Generate examples for parse_retrieve_query().

    Args:
    ----
        draw: hypothesis draw function.

    Returns:
    -------
       ParseRetrieveQueryComponents

    """
    query_components = draw(retrieve_query_strategy())
    data = pd.DataFrame({col: [1, 2, 3] for col in query_components.columns})
    return ParseRetrieveQueryComponents(
        **query_components.__dict__,
        data=data,
    )


# ---- compare query strategies


@dataclass
class CompareQueryComponents:
    """Encapsulates retrieve_query_strategy() outputs."""

    query: str
    columns: list[str]
    cursor_format: CursorFormat


@composite
def compare_query_strategy(draw: DrawFn) -> CompareQueryComponents:
    """Generate a random valid compare query.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        CompareQueryComponents

    """
    num_columns = draw(st.integers(min_value=2, max_value=5))
    columns = draw(
        st.lists(
            column_name_generator(),
            min_size=num_columns,
            max_size=num_columns,
            unique=True,
        )
    )

    format_pair = draw(format_pair_generator())
    query = draw(compare_query_generator(columns))
    return CompareQueryComponents(query, columns, format_pair.cursor_format)


@composite
def invalid_compare_query_strategy(
    draw: DrawFn,
) -> tuple[list[str], CursorFormat]:
    """Generate random invalid compare queries.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        list of invalid compare queries

    """
    random_text = draw(st.text(min_size=2, max_size=10))
    components = draw(compare_query_strategy())

    no_joins_query = re.sub("JOIN", "", components.query)
    no_id_query = components.query.replace("id", "")
    lowercase_query = components.query.lower()

    return [
        random_text,
        no_joins_query,
        no_id_query,
        lowercase_query,
    ], components.cursor_format


@dataclass
class ParseCompareQueryComponents:
    """Encapsulates parse_compare_strategy() outputs."""

    query: str
    columns: list[str]
    data: pd.DataFrame
    cursor_format: CursorFormat


@composite
def parse_compare_strategy(draw: DrawFn) -> ParseCompareQueryComponents:
    """Generate examples for parse_compare_query().

    Args:
    ----
        draw: hypothesis draw function.

    Returns:
    -------
        ParseCompareQueryComponents

    """
    query_components = draw(compare_query_strategy())
    # making sure '_id' does not accidentally show up in column names
    columns = [re.sub("_id", "__", col) for col in query_components.columns]
    data = pd.DataFrame({col: [1, 2, 3] for col in columns})
    return ParseCompareQueryComponents(
        **query_components.__dict__,
        data=data,
    )


# ---- Testing insert query parsing
TEST_FORMAT = "TEST"


@given(components=insert_query_strategy())
def test_check_insert_query(components: InsertQueryComponents) -> None:
    """Test whether valid insert queries correctly pass.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        check_insert_query(components.query, components.cursor_format) is None
    )


@given(data=invalid_insert_query_strategy())
def test_check_insert_query_raises(
    data: tuple[list[str], CursorFormat],
) -> None:
    """Test whether invalid insert queries correctly raise exceptions.

    Args:
    ----
        data: tuple of list of invalid queries, CursorFormat

    """
    queries, cursor_format = data
    for query in queries:
        with pytest.raises(InvalidInsertQueryError):
            check_insert_query(query, cursor_format)


@given(components=insert_query_strategy())
def test_get_table_from_insert(components: InsertQueryComponents) -> None:
    """Test whether get_table_from_insert() correctly retrieves the table.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_table_from_insert(components.query, components.cursor_format)
        == components.table
    )


@given(components=insert_query_strategy())
def test_get_columns_from_insert(components: InsertQueryComponents) -> None:
    """Test whether get_columns_from_insert() correctly retrieves columns from insert query.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_columns_from_insert(components.query, components.cursor_format)
        == components.columns
    )


@given(components=insert_query_strategy())
def test_get_columns_from_insert_raises(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_columns_from_insert() raises InvalidInsertQueryError correctly.

    Args:
    ----
        components: InsertQueryComponents

    """
    # columns entered wrong
    replace_columns = ", ".join(components.columns)
    wrong_query = re.sub(replace_columns, "", components.query)

    with pytest.raises(InvalidInsertQueryError):
        get_columns_from_insert(wrong_query, components.cursor_format)


@given(components=insert_query_strategy())
def test_get_values_from_insert(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_values_from_insert() correctly retrieves columns.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_values_from_insert(components.query, components.cursor_format)
        == components.values
    )


@given(components=insert_query_strategy(wrong_format_pair=True))
def test_get_values_from_insert_raises(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_values_from_insert() correctly throws an exception when formats do not match.

    Args:
    ----
        components: InsertQueryComponents

    """
    with pytest.raises(InvalidInsertQueryError):
        get_values_from_insert(components.query, components.cursor_format)


@given(components=parse_insert_strategy())
def test_parse_insert_query(components: ParseInsertQueryComponents) -> None:
    """Test whether parse_insert_query() correctly retrieves table, columns and values.

    Args:
    ----
        components: ParseInsertQueryComponents

    """
    out = components.columns, components.values
    assert (
        parse_insert_query(
            components.query, components.data, components.cursor_format
        )
        == out
    )


@given(components=parse_insert_strategy())
def test_parse_insert_query_raises(
    components: ParseInsertQueryComponents,
) -> None:
    """Test whether parse_insert_query() correctly raises an exception when columns don't appear in data.

    Args:
    ----
        components: ParseInsertQueryComponents

    """
    data = components.data
    data.columns = [f"{col}_" for col in data.columns]
    with pytest.raises(InvalidInsertQueryError):
        parse_insert_query(components.query, data, components.cursor_format)


# ---- Testing retrieve query parsing


@given(components=retrieve_query_strategy())
def test_check_retrieve_query(components: RetrieveQueryComponents) -> None:
    """Test whether valid retrieve queries correctly pass.

    Args:
    ----
        components: RetrieveQueryComponents

    """
    assert (
        check_retrieve_query(components.query, components.cursor_format) is None
    )


@given(data=invalid_retrieve_query_strategy())
def test_check_retrieve_query_raises(
    data: tuple[list[str], CursorFormat],
) -> None:
    """Test whether invalid retrieve queries correctly raise exceptions.

    Args:
    ----
        data: list of invalid queries, CursorFormat

    """
    queries, cursor_format = data
    for query in queries:
        with pytest.raises(InvalidRetrieveQueryError):
            check_retrieve_query(query, cursor_format)


@given(components=retrieve_query_strategy())
def test_get_table_from_retrieve(components: RetrieveQueryComponents) -> None:
    """Test whether get_table_from_retrieve() correctly retrieves table.

    Args:
    ----
        components: RetrieveQueryComponents

    """
    assert (
        get_table_from_retrieve(components.query, components.cursor_format)
        == components.table
    )


@given(components=retrieve_query_strategy())
def test_get_table_from_retrieve_raises(
    components: RetrieveQueryComponents,
) -> None:
    """Test whether get_table_from_retrieve() raises an exception when table cannot be found.

    Args:
    ----
        components: RetrieveQueryComponents

    """
    wrong_query = re.sub(
        rf"FROM\s*{components.table}", "FROM ", components.query
    )
    with pytest.raises(InvalidRetrieveQueryError):
        get_table_from_retrieve(wrong_query, components.cursor_format)


@given(components=retrieve_query_strategy())
def test_get_columns_from_retrieve(components: RetrieveQueryComponents) -> None:
    """Test whether get_columns_from_retrieve() correctly retrieves columns.

    Args:
    ----
        components: RetrieveQueryComponents

    """
    assert (
        get_columns_from_retrieve(components.query, components.cursor_format)
        == components.columns
    )


@given(components=retrieve_query_strategy())
def test_get_columns_from_retrieve_raises(
    components: RetrieveQueryComponents,
) -> None:
    """Test whether get_columns_from_retrieve() correctly raises an exception when columns are missing.

    Args:
    ----
        components: RetrieveQueryComponents

    """
    # columns entered wrong
    wrong_query = re.sub(
        r"\s*SELECT\s*.*\s*FROM", "SELECT FROM", components.query
    )
    with pytest.raises(InvalidRetrieveQueryError):
        get_columns_from_retrieve(wrong_query, components.cursor_format)


@given(components=parse_retrieve_strategy())
def test_parse_retrieve_query(components: ParseRetrieveQueryComponents) -> None:
    """Test whether parse_retrieve_query() correctly retrieves table and columns.

    Args:
    ----
        components: ParseRetrieveQueryComponents

    """
    out = components.columns
    assert (
        parse_retrieve_query(
            components.query, components.data, components.cursor_format
        )
        == out
    )


@given(components=parse_retrieve_strategy())
def test_parse_retrieve_query_raises(
    components: ParseRetrieveQueryComponents,
) -> None:
    """Test whether parse_retrieve_query() correctly raises an exception when columns don't appear in data.

    Args:
    ----
        components: ParseRetrieveQueryComponents

    """
    data = components.data
    data.columns = [f"{col}_" for col in data.columns]
    with pytest.raises(InvalidRetrieveQueryError):
        parse_retrieve_query(components.query, data, components.cursor_format)


# ---- Testing compare query parsing


@given(components=compare_query_strategy())
def test_check_compare_query(components: CompareQueryComponents) -> None:
    """Test whether valid compare queries correctly pass.

    Args:
    ----
        components: CompareQueryComponents

    """
    assert (
        check_compare_query(components.query, components.cursor_format) is None
    )


@given(data=invalid_compare_query_strategy())
def test_check_compare_query_raises(
    data: tuple[list[str], CursorFormat],
) -> None:
    """Test whether invalid compare data correctly raise exceptions.

    Args:
    ----
        data: list of invalid queries

    """
    queries, cursor_format = data
    for query in queries:
        with pytest.raises(InvalidCompareQueryError):
            check_compare_query(query, cursor_format)


@given(components=parse_compare_strategy())
def test_parse_compare_query(components: ParseCompareQueryComponents) -> None:
    """Test whether parse_compare_query() correctly passes.

    Args:
    ----
        components: ParseCompareQueryComponents

    """
    assert (
        parse_compare_query(
            components.query, components.data, components.cursor_format
        )
        is None
    )


@given(components=parse_compare_strategy())
def test_parse_compare_query_raises(
    components: ParseCompareQueryComponents,
) -> None:
    """Test whether parse_compare_query() correctly raises an exception when '_id' are in column names.

    Args:
    ----
        components: ParseCompareQueryComponents

    """
    data = components.data
    data.columns = [f"{col}_id" for col in data.columns]
    with pytest.raises(WrongDatasetPassedError):
        parse_compare_query(components.query, data, components.cursor_format)
