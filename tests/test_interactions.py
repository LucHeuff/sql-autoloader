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
    POSTGRES_VALUES_PATTERN,
    SQLITE_VALUES_PATTERN,
    PostgresCursor,
    SQLiteCursor,
)
from etl_components.interactions import (
    Cursor,
    InvalidInsertQueryError,
    ParsedQueryComponents,
    check_insert_query,
    get_columns_from_insert,
    get_table_from_insert,
    get_values_from_insert,
    parse_insert_query,
)

# Nomenclature
# - a generator is a sampler that is meant to be reused in multiple strategies
# - a strategy is a bespoke sampler for a specific test

pattern_fn = Callable[[str], str]


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


@dataclass
class GeneratedCursor:
    """Encapsulates cursor_generator() output."""

    cursor: Cursor
    pattern_function: pattern_fn


POSTGRES_CURSOR_PAIR = GeneratedCursor(PostgresCursor, postgres_value_pattern)
SQLITE_CURSOR_PAIR = GeneratedCursor(SQLiteCursor, sqlite_value_pattern)


@composite
def cursor_generator(draw: DrawFn) -> GeneratedCursor:
    """Generate a random pattern function.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        GeneratedCursor

    """
    cursor_and_pattern_options = [
        POSTGRES_CURSOR_PAIR,
        SQLITE_CURSOR_PAIR,
    ]
    return draw(st.sampled_from(cursor_and_pattern_options))


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


@dataclass
class InsertQueryComponents:
    """Encapsulates insert_query_strategy() outputs."""

    query: str
    table: str
    columns: list[str]
    values: list[str]


@composite
def insert_query_strategy(
    draw: DrawFn, generated_cursor: GeneratedCursor | None = None
) -> InsertQueryComponents:
    """Generate random valid insert query.

    Args:
    ----
        draw: hypothesis draw function
        generated_cursor: GeneratedCursor

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
    if generated_cursor is None:
        generated_cursor = draw(cursor_generator())
    query = draw(
        insert_query_generator(
            table, columns, values, generated_cursor.pattern_function
        )
    )
    return InsertQueryComponents(query, table, columns, values)


@composite
def invalid_insert_query_strategy(draw: DrawFn) -> list[str]:
    """Generate random invalid insert query.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        list of invalid insert queries


    """
    random_text = draw(st.text(min_size=2, max_size=10))

    table = draw(table_name_generator())
    num_columns = draw(st.integers(min_value=1, max_value=5))
    columns = draw(
        st.lists(
            column_name_generator(), min_size=num_columns, max_size=num_columns
        )
    )
    values = draw(
        st.lists(
            column_name_generator(), min_size=num_columns, max_size=num_columns
        )
    )
    pattern_function = draw(cursor_generator()).pattern_function

    valid_query = draw(
        insert_query_generator(table, columns, values, pattern_function)
    )

    no_table_query = valid_query.replace(table, "")
    no_values_query = valid_query.split("VALUES")[0]
    lowercase_query = valid_query.lower()

    return [
        random_text,
        no_table_query,
        no_values_query,
        lowercase_query,
    ]


@dataclass
class ParseInsertQueryComponents:
    """Encapsulates parse_insert_strategy() outputs."""

    cursor: Cursor
    query: str
    table: str
    columns: list[str]
    values: list[str]
    data: pd.DataFrame


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
    generated_cursor = draw(cursor_generator())
    query_components = draw(
        insert_query_strategy(generated_cursor=generated_cursor)
    )
    data = pd.DataFrame({col: [1, 2, 3] for col in query_components.values})
    return ParseInsertQueryComponents(
        generated_cursor.cursor,
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
    assert check_insert_query(components.query, TEST_FORMAT) is None


@given(queries=invalid_insert_query_strategy())
def test_check_insert_query_raises(queries: list[str]) -> None:
    """Test whether invalid input queries correctly raise exceptions.

    Args:
    ----
        queries: list of invalid queries

    """
    for query in queries:
        with pytest.raises(InvalidInsertQueryError):
            check_insert_query(query, TEST_FORMAT)


@given(components=insert_query_strategy())
def test_get_table_from_insert(components: InsertQueryComponents) -> None:
    """Test whether get_table_from_insert() correctly retrieves the table.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_table_from_insert(components.query, TEST_FORMAT) == components.table
    )


@given(components=insert_query_strategy())
def test_get_columns_from_insert(components: InsertQueryComponents) -> None:
    """Test whether get_columns_from_insert() correctly retrieves columns from insert query.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_columns_from_insert(components.query, TEST_FORMAT)
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
    replace_columns = "|".join(components.columns)
    wrong_query = re.sub(replace_columns, "", components.query)

    with pytest.raises(InvalidInsertQueryError):
        get_columns_from_insert(wrong_query, TEST_FORMAT)


@given(components=insert_query_strategy(generated_cursor=SQLITE_CURSOR_PAIR))
def test_get_values_from_insert_sqlite(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_values_from_insert() correctly retrieves columns for SQLite queries.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_values_from_insert(
            components.query, SQLITE_VALUES_PATTERN, TEST_FORMAT
        )
        == components.values
    )


@given(components=insert_query_strategy(generated_cursor=POSTGRES_CURSOR_PAIR))
def test_get_values_from_insert_sqlite_raises(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_values_from_insert() correctly retrieves columns for PostgreSQL queries.

    Args:
    ----
        components: InsertQueryComponents (using PostgreSQL format instead)

    """
    with pytest.raises(InvalidInsertQueryError):
        get_values_from_insert(
            components.query, SQLITE_VALUES_PATTERN, TEST_FORMAT
        )


@given(components=insert_query_strategy(generated_cursor=POSTGRES_CURSOR_PAIR))
def test_get_values_from_insert_postgres(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_values_from_insert() correctly retrieves columns for SQLite queries.

    Args:
    ----
        components: InsertQueryComponents

    """
    assert (
        get_values_from_insert(
            components.query, POSTGRES_VALUES_PATTERN, TEST_FORMAT
        )
        == components.values
    )


@given(components=insert_query_strategy(generated_cursor=SQLITE_CURSOR_PAIR))
def test_get_values_from_insert_postgres_raises(
    components: InsertQueryComponents,
) -> None:
    """Test whether get_values_from_insert() correctly retrieves columns for SQLite queries.

    Args:
    ----
        components: InsertQueryComponents (using SQLite format instead)

    """
    with pytest.raises(InvalidInsertQueryError):
        get_values_from_insert(
            components.query, POSTGRES_VALUES_PATTERN, TEST_FORMAT
        )


@given(components=parse_insert_strategy())
def test_parse_insert_query(components: ParseInsertQueryComponents) -> None:
    """Test whether parse_insert_query() correctly retrieves table, columns and values.

    Args:
    ----
        components: ParseInsertQueryComponents

    """
    out = ParsedQueryComponents(
        components.table, components.columns, components.values
    )
    assert (
        parse_insert_query(components.cursor, components.query, components.data)
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
        parse_insert_query(components.cursor, components.query, data)
