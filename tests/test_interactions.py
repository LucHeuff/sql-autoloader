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
    PostgresFormat,
    SQLFormat,
    SQLiteFormat,
)
from etl_components.interactions import (
    InsertAndRetrieveParts,
    InvalidCompareQueryError,
    InvalidInsertAndRetrieveQueryError,
    InvalidInsertQueryError,
    InvalidRetrieveQueryError,
    QueryParts,
    WrongDatasetPassedError,
    check_columns_in_data,
    parse_compare_query,
    parse_insert_and_retrieve_query,
    parse_insert_query,
    parse_retrieve_query,
)

# Nomenclature
# - a generator is a sampler that is meant to be reused in multiple strategies
# - a strategy is a bespoke sampler for a specific test

pattern_fn = Callable[[str], str]

# ---- Generators


@composite
def table_generator(draw: DrawFn) -> str:
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
def columns_generator(draw: DrawFn, size: int) -> list[str]:
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
        st.lists(
            st.text(
                alphabet=string.ascii_lowercase + "_", min_size=4, max_size=10
            ),
            min_size=size,
            max_size=size,
            unique=True,
        )
    )


@composite
def gaps_generator(draw: DrawFn, size: int) -> list[str]:
    """Generate a random whitespace gap.

    Args:
    ----
        draw: hypothesis draw function
        size: number of whitespace elements to generate

    Returns:
    -------
        gap as a atring

    """
    return draw(
        st.lists(
            st.text(string.whitespace, min_size=1, max_size=3),
            min_size=size,
            max_size=size,
        )
    )


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

    sql_format: SQLFormat
    values_pattern_function: pattern_fn


POSTGRES_FORMAT_PAIR = FormatPair(PostgresFormat(), postgres_value_pattern)
SQLITE_FORMAT_PAIR = FormatPair(SQLiteFormat(), sqlite_value_pattern)

POSTGRES_WRONG_PAIR = FormatPair(PostgresFormat(), sqlite_value_pattern)
SQLITE_WRONG_PAIR = FormatPair(SQLiteFormat(), postgres_value_pattern)


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
def insert_query_generator(draw: DrawFn) -> str:
    """Generate insert query with table, columns and values placeholders.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        query string with <table>, <columns> and <values> placeholders

    """
    pattern = (
        r"^\s+INSERT INTO\s+<table>\s+\(<columns>\)\s+VALUES\s+\(<values>\)"
    )
    return draw(st.from_regex(pattern))


@composite
def retrieve_query_generator(draw: DrawFn) -> str:
    """Generate retrieve query with table and columns placeholders.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        query string with <table> and <columns> placeholders


    """
    pattern = r"^\s+SELECT\s+id as <table>_id, <columns>\s+FROM\s+<table>\s+$"
    return draw(st.from_regex(pattern))


@composite
def compare_query_generator(draw: DrawFn, n_tables: int) -> str:
    """Generate compare query with tables and columns placeholders.

    Args:
    ----
        draw: hypothesis draw function
        n_tables: number of tables in the query

    Returns:
    -------
        query string with <table{n}> and <columns> placeholders

    """
    select_pattern = r"^\s+SELECT\s+<columns>\s+FROM <table0>\s+"
    join_patterns = [
        rf"JOIN <table{n+1}> ON <table{n+1}>\.<table{n}>_id = <table{n}>\.id\s+"
        for n in range(n_tables - 1)
    ]
    pattern = select_pattern + "".join(join_patterns)
    return draw(st.from_regex(pattern))


# ---- Strategies


@dataclass
class CheckColumnsComponents:
    """Encapsulates columns_in_data_strategy() output."""

    columns: list[str]
    data: pd.DataFrame
    correct: bool


@composite
def columns_in_data_strategy(draw: DrawFn) -> CheckColumnsComponents:
    """Generate columns, data and correctness for check_columns_in_data().

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        CheckColumnsComponents

    """
    n = draw(st.integers(min_value=1, max_value=10))
    correct = draw(st.booleans())
    columns = draw(columns_generator(n))
    data_columns = columns if correct else [f"{col}_" for col in columns]
    data = pd.DataFrame({col: [1, 2] for col in data_columns})
    return CheckColumnsComponents(columns, data, correct)


@dataclass
class ParseInsertComponents:
    """Encapsulates output from parse_insert_strategy."""

    query: str
    data: pd.DataFrame
    sql_format: SQLFormat
    parts: QueryParts


@composite
def parse_insert_query_strategy(draw: DrawFn) -> ParseInsertComponents:
    """Generate a valid insert query and its parts.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseInsertComponents

    """
    table = draw(table_generator())
    n_columns = draw(st.integers(min_value=1, max_value=5))
    columns = draw(columns_generator(n_columns))
    values = draw(columns_generator(n_columns))
    format_pair = draw(format_pair_generator())

    parts = QueryParts(table, columns, values)

    # translating parts to the right format
    columns_section = ", ".join(columns)
    values_section = ", ".join(
        format_pair.values_pattern_function(val) for val in values
    )

    query = (
        draw(insert_query_generator())
        .replace("<table>", table)
        .replace("<columns>", columns_section)
        .replace("<values>", values_section)
    )

    data = pd.DataFrame({col: [1] for col in values})

    return ParseInsertComponents(query, data, format_pair.sql_format, parts)


@dataclass
class ParseInvalidInsertComponents:
    """Encapsulates parse_invalid_insert_strategy() outputs."""

    query: str
    data: pd.DataFrame
    sql_format: SQLFormat
    wrong: list[str]


@composite
def parse_invalid_insert_query_strategy(
    draw: DrawFn,
) -> ParseInvalidInsertComponents:
    """Generate invalid insert query or data.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseInvalidInsertComponents

    """
    # Generating query parts
    table = draw(table_generator())
    # at least 2 columns, otherwise problems with columns are indistinguishable
    n_columns = draw(st.integers(min_value=4, max_value=8))
    columns = draw(columns_generator(n_columns))
    values = draw(columns_generator(n_columns))

    # generating what is wrong
    wrong_options = [
        "table",
        "columns",
        "values",
        "fewer_columns",
        "fewer_values",
        "data",
        "format",
        "lower",
    ]
    wrong = draw(
        st.lists(
            st.sampled_from(wrong_options),
            min_size=1,
            max_size=len(wrong_options),
            unique=True,
        )
    )
    # generating format pair (which may be wrong)
    format_pair = draw(
        format_pair_generator(wrong_format_pair=("format" in wrong))
    )

    # removing some columns or values if necessary
    if "fewer_columns" in wrong:
        columns = columns[:-1]

    if "fewer_values" in wrong:
        # remove a different number of values, in case "fewer_columns" is also chosen
        values = values[:-2]

    # translating parts to the right (or wrong) format
    table_section = "" if ("table" in wrong) else table
    columns_section = "" if ("columns" in wrong) else ", ".join(columns)
    values_section = (
        ""
        if ("values" in wrong)
        else ", ".join(
            format_pair.values_pattern_function(val) for val in values
        )
    )

    # generating query and replacing
    query = draw(insert_query_generator())
    query = (
        query.replace("<table>", table_section)
        .replace("<columns>", columns_section)
        .replace("<values>", values_section)
    )
    # to lowercase if required
    if "lower" in wrong:
        query = query.lower()

    # generating (wrong) data
    if "data" in wrong:
        data = pd.DataFrame({f"{col}_": [1] for col in values})
    else:
        data = pd.DataFrame({col: [1] for col in values})

    return ParseInvalidInsertComponents(
        query, data, format_pair.sql_format, wrong
    )


# ---- Retrieve query strategies


@dataclass
class ParseRetrieveComponents:
    """Encapsulates parse_retrieve_query_strategy() outputs."""

    query: str
    data: pd.DataFrame
    sql_format: SQLFormat
    parts: QueryParts


@composite
def parse_retrieve_query_strategy(draw: DrawFn) -> ParseRetrieveComponents:
    """Generate a valid retrieve query and its parts.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseRetrieveComponents

    """
    table = draw(table_generator())
    n_columns = draw(st.integers(min_value=2, max_value=5))
    columns = draw(columns_generator(n_columns))
    values = draw(columns_generator(n_columns))
    has_alias = draw(
        st.lists(st.booleans(), min_size=n_columns, max_size=n_columns)
    )

    # generating format pair
    format_pair = draw(format_pair_generator())

    columns_with_aliases = [
        f"{col} as {val}" if has else col
        for (col, val, has) in zip(columns, values, has_alias)
    ]
    columns_section = ", ".join(columns_with_aliases)
    # making sure values also follow the alias format
    values = [
        val if has else col
        for (col, val, has) in zip(columns, values, has_alias)
    ]

    parts = QueryParts(table, columns, values)

    query = (
        draw(retrieve_query_generator())
        .replace("<table>", table)
        .replace("<columns>", columns_section)
    )

    data = pd.DataFrame({col: [1] for col in values})

    return ParseRetrieveComponents(query, data, format_pair.sql_format, parts)


@dataclass
class ParseInvalidRetrieveComponents:
    """Encapsulates parse_invalid_retrieve_query_strategy() outputs."""

    query: str
    data: pd.DataFrame
    sql_format: SQLFormat
    wrong: list[str]


@composite
def parse_invalid_retrieve_query_strategy(
    draw: DrawFn,
) -> ParseInvalidRetrieveComponents:
    """Generate invalid retrieve query or data.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseInvalidRetrieveComponents

    """
    # Generating query parts
    table = draw(table_generator())
    # at least 2 columns, otherwise problems with columns are indistinguishable
    n_columns = draw(st.integers(min_value=2, max_value=5))
    columns = draw(columns_generator(n_columns))
    values = draw(columns_generator(n_columns))
    has_alias = draw(
        st.lists(st.booleans(), min_size=n_columns, max_size=n_columns)
    )
    format_pair = draw(format_pair_generator())

    # generating what is wrong
    wrong_options = ["no_table", "no_table_match", "columns", "data", "lower"]
    wrong = draw(
        st.lists(
            st.sampled_from(wrong_options),
            min_size=1,
            max_size=len(wrong_options),
            unique=True,
        )
    )

    # translating parts to the right (or wrong) format
    table_section = "" if ("no_table" in wrong) else table
    if "columns" in wrong:
        columns_section = ""
    else:
        # doing columns the correct way with optional aliases
        columns_with_aliases = [
            f"{col} as {val}" if has else col
            for (col, val, has) in zip(columns, values, has_alias)
        ]
        columns_section = ", ".join(columns_with_aliases)
        # making sure values also follow the alias format
        values = [
            val if has else col
            for (col, val, has) in zip(columns, values, has_alias)
        ]

    # generating query and replacing
    query = draw(retrieve_query_generator())
    query = query.replace("<table>", table_section).replace(
        "<columns>", columns_section
    )
    if "no_table_match" in wrong:
        query = re.sub(rf"FROM\s+{table}", f"FROM {table}_", query)

    if "lower" in wrong:
        query = query.lower()

    if "data" in wrong:
        data = pd.DataFrame({f"{col}_": [1] for col in values})
    else:
        data = pd.DataFrame({col: [1] for col in values})

    return ParseInvalidRetrieveComponents(
        query, data, format_pair.sql_format, wrong
    )


# ---- insert and retrieve strategies
@dataclass
class ParseInsertAndRetrieveComponents:
    """Encapsulates parse_insert_and_retrieve_strategy() outputs."""

    insert_query: str
    retrieve_query: str
    data: pd.DataFrame
    sql_format: SQLFormat
    parts: InsertAndRetrieveParts


@composite
def parse_insert_and_retrieve_query_strategy(
    draw: DrawFn,
) -> ParseInsertAndRetrieveComponents:
    """Generate valid insert and retrieve queries and their parts.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseInsertAndRetrieveComponents

    """
    table = draw(table_generator())
    n_columns = draw(st.integers(min_value=2, max_value=5))
    columns = draw(columns_generator(n_columns))
    values = draw(columns_generator(n_columns))
    format_pair = draw(format_pair_generator())

    parts = InsertAndRetrieveParts(values, values)

    insert_columns = ", ".join(columns)
    insert_values = ", ".join(
        format_pair.values_pattern_function(val) for val in values
    )
    retrieve_columns = ", ".join(
        f"{col} as {val}" for (col, val) in zip(columns, values)
    )

    insert_query = (
        draw(insert_query_generator())
        .replace("<table>", table)
        .replace("<columns>", insert_columns)
        .replace("<values>", insert_values)
    )

    retrieve_query = (
        draw(retrieve_query_generator())
        .replace("<table>", table)
        .replace("<columns>", retrieve_columns)
    )

    data = pd.DataFrame({col: [1] for col in values})

    return ParseInsertAndRetrieveComponents(
        insert_query, retrieve_query, data, format_pair.sql_format, parts
    )


@dataclass
class ParseInvalidInsertAndRetrieveComponents:
    """Encapsulates parse_invalid_insert_and_retrieve_query_strategy() outputs."""

    insert_query: str
    retrieve_query: str
    data: pd.DataFrame
    sql_format: SQLFormat
    wrong: list[str]


@composite
def parse_invalid_insert_and_retrieve_query_strategy(
    draw: DrawFn,
) -> ParseInvalidInsertAndRetrieveComponents:
    """Generate invalid insert and retrieve combination.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseInvalidInsertAndRetrieveComponents

    """
    table = draw(table_generator())
    # at least 2 columns, otherwise problems with columns are indistinguishable
    n_columns = draw(st.integers(min_value=2, max_value=5))
    columns = draw(columns_generator(n_columns))
    values = draw(columns_generator(n_columns))
    format_pair = draw(format_pair_generator())

    # generating what is going wrong
    wrong_options = ["tables", "columns"]
    wrong = draw(
        st.lists(
            st.sampled_from(wrong_options),
            min_size=1,
            max_size=len(wrong_options),
            unique=True,
        )
    )

    insert_columns = ", ".join(columns)
    insert_values = ", ".join(
        format_pair.values_pattern_function(val) for val in values
    )

    # generating data for retrieve query which may be wrong
    retrieve_table = f"{table}_" if ("tables" in wrong) else table
    retrieve_columns = (
        [f"{col}_" for col in columns] if ("columns" in wrong) else columns
    )

    retrieve_columns_section = ", ".join(
        f"{col} as {val}" for (col, val) in zip(retrieve_columns, values)
    )

    insert_query = (
        draw(insert_query_generator())
        .replace("<table>", table)
        .replace("<columns>", insert_columns)
        .replace("<values>", insert_values)
    )

    retrieve_query = (
        draw(retrieve_query_generator())
        .replace("<table>", retrieve_table)
        .replace("<columns>", retrieve_columns_section)
    )

    data = pd.DataFrame({col: [1] for col in values})

    return ParseInvalidInsertAndRetrieveComponents(
        insert_query, retrieve_query, data, format_pair.sql_format, wrong
    )


# ---- compare query strategies
@dataclass
class ParseCompareComponents:
    """Encapsulates parse_compare_query_strategy() outputs."""

    query: str
    orig_data: pd.DataFrame
    sql_format: SQLFormat


@composite
def parse_compare_query_strategy(draw: DrawFn) -> ParseCompareComponents:
    """Generate valid compare query and data.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
        ParseCompareComponents

    """
    n_tables = draw(st.integers(min_value=2, max_value=5))
    n_columns = draw(st.integers(min_value=2, max_value=5))
    tables = draw(columns_generator(n_tables))
    columns = draw(columns_generator(n_columns))
    format_pair = draw(format_pair_generator())

    query = draw(compare_query_generator(n_tables=n_tables))

    columns_section = ", ".join(columns)

    query = query.replace("<columns>", columns_section)
    for i, table in enumerate(tables):
        query = query.replace(f"<table{i}>", table)

    data = pd.DataFrame({col: [1] for col in columns})

    return ParseCompareComponents(query, data, format_pair.sql_format)


@dataclass
class ParseInvalidCompareComponents:
    """Encapsulates parse_invalid_compare_query_strategy() outputs."""

    query: str
    orig_data: pd.DataFrame
    sql_format: SQLFormat


@composite
def parse_invalid_compare_query_strategy(
    draw: DrawFn,
) -> ParseInvalidCompareComponents:
    """Generate a compare query in an incorrect format.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
       ParseInvalidCompareComponents

    """
    parts = draw(parse_compare_query_strategy())
    query = parts.query.lower()
    return ParseInvalidCompareComponents(
        query, parts.orig_data, parts.sql_format
    )


@composite
def parse_invalid_compare_dataset_strategy(
    draw: DrawFn,
) -> ParseInvalidCompareComponents:
    """Generate a compare query with wrong data.

    Args:
    ----
        draw: hypothesis draw function

    Returns:
    -------
       ParseInvalidCompareComponents


    """
    parts = draw(parse_compare_query_strategy())
    data = parts.orig_data.copy()
    data.columns = [f"{col}_id" for col in data.columns]
    return ParseInvalidCompareComponents(parts.query, data, parts.sql_format)


# ---- Testing check_columns_in_data


@given(components=columns_in_data_strategy())
def test_check_columns_in_data(components: CheckColumnsComponents) -> None:
    """Test whether check_columns_in_data() returns the correct boolean result.

    Args:
    ----
        components: CheckColumnsComponents

    """
    assert (
        check_columns_in_data(components.columns, components.data)
        == components.correct
    )


# ---- Testing insert query parsing


@given(components=parse_insert_query_strategy())
def test_parse_insert_query(components: ParseInsertComponents) -> None:
    """Test whether parse_insert_query() returns the correct output.

    Args:
    ----
        components: ParseInsertComponents

    """
    assert (
        parse_insert_query(
            components.query, components.data, components.sql_format
        )
        == components.parts
    )


@given(components=parse_invalid_insert_query_strategy())
def test_parse_insert_query_raises(
    components: ParseInvalidInsertComponents,
) -> None:
    """Test whether parse_insert_query() correctly raises exceptions.

    Args:
    ----
        components: ParseInvalidInsertComponents

    """
    with pytest.raises(InvalidInsertQueryError):
        parse_insert_query(
            components.query, components.data, components.sql_format
        )


# ---- Testing retrieve query parsing


@given(components=parse_retrieve_query_strategy())
def test_parse_retrieve_query(components: ParseRetrieveComponents) -> None:
    """Test whether parse_retrieve_query() returns the correct output.

    Args:
    ----
        components: ParseRetrieveComponents

    """
    assert (
        parse_retrieve_query(
            components.query, components.data, components.sql_format
        )
        == components.parts
    )


@given(components=parse_invalid_retrieve_query_strategy())
def test_parse_retrieve_query_raises(
    components: ParseInvalidRetrieveComponents,
) -> None:
    """Test whether parse_retrieve_query() correctly raises exceptions.

    Args:
    ----
        components: ParseInvalidRetrieveComponents

    """
    with pytest.raises(InvalidRetrieveQueryError):
        parse_retrieve_query(
            components.query, components.data, components.sql_format
        )


#  ---- Testing insert_and_retrieve ----


@given(components=parse_insert_and_retrieve_query_strategy())
def test_parse_insert_and_retrieve_query(
    components: ParseInsertAndRetrieveComponents,
) -> None:
    """Test whether parse_insert_and_retrieve_query() returns the correct output.

    Args:
    ----
        components: ParseInsertAndRetrieveComponents

    """
    assert (
        parse_insert_and_retrieve_query(
            components.insert_query,
            components.retrieve_query,
            components.data,
            components.sql_format,
        )
        == components.parts
    )


@given(components=parse_invalid_insert_and_retrieve_query_strategy())
def test_parse_insert_and_retrieve_query_raises(
    components: ParseInvalidInsertAndRetrieveComponents,
) -> None:
    """Test whether parse_insert_and_retrieve_query() correctly raises exceptions.

    Args:
    ----
        components: ParseInvalidInsertAndRetrieveComponents

    """
    with pytest.raises(InvalidInsertAndRetrieveQueryError):
        parse_insert_and_retrieve_query(
            components.insert_query,
            components.retrieve_query,
            components.data,
            components.sql_format,
        )


# ---- Testing compare
@given(components=parse_compare_query_strategy())
def test_parse_compare_query(components: ParseCompareComponents) -> None:
    """Test whether parse_compare_query() correctly passes.

    Args:
    ----
        components: ParseCompareComponents

    """
    assert (
        parse_compare_query(
            components.query, components.orig_data, components.sql_format
        )
        is None
    )


@given(components=parse_invalid_compare_query_strategy())
def test_parse_compare_query_raises_invalid(
    components: ParseCompareComponents,
) -> None:
    """Test whether parse_compare_query() correctly raises exception on invalid query.

    Args:
    ----
        components: ParseCompareComponents

    """
    with pytest.raises(InvalidCompareQueryError):
        parse_compare_query(
            components.query, components.orig_data, components.sql_format
        )


@given(components=parse_invalid_compare_dataset_strategy())
def test_parse_compare_query_raises_wrong_dataset(
    components: ParseCompareComponents,
) -> None:
    """Test whether parse_compare_query() correctly raises exception on invalid dataset.

    Args:
    ----
        components: ParseCompareComponents

    """
    with pytest.raises(WrongDatasetPassedError):
        parse_compare_query(
            components.query, components.orig_data, components.sql_format
        )
