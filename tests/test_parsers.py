import string
from dataclasses import dataclass

import hypothesis.strategies as st
import pytest
from hypothesis import given
from hypothesis.strategies import DrawFn, composite

from etl_components.parsers import QueryInputError, parse_input
from etl_components.schema import Column, Schema, Table

# ---- manual tests since hypothesis keeps taking forever to show results

mock_schema = Schema(
    [
        Table(
            "fiets",
            [
                Column("merk", "VARCHAR"),
                Column("prijs", "INT"),
                Column("kleur", "ENUM"),
            ],
        ),
        Table(
            "auto",
            [
                Column("merk", "VARCHAR"),
                Column("prijs", "INT"),
                Column("brandstof", "CHAR"),
            ],
        ),
    ]
)


def test_parse_input() -> None:
    """Test whether parse_input gives the correct results."""
    table = "fiets"
    columns = ["merk", "prijs"]
    assert parse_input(table, columns, mock_schema) == columns


def test_parse_input_fail_table() -> None:
    """Test whether parse_input throws an exception if table does not appear in schema."""
    table = "trein"
    columns = ["merk", "prijs"]
    with pytest.raises(QueryInputError):
        parse_input(table, columns, mock_schema)


def test_parse_input_fail_columns() -> None:
    """Test whether parse_input throws an exception if columns do not appear in table schema."""
    table = "fiets"
    columns = ["boot", "trein"]
    with pytest.raises(QueryInputError):
        parse_input(table, columns, mock_schema)


# ---- Grondigere test met hypothesis


@composite
def name_generator(draw: DrawFn) -> str:
    """Generate a name.

    Args:
    ----
        draw: DrawFn

    Returns:
    -------
        name

    """
    return draw(
        st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=10)
    )


@composite
def names_generator(draw: DrawFn, size: int) -> list[str]:
    """Generate a list of unique names.

    Args:
    ----
        draw: DrawFn
        size: number of names in list

    Returns:
    -------
       list of names of length [size]

    """
    return draw(
        st.lists(name_generator(), min_size=size, max_size=size, unique=True)
    )


@composite
def samples(draw: DrawFn, items: list, size: int | None = None) -> list:
    """Generate a list of samples from a list of items.

    Args:
    ----
        draw: DrawFn
        items: to draw from
        size: (Optional) number of items to draw. If None, draws a random number of items.

    Returns:
    -------
       list of unique samples from items

    """
    if size is not None and size > len(items):
        message = "Cannot have more samples than items."
        raise ValueError(message)

    min_size = len(items) // 2 if size is None else size
    max_size = len(items) if size is None else size

    return draw(
        st.lists(
            st.sampled_from(items),
            min_size=min_size,
            max_size=max_size,
            unique=True,
        )
    )


@dataclass
class InsertQueryComponents:
    """Encapsulates output from parse_insert_strategy()."""

    table: str
    columns: list[str]
    schema: Schema
    common_columns: list[str]
    fail_table: bool
    fail_columns: bool


@composite
def parse_insert_strategy(
    draw: DrawFn,
) -> InsertQueryComponents:
    """Generate data from strategy for parse_insert().

    Args:
    ----
        draw: DrawFn

    Returns:
    -------
        InsertQueryComponents

    """
    # generating failure cases
    fail_table = draw(st.booleans())
    fail_columns = draw(st.booleans())

    # generating candidate values
    table_candidates = draw(names_generator(6))
    column_candidates = draw(names_generator(12))

    # generating schema and correct table

    schema = Schema(
        [
            Table(
                table,
                [
                    Column(name, "EMPTY")
                    for name in draw(samples(column_candidates[:6]))
                ],
            )
            for table in table_candidates[:5]
        ]
    )
    # drawing table from the first 5 table candidates
    table = draw(st.sampled_from(table_candidates[:5]))

    # sampling column names
    # deliberately sampling from excluded set of column candidates when fail_columns
    columns_from = (
        column_candidates[6:] if fail_columns else schema(table).column_names
    )
    columns = draw(samples(columns_from))

    common_columns = list(set(columns) & set(schema(table).column_names))

    # setting the sixth table name when fail_table
    if fail_table:
        table = table_candidates[5]

    return InsertQueryComponents(
        table,
        columns,
        schema,
        common_columns,
        fail_table,
        fail_columns,
    )


@given(components=parse_insert_strategy())
def test_parse_insert(components: InsertQueryComponents) -> None:
    """Test whether correct input passes properly through parse_insert().

    Args:
    ----
        components: InsertQueryComponents

    """
    # testing failure cases
    if components.fail_table or components.fail_columns:
        with pytest.raises(QueryInputError):
            parse_input(components.table, components.columns, components.schema)
    else:
        # testing regular case
        assert (
            parse_input(components.table, components.columns, components.schema)
            == components.common_columns
        )
