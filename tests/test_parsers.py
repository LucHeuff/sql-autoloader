import string
from dataclasses import dataclass

import hypothesis.strategies as st
import pytest
from hypothesis import given
from hypothesis.strategies import DrawFn, composite

from etl_components.parsers import InsertQueryError, parse_insert


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
        raise ValueError("Cannot have more samples than items.")

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
    columns: dict[str, str]
    schema: dict[str, list[str]]
    data_columns: list[str]


@composite
def parse_insert_strategy(
    draw: DrawFn,
    *,
    fail_table: bool = False,
    fail_columns: bool = False,
    fail_values: bool = False,
) -> InsertQueryComponents:
    """Generate data from strategy for parse_insert().

    Args:
    ----
        draw: DrawFn
        fail_table: if table should be incorrect
        fail_columns: if columns should be incorrect
        fail_values: if values should be incorrect

    Returns:
    -------
        InsertQueryComponents

    """
    # generating candidate values
    table_candidates = draw(names_generator(6))
    column_candidates = draw(names_generator(12))
    values_candidates = draw(names_generator(12))

    # generating schema and correct table
    schema = {
        table: draw(samples(column_candidates[:6]))
        for table in table_candidates[:5]
    }
    table = draw(st.sampled_from(table_candidates[:5]))
    num_names = len(schema[table])  # tracking number of columns for this table

    # sampling column names
    # deliberately sampling from excluded set of column candidates when fail_columns
    columns_from = column_candidates[6:] if fail_columns else schema[table]
    db_columns = draw(samples(columns_from, size=num_names))

    # sampling value names
    # deliberately sampling from excluded set of column candidates when fail_columns
    data_columns = draw(samples(values_candidates[:6], size=num_names))
    values_from = values_candidates[6:] if fail_values else data_columns
    df_values = draw(samples(values_from, size=num_names))

    # generating column dict
    columns = dict(zip(db_columns, df_values))

    # setting the remaining table name when fail_table
    if fail_table:
        table = table_candidates[5]

    return InsertQueryComponents(table, columns, schema, data_columns)


@given(components=parse_insert_strategy())
def test_parse_insert(components: InsertQueryComponents) -> None:
    """Test whether correct input passes properly through parse_insert().

    Args:
    ----
        components: InsertQueryComponents

    """
    assert parse_insert(
        components.table,
        components.columns,
        components.schema,
        components.data_columns,
    ) == (components.table, components.columns)


@given(components=parse_insert_strategy(fail_table=True))
def test_parse_insert_table_exception(
    components: InsertQueryComponents,
) -> None:
    """Test whether parse_insert_query correctly throws an exception with unavailable table.

    Args:
    ----
        components: InsertQueryComponents

    """
    with pytest.raises(InsertQueryError):
        parse_insert(
            components.table,
            components.columns,
            components.schema,
            components.data_columns,
        )


@given(components=parse_insert_strategy(fail_columns=True))
def test_parse_insert_columns_exception(
    components: InsertQueryComponents,
) -> None:
    """Test whether parse_insert_query correctly throws an exception with incorrect columns.

    Args:
    ----
        components: InsertQueryComponents

    """
    with pytest.raises(InsertQueryError):
        parse_insert(
            components.table,
            components.columns,
            components.schema,
            components.data_columns,
        )


@given(components=parse_insert_strategy(fail_values=True))
def test_parse_insert_values_exception(
    components: InsertQueryComponents,
) -> None:
    """Test whether parse_insert_query correctly throws an exception with incorrect values.

    Args:
    ----
        components: InsertQueryComponents

    """
    with pytest.raises(InsertQueryError):
        parse_insert(
            components.table,
            components.columns,
            components.schema,
            components.data_columns,
        )
