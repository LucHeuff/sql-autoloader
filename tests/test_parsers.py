import string
from dataclasses import dataclass

import hypothesis.strategies as st
import pytest
from hypothesis import given
from hypothesis.strategies import DrawFn, composite

from etl_components.parsers import InsertQueryError, parse_insert
from etl_components.schema import Column, Schema, Table

# TODO something goes wrong with a circular import somehow


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
    schema: Schema
    data_columns: list[str]
    fail_table: bool
    fail_columns: bool
    fail_values: bool


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
    fail_table, fail_columns, fail_values = draw(
        st.lists(st.booleans(), min_size=3, max_size=3)
    )

    # generating candidate values
    table_candidates = draw(names_generator(6))
    column_candidates = draw(names_generator(12))
    values_candidates = draw(names_generator(12))
    dtypes = draw(names_generator(6))

    # generating schema and correct table

    schema = Schema(
        [
            Table(
                table,
                [
                    Column(name, dtype)
                    for (name, dtype) in zip(
                        draw(samples(column_candidates[:6])),
                        draw(samples(dtypes, size=6)),
                    )
                ],
            )
            for table in table_candidates[:5]
        ]
    )

    # schema = {
    #     table: draw(samples(column_candidates[:6]))
    #     for table in table_candidates[:5]
    # }
    table = draw(st.sampled_from(table_candidates[:5]))
    num_names = len(schema.table_names)

    # sampling column names
    # deliberately sampling from excluded set of column candidates when fail_columns
    columns_from = (
        column_candidates[6:] if fail_columns else schema(table).column_names
    )
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

    return InsertQueryComponents(
        table,
        columns,
        schema,
        data_columns,
        fail_table,
        fail_columns,
        fail_values,
    )


@given(components=parse_insert_strategy())
def test_parse_insert(components: InsertQueryComponents) -> None:
    """Test whether correct input passes properly through parse_insert().

    Args:
    ----
        components: InsertQueryComponents

    """
    # testing failure cases
    if (
        components.fail_table
        or components.fail_columns
        or components.fail_values
    ):
        with pytest.raises(InsertQueryError):
            parse_insert(
                components.table,
                components.columns,
                components.schema,
                components.data_columns,
            )
    else:
        # testing regular case
        assert (
            parse_insert(
                components.table,
                components.columns,
                components.schema,
                components.data_columns,
            )
            is None
        )
