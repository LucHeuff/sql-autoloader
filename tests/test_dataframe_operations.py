from dataclasses import dataclass

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import assume, given
from more_itertools import batched
from polars.testing import assert_frame_equal
from polars.testing.parametric import dataframes, dtypes

from etl_components.dataframe_operations import (
    check_nulls,
    compare,
    get_rows,
    has_nulls,
    match_dtypes,
    merge_ids,
)
from etl_components.exceptions import (
    CompareMissingRowsError,
    CompareNoExactMatchError,
    InvalidDataframeError,
    MatchDatatypesError,
    MissingKeysAfterMergeError,
)
from tests.generators import name_generator, names_generator, subselection

# ---- Testing has_nulls() and check_nulls()


@dataclass
class HasNullsStrategy:
    """Container for output of has_nulls_strategy."""

    df: pl.DataFrame
    has_nulls: bool


@st.composite
def has_nulls_strategy(draw: st.DrawFn) -> HasNullsStrategy:
    """Strategy that generates dataframes that contain nulls or not."""
    has_nulls = draw(st.booleans())
    if not has_nulls:
        df = draw(dataframes(min_size=1, allow_null=False))
    else:
        # Forcing nulls since dataframes don't allow for that. Still generating random columns and dtypes
        columns = draw(names_generator(min_size=1, max_size=5))
        d_types = draw(
            st.lists(dtypes(), min_size=len(columns), max_size=len(columns))
        )
        df = pl.DataFrame({col: None for col in columns}).cast(
            dict(zip(columns, d_types))
        )

    return HasNullsStrategy(df, has_nulls)


def test_basic_has_nulls() -> None:
    """Basic test whether has_nulls() correctly detects the presence of null values."""
    no_nulls = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    nulls = pl.DataFrame({"a": [1, 2, None], "b": [4, None, 6]})
    all_nulls = pl.DataFrame({"a": [None, None, None], "b": [None, None, None]})

    assert not has_nulls(no_nulls)
    assert has_nulls(nulls)
    assert has_nulls(all_nulls)


@given(strategy=has_nulls_strategy())
def test_has_nulls(strategy: HasNullsStrategy) -> None:
    """Simulation test whether has_nulls() correctly detects tht presence of null values."""
    assert has_nulls(strategy.df) == strategy.has_nulls


def test_basic_check_nulls() -> None:
    """Basic test whether check_nulls() raises an exception when it should."""
    no_nulls = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    nulls = pl.DataFrame({"a": [1, 2, None], "b": [4, None, 6]})

    assert check_nulls(no_nulls) == None
    with pytest.raises(InvalidDataframeError):
        check_nulls(nulls)


@given(strategy=has_nulls_strategy())
def test_check_nulls(strategy: HasNullsStrategy) -> None:
    """Simulation test whether check_nulls() raises an exception when it should."""
    if strategy.has_nulls:
        with pytest.raises(InvalidDataframeError):
            check_nulls(strategy.df)
    else:
        assert check_nulls(strategy.df) == None


# ---- Testing compare()


@dataclass
class GeneratedRows:
    """Container for output of rows_generator."""

    rows: list[dict]
    columns: list[str]
    df: pl.DataFrame


@st.composite
def rows_generator(draw: st.DrawFn, min_rows: int = 1) -> GeneratedRows:
    """Generate a list of rows and the accompanying pl.DataFrame.

    Args:
    ----
        draw: hypothesis draw function
        min_rows: minimum number of rows

    Returns:
    -------
        GeneratedRows

    """
    columns = draw(names_generator(min_size=3, max_size=5))
    n = len(columns)
    n_rows = draw(st.integers(min_rows, 5))
    values = list(range(n * n_rows))
    rows = [dict(zip(columns, _values)) for _values in batched(values, n)]
    return GeneratedRows(rows, columns, pl.DataFrame(rows))


@dataclass
class CompareStrategy:
    """Container for output of compare_strategy."""

    rows: list[dict]
    df: pl.DataFrame
    exact: bool
    exact_error: bool
    missing_rows_error: bool


@st.composite
def compare_strategy(draw: st.DrawFn) -> CompareStrategy:
    """Strategy generating cases for data comparison."""
    gen_rows = draw(rows_generator(min_rows=3))
    exact = draw(st.booleans())
    exact_error = False if not exact else draw(st.booleans())
    missing_rows_error = False if exact else draw(st.booleans())

    if exact_error:
        return CompareStrategy(
            gen_rows.rows,
            gen_rows.df.slice(0, 2),
            exact,
            exact_error,
            missing_rows_error,
        )
    if missing_rows_error:
        return CompareStrategy(
            gen_rows.rows[0:2],
            gen_rows.df,
            exact,
            exact_error,
            missing_rows_error,
        )
    return CompareStrategy(
        gen_rows.rows, gen_rows.df, exact, exact_error, missing_rows_error
    )


def test_basic_compare() -> None:
    """Basic test of compare()."""
    rows = [
        {"a": 1, "b": 2, "c": 3},
        {"a": 2, "b": 3, "c": 4},
        {"a": 3, "b": 4, "c": 5},
    ]
    df = pl.DataFrame(rows)
    assert compare(df, rows) is None
    assert compare(df.slice(0, 2), rows, exact=False) is None

    with pytest.raises(CompareNoExactMatchError):
        compare(df.slice(0, 2), rows, exact=True)

    with pytest.raises(CompareMissingRowsError):
        compare(df, rows[0:2], exact=False)


@given(strategy=compare_strategy())
def test_compare(strategy: CompareStrategy) -> None:
    """Simulation test of compare()."""
    if strategy.exact_error:
        with pytest.raises(CompareNoExactMatchError):
            compare(strategy.df, strategy.rows, exact=strategy.exact)
    elif strategy.missing_rows_error:
        with pytest.raises(CompareMissingRowsError):
            compare(strategy.df, strategy.rows, exact=strategy.exact)
    else:
        assert compare(strategy.df, strategy.rows, exact=strategy.exact) is None


# ---- Testing get_rows()


@dataclass
class GetRowsStrategy:
    """Container for output of get_rows_strategy."""

    df: pl.DataFrame
    columns: list[str]
    sub_columns: list[str]
    rows: list[dict]
    sub_rows: list[dict]


@st.composite
def get_rows_strategy(draw: st.DrawFn) -> GetRowsStrategy:
    """Generate a df, its rows, its columns and a subselection of columns and the accompanying rows."""
    gen_rows = draw(rows_generator())

    sub_columns = gen_rows.columns[0:2]

    sub_rows = [
        {k: v for (k, v) in row.items() if k in sub_columns}
        for row in gen_rows.rows
    ]
    return GetRowsStrategy(
        gen_rows.df, gen_rows.columns, sub_columns, gen_rows.rows, sub_rows
    )


def test_basic_get_rows() -> None:
    """Basic test of get_rows()."""
    columns = ["a", "b", "c"]
    rows = {col: 1 for col in columns}
    df = pl.DataFrame(rows)
    assert get_rows(df, columns) == [rows]
    # testing if it also works when subselecting some data
    assert get_rows(df, ["a", "c"]) == [{"a": 1, "c": 1}]


@given(strategy=get_rows_strategy())
def test_get_rows(strategy: GetRowsStrategy) -> None:
    """Test of get_rows()."""
    # Note: I don't care about the order in which rows appear, as long as they are there.
    assert all(
        row in get_rows(strategy.df, strategy.columns) for row in strategy.rows
    )
    assert all(
        row in get_rows(strategy.df, strategy.sub_columns)
        for row in strategy.sub_rows
    )


# ---- Testing match_dtypes()
@dataclass
class MatchDTypesStrategy:
    """Container for match_dtypes_strategy output."""

    df: pl.DataFrame
    rows: list[dict]
    match_columns: list[str]
    match_error: bool


@st.composite
def match_dtypes_strategy(draw: st.DrawFn) -> MatchDTypesStrategy:
    """Generate a dataframe and rows to test match_dtypes()."""
    match_error = draw(st.booleans())
    df = draw(
        dataframes(min_size=1, allow_null=False, allowed_dtypes=[pl.Float32])
    )
    match_columns = draw(subselection(df.columns))
    if match_error:
        rows = [{col: col for col in match_columns}]
    else:
        rows = [{col: n for (n, col) in enumerate(match_columns)}]
    return MatchDTypesStrategy(df, rows, match_columns, match_error)


def test_basic_match_dtypes() -> None:
    """Basic test of match_dtypes()."""
    df = pl.DataFrame({"a": ["A"], "b": [1]})
    rows = [{"a": 1, "b": "1"}, {"a": 2, "b": "2"}, {"a": 3, "b": "3"}]
    matched_df = match_dtypes(df, rows)
    assert df.dtypes == matched_df.dtypes
    fail_rows = [{"b": "A"}]
    with pytest.raises(MatchDatatypesError):
        match_dtypes(df, fail_rows)


@given(strategy=match_dtypes_strategy())
def test_match_dtypes(strategy: MatchDTypesStrategy) -> None:
    """Simulation test of match_dtypes()."""
    if strategy.match_error:
        with pytest.raises(MatchDatatypesError):
            match_dtypes(strategy.df, strategy.rows)
    else:
        assert (
            match_dtypes(strategy.df, strategy.rows).dtypes
            == strategy.df.select(strategy.match_columns).dtypes
        )


# ---- Testing merge_ids()
@dataclass
class MergeIDsStrategy:
    """Container for merge_ids_strategy output."""

    df: pl.DataFrame
    merged_df: pl.DataFrame
    rows: list[dict]
    alias: str
    allow_duplication: bool
    missing_keys: bool
    duplication_error: bool


@st.composite
def merge_ids_strategy(draw: st.DrawFn) -> MergeIDsStrategy:
    """Strategy for test of merge_ids()."""
    missing_keys = draw(st.booleans())
    allow_duplication = draw(st.booleans())
    duplication_error = False if not allow_duplication else draw(st.booleans())
    alias = draw(name_generator())
    df = draw(
        dataframes(
            min_size=3,
            allow_null=False,
            # excluding int64 because polars seems to have a weird issue joining on
            # large numbers...
            excluded_dtypes=[
                pl.Struct,
                pl.Categorical,
                pl.Decimal,
                pl.Int64,
                pl.UInt64,
            ],
        )
    )

    # Making sure that only unique rows are generated
    assume(len(df) == len(df.unique()))

    merged_df = df.with_row_index().rename({"index": alias})
    rows = merged_df.to_dicts()

    if missing_keys:
        # I get missings if not all rows that are in df are fetched from db
        # So I need to remove some of the rows
        rows = merged_df.to_dicts()[: len(merged_df) - 1]

    if allow_duplication:
        rows = rows + rows
        merged_df = pl.concat([merged_df, merged_df])

    return MergeIDsStrategy(
        df,
        merged_df,
        rows,
        alias,
        allow_duplication,
        missing_keys,
        duplication_error,
    )


def test_basic_merge_ids() -> None:
    """Basic test of merge_ids()."""
    alias = "a_id"
    df = pl.DataFrame({"a": ["A", "B", "C"]})
    db_fetch = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": 3, "a": "C"},
    ]
    db_fetch_duplicates = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    db_fetch_missings = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": None, "a": "C"},
    ]
    out_df = pl.DataFrame({"a": ["A", "B", "C"], "a_id": [1, 2, 3]})

    out_df_duplicates = pl.DataFrame(
        {
            "a": ["A", "B", "C", "C"],
            "a_id": [1, 2, 3, 3],
            "b": [1, 1, 1, 2],
        }
    )

    out = merge_ids(df, db_fetch, alias)
    assert_frame_equal(
        out_df, out, check_column_order=False, check_row_order=False
    )

    # testing allow_duplication=False
    with pytest.raises(AssertionError):
        merge_ids(df, db_fetch_duplicates, alias)

    out_duplicates = merge_ids(
        df, db_fetch_duplicates, alias, allow_duplication=True
    )
    assert_frame_equal(
        out_df_duplicates,
        out_duplicates,
        check_column_order=False,
        check_row_order=False,
    )

    # testing missing ids
    with pytest.raises(MissingKeysAfterMergeError):
        merge_ids(df, db_fetch_missings, alias)


@given(strategy=merge_ids_strategy())
def test_merge_ids(strategy: MergeIDsStrategy) -> None:
    """Simulation test of merge_ids()."""
    # This test fails on a AssertionError: Rows were duplicated when joining on ids that I cannot reproduce manually.
    # So maybe flaky test, maybe time to make the test a lot stricter
    if strategy.missing_keys:
        with pytest.raises(MissingKeysAfterMergeError):
            merge_ids(
                strategy.df,
                strategy.rows,
                strategy.alias,
                allow_duplication=strategy.allow_duplication,
            )
    elif strategy.duplication_error:
        with pytest.raises(AssertionError):
            merge_ids(strategy.df, strategy.rows, strategy.alias)
    else:
        out = merge_ids(
            strategy.df,
            strategy.rows,
            strategy.alias,
            allow_duplication=strategy.allow_duplication,
        )
        assert_frame_equal(
            out,
            strategy.merged_df,
            check_column_order=False,
            check_row_order=False,
            check_dtypes=False,
        )
