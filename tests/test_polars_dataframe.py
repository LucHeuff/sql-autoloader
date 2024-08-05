import polars as pl
import pytest
from polars.testing import assert_frame_equal

from etl_components.dataframe import get_dataframe
from etl_components.polars_dataframe import PolarsDataFrame


def test_columns() -> None:
    """Test PolarsDataFrame.columns returns the correct columns."""
    polars_df = PolarsDataFrame(pl.DataFrame({"a": [1], "b": [1], "c": [1]}))
    assert polars_df.columns == ["a", "b", "c"]


def test_data() -> None:
    """Test PolarsDataFrame.data returns the original polars dataframe."""
    pl_df = pl.DataFrame({"a": [1], "b": [1], "c": [1]})
    polars_df = PolarsDataFrame(pl_df)
    assert_frame_equal(pl_df, polars_df.data)


def test_rename() -> None:
    """Test PolarsDataFrame.rename() correctly renames the data."""
    polars_df = PolarsDataFrame(pl.DataFrame({"a": [1], "b": [1], "c": [1]}))
    polars_df.rename({"a": "A", "b": "B", "c": "C"})
    assert polars_df.columns == ["A", "B", "C"]


def test_drop() -> None:
    """Test PolarsDataFrame.drop() correctly drops columns."""
    polars_df = PolarsDataFrame(pl.DataFrame({"a": [1], "b": [1], "c": [1]}))
    polars_df.drop(["b", "c"])
    assert polars_df.columns == ["a"]


def test_rows() -> None:
    """Test PolarsDataFrame.rows() correctly returns rows."""
    polars_df = PolarsDataFrame(pl.DataFrame({"a": [1], "b": [1], "c": [1]}))
    assert polars_df.rows() == [{"a": 1, "b": 1, "c": 1}]
    # testing if it also works when subselecting some data
    assert polars_df.rows(["a", "c"]) == [{"a": 1, "c": 1}]


def test_merge_ids() -> None:
    """Test PolarsDataFrame.merge_ids() correctly merges ids."""
    polars_df = PolarsDataFrame(
        pl.DataFrame({"a": ["A", "B", "C"], "b": [1 / 3, 2 / 3, 3 / 3]})
    )
    db_fetch = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": 3, "a": "C"},
    ]
    out_df = pl.DataFrame(
        {"a": ["A", "B", "C"], "a_id": [1, 2, 3], "b": [1 / 3, 2 / 3, 3 / 3]}
    )
    polars_df.merge_ids(db_fetch)
    assert_frame_equal(
        polars_df.data,
        out_df,
        check_column_order=False,
        check_row_order=False,
    )


def test_merge_ids_allow_duplicate() -> None:
    """Test PolarsDataFrame.merge_ids() correctly merges ids."""
    polars_df = PolarsDataFrame(
        pl.DataFrame({"a": ["A", "B", "C"], "c": [1 / 3, 2 / 3, 3 / 3]})
    )
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    out_df = pl.DataFrame(
        {
            "a": ["A", "B", "C", "C"],
            "a_id": [1, 2, 3, 3],
            "b": [1, 1, 1, 2],
            "c": [1 / 3, 2 / 3, 3 / 3, 3 / 3],
        }
    )
    polars_df.merge_ids(db_fetch, allow_duplication=True)
    assert_frame_equal(
        polars_df.data,
        out_df,
        check_column_order=False,
        check_row_order=False,
    )


def test_merge_ids_duplicate_assertion() -> None:
    """Test PolarsDataFrame.merge_ids() correctly merges ids."""
    polars_df = PolarsDataFrame(pl.DataFrame({"a": ["A", "B", "C"]}))
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    with pytest.raises(AssertionError):
        polars_df.merge_ids(db_fetch)


def test_pass_dataframe() -> None:
    """Test if get_dataframe returns the PolarsDataFrame when given a PolarsDataFrame."""
    polars_df = PolarsDataFrame(pl.DataFrame())
    assert get_dataframe(polars_df) == polars_df
