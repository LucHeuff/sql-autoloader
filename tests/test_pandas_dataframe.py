import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_components.pandas_dataframe import PandasDataFrame


def test_columns() -> None:
    """Test PandasDataFrame.columns returns the correct columns."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": [1], "b": [1], "c": [1]}))
    assert pandas_df.columns == ["a", "b", "c"]


def test_data() -> None:
    """Test PandasDataFrame.data returns the original pandas dataframe."""
    pd_df = pd.DataFrame({"a": [1], "b": [1], "c": [1]})
    pandas_df = PandasDataFrame(pd_df)
    assert_frame_equal(pd_df, pandas_df.data)


def test_rename() -> None:
    """Test PandasDataFrame.rename() correctly renames the data."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": [1], "b": [1], "c": [1]}))
    pandas_df.rename({"a": "A", "b": "B", "c": "C"})
    assert pandas_df.columns == ["A", "B", "C"]


def test_drop() -> None:
    """Test PandasDataFrame.drop() correctly drops columns."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": [1], "b": [1], "c": [1]}))
    pandas_df.drop(["b", "c"])
    assert pandas_df.columns == ["a"]


def test_rows() -> None:
    """Test PandasDataFrame.rows() correctly returns rows."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": [1], "b": [1], "c": [1]}))
    assert pandas_df.rows() == [{"a": 1, "b": 1, "c": 1}]
    # testing if it also works when subselecting some data
    assert pandas_df.rows(["a", "c"]) == [{"a": 1, "c": 1}]


def test_merge_ids() -> None:
    """Test PandasDataFrame.merge_ids() correctly merges ids."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": ["A", "B", "C"]}))
    db_fetch = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": 3, "a": "C"},
    ]
    merge = pandas_df.merge_ids(db_fetch)
    assert_frame_equal(
        merge,
        pd.DataFrame(db_fetch),
        check_like=True,
    )


def test_merge_ids_allow_dupdicate() -> None:
    """Test PandasDataFrame.merge_ids() correctly merges ids."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": ["A", "B", "C"]}))
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    merge = pandas_df.merge_ids(db_fetch, allow_duplication=True)
    assert_frame_equal(
        merge,
        pd.DataFrame(db_fetch),
        check_like=True,
    )


def test_merge_ids_dupdicate_assertion() -> None:
    """Test PandasDataFrame.merge_ids() correctly merges ids."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": ["A", "B", "C"]}))
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    with pytest.raises(AssertionError):
        pandas_df.merge_ids(db_fetch)
