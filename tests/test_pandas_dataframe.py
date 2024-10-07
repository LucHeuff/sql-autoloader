import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from etl_components.dataframe import get_dataframe
from etl_components.exceptions import (
    CompareMissingRowsError,
    CompareNoExactMatchError,
    MatchDatatypesError,
)
from etl_components.pandas_dataframe import (
    PandasDataFrame,
)


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
    pandas_df.rename({})
    assert pandas_df.columns == ["a", "b", "c"]
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


def test_match_dtypes() -> None:
    """Test PandasDataFrame.match_dtypes correctly transforms columns."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": ["A"], "b": [1]}))
    rows = [{"a": 1, "b": "1"}, {"a": 2, "b": "2"}, {"a": 3, "b": "3"}]
    matched_df = pandas_df.match_dtypes(rows)
    assert (pandas_df.data.dtypes == matched_df.dtypes).all()
    fail_rows = [{"b": "A"}]
    with pytest.raises(MatchDatatypesError):
        pandas_df.match_dtypes(fail_rows)


def test_merge_ids() -> None:
    """Test PandasDataFrame.merge_ids() correctly merges ids."""
    pandas_df = PandasDataFrame(
        pd.DataFrame({"a": ["A", "B", "C"], "b": [1 / 3, 2 / 3, 3 / 3]})
    )
    db_fetch = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": 3, "a": "C"},
    ]
    out_df = pd.DataFrame(
        {
            "a": ["A", "B", "C"],
            "a_id": [1, 2, 3],
            "b": [1 / 3, 2 / 3, 3 / 3],
        }
    )
    pandas_df.merge_ids(db_fetch)
    assert_frame_equal(
        pandas_df.data,
        out_df,
        check_like=True,
    )


def test_merge_ids_allow_dupdicate() -> None:
    """Test PandasDataFrame.merge_ids() correctly handles dublication when merging ids."""
    pandas_df = PandasDataFrame(pd.DataFrame({"a": ["A", "B", "C"]}))
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    out_df = pd.DataFrame(
        {
            "a": ["A", "B", "C", "C"],
            "a_id": [1, 2, 3, 3],
            "b": [1, 1, 1, 2],
        }
    )
    # NOTE: some inplace stuff does happen on pandas_df, so test for failure first!
    with pytest.raises(AssertionError):
        pandas_df.merge_ids(db_fetch)
    pandas_df.merge_ids(db_fetch, allow_duplication=True)
    assert_frame_equal(
        pandas_df.data,
        out_df,
        check_like=True,
    )


def test_pass_dataframe() -> None:
    """Test if get_dataframe returns the PandasDataFrame when given a PandasDataFrame."""
    polars_df = PandasDataFrame(pd.DataFrame())
    assert get_dataframe(polars_df) == polars_df


def test_compare() -> None:
    """Test if compare() functions as it is supposed to."""
    data_rows = [
        {"a": 1, "b": 2},
        {"a": 2, "b": 3},
        {"a": 3, "b": 4},
    ]
    fail_types = [
        {"a": "3", "b": "4"},
        {"a": "2", "b": "3"},
        {"a": "1", "b": "2"},
    ]
    fail_db_missing = [
        {"a": 2, "b": 3},
        {"a": 1, "b": 2},
    ]
    fail_data_missing = [
        {"a": 1, "b": 2},
        {"a": 4, "b": 5},
        {"a": 3, "b": 4},
        {"a": 2, "b": 3},
    ]
    pandas_df = PandasDataFrame(pd.DataFrame(data_rows))

    assert pandas_df.compare(data_rows) is None
    assert pandas_df.compare(data_rows, exact=True) is None

    # testing exact=True path
    with pytest.raises(CompareNoExactMatchError):
        pandas_df.compare(fail_data_missing, exact=True)
    with pytest.raises(CompareNoExactMatchError):
        pandas_df.compare(fail_db_missing, exact=True)
    with pytest.raises(CompareNoExactMatchError):
        pandas_df.compare(fail_types, exact=True)

    # testing exact=False path
    assert pandas_df.compare(fail_data_missing, exact=False) is None
    with pytest.raises(CompareMissingRowsError):
        pandas_df.compare(fail_db_missing, exact=False)
