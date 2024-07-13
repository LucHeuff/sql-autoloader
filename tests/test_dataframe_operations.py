import polars as pl
import pytest
from polars.testing import assert_frame_equal

from etl_components.dataframe_operations import MissingIDsError, merge_ids


def test_merge_ids() -> None:
    """Test basic merging of ids."""
    data = pl.DataFrame({"a": ["A", "B", "C"]})
    out_data = pl.DataFrame({"a_id": [1, 2, 3], "a": ["A", "B", "C"]})
    db_fetch = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": 3, "a": "C"},
    ]

    assert_frame_equal(
        out_data, merge_ids(data, db_fetch), check_column_order=False
    )


def test_merge_ids_duplicate_assertion() -> None:
    """Test if merge_ids() raises AssertionError when duplicates arise in the data."""
    data = pl.DataFrame({"a": ["A", "B", "C"]})
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    with pytest.raises(AssertionError):
        merge_ids(data, db_fetch)


def test_merge_ids_allow_duplications() -> None:
    """Test if merge_ids() raises AssertionError when duplicates arise in the data."""
    data = pl.DataFrame({"a": ["A", "B", "C"]})
    out_data = pl.DataFrame(
        {"a_id": [1, 2, 3, 3], "a": ["A", "B", "C", "C"], "b": [1, 1, 1, 2]}
    )
    db_fetch = [
        {"a_id": 1, "a": "A", "b": 1},
        {"a_id": 2, "a": "B", "b": 1},
        {"a_id": 3, "a": "C", "b": 1},
        {"a_id": 3, "a": "C", "b": 2},
    ]
    assert_frame_equal(
        out_data,
        merge_ids(data, db_fetch, allow_duplication=True),
        check_column_order=False,
    )


def test_merge_ids_raises_exception() -> None:
    """Test whether merge_ids raises MissingIDsError when ids are missing after merge."""
    data = pl.DataFrame({"a": ["A", "B", "C"]})
    db_fetch = [
        {"a_id": 1, "a": "A"},
        {"a_id": 2, "a": "B"},
        {"a_id": None, "a": "C"},
    ]
    with pytest.raises(MissingIDsError):
        merge_ids(data, db_fetch)
