import pandas as pd
import polars as pl

from etl_components.convertors import pandas_to_dict, polars_to_dict

data = [
    {"a": 1, "b": 4, "c": "A"},
    {"a": 2, "b": 5, "c": "V"},
    {"a": 3, "b": 6, "c": "B"},
    {"a": 4, "b": 7, "c": "Q"},
    {"a": 5, "b": 8, "c": "P"},
]


def test_pandas_to_dict() -> None:
    """Test whether pandas_to_dict() returns the expected result."""
    df = pd.DataFrame(data)
    assert pandas_to_dict(df) == data


def test_polars_to_dict() -> None:
    """Test whether polars_to_dict() returns the expected result."""
    df = pl.DataFrame(data)
    assert polars_to_dict(df) == data
