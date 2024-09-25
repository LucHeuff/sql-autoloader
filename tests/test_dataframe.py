import pandas as pd
import polars as pl
import pytest

from etl_components.dataframe import get_dataframe
from etl_components.exceptions import UnknownDataframeError
from etl_components.pandas_dataframe import PandasDataFrame
from etl_components.polars_dataframe import PolarsDataFrame


def test_get_polars_dataframe() -> None:
    """Test whether get_dataframe correctly returns PolarsDataFrame."""
    polars_df = pl.DataFrame()
    assert isinstance(get_dataframe(polars_df), PolarsDataFrame)


def test_get_pandas_dataframe() -> None:
    """Test whether get_dataframe correctly returns PandasDataFrame."""
    pandas_df = pd.DataFrame()
    assert isinstance(get_dataframe(pandas_df), PandasDataFrame)


def test_get_dataframe_exception() -> None:
    """Test whether get_dataframe returns an exception with an unknown data type."""
    with pytest.raises(UnknownDataframeError):
        get_dataframe(1)
