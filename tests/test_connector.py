from dataclasses import dataclass

import hypothesis.strategies as st
import polars as pl
from hypothesis import given
from polars.testing import assert_frame_equal
from polars.testing.parametric import dataframes

from sql_autoloader.connector import postprocess, preprocess
from tests.generators import names_generator


@dataclass
class ProcessingStrategy:
    """Container for processing_strategy output."""

    df: pl.DataFrame
    columns: dict[str, str] | None


@st.composite
def processing_strategy(draw: st.DrawFn) -> ProcessingStrategy:
    """Strategy for property based test of preprocess() and process()."""
    df = draw(dataframes(min_cols=3, min_size=1, allow_null=False))
    cols_none = draw(st.booleans())
    if cols_none:
        columns = None
    else:
        n_cols = len(df.columns)
        aliases = draw(names_generator(min_size=n_cols, max_size=n_cols))
        columns = dict(zip(df.columns, aliases, strict=True))
    return ProcessingStrategy(df, columns)


@given(strategy=processing_strategy())
def test_processing(strategy: ProcessingStrategy) -> None:
    """Test whether preprocessing followed by postprocessing restores column names."""  # noqa: E501
    pre = preprocess(strategy.df, strategy.columns)
    post = postprocess(pre, strategy.columns)
    assert strategy.df.columns == post.columns
    assert not post.is_duplicated().any()
