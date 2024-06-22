from typing import Any, Callable

DataframeToDict = Callable[[Any], list[dict]]


def pandas_to_dict(df) -> list[dict]:  # noqa: ANN001
    """Convert a pandas dataframe to a list of dicts.

    Args:
    ----
        df: pandas dataframe

    Returns:
    -------
        list of dicts

    """
    return df.to_dict(orient="records")


def polars_to_dict(df) -> list[dict]:  # noqa: ANN001
    """Convert a polars dataframe to a list of dicts.

    Args:
    ----
        df: polars dataframe

    Returns:
    -------
        list of dicts

    """
    return df.to_dicts()
