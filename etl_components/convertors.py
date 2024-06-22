from typing import Any, Callable

DataframeToDict = Callable[[Any], list[dict]]


def pandas_to_dict(df) -> list[dict]:
    return df.to_dict(orient="records")


def polars_to_dict(df) -> list[dict]:
    return df.to_dicts()
