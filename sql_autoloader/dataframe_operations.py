import polars as pl
from polars.exceptions import InvalidOperationError

from sql_autoloader.exceptions import (
    CompareMissingRowsError,
    CompareNoExactMatchError,
    MatchDatatypesError,
    MissingKeysAfterMergeError,
)


def has_nulls(df: pl.DataFrame) -> bool:
    """Return whether dataframe contains nulls."""
    if len(df) == 0:
        return True
    return bool(df.null_count().sum_horizontal().item())


def compare(df: pl.DataFrame, db_rows: list[dict], *, exact: bool = True) -> None:
    """Compare rows from the database to rows in the dataframe.

    Args:
    ----
        df: pl.DataFrame of data to be compared to
        db_rows: output from cursor.fetchall() upon compare query
        exact: (Optional) whether all the rows in data must match all the
                rows retrieve from the database. If False, only checks if
                rows from data appear in rows from query.

    """
    data_rows = get_rows(df, df.columns)
    rows_in_db = [row in db_rows for row in data_rows]
    rows_in_data = [row in data_rows for row in db_rows]

    # if the comparison is set to exact, the rows from the database must
    # also all appear in the data
    if exact and (not all(rows_in_db) or not all(rows_in_data)):
        db_data = match_dtypes(df, db_rows)
        data_difference = df.with_row_index().filter(~pl.Series(rows_in_db))
        db_difference = db_data.filter(~pl.Series(rows_in_data))
        message = f"Datasets do not match exactly.\nRows in data and not in db:\n{data_difference}\nRows in db and not in data:\n{db_difference}"  # noqa: E501
        raise CompareNoExactMatchError(message)

    # check if all rows in data appear in the database
    if not all(rows_in_db):
        db_data = match_dtypes(df, db_rows)
        # checking which rows do not appear in dataframe
        missing_rows = df.with_row_index().filter(~pl.Series(rows_in_db))
        message = (
            f"Some rows from data were not found in the database:\n{missing_rows}"
        )
        raise CompareMissingRowsError(message)


def get_rows(df: pl.DataFrame, columns: list[str]) -> list[dict]:
    """Return unique rows from the data, for the selected columns.

    Args:
    ----
        df: pl.DataFrame to get rows from
        columns: list of columns to restrict the data to.


    Returns
    -------
        list of dictionaries limited to columns in `columns`

    """
    assert all(col in df.columns for col in columns), (
        "Not all columns appear in dataframe."
    )
    return df.select(columns).unique().to_dicts()


def match_dtypes(df: pl.DataFrame, db_rows: list[dict]) -> pl.DataFrame:
    """Create a pl.DataFrame with matching dtypes with self.df from database rows.

    Args:
    ----
        df: pl.DataFrame to match dtypes with
        db_rows: rows to be converted to matching dataframe

    Returns
    -------
       pl.DataFrame from db_rows with matching dtypes

    """
    # only adding columns to the schema that appear in db_rows
    schema = {
        col: dtype
        for (col, dtype) in zip(df.columns, df.dtypes, strict=False)
        if col in db_rows[0]
    }

    try:
        return pl.DataFrame(db_rows).cast(schema)  # pyright: ignore[reportArgumentType]
    except InvalidOperationError as e:
        message = f"Matching dtypes failed with the following error:\n{e}"
        raise MatchDatatypesError(message) from e


def merge_ids(
    df: pl.DataFrame,
    db_fetch: list[dict],
    alias: str,
    *,
    allow_duplication: bool = False,
) -> pl.DataFrame:
    """Merge data with ids from database with data, using polars.

    Args:
    ----
        df: pl.DataFrame where ids are to be merged into
        db_fetch: output from cursor.fetchall()
        alias: under which ids were fetched and which should not contain missings
        allow_duplication: (Optional) whether merging ids is allowed to duplicate rows.
                           Default: false

    Raises
    ------
        MissingKeysAfterMergeError: if as a result of merging any *_id columns now contain misings.

    """  # noqa: E501
    orig_len = len(df)
    db_data = match_dtypes(df, db_fetch)

    assert alias in db_data.columns, (
        "Provided alias not found in fetch from database."
    )

    # taking the columns the two datasets have in common as join columns
    on_columns = list(set(df.columns) & set(db_data.columns))

    df = df.join(db_data, on=on_columns, how="left", join_nulls=True)

    # sanity checks: dataset should not shrink and rows should not be duplicated
    assert len(df) >= orig_len, "Rows were lost when joining on ids."
    # NOTE: there is a strange case when using large integers
    # where the row is simply dropped?
    # This seems to a somewhat reproducible issue with polars though...
    assert len(df) == orig_len or allow_duplication, (
        "Rows were duplicated when joining on ids."
    )

    # Alias should not return empty
    if has_nulls(df.select(alias)):
        rows_with_missings = df.filter(pl.any_horizontal(alias).is_null())
        message = "Some id's were returned as NA:\n" + str(rows_with_missings)
        raise MissingKeysAfterMergeError(message)

    return df
