import polars as pl


class MissingIDsError(Exception):
    """Used when joining upon retrieving from the database results in missing ids."""

    pass


def merge_ids(
    data: pl.DataFrame, db_fetch: list[dict], *, allow_duplication: bool = False
) -> pl.DataFrame:
    """Merge data with ids from database with data, using polars.

    Args:
    ----
        data: DataFrame where ids are to be merged into
        db_fetch: output from cursor.fetchall()
        allow_duplication: (Optional) whether merging ids is allowed to duplicate rows.
                            Default: false

    Returns:
    -------
        pl.DataFrame merged with ids from database


    Raises:
    ------
        MissingIDsError: if as a result of merging any *_id columns now contain misings.

    """
    orig_len = len(data)
    db_data = pl.DataFrame(db_fetch)

    # taking the columns the two datasets have in common as join columns
    on_columns = list(set(data.columns) & set(db_data.columns))

    data = data.join(db_data, on=on_columns, how="left")

    # sanity check: dataset should not shrink (impossible?) and rows should not be duplicated
    assert not len(data) < orig_len, "Rows were lost when joining on ids."
    assert (
        not len(data) > orig_len or allow_duplication
    ), "Rows were duplicated when joining on ids."

    # checking if any of the id columns are now empty
    id_cols = pl.col("^.*_id$")
    if data.select(id_cols).null_count().sum_horizontal().item():
        rows_with_missings = data.filter(pl.any_horizontal(id_cols).is_null())
        message = "Some id's were returned as NA:\n"
        raise MissingIDsError(message + str(rows_with_missings))

    return data
