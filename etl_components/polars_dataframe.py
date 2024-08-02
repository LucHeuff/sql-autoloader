import polars as pl


class MissingIDsError(Exception):
    """Raised when merging data from the db results in missing values in id columns."""


class PolarsDataFrame:
    """Wrapper for polars dataframes."""

    def __init__(self, data: pl.DataFrame) -> None:
        """PolarsDataFrame constructor."""
        self.df = data.clone()

    def rename(self, mapping: dict[str, str]) -> None:
        """Rename columns.

        Args:
        ----
            mapping: dictionary of {old_name: new_name}

        """
        self.df = self.df.rename(mapping)

    def drop(self, columns: list[str]) -> None:
        """Drop columns from data.

        Args:
        ----
            columns: list of columns to be dropped

        """
        self.df = self.df.drop(columns)

    def rows(self, columns: list[str] | None = None) -> list[dict]:
        """Return unique rows from the data, for the selected columns.

        Args:
        ----
            columns: (Optional) list of columns to restrict the data to.
                     if None, return all columns.

        """
        columns = self.columns if columns is None else columns
        return self.df.select(columns).drop_nulls().unique().to_dicts()

    def merge_ids(
        self,
        db_fetch: list[dict],
        *,
        allow_duplication: bool = False,
    ) -> None:
        """Merge data with ids from database with data, using polars.

        Args:
        ----
            data: DataFrame where ids are to be merged into
            db_fetch: output from cursor.fetchall()
            allow_duplication: (Optional) whether merging ids is allowed to duplicate rows.
                                Default: false

        Raises:
        ------
            MissingIDsError: if as a result of merging any *_id columns now contain misings.

        """
        orig_len = len(self.df)
        # determining data types in self.df to make sure I don't get errors with dtypes, only for columns that are in db_fetch
        schema = {
            col: dtype
            for (col, dtype) in zip(self.df.columns, self.df.dtypes)
            if col in db_fetch[0]
        }
        db_data = pl.DataFrame(db_fetch).cast(schema)  # type: ignore

        # taking the columns the two datasets have in common as join columns
        on_columns = list(set(self.columns) & set(db_data.columns))

        self.df = self.df.join(db_data, on=on_columns, how="left")

        # sanity check: dataset should not shrink (impossible?) and rows should not be duplicated
        assert (
            not len(self.df) < orig_len
        ), "Rows were lost when joining on ids."
        assert (
            not len(self.df) > orig_len or allow_duplication
        ), "Rows were duplicated when joining on ids."

        # checking if any of the id columns are now empty
        id_cols = pl.col("^.*_id$")
        if self.df.select(id_cols).null_count().sum_horizontal().item():
            rows_with_missings = self.df.filter(
                pl.any_horizontal(id_cols).is_null()
            )
            message = "Some id's were returned as NA:\n"
            raise MissingIDsError(message + str(rows_with_missings))

    @property
    def columns(self) -> list[str]:
        """Returns a list of column names."""
        return self.df.columns

    @property
    def data(self) -> pl.DataFrame:
        """Returns the data as the original dataframe type."""
        return self.df
