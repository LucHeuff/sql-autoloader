import polars as pl
from polars.exceptions import ComputeError

from etl_components.dataframe import (
    CompareMissingDataRowsError,
    CompareNoExactMatchError,
    MatchDatatypesError,
    MissingIDsError,
)


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

    def match_dtypes(self, db_rows: list[dict]) -> pl.DataFrame:
        """Create a pl.DataFrame with matching dtypes with self.df from database rows.

        Args:
        ----
            db_rows: rows to be converted to matching dataframe

        Returns:
        -------
           pl.DataFrame from db_rows with matching dtypes

        """
        # only adding columns to the schema that appear in db_rows
        schema = {
            col: dtype
            for (col, dtype) in zip(self.df.columns, self.df.dtypes)
            if col in db_rows[0]
        }
        try:
            return pl.DataFrame(db_rows).cast(schema)  # type: ignore
        except ComputeError as c:
            message = f"Matching dtypes failed with the following error:\n{c}"
            raise MatchDatatypesError(message) from c

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
        db_data = self.match_dtypes(db_fetch)

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

    def compare(self, db_rows: list[dict], *, exact: bool = True) -> None:
        """Compare rows from the database to rows in the dataframe.

        Args:
        ----
            db_rows: output from cursor.fetchall() upon compare query
            exact: (Optional) whether all the rows in data must match all the
                    rows retrieve from the database. If False, only checks if
                    rows from data appear in rows from query.

        """
        data_rows = self.rows()
        rows_in_db = [row in db_rows for row in data_rows]
        rows_in_data = [row in data_rows for row in db_rows]

        # if the comparison is set to exact, the rows from the database must
        # also all appear in the data
        if exact and (not all(rows_in_db) or not all(rows_in_data)):
            db_data = self.match_dtypes(db_rows)
            data_difference = self.df.with_row_index().filter(
                ~pl.Series(rows_in_db)
            )
            db_difference = db_data.filter(~pl.Series(rows_in_data))
            message = f"Datasets do not match exactly.\nRows in data and not in db:\n{data_difference}\nRows in db and not in data:\n{db_difference}"
            raise CompareNoExactMatchError(message)

        # check if all rows in data appear in the database
        if not all(rows_in_db):
            # if the following statement passes, dtypes match
            db_data = self.match_dtypes(db_rows)
            # checking which rows do not appear in dataframe
            missing_rows = self.df.with_row_index().filter(
                ~pl.Series(rows_in_db)
            )
            message = f"Some rows from data were not found in the database:\n{missing_rows}"
            raise CompareMissingDataRowsError(message)

    @property
    def columns(self) -> list[str]:
        """Returns a list of column names."""
        return self.df.columns

    @property
    def data(self) -> pl.DataFrame:
        """Returns the data as the original dataframe type."""
        return self.df

    def __len__(self) -> int:
        """Return the length of the dataframe."""
        return len(self.df)
