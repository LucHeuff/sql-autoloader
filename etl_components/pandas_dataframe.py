import pandas as pd

from etl_components.dataframe import (
    CompareMissingDataRowsError,
    CompareNoExactMatchError,
    MatchDatatypesError,
    MissingIDsError,
)


class PandasDataFrame:
    """Wrapper for pandas dataframes."""

    def __init__(self, data: pd.DataFrame) -> None:
        """PandasDataframe constructor."""
        self.df = data.copy()

    def rename(self, mapping: dict[str, str]) -> None:
        """Rename columns.

        Args:
        ----
            mapping: dictionary of {old_name: new_name}

        """
        self.df = self.df.rename(columns=mapping)

    def drop(self, columns: list[str]) -> None:
        """Drop columns from data.

        Args:
        ----
            columns: list of columns to be dropped

        """
        self.df = self.df.drop(columns=columns, errors="ignore")

    def rows(self, columns: list[str] | None = None) -> list[dict]:
        """Return unique rows from the data, for the selected columns.

        Args:
        ----
            columns: (Optional) list of columns to restrict the data to.
                     if None, return all columns.

        """
        columns = self.columns if columns is None else columns
        return self.df.filter(items=columns).to_dict("records")

    def match_dtypes(self, db_rows: list[dict]) -> pd.DataFrame:
        """Create a pd.DataFrame with matching dtypes with self.df from database rows.

        Args:
        ----
            db_rows: rows to be converted to matching dataframe

        Returns:
        -------
           pd.DataFrame from db_rows with matching dtypes

        """
        # filtering out columns that don't appear in db_rows to prevent annoying errors.
        schema = self.df.dtypes.filter(items=db_rows[0].keys()).to_dict()
        try:
            return (
                pd.DataFrame(db_rows)
                .replace({float("nan"): None, "": None})
                .astype(schema)
            )
        except ValueError as v:
            message = f"Matching dtypes failed with the following error:\n{v}"
            raise MatchDatatypesError(message) from v

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

        Returns:
        -------
            pl.DataFrame merged with ids from database


        Raises:
        ------
            MissingIDsError: if as a result of merging any *_id columns now contain misings.

        """
        orig_len = len(self.df)
        db_data = self.match_dtypes(db_fetch)

        # taking the columns the two datasets have in common as join columns
        on_columns = list(set(self.columns) & set(db_data.columns))

        self.df = self.df.merge(db_data, on=on_columns, how="left")

        # sanity check: dataset should not shrink (impossible?) and rows should not be duplicated
        assert (
            not len(self.df) < orig_len
        ), "Rows were lost when joining on ids."
        assert (
            not len(self.df) > orig_len or allow_duplication
        ), "Rows were duplicated when joining on ids."

        # checking if any of the id columns are now empty
        missing_ids = self.df.filter(regex="_id$").isna()
        if bool(missing_ids.any(axis=None)):
            rows_with_missings = self.df[missing_ids.any(axis=1)]
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
                    Defaults to True.

        """
        data_rows = self.rows()
        rows_in_db = [row in db_rows for row in data_rows]
        rows_in_data = [row in data_rows for row in db_rows]

        # if the comparison is set to exact, the rows from the database must
        # also all appear in the data
        if exact and (not all(rows_in_db) or not all(rows_in_data)):
            db_data = self.match_dtypes(db_rows)
            data_difference = self.df[~pd.Series(rows_in_db)]
            db_difference = db_data[~pd.Series(rows_in_data)]
            message = f"Datasets do not match exactly.\nRows in data and not in db:\n{data_difference}\nRows in db and not in data:\n{db_difference}"
            raise CompareNoExactMatchError(message)

        # check if all rows in data appear in the database
        if not all(rows_in_db):
            # if the following statement passes, dtypes match
            db_data = self.match_dtypes(db_rows)
            # checking which rows do not appear in dataframe
            missing_rows = self.df[~pd.Series(rows_in_db)]
            message = f"Some rows from data were not found in the database:\n{missing_rows}"
            raise CompareMissingDataRowsError(message)

    @property
    def columns(self) -> list[str]:
        """Returns a list of column names."""
        return self.df.columns.tolist()

    @property
    def data(self) -> pd.DataFrame:
        """Returns the data as the original dataframe type."""
        return self.df

    def __len__(self) -> int:
        """Return the length of the dataframe."""
        return len(self.df)
