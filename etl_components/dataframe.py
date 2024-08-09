from typing import Any, Protocol, runtime_checkable


class UnknownDataframeTypeError(Exception):
    """Raised when an unknown dataframe type is passed to get_dataframe()."""


class MissingIDsError(Exception):
    """Raised when merging data from the db results in missing values in id columns."""


class MatchDatatypesError(Exception):
    """Raised when matching datatypes fails."""


class CompareMissingDataRowsError(Exception):
    """Raised during comparison when rows from data are not in the database."""


class CompareNoExactMatchError(Exception):
    """Raised during comparison when rows from data and database are not an exact match."""


@runtime_checkable
class DataFrame(Protocol):
    """Wrapper class for dataframe-like objects."""

    def rename(self, mapping: dict[str, str]) -> None:
        """Rename columns.

        Args:
        ----
            mapping: dictionary of {old_name: new_name}

        """

    def drop(self, columns: list[str]) -> None:
        """Drop columns from data.

        Args:
        ----
            columns: list of columns to be dropped

        """

    def rows(self, columns: list[str] | None = None) -> list[dict]:
        """Return unique rows from the data, for the selected columns.

        Args:
        ----
            columns: (Optional) list of columns to restrict the data to.
                     if None, return all columns.

        """
        ...

    def match_dtypes(self, db_rows: list[dict]) -> Any:  # noqa: ANN401
        """Create a DataFrame with matching dtypes with self.df.

        Args:
        ----
            db_rows: rows to be converted to matching dataframe

        Returns:
        -------
            dataframe with matching dtypes

        """

    def merge_ids(
        self, db_fetch: list[dict], *, allow_duplication: bool = False
    ) -> None:
        """Merge data with ids from the database.

        Args:
        ----
            db_fetch: output from cursor.fetchall()
            allow_duplication: (Optional) whether merging ids is allowed to duplicate rows.

        Raises:
        ------
            MissingIDsError: if as a result of merging any id columns now contain missings.

        """

    def compare(self, db_rows: list[dict], *, exact: bool = True) -> None:
        """Compare rows from the database to rows in the dataframe.

        Args:
        ----
            db_rows: output from cursor.fetchall() upon compare query
            exact: (Optional) whether all the rows in data must match all the
                    rows retrieve from the database. If False, only checks if
                    rows from data appear in rows from query.

        """

    @property
    def columns(self) -> list[str]:
        """Returns a list of column names."""
        ...

    @property
    def data(self) -> Any:  # noqa: ANN401
        """Returns the data as the original dataframe type."""
        ...

    def __len__(self) -> int:
        """Return the length of the dataframe."""
        ...


def get_dataframe(df) -> DataFrame:  # noqa: ANN001
    """Construct DataFrame wrapper around input dataframe.

    Args:
    ----
        df: input DataFrame, either `polars` or `pandas`

    Returns:
    -------
        DataFrame wrapper containing df

    """
    if isinstance(df, DataFrame):
        return df

    match str(type(df)):
        case "<class 'polars.dataframe.frame.DataFrame'>":
            from etl_components.polars_dataframe import PolarsDataFrame

            return PolarsDataFrame(df)
        case "<class 'pandas.core.frame.DataFrame'>":
            from etl_components.pandas_dataframe import PandasDataFrame

            return PandasDataFrame(df)

        case _:
            message = (
                "Expecting pandas or polars dataframe, but got {type(df)}."
            )
            raise UnknownDataframeTypeError(message)
