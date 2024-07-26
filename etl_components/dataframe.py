from typing import Any, Protocol


class UnknownDataframeTypeError(Exception):
    """Raised when an unknown dataframe type is passed to get_dataframe()."""


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

    def rows(self, columns: list[str]) -> list[dict]:
        """Return unique rows from the data, for the selected columns.

        Args:
        ----
            columns: list of columns to restrict the data to.

        """
        ...

    def merge_ids(
        self, db_fetch: list[dict], *, allow_duplication: bool = False
    ) -> Any:  # noqa: ANN401
        """Merge data with ids from the database.

        Args:
        ----
            db_fetch: output from cursor.fetchall()
            allow_duplication: (Optional) whether merging ids is allowed to duplicate rows.

        Raises:
        ------
            MissingIDsError: if as a rsult of merging any id columns now contain missings.

        """

    @property
    def columns(self) -> list[str]:
        """Returns a list of column names."""
        ...

    @property
    def data(self) -> Any:  # noqa: ANN401
        """Returns the data as the original dataframe type."""
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
    match str(type(df)):
        case "<class 'polars.dataframe.frame.DataFrame'>":
            from etl_components.polars_dataframe import PolarsDataFrame

            # TODO figure out why PolarsDataFrame apparently isn't recognised as a DataFrame
            return PolarsDataFrame(df)
        case "<class 'pandas.core.frame.DataFrame'>":
            from etl_components.pandas_dataframe import PandasDataFrame

            return PandasDataFrame(df)
        case _:
            message = (
                "Expecting pandas or polars dataframe, but got {type(df)}."
            )
            raise UnknownDataframeTypeError(message)
