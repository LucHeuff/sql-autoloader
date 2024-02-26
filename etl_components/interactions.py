import re
from dataclasses import dataclass
from typing import Protocol

import pandas as pd


class Cursor(Protocol):
    """Protocol for cursor used by interaction functions."""

    insert_format: str
    values_pattern: str

    def execute(self, query: str, data: dict | None = None) -> None:
        """Execute query.

        Args:
        ----
            query: SQL query as string
            data: data in dictionary format

        """
        ...

    def executemany(self, query: str, data: list[dict]) -> None:
        """Executy query for multiple input rows.

        Args:
        ----
            query: SQL query as string
            data: data in list of dictionaries

        """
        ...

    def fetchall(self) -> list[dict]:
        """Fetch all results from cursor.

        Returns
        -------
            list of dictionaries containing data fetched from cursor.

        """
        ...


class InvalidInsertQueryError(Exception):
    """Exception for invalid insert query."""

    pass


class InvalidRetrieveQueryError(Exception):
    """Exception for invalid retrieve query."""

    pass


class InvalidCompareQueryError(Exception):
    """Exception for invalid compare query."""

    pass


def check_insert_query(query: str, correct_format: str) -> None:
    """Check if input query conforms to format.

    Args:
    ----
        query: to be checked.
        correct_format: what the correct format should be.

    Raises:
    ------
        InvalidInsertQueryError when query does not conformt to format.

    """
    pattern = r"\s*INSERT INTO\s*\w+\s*\(.*\)\s*VALUES\s*\(.*\)"
    if not re.match(pattern, query):
        message = (
            f"Invalid insert query format. Correct format is:\n{correct_format}"
        )
        raise InvalidInsertQueryError(message)


def get_table_from_insert(query: str, correct_format: str) -> str:
    """Read table name from query.

    Args:
    ----
        query: to be parsed.
        correct_format: what the correct format should be.

    Returns:
    -------
        table name.

    Raises:
    ------
        InvalidInsertQueryError: if table name could not be found.

    """
    pattern = r"\s*INSERT INTO\s*(\w+)"
    result = re.match(pattern, query)
    if result is None:
        message = f"Invalid insert query, could not find <table>. Correct format is:\n{correct_format}"
        raise InvalidInsertQueryError(message)
    return result.group(1)


def get_columns_from_insert(query: str, correct_format: str) -> list[str]:
    """Read column names from insert query.

    Args:
    ----
        query: to be parsed.
        correct_format: what the correct format should be.

    Returns:
    -------
        list of column names.


    Raises:
    ------
        InvalidInsertQueryError: if no columns can be found.

    """
    message = f"Invalid insert query, could not find columns. Correct format is:\n{correct_format}"
    section_pattern = r"\s*INSERT INTO\s*\w+\s*\((.*)\)\s*VALUES"
    columns_section = re.match(section_pattern, query)
    if columns_section is None:
        raise InvalidInsertQueryError(message)
    columns_pattern = r"(\w+)"
    columns = re.findall(columns_pattern, columns_section.group(1))
    if not columns:
        raise InvalidInsertQueryError(message)
    return columns


def get_values_from_insert(
    query: str, pattern: str, correct_format: str
) -> list[str]:
    """Extract insert column names from insert query.

    Args:
    ----
        query: from which column names must be extracted.
        pattern: pattern with which to extract values.
        correct_format: what the correct format should be.

    Raises:
    ------
        InvalidInsertQueryError when no column names can be extracted.

    """
    columns = re.findall(pattern, query)
    if not columns:
        message = f"Columns provided using invalid format. Correct format is:\n{correct_format}"
        raise InvalidInsertQueryError(message)
    return columns


@dataclass
class ParsedQueryComponents:
    """Contains output values of parse_insert_query()."""

    table: str
    columns: list[str]
    values: list[str]


def parse_insert_query(
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
) -> ParsedQueryComponents:
    """Perform linter checks on query and data and return table name, column names and value column names.

    Args:
    ----
        cursor: Cursor that performs interactions with the database
        query: insert query to be parsed
        data: to be inserted into the database.

    Returns:
    -------
        InsertQueryComponents met table, columns, values

    Raises:
    ------
        InvalidInsertQueryError: when value columns do not appear in data

    """
    correct_format = cursor.insert_format
    pattern = cursor.values_pattern
    check_insert_query(query, correct_format)
    table = get_table_from_insert(query, correct_format)
    columns = get_columns_from_insert(query, correct_format)
    values = get_values_from_insert(query, pattern, correct_format)

    if not all(value in data.columns for value in values):
        message = f"""Value columns in insert query do not match columns in data:
        Values are:
            {values}
        but available columns are:
            {data.columns.tolist()}
        """
        raise InvalidInsertQueryError(message)
    return ParsedQueryComponents(table, columns, values)


# TODO functies om retrieve te parsen
# TODO parse_insert_and_retrieve_query maken

# TODO compare_query parse dingen maken


def insert(cursor: Cursor, query: str, data: pd.DataFrame) -> None:
    """Insert data into database.

    Args:
    ----
        cursor: cursor that performs interactions with the database.
        query: insert query of the following format:
            INSERT INTO <table> (column1, column2, ...)
            VALUES (...) # depending on sqlite or psycopg connection
            ...
        data: to be inserted into the database

    """
    columns = parse_insert_query(cursor, query, data)

    data = data[columns].drop_duplicates()
    cursor.executemany(query, data.to_dict("records"))


def retrieve_ids(
    cursor: Cursor, query: str, data: pd.DataFrame
) -> pd.DataFrame:
    columns = parse_retrieve_query(cursor, query, data)
    orig_len = len(data)

    cursor.execute(query)
    ids_data = pd.DataFrame(cursor.fetchall())

    data = data.merge(ids_data, how="left", on=columns)
    assert not len(data) < orig_len, "Rows were lost when merging on ids."
    assert not len(data) > orig_len, "Rows were duplicated when merging on ids."

    return data


def insert_and_retrieve_ids(
    cursor: Cursor,
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
) -> pd.DataFrame:
    insert_table, insert_columns, insert_values = parse_insert_query(
        cursor, insert_query, data, all_components=True
    )


def compare(cursor: Cursor, query: str, data: pd.DataFrame) -> pd.DataFrame:
    pass
