import re
from dataclasses import dataclass
from typing import Protocol

import pandas as pd


class Cursor(Protocol):
    """Protocol for cursor used by interaction functions."""

    insert_format: str
    values_pattern: str
    retrieve_format: str

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
    section_pattern = r"^\s*INSERT INTO\s*\w+\s*\((.*)\)\s*VALUES"
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

    Returns:
    -------
        insert columns names

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
class ParsedInsertComponents:
    """Contains output values of parse_insert_query()."""

    table: str
    columns: list[str]
    values: list[str]


def parse_insert_query(
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
) -> ParsedInsertComponents:
    """Perform linter checks on insert query and data and return table name, column names and value column names.

    Args:
    ----
        cursor: Cursor that performs interactions with the database
        query: insert query to be parsed
        data: to be inserted into the database.

    Returns:
    -------
        ParsedInsertComponents

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
    return ParsedInsertComponents(table, columns, values)


def check_retrieve_query(query: str, correct_format: str) -> None:
    """Check if retrieve query conforms to format.

    Args:
    ----
        query: to be checked.
        correct_format: what the correct format should be.

    Raises:
    ------
        InvalidRetrieveQueryError: when query does not conform to format.

    """
    pattern = r"\s*SELECT\s*id as .*\s*FROM\s*\w+$"
    if not re.fullmatch(pattern, query):
        message = f"Invalid retrieve query format. Correct format is:\n{correct_format}"
        raise InvalidRetrieveQueryError(message)


def get_table_from_retrieve(query: str, correct_format: str) -> str:
    """Extract table name from retrieve query.

    Args:
    ----
        query: from which table must be extracted.
        correct_format: what the correct format should be.

    Returns:
    -------
        table name

    Raises:
    ------
        InvalidRetrieveQueryError: if table cannot be found.

    """
    # fancy named regex that immediately checks if table matches in id and FROM
    pattern = r"^\s*SELECT\s*id as (?P<table>\w+)_id.*\s*FROM\s*(?P=table)\s*$"
    result = re.match(pattern, query)
    if result is None:
        message = f"Invalid retrieve query, could not find <table>. Correct format is\n{correct_format}"
        raise InvalidRetrieveQueryError(message)
    return result.group(1)


def get_columns_from_retrieve(query: str, correct_format: str) -> list[str]:
    """Extract columns from retrieve query.

    Args:
    ----
        query: from which table must be extracted.
        correct_format: what the correct format should be.

    Returns:
    -------
        column names

    Raises:
    ------
        InvalidRetrieveQueryError: if no columns could be found.

    """
    message = f"Invalid retrieve query, could not find columns. Correct format is:\n{correct_format}"
    section_pattern = r"^\s*SELECT\s*id as \w+_id, (.*)\s*FROM"
    columns_section = re.match(section_pattern, query)
    if columns_section is None:
        raise InvalidRetrieveQueryError(message)
    columns = [
        col_parts.strip().split(" ")[-1]
        for col_parts in columns_section.group(1).split(", ")
    ]
    if not columns:
        raise InvalidRetrieveQueryError(message)
    return columns


@dataclass
class ParsedRetrieveComponents:
    """Encapsulates parse_retrieve_query() output."""

    table: str
    columns: list[str]


def parse_retrieve_query(
    cursor: Cursor, query: str, data: pd.DataFrame
) -> ParsedRetrieveComponents:
    """Perform linter checks on retrieve query and return table name and column names.

    Args:
    ----
        cursor: Cursor that performs interactions with the database
        query: insert query to be parsed
        data: to be inserted into the database.

    Returns:
    -------
        ParsedRetrieveComponents


    Raises:
    ------
        InvalidRetrieveQueryError: when columns do not appear in data

    """
    correct_format = cursor.retrieve_format
    check_retrieve_query(query, correct_format)
    table = get_table_from_retrieve(query, correct_format)
    columns = get_columns_from_retrieve(query, correct_format)

    # excluding the first since it is the <table>_id columns that is going to be added
    if not all(column in data.columns for column in columns):
        message = f"""Columns in retrieve query do not match columns in data:
        Columns are:
            {columns}
        but available columns in data are:
            {data.columns.tolist()}

        """
        raise InvalidRetrieveQueryError(message)
    return ParsedRetrieveComponents(table, columns)


# TODO parse_insert_and_retrieve_query maken

# TODO compare_query parse dingen maken


def insert(cursor: Cursor, query: str, data: pd.DataFrame) -> None:
    """Insert data into database.

    Args:
    ----
        cursor: cursor that performs interactions with the database.
        query: insert query of the following format:
            INSERT INTO <table> (<column1>, <column2>, ...)
            VALUES (...) # depending on sqlite or psycopg connection
            ...
        data: to be inserted into the database

    """
    columns = parse_insert_query(cursor, query, data)

    data = data[columns].drop_duplicates()  # type: ignore
    cursor.executemany(query, data.to_dict("records"))


def retrieve_ids(
    cursor: Cursor, query: str, data: pd.DataFrame, *, replace: bool = True
) -> pd.DataFrame:
    """Retrieve ids from database.

    Args:
    ----
        cursor: cursor that performs interactoins with the database.
        query: retrieve query of the following format:
            SELECT id as <table>_id, <column1>, <column2> as <alias>, ... FROM <table>
        data: to which ids are to be merged
        replace: whether original columns without _id suffix are to be removed.

    Returns:
    -------
        data to which the id columns are merged.


    """
    query_components = parse_retrieve_query(cursor, query, data)
    orig_len = len(data)

    cursor.execute(query)
    ids_data = pd.DataFrame(cursor.fetchall())

    data = data.merge(ids_data, how="left", on=query_components.columns)
    assert not len(data) < orig_len, "Rows were lost when merging on ids."
    assert not len(data) > orig_len, "Rows were duplicated when merging on ids."

    if replace:
        non_id_columns = [
            col for col in query_components.columns if "_id" not in col
        ]
        data = data.drop(columns=non_id_columns)

    return data


def insert_and_retrieve_ids(
    cursor: Cursor,
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
) -> pd.DataFrame:
    insert_table, insert_columns, insert_values = parse_insert_query(
        cursor, insert_query, data
    )


def compare(cursor: Cursor, query: str, data: pd.DataFrame) -> pd.DataFrame:
    pass
