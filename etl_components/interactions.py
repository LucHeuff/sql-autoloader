import re

import pandas as pd

from etl_components.connections import Cursor, CursorFormat, get_cursor_formats


class InvalidInsertQueryError(Exception):
    """Exception for invalid insert query."""

    pass


class InvalidRetrieveQueryError(Exception):
    """Exception for invalid retrieve query."""

    pass


class InvalidInsertAndRetrieveQueryError(Exception):
    """Exception when insert and retrieve queries don't align."""

    pass


class InvalidCompareQueryError(Exception):
    """Exception for invalid compare query."""

    pass


class WrongDatasetPassedError(Exception):
    """Exception for when the wrong dataset is passed into compare()."""

    pass


# ---- Functions to check insert query


def check_insert_query(query: str, cursor_format: CursorFormat) -> None:
    """Check if input query conforms to format.

    Args:
    ----
        query: to be checked
        cursor_format: CursorFormat

    Raises:
    ------
        InvalidInsertQueryError: when query does not conformt to format

    """
    pattern = r"\s*INSERT INTO\s*\w+\s*\(.*\)\s*VALUES\s*\(.*\)"
    correct = cursor_format.insert_format
    if not re.match(pattern, query):
        message = f"Invalid insert query format. Correct format is:\n{correct}"
        raise InvalidInsertQueryError(message)


def get_table_from_insert(query: str, cursor_format: CursorFormat) -> str:
    """Read table name from query.

    Args:
    ----
        query: to be parsed.
        cursor_format: CursorFormat

    Returns:
    -------
        table name

    Raises:
    ------
        InvalidInsertQueryError: if table name could not be found

    """
    pattern = r"\s*INSERT INTO\s*(\w+)"
    correct = cursor_format.insert_format
    result = re.match(pattern, query)
    if result is None:
        message = f"Invalid insert query, could not find <table>. Correct format is:\n{correct}"
        raise InvalidInsertQueryError(message)
    return result.group(1)


def get_columns_from_insert(
    query: str, cursor_format: CursorFormat
) -> list[str]:
    """Read column names from insert query.

    Args:
    ----
        query: to be parsed
        cursor_format: CursorFormat

    Returns:
    -------
        list of column names


    Raises:
    ------
        InvalidInsertQueryError: if no columns can be found

    """
    correct = cursor_format.insert_format
    message = f"Invalid insert query, could not find columns. Correct format is:\n{correct}"
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
    query: str, cursor_format: CursorFormat
) -> list[str]:
    """Extract insert value names from insert query.

    Args:
    ----
        query: from which value names must be extracted
        cursor_format: CursorFormat

    Returns:
    -------
        insert values names

    Raises:
    ------
        InvalidInsertQueryError when no value names can be extracted

    """
    correct = cursor_format.insert_format
    pattern = cursor_format.values_pattern
    values = re.findall(pattern, query)
    if not values:
        message = f"Columns provided using invalid format. Correct format is:\n{correct}"
        raise InvalidInsertQueryError(message)
    return values


# ---- Functions to check retrieve query


def check_retrieve_query(query: str, cursor_format: CursorFormat) -> None:
    """Check if retrieve query conforms to format.

    Args:
    ----
        query: to be checked
        cursor_format: CursorFormat

    Raises:
    ------
        InvalidRetrieveQueryError: when query does not conform to format

    """
    correct = cursor_format.retrieve_format
    pattern = r"\s*SELECT\s*id as .*\s*FROM\s*\w+$"
    if not re.fullmatch(pattern, query):
        message = (
            f"Invalid retrieve query format. Correct format is:\n{correct}"
        )
        raise InvalidRetrieveQueryError(message)


def get_table_from_retrieve(query: str, cursor_format: CursorFormat) -> str:
    """Extract table name from retrieve query.

    Args:
    ----
        query: from which table must be extracted
        cursor_format: CursorFormat

    Returns:
    -------
        table name

    Raises:
    ------
        InvalidRetrieveQueryError: if table cannot be found

    """
    # fancy named regex that immediately checks if table matches in id and FROM
    correct = cursor_format.retrieve_format
    pattern = r"^\s*SELECT\s*id as (?P<table>\w+)_id.*\s*FROM\s*(?P=table)\s*$"
    result = re.fullmatch(pattern, query)
    if result is None:
        message = f"Invalid retrieve query, could not find <table>. Correct format is\n{correct}"
        raise InvalidRetrieveQueryError(message)
    return result.group(1)


def get_columns_from_retrieve(
    query: str, cursor_format: CursorFormat
) -> list[str]:
    """Extract columns from retrieve query.

    Args:
    ----
        query: from which table must be extracted
        cursor_format: CursorFormat

    Returns:
    -------
        column names

    Raises:
    ------
        InvalidRetrieveQueryError: if no columns could be found

    """
    correct = cursor_format.retrieve_format
    message = f"Invalid retrieve query, could not find columns. Correct format is:\n{correct}"
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


# ---- Functions to check compare query


def check_compare_query(query: str, cursor_format: CursorFormat) -> None:
    """Check if compare query conforms to format.

    Args:
    ----
        query: to be checked.
        cursor_format: CursorFormat

    Raises:
    ------
        InvalidCompareQueryError: when query does not conform to format

    """
    correct = cursor_format.compare_format
    pattern = r"^\s*SELECT\s*[\S\s]+FROM \w+\s*(?:JOIN \w+ ON \w+\.\w+_id = \w+\.id\s*)+"
    if not re.match(pattern, query):
        message = f"Invalid compare query format. Correct format is\n{correct}"
        raise InvalidCompareQueryError(message)


# ---- Common check functions


def check_columns_in_data(columns: list[str], data: pd.DataFrame) -> bool:
    """Check whether column names are among the columns in data.

    Args:
    ----
        columns: list of column names
        data: to compare columns to

    Returns:
    -------
        whether columns are in the data

    """
    return all(column in data.columns for column in columns)


# ---- Parse functions


def parse_insert_query(
    query: str,
    data: pd.DataFrame,
    cursor_formats: CursorFormat,
) -> tuple[list[str], list[str]]:
    """Perform linter checks on insert query and data and return table name, column names and value column names.

    Args:
    ----
        query: insert query to be parsed
        data: to be inserted into the database
        cursor_formats: CursorFormats containing correct formats for database

    Returns:
    -------
        column names, value names from insert query

    Raises:
    ------
        InvalidInsertQueryError: when value columns do not appear in data

    """
    check_insert_query(query, cursor_formats)
    columns = get_columns_from_insert(query, cursor_formats)
    values = get_values_from_insert(query, cursor_formats)

    if not check_columns_in_data(values, data):
        message = f"""Value columns in insert query do not match columns in data:
        Values are:
            {values}
        but available columns are:
            {data.columns.tolist()}
        """
        raise InvalidInsertQueryError(message)
    return columns, values


def parse_retrieve_query(
    query: str, data: pd.DataFrame, cursor_formats: CursorFormat
) -> list[str]:
    """Perform linter checks on retrieve query and return table name and column names.

    Args:
    ----
        query: insert query to be parsed
        data: to be inserted into the database
        cursor_formats: CursorFormats containing correct formats for database

    Returns:
    -------
        columns from retrieve query

    Raises:
    ------
        InvalidRetrieveQueryError: when columns do not appear in data

    """
    check_retrieve_query(query, cursor_formats)
    columns = get_columns_from_retrieve(query, cursor_formats)

    # excluding the first since it is the <table>_id columns that is going to be added
    if not check_columns_in_data(columns, data):
        message = f"""Columns in retrieve query do not match columns in data:
        Columns are:
            {columns}
        but available columns in data are:
            {data.columns.tolist()}

        """
        raise InvalidRetrieveQueryError(message)
    return columns


def parse_insert_and_retrieve_query(
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
    cursor_formats: CursorFormat,
) -> list[str]:
    """Perform linter checks on insert and retrieve queries and data and check consistency.

    Args:
    ----
        insert_query: insert query to be parsed
        retrieve_query: retrieve query to be parsed
        data: to be inserted into the database
        cursor_formats: CursorFormats containing correct formats for database

    Returns:
    -------
        column names that are inserted and retrieved


    Raises:
    ------
        InvalidInsertAndRetrieveQueryError: if tables or columns don't match between queries

    """
    insert_columns, insert_values = parse_insert_query(
        insert_query, data, cursor_formats
    )

    insert_table = get_table_from_insert(insert_query, cursor_formats)
    retrieve_columns = parse_retrieve_query(
        retrieve_query, data, cursor_formats
    )
    retrieve_table = get_table_from_retrieve(retrieve_query, cursor_formats)

    if insert_table != retrieve_table:
        message = f"""Insert and retrieve queries don't match. 
        The table to which is inserted should match the table from which is retrieved, but received:
        insert table: {insert_table}, retrieve table: {retrieve_table}
        """
        raise InvalidInsertAndRetrieveQueryError(message)

    if insert_values != retrieve_columns:
        message = f"""Insert and retrieve queries don't match. 
        The columns that are inserted should match the columns that are retrieved, but received:
        insert columns: 
            {insert_values}
        retrieve columns:
            {retrieve_columns}
        """
        raise InvalidInsertAndRetrieveQueryError(message)

    return insert_columns


def parse_compare_query(
    query: str, orig_data: pd.DataFrame, cursor_formats: CursorFormat
) -> None:
    """Perform linter checks on compare query and check if correct dataset is passed.

    Args:
    ----
        query: insert query to be parsed
        orig_data: to be compared against data in the database
        cursor_formats: CursorFormats containing correct formats for database

    Raises:
    ------
        WrongDatasetPassedError: when columns with '_id' in their name are detected in orig_data

    """
    check_compare_query(query, cursor_formats)
    if any("_id" in col for col in orig_data.columns):
        message = """Dataset contains columns with '_id' in the name. 
        Did you perhaps pass [data] instead of [orig_data] to compare()? 😊"""
        raise WrongDatasetPassedError(message)


# ---- Database interface functions


# TODO optie toevoegen om via COPY data in te voegen
def _insert(
    cursor: Cursor, query: str, data: pd.DataFrame, columns: list[str]
) -> None:
    """Perform insert operation.

    Args:
    ----
        cursor: cursor that performs interactions with the database.
        query: insert query
        data: to be inserted into the database
        columns: that are to be inserted from data

    """
    data = data[columns].drop_duplicates()  # type: ignore
    cursor.executemany(query, data.to_dict("records"))  # type: ignore


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
    cursor_formats = get_cursor_formats(cursor)
    columns, _ = parse_insert_query(query, data, cursor_formats)
    _insert(cursor, query, data, columns)


def _retrieve(
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
    columns: list[str],
    *,
    replace: bool,
) -> pd.DataFrame:
    """Perform retrieval from database.

    Args:
    ----
        cursor: cursor that performs interactions with the database.
        query: retrieve query
        data: to attach ids to
        columns: on which to merge
        replace: if columns are to be dropped

    Returns:
    -------
        pd.DataFrame with ids columns merged in

    """
    orig_len = len(data)

    cursor.execute(query)  # type: ignore
    ids_data = pd.DataFrame(cursor.fetchall())

    data = data.merge(ids_data, how="left", on=columns)
    assert not len(data) < orig_len, "Rows were lost when merging on ids."
    assert not len(data) > orig_len, "Rows were duplicated when merging on ids."

    if replace:
        non_id_columns = [col for col in columns if "_id" not in col]
        data = data.drop(columns=non_id_columns)

    return data


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
        replace: whether original columns without _id suffix are to be removed

    Returns:
    -------
        data to which the id columns are merged


    """
    cursor_formats = get_cursor_formats(cursor)
    columns = parse_retrieve_query(query, data, cursor_formats)
    return _retrieve(cursor, query, data, columns, replace=replace)


def insert_and_retrieve_ids(
    cursor: Cursor,
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
    *,
    replace: bool = True,
) -> pd.DataFrame:
    """Insert data into database and retrieve the newly created ids.

    Args:
    ----
        cursor: cursor that performs interactions with the database
        insert_query: insert query of the following format:
            INSERT INTO <table> (<column1>, <column2>, ...)
            VALUES (...) # depending on sqlite or psycopg connection
            ...
        retrieve_query: retrieve query of the followig format:
            SELECT id as <table>_id, <column1>, <column2> as <alias>, ... FROM <table>
        data: to be inserted from and to which ids are to be merged
        replace: whether original columns without _id suffix are to be removed

    Returns:
    -------
        data to which the id columns are merged

    """
    cursor_formats = get_cursor_formats(cursor)
    columns = parse_insert_and_retrieve_query(
        insert_query, retrieve_query, data, cursor_formats
    )
    _insert(cursor, insert_query, data, columns)
    return _retrieve(cursor, retrieve_query, data, columns, replace=replace)


def compare(cursor: Cursor, query: str, orig_data: pd.DataFrame) -> None:
    """Compare data in the database against the original dataset.

    Args:
    ----
        cursor: cursor that peforms interactions with the database
        query: compare query of the following format:
            SELECT
                <table>.<column> as <alias>,
                <table>.<column>,
                <column>,
                ...
            FROM <table>
                JOIN <other_table> ON <other_table>.<table>_id = <table>.id
                JOIN ...
            ...
        orig_data: original data to be stored in the database, before any processing

    """
    cursor_formats = get_cursor_formats(cursor)
    parse_compare_query(query, orig_data, cursor_formats)
    cursor.execute(query)  # type: ignore
    data = pd.DataFrame(cursor.fetchall())

    orig_data = orig_data.sort_values(by=orig_data.columns.tolist())
    data = data.sort_values(by=orig_data.columns.tolist())

    pd.testing.assert_frame_equal(orig_data, data, check_like=True)
