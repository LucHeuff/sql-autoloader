import re
from dataclasses import dataclass

import pandas as pd

from etl_components.connections import (
    Cursor,
    PostgresCursor,
    SQLFormat,
    get_sql_format,
)


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


@dataclass
class QueryParts:
    """Encapsulates parse_*_query() output."""

    table: str
    columns: list[str]
    values: list[str]


def parse_insert_query(
    query: str, data: pd.DataFrame, sql_format: SQLFormat
) -> QueryParts:
    """Perform linter checks on insert query and data and return table name, column names and value column names.

    Args:
    ----
        query: insert query to be parsed
        data: to be inserted into the database
        sql_format: SQLFormat referring to the SQL dialect

    Returns:
    -------
        InsertParts

    Raises:
    ------
        InvalidInsertQueryError: when relevant parts cannot be found

    """
    correct = sql_format.insert_format

    query_pattern = r"^\s*INSERT INTO\s*(?P<table>\w+)\s*\((?P<columns>[\S\s]*)\)\s*VALUES\s*\((?P<values>[\S\s]*)\)"
    columns_pattern = r"(\w+)"
    values_pattern = sql_format.values_pattern

    match = re.match(query_pattern, query)
    # Check if the query has the correct format
    if match is None:
        message = f"Invalid insert query. correct format is:\n{correct}"
        raise InvalidInsertQueryError(message)

    table = match.group("table")
    columns = re.findall(columns_pattern, match.group("columns"))
    # Check if columns were found
    if not columns:
        message = f"Invalid insert query, could not find columns to insert into. Correct format is:\n{correct}"
        raise InvalidInsertQueryError(message)

    values = re.findall(values_pattern, match.group("values"))
    # Check if values were found
    if not values:
        message = f"Invalid insert query, could not find columns to insert values from. Correct format is:\n{correct}"
        raise InvalidInsertQueryError(message)

    # Check if values appear in the data
    if not check_columns_in_data(values, data):
        message = f"""Columns from data from which insert query should insert values do not match columns in data:
        Value column are:
            {values}
        but available columns are:
            {data.columns.tolist()}
        """
        raise InvalidInsertQueryError(message)

    return QueryParts(table, columns, values)


def parse_retrieve_query(
    query: str, data: pd.DataFrame, sql_format: SQLFormat
) -> QueryParts:
    """Perform linter checks on retrieve query and return table name and column names.

    Args:
    ----
        query: insert query to be parsed
        data: to be inserted into the database
        sql_format: SQLFormat referring to the SQL dialect

    Returns:
    -------
        QueryParts

    Raises:
    ------
        InvalidRetrieveQueryError: when relevant parts cannot be found

    """
    correct = sql_format.retrieve_format

    query_pattern = r"^\s*SELECT\s*id as (?P<table>\w+)_id, (?P<columns>[\S\s]*)\s*FROM\s*(?P=table)\s*$"

    match = re.fullmatch(query_pattern, query)
    # Check if the query hast the correct format
    if match is None:
        message = f"Invalid retrieve query. correct format is:\n{correct}"
        raise InvalidRetrieveQueryError(message)

    table = match.group("table")

    columns_section = match.group("columns").split(", ")
    # splitting out parts: e.g "name" or "name as alias" as separate lists
    column_parts = [col.strip().split(" ") for col in columns_section]
    # the first elements now termed 'columns', last elements termed 'values'
    columns, values = zip(*[(col[0], col[-1]) for col in column_parts])
    # making sure columns and values are lists
    columns, values = list(columns), list(values)
    # Check if something was found for columns and values
    if not columns or not values:
        message = f"Invalid retrieve query, could not find columns. Correct format is:\n{correct}"
        raise InvalidRetrieveQueryError(message)

    # Check if the columns named in values appear in the data
    if not check_columns_in_data(values, data):
        message = f"""Columns in retrieve query do not match columns in data:
        Columns are:
            {values}
        but available columns in data are:
            {data.columns.tolist()}
        Are you using the right aliases?
        """
        raise InvalidRetrieveQueryError(message)

    return QueryParts(table, columns, values)


@dataclass
class InsertAndRetrieveParts:
    """Encapsulates parse_insert_and_retrieve_query() outputs."""

    insert_values: list[str]
    retrieve_values: list[str]


def parse_insert_and_retrieve_query(
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
    sql_format: SQLFormat,
) -> InsertAndRetrieveParts:
    """Perform linter checks on insert and retrieve queries and data and check consistency.

    Args:
    ----
        insert_query: insert query to be parsed
        retrieve_query: retrieve query to be parsed
        data: to be inserted into the database
        sql_format: SQLFormat referring to the SQL dialect


    Returns:
    -------
        retrieve columns, insert_values


    Raises:
    ------
        InvalidInsertAndRetrieveQueryError: if tables or columns don't match between queries

    """
    insert_parts = parse_insert_query(insert_query, data, sql_format)
    retrieve_parts = parse_retrieve_query(retrieve_query, data, sql_format)

    if insert_parts.table != retrieve_parts.table:
        message = f"""Insert and retrieve queries don't match. 
        The table to which is inserted should match the table from which is retrieved, but received:
        insert table: {insert_parts.table}, retrieve table: {retrieve_parts.table}
        """
        raise InvalidInsertAndRetrieveQueryError(message)

    if insert_parts.columns != retrieve_parts.columns:
        message = f"""Insert and retrieve queries don't match. 
        The columns that are inserted should match the columns that are retrieved, but received:
        insert columns: 
            {insert_parts.columns}
        retrieve columns:
            {retrieve_parts.columns}
        """
        raise InvalidInsertAndRetrieveQueryError(message)

    # NOTE: no need to compare values, that happens intrinsically with comparison to data!

    return InsertAndRetrieveParts(insert_parts.values, retrieve_parts.values)


def parse_compare_query(
    query: str, orig_data: pd.DataFrame, sql_format: SQLFormat
) -> None:
    """Perform linter checks on compare query and check if correct dataset is passed.

    Args:
    ----
        query: insert query to be parsed
        orig_data: to be compared against data in the database
        sql_format: SQLFormat referring to the SQL dialect

    Raises:
    ------
        InvalidCompareQueryError: when query has the wrong format
        WrongDatasetPassedError: when columns with '_id' in their name are detected in orig_data

    """
    correct = sql_format.compare_format

    pattern = r"^\s*SELECT\s*[\S\s]+FROM \w+\s*(?:JOIN \w+ ON \w+\.\w+_id = \w+\.id\s*)+"
    if not re.match(pattern, query):
        message = f"Invalid compare query format. Correct format is\n{correct}"
        raise InvalidCompareQueryError(message)

    if any("_id" in col for col in orig_data.columns):
        message = """Dataset contains columns with '_id' in the name. 
        Did you perhaps pass [data] instead of [orig_data] to compare()? 😊"""
        raise WrongDatasetPassedError(message)


# ---- Database interface functions

# TODO check of deze nog allemaal naar behoren werken na refactor


def _insert(
    cursor: Cursor, query: str, data: pd.DataFrame, values: list[str]
) -> None:
    """Perform insert operation.

    Args:
    ----
        cursor: cursor that performs interactions with the database.
        query: insert query
        data: to be inserted into the database
        values: column names that are to be inserted from data

    """
    data = data[values].drop_duplicates()  # type: ignore
    cursor.executemany(query, data.to_dict("records"))  # type: ignore


# TODO optie toevoegen om via COPY data in te voegen
def _insert_with_copy(
    cursor: PostgresCursor, query: str, data: pd.DataFrame, values: list[str]
) -> None:
    pass


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
    sql_format = get_sql_format(cursor)
    parts = parse_insert_query(query, data, sql_format)

    _insert(cursor, query, data, parts.values)


def _retrieve(
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
    values: list[str],
    *,
    replace: bool,
) -> pd.DataFrame:
    """Perform retrieval from database.

    Args:
    ----
        cursor: cursor that performs interactions with the database.
        query: retrieve query
        data: to attach ids to
        values: columns on which to merge
        replace: if columns are to be dropped

    Returns:
    -------
        pd.DataFrame with ids columns merged in

    """
    orig_len = len(data)

    cursor.execute(query)  # type: ignore
    ids_data = pd.DataFrame(cursor.fetchall())

    data = data.merge(ids_data, how="left", on=values)
    assert not len(data) < orig_len, "Rows were lost when merging on ids."
    assert not len(data) > orig_len, "Rows were duplicated when merging on ids."

    if replace:
        non_id_columns = [col for col in values if "_id" not in col]
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
    sql_format = get_sql_format(cursor)
    parts = parse_retrieve_query(query, data, sql_format)
    return _retrieve(cursor, query, data, parts.values, replace=replace)


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
    sql_format = get_sql_format(cursor)

    parts = parse_insert_and_retrieve_query(
        insert_query, retrieve_query, data, sql_format
    )
    _insert(cursor, insert_query, data, parts.insert_values)
    return _retrieve(
        cursor, retrieve_query, data, parts.retrieve_values, replace=replace
    )


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
    sql_format = get_sql_format(cursor)
    parse_compare_query(query, orig_data, sql_format)
    cursor.execute(query)  # type: ignore
    data = pd.DataFrame(cursor.fetchall())

    orig_data = orig_data.sort_values(by=orig_data.columns.tolist())
    data = data.sort_values(by=orig_data.columns.tolist())

    pd.testing.assert_frame_equal(orig_data, data, check_like=True)
