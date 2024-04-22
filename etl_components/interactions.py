import re
from dataclasses import dataclass
from io import StringIO

import numpy as np
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


class CopyNotAvailableError(Exception):
    """Exception when the user tries insert with use_copy=True when COPY is not supported."""

    pass


class FailedMergeOnRetrieveError(Exception):
    """Exception when merging in ids results in NaNs in an id column."""


# ---- suppoert functions


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


def replace_na(data: pd.DataFrame) -> pd.DataFrame:
    """Replace all values that pandas recognises as na with None.

    Postgres does not consistently handle np.nan as NULL, and when returned
    pandas will not recognise a NaN from Postgres as missing.
    Hence the need to uniformly treat missing values.

    Args:
    ----
        data: in which missing values are to be replaced with None

    Returns:
    -------
        data where missing values are replaced with None

    """
    return data.where(data.notnull(), None)


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
    """Perform linter checks on insert query and data and return table, columns and values.

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

    query_pattern = sql_format.insert_pattern
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

    # Check if an equal number of columns and values was found
    if len(columns) != len(values):
        message = f"""Invalid insert query, extracted unequal number of database columns and dataset values.
        Extrated columns are:
            {columns}
        but values are:
            {values}
        Did you forget a column, or specify it in an incorrect format? Correct format is:\n{correct}
        """
        raise InvalidInsertQueryError(message)
    # Check if values appear in the data
    if not check_columns_in_data(values, data):
        columns = set(data.columns.tolist())
        match = set(values) & columns
        message = f"""Mismatch between columns in query and columns in data:
        Columns in insert query without a match:
            {list(set(values) - match)}
        Available columns in data without match:
            {list(columns - match)}
        """
        raise InvalidInsertQueryError(message)

    return QueryParts(table, columns, values)


def parse_retrieve_query(
    query: str, data: pd.DataFrame, sql_format: SQLFormat
) -> QueryParts:
    """Perform linter checks on retrieve query and return table, columns and values.

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

    columns_section = re.sub(r"\s+", " ", match.group("columns")).split(", ")
    # splitting out parts: e.g "name" or "name as alias" as separate lists
    column_parts = [col.strip().split(" ") for col in columns_section]
    # the first elements now termed 'columns', last elements termed 'values'
    columns, values = zip(*[(col[0], col[-1]) for col in column_parts])
    # making sure columns and values are lists
    columns, values = list(columns), list(values)
    # Check if something was found for columns and values
    if not columns or not values:
        message = f"Invalid retrieve query, could not find database columns. Correct format is:\n{correct}"
        raise InvalidRetrieveQueryError(message)

    # Check if the values appear in the data
    if not check_columns_in_data(values, data):
        columns = set(data.columns.tolist())
        match = set(values) & columns
        message = f"""Mismatch between column names retrieved from database and column names in data:
        Retrieved columns without a match:
            {list(set(values) - match)}
        Available columns in data without a match:
            {list(columns - match)}
        Are you using the right aliases?
        """
        raise InvalidRetrieveQueryError(message)

    return QueryParts(table, columns, values)


def parse_insert_and_retrieve_query(
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
    sql_format: SQLFormat,
) -> tuple[QueryParts, QueryParts]:
    """Perform linter checks on insert and retrieve queries and data and check consistency.

    Args:
    ----
        insert_query: insert query to be parsed
        retrieve_query: retrieve query to be parsed
        data: to be inserted into the database
        sql_format: SQLFormat referring to the SQL dialect


    Returns:
    -------
        insert QueryParts, retrieve QueryParts


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

    insert_pairs = list(zip(insert_parts.columns, insert_parts.values))
    retrieve_pairs = list(zip(retrieve_parts.columns, retrieve_parts.values))

    if set(insert_pairs) != set(retrieve_pairs):
        match_pairs = set(insert_pairs) & set(retrieve_pairs)
        message = f"""Insert and retrieve queries don't match.
        Column name pairs in database and in dataframe should match between insert and retrieve queries, but received:
        Nonmatching pairs from insert query:
            {list(set(insert_pairs) - match_pairs)}
        Nonmatching pairs from retrieve query:
            {list(set(retrieve_pairs) - match_pairs)}
        """
        raise InvalidInsertAndRetrieveQueryError(message)

    return insert_parts, retrieve_parts


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

    pattern = r"^\s*SELECT\s*(?P<columns>[\S\s]+)FROM \w+\s*(?:(?:[A-Z]+\s)*JOIN \w+ ON \w+\.\w+_id = \w+\.id\s*)*"
    match = re.match(pattern, query)
    if not match:
        message = f"Invalid compare query format. Correct format is\n{correct}"
        raise InvalidCompareQueryError(message)

    columns_section = re.sub(r"\s+", " ", match.group("columns")).split(", ")
    # splitting out parts: e.g "name" or "name as alias" as separate lists
    column_parts = [col.strip().split(" ") for col in columns_section]
    # the first elements now termed 'columns', last elements termed 'values'
    values = {col[-1] for col in column_parts}

    if values != (columns := set(orig_data.columns.tolist())):
        match = values & columns
        message = f"""Invalid compare query format, mismatch between columns read from database and columns in orig_data:
        Read out from database without a match:
            {list(values - match)}
        But columns in [orig_data]:
            {list(columns - match)}
        """
        raise InvalidCompareQueryError(message)


# ---- Database interface functions


def _insert_without_copy(
    cursor: Cursor, query: str, data: pd.DataFrame, parts: QueryParts
) -> None:
    """Perform insert operation.

    Args:
    ----
        cursor: that performs interactions with the database.
        query: insert query
        data: to be inserted into the database
        parts: QueryParts

    """
    data = data[parts.values].drop_duplicates()  # type: ignore
    cursor.executemany(query, data.to_dict("records"))  # type: ignore


def _insert_with_copy(
    cursor: PostgresCursor, data: pd.DataFrame, parts: QueryParts
) -> None:
    """Perform insert operation using COPY protocol.

    Args:
    ----
        cursor: that performs interactions with the database.
        data: to be inserted into the database
        parts: QueryParts

    """
    data = (
        data[parts.values]
        .drop_duplicates()
        .rename(columns=dict(zip(parts.values, parts.columns)))
    )  # type: ignore

    columns_section = ", ".join(parts.columns)

    query = f"COPY {parts.table} ({columns_section}) FROM STDIN (FORMAT CSV, DELIMITER ',')"

    # storing into memory that pretends to be a CSV
    string_io = StringIO()
    string_io.write(data.to_csv(index=False, header=False, sep=","))
    string_io.seek(0)

    with cursor.copy(query) as copy:  # type: ignore
        copy.write(string_io.read())


def _insert(  # noqa: PLR0913
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
    sql_format: SQLFormat,
    parts: QueryParts,
    *,
    use_copy: bool = False,
) -> None:
    """Perform the insert operation.

    Args:
    ----
        cursor: that performs interactions with the database.
        query: insert query
        data: to be inserted
        sql_format: SQLFormat
        parts: QueryParts
        use_copy: if COPY protocol is to be used (default: False)

    Raises:
    ------
        CopyNotAvailableError: when use_copy is called but not supported for cursor.

    """
    data = replace_na(data)
    if sql_format.copy_available and use_copy:
        _insert_with_copy(cursor, data, parts)  # type: ignore
    elif not sql_format.copy_available and use_copy:
        raise CopyNotAvailableError("COPY not available for this cursor")
    else:
        _insert_without_copy(cursor, query, data, parts)


def insert(
    cursor: Cursor, query: str, data: pd.DataFrame, *, use_copy: bool = False
) -> None:
    """Insert data into database.

    Args:
    ----
        cursor: that performs interactions with the database.
        query: insert query of the following format:
            INSERT INTO <table> (<column_db_1>, <column_db_2>, ...)
            VALUES (...)  (format depending on sqlite or psycopg connection)

        data: to be inserted into the database
        use_copy: whether to use COPY if the cursor supports this.
            NOTE: regular queries will be translated to COPY,
            but COPY simply appends and does not support handling validity checks.
            Consistent behaviour is not guaranteed. Use at your own risk.

    Raises:
    ------
        CopyNotAvailableError: when use_copy is called but not supported for cursor.

    """
    sql_format = get_sql_format(cursor)
    parts = parse_insert_query(query, data, sql_format)
    _insert(cursor, query, data, sql_format, parts, use_copy=use_copy)


def _retrieve(  # noqa: PLR0913
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
    parts: QueryParts,
    *,
    replace: bool,
    allow_shrinking: bool = False,
    allow_duplication: bool = False,
) -> pd.DataFrame:
    """Perform retrieval from database.

    Args:
    ----
        cursor: that performs interactions with the database.
        query: retrieve query
        data: to attach ids to
        parts: QueryParts
        replace: if columns are to be dropped
        allow_shrinking: if rows are allowed to be lost when merging ids
        allow_duplication: if rows are allowed to be duplicated when merging ids

    Returns:
    -------
        pd.DataFrame with ids columns merged in

    """
    orig_len = len(data)

    cursor.execute(query)  # type: ignore
    ids_data = replace_na(pd.DataFrame(cursor.fetchall()))

    assert len(ids_data) > 0, "Retrieve query did not return any results."

    datetime_columns = data.select_dtypes("datetime").columns.tolist()

    # converting columns to datetime if they are in the original data
    for col in datetime_columns:
        if col in ids_data.columns:
            ids_data[col] = pd.to_datetime(ids_data[col])

    data = replace_na(data).merge(ids_data, how="left", on=parts.values)
    assert (
        not len(data) < orig_len or allow_shrinking
    ), "Rows were lost when merging on ids."
    assert (
        not len(data) > orig_len or allow_duplication
    ), "Rows were duplicated when merging on ids."

    if (missing_ids := data.filter(regex="_id$").isna()).any(axis=None):  # type: ignore
        missing_id_rows = data[missing_ids.any(axis=1)]
        message = f"Some id's were returned as NaN:\n{str(missing_id_rows)}"
        raise FailedMergeOnRetrieveError(message)

    if replace:
        non_id_columns = [col for col in parts.values if "_id" not in col]
        data = data.drop(columns=non_id_columns)

    return data


def retrieve_ids(  # noqa: PLR0913
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
    *,
    replace: bool = True,
    allow_shrinking: bool = False,
    allow_duplication: bool = False,
) -> pd.DataFrame:
    """Retrieve ids from database.

    Args:
    ----
        cursor: that performs interactoins with the database.
        query: retrieve query of the following format:
            SELECT id as <table>_id, <column_db_1> as <column_df_1>, <column_db_2> FROM <table>
        data: to which ids are to be merged
        replace: whether original columns without _id suffix are to be removed
        allow_shrinking: if rows are allowed to be lost when merging ids
        allow_duplication: if rows are allowed to be duplicated when merging ids

    Returns:
    -------
        data to which the id columns are merged


    """
    sql_format = get_sql_format(cursor)
    parts = parse_retrieve_query(query, data, sql_format)
    return _retrieve(
        cursor,
        query,
        data,
        parts,
        replace=replace,
        allow_shrinking=allow_shrinking,
        allow_duplication=allow_duplication,
    )


def insert_and_retrieve_ids(  # noqa: PLR0913
    cursor: Cursor,
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
    *,
    replace: bool = True,
    use_copy: bool = False,
    allow_shrinking: bool = False,
    allow_duplication: bool = False,
) -> pd.DataFrame:
    """Insert data into database and retrieve the newly created ids.

    Args:
    ----
        cursor: that performs interactions with the database
        insert_query: insert query of the following format:
            INSERT INTO <table> (<column_db_1>, <column_db_2>, ...)
            VALUES (...)  (format depending on sqlite or psycopg connection)
            ...
        retrieve_query: retrieve query of the followig format:
            SELECT id as <table>_id, <column_db_1> as <column_df_1>, <column_db_2> FROM <table>
        data: to be inserted from and to which ids are to be merged
        replace: whether original columns without _id suffix are to be removed
        use_copy: whether to use COPY if the cursor supports this.
            NOTE: regular queries will be translated to COPY,
            but COPY simply appends and does not support handling validity checks.
            Consistent behaviour is not guaranteed. Use at your own risk.
        allow_shrinking: if rows are allowed to be lost when merging ids
        allow_duplication: if rows are allowed to be duplicated when merging ids

    Returns:
    -------
        data to which the id columns are merged

    """
    sql_format = get_sql_format(cursor)

    insert_parts, retrieve_parts = parse_insert_and_retrieve_query(
        insert_query, retrieve_query, data, sql_format
    )
    _insert(
        cursor, insert_query, data, sql_format, insert_parts, use_copy=use_copy
    )
    return _retrieve(
        cursor,
        retrieve_query,
        data,
        retrieve_parts,
        replace=replace,
        allow_shrinking=allow_shrinking,
        allow_duplication=allow_duplication,
    )


def compare(
    cursor: Cursor, query: str, orig_data: pd.DataFrame, *, exact: bool = True
) -> None:
    """Compare data in the database against the original dataset.

    Args:
    ----
        cursor: that peforms interactions with the database
        query: compare query of the following format:
            SELECT
                <table>.<column_db_1> as <column_df_1>,
                <table>.<column_db_2>,
                <column_db_3>,
                ...
            FROM <table>
                JOIN <other_table> ON <other_table>.<table>_id = <table>.id
                JOIN ...
            ...
        orig_data: original data to be stored in the database, before any processing.
                   This is used to compare the data in the database against.
        exact: whether the data from the database must exactly match the orig_data.
               if set to False, will check if orig_data is a subset of data in database.
               (Default: True)

    """
    sql_format = get_sql_format(cursor)
    parse_compare_query(query, orig_data, sql_format)
    cursor.execute(query)  # type: ignore
    data = pd.DataFrame(cursor.fetchall())

    assert len(data) > 0, "Compare query did not return any results."

    def preprocess(data: pd.DataFrame) -> pd.DataFrame:
        return (
            replace_na(data)
            .sort_values(by=orig_data.columns.tolist())
            .reset_index(drop=True)
            .fillna(np.nan)
        )

    # converting columns to datetime if they are in orig_data
    datetime_columns = orig_data.select_dtypes("datetime").columns.tolist()
    for col in datetime_columns:
        if col in data:
            data[col] = pd.to_datetime(data[col])

    if exact:
        # Converting dtypes in data to match with orig_data
        for col, dtype in orig_data.dtypes.items():
            if data[col].dtype != dtype:
                data[col] = data[col].astype(dtype)

        pd.testing.assert_frame_equal(
            preprocess(orig_data),
            preprocess(data),
            check_like=True,
            check_column_type=False,
        )
    else:
        # testing whether orig_data is a subset of data
        assert (
            (merged_shape := orig_data.merge(data).drop_duplicates().shape)
            == (orig_shape := orig_data.shape)
        ), f"Original data and retrieved data do not have the same shape ({orig_shape=} != {merged_shape=})."
