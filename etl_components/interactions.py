import re
from dataclasses import dataclass
from io import StringIO

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
    # query_pattern = r"^\s*INSERT INTO\s*(?P<table>\w+)\s*\((?P<columns>[\S\s]*)\)\s*VALUES\s*\((?P<values>[\S\s]*)\)"
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
        message = f"""Names under which columns are retrieved from the database do not match column names in data:
        Columns are retrieved as:
            {values}
        but available columns in data are:
            {data.columns.tolist()}
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
        message = f"""Insert and retrieve queries don't match.
        columns name pairs in database and in dataframe should match between insert and retrieve queries, but received:
        from insert query:
            {insert_pairs}
        from retrieve query:
            {retrieve_pairs}
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

    pattern = r"^\s*SELECT\s*(?P<columns>[\S\s]+)FROM \w+\s*(?:JOIN \w+ ON \w+\.\w+_id = \w+\.id\s*)*"
    match = re.match(pattern, query)
    if not match:
        message = f"Invalid compare query format. Correct format is\n{correct}"
        raise InvalidCompareQueryError(message)

    columns_section = re.sub(r"\s+", " ", match.group("columns")).split(", ")
    # splitting out parts: e.g "name" or "name as alias" as separate lists
    column_parts = [col.strip().split(" ") for col in columns_section]
    # the first elements now termed 'columns', last elements termed 'values'
    values = [col[-1] for col in column_parts]

    if set(values) != set(orig_data.columns.tolist()):
        message = f"""Invalid compare query format, column names read out from database do not match columns in [orig_data]:
        Read out from database:
            {values}
        But columns in [orig_data]:
            {orig_data.columns.tolist()}
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
        CopyNotAvailableError:

    """
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
            ...
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


def _retrieve(
    cursor: Cursor,
    query: str,
    data: pd.DataFrame,
    parts: QueryParts,
    *,
    replace: bool,
) -> pd.DataFrame:
    """Perform retrieval from database.

    Args:
    ----
        cursor: that performs interactions with the database.
        query: retrieve query
        data: to attach ids to
        parts: QueryParts
        replace: if columns are to be dropped

    Returns:
    -------
        pd.DataFrame with ids columns merged in

    """
    orig_len = len(data)

    cursor.execute(query)  # type: ignore
    ids_data = pd.DataFrame(cursor.fetchall())

    assert len(ids_data) > 0, "Retrieve query did not return any results."

    datetime_columns = data.select_dtypes("datetime").columns.tolist()

    # converting columns to datetime if they are in the original data
    for col in datetime_columns:
        if col in ids_data.columns:
            ids_data[col] = pd.to_datetime(ids_data[col])

    data = data.merge(ids_data, how="left", on=parts.values)
    assert not len(data) < orig_len, "Rows were lost when merging on ids."
    assert not len(data) > orig_len, "Rows were duplicated when merging on ids."

    assert (
        not data.filter(regex="_id$").isna().any(axis=None)  # type: ignore
    ), "Some id's were returned as NaN."

    if replace:
        non_id_columns = [col for col in parts.values if "_id" not in col]
        data = data.drop(columns=non_id_columns)

    return data


def retrieve_ids(
    cursor: Cursor, query: str, data: pd.DataFrame, *, replace: bool = True
) -> pd.DataFrame:
    """Retrieve ids from database.

    Args:
    ----
        cursor: that performs interactoins with the database.
        query: retrieve query of the following format:
            SELECT id as <table>_id, <column_db_1> as <column_df_1>, <column_db_2> FROM <table>
        data: to which ids are to be merged
        replace: whether original columns without _id suffix are to be removed

    Returns:
    -------
        data to which the id columns are merged


    """
    sql_format = get_sql_format(cursor)
    parts = parse_retrieve_query(query, data, sql_format)
    return _retrieve(cursor, query, data, parts, replace=replace)


def insert_and_retrieve_ids(
    cursor: Cursor,
    insert_query: str,
    retrieve_query: str,
    data: pd.DataFrame,
    *,
    replace: bool = True,
    use_copy: bool = False,
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
        cursor, retrieve_query, data, retrieve_parts, replace=replace
    )


def compare(cursor: Cursor, query: str, orig_data: pd.DataFrame) -> None:
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

    """
    sql_format = get_sql_format(cursor)
    parse_compare_query(query, orig_data, sql_format)
    cursor.execute(query)  # type: ignore
    data = pd.DataFrame(cursor.fetchall())

    assert len(data) > 0, "Compare query did not return any results."

    # resetting indices because they don't really matter in themselves
    orig_data = orig_data.sort_values(
        by=orig_data.columns.tolist()
    ).reset_index(drop=True)

    data = data.sort_values(by=orig_data.columns.tolist()).reset_index(
        drop=True
    )

    # converting columns to datetime if they are in orig_data
    datetime_columns = orig_data.select_dtypes("datetime").columns.tolist()
    for col in datetime_columns:
        if col in data:
            data[col] = pd.to_datetime(data[col])

    pd.testing.assert_frame_equal(
        orig_data,
        data,
        check_like=True,
    )
