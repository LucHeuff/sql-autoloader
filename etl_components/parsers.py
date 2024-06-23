class InsertQueryError(Exception):
    """Raised when something is wrong with the input for an insert query."""

    pass


class RetrieveQueryError(Exception):
    """Raised when something is wrong with the input for a retrieve query."""

    pass


class CompareQueryError(Exception):
    """Raised when something is wrong with the input for a compare query."""

    pass


def parse_insert(
    table: str,
    columns: dict[str, str],
    schema: dict[str, list[str]],
    data_columns: list[str],
) -> tuple[str, dict[str, str]]:
    """Parse input values for insert query.

    Checks whether table exists in the database, columns exist for that table,
    and if the values exist in the dataframe.

    Args:
    ----
        table: name of table to be inserted into
        columns: dict of format {column: value, ...}
        schema: database schema
        data_columns: list of columns from dataframe

    Returns:
    -------
        QueryParts

    Raises:
    ------
        InsertQueryError: when table does not exist in database
                          when columns do not exist for that table
                          when values do not exist in the dataframe

    """
    schema_tables = schema.keys()
    if table not in schema_tables:
        message = f"'{table}' does not exists in database schema."
        raise InsertQueryError(message)

    schema_columns = schema[table]

    # parsing the columns and values that were entered
    db_columns = list(columns.keys())
    df_values = list(columns.values())

    # checking if the provided columns exists in the database
    if not all(col in schema_columns for col in db_columns):
        na_columns = [col for col in db_columns if col not in schema_columns]
        message = f"columns {na_columns} do not exists in table {table}."
        raise InsertQueryError(message)

    # checking if the provided values exist in the dataframe
    if not all(val in data_columns for val in df_values):
        na_values = [val for val in df_values if val not in data_columns]
        message = f"values {na_values} do not exist in dataframe."
        raise InsertQueryError(message)

    return table, columns
