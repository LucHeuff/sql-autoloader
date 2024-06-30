from etl_components.schema import Schema


class QueryInputError(Exception):
    """Raised when something is wrong with the input for an insert query."""

    pass


def parse_input(
    table: str,
    columns: dict[str, str],
    schema: Schema,
    data_columns: list[str],
) -> None:
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
        QueryInputError: when table does not exist in database
                          when columns do not exist for that table
                          when values do not exist in the dataframe

    """
    if table not in schema.table_names:
        message = f"'{table}' does not exists in database schema."
        raise QueryInputError(message)

    # parsing the columns and values that were entered
    db_columns = list(columns.keys())
    df_values = list(columns.values())

    schema_table = schema(table)

    # checking if the provided columns exists in the database
    if not all(col in schema_table.column_names for col in db_columns):
        na_columns = [
            col for col in db_columns if col not in schema_table.column_names
        ]
        message = f"columns {na_columns} do not exists in table {table}."
        raise QueryInputError(message)

    # checking if the provided values exist in the dataframe
    if not all(val in data_columns for val in df_values):
        na_values = [val for val in df_values if val not in data_columns]
        message = f"values {na_values} do not exist in dataframe."
        raise QueryInputError(message)
