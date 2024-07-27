from etl_components.schema import Schema


class QueryInputError(Exception):
    """Raised when something is wrong with the input for an insert query."""


def parse_input(
    table: str,
    columns: list[str],
    schema: Schema,
) -> list[str]:
    """Parse input values for insert or retrieve query, and return columns that table and data have in common.

    Checks whether table exists in the database,
    and whether any of columns exist for that table.

    Args:
    ----
        table: name of table to be inserted into
        columns: list of columns in dataframe
        schema: database schema

    Raises:
    ------
        QueryInputError: when table does not exist in database
                          when no columns exist for that table

    Returns:
    -------
        list of columns that table and data have in common.

    """
    if table not in schema.table_names:
        message = f"'{table}' does not exists in database schema."
        raise QueryInputError(message)

    schema_table = schema(table)

    if not any(col in schema_table.column_names for col in columns):
        message = f"None of [{columns}] exist in {table}. Table schema is:\n{schema_table}"
        raise QueryInputError(message)

    common = set(columns) & set(schema_table.column_names)
    return list(common)
