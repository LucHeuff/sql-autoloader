from etl_components.schema import Schema


class QueryInputError(Exception):
    """Raised when something is wrong with the input for an insert query."""

    pass


def parse_input(
    table: str,
    columns: list[str],
    schema: Schema,
) -> None:
    """Parse input values for insert query.

    Checks whether table exists in the database,
    and whether any of columns exist for that table.

    Args:
    ----
        table: name of table to be inserted into
        columns: dict of format {column: value, ...}
        schema: database schema

    Raises:
    ------
        QueryInputError: when table does not exist in database
                          when no columns exist for that table

    """
    if table not in schema.table_names:
        message = f"'{table}' does not exists in database schema."
        raise QueryInputError(message)

    schema_table = schema(table)

    if not any(col in schema_table.column_names for col in columns):
        message = f"None of {columns} exist in {table}. Table schema is:\n{str(schema_table)}"
        raise QueryInputError(message)
