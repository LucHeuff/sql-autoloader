from etl_components.query_generators import (
    create_insert_query,
    create_retrieve_query,
)


def test_create_insert_query_sqlite() -> None:
    """Test whether create_insert_query works as intended for sqlite3 connectors."""
    table = "test"
    columns = ["test1", "test2", "test3", "test4"]
    insert_prefix = "INSERT OR IGNORE"
    insert_postfix = ""

    def param_method(column: str) -> str:
        return f":{column}"

    query = "INSERT OR IGNORE INTO test (test1, test2, test3, test4)\nVALUES (:test1, :test2, :test3, :test4)"

    assert (
        create_insert_query(
            table, columns, insert_prefix, insert_postfix, param_method
        )
        == query.strip()
    )


def test_create_insert_query_postgres() -> None:
    """Test whether create_insert_quer works as intended for psycopg connectors."""
    table = "test"
    columns = ["test1", "test2", "test3", "test4"]
    insert_prefix = "INSERT"
    insert_postfix = "\nON CONFLICT DO NOTHING"

    def param_method(column: str) -> str:
        return f"%({column})s"

    query = "INSERT INTO test (test1, test2, test3, test4)\nVALUES (%(test1)s, %(test2)s, %(test3)s, %(test4)s)\nON CONFLICT DO NOTHING"

    assert (
        create_insert_query(
            table, columns, insert_prefix, insert_postfix, param_method
        )
        == query.strip()
    )


def test_create_retrieve_query() -> None:
    """Test whether create_retrieve_query works as intended."""
    table = "test"
    columns = ["test1", "test2", "test3", "test4"]
    query = "SELECT id, test1, test2, test3, test4 FROM test"
    assert create_retrieve_query(table, columns) == query
