from etl_components.query_generators import create_retrieve_query


def test_create_retrieve_query() -> None:
    """Test whether create_retrieve_query works as intended."""
    table = "test"
    columns = ["test1", "test2", "test3", "test4"]
    query = "SELECT id, test1, test2, test3, test4 FROM test"
    assert create_retrieve_query(table, columns) == query
