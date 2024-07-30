from etl_components.sqlite_connector import (
    _get_insert_query,
    _get_retrieve_query,
)


def test_get_insert_query() -> None:
    """Test if _get_insert_query() works as intended."""
    table = "fiets"
    columns = ["kleur", "zadel", "wielen"]
    query = "INSERT OR IGNORE INTO fiets (kleur, zadel, wielen) VALUES (:kleur, :zadel, :wielen)"
    assert _get_insert_query(table, columns) == query


def test_get_retrieve_query() -> None:
    """Test if _get_retrieve_query() works as intended."""
    table = "fiets"
    columns = ["kleur", "zadel", "wielen"]
    query = "SELECT id as fiets_id, kleur, zadel, wielen FROM fiets"
    assert _get_retrieve_query(table, columns) == query
