import pytest

from etl_components.parsers import QueryInputError, parse_input
from etl_components.schema import Schema

# ---- manual tests since hypothesis keeps taking forever to show results


def get_tables() -> list[str]:
    """Mock get_tables."""
    return ["fiets", "auto"]


def get_table_schema(table_name: str) -> str:
    """Mock get_table_schema."""
    return {"fiets": "CREATE fiets", "auto": "CREATE auto"}[table_name]


def get_columns(table_name: str) -> list[str]:
    """Mock get_columns."""
    return {
        "fiets": ["merk", "prijs", "kleur"],
        "auto": ["merk", "prijs", "brandstof"],
    }[table_name]


def get_references(table_name: str) -> list[dict[str, str]]:
    """Mock get_references."""
    return {"fiets": [], "auto": []}[table_name]


mock_schema = Schema(get_tables, get_table_schema, get_columns, get_references)


def test_parse_input() -> None:
    """Test whether parse_input gives the correct results."""
    table = "fiets"
    columns = ["merk", "prijs"]
    assert set(parse_input(table, columns, mock_schema)) == set(columns)


def test_parse_input_fail_table() -> None:
    """Test whether parse_input throws an exception if table does not appear in schema."""
    table = "trein"
    columns = ["merk", "prijs"]
    with pytest.raises(QueryInputError):
        parse_input(table, columns, mock_schema)


def test_parse_input_fail_columns() -> None:
    """Test whether parse_input throws an exception if columns do not appear in table schema."""
    table = "fiets"
    columns = ["boot", "trein"]
    with pytest.raises(QueryInputError):
        parse_input(table, columns, mock_schema)
