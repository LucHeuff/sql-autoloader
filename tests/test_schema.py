import pytest

from etl_components.schema import Reference, Schema, SchemaError, Table


def test_table() -> None:
    """Test if Table functions work as intended."""
    name = "fiets"
    sql = "CREATE fiets"
    columns = ["id", "name"]
    references = [Reference("fabriek_id", "fabriek", "id")]
    referred_by = ["eignaar"]

    table = Table(name, sql, columns, references, referred_by)

    assert table.refers_to == ["fabriek"]
    assert table.get_reference("fabriek") == references[0]
    assert str(table) == sql
    with pytest.raises(SchemaError):
        table.get_reference("doos")


def test_print_schema() -> None:
    """Test if schema_to_string formats the schema correctly."""
    tables = [
        Table("fiets", "CREATE fiets", ["", ""], [Reference("", "", "")], None),
        Table(
            "fabriek", "CREATE fabriek", ["", ""], [Reference("", "", "")], None
        ),
    ]
    schema = Schema(tables)

    assert schema.table_names == ["fiets", "fabriek"]
    for table in tables:
        assert schema(table.name) == table
    assert str(schema) == "CREATE fiets\n\nCREATE fabriek"
    with pytest.raises(SchemaError):
        schema("doos")
