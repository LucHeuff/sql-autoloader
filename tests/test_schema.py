import pytest

from etl_components.schema import Reference, Schema, SchemaError, Table


def test_table() -> None:
    """Test if Table functions work as intended."""
    name = "fiets"
    sql = "CREATE fiets"
    columns = ["id", "name"]
    references = [Reference("fabriek_id", "fabriek", "id")]
    refers_to = ["fabriek"]
    referred_by = ["eigenaar"]

    table = Table(name, sql, columns, references, refers_to, referred_by)

    assert table.refers_to == ["fabriek"]
    assert (
        table.get_reference("fabriek")
        == "JOIN fabriek ON fiets.fabriek_id = fabriek.id"
    )
    assert str(table) == sql
    with pytest.raises(SchemaError):
        table.get_reference("doos")


def test_schema() -> None:
    """Test if schema_to_string formats the schema correctly."""

    def get_tables() -> list[str]:
        return ["fiets", "fabriek"]

    def get_table_schema(table_name: str) -> str:
        schemas = {"fiets": "CREATE fiets", "fabriek": "CREATE fabriek"}
        return schemas[table_name]

    def get_columns(table_name: str) -> list[str]:
        columns = {
            "fiets": ["name", "fabriek_id"],
            "fabriek": ["id", "name"],
        }
        return columns[table_name]

    def get_references(table_name: str) -> list[dict[str, str]]:
        references = {
            "fiets": [{"column": "fabriek_id", "table": "fabriek", "to": "id"}],
            "fabriek": [],
        }
        return references[table_name]

    def get_refers_to(table_name: str) -> list[str]:
        refers_to = {"fiets": ["fabriek"], "fabriek": []}
        return refers_to[table_name]

    def get_referred_by(table_name: str) -> list[str]:
        referred_by = {"fiets": [], "fabriek": ["fiets"]}
        return referred_by[table_name]

    schema = Schema(get_tables, get_table_schema, get_columns, get_references)

    # ---- Testing basic properties of Schema
    assert schema.table_names == ["fiets", "fabriek"]
    for table in get_tables():
        assert schema(table).name == table
        assert schema.get_table_schema(table) == get_table_schema(table)
        assert schema.get_columns(table) == get_columns(table)
        assert schema(table).references == [
            Reference(**ref) for ref in get_references(table)
        ]
        assert schema.get_table_refers_to(table) == get_refers_to(table)
        assert schema.get_table_referred_by(table) == get_referred_by(table)
    assert str(schema) == "CREATE fiets\n\nCREATE fabriek"
    # Schema should raise an error when the table does not exist
    with pytest.raises(SchemaError):
        schema("doos")

    # ---- Testing retrieving tables through columns
    assert schema.get_table_by_column("fabriek_id") == "fiets"
    assert schema.get_table_by_column("fiets.fabriek_id") == "fiets"
    # schema should raise an error when the column doesn't exist
    with pytest.raises(SchemaError):
        schema.get_table_by_column("doos")
    # schema should raise an error when the column name appears in multiple tables
    with pytest.raises(SchemaError):
        schema.get_table_by_column("name")
    # schema should raise an error when column doesn't exist for prefixed table
    with pytest.raises(SchemaError):
        schema.get_table_by_column("fiets.doos")
