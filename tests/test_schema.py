# ruff: noqa: SLF001
import pytest

from etl_components.schema import Reference, Schema, SchemaError, Table


def test_table() -> None:
    """Test if Table functions work as intended."""
    name = "fiets"
    sql = "CREATE fiets"
    columns = ["id", "name"]
    references = [Reference("fiets", "fabriek_id", "fabriek", "id")]
    refers_to = ["fabriek"]
    referred_by = ["eigenaar"]

    table = Table(name, sql, columns, references, refers_to, referred_by)

    assert table.name == name
    assert table.sql == sql
    assert table.columns == columns
    assert table.references == references
    assert table.refers_to == refers_to
    assert table.referred_by == referred_by
    assert str(table) == sql


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
            "fiets": [
                {
                    "from_table": "fiets",
                    "from_column": "fabriek_id",
                    "to_table": "fabriek",
                    "to_column": "id",
                }
            ],
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
    assert schema._table_names == ["fiets", "fabriek"]
    for table in get_tables():
        assert schema._get_table(table).name == table
        assert schema.get_columns(table) == get_columns(table)
        assert schema._get_table(table).references == [
            Reference(**ref) for ref in get_references(table)
        ]
        assert schema._get_table_refers_to(table) == get_refers_to(table)
        assert schema._get_table_referred_by(table) == get_referred_by(table)
    assert str(schema) == "CREATE fiets\n\nCREATE fabriek"
    # Schema should raise an error when the table does not exist
    with pytest.raises(SchemaError):
        schema._get_table("doos")

    # ---- Testing retrieving tables through columns
    assert schema._get_table_by_column("fabriek_id") == "fiets"
    assert schema._get_table_by_column("fiets.fabriek_id") == "fiets"
    assert schema._get_table_by_column("fiets.name") == "fiets"
    # schema should raise an error when the column doesn't exist
    with pytest.raises(SchemaError):
        schema._get_table_by_column("doos")
    # schema should raise an error when the column name appears in multiple tables
    with pytest.raises(SchemaError):
        schema._get_table_by_column("name")
    # schema should raise an error when column doesn't exist for prefixed table
    with pytest.raises(SchemaError):
        schema._get_table_by_column("fiets.doos")

    # ---- testing input parsing
    assert set(schema.parse_input("fiets", ["name", "fabriek_id"])) == {
        "name",
        "fabriek_id",
    }
    assert set(schema.parse_input("fiets", ["name"])) == {"name"}
    # parse_input should raise an error when none of the columns exist for the table
    with pytest.raises(SchemaError):
        schema.parse_input("fiets", ["doos", "truck"])

    # ---- testing get_tables_from_columns
    assert set(schema._get_tables_from_columns(["fabriek_id", "id"])) == {
        "fiets",
        "fabriek",
    }
    assert set(
        schema._get_tables_from_columns(["fiets.name", "fabriek.name"])
    ) == {
        "fiets",
        "fabriek",
    }
    with pytest.raises(SchemaError):
        schema._get_tables_from_columns(["fabriek_id"])

    # ---- testing get_insert_and_retrieve_tables
    insert_tables = schema.get_insert_and_retrieve_tables(
        ["fiets.name", "fabriek.name"]
    )
    assert insert_tables.insert_and_retrieve == ["fabriek"]
    assert insert_tables.insert == ["fiets"]

    # ---- testing get_compare_query
    compare = "SELECT fiets.name, fabriek.name\nFROM fiets\n\tJOIN fabriek ON fiets.fabriek_id = fabriek.id"
    compare_where = compare + "\nWHERE fiets.name = 'van mij'"

    assert schema.get_compare_query(["fiets.name", "fabriek.name"]) == compare
    assert (
        schema.get_compare_query(
            ["fiets.name", "fabriek.name"], where="WHERE fiets.name = 'van mij'"
        )
        == compare_where
    )
    assert (
        schema.get_compare_query(
            ["fiets.name", "fabriek.name"], where="fiets.name = 'van mij'"
        )
        == compare_where
    )
