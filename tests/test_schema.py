# ruff: noqa: SLF001
import pytest

from etl_components.exceptions import (
    AliasDoesNotExistError,
    ColumnsDoNotExistError,
    EmptyColumnListError,
    InvalidReferenceError,
    InvalidTableError,
    NoPrimaryKeyError,
    TableDoesNotExistError,
)
from etl_components.schema import (
    Reference,
    ReferenceDict,
    Schema,
    Table,
    TableDict,
)


def test_table() -> None:
    """Test Table dataclass."""
    full = {
        "name": "full",
        "primary_key": "id",
        "foreign_keys": ["half_id", "quarter_id"],
        "columns": ["one", "two", "three"],
    }
    no_foreign = {
        "name": "no_foreign",
        "primary_key": "key",
        "columns": ["one", "two", "three"],
    }
    only_columns = {"name": "only_columns", "columns": ["one", "two", "three"]}
    only_foreign = {
        "name": "only_foreign",
        "columns": [],
        "foreign_keys": ["half_id", "quarter_id"],
    }

    full_table = Table(**full)
    no_foreign_table = Table(**no_foreign)
    only_columns_table = Table(**only_columns)
    only_foreign_table = Table(**only_foreign)

    assert full_table.has_primary_key == True
    assert (
        str(full_table)
        == "Table full (\n\tid\n\thalf_id\n\tquarter_id\n\tone\n\ttwo\n\tthree\n)"
    )

    assert no_foreign_table.has_primary_key == True
    assert (
        str(no_foreign_table)
        == "Table no_foreign (\n\tkey\n\tone\n\ttwo\n\tthree\n)"
    )

    assert only_columns_table.has_primary_key == False
    assert str(only_columns_table) == str(
        "Table only_columns (\n\tone\n\ttwo\n\tthree\n)"
    )

    assert only_foreign_table.has_primary_key == False
    assert (
        str(only_foreign_table)
        == "Table only_foreign (\n\thalf_id\n\tquarter_id\n)"
    )

    # Testing if validation functions correctly
    empty_table = {"name": "empty", "columns": []}
    with pytest.raises(InvalidTableError):
        Table(**empty_table)


def test_reference() -> None:
    """Test Reference dataclass."""
    ref = {
        "from_table": "from",
        "from_key": "to_id",
        "to_table": "to",
        "to_key": "id",
    }
    reference = Reference(**ref)

    assert str(reference) == "ON from.to_id = to.id"

    # Testing whether invalid reference is caught

    self_ref = {
        "from_table": "from",
        "from_key": "from_id",
        "to_table": "from",
        "to_key": "id",
    }

    with pytest.raises(InvalidReferenceError):
        Reference(**self_ref)


def test_schema() -> None:
    """Test Schema class."""
    # Creating tables
    tables: list[TableDict] = [
        {
            "name": "eigenaar",
            "columns": ["naam"],
            "primary_key": "id",
            "foreign_keys": [],
        },
        {
            "name": "merk",
            "columns": ["naam"],
            "primary_key": "id",
            "foreign_keys": [],
        },
        {
            "name": "voertuig_type",
            "columns": ["naam"],
            "primary_key": "id",
            "foreign_keys": [],
        },
        {
            "name": "dealer",
            "columns": ["naam"],
            "primary_key": "id",
            "foreign_keys": [],
        },
        {
            "name": "voertuig",
            "columns": [],
            "primary_key": "id",
            "foreign_keys": ["type_id", "merk_id"],
        },
        {
            "name": "merk_dealer",
            "columns": [],
            "primary_key": "id",
            "foreign_keys": ["type_id", "merk_id"],
        },
        {
            "name": "voertuig_eigenaar",
            "columns": [],
            "primary_key": "",
            "foreign_keys": ["eigenaar_id", "voertuig_id"],
        },
        {
            "name": "aankoop",
            "columns": ["datum"],
            "primary_key": "",
            "foreign_keys": ["voertuig_id", "dealer_id"],
        },
    ]

    # Creating references
    references: list[ReferenceDict] = [
        {
            "from_table": "voertuig",
            "from_key": "type_id",
            "to_table": "voertuig_type",
            "to_key": "id",
        },
        {
            "from_table": "voertuig",
            "from_key": "merk_id",
            "to_table": "merk",
            "to_key": "id",
        },
        {
            "from_table": "merk_dealer",
            "from_key": "merk_id",
            "to_table": "merk",
            "to_key": "id",
        },
        {
            "from_table": "merk_dealer",
            "from_key": "dealer_id",
            "to_table": "dealer",
            "to_key": "id",
        },
        {
            "from_table": "voertuig_eigenaar",
            "from_key": "eigenaar_id",
            "to_table": "eigenaar",
            "to_key": "id",
        },
        {
            "from_table": "voertuig_eigenaar",
            "from_key": "voertuig_id",
            "to_table": "voertuig",
            "to_key": "id",
        },
        {
            "from_table": "aankoop",
            "from_key": "voertuig_id",
            "to_table": "voertuig",
            "to_key": "id",
        },
        {
            "from_table": "aankoop",
            "from_key": "dealer_id",
            "to_table": "dealer",
            "to_key": "id",
        },
    ]

    def get_schema() -> tuple[list[TableDict], list[ReferenceDict]]:
        return tables, references

    schema = Schema(get_schema)

    # --- Testing Schema.get_columns

    # Testing if correct exception is raised when the table does not exist
    with pytest.raises(TableDoesNotExistError):
        schema.get_columns("trein")

    assert schema.get_columns("eigenaar") == ["naam"]
    assert schema.get_columns("merk") == ["naam"]
    assert schema.get_columns("voertuig_type") == ["naam"]
    assert schema.get_columns("dealer") == ["naam"]
    assert schema.get_columns("voertuig") == []
    assert schema.get_columns("merk_dealer") == []
    assert schema.get_columns("voertuig_eigenaar") == []
    assert schema.get_columns("aankoop") == ["datum"]

    # --- Testing schema._get_table
    with pytest.raises(TableDoesNotExistError):
        schema._get_table("trein")

    for table in tables:
        assert schema._get_table(table["name"]) == Table(**table)

    # --- Testing column_table_mapping

    # first inverting the mapping from tables
    mapping = {}
    for d in tables:
        for col in d["columns"]:
            if not col in mapping:
                mapping[col] = [d["name"]]
            else:
                mapping[col] += [d["name"]]

    assert schema._column_table_mapping == mapping

    # --- Testing parse_insert

    # test if exception is raised for empty list of columns
    with pytest.raises(EmptyColumnListError):
        schema.parse_insert("eigenaar", [])
    # test if exception is raised for nonexisting columns
    with pytest.raises(ColumnsDoNotExistError):
        schema.parse_insert("eigenaar", ["fiets", "trein"])

    for table_name in schema.graph.nodes:
        table = schema._get_table(table_name)
        # testing whether parse_insert returns the common columns correctly
        if table.columns:
            assert (
                schema.parse_insert(table_name, table.columns) == table.columns
            )
        assert set(
            schema.parse_insert(table_name, table.columns_and_foreign_keys)
        ) == set(table.columns_and_foreign_keys)
