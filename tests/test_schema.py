# ruff: noqa: SLF001
from dataclasses import dataclass
from pprint import pp

import hypothesis.strategies as st
import pytest
from hypothesis import given
from networkx import has_path

from etl_components.exceptions import (
    AliasDoesNotExistError,
    ColumnIsAmbiguousError,
    ColumnsDoNotExistOnTableError,
    EmptyColumnListError,
    InvalidReferenceError,
    InvalidTableError,
    NoPrimaryKeyError,
    NoSuchColumnForTableError,
    NoSuchColumnInSchemaError,
    TableDoesNotExistError,
)
from etl_components.schema import (
    Reference,
    ReferenceDict,
    Schema,
    Table,
    TableDict,
)
from tests.generators import name_generator, names_generator

# --- Testing table


@dataclass
class TableStrategy:
    """Container for output of table_strategy()."""

    table_dict: TableDict
    all_columns: list[str]
    prefix_column_map: dict[str, str]
    has_primary_key: bool
    prefixed_columns_without: list[tuple[str, str]]
    prefixed_columns_with: list[tuple[str, str]]


@st.composite
def table_strategy(draw: st.DrawFn) -> TableStrategy:
    """Strategy for testing Table."""
    name: str = draw(name_generator())
    has_primary_key = draw(st.booleans())
    primary_key = draw(name_generator()) if has_primary_key else ""
    all_columns = draw(names_generator(min_size=3))
    n_foreign_keys = draw(st.integers(0, len(all_columns)))
    foreign_keys = all_columns[:n_foreign_keys]
    columns = all_columns[n_foreign_keys:]
    prefix_column_map = {f"{name}.{col}": col for col in all_columns}
    # making a list with [( prefixed, non_prefixed )] to test get_prefixed_columns
    prefixed_columns_without = [(f"{name}.{col}", col) for col in all_columns]
    # making a list with [( prefixed, prefixed )] to test get_prefixed_columns
    prefixed_columns_with = [(col, col) for col in prefix_column_map]

    table_dict: TableDict = {
        "name": name,
        "columns": columns,
        "primary_key": primary_key,
        "foreign_keys": foreign_keys,
    }
    return TableStrategy(
        table_dict,
        all_columns,
        prefix_column_map,
        has_primary_key,
        prefixed_columns_without,
        prefixed_columns_with,
    )


def test_basic_table() -> None:
    """Basic test of Table."""
    columns = ["one", "two"]
    foreign_keys = ["one_id", "two_id"]

    table_dict = {
        "name": "test",
        "primary_key": "id",
        "foreign_keys": foreign_keys,
        "columns": columns,
    }

    prefix_column_map = {f"test.{col}": col for col in columns + foreign_keys}

    table = Table(**table_dict)
    # Testing properties
    assert table.columns_and_foreign_keys == foreign_keys + columns
    assert table.has_primary_key
    assert table.prefix_column_map == prefix_column_map
    # Testing __contains__
    assert all(col in table for col in columns + foreign_keys)
    # Testing methods
    assert table.get_common_columns(["one"]) == ["one"]
    assert table.get_prefixed_columns(["one", "test.two"]) == [
        ("test.one", "one"),
        ("test.two", "test.two"),
    ]
    # Testing __str__
    assert (
        str(table) == "Table test (\n\tid\n\tone_id\n\ttwo_id\n\tone\n\ttwo\n)"
    )


def test_empty_table() -> None:
    """Test if providing an empty table to Table results in the correct error."""
    empty = {"name": "", "columns": []}
    with pytest.raises(InvalidTableError):
        Table(**empty)


@given(strategy=table_strategy())
def test_table(strategy: TableStrategy) -> None:
    """Simulation test of Table."""
    table = Table(**strategy.table_dict)

    # Testing properties
    assert table.columns_and_foreign_keys == strategy.all_columns
    assert table.has_primary_key == strategy.has_primary_key
    assert table.prefix_column_map == strategy.prefix_column_map
    # Testing __contains__
    assert all(col in table for col in strategy.all_columns)
    # Testing get_common_columns
    assert set(table.get_common_columns(strategy.all_columns)) == set(
        strategy.all_columns
    )
    n = len(strategy.all_columns)  # to test if subset also works
    assert set(table.get_common_columns(strategy.all_columns[: n - 1])) == set(
        strategy.all_columns[: n - 1]
    )
    # Testing get_prefixed_columns
    assert (
        table.get_prefixed_columns(strategy.all_columns)
        == strategy.prefixed_columns_without
    )
    assert (
        table.get_prefixed_columns(list(strategy.prefix_column_map.keys()))
        == strategy.prefixed_columns_with
    )


# --- Testing Reference


def test_basic_reference() -> None:
    """Basic test of Reference dataclass."""
    ref = {
        "from_table": "from",
        "from_key": "to_id",
        "to_table": "to",
        "to_key": "id",
    }
    reference = Reference(**ref)
    assert str(reference) == "ON from.to_id = to.id"


def test_self_reference() -> None:
    """Test whether a self reference raises the correct exception."""
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
            "foreign_keys": ["dealer_id", "merk_id"],
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
            "foreign_keys": ["voertuig_id", "merk_dealer_id"],
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
            "from_key": "merk_dealer_id",
            "to_table": "merk_dealer",
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

    # --- Testing schema.column_table_mapping

    # first inverting the mapping from tables
    mapping = {}
    for d in tables:
        for col in d["columns"]:
            if not col in mapping:
                mapping[col] = [d["name"]]
            else:
                mapping[col] += [d["name"]]

    assert schema._column_table_mapping == mapping

    # --- Testing schema._get_table_by_column

    # test if exception is raised when prefixed column does not exist for table when prefixed
    with pytest.raises(NoSuchColumnForTableError):
        schema._get_table_name_by_column("eigenaar.fiets")
    # test if exception is raised when non-prefixed column does not exist in schema
    with pytest.raises(NoSuchColumnInSchemaError):
        schema._get_table_name_by_column("fiets")
    # test if exception is raised when non-prefixed column is ambiguous
    with pytest.raises(ColumnIsAmbiguousError):
        schema._get_table_name_by_column("naam")

    # testing happy path
    columns_and_table_names = [
        ("eigenaar.naam", "eigenaar"),
        ("datum", "aankoop"),
    ]

    for column, table_name in columns_and_table_names:
        assert schema._get_table_name_by_column(column) == table_name

    # --- Testing schema._get_table_prefix_map

    prefixes_and_columns = [
        ("eigenaar", ["eigenaar.naam"], {"eigenaar.naam": "naam"}),
        (
            "voertuig",
            ["voertuig.type_id", "merk_id"],
            {"voertuig.type_id": "type_id"},
        ),
    ]

    for table, columns, mapping in prefixes_and_columns:
        assert schema._get_table_prefix_map(table, columns) == mapping

    # --- Testing schema.parse_insert

    # test if exception is raised for empty list of columns
    with pytest.raises(EmptyColumnListError):
        schema.parse_insert("eigenaar", [])
    # test if exception is raised for nonexisting columns
    with pytest.raises(ColumnsDoNotExistOnTableError):
        schema.parse_insert("eigenaar", ["fiets", "trein"])

    test_insert = [
        # format: table_name, columns
        ("eigenaar", ["naam"]),
        ("merk", ["naam"]),
        ("voertuig_type", ["naam"]),
        ("dealer", ["naam"]),
        ("voertuig", ["type_id", "merk_id"]),
        ("merk_dealer", ["merk_id", "dealer_id"]),
        ("voertuig_eigenaar", ["eigenaar_id", "voertuig_id"]),
        ("aankoop", ["voertuig_id", "merk_dealer_id", "datum"]),
    ]

    for test in test_insert:
        test_table, test_columns = test
        assert set(schema.parse_insert(test_table, test_columns)) == set(
            test_columns
        )

    # ---- Testing schema.parse_retrieve

    # test if exception if raised for empty list of columns
    with pytest.raises(EmptyColumnListError):
        schema.parse_retrieve("eigenaar", "eigenaar_id", [])
    # test if exception is raised for nonexisting columns
    with pytest.raises(ColumnsDoNotExistOnTableError):
        schema.parse_retrieve("eigenaar", "eigenaar_id", ["fiets", "trein"])
    # test if exception is raised when trying to retrieve from a table without a primary key
    with pytest.raises(NoPrimaryKeyError):
        schema.parse_retrieve("aankoop", "aankoop_id", ["datum"])
        # # test if exception is raised for nonexisting foreign key alias
    with pytest.raises(AliasDoesNotExistError):
        schema.parse_retrieve("eigenaar", "fiets_id", ["naam"])

    test_retrieve = [
        # format: table, alias, columns
        ("eigenaar", "eigenaar_id", ["naam"], "id"),
        ("merk", "merk_id", ["naam"], "id"),
        ("dealer", "dealer_id", ["naam"], "id"),
        ("voertuig_type", "type_id", ["naam"], "id"),
        ("voertuig", "voertuig_id", ["type_id", "merk_id"], "id"),
    ]

    for test in test_retrieve:
        test_table, test_alias, test_columns, test_key = test
        out_key, out_columns = schema.parse_retrieve(
            test_table, test_alias, test_columns
        )
        assert test_key == out_key
        assert set(out_columns) == set(test_columns)

    # ---- Testing schema.get_load_instructions
    instruction_columns = [
        "eigenaar.naam",
        "voertuig_type.naam",
        "merk.naam",
        "dealer.naam",
        "datum",
    ]

    insert_and_retrieve_dicts = [
        {
            "table": "eigenaar",
            "alias": "eigenaar_id",
            "columns": {"eigenaar.naam": "naam"},
        },
        {"table": "merk", "alias": "merk_id", "columns": {"merk.naam": "naam"}},
        {
            "table": "voertuig_type",
            "alias": "type_id",
            "columns": {"voertuig_type.naam": "naam"},
        },
        {
            "table": "dealer",
            "alias": "dealer_id",
            "columns": {"dealer.naam": "naam"},
        },
        {"table": "voertuig", "alias": "voertuig_id", "columns": {}},
        {"table": "merk_dealer", "alias": "merk_dealer_id", "columns": {}},
    ]
    insert_dicts = [
        {"table": "voertuig_eigenaar", "columns": {}},
        {"table": "aankoop", "columns": {}},
    ]

    load_instructions = schema.get_load_instructions(instruction_columns)
    assert load_instructions.insert_and_retrieve == insert_and_retrieve_dicts
    assert load_instructions.insert == insert_dicts

    # ---- Testing schema.get_compare_query
    columns = [
        "voertuig_type.naam",
        "eigenaar.naam",
        "merk.naam",
        "dealer.naam",
        "datum",
    ]
    compare_query = """SELECT\naankoop.datum as 'datum',\ndealer.naam as 'dealer.naam',\neigenaar.naam as 'eigenaar.naam',\nmerk.naam as 'merk.naam',\nvoertuig_type.naam as 'voertuig_type.naam'\nFROM eigenaar\nLEFT JOIN voertuig_eigenaar ON voertuig_eigenaar.eigenaar_id = eigenaar.id\nLEFT JOIN voertuig ON voertuig_eigenaar.voertuig_id = voertuig.id\nLEFT JOIN voertuig_type ON voertuig.type_id = voertuig_type.id\nLEFT JOIN merk ON voertuig.merk_id = merk.id\nLEFT JOIN merk_dealer ON merk_dealer.merk_id = merk.id\nLEFT JOIN dealer ON merk_dealer.dealer_id = dealer.id\nLEFT JOIN aankoop ON aankoop.merk_dealer_id = merk_dealer.id"""

    assert schema.get_compare_query(columns) == compare_query
