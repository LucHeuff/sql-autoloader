# ruff: noqa: SLF001
import pytest

from etl_components.schema import (
    Column,
    ColumnError,
    Schema,
    SchemaError,
    Table,
)


def test_column() -> None:
    """Testing Column class and methods."""
    primary = {
        "table": "fiets",
        "name": "id",
        "dtype": "INT",
        "primary_key": True,
        "nullable": False,
        "unique": True,
        "default_value": None,
    }
    foreign = {
        "table": "fiets",
        "name": "fabriek_id",
        "dtype": "INT",
        "primary_key": False,
        "nullable": False,
        "unique": True,
        "default_value": None,
        "foreign_key": True,
        "to_table": "fabriek",
        "to_column": "id",
        "on_delete": "CASCADE",
    }
    not_nullable = {
        "table": "fiets",
        "name": "merk",
        "dtype": "TEXT",
        "primary_key": False,
        "nullable": False,
        "unique": True,
        "default_value": None,
    }
    nullable = {
        "table": "fiets",
        "name": "verzekering",
        "dtype": "TEXT",
        "primary_key": False,
        "nullable": True,
        "unique": False,
        "default_value": None,
    }
    default = {
        "table": "fiets",
        "name": "wielen",
        "dtype": "INT",
        "primary_key": False,
        "nullable": False,
        "unique": False,
        "default_value": 2,
    }

    primary_column = Column(**primary)
    foreign_column = Column(**foreign)
    not_nullable_column = Column(**not_nullable)
    nullable_column = Column(**nullable)
    default_column = Column(**default)

    assert str(primary_column) == "id INT PRIMARY KEY"
    assert (
        str(foreign_column)
        == "fabriek_id INT REFERENCES fabriek (id) ON DELETE CASCADE"
    )
    assert str(not_nullable_column) == "merk TEXT UNIQUE NOT NULL"
    assert str(nullable_column) == "verzekering TEXT"
    assert str(default_column) == "wielen INT DEFAULT 2"

    # testing get_reference
    assert (
        foreign_column.get_reference()
        == "JOIN fabriek ON fiets.fabriek_id = fabriek.id"
    )


def test_column_exceptions() -> None:
    """Test if Column throws the appropriate exceptions."""
    primary_and_foreign = {
        "table": "fiets",
        "name": "id",
        "dtype": "INT",
        "primary_key": True,
        "nullable": False,
        "unique": True,
        "default_value": None,
        "foreign_key": True,
    }
    foreign_no_reference = {
        "table": "fiets",
        "name": "id",
        "dtype": "INT",
        "primary_key": False,
        "nullable": False,
        "unique": True,
        "default_value": None,
        "foreign_key": True,
    }
    no_reference = {
        "table": "fiets",
        "name": "wielen",
        "dtype": "INT",
        "primary_key": False,
        "nullable": False,
        "unique": False,
        "default_value": 2,
    }
    with pytest.raises(ColumnError):
        Column(**primary_and_foreign)

    with pytest.raises(ColumnError):
        Column(**foreign_no_reference)

    column = Column(**no_reference)
    with pytest.raises(ColumnError):
        column.get_reference()


def test_table() -> None:
    """Test if Table functions work as intended."""
    name = "fiets"
    sql = "CREATE TABLE fiets (\n\tid INT PRIMARY KEY,\n\tfabriek_id INT REFERENCES fabriek (id) ON DELETE CASCADE,\n\tname TEXT UNIQUE NOT NULL,\n\twheels INT DEFAULT 2,\n\tUNIQUE (name, wheels)\n)"
    columns = [
        Column(
            name,
            "id",
            "INT",
            primary_key=True,
            nullable=False,
            unique=False,
            default_value=None,
        ),
        Column(
            name,
            "fabriek_id",
            "INT",
            primary_key=False,
            nullable=False,
            unique=False,
            default_value=None,
            foreign_key=True,
            to_table="fabriek",
            to_column="id",
            on_delete="CASCADE",
        ),
        Column(
            name,
            "name",
            "TEXT",
            primary_key=False,
            nullable=False,
            unique=True,
            default_value=None,
        ),
        Column(
            name,
            "wheels",
            "INT",
            primary_key=False,
            nullable=False,
            unique=False,
            default_value="2",
        ),
    ]
    constraint = ["name", "wheels"]

    table = Table(name, columns, constraint)

    assert table.column_names == ["id", "fabriek_id", "name", "wheels"]
    assert table.has_primary_key == True
    assert table.has_foreign_key == True
    assert table.get_non_id_column_names() == ["name", "wheels"]
    assert str(table) == sql


def test_schema() -> None:
    """Test of schema behaves as expected."""

    def get_tables() -> list[tuple[str, list[str]]]:
        return [
            ("kleur", []),
            ("eigenaar", []),
            ("voertuig_type", []),
            ("voertuig", ["voertuig_type_id", "kleur_id"]),
            ("voertuig_eigenaar", ["voertuig_id", "eigenaar_id"]),
        ]

    def get_columns(table_name: str) -> list[dict]:
        columns_map = {
            "kleur": [
                {
                    "name": "id",
                    "dtype": "INTEGER",
                    "primary_key": True,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
                {
                    "name": "kleur",
                    "dtype": "TEXT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
            ],
            "eigenaar": [
                {
                    "name": "id",
                    "dtype": "INTEGER",
                    "primary_key": True,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
                {
                    "name": "eigenaar",
                    "dtype": "TEXT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
            ],
            "voertuig_type": [
                {
                    "name": "id",
                    "dtype": "INTEGER",
                    "primary_key": True,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
                {
                    "name": "type",
                    "dtype": "TEXT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
            ],
            "voertuig": [
                {
                    "name": "id",
                    "dtype": "INTEGER",
                    "primary_key": True,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
                {
                    "name": "voertuig_type_id",
                    "dtype": "INT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                    "foreign_key": True,
                    "to_table": "voertuig_type",
                    "to_column": "id",
                    "on_delete": "CASCADE",
                },
                {
                    "name": "kleur_id",
                    "dtype": "INT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                    "foreign_key": True,
                    "to_table": "kleur",
                    "to_column": "id",
                    "on_delete": "CASCADE",
                },
            ],
            "voertuig_eigenaar": [
                {
                    "name": "voertuig_id",
                    "dtype": "INT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                    "foreign_key": True,
                    "to_table": "voertuig",
                    "to_column": "id",
                    "on_delete": "CASCADE",
                },
                {
                    "name": "eigenaar_id",
                    "dtype": "INT",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                    "foreign_key": True,
                    "to_table": "eigenaar",
                    "to_column": "id",
                    "on_delete": "CASCADE",
                },
                {
                    "name": "sinds",
                    "dtype": "date",
                    "primary_key": False,
                    "nullable": False,
                    "unique": True,
                    "default_value": None,
                },
            ],
        }
        return columns_map[table_name]

    def get_refers_to(table_name: str) -> list[str]:
        refers_to = {
            "kleur": [],
            "eigenaar": [],
            "voertuig_type": [],
            "voertuig": ["voertuig_type", "kleur"],
            "voertuig_eigenaar": ["voertuig", "eigenaar"],
        }
        return refers_to[table_name]

    def get_referred_by(table_name: str) -> list[str]:
        referred_by = {
            "kleur": ["voertuig"],
            "eigenaar": ["voertuig_eigenaar"],
            "voertuig_type": ["voertuig"],
            "voertuig": ["voertuig_eigenaar"],
            "voertuig_eigenaar": [],
        }
        return referred_by[table_name]

    schema = Schema(get_tables, get_columns)

    # ---- Testing basic properties of Schema
    tables = [
        "kleur",
        "eigenaar",
        "voertuig_type",
        "voertuig",
        "voertuig_eigenaar",
    ]
    column_table_mapping = {
        "kleur": ["kleur"],
        "eigenaar_id": ["voertuig_eigenaar"],
        "kleur_id": ["voertuig"],
        "voertuig_id": ["voertuig_eigenaar"],
        "id": ["kleur", "eigenaar", "voertuig_type", "voertuig"],
        "sinds": ["voertuig_eigenaar"],
        "eigenaar": ["eigenaar"],
        "type": ["voertuig_type"],
        "voertuig_type_id": ["voertuig"],
    }
    table_rank = {
        "kleur": 0,
        "eigenaar": 0,
        "voertuig_type": 0,
        "voertuig": 1,
        "voertuig_eigenaar": 2,
    }
    non_id_columns = {
        "kleur": ["kleur"],
        "eigenaar": ["eigenaar"],
        "voertuig_type": ["type"],
        "voertuig": [],
        "voertuig_eigenaar": ["sinds"],
    }

    assert schema.table_names == tables
    assert schema.column_name_to_table_mapping == {
        key: [schema(v) for v in value]
        for (key, value) in column_table_mapping.items()
    }

    for table in tables:
        assert schema(table).name == table
        assert schema.refers_to[table] == [
            schema(t) for t in get_refers_to(table)
        ]
        assert schema.referred_by[table] == [
            schema(t) for t in get_referred_by(table)
        ]
        assert schema._get_table_rank(schema(table)) == table_rank[table]
        assert schema.table_rank[table] == table_rank[table]
        assert schema.get_non_id_columns(table) == non_id_columns[table]

    # Schema should raise an error when the table does not exist
    with pytest.raises(SchemaError):
        schema("doos")

    # ---- Testing retrieving tables through columns
    assert schema._get_table_by_column("kleur") == schema("kleur")
    assert schema._get_table_by_column("eigenaar.eigenaar") == schema(
        "eigenaar"
    )
    assert schema._get_table_by_column("voertuig.kleur_id") == schema(
        "voertuig"
    )
    # schema should raise an error when the column doesn't exist
    with pytest.raises(SchemaError):
        schema._get_table_by_column("doos")
    # schema should raise an error when the column name appears in multiple tables
    with pytest.raises(SchemaError):
        schema._get_table_by_column("id")
    # schema should raise an error when column doesn't exist for prefixed table
    with pytest.raises(SchemaError):
        schema._get_table_by_column("voertuig_eigenaar.id")

    # ---- testing input parsing
    assert set(schema.parse_input("kleur", ["id", "kleur"])) == {
        "id",
        "kleur",
    }
    assert set(schema.parse_input("voertuig_eigenaar", ["sinds"])) == {"sinds"}
    # parse_input should raise an error when none of the columns exist for the table
    with pytest.raises(SchemaError):
        schema.parse_input("kleur", ["doos", "truck"])

    # ---- testing _get_unique_tables
    assert schema._get_unique_tables(
        [schema("kleur"), schema("kleur"), schema("eigenaar")]
    ) == [schema("kleur"), schema("eigenaar")]

    # ---- testing _get_tables_from_columns
    # passing all regular column names should result in all tables
    assert all(
        schema(table)
        in schema._get_tables_from_columns(
            ["eigenaar", "type", "kleur", "sinds"]
        )
        for table in tables
    )
    assert all(
        schema(table)
        in schema._get_tables_from_columns(
            ["eigenaar.eigenaar", "type", "kleur", "voertuig_eigenaar.sinds"]
        )
        for table in tables
    )
    # only "kleur" and "type" should result in tables kleur, voertuig and voertuig_type
    assert all(
        schema(table) in schema._get_tables_from_columns(["kleur", "type"])
        for table in ["kleur", "voertuig", "voertuig_type"]
    )
    # same as above, but prefixing the table should not make a difference
    assert all(
        schema(table)
        in schema._get_tables_from_columns(
            ["kleur.kleur", "voertuig_type.type"]
        )
        for table in ["kleur", "voertuig", "voertuig_type"]
    )
    # only "eigenaar" and "kleur" should result in tables eigenaar and kleur
    assert all(
        schema(table) in schema._get_tables_from_columns(["kleur", "eigenaar"])
        for table in ["kleur", "eigenaar"]
    )
    # should raise an error if stuff is left incomplete
    with pytest.raises(SchemaError):
        schema._get_tables_from_columns(["sinds"])
    with pytest.raises(SchemaError):
        schema._get_tables_from_columns(["voertuig_eigenaar.sinds"])
    with pytest.raises(SchemaError):
        schema._get_tables_from_columns(["voertuig_id", "eigenaar_id"])
    with pytest.raises(SchemaError):
        schema._get_tables_from_columns(["sinds", "voertuig_id", "eigenaar_id"])

    # ---- testing get_insert_and_retrieve_tables
    # testing partial result -> all tables have a primary key, so insert_and_retrieve
    insert_and_retrieve, insert = schema.get_insert_and_retrieve_tables(
        ["kleur", "type"]
    )
    assert insert_and_retrieve == ["kleur", "voertuig_type", "voertuig"]
    assert insert == []
    insert_and_retrieve, insert = schema.get_insert_and_retrieve_tables(
        ["kleur.kleur", "voertuig_type.type"]
    )
    assert insert_and_retrieve == ["kleur", "voertuig_type", "voertuig"]
    assert insert == []
    # testing complete result -> voertuig_eigenaar does not have a primary key
    insert_and_retrieve, insert = schema.get_insert_and_retrieve_tables(
        ["kleur", "type", "eigenaar", "sinds"]
    )
    assert insert_and_retrieve == [
        "kleur",
        "voertuig_type",
        "eigenaar",
        "voertuig",
    ]
    assert insert == ["voertuig_eigenaar"]
    insert_and_retrieve, insert = schema.get_insert_and_retrieve_tables(
        ["kleur.kleur", "type", "eigenaar.eigenaar", "voertuig_eigenaar.sinds"]
    )
    assert insert_and_retrieve == [
        "kleur",
        "voertuig_type",
        "eigenaar",
        "voertuig",
    ]
    assert insert == ["voertuig_eigenaar"]

    # ---- testing get_compare_query
    compare = "SELECT kleur.kleur, voertuig_type.type\nFROM voertuig\nJOIN voertuig_type ON voertuig.voertuig_type_id = voertuig_type.id\nJOIN kleur ON voertuig.kleur_id = kleur.id"
    compare_where = compare + "\nWHERE kleur = 'rood'"

    assert schema.get_compare_query(["kleur", "type"]) == compare
    assert (
        schema.get_compare_query(
            ["kleur", "type"], where="WHERE kleur = 'rood'"
        )
        == compare_where
    )
    assert (
        schema.get_compare_query(
            ["kleur.kleur", "voertuig_type.type"], where="WHERE kleur = 'rood'"
        )
        == compare_where
    )
