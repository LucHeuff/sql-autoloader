import polars as pl

from etl_components.sqlite_connector import (
    SQLiteConnector,
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


def test_integration() -> None:
    """Test if SQLiteConnetor works in integration setting."""
    schema = """
    CREATE TABLE IF NOT EXISTS kleur (
        id INTEGER PRIMARY KEY,
        kleur TEXT UNIQUE
    );


    CREATE TABLE IF NOT EXISTS eigenaar (
        id INTEGER PRIMARY KEY,
        eigenaar TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS voertuig_type (
        id INTEGER PRIMARY KEY,
        type TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS voertuig (
        id INTEGER PRIMARY KEY,
        voertuig_type_id INT REFERENCES voertuig_type (id),
        kleur_id INT REFERENCES kleur (id),
        UNIQUE (voertuig_type_id, kleur_id)
    );

    CREATE TABLE IF NOT EXISTS voertuig_eigenaar (
        voertuig_id INT REFERENCES voertuig (id),
        eigenaar_id INT REFERENCES eigenaar (id),
        sinds TEXT,
        UNIQUE (voertuig_id, eigenaar_id)
    );
    """
    data = pl.DataFrame(
        {
            "eigenaar": ["Dave", "Luc", "Erwin", "Erwin"],
            "soort_voertuig": ["auto", "fiets", "auto", "motor"],
            "kleur": ["rood", "blauw", "zilver", "rood"],
            "sinds": ["2022-01-18", "2019-03-23", "2021-03-05", "2018-03-05"],
        }
    )

    with SQLiteConnector(":memory:") as sqlite:
        with sqlite.cursor() as cursor:
            cursor.executescript(schema)  # type: ignore

        sqlite.update_schema()
        sqlite.load(data, columns={"soort_voertuig": "type"})
