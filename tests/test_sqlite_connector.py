from tempfile import NamedTemporaryFile

import polars as pl
from polars.testing import assert_frame_equal

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
    key = "id"
    alias = "fiets_id"
    columns = ["kleur", "zadel", "wielen"]
    query = "SELECT id as fiets_id, kleur, zadel, wielen FROM fiets"
    assert _get_retrieve_query(table, key, alias, columns) == query


def test_integration() -> None:
    """Test if SQLiteConnetor works in integration setting."""
    schema = """
    CREATE TABLE eigenaar (
      id INTEGER PRIMARY KEY,
      naam TEXT UNIQUE NOT NULL
    );

    CREATE TABLE merk (id INTEGER PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE voertuig_type (id INTEGER PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE dealer (
      id INTEGER PRIMARY KEY,
      naam TEXT UNIQUE NOT NULL
    );

    CREATE TABLE voertuig (
      id INTEGER PRIMARY KEY,
      type_id INTEGER REFERENCES voertuig_type (id),
      merk_id INTEGER REFERENCES merk (id),
      UNIQUE (type_id, merk_id)
    );

    CREATE TABLE merk_dealer (
      id INTEGER PRIMARY KEY,
      merk_id INTEGER REFERENCES merk (id),
      dealer_id INTEGER REFERENCES dealer (id),
      UNIQUE (merk_id, dealer_id)
    );

    CREATE TABLE voertuig_eigenaar (
      eigenaar_id INTEGER REFERENCES eigenaar (id),
      voertuig_id INTEGER REFERENCES voertuig (id),
      UNIQUE (voertuig_id, eigenaar_id)
    );

    CREATE TABLE aankoop (
      voertuig_id INTEGER REFERENCES voertuig (id),
      dealer_id INTEGER REFERENCES dealer (id),
      datum TEXT,
      UNIQUE (voertuig_id, dealer_id, datum)
    );
    """

    compare_query = """
    SELECT
      eigenaar.naam as eigenaar,
      voertuig_type.naam as type,
      merk.naam as merk,
      dealer.naam as dealer,
      aankoop.datum as aankoop
    FROM
      eigenaar
      LEFT JOIN voertuig_eigenaar ON voertuig_eigenaar.eigenaar_id = eigenaar.id
      LEFT JOIN voertuig ON voertuig_eigenaar.voertuig_id = voertuig.id
      LEFT JOIN voertuig_type ON voertuig.type_id = voertuig_type.id
      LEFT JOIN merk ON voertuig.merk_id = merk.id
      LEFT JOIN merk_dealer ON merk_dealer.merk_id = merk.id
      LEFT JOIN dealer ON merk_dealer.dealer_id = dealer.id
      LEFT JOIN aankoop ON aankoop.voertuig_id = voertuig.id;
    """
    data = pl.DataFrame(
        {
            "eigenaar": ["Luc", "Dave", "Erwin", "Erwin"],
            "soort_voertuig": ["fiets", "auto", "auto", "motor"],
            "merk": ["Batavus", "Renault", "Toyota", "Kawasaki"],
            "dealer": [
                None,
                "Zoest Occasions",
                "Zoest Occasions",
                "Berts Tweewielers",
            ],
            "aankoop": [None, "2021-06-25", "2022-10-13", "2020-02-03"],
        }
    )

    # testing against a temporary file instead of in memory, since
    # real use probably won't be in memory either.
    with NamedTemporaryFile(suffix=".db") as file:
        with SQLiteConnector(file.name) as sqlite:
            with sqlite.cursor() as cursor:
                cursor.executescript(schema)

            sqlite.update_schema()
            sqlite.load(
                data,
                compare_query,
                columns={"soort_voertuig": "type"},
            )

        # Testing if the data were saved to the file as well
        with SQLiteConnector(file.name) as sqlite:  # noqa: SIM117
            with sqlite.cursor() as cursor:
                cursor.execute(compare_query)
                db_data = pl.DataFrame(cursor.fetchall())

        assert_frame_equal(
            data.rename({"soort_voertuig": "type"}),
            db_data,
            check_row_order=False,
            check_column_order=False,
        )
