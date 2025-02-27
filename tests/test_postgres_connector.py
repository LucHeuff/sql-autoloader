import polars as pl
from polars.testing import assert_frame_equal

from sql_autoloader.postgres import (
    PostgresConnector,
)
from sql_autoloader.postgres.postgres_connector import (
    _get_insert_query,
    _get_retrieve_query,
)

CONNECT_STRING = "postgresql://test:test@localhost/test"


def test_get_insert_query() -> None:
    """Test whether _get_insert_query() works as intended."""
    table = "fiets"
    columns = ["kleur", "zadel", "wielen"]
    query = "INSERT INTO fiets (kleur, zadel, wielen) VALUES (%(kleur)s, %(zadel)s, %(wielen)s) ON CONFLICT DO NOTHING"  # noqa: E501
    assert _get_insert_query(table, columns) == query


def test_get_retrieve_query() -> None:
    """Test whether _get_retrieve_query() works as intended."""
    table = "fiets"
    key = "id"
    alias = "fiets_id"
    columns = ["kleur", "zadel", "wielen"]
    query = "SELECT id as fiets_id, kleur, zadel, wielen FROM fiets"
    assert _get_retrieve_query(table, key, alias, columns) == query


# --- Integration tests

# NOTE: This will fail when Postgres is not installed on the current machine.


def test_basic_integration() -> None:
    """Basic test of whether PostgreSQLConnector works in integration setting."""
    schema = """
    DROP OWNED BY test;

    CREATE TABLE eigenaar (id SERIAL PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE merk (id SERIAL PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE voertuig_type (id SERIAL PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE dealer (id SERIAL PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE voertuig (
      id SERIAL PRIMARY KEY,
      type_id INTEGER REFERENCES voertuig_type (id),
      merk_id INTEGER REFERENCES merk (id),
      UNIQUE (type_id, merk_id)
    );

    CREATE TABLE merk_dealer (
      id SERIAL PRIMARY KEY,
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
      merk_dealer_id INTEGER REFERENCES merk_dealer (id),
      datum TEXT,
      UNIQUE (voertuig_id, merk_dealer_id, datum)
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
                "Kees Klappertand",
                "Zoest Occasions",
                "Zoest Occasions",
                "Berts Tweewielers",
            ],
            "aankoop": ["2024-03-01", "2021-06-25", "2022-10-13", "2020-02-03"],
        }
    )

    columns = {
        "eigenaar": "eigenaar.naam",
        "soort_voertuig": "voertuig_type.naam",
        "merk": "merk.naam",
        "dealer": "dealer.naam",
        "aankoop": "datum",
    }

    with PostgresConnector(CONNECT_STRING) as postgres:
        postgres.cursor.execute(schema)
        postgres.update_schema()
        postgres.load(data, columns=columns)

        postgres.cursor.execute(compare_query)
        db_data = pl.DataFrame(postgres.cursor.fetchall())

        assert_frame_equal(
            data.rename({"soort_voertuig": "type"}),
            db_data,
            check_row_order=False,
            check_column_order=False,
        )
