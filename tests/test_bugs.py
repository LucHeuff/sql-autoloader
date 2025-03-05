"""These are all tests from bugs that appeared in production use."""

import polars as pl
from polars.testing import assert_frame_equal

from sql_autoloader import SQLiteConnector

# Based on a realistic schema
schema = """

CREATE TABLE stof (
    id INTEGER PRIMARY KEY,
    ec_number TEXT UNIQUE NOT NULL
);

CREATE TABLE stof_cas_number (
    stof_id INTEGER REFERENCES stof (id) ON DELETE CASCADE,
    cas_number TEXT NOT NULL,
    UNIQUE (stof_id, cas_number)
);

CREATE TABLE stof_naam (
    stof_id INTEGER REFERENCES stof (id) ON DELETE CASCADE,
    substance_name TEXT NOT NULL,
    UNIQUE (stof_id, substance_name)
);

CREATE TABLE stof_nl_naam (
    stof_id INTEGER REFERENCES stof (id) ON DELETE CASCADE,
    stof_naam TEXT NOT NULL,
    UNIQUE (stof_id, stof_naam)
);

CREATE TABLE stoffenlijst (
    id INTEGER PRIMARY KEY,
    stoffenlijst TEXT UNIQUE NOT NULL
);

CREATE TABLE stof_op_lijst (
    stof_id INTEGER REFERENCES stof (id) ON DELETE CASCADE,
    stoffenlijst_id INTEGER REFERENCES stoffenlijst (id) ON DELETE CASCADE,
    UNIQUE (stof_id, stoffenlijst_id)
);

CREATE TABLE adres (
    id INTEGER PRIMARY KEY,
    straatnaam TEXT NOT NULL,
    postcode TEXT NOT NULL,
    plaats TEXT NOT NULL,
    UNIQUE (straatnaam, postcode, plaats)
);

CREATE TABLE hoofddossier (
    id INTEGER PRIMARY KEY,
    stof_id INTEGER REFERENCES stof (id) ON DELETE CASCADE,
    dossier_nummer TEXT UNIQUE NOT NULL,
    joint_submission INTEGER NOT NULL CHECK (joint_submission IN (0, 1))
);

CREATE TABLE dossier (
    id INTEGER PRIMARY KEY,
    hoofddossier_id INTEGER REFERENCES hoofddossier (id) ON DELETE CASCADE,
    adres_id INTEGER REFERENCES adres (id) ON DELETE CASCADE,
    reference_number TEXT UNIQUE NOT NULL
);

"""


def test_stoffen() -> None:
    """Test bug with empty predecessors.

    A bug occurred when a table with no predecessors was not part of the tables
    being loaded. This caused that table to be erroneously added to the loading
    instructions, which was of course not possible to load, failing the whole
    operation.
    """
    data = pl.DataFrame(
        {
            "ec_number": ["1", "2", "3", "4", "4"],
            "cas_number": ["A", "B", None, None, None],
            "substance_name": ["c", None, "d", None, None],
        }
    )

    query = """
    SELECT ec_number, cas_number, substance_name
    FROM stof
        LEFT JOIN stof_cas_number ON stof_cas_number.stof_id = stof.id
        LEFT JOIN stof_naam ON stof_naam.stof_id = stof.id
    """

    with SQLiteConnector(":memory:") as sqlite:
        sqlite.cursor.executescript(schema)
        sqlite.update_schema()

        sqlite.load(data)

        # Testing whether the data can be retrieved correctly
        # (Technically load already performs this test in compare)
        sqlite.cursor.execute(query)
        db_data = pl.DataFrame(sqlite.cursor.fetchall())

        assert_frame_equal(
            data.unique(), db_data, check_row_order=False, check_column_order=False
        )
