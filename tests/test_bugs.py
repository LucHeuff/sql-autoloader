"""These are all tests from bugs that appeared in production use."""

import polars as pl
from polars.testing import assert_frame_equal

from sql_autoloader import SQLiteConnector


def test_empty_predecessors() -> None:
    """Test bug with empty predecessors.

    A bug occurred when a table with no predecessors was not part of the tables
    being loaded. This caused that table to be erroneously added to the loading
    instructions, which was of course not possible to load, failing the whole
    operation.
    """
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

    CREATE TABLE stoffenlijst (
        id INTEGER PRIMARY KEY,
        stoffenlijst TEXT UNIQUE NOT NULL
    );

    CREATE TABLE stof_op_lijst (
        stof_id INTEGER REFERENCES stof (id) ON DELETE CASCADE,
        stoffenlijst_id INTEGER REFERENCES stoffenlijst (id) ON DELETE CASCADE,
        UNIQUE (stof_id, stoffenlijst_id)
    );
    """

    data = pl.DataFrame(
        {
            "ec_number": ["1", "2", "3"],
            "cas_number": ["A", "B", None],
            "substance_name": ["c", None, "d"],
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
            data, db_data, check_row_order=False, check_column_order=False
        )
