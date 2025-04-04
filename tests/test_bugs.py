"""These are all tests from bugs that appeared in production use."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sql_autoloader import SQLiteConnector
from sql_autoloader.exceptions import (
    ColumnIsAmbiguousError,
    CompareNoExactMatchError,
    TableDoesNotExistError,
)


def test_empty_predecessors_bug() -> None:
    """Test bug with empty predecessors.

    # A bug occurred when a table with no predecessors was not part of the tables
    # being loaded. This caused that table to be erroneously added to the loading
    # instructions, which was of course not possible to load, failing the whole
    # operation.
    #
    """
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


def test_duplicate_with_missings_bug() -> None:
    """Test if datasets with partial missings get loaded and compared correctly."""
    schema = """
    CREATE TABLE a (
        id INTEGER PRIMARY KEY,
        a INTEGER UNIQUE NOT NULL
    );

    CREATE TABLE b (
        a_id INTEGER REFERENCES a (id),
        b INTEGER UNIQUE NOT NULL
    );

    CREATE TABLE c (
        a_id INTEGER REFERENCES a (id),
        c TEXT UNIQUE NOT NULL
    );
    """
    data = pl.DataFrame({"a": [1, 1], "b": [2, 2], "c": [None, "a"]})

    with SQLiteConnector(":memory:") as sqlite:
        sqlite.cursor.executescript(schema)
        sqlite.update_schema()

        with pytest.raises(CompareNoExactMatchError):
            sqlite.load(data)

        sqlite.load(data, exact=False)


def test_schema_bug() -> None:
    """Test error when misspecifying reference tables."""
    schema = """
    CREATE TABLE a (
        id INTEGER PRIMARY KEY,
        a TEXT
    );

    CREATE TABLE b (
        a_id INTEGER REFERENCES aa (id),
        b TEXT
    );

    """
    with SQLiteConnector(":memory:") as sqlite:
        sqlite.cursor.executescript(schema)
        with pytest.raises(TableDoesNotExistError):
            sqlite.update_schema()


def test_ambiguous_bug() -> None:
    """Test error where ambiguousness check is tripped unnecessarily."""
    schema = """
    CREATE TABLE a (
        id INTEGER PRIMARY KEY,
        a TEXT
    );

    CREATE TABLE b (
        a_id INTEGER REFERENCES a (id),
        b TEXT
    );

    CREATE TABLE c (
        a1_id INTEGER REFERENCES a (id),
        a2_id INTEGER REFERENCES a (id),
        c TEXT
    );
    """
    data = pl.DataFrame({"a": ["one", "one"], "b": ["one", "two"]})

    with SQLiteConnector(":memory:") as sqlite:
        sqlite.cursor.executescript(schema)
        sqlite.update_schema()
        sqlite.load(data)


def test_retrieve_drop_bug() -> None:
    """Test if retrieve_ids tries to drop columns that aren't there."""
    schema = """
    CREATE TABLE a (
        id INTEGER PRIMARY KEY,
        a TEXT UNIQUE,
        extra TEXT
    );

    CREATE TABLE b (
        a_id INTEGER REFERENCES a (id),
        b TEXT
    );
    """
    data = pl.DataFrame(
        {
            "a": ["one", "two", "three"],
            "b": ["een", "twee", "drie"],
            "extra": ["this", "is", "fun"],
        }
    )
    retrieve_data = pl.DataFrame({"a": ["one", "two"]})

    with SQLiteConnector(":memory:") as sqlite:
        sqlite.cursor.executescript(schema)
        sqlite.update_schema()
        sqlite.load(data)

        sqlite.retrieve_ids(retrieve_data, table="a", alias="a_id")


def test_load_ids_bug() -> None:
    """Test if load() can also handle loading into foreign key columns."""
    schema = """
    CREATE TABLE a (id INTEGER PRIMARY KEY, a TEXT UNIQUE NOT NULL);

    CREATE TABLE b (
        id INTEGER PRIMARY KEY,
        a_id INTEGER REFERENCES a (id),
        b TEXT UNIQUE NOT NULL
    );

    CREATE TABLE c (
        a_id INTEGER REFERENCES a (id),
        b_id INTEGER REFERENCES b (id),
        c TEXT UNIQUE NOT NULL
    );

    """
    data = pl.DataFrame(
        {"a_id": [1, 2, 3], "b_id": [1, 2, 3], "c": ["one", "two", "three"]}
    )

    with SQLiteConnector(":memory:") as sqlite:
        sqlite.cursor.executescript(schema)
        sqlite.update_schema()

        with pytest.raises(ColumnIsAmbiguousError):
            sqlite.load(data)

        sqlite.load(data, columns={"a_id": "c.a_id"})
