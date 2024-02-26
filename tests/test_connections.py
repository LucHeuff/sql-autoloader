from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from etl_components.connections import (
    PostgresCursor,
    RollbackCausedError,
    SQLiteCursor,
)

SQLITE_FILE = "test.db"


def test_sqlite_cursor() -> None:
    """Test cursor.

    - create new table
    - populate table
    - select from table
    - compare with what was inserted

    """
    create = """
    CREATE TABLE test (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    insert = """
    INSERT INTO test (name) VALUES (:name)
    ON CONFLICT DO NOTHING
    """
    select = "SELECT name FROM test"
    data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Alice"]})
    with SQLiteCursor() as cursor:
        cursor.execute(create)
        cursor.executemany(insert, data.to_dict("records"))
        cursor.execute(select)
        db_data = pd.DataFrame(cursor.fetchall())

        np.testing.assert_array_equal(data.name.unique(), db_data.name.unique())


def test_sqlite_cursor_exception() -> None:
    """Test whether cursor correctly rolls back on exception."""
    create = """
    CREATE TABLE test (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    insert = """
    INSERT INTO test (id, name) VALUES (:id, :name)
    ON CONFLICT DO NOTHING 
    """
    select = "SELECT * FROM test"
    pass_data = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    )
    fail_data = pd.DataFrame(
        {"id": [4, 5, 6], "name": ["Doug", "Eva", "Felix"]}
    )

    # first adding the pass data
    with SQLiteCursor(SQLITE_FILE) as cursor:
        cursor.execute(create)
        cursor.executemany(insert, pass_data.to_dict("records"))
    # trying to add fail data but running into an error
    with pytest.raises(RollbackCausedError), SQLiteCursor(
        SQLITE_FILE
    ) as cursor:
        cursor.executemany(insert, fail_data.to_dict("records"))
        raise ValueError("An error occurs.")

    with SQLiteCursor(SQLITE_FILE) as cursor:
        cursor.execute(select)
        db_data = pd.DataFrame(cursor.fetchall())

        pd.testing.assert_frame_equal(pass_data, db_data)


def test_postgres_cursor() -> None:
    """Test cursor.

    - create new table
    - populate table
    - select from table
    - compare with what was inserted

    """
    create = """
    CREATE TABLE test (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    insert = """
    INSERT INTO test (name) VALUES (%(name)s)
    ON CONFLICT DO NOTHING
    """
    select = "SELECT name FROM test"
    data = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Alice"]})
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create)
        cursor.executemany(insert, data.to_dict("records"))
        cursor.execute(select)
        db_data = pd.DataFrame(cursor.fetchall())

        np.testing.assert_array_equal(data.name.unique(), db_data.name.unique())


def test_postgres_cursor_exception() -> None:
    """Test whether cursor correctly rolls back on exception."""
    create = """
    CREATE TABLE test (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    insert = """
    INSERT INTO test (id, name) VALUES (%(id)s, %(name)s)
    ON CONFLICT DO NOTHING
    """
    select = "SELECT * FROM test"
    pass_data = pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    )
    fail_data = pd.DataFrame(
        {"id": [4, 5, 6], "name": ["Doug", "Eva", "Felix"]}
    )

    # first adding the pass data
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create)
        cursor.executemany(insert, pass_data.to_dict("records"))
    # trying to add fail data but running into an error
    with pytest.raises(RollbackCausedError), PostgresCursor() as cursor:
        cursor.executemany(insert, fail_data.to_dict("records"))
        raise ValueError("An error occurs.")
    # checking if rollback was correctly performed
    with PostgresCursor() as cursor:
        cursor.execute(select)
        db_data = pd.DataFrame(cursor.fetchall())

        pd.testing.assert_frame_equal(pass_data, db_data)


def setup() -> None:
    """Remove database stuff before tests have run."""
    sqlite_db = Path(SQLITE_FILE)
    if sqlite_db.is_file():
        sqlite_db.unlink()
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")


def teardown() -> None:
    """Remove database stuff after tests have run."""
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
    Path(SQLITE_FILE).unlink()
