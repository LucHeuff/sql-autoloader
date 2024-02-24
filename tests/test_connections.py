import numpy as np
import pandas as pd
import pytest

from postgres_etl_components.connections import (
    PostgresCursor,
    RollbackCausedError,
)


def test_cursor() -> None:
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


def test_cursor_exception() -> None:
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

    with PostgresCursor() as cursor:
        cursor.execute(select)
        db_data = pd.DataFrame(cursor.fetchall())

        pd.testing.assert_frame_equal(pass_data, db_data)


def setup() -> None:
    """Remove everything in test database."""
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")


def teardown() -> None:
    """Remove everything in test database."""
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
