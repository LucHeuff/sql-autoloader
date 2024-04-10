import string
from datetime import date

import hypothesis.strategies as st
import numpy as np
import pandas as pd
import pytest
from hypothesis import given
from hypothesis.strategies import DrawFn, composite

from etl_components.connections import PostgresCursor, SQLiteCursor
from etl_components.interactions import (
    CopyNotAvailableError,
    compare,
    insert,
    insert_and_retrieve_ids,
)

# ---- Generators


@composite
def elements_generator(draw: DrawFn, size: int) -> list:
    """Generate list elements that can also be missing.

    Args:
    ----
        draw: hypothesis draw function
        size: number of elements

    Returns:
    -------
        list of elements

    """
    elements = draw(
        st.lists(
            st.text(alphabet=string.ascii_lowercase, min_size=4, max_size=10),
            min_size=size,
            max_size=2 * size,
            unique=True,
        )
    ) + [np.nan, None]
    return draw(
        st.lists(
            st.sampled_from(elements), min_size=size, max_size=size, unique=True
        )
    )


@composite
def datetime_generator(draw: DrawFn, size: int) -> list[str]:
    """Generate a list of date strings that can also be missing.

    Args:
    ----
        draw: hypothesis draw function
        size: number of elements

    Returns:
    -------
        list of dates


    """
    elements = [
        date.isoformat()
        for date in draw(
            st.lists(
                st.dates(
                    min_value=date(1900, 1, 1), max_value=date(2100, 1, 1)
                ),
                min_size=size,
                max_size=2 * size,
                unique=True,
            )
        )
    ] + [
        np.nan,
        None,
    ]
    return draw(
        st.lists(
            st.sampled_from(elements), min_size=size, max_size=size, unique=True
        )
    )


@composite
def dataframe_generator(draw: DrawFn, rows: int = 5) -> pd.DataFrame:
    """Generate a random dataframe where elements can be missing.

    Args:
    ----
        draw: hypothesis draw function
        rows: number of rows in dataframe

    Returns:
    -------
        random dataframe

    """
    return pd.DataFrame(
        {
            "vehicle": draw(elements_generator(rows)),
            "colour": draw(elements_generator(rows)),
            "invented": draw(datetime_generator(rows)),
        }
    ).drop_duplicates()


@given(data=dataframe_generator(), exact=st.booleans())
def test_integration_sqlite(data: pd.DataFrame, *, exact: bool) -> None:
    """Integration test for sqlite cursor.

    Args:
    ----
        data: randomised dataset
        exact: whether compare check should be exact

    """
    create_vehicle = """
    CREATE TABLE vehicle (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        invented DATE
    )
    """
    create_colour = """
    CREATE TABLE colour (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    create_vehicle_colour = """
    CREATE TABLE vehicle_colour (
        vehicle_id INT REFERENCES vehicle (id) ON DELETE CASCADE,
        colour_id INT REFERENCES colour (id) ON DELETE CASCADE,
        UNIQUE (vehicle_id, colour_id)
    )
    """
    insert_vehicle = """
    INSERT OR IGNORE INTO vehicle (name, invented) VALUES (:vehicle, :invented)
    """
    retrieve_vehicle = (
        "SELECT id as vehicle_id, name as vehicle, invented FROM vehicle"
    )
    insert_colour = """
    INSERT OR IGNORE INTO colour (name) VALUES (:colour)
    """
    retrieve_colour = "SELECT id as colour_id, name as colour FROM colour"
    insert_vehicle_colour = """
    INSERT OR IGNORE INTO vehicle_colour (vehicle_id, colour_id) VALUES (:vehicle_id, :colour_id)
    """

    compare_query = """
    SELECT vehicle.name as vehicle, invented, colour.name  as colour FROM vehicle
    INNER JOIN vehicle_colour ON vehicle_colour.vehicle_id = vehicle.id
    JOIN colour ON vehicle_colour.colour_id = colour.id
    """

    orig_data = data.copy() if exact else data.sample(frac=0.5).copy()
    with SQLiteCursor(":memory:") as cursor:
        cursor.execute(create_vehicle)
        cursor.execute(create_colour)
        cursor.execute(create_vehicle_colour)
        data = insert_and_retrieve_ids(
            cursor, insert_vehicle, retrieve_vehicle, data
        )
        data = insert_and_retrieve_ids(
            cursor, insert_colour, retrieve_colour, data
        )
        insert(cursor, insert_vehicle_colour, data)
        insert(cursor, insert_vehicle_colour, data)  # repeat to test OR IGNORE
        compare(cursor, compare_query, orig_data, exact=exact)


@given(data=dataframe_generator())
def test_integration_sqlite_asserts(data: pd.DataFrame) -> None:
    """Integration test for sqlite cursor, checking whether pytest raises an assertion error when the data doesn't match.

    Args:
    ----
        data: randomised dataset
        exact: whether compare check should be exact

    """
    create_vehicle = """
    CREATE TABLE vehicle (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        invented DATE
    )
    """
    create_colour = """
    CREATE TABLE colour (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    create_vehicle_colour = """
    CREATE TABLE vehicle_colour (
        vehicle_id INT REFERENCES vehicle (id) ON DELETE CASCADE,
        colour_id INT REFERENCES colour (id) ON DELETE CASCADE,
        UNIQUE (vehicle_id, colour_id)
    )
    """
    insert_vehicle = """
    INSERT OR IGNORE INTO vehicle (name, invented) VALUES (:vehicle, :invented)
    """
    retrieve_vehicle = (
        "SELECT id as vehicle_id, name as vehicle, invented FROM vehicle"
    )
    insert_colour = """
    INSERT OR IGNORE INTO colour (name) VALUES (:colour)
    """
    retrieve_colour = "SELECT id as colour_id, name as colour FROM colour"
    insert_vehicle_colour = """
    INSERT OR IGNORE INTO vehicle_colour (vehicle_id, colour_id) VALUES (:vehicle_id, :colour_id)
    """

    compare_query = """
    SELECT vehicle.name as vehicle, invented, colour.name  as colour FROM vehicle
    INNER JOIN vehicle_colour ON vehicle_colour.vehicle_id = vehicle.id
    JOIN colour ON vehicle_colour.colour_id = colour.id
    """

    orig_data = data.copy()
    data = data.sample(frac=0.5)

    with SQLiteCursor(":memory:") as cursor:
        cursor.execute(create_vehicle)
        cursor.execute(create_colour)
        cursor.execute(create_vehicle_colour)
        data = insert_and_retrieve_ids(
            cursor, insert_vehicle, retrieve_vehicle, data
        )
        data = insert_and_retrieve_ids(
            cursor, insert_colour, retrieve_colour, data
        )
        insert(cursor, insert_vehicle_colour, data)
        insert(cursor, insert_vehicle_colour, data)  # repeat to test OR IGNORE
        with pytest.raises(AssertionError):
            compare(cursor, compare_query, orig_data, exact=False)


def test_integration_postgres() -> None:
    """Perform integration test using Postgres."""
    create_vehicle = """
    CREATE TABLE vehicle (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        invented DATE
    )
    """
    create_colour = """
    CREATE TABLE colour (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    create_vehicle_colour = """
    CREATE TABLE vehicle_colour (
        vehicle_id INT REFERENCES vehicle (id) ON DELETE CASCADE,
        colour_id INT REFERENCES colour (id) ON DELETE CASCADE,
        UNIQUE (vehicle_id, colour_id)
    )
    """
    insert_vehicle = """
    INSERT INTO vehicle (name, invented) VALUES (%(vehicle)s, %(invented)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_vehicle = (
        "SELECT id as vehicle_id, name as vehicle, invented FROM vehicle"
    )
    insert_colour = """
    INSERT INTO colour (name) VALUES (%(colour)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_colour = "SELECT id as colour_id, name as colour FROM colour"
    insert_vehicle_colour = """
    INSERT INTO vehicle_colour (vehicle_id, colour_id) VALUES (%(vehicle_id)s, %(colour_id)s)
    ON CONFLICT DO NOTHING
    """

    compare_query = """
    SELECT vehicle.name as vehicle, colour.name as colour, invented FROM vehicle
    INNER JOIN vehicle_colour ON vehicle_colour.vehicle_id = vehicle.id
    JOIN colour ON vehicle_colour.colour_id = colour.id
    """

    data = (
        pd.DataFrame(
            {
                "index": [0, 0, 0, 0, 0],
                "vehicle": ["bike", "boat", "train", "bike", np.nan],
                "invented": [
                    "1912-04-05",
                    "1900-03-07",
                    "1850-03-03",
                    "1912-04-05",
                    "1900-01-01",
                ],
                "colour": ["red", "yellow", "yellow", "blue", None],
            }
        )
        .assign(invented=lambda df: pd.to_datetime(df.invented))
        .set_index("index")
    )
    orig_data = data.copy()
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create_vehicle)
        cursor.execute(create_colour)
        cursor.execute(create_vehicle_colour)
        data = insert_and_retrieve_ids(
            cursor, insert_vehicle, retrieve_vehicle, data
        )

        data = insert_and_retrieve_ids(
            cursor, insert_colour, retrieve_colour, data
        )

        insert(cursor, insert_vehicle_colour, data)
        compare(cursor, compare_query, orig_data)


def test_integration_postgres_not_exact() -> None:
    """Perform integration test using Postgres."""
    create_vehicle = """
    CREATE TABLE vehicle (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        invented DATE
    )
    """
    create_colour = """
    CREATE TABLE colour (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE
    )
    """
    create_vehicle_colour = """
    CREATE TABLE vehicle_colour (
        vehicle_id INT REFERENCES vehicle (id) ON DELETE CASCADE,
        colour_id INT REFERENCES colour (id) ON DELETE CASCADE,
        UNIQUE (vehicle_id, colour_id)
    )
    """
    insert_vehicle = """
    INSERT INTO vehicle (name, invented) VALUES (%(vehicle)s, %(invented)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_vehicle = (
        "SELECT id as vehicle_id, name as vehicle, invented FROM vehicle"
    )
    insert_colour = """
    INSERT INTO colour (name) VALUES (%(colour)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_colour = "SELECT id as colour_id, name as colour FROM colour"
    insert_vehicle_colour = """
    INSERT INTO vehicle_colour (vehicle_id, colour_id) VALUES (%(vehicle_id)s, %(colour_id)s)
    ON CONFLICT DO NOTHING
    """

    compare_query = """
    SELECT vehicle.name as vehicle, colour.name as colour, invented FROM vehicle
    INNER JOIN vehicle_colour ON vehicle_colour.vehicle_id = vehicle.id
    JOIN colour ON vehicle_colour.colour_id = colour.id
    """

    data = (
        pd.DataFrame(
            {
                "index": [0, 0, 0, 0, 0],
                "vehicle": ["bike", "boat", "train", "bike", np.nan],
                "invented": [
                    "1912-04-05",
                    "1900-03-07",
                    "1850-03-03",
                    "1912-04-05",
                    "1900-01-01",
                ],
                "colour": ["red", "yellow", "yellow", "blue", None],
            }
        )
        .assign(invented=lambda df: pd.to_datetime(df.invented))
        .set_index("index")
    )
    orig_data = data.sample(frac=0.5).copy()
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create_vehicle)
        cursor.execute(create_colour)
        cursor.execute(create_vehicle_colour)
        data = insert_and_retrieve_ids(
            cursor, insert_vehicle, retrieve_vehicle, data
        )

        data = insert_and_retrieve_ids(
            cursor, insert_colour, retrieve_colour, data
        )

        insert(cursor, insert_vehicle_colour, data)
        compare(cursor, compare_query, orig_data, exact=False)


def test_sqlite_copy_raises() -> None:
    """Test whether calling a SQLiteCursor with use_copy raises an exception."""
    create_vliegtuig = """
    CREATE TABLE vliegtuig (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    insert_vliegtuig = "INSERT INTO vliegtuig (naam) VALUES (:vliegtuig)"
    data = pd.DataFrame(
        {
            "vliegtuig": ["Boeing", "Airbus", "Bombardier", "Embraer"],
        }
    )

    with pytest.raises(CopyNotAvailableError):  # noqa: SIM117
        with SQLiteCursor(":memory:") as cursor:
            cursor.execute(create_vliegtuig)
            insert(cursor, insert_vliegtuig, data, use_copy=True)


def test_sqlite_insert_and_retrieve_copy_raises() -> None:
    """Test whether calling a SQLiteCursor with use_copy raises an exception."""
    create_vliegtuig = """
    CREATE TABLE vliegtuig (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    insert_vliegtuig = "INSERT INTO vliegtuig (naam) VALUES (:vliegtuig)"
    retrieve_vliegtuig = (
        "SELECT id as vliegtuig_id, naam as vliegtuig FROM vliegtuig"
    )
    data = pd.DataFrame(
        {
            "vliegtuig": ["Boeing", "Airbus", "Bombardier", "Embraer"],
        }
    )

    with pytest.raises(CopyNotAvailableError):  # noqa: SIM117
        with SQLiteCursor(":memory:") as cursor:
            cursor.execute(create_vliegtuig)
            _ = insert_and_retrieve_ids(
                cursor,
                insert_vliegtuig,
                retrieve_vliegtuig,
                data,
                use_copy=True,
            )


def test_postgres_copy() -> None:
    """Test whether inserting with copy works as intended."""
    create_vliegtuig = """
    CREATE TABLE vliegtuig (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    insert_vliegtuig = """INSERT INTO vliegtuig (naam) VALUES (%(vliegtuig)s)
    ON CONFLICT DO NOTHING
    """
    data = pd.DataFrame(
        {
            "vliegtuig": ["Boeing", "Airbus", "Bombardier", "Embraer"],
        }
    )

    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create_vliegtuig)
        insert(cursor, insert_vliegtuig, data, use_copy=True)
        cursor.execute("SELECT naam as vliegtuig FROM vliegtuig")
        test = pd.DataFrame(cursor.fetchall())

        pd.testing.assert_frame_equal(data, test, check_like=True)


def test_postgres_insert_and_retrieve_copy() -> None:
    """Test whether inserting with copy works as intended."""
    create_vliegmachine = """
    CREATE TABLE vliegmachine (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    insert_vliegmachine = """INSERT INTO vliegmachine (naam) VALUES (%(vliegtuig)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_vliegmachine = (
        "SELECT id as vliegmachine_id, naam as vliegtuig FROM vliegmachine"
    )
    data = pd.DataFrame(
        {
            "vliegtuig": ["Boeing", "Airbus", "Bombardier", "Embraer"],
        }
    )
    compare_data = data.assign(vliegmachine_id=[1, 2, 3, 4])

    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create_vliegmachine)
        test = insert_and_retrieve_ids(
            cursor,
            insert_vliegmachine,
            retrieve_vliegmachine,
            data,
            replace=False,
            use_copy=True,
        )
        cursor.execute("SELECT naam as vliegtuig FROM vliegmachine")

        pd.testing.assert_frame_equal(compare_data, test, check_like=True)


def test_postgres_datetime() -> None:
    """Test whether inserting date formats is problematic."""
    create_activity = """
    CREATE TABLE activity (
        id SERIAL PRIMARY KEY,
        name TEXT
    )
    """
    create_schedule = """
    CREATE TABLE schedule (
        id SERIAL PRIMARY KEY,
        activity_id INT REFERENCES activity (id),
        date DATE
    )
    """
    insert_activity = """
    INSERT INTO activity (name) VALUES (%(activity)s)"""
    retrieve_activity = (
        "SELECT id as activity_id, name as activity FROM activity"
    )

    insert_schedule = """
    INSERT INTO schedule (date, activity_id) VALUES (%(date)s, %(activity_id)s)"""
    compare_schedule = """
    SELECT activity.name as activity, schedule.date as date
    FROM activity
        JOIN schedule ON schedule.activity_id = activity.id
    """
    data = pd.DataFrame(
        {
            "date": ["2023-05-03", "2024-09-05", "2052-06-02"],
            "activity": ["Build LEGO", "Eat ice cream", "Watch stars"],
        }
    ).assign(date=lambda df: pd.to_datetime(df.date))

    orig_data = data.copy()
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
        cursor.execute(create_activity)
        cursor.execute(create_schedule)
        data = insert_and_retrieve_ids(
            cursor, insert_activity, retrieve_activity, data
        )
        insert(cursor, insert_schedule, data)

        compare(cursor, compare_schedule, orig_data)


def setup() -> None:
    """Remove database stuff before tests have run."""
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")


def teardown() -> None:
    """Remove database stuff after tests have run."""
    with PostgresCursor() as cursor:
        cursor.execute("DROP owned by test_user")
