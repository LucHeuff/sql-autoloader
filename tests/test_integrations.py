import pandas as pd
import pytest

from etl_components.connections import PostgresCursor, SQLiteCursor
from etl_components.interactions import (
    CopyNotAvailableError,
    compare,
    insert,
    insert_and_retrieve_ids,
)

DATA = pd.DataFrame(
    {
        "voertuig": ["fiets", "boot", "trein", "fiets"],
        "kleur": ["rood", "geel", "geel", "blauw"],
    }
)


def test_integration_sqlite() -> None:
    """Perform integration test using SQLite."""
    create_voertuig = """
    CREATE TABLE voertuig (
        id INTEGER PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    create_kleur = """
    CREATE TABLE kleur (
        id INTEGER PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    create_voertuig_kleur = """
    CREATE TABLE voertuig_kleur (
        voertuig_id INT REFERENCES voertuig (id) ON DELETE CASCADE,
        kleur_id INT REFERENCES kleur (id) ON DELETE CASCADE,
        UNIQUE (voertuig_id, kleur_id)
    )
    """
    insert_voertuig = """
    INSERT INTO voertuig (naam) VALUES (:voertuig)
    ON CONFLICT DO NOTHING
    """
    retrieve_voertuig = (
        "SELECT id as voertuig_id, naam as voertuig FROM voertuig"
    )
    insert_kleur = """
    INSERT INTO kleur (naam) VALUES (:kleur)
    ON CONFLICT DO NOTHING
    """
    retrieve_kleur = "SELECT id as kleur_id, naam as kleur FROM kleur"
    insert_voertuig_kleur = """
    INSERT INTO voertuig_kleur (voertuig_id, kleur_id) VALUES (:voertuig_id, :kleur_id)
    ON CONFLICT DO NOTHING
    """

    compare_query = """
    SELECT voertuig.naam as voertuig, kleur.naam as kleur FROM voertuig
    JOIN voertuig_kleur ON voertuig_kleur.voertuig_id = voertuig.id
    JOIN kleur ON voertuig_kleur.kleur_id = kleur.id
    """

    data = pd.DataFrame(
        {
            "index": [0, 0, 0, 0],
            "voertuig": ["fiets", "boot", "trein", "fiets"],
            "kleur": ["rood", "geel", "geel", "blauw"],
        }
    ).set_index("index")
    orig_data = data.copy()
    with SQLiteCursor(":memory:") as cursor:
        cursor.execute(create_voertuig)
        cursor.execute(create_kleur)
        cursor.execute(create_voertuig_kleur)
        data = insert_and_retrieve_ids(
            cursor, insert_voertuig, retrieve_voertuig, data
        )
        data = insert_and_retrieve_ids(
            cursor, insert_kleur, retrieve_kleur, data
        )
        insert(cursor, insert_voertuig_kleur, data)
        compare(cursor, compare_query, orig_data)


def test_integration_postgres() -> None:
    """Perform integration test using Postgres."""
    create_voertuig = """
    CREATE TABLE voertuig (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    create_kleur = """
    CREATE TABLE kleur (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    create_voertuig_kleur = """
    CREATE TABLE voertuig_kleur (
        voertuig_id INT REFERENCES voertuig (id) ON DELETE CASCADE,
        kleur_id INT REFERENCES kleur (id) ON DELETE CASCADE,
        UNIQUE (voertuig_id, kleur_id)
    )
    """
    insert_voertuig = """
    INSERT INTO voertuig (naam) VALUES (%(voertuig)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_voertuig = (
        "SELECT id as voertuig_id, naam as voertuig FROM voertuig"
    )
    insert_kleur = """
    INSERT INTO kleur (naam) VALUES (%(kleur)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_kleur = "SELECT id as kleur_id, naam as kleur FROM kleur"
    insert_voertuig_kleur = """
    INSERT INTO voertuig_kleur (voertuig_id, kleur_id) VALUES (%(voertuig_id)s, %(kleur_id)s)
    ON CONFLICT DO NOTHING
    """

    compare_query = """
    SELECT voertuig.naam as voertuig, kleur.naam as kleur FROM voertuig
    JOIN voertuig_kleur ON voertuig_kleur.voertuig_id = voertuig.id
    JOIN kleur ON voertuig_kleur.kleur_id = kleur.id
    """

    data = pd.DataFrame(
        {
            "index": [0, 0, 0, 0],
            "voertuig": ["fiets", "boot", "trein", "fiets"],
            "kleur": ["rood", "geel", "geel", "blauw"],
        }
    ).set_index("index")
    orig_data = data.copy()
    with PostgresCursor() as cursor:
        cursor.execute(create_voertuig)
        cursor.execute(create_kleur)
        cursor.execute(create_voertuig_kleur)
        data = insert_and_retrieve_ids(
            cursor, insert_voertuig, retrieve_voertuig, data
        )
        data = insert_and_retrieve_ids(
            cursor, insert_kleur, retrieve_kleur, data
        )
        insert(cursor, insert_voertuig_kleur, data)
        compare(cursor, compare_query, orig_data)


def test_sqlite_copy_raises() -> None:
    """Test whether calling a SQLiteCursor with use_copy raises an exception."""
    create_vliegtuig = """
    CREATE TABLE vliegtuig (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    insert_vliegtuig = """INSERT INTO vliegtuig (naam) VALUES (:vliegtuig)
    ON CONFLICT DO NOTHING
    """
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
    insert_vliegtuig = """INSERT INTO vliegtuig (naam) VALUES (:vliegtuig)
    ON CONFLICT DO NOTHING
    """
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
        cursor.execute(create_vliegtuig)
        insert(cursor, insert_vliegtuig, data, use_copy=True)
        cursor.execute("SELECT naam as vliegtuig FROM vliegtuig")
        test = pd.DataFrame(cursor.fetchall())

        pd.testing.assert_frame_equal(data, test, check_like=True)


def test_postgres_insert_and_retrieve_copy() -> None:
    """Test whether inserting with copy works as intended."""
    create_vliegtuig = """
    CREATE TABLE vliegmachine (
        id SERIAL PRIMARY KEY,
        naam TEXT UNIQUE
    )
    """
    insert_vliegtuig = """INSERT INTO vliegmachine (naam) VALUES (%(vliegtuig)s)
    ON CONFLICT DO NOTHING
    """
    retrieve_vliegtuig = (
        "SELECT id as vliegmachine_id, naam as vliegtuig FROM vliegmachine"
    )
    data = pd.DataFrame(
        {
            "vliegtuig": ["Boeing", "Airbus", "Bombardier", "Embraer"],
        }
    )
    compare_data = data.assign(vliegmachine_id=[1, 2, 3, 4])

    with PostgresCursor() as cursor:
        cursor.execute(create_vliegtuig)
        test = insert_and_retrieve_ids(
            cursor,
            insert_vliegtuig,
            retrieve_vliegtuig,
            data,
            replace=False,
            use_copy=True,
        )
        cursor.execute("SELECT naam as vliegtuig FROM vliegtuig")

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
    SELECT activity.name as activity, date 
    FROM activity
        JOIN schedule ON schedule.activity_id = activity.id
    """
    data = pd.DataFrame(
        {
            "date": ["2023-05-03", "2024-09-05", "2052-06-02"],
            "activity": ["Build LEGO", "Eat ice cream", "Watch stars"],
        }
    ).assign(date=lambda df: pd.to_datetime(df.date))

    with PostgresCursor() as cursor:
        orig_data = data.copy()
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
