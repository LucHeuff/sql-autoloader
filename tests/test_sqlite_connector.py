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
    CREATE TABLE colour (
        id INTEGER PRIMARY KEY,
        colour TEXT UNIQUE
    );

    CREATE TABLE vehicle (
        id INTEGER PRIMARY KEY,
        vehicle TEXT UNIQUE,
        production_date date
    );

    CREATE TABLE vehicle_colour (
        colour_id INTEGER REFERENCES colour (id),
        vehicle_id INTEGER REFERENCES vehicle (id),
        UNIQUE(colour_id, vehicle_id)
    );
    """

    compare_query = """
    SELECT vehicle, colour, production_date
    FROM vehicle
        JOIN vehicle_colour ON vehicle_colour.vehicle_id = vehicle.id
        JOIN colour ON vehicle_colour.colour_id = colour.id
    """
    data = pl.DataFrame(
        {
            "vehicle": ["Car", "Bike", "Bike ", "Train"],
            "date": [
                "2000-01-01",
                "2000-02-02",
                "2000-03-03",
                "2000-04-04",
            ],
            "colour": ["Red", "Green", "Black", "Yellow"],
        },
        schema_overrides={"date": pl.Date},
    )

    with SQLiteConnector(":memory:") as sqlite:
        with sqlite.cursor() as cursor:
            cursor.executescript(schema)  # type: ignore

        sqlite.update_schema()
        sqlite.print_schema()

        orig_data = data.clone()

        data = sqlite.insert_and_retrieve_ids(data, "colour")
        data = sqlite.insert_and_retrieve_ids(
            data, "vehicle", columns={"date": "production_date"}, replace=False
        )
        sqlite.insert(data, "vehicle_colour")
        sqlite.compare(
            orig_data,
            compare_query,
            columns={"date": "production_date"},
            exact=True,
        )
