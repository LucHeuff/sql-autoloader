from collections import Counter
from dataclasses import dataclass
from tempfile import NamedTemporaryFile

import hypothesis.strategies as st
import networkx as nx
import polars as pl
from hypothesis import given
from more_itertools import batched
from polars.testing import assert_frame_equal

from sql_autoloader.sqlite_connector import (
    SQLiteConnector,
    _get_insert_query,
    _get_retrieve_query,
)
from tests.generators import dag_generator, names_generator


def test_get_insert_query() -> None:
    """Test whether _get_insert_query() works as intended."""
    table = "fiets"
    columns = ["kleur", "zadel", "wielen"]
    query = "INSERT OR IGNORE INTO fiets (kleur, zadel, wielen) VALUES (:kleur, :zadel, :wielen)"
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


def test_basic_integration() -> None:
    """Basic test of whether SQLiteConnetor works in integration setting."""
    schema = """
    CREATE TABLE eigenaar (id INTEGER PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE merk (id INTEGER PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE voertuig_type (id INTEGER PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

    CREATE TABLE dealer (id INTEGER PRIMARY KEY, naam TEXT UNIQUE NOT NULL);

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

    # testing against a temporary file instead of in memory, since
    # real use probably won't be in memory either.
    with NamedTemporaryFile(suffix=".db") as file:
        with SQLiteConnector(file.name) as sqlite:
            sqlite.cursor.executescript(schema)

            sqlite.update_schema()
            sqlite.load(data, columns=columns)

        # Testing if the data were saved to the file as well
        with SQLiteConnector(file.name) as sqlite:
            sqlite.cursor.execute(compare_query)
            db_data = pl.DataFrame(sqlite.cursor.fetchall())

        assert_frame_equal(
            data.rename({"soort_voertuig": "type"}),
            db_data,
            check_row_order=False,
            check_column_order=False,
        )


@dataclass
class SQLiteReference:
    """Stores a SQLite reference."""

    foreign_key: str
    to_table: str
    to_key: str

    def __str__(self) -> str:
        """Get SQL representation of reference."""
        return f"{self.foreign_key} INTEGER REFERENCES {self.to_table} ({self.to_key})"


@dataclass
class SQLiteTable:
    """Stores a SQLite table."""

    name: str
    primary_key: str
    columns: list[str]
    references: list[SQLiteReference]

    def __str__(self) -> str:
        """Get SQL representation of table."""
        cols = [str(ref) for ref in self.references] + [
            f"{col} TEXT" for col in self.columns
        ]
        if self.primary_key != "":
            cols = [f"{self.primary_key} INTEGER PRIMARY KEY", *cols]

        contents = ", ".join(cols)
        return f"CREATE TABLE {self.name} ({contents});"


@dataclass
class IntegrationStrategy:
    """Container for output of sqlite_integration_strategy()."""

    schema: str
    df: pl.DataFrame
    no_isolates: bool


@st.composite
def integration_strategy(draw: st.DrawFn) -> IntegrationStrategy:
    """Simulate input for testing SQLiteConnetor."""
    no_isolates = draw(st.booleans())
    table_names = draw(names_generator(min_size=3, max_size=7))
    # Generating a random DAG
    graph = draw(dag_generator(table_names, no_isolates=no_isolates))

    # Generating the tables
    tables: dict[str, SQLiteTable] = {}
    columns = []

    order = nx.topological_sort(graph)

    for table in order:
        successors = list(graph.successors(table))
        predecessors = list(graph.predecessors(table))

        primary_key = ""
        references = []

        # Generating all columns in one go to make sure they are unique
        # (and speed up the randomisation process)

        # need 1 primary key if the table has successors
        pk = len(successors) > 0

        # making sure there is at least one column in the table if there is no primary key
        num_cols = max(1, pk + 1)

        sample_columns = draw(
            names_generator(min_size=num_cols, max_size=num_cols + 2)
        )

        # preventing generating SQL keywords by accident
        sample_columns = [f"_{col}" for col in sample_columns]

        if pk:
            # peel off the first column as the primary key
            primary_key, sample_columns = (
                sample_columns[:1][0],
                sample_columns[1:],
            )

        if len(predecessors) > 0:
            references = [
                SQLiteReference(f"{pred}_id", pred, tables[pred].primary_key)
                for pred in predecessors
            ]

        tables[table] = SQLiteTable(
            table, primary_key, sample_columns, references
        )
        # update list of columns to see if names are duplicate in the schema
        columns += sample_columns

    schema = "\n\n".join(str(table) for table in tables.values())

    # Generating the dataframe
    column_frequency = Counter(columns)
    column_names = [
        f"{table.name}.{col}" if column_frequency[col] > 1 else col
        for table in tables.values()
        for col in table.columns
    ]
    n_rows = draw(st.integers(3, 5))
    # generating integers for all columns, to make sure every row is unique
    n = len(column_names)
    assert n > 0, "No columns were generated in simulation process."
    values = [str(val) for val in range(n * n_rows)]
    rows = [dict(zip(column_names, _values)) for _values in batched(values, n)]

    df = pl.DataFrame(rows)

    return IntegrationStrategy(schema, df, no_isolates)


@given(strategy=integration_strategy())
def test_integration(strategy: IntegrationStrategy) -> None:
    """Simulation test of SQLiteConnector."""
    with NamedTemporaryFile(suffix=".db") as file:  # noqa: SIM117
        with SQLiteConnector(file.name) as sqlite:
            sqlite.cursor.executescript(strategy.schema)
            sqlite.update_schema()

            # If there are no isolates, can test compare query generation
            if strategy.no_isolates:
                sqlite.load(strategy.df, compare=True)
                compare_query = sqlite.schema.get_compare_query(
                    strategy.df.columns
                )
                sqlite.cursor.execute(compare_query)
                db_data = pl.DataFrame(sqlite.cursor.fetchall())

                assert_frame_equal(
                    strategy.df,
                    db_data,
                    check_row_order=False,
                    check_column_order=False,
                )

            # If there are isolates, just test if loading runs
            else:
                sqlite.load(strategy.df, compare=False)
