# SQL Autoloader

Contains automation of loading data into a SQL-like database, built around [`polars`](https://pola.rs/).

Also includes convience functions for semi-manually inserting and retrieving data.
Currently, `SQLite` (through `sqlite3`) and `PostgreSQL` (through optional extra `psycopg`) are supported.

# Installation

## `pip`
Install with `pip` using:

```
pip install git+https://github.com/LucHeuff/sql-autoloader.git
```
To add the `psycopg` extra use:

```
pip install gi+https://github.com/LucHeuff/sql-autoloader.git#egg=sql-autoloader[postgres]
```

# How does it work?

Often, loading data into a SQL database consists of two basic steps that are repeated many times:

1. `INSERT`ing data into a table
2. `RETRIEVE`ing the primary keys the database associated with the inserted data.

If done manually, this requires either writing repetitive `INSERT` and `RETRIEVE` queries by hand, 
or writing some object-oriented replica of your database schema using an ORM and letting that handle 
the loading steps (but this requires recording the database schema in two separate places).

Both of these work fine as long as the database schema does not change, but become labourious to edit
once the schema does change.

`sql-autoloader` simplifies this process by being aware of the database schema, and automatically generating 
the required `INSERT` and `RETRIEVE` SQL queries based on the data that you are trying to load. 
`sql-autoloader` does this by trying to match column names in the data with column names in the database schema,
and also figures out the order in which to load such that consistency across referencing tables is maintained.

This does mean that `sql-autoloader` needs to make assumptions.

## Assumptions
- Primary and foreign keys should never be empty.
    During the `RETRIEVE` step, primary keys from the database are joined to the original data.
    This allows `sql-autoloader` to properly link primary and foreign keys.
    This does mean that every foreign key reference should end up with a primary key.
    In other words, these values cannot be missing. `sql-autoloader` will perform a check that this is the case.
    > ℹ️  If this check fails, this usually points to a design assumption being violated. This might mean your
    > data is incorrect, or your assumptions about the data are.
- The database schema is defined prior to loading.
    `sql-autoloader` reads the schema from the database, and tries to match this with the data you want to load.
    That means the schema must be already be defined at the time of loading. 
- There are no loops in the database schema.
    Internally, the schema is assumed to form a Directed Acyclic Graph, meaning that there are no cycles of tables
    that reference each other in a loop.
- Foreign keys are named consistently.
    As far as I am aware, SQL does not require foreign keys referring to the same primary key in another table to have the same name.
    However, this makes algorithmically figuring out the order in which tables should be loaded much more difficult,
    so `sql-autoloader` requires all foreign keys that refer to the same primary key to have the same name. 

> ℹ️ `sql-autoloader` will automatically raise exceptions if these assumptions are not met.

## Validation
By default, `sql-autoloader` will try to validate the loading operation by retrieving all the data it loaded
and comparing that to the original data provided by the user. If these data do not match, all changes to the database are rolled back.
Automatically generating the comparison query comes with an additional assumption: 

-  All tables on which data is loaded are connected.
    This means there exists a single query consisting of multiple `JOIN` statements that reconstruct the original data.
    That also means there can be no isolated tables, or sets of isolated tables in the loading operation.

> ℹ️ This doesn't mean that all your tables need to be connected, this only needs to hold for the tables into which data are loaded.

If your loading operation does match this assumption, you can either provide your own comparison query (through the `compare_query=` argument)
or disable the validation entirely (by setting `compare=False`).

The automatically generated query can also be restricted by adding a `WHERE`-clause (through the `where=` argument),
or relaxed by setting `exact=False`, meaning that instead of having to exactly match, the original data only needs to appear in the data retrieved from the database.

# How do I use it?

`sql-autoloader` provides a context manager for each supported database. For example:

SQLite:
```
from sql_autoloader import SQLiteConnector

credentials = '<path_to_file>.db'

with SQLiteConnector(credentials) as sqlite:
   sqlite.load(data) 

```

Postgres:
```
from sql_autoloader.postgres import PostgresConnector

credentials = 'postgresql://<username>:<password>@<host>:<port>/<db_name>'

with PostgresConnector(credentials) as postgres:
    postgres.load(data)

```

The context manager handles opening and closing the connection, and will roll back any changes on the database if an error occurs.
In addition, it will also create a cursor and expose it (e.g. `sqlite.cursor` exposes a `sqlite3.Cursor`, `postgres.cursor` exposes a `psycopg.Cursor`) 
in case you need access to the cursor directly.

For example, this can be useful when you want to create the database schema from within Python:

```
from sql_autoloader import SQLiteConnector

schema = """
CREATE TABLE demo (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
...
"""
credentials = '<path_to_file>.db'

with SQLiteConnector(credentials) as sqlite:
   sqlite.cursor.executescript(schema) 
   sqlite.update_schema() # The schema changed since when the context manager was created, so we need to update

   sqlite.load(data) 
```

> ⚠️ All connectors assume `data` to be a `polars.DataFrame`. If you are coming from `pandas` instead,
> you can easily convert your `pandas.DataFrame` to a `polars.DataFrame` using:
> ```
> import polars as pl
> polars_df = pl.from_pandas(pandas_df)
> ```

# Documentation

## Connector
All `Connector`s have the following methods:

### `load`
This is the main intended way for `sql-autoloader` to be used, which tries to automatically load the provided data
```
load(
    data: pl.DataFrame,
    columns: dict[str, str] | None = None,
    compare: bool = True,
    compare_query: str | None = None,
    replace: bool = True,
    allow_duplication: bool = False,
    where: str | None = None,
    exact: bool = True,
) -> pl.DataFrame
```
`data`: a `polars.DataFrame` containing data to be loaded into the database\
`columns` (Optional): Translation of columns in the data to the relevant column names in the database. If the same column name appears on multiple tables in the database, prefix the column with the desired table using the format `\<table\>.\<column_name\>`\
`compare` (Optional): whether comparison needs to be performed\
`compare_query` (Optional): allows you to provide a custom comparison query for data validation. This is ignored when `compare=False`\
`replace` (Optional): Whether columns can be replaced with the relevant foreign keys upon retrieval. If set to `False`, all columns are preserved\
`allow_duplication` (Optional): Whether rows are allowed to be duplicated upon retrieval\
`where` (Optional): allows adding a `WHERE`-clause to be added onto the comparison query. Please prefix the column you are conditioning on with its relevant table, otherwise this condition may result in SQL errors\
`exact` (Optional): whether the rows in the data retrieved through the comparison query must match `data` exactly. If set to `False`, will only check if the rows from `data` appear in the retrieved data\

This function will return the original data including the foreign keys (where original columns were replaced depending on `replace`), in case you want to use these downstream.

If for some reason `load()` does not produce the desired results, the loop can be constructed manually using the
`insert()`, `retrieve_ids()` or `insert_and_retrieve_ids()` methods.

### `insert`
This method inserts provided data into a single table. This can be used manually if `load()` is not working as desired.
```
insert(
    data: pl.DataFrame,
    table: str,
    columns: dict[str, str] | None = None,
) -> None:
```
`data`: a `polars.DataFrame` containing data to be loaded into the table\
`table`: the table that the data should be loaded into\
`columns` (Optional): Translation of columns in the data to the relevant column names in the table, in the format {<data_name>: <db_name>}\

As insertion is an operation on the database only, this method does not return anything.

> ℹ️ any columns that are present in `data` that are not relevant to `table` are simply ignored.

### `retrieve_ids`
This methods retrieves primary keys from a single table and joins them to the provided data under the given alias.
```
retrieve_ids(
    data: pl.DataFrame,
    table: str,
    alias: str,
    columns: dict[str, str] | None = None,
    replace: bool = True,
    allow_duplication: bool = False,
) -> pl.DataFrame:
```
`data`: a `polars.DataFrame` containing data to which the keys should be joined\
`table`: the table from which the primary keys should be retrieved\
`alias`: the alias under which the primary key should be retrieved. Usually this is the name of the foreign key in some other table, referring to this table\
`columns` (Optional): Translation of columns in the data to the relevant column names in the table, in the format {<data_name>: <db_name>}\
`replace` (Optional): Whether columns can be replaced with the relevant foreign keys upon retrieval. If set to `False`, all columns are preserved\
`allow_duplication` (Optional): Whether rows are allowed to be duplicated upon retrieval\ 

This method will return a dataframe onto which the primary keys of `table` were joined, under the provided `alias`.

> ℹ️ any columns that are present in `data` that are not relevant to `table` are simply ignored.

### `insert_and_retrieve_ids`
This is a convenience method that chains `insert()` and `retrieve_ids()` for the same table.
```
insert_and_retrieve_ids(
    data: pl.DataFrame,
    table: str,
    alias: str,
    columns: dict[str, str] | None = None,
    replace: bool = True,
    allow_duplication: bool = False,
) -> pl.DataFrame:
```
*For parameter and output specification refer to `insert()` and `retrieve_ids()` above*

### `compare`
This method performs comparison between the provided data and data fetched from the database using a provided query
```
compare(
    data: pl.DataFrame,
    query: str | None = None,
    columns: dict[str, str] | None = None,
    where: str | None = None,
    exact: bool = True,
) -> None:
```
`data`: a `polars.DataFrame` containing data against which should be compared\
`query` (Optional): a `SELECT` query to be run against the database, to fetch data that should be compared to `data`\
    If left empty, the method will attempt to generate a comparison query automatically\
`columns` (Optional): Translation of columns in the data to the relevant column names in the table, in the format {<data_name>: <db_name>}\
`where` (Optional): a `WHERE` clause to filter selection from the database. Should always use table prefixes for the columns being conditioned on\
                     Mostly intended when `query` is left empty, otherwise you could just bundle it there as well\
`exact` (Optional): whether the rows in the data retrieved through the comparison query must match `data` exactly. If set to `False`, will only check if the rows from `data` appear in the retrieved data\

### `update_schema`
The database schema is retrieved whenever the `*Connector` context manager is created.
However, you may wish to create or adjust the database schema from within the context manager itself, at which point the
schema in the database and the schema in the `*Connector` are out of sync.
`update_schema()` allows you to update the schema in the `*Connector`.

For example:
```
from sql_autoloader import SQLiteConnector

schema = "<some valid SQL schema>"

# creating a context manager on a new database file, so it is empty at the start
with SQLiteConnector("new.db") as sqlite:
    # at this point, the schema representation is empty
    sqlite.cursor.executescript(schema)
    # schema inside the SQLite database is now updated, but the schema representation is still empty
    sqlite.update_schema() # update the schema representation as well.
    
```

### `print_schema`
`print_schema()` is a convenience function to show a list of tables and the names of columns that the `*Connector` knows.
This is not intended as a replacement of the full SQL schema, but instead as a reference to quickly check if everything is in working order,
or if you don't have access to the full SQL schema for some reason.

For example:
```
with SQLiteConnector(credentials) as sqlite:
    sqlite.print_schema()
```
> ⚠️ This information is incomplete, as the `*Connector` is not aware of table and column constraints, or default values.

### `schema_is_empty`
`schema_is_empty()` is a convenience function to check whether a schema exists in the database.
This function makes checking whether database tables should be loaded a little bit easier.

For example:
```
with SQLiteConnector(credentials) as sqlite:
    # only creating tables if none exist yet
    if sqlite.schema_is_empty():
        sqlite.cursor.executescript(schema)
        sqlite.update_schema()

```

## SQLiteConnector
The `SQLiteConnector` wraps the `sql-autoloader` functionality around the `sqlite3` library.
```
SQLiteConnector(
    credentials: str,
    allow_custom_dtypes: bool = False
)
```
`credentials`: path to a `sqlite` database, or ':memory:' for a SQLite database existing only in memory\
`allow_custom_dtypes` (Optional): enables custom datatypes, and can be used in combination with custom adapters and converters. For more information see the [sqlite3 documentation](https://docs.python.org/3/library/sqlite3.html#sqlite3-adapter-converter-recipes)

The `SQLiteConnector.cursor` property exposes the `sqlite3.Cursor` used internally for manual use. See the [sqlite3 documentation on Cursors](https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor) for more information.
> ⚠️ `SQLiteConnector` assumes that the cursor will be closed once the context manager exits. Closing the cursor prematurely will cause issues.

## PostgresConnector
The `PostgresConnector` wraps the `sql-autoloader` functionality around the `psycopg` library.
```
PostgresConnector(
    credentials: str
)
```
`credentials`: a [connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) to a running PostgreSQL server\

The `PostgresCursor.cursor` property exposes the `psycopg.Cursor` used internally for manual use. See the [psycopg documentation on Cursors](https://www.psycopg.org/psycopg3/docs/api/cursors.html) for more information.
> ⚠️ `PostgresConnector` assumes that the cursor will be closed once the context manager exits. Closing the cursor prematurely will cause issues.

## Troubleshooting
Since the `load()` operation has a lot of moving parts, troubleshooting can be difficult.
For that reason, the basic load operations write what they are trying to do, and the SQL query they are trying to execute
to the debugging logs.
These can be accessed using the builting `logging` module by setting the level to `logging.DEBUG`, for instance:
```
import logging
logging.getLogger("sql_autoloader").setLevel(level=logging.DEBUG) 
```

