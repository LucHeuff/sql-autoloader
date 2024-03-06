# ETL Components

Contains convenience functions for ETL components built on top of `pandas`, `psycopg` and `sqlite3`.

These are opinionated functions for inserting, retrieving and comparing data in to a SQL database.
Currently, `SQLite` and `PostgreSQL` are supported.

# Installation

This package can be installed using `pip`:

```
pip install git+https://github.com/LucHeuff/etl-components.git
```

or using [`poetry`](https://python-poetry.org):

```
poetry install git+https://github.com/LucHeuff/etl-components.git
```

or by adding it to `pyproject.toml`:

```
[tool.poetry.dependencies]
etl-components = { git = "https://github.com/LucHeuff/etl-components.git"}
```

# How does it work?

## `Cursor`s

The package exposes two convenience wrapper functions to connect to a database:

- `PostgresCursor`: which uses `psycopg` under the hood to connect to a PostgreSQL database server. This cursor requires a `.env` file in which credentials are provided.
- `SQLiteCursor`: which uses `sqlite3` under the hood to connect to a sqlite database file. This cursor needs a filename on initalisation.

## Interaction functions

The package defines three modes of interaction with the database: `insert`, `retrieve_ids` and `compare`.
`insert` is used to insert unique data into the database,
`retrieve_ids` retrieves ID values from the database and merge these into the data,
`compare` is used to validate that the data was correctly stored into the database.

A convenience function `insert_and_retrieve_ids` is provided as this those two actions are often combined.

Each of these functions requires a valid SQL query to perform the action on the database.
The package is opinionated in the sense that it will try to enforce consistent queries and tries to catch common mistakes.

# Documentation

## Cursors

### `SQLiteCursor`

- `SQLiteCursor(filename: str)`: returns a `sqlite3.Cursor` object to interact with the database. Data rows are returned as dictionaries.

Example use:

```
with SQLiteCursor("test.db") as cursor:
    cursor.execute("SELECT * FROM test")
    data = pd.DataFrame(cursor.fetchall())
```

### `PostgresCursor`:

- `PostgresCursor()`: returns a `psycopg.Cursor` object to interact with the database. Data rows are returned as dictionaries.

Example use:

```
with PostgresCursor() as cursor:
    cursor.execute("SELECT * FROM test")
    data = pd.DataFrame(cursor.fetchall())
```

To connect to the database server, `PostgresCursor` requires the following credentials to be defined in a `.env` file in the project root:

- `HOST`: database host ip to PostgreSQL server
- `PORT`: port to which PostgreSQL server listens (usually 5432)
- `DB`: name of database to connect to
- `USER` : username that has right on database
- `PASSWORD` : to authenticate user

## Functions

### insert

`insert(cursor: Cursor, query: str, data: pd.DataFrame, use_copy: bool = False)`

- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`
- `query`: an insert query of the correct format (see below)
- `data`: a `pandas.DataFrame` containing at least the columns to be inserted.
- `use_copy`: allows inserting using the COPY protocol when using a `PostgresCursor`

> NOTE:
> When `use_copy` is enabled, the regular insert query is translated into a COPY query.
> However, COPY does not support all the functionality that INSERT INTO provides.
> Mainly, COPY will append to existing data, but will not handle constraint conflicts.
> For more details, refer to the [PostgreSQL COPY documentation](https://www.postgresql.org/docs/current/sql-copy.html).

### retrieve_ids

`retrieve_ids(cursor: Cursor, query: str, data: pd.DataFrame, replace: bool = True) -> pd.DataFrame`

- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`
- `query`: a retrieve query of the correct format (see below)
- `data`: a `pandas.DataFrame` containing the columns to be merged
- `replace`: whether the merge columns should be replaced with the IDs from the database.

### insert_and_retrieve_ids

`insert_and_retrieve_ids(cursor: Cursor, insert_query: str, retrieve_query: str, data: pd.DataFrame, replace: bool = True, use_copy: bool = False) -> pd.DataFrame:`

- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`
- `insert_query`: an insert query of the correct format (see below)
- `retrieve_query`: a retrieve query of the correct format (see below)
- `data`: a `pandas.DataFrame` containing at least the columns to be inserted.
- `replace`: whether the merge columns should be replaced with the IDs from the database.
- `use_copy`: allows inserting using the COPY protocol when using a `PostgresCursor`

### compare

`compare(cursor: Cursor, query: str, orig_data: pd.DataFrame)`

- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`
- `query`: a compare query of the correct format (see below)
- `orig_data`: original data that was inserted into the database (devoid of ids).

## Formats

### insert formats

_The insert formats differ, since `sqlite3` and `psycopg` handle inserting using dictionary keys differently._

#### SQlite

```
INSERT INTO <table> (<column_db_1>, <column_db_2>, ...)
VALUES (:<column_df_1>, :<column_df_2>, ...)
```

#### PostgreSQL

```
INSERT INTO <table> (<column_db_1>, <column_db_2>, ...)
VALUES (%(<column_df_1>)s, %(<column_df_2>)s, ...)
```

### retrieve format

```
SELECT id as <table>_id, <column_db_1> as <column_df_1>, <column_db_2> FROM <table>
```

### compare format

```
SELECT
    <table>.<column_db_1> as <column_df_1>,
    <table>.<column_db_2>,
    <column_db_3>,
    ...
FROM <table>
    JOIN <other_table> ON <other_table>.<table>_id = <table>.id
    JOIN ...
...
```
