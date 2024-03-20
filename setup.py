# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['etl_components']

package_data = \
{'': ['*']}

install_requires = \
['pandas>=2.2.0,<3.0.0',
 'psycopg>=3.1.18,<4.0.0',
 'python-dotenv>=1.0.1,<2.0.0']

setup_kwargs = {
    'name': 'etl-components',
    'version': '1.0.8',
    'description': 'Components to build ETL pipelines using pandas and psycopg or sqlite',
    'long_description': '# ETL Components\n\nContains convenience functions for ETL components built on top of `pandas`, `psycopg` and `sqlite3`.\n\nThese are opinionated functions for inserting, retrieving and comparing data in to a SQL database.\nCurrently, `SQLite` and `PostgreSQL` are supported.\n\n# Installation\n\nThis package can be installed using `pip`:\n\n```\npip install git+https://github.com/LucHeuff/etl-components.git\n```\n\nor using [`poetry`](https://python-poetry.org):\n\n```\npoetry install git+https://github.com/LucHeuff/etl-components.git\n```\n\nor by adding it to `pyproject.toml`:\n\n```\n[tool.poetry.dependencies]\netl-components = { git = "https://github.com/LucHeuff/etl-components.git"}\n```\n\n# How does it work?\n\n## `Cursor`s\n\nThe package exposes two convenience wrapper functions to connect to a database:\n\n- `PostgresCursor`: which uses `psycopg` under the hood to connect to a PostgreSQL database server. This cursor requires a `.env` file in which credentials are provided.\n- `SQLiteCursor`: which uses `sqlite3` under the hood to connect to a sqlite database file. This cursor needs a filename on initalisation.\n\n## Interaction functions\n\nThe package defines three modes of interaction with the database: `insert`, `retrieve_ids` and `compare`.\n`insert` is used to insert unique data into the database,\n`retrieve_ids` retrieves ID values from the database and merge these into the data,\n`compare` is used to validate that the data was correctly stored into the database.\n\nA convenience function `insert_and_retrieve_ids` is provided as this those two actions are often combined.\n\nEach of these functions requires a valid SQL query to perform the action on the database.\nThe package is opinionated in the sense that it will try to enforce consistent queries and tries to catch common mistakes.\n\n# Documentation\n\n## Cursors\n\n### `SQLiteCursor`\n\n- `SQLiteCursor(filename: str)`: returns a `sqlite3.Cursor` object to interact with the database. Data rows are returned as dictionaries.\n\nExample use:\n\n```\nwith SQLiteCursor("test.db") as cursor:\n    cursor.execute("SELECT * FROM test")\n    data = pd.DataFrame(cursor.fetchall())\n```\n\n### `PostgresCursor`:\n\n- `PostgresCursor()`: returns a `psycopg.Cursor` object to interact with the database. Data rows are returned as dictionaries.\n\nExample use:\n\n```\nwith PostgresCursor() as cursor:\n    cursor.execute("SELECT * FROM test")\n    data = pd.DataFrame(cursor.fetchall())\n```\n\nTo connect to the database server, `PostgresCursor` requires the following credentials to be defined in a `.env` file in the project root:\n\n- `HOST`: database host ip to PostgreSQL server\n- `PORT`: port to which PostgreSQL server listens (usually 5432)\n- `DB`: name of database to connect to\n- `USER` : username that has right on database\n- `PASSWORD` : to authenticate user\n\n## Functions\n\n### insert\n\n`insert(cursor: Cursor, query: str, data: pd.DataFrame, use_copy: bool = False)`\n\n- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`\n- `query`: an insert query of the correct format (see below)\n- `data`: a `pandas.DataFrame` containing at least the columns to be inserted.\n- `use_copy`: allows inserting using the COPY protocol when using a `PostgresCursor`\n\n> NOTE:\n> When `use_copy` is enabled, the regular insert query is translated into a COPY query.\n> However, COPY does not support all the functionality that INSERT INTO provides.\n> Mainly, COPY will append to existing data, but will not handle constraint conflicts.\n> For more details, refer to the [PostgreSQL COPY documentation](https://www.postgresql.org/docs/current/sql-copy.html).\n\n### retrieve_ids\n\n`retrieve_ids(cursor: Cursor, query: str, data: pd.DataFrame, replace: bool = True) -> pd.DataFrame`\n\n- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`\n- `query`: a retrieve query of the correct format (see below)\n- `data`: a `pandas.DataFrame` containing the columns to be merged\n- `replace`: whether the merge columns should be replaced with the IDs from the database.\n\n### insert_and_retrieve_ids\n\n`insert_and_retrieve_ids(cursor: Cursor, insert_query: str, retrieve_query: str, data: pd.DataFrame, replace: bool = True, use_copy: bool = False) -> pd.DataFrame:`\n\n- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`\n- `insert_query`: an insert query of the correct format (see below)\n- `retrieve_query`: a retrieve query of the correct format (see below)\n- `data`: a `pandas.DataFrame` containing at least the columns to be inserted.\n- `replace`: whether the merge columns should be replaced with the IDs from the database.\n- `use_copy`: allows inserting using the COPY protocol when using a `PostgresCursor`\n\n### compare\n\n`compare(cursor: Cursor, query: str, orig_data: pd.DataFrame)`\n\n- `cursor`: either a `PostgresCursor` or a `SQLiteCursor`\n- `query`: a compare query of the correct format (see below)\n- `orig_data`: original data that was inserted into the database (devoid of ids).\n\n## Formats\n\n### insert formats\n\n_The insert formats differ, since `sqlite3` and `psycopg` handle inserting using dictionary keys differently._\n\n#### SQlite\n\n```\nINSERT INTO <table> (<column_db_1>, <column_db_2>, ...)\nVALUES (:<column_df_1>, :<column_df_2>, ...)\n```\n\n#### PostgreSQL\n\n```\nINSERT INTO <table> (<column_db_1>, <column_db_2>, ...)\nVALUES (%(<column_df_1>)s, %(<column_df_2>)s, ...)\n```\n\n### retrieve format\n\n```\nSELECT id as <table>_id, <column_db_1> as <column_df_1>, <column_db_2> FROM <table>\n```\n\n### compare format\n\n```\nSELECT\n    <table>.<column_db_1> as <column_df_1>,\n    <table>.<column_db_2>,\n    <column_db_3>,\n    ...\nFROM <table>\n    JOIN <other_table> ON <other_table>.<table>_id = <table>.id\n    JOIN ...\n...\n```\n',
    'author': 'Luc Heuff',
    'author_email': '10941592+LucHeuff@users.noreply.github.com',
    'maintainer': 'None',
    'maintainer_email': 'None',
    'url': 'None',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '>=3.12,<4.0',
}


setup(**setup_kwargs)

