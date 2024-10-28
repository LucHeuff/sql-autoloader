# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['sql_autoloader']

package_data = \
{'': ['*']}

install_requires = \
['more-itertools>=10.5.0,<11.0.0',
 'networkx>=3.3,<4.0',
 'polars>=1.9.0,<2.0.0',
 'pydantic>=2.9.2,<3.0.0']

setup_kwargs = {
    'name': 'sql-autoloader',
    'version': '2.0.0',
    'description': 'Components to automate loading steps in ETL pipelines',
    'long_description': '# ETL Components\n\nContains convenience functions for ETL components built on top of `pandas`, `psycopg` and `sqlite3`.\n\nThese are opinionated functions for inserting, retrieving and comparing data in to a SQL database.\nCurrently, `SQLite` and `PostgreSQL` are supported.\n\n# Installation\n\nThis package can be installed using `pip`:\n\n```\npip install git+https://github.com/LucHeuff/etl-components.git\n```\n\nor using [`poetry`](https://python-poetry.org):\n\n```\npoetry install git+https://github.com/LucHeuff/etl-components.git\n```\n\nor by adding it to `pyproject.toml`:\n\n```\n[tool.poetry.dependencies]\netl-components = { git = "https://github.com/LucHeuff/etl-components.git"}\n```\n\n# How does it work?\n\n`TODO`: Rewrite\n\n',
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

