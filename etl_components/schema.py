from dataclasses import dataclass


@dataclass
class Column:
    """Describes a column with a name and a data type."""

    name: str
    data_type: str

    def __str__(self) -> str:
        """Readable representation of the column.

        Returns
        -------
            string representation of column.


        """
        return f"  {self.name}: {self.data_type}"


@dataclass
class Table:
    """Describes a table with a name and a list of columns."""

    name: str
    columns: list[Column]

    @property
    def column_names(self) -> list[str]:
        """Get a list of names of columns in the table.

        Returns
        -------
            list of column names


        """
        return [column.name for column in self.columns]

    def __call__(self, column_name: str) -> Column:
        """Retrieve the column that belongs to the provided column_name.

        Args:
        ----
            column_name: of the desired column

        Returns:
        -------
           Column with the provided column_name

        Raises:
        ------
            ValueError: if the provided column_name does appear in the table

        """
        if column_name not in self.column_names:
            message = f"'{column_name}' doe snot appear in table."
            raise ValueError(message)
        return self.columns[self.column_names.index(column_name)]

    def __str__(self) -> str:
        """Readable representation of the table.

        Returns
        -------
            string representation of table

        """
        columns = "\n".join(list(map(str, self.columns)))
        return f"TABLE {self.name} {{\n{columns}\n}}"


@dataclass
class Schema:
    """Describes a database Schema consisting of a list of tables."""

    tables: list[Table]

    @property
    def table_names(self) -> list[str]:
        """Get a list of names of tables in the schema.

        Returns
        -------
            list of table names


        """
        return [table.name for table in self.tables]

    def __call__(self, table_name: str) -> Table:
        """Retrieve the Table that belongs to provided table_name.

        Args:
        ----
            table_name: of the desired table

        Returns:
        -------
            Table with the provided table_name

        Raises:
        ------
            ValueError: if table_name does not appear in schema.

        """
        if table_name not in self.table_names:
            message = f"'{table_name}' does not appear in schema."
            raise ValueError(message)
        return self.tables[self.table_names.index(table_name)]

    def __str__(self) -> str:
        """Readable representation of schema.

        Returns
        -------
            string representation of schema


        """
        return "\n\n".join(list(map(str, self.tables)))
