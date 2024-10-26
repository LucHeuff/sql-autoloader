class ETLComponentsError(Exception):
    """Base class for exceptions in this package."""


# ---- Errors in dataframe operations
class InvalidDataframeError(ETLComponentsError):
    """Raised when the dataframe provided is invalid."""


class MissingKeysAfterMergeError(ETLComponentsError):
    """Raised when merging data from the db results in missing values in id columns."""


class MatchDatatypesError(ETLComponentsError):
    """Raised when matching datatypes fails."""


class CompareMissingRowsError(ETLComponentsError):
    """Raised during comparison when rows from data do not exist in the database."""


class CompareNoExactMatchError(ETLComponentsError):
    """Raised during comparison when rows from data and rows from database are no exact match."""


# ---- SchemaError and children
class SchemaError(ETLComponentsError):
    """Raised when an error occurs when using the Schema."""


class TableDoesNotExistError(SchemaError):
    """Raised when the requested table does not exist in the Schema."""


class EmptyColumnListError(SchemaError):
    """Raised when an empty list of columns is passed."""


class NoSuchColumnForTableError(SchemaError):
    """Raised when a prefix is used but the column does not exist for that table."""


class NoSuchColumnInSchemaError(SchemaError):
    """Raised when the provided column name does not appear anywhere in the schema."""


class ColumnsDoNotExistOnTableError(SchemaError):
    """Raised when all the columns do not exist on a Table."""


class ColumnIsAmbiguousError(SchemaError):
    """Raised when a column name can refer to multiple tables."""


class NoPrimaryKeyError(SchemaError):
    """Raised when the Table has no primary key."""


class AliasDoesNotExistError(SchemaError):
    """Raised when the provided alias does not exist."""


class AmbiguousAliasesError(SchemaError):
    """Raised when multiple aliases exist and it is unclear which should be used."""


class IsolatedTablesError(SchemaError):
    """Raised when compare query generation encounters isolated tables."""


class IsolatedSubgraphsError(SchemaError):
    """Raised when compare query generation encounters isolated subgraphs."""


class InvalidTableError(ETLComponentsError):
    """Raised when Table validation fails."""


class InvalidReferenceError(ETLComponentsError):
    """Raised when Reference validation fails."""
