class ETLComponentsError(Exception):
    """Base class for exceptions in this package."""


class UnknownDataframeError(ETLComponentsError):
    """Raised when an unknown dataframe type is provided."""


class MissingKeysAfterMergeError(ETLComponentsError):
    """Raised when merging data from the db results in missing values in id columns."""


class MatchDatatypesError(ETLComponentsError):
    """Raised when matching datatypes fails."""


class CompareMissingRowsError(ETLComponentsError):
    """Raised during comparison when rows from data do not exist in the database."""


class CompareNoExactMatchError(ETLComponentsError):
    """Raised during comparison when rows from data and rows from database are no exact match."""


class SchemaError(ETLComponentsError):
    """Raised when an error occurs when using the Schema."""


class InvalidTableError(ETLComponentsError):
    """Raised when Table validation fails."""


class InvalidReferenceError(ETLComponentsError):
    """Raised when Reference validation fails."""
