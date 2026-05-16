"""SQLite database resources."""


class ColumnDefinition:
    """Column definition.

    Attributes:
      name (str): name of the column.
      value_type (str): value type of the column.
    """

    def __init__(self):
        """Initializes a column definition."""
        super().__init__()
        self.name = None
        self.value_type = None


class DatabaseDefinition:
    """Database definition.

    Attributes:
      artifact_definition (str): name of the corresponding Digital Forensics
          Artifact definition.
      database_identifier (str): identifier of the database.
    """

    def __init__(self):
        """Initializes a database definition."""
        super().__init__()
        self.artifact_definition = None
        self.database_identifier = None
