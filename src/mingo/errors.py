class DataFileFormatError(Exception):
    "Unexpected format"
    pass


class DatabaseCreationError(Exception):
    """Error while attempting to create a database"""
    pass
