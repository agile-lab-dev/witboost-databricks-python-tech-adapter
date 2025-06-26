"""
Custom exceptions for handling Databricks.
"""


class DatabricksMapperError(Exception):
    """
    Base exception for all errors raised by the DatabricksMapper.

    This exception serves as a common parent for all other specific exceptions
    in this module. Catching this exception will handle any error originating
    from the Databricks mapping logic.
    """

    pass
