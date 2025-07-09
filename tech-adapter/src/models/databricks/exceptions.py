"""
Custom exceptions for handling Databricks.
"""


class DatabricksError(Exception):
    """
    Base exception for all errors raised by Databricks handlers.

    This exception serves as a common parent for all other specific exceptions
    in this module. Catching this exception will handle any error originating
    from the Databricks logic.
    """

    pass


class DatabricksMapperError(DatabricksError):
    pass


class WorkspaceManagerError(DatabricksError):
    """
    Base exception for all errors raised by the WorkspaceManager.

    This exception is used for any failure within the WorkspaceManager,
    including resource not found errors, API call failures, and other
    operational issues.
    """

    pass


class RepoManagerError(DatabricksError):
    """Raised for any failure during repository management operations."""

    pass


class JobManagerError(DatabricksError):
    """Raised for any failure during Databricks Job management operations."""

    pass


class WorkspaceHandlerError(DatabricksError):
    """Base exception for failures on the WorkspaceHandler class."""

    pass


class WorkflowManagerError(DatabricksError):
    """Base exception for any failure during Databricks Workflow management operations."""

    pass


class DLTManagerError(DatabricksError):
    """Base exception for any failure during Databricks DLT management operations."""

    pass


class UnityCatalogError(DatabricksError):
    """
    Base exception for any failure during operations related to the Unity Catalog
    """

    pass


class StatementExecutionError(DatabricksError):
    """
    Base exception for any failures during the execution of SQL statements
    """

    pass
