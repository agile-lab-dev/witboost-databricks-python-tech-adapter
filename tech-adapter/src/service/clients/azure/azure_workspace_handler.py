import re
from enum import StrEnum
from typing import Optional

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk import WorkspaceClient
from loguru import logger
from pydantic import BaseModel

from src import settings
from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import DatabricksComponent
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import WorkspaceHandlerError
from src.service.clients.azure.azure_workspace_manager import AzureWorkspaceManager
from src.settings.databricks_tech_adapter_settings import AzureAuthSettings, DatabricksAuthSettings


class AuthType(StrEnum):
    """Authentication type for the Databricks WorkspaceClient."""

    AZURE = "AZURE"
    OAUTH = "OAUTH"


class WorkspaceClientConfigParams(BaseModel):
    """
    Configuration parameters for creating a Databricks WorkspaceClient.

    This class validates that the correct set of parameters is provided
    for the chosen authentication type.
    """

    auth_type: AuthType
    workspace_host: str
    workspace_name: str  # Used for logging and error messages


class AzureAuthWorkspaceClientConfigParams(WorkspaceClientConfigParams):
    # --- Azure Service Principal Authentication ---
    # These fields are required when auth_type is AZURE
    auth_type: AuthType = AuthType.AZURE
    databricks_auth_config: DatabricksAuthSettings
    azure_auth_config: AzureAuthSettings


class OAuthWorkspaceClientConfigParams(WorkspaceClientConfigParams):
    # --- Databricks OAuth M2M Authentication ---
    # These fields are required when auth_type is OAUTH
    auth_type: AuthType = AuthType.OAUTH
    databricks_client_id: str
    databricks_client_secret: str


def create_workspace_client(params: WorkspaceClientConfigParams) -> WorkspaceClient:
    """
    Factory function to create a Databricks WorkspaceClient based on configuration parameters.

    Args:
        params: A WorkspaceClientConfigParams object containing all necessary
                configuration for the desired authentication method.

    Returns:
        An initialized WorkspaceClient.

    Raises:
        WorkspaceClientCreationError: If client initialization fails for any reason.
        ValueError: If the configuration parameters are invalid for the auth type.
    """
    try:
        logger.info(
            "Creating WorkspaceClient for workspace '{}' with auth type {}", params.workspace_name, params.auth_type
        )

        if isinstance(params, AzureAuthWorkspaceClientConfigParams):
            return WorkspaceClient(
                host=params.workspace_host,
                azure_client_id=params.azure_auth_config.client_id,
                azure_client_secret=params.azure_auth_config.client_secret,
                azure_tenant_id=params.azure_auth_config.tenant_id,
            )
        elif isinstance(params, OAuthWorkspaceClientConfigParams):
            return WorkspaceClient(
                host=params.workspace_host,
                client_id=params.databricks_client_id,
                client_secret=params.databricks_client_secret,
            )
        # This case should be unreachable due to the Enum validation in Pydantic
        else:
            raise ValueError(f"Invalid auth type: {params.auth_type}")

    except (ValueError, Exception) as e:
        error_msg = f"Error initializing the Workspace Client for {params.workspace_name}"
        logger.error(error_msg)
        raise WorkspaceHandlerError(
            f"Error initializing the Workspace Client for {params.workspace_name}. Details: {e}"
        ) from e


class WorkspaceHandler:
    """
    Handles retrieving information about Databricks workspaces.
    TODO: Handle workspace creation and permissions assignment
    """

    # Pre-compile the regex for efficiency, equivalent to Java's static Pattern
    _DATABRICKS_URL_PATTERN = re.compile(r"(?:https://)?adb-(\d+)\.\d+\.azuredatabricks\.net")

    def __init__(self, azure_workspace_manager: AzureWorkspaceManager):
        """
        Initializes the WorkspaceHandler.

        Args:
            azure_workspace_manager: Client to interact with Azure for workspace info.
        """
        self.azure_workspace_manager = azure_workspace_manager

    def provision_workspace(self, data_product: DataProduct, component: DatabricksComponent) -> DatabricksWorkspaceInfo:
        logger.info(f"Provisioning and/or retrieving information about workspace for component '{component.id}'")
        info = self.get_workspace_info(component)

        if info:
            logger.info(f"Workspace for component '{component.id}' exists with name '{info.name}'")
            return info

        # TODO implement provision
        error_msg = "Witboost managed workspace are not yet supported on this version"
        logger.error(error_msg)
        raise WorkspaceHandlerError(error_msg)

    def get_workspace_info(self, component: DatabricksComponent) -> Optional[DatabricksWorkspaceInfo]:
        """
        Returns information for a Databricks Workspace from a component definition.

        If the `workspace` field in the specific class is a URL, the workspace is
        considered unmanaged. Otherwise, it's looked up in Azure.

        Args:
            component: The component containing specific configuration.

        Returns:
            A DatabricksWorkspaceInfo object if found, otherwise None.

        Raises:
            WorkspaceHandlerError: If the lookup process fails.
        """
        try:
            workspace_identifier = component.specific.workspace
            return self.get_workspace_info_by_name(workspace_identifier)
        except WorkspaceHandlerError as e:
            error_msg = f"Could not determine workspace for component '{component.name}'"
            logger.error(error_msg)
            raise WorkspaceHandlerError(error_msg) from e

    def get_workspace_info_by_name(self, workspace_identifier: str) -> Optional[DatabricksWorkspaceInfo]:
        """
        Returns information for a Databricks Workspace from its name or URL.

        Args:
            workspace_identifier: The workspace name or its full URL.

        Returns:
            A DatabricksWorkspaceInfo object if found, otherwise None.
        """
        match = self._DATABRICKS_URL_PATTERN.match(workspace_identifier)
        if match:
            workspace_id = match.group(1)
            logger.info("Workspace identifier '{}' is a URL. Treating as unmanaged", workspace_identifier)
            return DatabricksWorkspaceInfo.build_unmanaged(
                databricks_host=workspace_identifier,
                id=workspace_id,
                azure_resource_url=workspace_identifier,
                provisioning_state=ProvisioningState.SUCCEEDED,
            )

        # If not a URL, assume it's a name and query Azure
        logger.info(f"Looking up managed workspace with name: '{workspace_identifier}'")
        if not settings.azure.permissions:
            error_msg = (
                "Error, received request workspace is set to be managed, but azure.permissions is not "
                "configured on Tech Adapter"
            )
            logger.error(error_msg)
            raise WorkspaceHandlerError(error_msg)

        managed_resource_group_id = (
            f"/subscriptions/{settings.azure.auth.subscription_id}/resourceGroups/{workspace_identifier}-rg"
        )

        return self.azure_workspace_manager.get_workspace(workspace_identifier, managed_resource_group_id)

    def get_workspace_client(self, databricks_workspace_info: DatabricksWorkspaceInfo) -> WorkspaceClient:
        """
        Creates a WorkspaceClient for interacting with a Databricks workspace.

        Args:
            databricks_workspace_info: The information object for the target workspace.

        Returns:
            An authenticated WorkspaceClient instance.

        Raises:
            WorkspaceHandlerError: If client creation fails.
        """
        try:
            # Assuming the factory expects a dictionary of parameters
            config_params: WorkspaceClientConfigParams = AzureAuthWorkspaceClientConfigParams(
                workspace_host=databricks_workspace_info.databricks_host,
                workspace_name=databricks_workspace_info.name,
                databricks_auth_config=settings.databricks.auth,
                azure_auth_config=settings.azure.auth,
            )
            return create_workspace_client(config_params)
        except Exception as e:
            error_msg = f"Failed to create Databricks workspace client for '{databricks_workspace_info.name}'"
            logger.error(error_msg)
            raise WorkspaceHandlerError(error_msg) from e
