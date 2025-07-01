from typing import Optional

from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.databricks.models import Workspace
from loguru import logger

from src import settings
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import WorkspaceHandlerError


class AzureWorkspaceManager:
    """
    Manages operations on Azure Databricks workspaces.
    # TODO Implement creation/deletion methods
    """

    def __init__(
        self,
        azure_databricks_manager: AzureDatabricksManagementClient,
    ):
        """
        Initializes the AzureWorkspaceManager.

        Args:
            azure_databricks_manager: The authenticated Azure Databricks SDK client.
        """
        self.azure_databricks_manager = azure_databricks_manager

    def get_workspace(self, workspace_name: str, managed_resource_group_id: str) -> Optional[DatabricksWorkspaceInfo]:
        """
        Retrieves information about an existing Azure Databricks workspace.

        It searches for a workspace matching the given name and managed resource group ID
        (both case-insensitively).

        Args:
            workspace_name: The name of the workspace to retrieve.
            managed_resource_group_id: The managed resource group ID for the workspace.

        Returns:
            A DatabricksWorkspaceInfo object if the workspace exists, otherwise None.

        Raises:
            AzureWorkspaceManagerError: If an unexpected error occurs during the API call.
        """
        if not settings.azure.permissions:
            error_msg = (
                "azure.permissions is not configured on the Tech Adapter. Cannot retrieve information about workspaces"
            )
            logger.error(error_msg)
            raise WorkspaceHandlerError(error_msg)

        logger.info(
            "Searching for workspace '{}' in managed resource group '{}'", workspace_name, managed_resource_group_id
        )
        try:
            workspaces_iterator = self.azure_databricks_manager.workspaces.list_by_subscription()

            # Find the first workspace that matches the criteria (case-insensitive).
            found_workspace: Optional[Workspace] = next(
                (
                    ws
                    for ws in workspaces_iterator
                    if ws.name.lower() == workspace_name.lower()  # type:ignore[attr-defined]
                    and ws.managed_resource_group_id.lower() == managed_resource_group_id.lower()
                ),
                None,  # Default value if no item is found
            )

            if not found_workspace:
                logger.warning("Workspace '{}' not found.", workspace_name)
                return None

            logger.success("Found workspace '{}' with ID '{}'.", workspace_name, found_workspace.workspace_id)

            resource_id = (
                f"/subscriptions/{settings.azure.auth.subscription_id}"
                f"/resourceGroups/{settings.azure.permissions.resource_group}"
                f"/providers/Microsoft.Databricks/workspaces/{found_workspace.name}"
            )
            azure_url = f"https://portal.azure.com/#@{settings.azure.auth.tenant_id}" f"/resource/{resource_id}"

            return DatabricksWorkspaceInfo.build_managed(
                name=found_workspace.name,  # type:ignore[arg-type]
                id=found_workspace.workspace_id,  # type:ignore[arg-type]
                databricks_host=found_workspace.workspace_url,  # type:ignore[arg-type]
                azure_resource_id=found_workspace.id,  # type:ignore[arg-type]
                azure_resource_url=azure_url,
                provisioning_state=found_workspace.provisioning_state,  # type:ignore[arg-type]
            )

        except Exception as e:
            error_msg = f"An error occurred while getting info for workspace '{workspace_name}'"
            logger.error(error_msg)
            raise WorkspaceHandlerError(error_msg) from e
