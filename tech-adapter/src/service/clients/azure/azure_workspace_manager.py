from typing import Optional

from azure.core.exceptions import ResourceExistsError
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.databricks.aio import AzureDatabricksManagementClient as AsyncAzureDatabricksManagementClient
from azure.mgmt.databricks.models import ProvisioningState, Sku, Workspace
from loguru import logger

from src import settings
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import AzureWorkspaceManagerError
from src.service.clients.azure.lro_polling.verbose_async_arm_polling import VerboseAsyncARMPolling
from src.settings.databricks_tech_adapter_settings import SkuType


class AzureWorkspaceManager:
    """
    Manages operations on Azure Databricks workspaces, such as creation, deletion,
    and retrieval of information.
    """

    def __init__(
        self,
        sync_azure_databricks_manager: AzureDatabricksManagementClient,
        async_azure_databricks_manager: AsyncAzureDatabricksManagementClient,
    ):
        """
        Initializes the AzureWorkspaceManager.

        Args:
            sync_azure_databricks_manager: The authenticated Azure Databricks SDK client used to make synchronous calls.
            async_azure_databricks_manager: The authenticated Azure Databricks SDK client used to make asynchronous,
            Long-Running Operation (LRO) calls.
        """
        self.sync_azure_databricks_manager = sync_azure_databricks_manager
        self.async_azure_databricks_manager = async_azure_databricks_manager

    async def create_if_not_exists_workspace(
        self,
        workspace_name: str,
        region: str,
        existing_resource_group_name: str,
        managed_resource_group_id: str,
        sku_type: SkuType,
    ) -> DatabricksWorkspaceInfo:
        """
        Creates a new Azure Databricks workspace if it does not already exist.

        Args:
            workspace_name: The name for the new workspace.
            region: The Azure region for the workspace (e.g., 'westeurope').
            existing_resource_group_name: The name of the resource group for the workspace.
            managed_resource_group_id: The resource ID for the managed resource group.
            sku_type: The SKU for the workspace (e.g., SkuType.PREMIUM).

        Returns:
            A DatabricksWorkspaceInfo object for the existing or newly created workspace.

        Raises:
            WorkspaceManagerError: If workspace creation fails or checking for existence fails.
        """
        try:
            existing_workspace = self.get_workspace(workspace_name, managed_resource_group_id)
            if existing_workspace:
                logger.info("Workspace '{}' already exists. Skipping creation.", workspace_name)
                return existing_workspace

            logger.info("Creating workspace '{}' in region '{}'.", workspace_name, region)
            if not settings.azure.permissions:
                error_msg = (
                    "Error, received request workspace is set to be managed, but azure.permissions is not "
                    "configured on Tech Adapter"
                )
                logger.error(error_msg)
                raise AzureWorkspaceManagerError(error_msg)

            workspace_parameters = Workspace(
                location=region,
                managed_resource_group_id=managed_resource_group_id,
                sku=Sku(name=sku_type.value),
            )

            poller = await self.async_azure_databricks_manager.workspaces.begin_create_or_update(
                resource_group_name=existing_resource_group_name,
                workspace_name=workspace_name,
                parameters=workspace_parameters,
                polling=VerboseAsyncARMPolling(f"Workspace {workspace_name} creation"),
            )
            new_workspace = await poller.result()
            logger.debug("Response of Workspace LRO creation for {}: {}", workspace_name, new_workspace)
            if new_workspace.provisioning_state != ProvisioningState.SUCCEEDED:
                error_msg = (
                    f"The workspace '{workspace_name}' exists but its state "
                    f"is not yet Succeeded. Please wait and try again."
                )
                logger.warning(error_msg)
                raise AzureWorkspaceManagerError(error_msg)

            logger.success("Workspace '{}' is now available at: {}", new_workspace.name, new_workspace.workspace_url)

            resource_id = (
                f"/subscriptions/{settings.azure.auth.subscription_id}/resourceGroups/"
                f"{settings.azure.permissions.resource_group}/providers/Microsoft.Databricks/workspaces/"
                f"{new_workspace.name}"
            )
            azure_url = f"https://portal.azure.com/#@{settings.azure.auth.tenant_id}/resource/{resource_id}"

            return DatabricksWorkspaceInfo.build_managed(
                name=new_workspace.name,  # type:ignore[arg-type]
                id=new_workspace.workspace_id,  # type:ignore[arg-type]
                databricks_host=new_workspace.workspace_url,  # type:ignore[arg-type]
                azure_resource_id=new_workspace.id,  # type:ignore[arg-type]
                azure_resource_url=azure_url,
                provisioning_state=new_workspace.provisioning_state,  # type:ignore[arg-type]
            )
        except AzureWorkspaceManagerError:
            raise
        except ResourceExistsError as e:
            error_msg = (
                f"The workspace '{workspace_name}' is currently being created, please wait and try again. "
                f"If you have several Databricks components on your system, please define dependencies among "
                f"them in order to have a root component that creates the workspace, while all others wait "
                f"for provisioning completion before starting."
            )
            logger.warning(error_msg)
            raise AzureWorkspaceManagerError(error_msg) from e
        except Exception as e:
            error_msg = f"An error occurred creating workspace '{workspace_name}'"
            logger.error("An error occurred creating workspace '{}'. Details: {}", workspace_name, e)
            raise AzureWorkspaceManagerError(error_msg) from e

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
            WorkspaceManagerError: If an unexpected error occurs during the API call.
        """
        if not settings.azure.permissions:
            error_msg = (
                "azure.permissions is not configured on the Tech Adapter. Cannot retrieve information about workspaces"
            )
            logger.error(error_msg)
            raise AzureWorkspaceManagerError(error_msg)

        logger.info(
            "Searching for workspace '{}' in managed resource group '{}'", workspace_name, managed_resource_group_id
        )
        try:
            workspaces_iterator = self.sync_azure_databricks_manager.workspaces.list_by_subscription()

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
            raise AzureWorkspaceManagerError(error_msg) from e
