import re
import uuid
from enum import StrEnum
from typing import Optional

from azure.mgmt.authorization.models import PrincipalType
from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk import WorkspaceClient
from loguru import logger
from pydantic import BaseModel

from src import settings
from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import DatabricksComponent
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import MapperError
from src.models.exceptions import WorkspaceHandlerError, build_error_message_from_chained_exception
from src.service.clients.azure.azure_permissions_manager import AzurePermissionsManager
from src.service.clients.azure.azure_workspace_manager import AzureWorkspaceManager
from src.service.principals_mapping.azure_mapper import AzureMapper
from src.settings.databricks_tech_adapter_settings import (
    AzureAuthSettings,
    AzurePermissionsSettings,
    DatabricksAuthSettings,
)


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
            [f"Error initializing the Workspace Client for {params.workspace_name}. Details: {e}"]
        ) from e


class AzureWorkspaceHandler:
    """
    Handles the provisioning and management of Azure Databricks workspaces,
    including creation, information retrieval, and permission assignments.
    """

    _DATABRICKS_URL_PATTERN = re.compile(r"(?:https://)?adb-(\d+)\.\d+\.azuredatabricks\.net")

    def __init__(
        self,
        azure_workspace_manager: AzureWorkspaceManager,
        azure_permissions_manager: AzurePermissionsManager,
        azure_mapper: AzureMapper,
    ):
        """
        Initializes the WorkspaceHandler.

        Args:
            azure_workspace_manager: Client for Azure Databricks workspace operations.
            azure_permissions_manager: Client for managing Azure role assignments.
            azure_mapper: Client for mapping principals to Azure Object IDs.
            workspace_client_factory: A factory function to create WorkspaceClient instances.
        """
        self.azure_workspace_manager = azure_workspace_manager
        self.azure_permissions_manager = azure_permissions_manager
        self.azure_mapper = azure_mapper

    async def provision_workspace(
        self, data_product: DataProduct, component: DatabricksComponent
    ) -> DatabricksWorkspaceInfo:
        """
        Provisions a Databricks workspace and assigns initial permissions.

        This method checks if a workspace exists. If it's an unmanaged workspace (URL-based),
        it returns its info directly. If it's a managed workspace, it creates it if it doesn't
        exist and then assigns configured Azure roles to the data product owner and dev group.

        Args:
            data_product: The data product related to the workspace to provision
            component: The component storing the definition of the workspace to provision

        Returns:
            A DatabricksWorkspaceInfo object for the provisioned workspace.

        Raises:
            WorkspaceHandlerError: If workspace provisioning or permission assignment fails.
        """
        logger.info("Provisioning and/or retrieving information for component '{}'", component.id)

        # Check if workspace info can be retrieved
        workspace_info = self.get_workspace_info_by_name(component.specific.workspace)

        # If workspace exists and is unmanaged, return immediately
        if workspace_info and not workspace_info.is_managed:
            logger.info("Component '{}' uses an existing unmanaged workspace. No provisioning needed.", component.id)
            return workspace_info

        if not settings.azure.permissions:
            error_msg = (
                "Error, received request workspace is set to be managed, but azure.permissions is not "
                "configured on Tech Adapter"
            )
            logger.error(error_msg)
            raise WorkspaceHandlerError([error_msg])

        # Create the workspace if it doesn't exist
        new_workspace_info = await self._create_if_not_exists_databricks_workspace(
            component, settings.azure.permissions
        )

        # Assign Azure permissions
        await self._manage_azure_permissions(
            new_workspace_info,
            data_product.dataProductOwner,
            settings.azure.permissions,
            settings.azure.permissions.dp_owner_role_definition_id,
            PrincipalType.USER,
        )

        # TODO: This is a temporary solution
        dev_group = data_product.devGroup
        if not dev_group.startswith("group:"):
            dev_group = f"group:{dev_group}"

        await self._manage_azure_permissions(
            new_workspace_info,
            dev_group,
            settings.azure.permissions,
            settings.azure.permissions.dev_group_role_definition_id,
            PrincipalType.GROUP,
        )

        return new_workspace_info

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
            raise WorkspaceHandlerError([error_msg])

        managed_resource_group_id = (
            f"/subscriptions/{settings.azure.auth.subscription_id}/resourceGroups/{workspace_identifier}-rg"
        )

        return self.azure_workspace_manager.get_workspace(workspace_identifier, managed_resource_group_id)

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
            raise WorkspaceHandlerError([error_msg]) from e

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
            raise WorkspaceHandlerError([error_msg]) from e

    async def _create_if_not_exists_databricks_workspace(
        self, component: DatabricksComponent, permissions_settings: AzurePermissionsSettings
    ) -> DatabricksWorkspaceInfo:
        """Creates a managed Azure Databricks workspace if it doesn't already exist.

        Args:
            permissions: Azure permission settings
        """
        try:
            workspace_name = component.specific.workspace
            managed_resource_group_id = (
                f"/subscriptions/{settings.azure.auth.subscription_id}/resourceGroups/{workspace_name}-rg"
            )

            return await self.azure_workspace_manager.create_if_not_exists_workspace(
                workspace_name=workspace_name,
                region="westeurope",
                existing_resource_group_name=permissions_settings.resource_group,
                managed_resource_group_id=managed_resource_group_id,
                sku_type=settings.azure.auth.sku_type,
            )
        except Exception as e:
            error_msg = f"An error occurred while creating workspace for component '{component.name}'"
            logger.error(
                "An error occurred while creating workspace for component '{}'. Details: {}", component.name, e
            )
            raise WorkspaceHandlerError([error_msg]) from e

    async def _manage_azure_permissions(
        self,
        databricks_workspace_info: DatabricksWorkspaceInfo,
        entity: str,
        permissions_settings: AzurePermissionsSettings,
        role_definition_id: str,
        principal_type: PrincipalType,
    ) -> None:
        """Manages Azure role assignments for a given entity on the workspace resource."""
        if not role_definition_id:
            logger.info("No role definition ID provided for entity '{}'. Skipping permission management.", entity)
            return

        logger.info(
            "Managing permissions for {} on workspace {}. Assigning role definition {}",
            entity,
            databricks_workspace_info.name,
            role_definition_id,
        )

        try:
            # Map the user/group name to its Azure Object ID
            entity_map = await self.azure_mapper.map({entity})
            entity_id = entity_map.get(entity)
            if not entity_id:
                error_msg = "Error while mapping principals. Failed to retrieve outcome of mapping"
                logger.error(error_msg)
                raise WorkspaceHandlerError([error_msg])

            if isinstance(entity_id, MapperError):
                raise entity_id  # Propagate the mapping error

            if role_definition_id.lower() == "no_permissions":
                self._handle_no_permissions(databricks_workspace_info, entity_id, permissions_settings)
            else:
                self._assign_permissions_to_entity(
                    databricks_workspace_info, entity_id, role_definition_id, principal_type, permissions_settings
                )

        except Exception as e:
            error_msg = (
                f"An error occurred while handling permissions for {entity} "
                f"on Azure resource '{databricks_workspace_info}'"
            )
            logger.error(
                "An error occurred while handling permissions for {} on Azure resource '{}'. Details: {}",
                entity,
                databricks_workspace_info.name,
                e,
            )
            raise WorkspaceHandlerError([error_msg]) from e

    def _handle_no_permissions(
        self,
        databricks_workspace_info: DatabricksWorkspaceInfo,
        entity_id: str,
        permissions_settings: AzurePermissionsSettings,
    ) -> None:
        """Removes all role assignments for a principal on the workspace resource."""
        logger.info(
            "Configuration is 'no_permissions'. Removing all roles for principal {} on workspace {}",
            entity_id,
            databricks_workspace_info.name,
        )

        permissions = self.azure_permissions_manager.get_principal_role_assignments_on_resource(
            resource_group_name=permissions_settings.resource_group,
            resource_provider_namespace="Microsoft.Databricks",
            resource_type="workspaces",
            resource_name=databricks_workspace_info.name,
            principal_id=entity_id,
        )

        errors: list[Exception] = []
        for role_assignment in permissions:
            if role_assignment.id and databricks_workspace_info.name in role_assignment.id:  # type:ignore
                try:
                    self.azure_permissions_manager.delete_role_assignment(
                        workspace_name=databricks_workspace_info.name, role_assignment_id=role_assignment.id
                    )
                except Exception as e:
                    errors.append(e)
        if errors:
            logger.error("{} errors found while deleting role assignments: {}", len(errors), errors)
            raise WorkspaceHandlerError([build_error_message_from_chained_exception(error) for error in errors])

        logger.info("Removed role assignments for {} on Azure resource {}", entity_id, databricks_workspace_info.name)

    def _assign_permissions_to_entity(
        self,
        databricks_workspace_info: DatabricksWorkspaceInfo,
        entity_id: str,
        role_definition_id: str,
        principal_type: PrincipalType,
        permissions_settings: AzurePermissionsSettings,
    ) -> None:
        """Assigns a specific role to a principal on the workspace resource."""
        resource_id = (
            f"/subscriptions/{settings.azure.auth.subscription_id}/"
            f"resourceGroups/{permissions_settings.resource_group}/"
            f"providers/Microsoft.Databricks/workspaces/{databricks_workspace_info.name}"
        )

        self.azure_permissions_manager.assign_permissions(
            resource_id=resource_id,
            permission_id=str(uuid.uuid4()),
            role_definition_id=role_definition_id,
            principal_id=entity_id,
            principal_type=principal_type,
        )
