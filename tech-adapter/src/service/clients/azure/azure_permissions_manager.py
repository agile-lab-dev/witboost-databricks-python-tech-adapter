from typing import List

from azure.core.exceptions import ResourceExistsError
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.authorization.models import PrincipalType, RoleAssignment, RoleAssignmentCreateParameters
from loguru import logger

from src.models.databricks.exceptions import AzurePermissionsError


class AzurePermissionsManager:
    """
    Manages Azure Role-Based Access Control (RBAC) assignments for resources.
    """

    def __init__(self, auth_client: AuthorizationManagementClient):
        """
        Initializes the AzurePermissionsManager.

        Args:
            auth_client: An authenticated client for Azure authorization operations.
        """
        self.auth_client = auth_client

    def assign_permissions(
        self,
        resource_id: str,
        permission_id: str,
        role_definition_id: str,
        principal_id: str,
        principal_type: PrincipalType,
    ) -> None:
        """
        Assigns a role to a principal for a specific resource scope.

        If the role assignment already exists, the operation is skipped gracefully.

        Args:
            resource_id: The scope of the resource (e.g., a subscription, resource group, or specific resource).
            permission_id: A unique GUID for the role assignment name.
            role_definition_id: The full resource ID of the role definition to assign.
            principal_id: The object ID of the principal (user, group, or service principal).
            principal_type: The type of the principal (e.g., PrincipalType.USER).

        Raises:
            AzurePermissionsError: If the role assignment fails for any reason other than
                                   it already existing.
        """
        try:
            logger.info(
                "Assigning permissions for principal [ID: {}, Type: {}] on resource [ID: {}]",
                principal_id,
                principal_type,
                resource_id,
            )
            parameters = RoleAssignmentCreateParameters(  # type:ignore[call-arg]
                role_definition_id=role_definition_id,
                principal_id=principal_id,
                principal_type=principal_type,
            )
            self.auth_client.role_assignments.create(
                scope=resource_id,
                role_assignment_name=permission_id,
                parameters=parameters,
            )
            logger.success(
                "Successfully assigned role '{}' to principal '{}' on resource '{}'.",
                role_definition_id,
                principal_id,
                resource_id,
            )
        except ResourceExistsError:
            logger.info(
                "Role assignment already exists for principal [ID: {}] on resource [ID: {}]. Skipping creation.",
                principal_id,
                resource_id,
            )
        except Exception as e:
            error_msg = (
                f"Error assigning permissions for principal " f"[ID: {principal_id}] on resource [ID: {resource_id}]"
            )
            logger.error(
                "Error assigning permissions for principal [ID: {}] on resource [ID: {}]. Details: {}",
                principal_id,
                resource_id,
                e,
            )
            raise AzurePermissionsError(error_msg) from e

    def get_principal_role_assignments_on_resource(
        self,
        resource_group_name: str,
        resource_provider_namespace: str,
        resource_type: str,
        resource_name: str,
        principal_id: str,
    ) -> List[RoleAssignment]:
        """
        Retrieves the role assignments for a specific principal on a given Azure resource.

        Args:
            resource_group_name: The name of the resource group containing the resource.
            resource_provider_namespace: The provider namespace (e.g., 'Microsoft.Databricks').
            resource_type: The type of the resource (e.g., 'workspaces').
            resource_name: The name of the resource.
            principal_id: The object ID of the principal to filter by.

        Returns:
            A list of RoleAssignment objects for the specified principal.

        Raises:
            AzurePermissionsError: If retrieving the role assignments fails.
        """
        logger.info(
            "Retrieving role assignments for principal [ID: {}] on resource [Name: {}, Group: {}].",
            principal_id,
            resource_name,
            resource_group_name,
        )
        try:
            all_assignments = self.auth_client.role_assignments.list_for_resource(
                resource_group_name=resource_group_name,
                resource_provider_namespace=resource_provider_namespace,
                parent_resource_path="",  # Should be empty
                resource_type=resource_type,
                resource_name=resource_name,
            )
            # Filter the assignments for the specified principal
            return [ra for ra in all_assignments if ra.principal_id and ra.principal_id.lower() == principal_id.lower()]
        except Exception as e:
            error_msg = (
                f"Error retrieving role assignments for principal [ID: {principal_id}] "
                f"on resource [Name: {resource_name}]."
            )
            logger.error(
                "Error retrieving role assignments for principal [ID: {}] on resource [Name: {}]. Details: {}",
                principal_id,
                resource_name,
                e,
            )
            raise AzurePermissionsError(error_msg) from e

    def delete_role_assignment(self, workspace_name: str, role_assignment_id: str) -> None:
        """
        Deletes a specific role assignment by its fully qualified ID.

        Args:
            workspace_name: The name of the resource (used for logging).
            role_assignment_id: The fully qualified ID of the role assignment to delete.

        Raises:
            AzurePermissionsError: If the deletion fails.
        """
        logger.info(
            "Deleting role assignment [ID: {}] on resource [Name: {}].",
            role_assignment_id,
            workspace_name,
        )
        try:
            self.auth_client.role_assignments.delete_by_id(role_assignment_id)
            logger.success(
                "Successfully deleted role assignment [ID: {}] on resource [Name: {}].",
                role_assignment_id,
                workspace_name,
            )
        except Exception as e:
            error_msg = (
                f"Error deleting role assignment [ID: {role_assignment_id}] " f"on resource [Name: {workspace_name}]."
            )
            logger.error(
                "Error deleting role assignment [ID: {}] on resource [Name: {}]. Details: {}",
                role_assignment_id,
                workspace_name,
                e,
            )
            raise AzurePermissionsError(error_msg.format(role_assignment_id, workspace_name)) from e
