from typing import Literal, Union

from databricks.sdk import AccountClient
from databricks.sdk.service.iam import Group, User, WorkspacePermission
from loguru import logger

from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import IdentityManagerError


class IdentityManager:
    """
    Manages user and group assignments to a specific Databricks workspace
    by interacting with the Databricks account-level APIs.
    """

    def __init__(self, account_client: AccountClient, workspace_info: DatabricksWorkspaceInfo):
        """
        Initializes the IdentityManager.

        Args:
            account_client: An authenticated Databricks AccountClient.
            workspace_info: Information about the target workspace.
        """
        self.account_client = account_client
        self.workspace_info = workspace_info

    def create_or_update_user_with_admin_privileges(self, username: str) -> None:
        """
        Assigns a user to the workspace with ADMIN privileges.

        This method finds the user at the account level by their username (email)
        and then creates or updates their assignment in the target workspace to
        ensure they have ADMIN permissions.

        Args:
            username: The username (email address) of the user.

        Raises:
            IdentityManagerError: If the user is not found at the account level or
                                  if the assignment operation fails.
        """
        self._create_or_update_principal(username, "user")

    def create_or_update_group_with_user_privileges(self, group_name: str) -> None:
        """
        Assigns a group to the workspace with USER privileges.

        This method finds the group at the account level by its display name
        and then creates or updates its assignment in the target workspace to
        ensure it has USER permissions.

        Args:
            group_name: The display name of the group.

        Raises:
            IdentityManagerError: If the group is not found at the account level or
                                  if the assignment operation fails.
        """
        self._create_or_update_principal(group_name, "group")

    def _create_or_update_principal(self, principal_name: str, principal_type: Literal["user", "group"]) -> None:
        """
        A generic helper to assign a principal (user or group) to the workspace.

        Args:
            principal_name: The name of the user or group.
            principal_type: The type of the principal, either 'user' or 'group'.

        Raises:
            IdentityManagerError: If the principal is not found or the API call fails.
        """
        logger.info("Importing/updating {} {} in {}", principal_type, principal_name, self.workspace_info.name)

        try:
            principals: Union[list[User], list[Group]] = []
            permissions: list[WorkspacePermission] = []
            if principal_type == "user":
                filter_str = f"userName eq '{principal_name}'"
                principals = list(self.account_client.users.list(filter=filter_str))
                permissions = [WorkspacePermission.ADMIN]
            else:  # group
                filter_str = f"displayName eq '{principal_name}'"
                principals = list(self.account_client.groups.list(filter=filter_str))
                permissions = [WorkspacePermission.USER]

            if not principals or len(principals) == 0 or not principals[0].id:
                error_msg = "{} {} not found at Databricks account level."
                logger.error(error_msg, principal_type.capitalize(), principal_name)
                raise IdentityManagerError(error_msg.format(principal_type.capitalize(), principal_name))

            self.account_client.workspace_assignment.update(
                workspace_id=int(self.workspace_info.id),
                principal_id=int(principals[0].id),
                permissions=permissions,
            )
            logger.success(
                "Successfully updated assignment for {} '{}' in workspace '{}'.",
                principal_type,
                principal_name,
                self.workspace_info.name,
            )

        except IdentityManagerError:
            raise
        except Exception as e:
            error_msg = (
                f"An error occurred while updating {principal_type} {principal_name} " f"in {self.workspace_info.name}."
            )
            logger.error(
                "An error occurred while updating {} {} in {}. Details: {}",
                principal_type,
                principal_name,
                self.workspace_info.name,
                e,
            )
            raise IdentityManagerError(error_msg) from e
