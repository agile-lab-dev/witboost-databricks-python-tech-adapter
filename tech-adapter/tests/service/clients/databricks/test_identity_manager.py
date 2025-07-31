import unittest
from unittest.mock import MagicMock

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.iam import Group, User, WorkspacePermission

from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import IdentityManagerError
from src.service.clients.databricks.identity_manager import IdentityManager


class TestIdentityManager(unittest.TestCase):
    """Unit tests for the IdentityManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_account_client = MagicMock()

        # Test data
        self.workspace_id = 12345
        self.workspace_name = "test-workspace"
        self.workspace_info = DatabricksWorkspaceInfo(
            id=str(self.workspace_id),
            name=self.workspace_name,
            azure_resource_id="res-id-123",
            location="westeurope",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )
        self.username = "test.user@example.com"
        self.user_id = 67890
        self.group_name = "test-group"
        self.group_id = 98765

        # Class under test
        self.identity_manager = IdentityManager(self.mock_account_client, self.workspace_info)

    def test_create_or_update_user_with_admin_privileges_success(self):
        """Test successful assignment of a user with admin privileges."""
        # Arrange
        mock_user = User(user_name=self.username, id=str(self.user_id))
        self.mock_account_client.users.list.return_value = [mock_user]

        # Act
        self.identity_manager.create_or_update_user_with_admin_privileges(self.username)

        # Assert
        self.mock_account_client.users.list.assert_called_once_with(filter=f"userName eq '{self.username}'")
        self.mock_account_client.workspace_assignment.update.assert_called_once_with(
            workspace_id=self.workspace_id, principal_id=self.user_id, permissions=[WorkspacePermission.ADMIN]
        )
        self.mock_account_client.groups.list.assert_not_called()

    def test_create_or_update_user_fails_when_not_found(self):
        """Test failure when the user is not found at the account level."""
        # Arrange
        self.mock_account_client.users.list.return_value = []

        # Act & Assert
        with self.assertRaisesRegex(IdentityManagerError, "User .* not found"):
            self.identity_manager.create_or_update_user_with_admin_privileges(self.username)
        self.mock_account_client.workspace_assignment.update.assert_not_called()

    def test_create_or_update_user_fails_on_list_api_error(self):
        """Test failure when the user list API call raises an exception."""
        # Arrange
        self.mock_account_client.users.list.side_effect = Exception("API List Error")

        # Act & Assert
        with self.assertRaisesRegex(IdentityManagerError, "An error occurred while updating user"):
            self.identity_manager.create_or_update_user_with_admin_privileges(self.username)
        self.mock_account_client.workspace_assignment.update.assert_not_called()

    def test_create_or_update_user_fails_on_update_api_error(self):
        """Test failure when the workspace assignment update API call raises an exception."""
        # Arrange
        mock_user = User(user_name=self.username, id=str(self.user_id))
        self.mock_account_client.users.list.return_value = [mock_user]
        self.mock_account_client.workspace_assignment.update.side_effect = Exception("API Update Error")

        # Act & Assert
        with self.assertRaisesRegex(IdentityManagerError, "An error occurred while updating user"):
            self.identity_manager.create_or_update_user_with_admin_privileges(self.username)

    def test_create_or_update_group_with_user_privileges_success(self):
        """Test successful assignment of a group with user privileges."""
        # Arrange
        mock_group = Group(display_name=self.group_name, id=str(self.group_id))
        self.mock_account_client.groups.list.return_value = [mock_group]

        # Act
        self.identity_manager.create_or_update_group_with_user_privileges(self.group_name)

        # Assert
        self.mock_account_client.groups.list.assert_called_once_with(filter=f"displayName eq '{self.group_name}'")
        self.mock_account_client.workspace_assignment.update.assert_called_once_with(
            workspace_id=self.workspace_id, principal_id=self.group_id, permissions=[WorkspacePermission.USER]
        )
        self.mock_account_client.users.list.assert_not_called()

    def test_create_or_update_group_fails_when_not_found(self):
        """Test failure when the group is not found at the account level."""
        # Arrange
        self.mock_account_client.groups.list.return_value = []

        # Act & Assert
        with self.assertRaisesRegex(IdentityManagerError, "Group .* not found"):
            self.identity_manager.create_or_update_group_with_user_privileges(self.group_name)
        self.mock_account_client.workspace_assignment.update.assert_not_called()

    def test_create_or_update_principal_fails_if_id_is_none(self):
        """Test failure if the found principal has a null/None ID."""
        # Arrange: User case
        mock_user_no_id = User(user_name=self.username, id=None)
        self.mock_account_client.users.list.return_value = [mock_user_no_id]

        # Act & Assert: User case
        with self.assertRaisesRegex(IdentityManagerError, "User .* not found"):
            self.identity_manager.create_or_update_user_with_admin_privileges(self.username)

        # Arrange: Group case
        mock_group_no_id = Group(display_name=self.group_name, id=None)
        self.mock_account_client.groups.list.return_value = [mock_group_no_id]

        # Act & Assert: Group case
        with self.assertRaisesRegex(IdentityManagerError, "Group .* not found"):
            self.identity_manager.create_or_update_group_with_user_privileges(self.group_name)

        # Verify no update calls were made
        self.mock_account_client.workspace_assignment.update.assert_not_called()
