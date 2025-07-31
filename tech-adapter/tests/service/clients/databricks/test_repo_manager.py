import os
import unittest
from unittest.mock import MagicMock

from databricks.sdk.errors import ResourceAlreadyExists, ResourceDoesNotExist
from databricks.sdk.service.workspace import (
    CreateRepoResponse,
    GetRepoResponse,
    ObjectInfo,
    ObjectType,
    RepoAccessControlRequest,
    RepoAccessControlResponse,
    RepoPermission,
    RepoPermissionLevel,
    RepoPermissions,
)

from src.models.databricks.exceptions import RepoManagerError
from src.service.clients.databricks.repo_manager import RepoManager


class TestRepoManager(unittest.TestCase):
    """Unit tests for the RepoManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_workspace_client = MagicMock()
        self.workspace_name = "test-workspace"
        self.repo_manager = RepoManager(self.mock_workspace_client, self.workspace_name)

        # Common test data
        self.git_url = "https://gitlab.com/test/repo.git"
        self.provider = "gitLab"
        self.repo_id = 12345
        self.absolute_repo_path = "/Repos/test-org/test-repo"
        self.username = "test.user@example.com"
        self.group_name = "test-group"

    def test_create_repo_success(self):
        """Test successful creation of a new repository."""
        # Arrange
        self.mock_workspace_client.repos.create.return_value = CreateRepoResponse(id=self.repo_id)

        # Act
        result_id = self.repo_manager.create_repo(self.git_url, self.provider, self.absolute_repo_path)

        # Assert
        self.assertEqual(result_id, self.repo_id)
        self.mock_workspace_client.repos.create.assert_called_once_with(
            url=self.git_url, provider=self.provider, path=self.absolute_repo_path
        )

    def test_create_repo_already_exists_handled(self):
        """Test handling of a repository that already exists using ResourceAlreadyExists."""
        # Arrange
        self.mock_workspace_client.repos.create.side_effect = ResourceAlreadyExists("it exists")
        parent_folder = os.path.dirname(self.absolute_repo_path)
        mock_object_info = ObjectInfo(object_id=self.repo_id, object_type=ObjectType.REPO, path=self.absolute_repo_path)
        self.mock_workspace_client.workspace.list.return_value = [mock_object_info]
        self.mock_workspace_client.repos.get.return_value = GetRepoResponse(
            id=self.repo_id, path=self.absolute_repo_path
        )

        # Act
        result_id = self.repo_manager.create_repo(self.git_url, self.provider, self.absolute_repo_path)

        # Assert
        self.assertEqual(result_id, self.repo_id)
        self.mock_workspace_client.workspace.list.assert_called_once_with(parent_folder)
        self.mock_workspace_client.repos.get.assert_called_once_with(self.repo_id)

    def test_create_repo_fails_if_existing_cannot_be_retrieved(self):
        """Test failure if an existing repo cannot be found after an exists error."""
        # Arrange
        self.mock_workspace_client.repos.create.side_effect = ResourceAlreadyExists("it exists")
        mock_object_info = ObjectInfo(
            object_id=self.repo_id, object_type=ObjectType.DIRECTORY, path=self.absolute_repo_path
        )
        self.mock_workspace_client.workspace.list.return_value = [mock_object_info]

        # Act & Assert
        with self.assertRaisesRegex(RepoManagerError, "exists but could not be retrieved"):
            self.repo_manager.create_repo(self.git_url, self.provider, self.absolute_repo_path)

    def test_delete_repo_success(self):
        """Test successful deletion of a repository."""
        # Arrange
        parent_folder = os.path.dirname(self.absolute_repo_path)
        mock_object_info = ObjectInfo(object_id=self.repo_id, object_type=ObjectType.REPO, path=self.absolute_repo_path)
        self.mock_workspace_client.workspace.list.return_value = [mock_object_info]
        self.mock_workspace_client.repos.get.return_value = GetRepoResponse(id=self.repo_id, url=self.git_url)

        # Act
        self.repo_manager.delete_repo(self.git_url, self.absolute_repo_path)

        # Assert
        self.mock_workspace_client.workspace.list.assert_called_once_with(parent_folder)
        self.mock_workspace_client.repos.get.assert_called_once_with(self.repo_id)
        self.mock_workspace_client.repos.delete.assert_called_once_with(repo_id=self.repo_id)

    def test_delete_repo_skips_if_not_found(self):
        """Test that deletion is skipped if the repository does not exist."""
        # Arrange
        self.mock_workspace_client.workspace.list.return_value = []  # Repo not found in parent dir

        # Act
        try:
            self.repo_manager.delete_repo(self.git_url, self.absolute_repo_path)
        except RepoManagerError:
            self.fail("RepoManagerError raised for a non-existent repo, but it should be skipped.")

        # Assert
        self.mock_workspace_client.repos.delete.assert_not_called()

    def test_delete_repo_skips_on_resource_does_not_exist(self):
        """Test that deletion skips on ResourceDoesNotExist, e.g., a race condition."""
        # Arrange
        mock_object_info = ObjectInfo(object_id=self.repo_id, object_type=ObjectType.REPO, path=self.absolute_repo_path)
        self.mock_workspace_client.workspace.list.return_value = [mock_object_info]
        self.mock_workspace_client.repos.get.return_value = GetRepoResponse(id=self.repo_id, url=self.git_url)
        self.mock_workspace_client.repos.delete.side_effect = ResourceDoesNotExist("deleted by another process")

        # Act
        try:
            self.repo_manager.delete_repo(self.git_url, self.absolute_repo_path)
        except RepoManagerError:
            self.fail("RepoManagerError raised on ResourceDoesNotExist, but it should be skipped.")

        # Assert
        self.mock_workspace_client.repos.delete.assert_called_once()

    def test_assign_permissions_to_user_success(self):
        """Test assigning permissions to a user."""
        # Arrange
        self.mock_workspace_client.repos.get_permissions.return_value = RepoPermissions(access_control_list=[])

        # Act
        self.repo_manager.assign_permissions_to_user(str(self.repo_id), self.username, RepoPermissionLevel.CAN_MANAGE)

        # Assert
        expected_acl = [
            RepoAccessControlRequest(
                user_name=self.username, group_name=None, permission_level=RepoPermissionLevel.CAN_MANAGE
            )
        ]
        self.mock_workspace_client.repos.set_permissions.assert_called_once_with(
            repo_id=str(self.repo_id), access_control_list=expected_acl
        )

    def test_remove_permissions_from_group_success(self):
        """Test removing permissions from a group."""
        # Arrange
        initial_acl_response = [
            RepoAccessControlResponse(
                group_name=self.group_name,
                all_permissions=[RepoPermission(permission_level=RepoPermissionLevel.CAN_EDIT, inherited=False)],
            ),
            RepoAccessControlResponse(
                user_name=self.username,
                all_permissions=[RepoPermission(permission_level=RepoPermissionLevel.CAN_MANAGE, inherited=False)],
            ),
        ]
        self.mock_workspace_client.repos.get_permissions.return_value = RepoPermissions(
            access_control_list=initial_acl_response
        )

        # Act
        self.repo_manager.remove_permissions_from_group(str(self.repo_id), self.group_name)

        # Assert
        # The group should be removed, leaving only the user
        final_acl_request = [
            RepoAccessControlRequest(
                user_name=self.username, group_name=None, permission_level=RepoPermissionLevel.CAN_MANAGE
            )
        ]
        self.mock_workspace_client.repos.set_permissions.assert_called_once_with(
            repo_id=str(self.repo_id), access_control_list=final_acl_request
        )

    def test_remove_permissions_does_nothing_if_principal_not_found(self):
        """Test that removing permissions does nothing if the principal is not in the ACL."""
        # Arrange
        initial_acl_response = [
            RepoAccessControlResponse(
                user_name=self.username,
                all_permissions=[RepoPermission(permission_level=RepoPermissionLevel.CAN_MANAGE, inherited=False)],
            )
        ]
        self.mock_workspace_client.repos.get_permissions.return_value = RepoPermissions(
            access_control_list=initial_acl_response
        )

        # Act
        self.repo_manager.remove_permissions_from_user(
            str(self.repo_id), "another.user@example.com"
        )  # Removing a different user

        # Assert
        self.mock_workspace_client.repos.set_permissions.assert_not_called()

    def test_assign_permissions_fails_on_api_error(self):
        """Test failure when assigning permissions due to an API error."""
        # Arrange
        self.mock_workspace_client.repos.get_permissions.side_effect = Exception("API Error")

        # Act & Assert
        with self.assertRaisesRegex(RepoManagerError, "Error assigning permission"):
            self.repo_manager.assign_permissions_to_user(str(self.repo_id), self.username, RepoPermissionLevel.CAN_EDIT)

    def test_convert_permissions_fails_on_empty_permission_level(self):
        """Test failure in permission conversion if a response has no permission level."""
        # Arrange
        bad_acl_response = [
            RepoAccessControlResponse(
                user_name=self.username,
                all_permissions=[],  # No permission level provided
            )
        ]

        # Act & Assert
        with self.assertRaisesRegex(RepoManagerError, "has no permission levels"):
            self.repo_manager._convert_to_access_control_requests(bad_acl_response)
