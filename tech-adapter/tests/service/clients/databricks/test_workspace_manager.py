import unittest
from unittest.mock import MagicMock

from databricks.sdk import Workspace
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sdk.service.oauth2 import CreateServicePrincipalSecretResponse
from databricks.sdk.service.sql import GetWarehouseResponse
from databricks.sdk.service.workspace import CredentialInfo

from src.models.databricks.exceptions import DatabricksWorkspaceManagerError
from src.service.clients.databricks.workspace_manager import WorkspaceManager
from src.settings.databricks_tech_adapter_settings import GitSettings


class TestWorkspaceManager(unittest.TestCase):
    """Unit tests for the WorkspaceManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_workspace_client = MagicMock()
        self.mock_account_client = MagicMock()

        self.manager = WorkspaceManager(self.mock_workspace_client, self.mock_account_client)

        # Common test data
        self.workspace_host = "https://adb-1234567890.1.azuredatabricks.net"
        self.workspace_name = "test-workspace"
        self.sql_warehouse_name = "test-warehouse"
        self.sql_warehouse_id = "wh-id-123"
        self.cluster_name = "test-cluster"
        self.cluster_id = "cluster-id-456"
        self.sp_name = "Test Service Principal"
        self.sp_app_id = "app-id-789"
        self.sp_id = 98765

        # Mock the workspace client config
        self.mock_workspace_client.config.host = self.workspace_host

    def tearDown(self):
        """Clear caches after each test to ensure test isolation."""
        self.manager.get_workspace_name.cache_clear()

    def test_get_workspace_name_success(self):
        """Test successful retrieval of the workspace name."""
        # Arrange
        workspaces = [
            Workspace(deployment_name="adb-9999999999.9", workspace_name="other"),
            Workspace(deployment_name="adb-1234567890.1", workspace_name=self.workspace_name),
        ]
        self.mock_account_client.workspaces.list.return_value = workspaces

        # Act
        result_name = self.manager.get_workspace_name()

        # Assert
        self.assertEqual(result_name, self.workspace_name)
        self.mock_account_client.workspaces.list.assert_called_once()

    def test_get_workspace_name_is_cached(self):
        """Test that get_workspace_name result is cached after the first call."""
        # Arrange
        workspaces = [Workspace(deployment_name="adb-1234567890.1", workspace_name=self.workspace_name)]
        self.mock_account_client.workspaces.list.return_value = workspaces

        # Act
        result1 = self.manager.get_workspace_name()
        result2 = self.manager.get_workspace_name()  # This call should hit the cache

        # Assert
        self.assertEqual(result1, self.workspace_name)
        self.assertEqual(result2, self.workspace_name)
        # Verify the expensive API call was only made ONCE
        self.mock_account_client.workspaces.list.assert_called_once()

    def test_get_workspace_name_not_found(self):
        """Test failure when no workspace matches the configured host."""
        # Arrange
        self.mock_account_client.workspaces.list.return_value = [
            Workspace(deployment_name="adb-9999999999.9", workspace_name="other")
        ]

        # Act & Assert
        with self.assertRaisesRegex(DatabricksWorkspaceManagerError, "No workspace found for host"):
            self.manager.get_workspace_name()

    def test_get_sql_warehouse_id_from_name_success(self):
        """Test successful retrieval of a SQL warehouse ID."""
        # Arrange
        warehouses = [
            GetWarehouseResponse(name="other-wh", id="other-id"),
            GetWarehouseResponse(name=self.sql_warehouse_name, id=self.sql_warehouse_id),
        ]
        self.mock_workspace_client.warehouses.list.return_value = warehouses

        # Act
        result_id = self.manager.get_sql_warehouse_id_from_name(self.sql_warehouse_name)

        # Assert
        self.assertEqual(result_id, self.sql_warehouse_id)

    def test_get_compute_cluster_id_from_name_not_found(self):
        """Test failure when a compute cluster is not found."""
        # Arrange
        self.mock_workspace_client.clusters.list.return_value = []
        self.manager.get_workspace_name = MagicMock(return_value=self.workspace_name)

        # Act & Assert
        with self.assertRaisesRegex(DatabricksWorkspaceManagerError, f"Cluster '{self.cluster_name}' not found"):
            self.manager.get_compute_cluster_id_from_name(self.cluster_name)

    def test_set_git_credentials_creates_new(self):
        """Test that new Git credentials are created when none exist."""
        # Arrange
        git_config = GitSettings(username="testuser", token="testtoken", provider="gitLab")
        self.mock_workspace_client.git_credentials.list.return_value = []
        self.manager.get_workspace_name = MagicMock(return_value=self.workspace_name)

        # Act
        self.manager.set_git_credentials(git_config)

        # Assert
        self.mock_workspace_client.git_credentials.list.assert_called_once()
        self.mock_workspace_client.git_credentials.create.assert_called_once_with(
            personal_access_token=git_config.token, git_username=git_config.username, git_provider=git_config.provider
        )
        self.mock_workspace_client.git_credentials.update.assert_not_called()

    def test_set_git_credentials_updates_existing(self):
        """Test that existing Git credentials for the same provider are updated."""
        # Arrange
        git_config = GitSettings(username="newuser", token="newtoken", provider="gitLab")
        existing_cred = CredentialInfo(credential_id=777, git_provider="gitLab")
        self.mock_workspace_client.git_credentials.list.return_value = [existing_cred]
        self.manager.get_workspace_name = MagicMock(return_value=self.workspace_name)  # Simplify

        # Act
        self.manager.set_git_credentials(git_config)

        # Assert
        self.mock_workspace_client.git_credentials.list.assert_called_once()
        self.mock_workspace_client.git_credentials.update.assert_called_once_with(
            credential_id=existing_cred.credential_id,
            git_username=git_config.username,
            personal_access_token=git_config.token,
            git_provider=git_config.provider,
        )
        self.mock_workspace_client.git_credentials.create.assert_not_called()

    def test_get_service_principal_from_name_success(self):
        """Test successful retrieval of a service principal by its display name."""
        # Arrange
        expected_sp = ServicePrincipal(display_name=self.sp_name, application_id=self.sp_app_id)
        self.mock_workspace_client.service_principals.list.return_value = [
            ServicePrincipal(display_name="other-sp"),
            expected_sp,
        ]
        self.manager.get_workspace_name = MagicMock(return_value=self.workspace_name)

        # Act
        result_sp = self.manager.get_service_principal_from_name(self.sp_name)

        # Assert
        self.assertEqual(result_sp, expected_sp)

    def test_get_service_principal_from_name_not_found(self):
        """Test that None is returned when a service principal name is not found."""
        # Arrange
        self.mock_workspace_client.service_principals.list.return_value = []
        self.manager.get_workspace_name = MagicMock(return_value=self.workspace_name)

        # Act
        result = self.manager.get_service_principal_from_name(self.sp_name)

        # Assert
        self.assertIsNone(result)

    def test_generate_secret_for_service_principal_success(self):
        """Test successful generation of a service principal secret."""
        # Arrange
        secret_id = "secret-id-1"
        secret_value = "secret-value"
        lifetime = 3600
        self.mock_account_client.service_principal_secrets.create.return_value = CreateServicePrincipalSecretResponse(
            id=secret_id, secret=secret_value
        )

        # Act
        result_id, result_secret = self.manager.generate_secret_for_service_principal(self.sp_id, lifetime)

        # Assert
        self.assertEqual(result_id, secret_id)
        self.assertEqual(result_secret, secret_value)
        self.mock_account_client.service_principal_secrets.create.assert_called_once_with(
            service_principal_id=self.sp_id, lifetime=f"{lifetime}s"
        )

    def test_delete_service_principal_secret(self):
        """Test successful deletion of a service principal secret."""
        # Arrange
        secret_id_to_delete = "secret-id-to-delete"

        # Act
        self.manager.delete_service_principal_secret(self.sp_id, secret_id_to_delete)

        # Assert
        self.mock_account_client.service_principal_secrets.delete.assert_called_once_with(
            service_principal_id=self.sp_id, secret_id=secret_id_to_delete
        )
