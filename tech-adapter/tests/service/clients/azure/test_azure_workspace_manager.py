import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from azure.core.exceptions import ResourceExistsError
from azure.mgmt.databricks.models import ProvisioningState, Workspace

from src.models.databricks.exceptions import AzureWorkspaceManagerError
from src.service.clients.azure.azure_workspace_manager import AzureWorkspaceManager


class TestAzureWorkspaceManager(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the AzureWorkspaceManager class."""

    def setUp(self):
        """Set up mocks for all external dependencies."""
        self.mock_sync_client = MagicMock()
        self.mock_async_client = AsyncMock()

        # We must patch the global settings object to provide controlled test values.
        self.settings_patcher = patch("src.service.clients.azure.azure_workspace_manager.settings")
        self.mock_settings = self.settings_patcher.start()
        self.addCleanup(self.settings_patcher.stop)  # Ensures the patch is stopped after each test

        # Configure mock settings values required by the class
        self.mock_settings.azure.permissions = MagicMock(resource_group="test-rg-main")
        self.mock_settings.azure.auth = MagicMock(subscription_id="test-sub-id", tenant_id="test-tenant-id")

        # Instantiate the class under test with mocked clients
        self.manager = AzureWorkspaceManager(self.mock_sync_client, self.mock_async_client)

        # Common test data
        self.workspace_name = "test-workspace"
        self.region = "westeurope"
        self.managed_rg_id = f"/subscriptions/test-sub-id/resourceGroups/{self.workspace_name}-rg"
        self.sku_type = MagicMock(value="premium")
        self.mock_existing_workspace_info = MagicMock(name="existing-ws")

    # --- Tests for get_workspace ---

    def test_get_workspace_success(self):
        """Test successful retrieval of an existing workspace."""
        # Arrange
        # Create mock Workspace objects to be returned by the SDK
        matching_workspace = Workspace(
            managed_resource_group_id=self.managed_rg_id,
            location=self.region,
        )
        # Use a different case to test the case-insensitive comparison
        matching_workspace.name = self.workspace_name.upper()
        matching_workspace.workspace_id = "ws-id-123"
        matching_workspace.workspace_url = "https://host.com"
        matching_workspace.id = "azure-res-id"
        matching_workspace.provisioning_state = ProvisioningState.SUCCEEDED

        non_matching_workspace = Workspace(managed_resource_group_id="other-rg-id", location=self.region)
        non_matching_workspace.name = "other-workspace"

        self.mock_sync_client.workspaces.list_by_subscription.return_value = [
            non_matching_workspace,
            matching_workspace,
        ]

        # Act
        result = self.manager.get_workspace(self.workspace_name, self.managed_rg_id)

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(result.name.lower(), self.workspace_name.lower())
        self.assertEqual(result.id, "ws-id-123")
        self.mock_sync_client.workspaces.list_by_subscription.assert_called_once()

    def test_get_workspace_not_found(self):
        """Test that None is returned when no matching workspace is found."""
        # Arrange
        workspace = Workspace(managed_resource_group_id="other-rg", location=self.region)
        workspace.name = "other-ws"
        self.mock_sync_client.workspaces.list_by_subscription.return_value = [workspace]

        # Act
        result = self.manager.get_workspace(self.workspace_name, self.managed_rg_id)

        # Assert
        self.assertIsNone(result)

    def test_get_workspace_api_error(self):
        """Test that an API error during retrieval is wrapped in the correct exception."""
        # Arrange
        self.mock_sync_client.workspaces.list_by_subscription.side_effect = Exception("API is down")

        # Act & Assert
        with self.assertRaisesRegex(AzureWorkspaceManagerError, "An error occurred while getting info for workspace"):
            self.manager.get_workspace(self.workspace_name, self.managed_rg_id)

    # --- Tests for create_if_not_exists_workspace ---

    async def test_create_if_not_exists_skips_if_workspace_exists(self):
        """Test that creation is skipped if the workspace already exists."""
        # Arrange: Mock the synchronous client to return an existing workspace.
        workspace = Workspace(managed_resource_group_id=self.managed_rg_id, location=self.region)
        workspace.name = self.workspace_name
        workspace.provisioning_state = ProvisioningState.SUCCEEDED
        workspace.id = "new-azure-res-id"
        workspace.workspace_id = "new-ws-id"
        workspace.workspace_url = "https://new-host.com"
        self.mock_sync_client.workspaces.list_by_subscription.return_value = [workspace]

        # Act
        result = await self.manager.create_if_not_exists_workspace(
            self.workspace_name, self.region, "test-rg-main", self.managed_rg_id, self.sku_type
        )

        # Assert
        self.assertIsNotNone(result)
        # Verify that the async creation method was never called
        self.mock_async_client.workspaces.begin_create_or_update.assert_not_called()

    async def test_create_if_not_exists_success(self):
        """Test the successful creation of a new workspace."""
        # Arrange
        # 1. No existing workspace is found
        self.mock_sync_client.workspaces.list_by_subscription.return_value = []

        # 2. Mock the async poller and its result
        mock_poller = AsyncMock()
        mock_workspace_result = Workspace(
            location=self.region,
            managed_resource_group_id=self.managed_rg_id,
        )
        mock_workspace_result.provisioning_state = ProvisioningState.SUCCEEDED
        mock_workspace_result.id = "new-azure-res-id"
        mock_workspace_result.name = self.workspace_name
        mock_workspace_result.workspace_id = "new-ws-id"
        mock_workspace_result.workspace_url = "https://new-host.com"
        mock_poller.result.return_value = mock_workspace_result
        self.mock_async_client.workspaces.begin_create_or_update.return_value = mock_poller

        # Act
        result = await self.manager.create_if_not_exists_workspace(
            self.workspace_name, self.region, "test-rg-main", self.managed_rg_id, self.sku_type
        )

        # Assert
        self.assertIsNotNone(result)
        self.assertTrue(result.is_managed)
        self.assertEqual(result.name, self.workspace_name)
        self.assertEqual(result.id, "new-ws-id")
        self.mock_async_client.workspaces.begin_create_or_update.assert_awaited_once()

        # Verify the parameters passed to the create call
        _, kwargs = self.mock_async_client.workspaces.begin_create_or_update.await_args
        params = kwargs["parameters"]
        self.assertEqual(params.location, self.region)
        self.assertEqual(params.sku.name, self.sku_type.value)

    async def test_create_if_not_exists_fails_if_provisioning_state_not_succeeded(self):
        """Test that an error is raised if the created workspace is not in a Succeeded state."""
        # Arrange
        self.mock_sync_client.workspaces.list_by_subscription.return_value = []
        mock_poller = AsyncMock()
        mock_poller.result.return_value = Workspace(
            provisioning_state=ProvisioningState.FAILED,
            location=self.region,
            managed_resource_group_id=self.managed_rg_id,
        )
        self.mock_async_client.workspaces.begin_create_or_update.return_value = mock_poller

        # Act & Assert
        with self.assertRaisesRegex(AzureWorkspaceManagerError, "state is not yet Succeeded"):
            await self.manager.create_if_not_exists_workspace(
                self.workspace_name, self.region, "test-rg-main", self.managed_rg_id, self.sku_type
            )

    async def test_create_if_not_exists_handles_resource_exists_error(self):
        """Test that a ResourceExistsError from the SDK is handled and wrapped."""
        # Arrange
        self.mock_sync_client.workspaces.list_by_subscription.return_value = []
        self.mock_async_client.workspaces.begin_create_or_update.side_effect = ResourceExistsError(
            "Simulating a race condition where creation has just started."
        )

        # Act & Assert
        with self.assertRaisesRegex(AzureWorkspaceManagerError, "is currently being created"):
            await self.manager.create_if_not_exists_workspace(
                self.workspace_name, self.region, "test-rg-main", self.managed_rg_id, self.sku_type
            )
