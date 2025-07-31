import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from azure.mgmt.authorization.models import PrincipalType
from azure.mgmt.databricks.models import ProvisioningState

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import MapperError
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    JobClusterSpecific,
)
from src.models.exceptions import WorkspaceHandlerError
from src.service.clients.azure.azure_workspace_handler import (
    AzureAuthWorkspaceClientConfigParams,
    AzureWorkspaceHandler,
    OAuthWorkspaceClientConfigParams,
    create_workspace_client,
)
from src.settings.databricks_tech_adapter_settings import AzureAuthSettings, DatabricksAuthSettings


# Test the factory function separately, as it has no dependencies on the class.
@patch("src.service.clients.azure.azure_workspace_handler.WorkspaceClient")
class TestCreateWorkspaceClient(unittest.TestCase):
    def test_creates_with_azure_auth(self, MockWorkspaceClient):
        """Test client creation with Azure Service Principal authentication."""
        # Arrange
        params = AzureAuthWorkspaceClientConfigParams(
            workspace_host="https://host",
            workspace_name="ws-name",
            azure_auth_config=AzureAuthSettings(
                client_id="azure-cid",
                client_secret="azure-csec",
                tenant_id="azure-tid",
                subscription_id="subscription_id",
            ),
            databricks_auth_config=DatabricksAuthSettings(account_id="abcdef"),
        )

        # Act
        create_workspace_client(params)

        # Assert
        MockWorkspaceClient.assert_called_once_with(
            host="https://host",
            azure_client_id="azure-cid",
            azure_client_secret="azure-csec",
            azure_tenant_id="azure-tid",
        )

    def test_creates_with_oauth(self, MockWorkspaceClient):
        """Test client creation with Databricks OAuth M2M authentication."""
        # Arrange
        params = OAuthWorkspaceClientConfigParams(
            workspace_host="https://host",
            workspace_name="ws-name",
            databricks_client_id="db-cid",
            databricks_client_secret="db-csec",
        )

        # Act
        create_workspace_client(params)

        # Assert
        MockWorkspaceClient.assert_called_once_with(host="https://host", client_id="db-cid", client_secret="db-csec")


# Test the main handler class
class TestAzureWorkspaceHandler(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """Set up mocks for all dependencies."""
        self.mock_azure_workspace_manager = MagicMock(
            get_workspace=MagicMock(), create_if_not_exists_workspace=AsyncMock()
        )
        self.mock_azure_permissions_manager = MagicMock(
            assign_permissions=MagicMock(),
            get_principal_role_assignments_on_resource=MagicMock(),
            delete_role_assignment=MagicMock(),
        )
        self.mock_azure_mapper = MagicMock(map=AsyncMock())

        # Patch the global settings object for the duration of the test
        self.settings_patcher = patch("src.service.clients.azure.azure_workspace_handler.settings")
        self.mock_settings = self.settings_patcher.start()
        self.addCleanup(self.settings_patcher.stop)

        # Configure mock settings
        self.mock_settings.azure.auth.subscription_id = "sub-id"
        self.mock_settings.azure.auth.sku_type = "premium"
        self.mock_settings.azure.permissions = MagicMock(
            resource_group="rg-main",
            dp_owner_role_definition_id="owner-role-id",
            dev_group_role_definition_id="dev-role-id",
        )

        # Instantiate the class under test with mocked dependencies
        self.handler = AzureWorkspaceHandler(
            azure_workspace_manager=self.mock_azure_workspace_manager,
            azure_permissions_manager=self.mock_azure_permissions_manager,
            azure_mapper=self.mock_azure_mapper,
        )

        # Common test data
        self.workspace_name = "test-managed-ws"
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner="owner@test.com",
            devGroup="group:dev-group",
            name="dp-name",
            description="description",
            kind="dataproduct",
            domain="domain:domain",
            version="0.0.0",
            environment="development",
            ownerGroup="group:dev-group",
            specific={},
            components=[],
            tags=[],
        )
        self.component = JobWorkload(
            id="comp-id",
            name="comp-name",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace=self.workspace_name,
                repoPath="repoPath",
                jobName="jobName",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="gitRepoUrl", gitReference="gitReference", gitPath="gitPath", gitReferenceType="branch"
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    async def test_provision_workspace_for_unmanaged_url_skips_provisioning(self):
        """Test that an unmanaged workspace (URL) is identified and provisioning is skipped."""
        # Arrange
        unmanaged_url = "https://adb-12345.6.azuredatabricks.net"
        self.component.specific.workspace = unmanaged_url

        # Act
        result_info = await self.handler.provision_workspace(self.data_product, self.component)

        # Assert
        self.assertFalse(result_info.is_managed)
        self.assertEqual(result_info.databricks_host, unmanaged_url)
        self.mock_azure_workspace_manager.get_workspace.assert_not_called()
        self.mock_azure_workspace_manager.create_if_not_exists_workspace.assert_not_called()

    async def test_provision_workspace_for_new_managed_workspace(self):
        """Test full provisioning flow for a new managed workspace."""
        # Arrange
        # 1. Workspace does not exist initially
        self.mock_azure_workspace_manager.get_workspace.return_value = None
        # 2. Creation returns a new workspace info object
        self.mock_azure_workspace_manager.create_if_not_exists_workspace.return_value = self.workspace_info
        # 3. Mapper successfully maps owner and group
        self.mock_azure_mapper.map.return_value = {
            "owner@test.com": "owner-obj-id",
            "group:dev-group": "group-obj-id",
        }

        # Act
        result_info = await self.handler.provision_workspace(self.data_product, self.component)

        # Assert
        self.assertEqual(result_info, self.workspace_info)
        # Check creation call
        self.mock_azure_workspace_manager.create_if_not_exists_workspace.assert_awaited_once()
        # Check permission calls
        self.assertEqual(self.mock_azure_permissions_manager.assign_permissions.call_count, 2)
        assign_calls = self.mock_azure_permissions_manager.assign_permissions.call_args_list
        self.assertEqual(assign_calls[0].kwargs["principal_id"], "owner-obj-id")
        self.assertEqual(assign_calls[0].kwargs["principal_type"], PrincipalType.USER)
        self.assertEqual(assign_calls[1].kwargs["principal_id"], "group-obj-id")
        self.assertEqual(assign_calls[1].kwargs["principal_type"], PrincipalType.GROUP)

    def test_get_workspace_info_by_name_for_managed_workspace(self):
        """Test retrieving info for a managed workspace by name."""
        # Arrange
        self.mock_azure_workspace_manager.get_workspace.return_value = self.workspace_info

        # Act
        result_info = self.handler.get_workspace_info_by_name(self.workspace_name)

        # Assert
        self.assertEqual(result_info, self.workspace_info)
        expected_rg_id = f"/subscriptions/sub-id/resourceGroups/{self.workspace_name}-rg"
        self.mock_azure_workspace_manager.get_workspace.assert_called_once_with(self.workspace_name, expected_rg_id)

    async def test_manage_azure_permissions_for_no_permissions(self):
        """Test that 'no_permissions' logic correctly removes existing roles."""
        # Arrange, we're mocking RoleAssignment since is a readonly class
        role_assignment_to_delete = MagicMock()
        role_assignment_to_delete.id = "test-workspace/role-assignment-id"
        self.mock_azure_permissions_manager.get_principal_role_assignments_on_resource.return_value = [
            role_assignment_to_delete
        ]
        self.mock_azure_mapper.map.return_value = {"principal-to-clean": "principal-obj-id"}

        # Act
        await self.handler._manage_azure_permissions(
            self.workspace_info,
            entity="principal-to-clean",
            permissions_settings=self.mock_settings.azure.permissions,
            role_definition_id="no_permissions",  # Special keyword
            principal_type=PrincipalType.USER,
        )

        # Assert
        self.mock_azure_permissions_manager.get_principal_role_assignments_on_resource.assert_called_once()
        self.mock_azure_permissions_manager.delete_role_assignment.assert_called_once_with(
            workspace_name=self.workspace_info.name, role_assignment_id="test-workspace/role-assignment-id"
        )
        self.mock_azure_permissions_manager.assign_permissions.assert_not_called()

    async def test_provision_workspace_fails_if_mapper_fails(self):
        """Test that provisioning fails if the AzureMapper returns an error."""
        # Arrange
        self.mock_azure_workspace_manager.get_workspace.return_value = None
        self.mock_azure_workspace_manager.create_if_not_exists_workspace.return_value = self.workspace_info
        # Simulate a mapping failure for the owner
        self.mock_azure_mapper.map.return_value = {"owner@test.com": MapperError("User not found")}

        # Act & Assert
        with self.assertRaises(WorkspaceHandlerError):
            await self.handler.provision_workspace(self.data_product, self.component)

    def test_get_workspace_info_by_name_for_unmanaged_workspace(self):
        """Test retrieving info for an unmanaged workspace via its URL."""
        # Arrange
        unmanaged_url = "https://adb-555123.7.azuredatabricks.net"

        # Act
        result_info = self.handler.get_workspace_info_by_name(unmanaged_url)

        # Assert
        self.assertIsNotNone(result_info)
        self.assertFalse(result_info.is_managed)
        self.assertEqual(result_info.databricks_host, unmanaged_url)
        self.assertEqual(result_info.id, "555123")  # Verify regex parsing
        # Crucially, no call should be made to the Azure API for a URL
        self.mock_azure_workspace_manager.get_workspace.assert_not_called()

    def test_get_workspace_info_from_component(self):
        """
        Test getting workspace info from a component

        """
        self.mock_azure_workspace_manager.get_workspace.return_value = self.workspace_info

        # Act
        result_info = self.handler.get_workspace_info(self.component)

        # Assert
        # 1. Check that the final result is correct.
        self.assertEqual(result_info, self.workspace_info)

        # 2. Verify that the external dependency was called correctly. This proves
        #    the internal call chain worked as expected.
        expected_rg_id = (
            f"/subscriptions/{self.mock_settings.azure.auth.subscription_id}/resourceGroups/{self.workspace_name}-rg"
        )
        self.mock_azure_workspace_manager.get_workspace.assert_called_once_with(self.workspace_name, expected_rg_id)
