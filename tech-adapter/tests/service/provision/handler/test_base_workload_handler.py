import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel
from databricks.sdk.service.pipelines import PipelineAccessControlRequest, PipelinePermissionLevel
from databricks.sdk.service.workspace import RepoPermissionLevel

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import DatabricksMapperError
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    JobClusterSpecific,
)
from src.models.exceptions import ProvisioningError
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler
from src.settings.databricks_tech_adapter_settings import DatabricksRepoPermissionsSettings


class TestBaseWorkloadHandler(unittest.TestCase):
    """Unit tests for the BaseWorkloadHandler class."""

    def setUp(self):
        """Set up the test environment with minimal, direct dependencies."""
        self.mock_account_client = MagicMock()
        self.mock_workspace_client = MagicMock()

        # The handler under test
        self.handler = BaseWorkloadHandler(self.mock_account_client)

        # We must patch the global settings object to provide controlled test values.
        self.settings_patcher = patch("src.service.provision.handler.base_workload_handler.settings")
        self.mock_settings = self.settings_patcher.start()
        self.addCleanup(self.settings_patcher.stop)  # Ensures the patch is stopped after each test

        # Common Test Data
        self.owner_name = "owner@test.com"
        self.dev_group_name = "dev-group"
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner="user:owner_test.com",
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
        specific = DatabricksJobWorkloadSpecific(
            workspace="test-workspace",
            metastore="test_metastore",
            repoPath="test/repo",
            jobName="jobName",
            git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                gitRepoUrl="https://gitlab.com/test/repo.git",
                gitReference="gitReference",
                gitPath="gitPath",
                gitReferenceType="branch",
            ),
            cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
        )
        self.specific = specific
        self.workload = JobWorkload(
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
            specific=specific,
        )
        self.workspace_info_managed = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    def test_create_repository_with_permissions_managed_workspace(self):
        """Test the full flow for creating a repo in a managed workspace."""
        # Use context managers to patch dependencies instantiated inside the method
        with patch("src.service.provision.handler.base_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.base_workload_handler.IdentityManager"
        ) as MockIdentityManager, patch(
            "src.service.provision.handler.base_workload_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager, patch(
            "src.service.provision.handler.base_workload_handler.WorkspaceManager"
        ) as MockWSManager:
            # Arrange
            # We must configure the settings that the permission logic relies on.
            self.mock_settings.git.provider = "gitLab"
            self.mock_settings.databricks.permissions.workload.repo = DatabricksRepoPermissionsSettings(
                owner="CAN_MANAGE", developer="CAN_EDIT"
            )

            mock_repo_manager = MockRepoManager.return_value
            mock_identity_manager = MockIdentityManager.return_value
            mock_unity_manager = MockUnityCatalogManager.return_value
            mock_workspace_manager = MockWSManager.return_value
            mock_repo_manager.create_repo.return_value = 987

            # Act
            self.handler.create_repository_with_permissions(
                self.specific,
                self.mock_workspace_client,
                self.workspace_info_managed,
                self.owner_name,
                self.dev_group_name,
            )

            # Assert: Verify orchestration of all managers
            mock_workspace_manager.set_git_credentials.assert_called_once()
            self.mock_workspace_client.workspace.mkdirs.assert_called_once_with(path="/test")
            mock_repo_manager.create_repo.assert_called_once_with(
                "https://gitlab.com/test/repo.git", "gitLab", "/test/repo"
            )
            mock_unity_manager.attach_metastore.assert_called_once_with("test_metastore")
            mock_identity_manager.create_or_update_user_with_admin_privileges.assert_called_once_with(self.owner_name)
            mock_identity_manager.create_or_update_group_with_user_privileges.assert_called_once_with(
                self.dev_group_name
            )
            mock_repo_manager.assign_permissions_to_user.assert_called_once_with(
                "987", self.owner_name, RepoPermissionLevel.CAN_MANAGE
            )
            mock_repo_manager.assign_permissions_to_group.assert_called_once_with(
                "987", self.dev_group_name, RepoPermissionLevel.CAN_EDIT
            )

    def test_create_repository_unmanaged_workspace_skips_identity_and_unity_catalog(self):
        """Test that identity and UnityCatalog steps are skipped for an unmanaged workspace."""
        with patch("src.service.provision.handler.base_workload_handler.IdentityManager") as MockIdentityManager, patch(
            "src.service.provision.handler.base_workload_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager, patch(
            "src.service.provision.handler.base_workload_handler.WorkspaceManager"
        ) as MockWSManager:
            # Arrange
            workspace_info_unmanaged = DatabricksWorkspaceInfo(
                id="12345",
                name="adb-123456789.9.azuredatabricks.net",
                azure_resource_id=None,
                azure_resource_url="https://portal.azure.com",
                databricks_host="https://adb-123456789.9.azuredatabricks.net",
                provisioning_state=ProvisioningState.SUCCEEDED,
                is_managed=False,
            )
            self.mock_settings.git.provider = "gitLab"
            self.mock_settings.databricks.permissions.workload.repo = DatabricksRepoPermissionsSettings(
                owner="CAN_MANAGE", developer="CAN_EDIT"
            )

            mock_identity_manager_instance = MockIdentityManager.return_value
            mock_unity_manager_instance = MockUnityCatalogManager.return_value
            mock_workspace_manager = MockWSManager.return_value
            # Act
            self.handler.create_repository_with_permissions(
                self.specific,
                self.mock_workspace_client,
                workspace_info_unmanaged,
                self.owner_name,
                self.dev_group_name,
            )

            # Assert
            mock_workspace_manager.set_git_credentials.assert_called_once()
            mock_unity_manager_instance.attach_metastore.assert_not_called()
            mock_identity_manager_instance.create_or_update_user_with_admin_privileges.assert_not_called()
            mock_identity_manager_instance.create_or_update_group_with_user_privileges.assert_not_called()

    def test_map_principals_success(self):
        """Test successful mapping of all principals."""
        with patch("src.service.provision.handler.base_workload_handler.DatabricksMapper") as MockDatabricksMapper:
            # Arrange
            mock_mapper_instance = MockDatabricksMapper.return_value
            mock_mapper_instance.map.return_value = {
                "user:owner_test.com": self.owner_name,
                f"group:{self.dev_group_name}": self.dev_group_name,
            }

            # Act
            result = self.handler.map_principals(self.data_product, self.workload)

            # Assert
            MockDatabricksMapper.assert_called_once_with(self.mock_account_client)
            mock_mapper_instance.map.assert_called_once_with({"user:owner_test.com", f"group:{self.dev_group_name}"})
            self.assertEqual(result["user:owner_test.com"], self.owner_name)
            self.assertEqual(result[f"group:{self.dev_group_name}"], self.dev_group_name)

    def test_map_principals_failure(self):
        """Test that an error is raised if any principal mapping fails."""
        with patch("src.service.provision.handler.base_workload_handler.DatabricksMapper") as MockDatabricksMapper:
            # Arrange
            mock_mapper_instance = MockDatabricksMapper.return_value
            mock_mapper_instance.map.return_value = {
                self.owner_name: DatabricksMapperError("User not found"),
                f"group:{self.dev_group_name}": self.dev_group_name,  # One succeeds
            }

            # Act & Assert
            with self.assertRaises(ProvisioningError) as cm:
                self.handler.map_principals(self.data_product, self.workload)

            # Check that the error message contains the specific failure
            self.assertIn("User not found", str(cm.exception))

    def test_update_job_permissions(self):
        """Test successful update of job permissions based on settings."""
        with patch("src.service.provision.handler.base_workload_handler.settings") as mock_settings:
            # Arrange
            mock_settings.databricks.permissions.workload.job = MagicMock(owner="CAN_MANAGE_RUN", developer="CAN_VIEW")
            job_id, owner_id, dev_group_id = 123, "owner@test.com", "dev-group"

            # Act
            self.handler.update_job_permissions(self.mock_workspace_client, job_id, owner_id, dev_group_id)

            # Assert
            expected_acl = [
                JobAccessControlRequest(user_name=owner_id, permission_level=JobPermissionLevel.CAN_MANAGE_RUN),
                JobAccessControlRequest(group_name=dev_group_id, permission_level=JobPermissionLevel.CAN_VIEW),
            ]
            self.mock_workspace_client.jobs.update_permissions.assert_called_once_with(
                job_id=str(job_id), access_control_list=expected_acl
            )

    def test_update_pipeline_permissions(self):
        """Test successful update of DLT pipeline permissions based on settings."""
        with patch("src.service.provision.handler.base_workload_handler.settings") as mock_settings:
            # Arrange
            mock_settings.databricks.permissions.workload.pipeline = MagicMock(owner="CAN_MANAGE", developer="CAN_VIEW")
            pipeline_id, owner_id, dev_group_id = "dlt-id-abc", "owner@test.com", "dev-group"

            # Act
            self.handler.update_pipeline_permissions(self.mock_workspace_client, pipeline_id, owner_id, dev_group_id)

            # Assert
            expected_acl = [
                PipelineAccessControlRequest(user_name=owner_id, permission_level=PipelinePermissionLevel.CAN_MANAGE),
                PipelineAccessControlRequest(
                    group_name=dev_group_id, permission_level=PipelinePermissionLevel.CAN_VIEW
                ),
            ]
            self.mock_workspace_client.pipelines.update_permissions.assert_called_once_with(
                pipeline_id=pipeline_id, access_control_list=expected_acl
            )
