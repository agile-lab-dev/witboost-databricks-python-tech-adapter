import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sdk.service.jobs import BaseJob

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    JobClusterSpecific,
)
from src.models.exceptions import ProvisioningError
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler
from src.service.provision.handler.job_workload_handler import JobWorkloadHandler


class TestJobWorkloadHandler(unittest.TestCase):
    """Unit tests for the JobWorkloadHandler class."""

    def setUp(self):
        """Set up the test environment with minimal dependencies."""
        self.mock_account_client = MagicMock()
        self.mock_workspace_client = MagicMock()

        # The handler under test
        self.handler = JobWorkloadHandler(self.mock_account_client)

        # --- Common Test Data ---
        self.owner_principal = "user:owner_test.com"
        self.dev_group_principal = "group:dev-group"
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner=self.owner_principal,
            devGroup=self.dev_group_principal,
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

        self.job_specific = DatabricksJobWorkloadSpecific(
            workspace="test-workspace",
            metastore="test_metastore",
            repoPath="Repos/test/repo",
            jobName="test_job",
            git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                gitRepoUrl="https://test.repo",
                gitReference="gitReference",
                gitPath="gitPath",
                gitReferenceType="branch",
            ),
            cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
        )
        self.job_component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=self.job_specific,
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

    @patch.object(BaseWorkloadHandler, "update_job_permissions")
    @patch.object(BaseWorkloadHandler, "set_service_principal_git_credentials")
    @patch.object(BaseWorkloadHandler, "create_repository_with_permissions")
    @patch.object(BaseWorkloadHandler, "map_principals")
    @patch("src.service.provision.handler.job_workload_handler.JobManager")
    def test_provision_workload_success_without_run_as(
        self, MockJobManager, mock_map_principals, mock_create_repo, mock_set_git_creds, mock_update_perms
    ):
        """Test the successful provisioning flow without a 'runAs' principal."""
        # Arrange
        mock_job_manager = MockJobManager.return_value
        mock_job_manager.create_or_update_job_with_new_cluster.return_value = 123
        mock_map_principals.return_value = {
            self.owner_principal: self.owner_principal,
            self.dev_group_principal: self.dev_group_principal,
        }
        self.job_component.specific.runAsPrincipalName = None

        # Act
        result_id = self.handler.provision_workload(
            self.data_product, self.job_component, self.mock_workspace_client, self.workspace_info
        )

        # Assert
        self.assertEqual(result_id, "123")
        # Verify orchestration steps
        mock_map_principals.assert_called_once_with(self.data_product, self.job_component)
        mock_create_repo.assert_called_once()
        mock_job_manager.create_or_update_job_with_new_cluster.assert_called_once_with(
            job_name="test_job",
            description="",
            task_key="Task1",
            run_as=None,
            job_cluster_specific=self.job_specific.cluster,
            scheduling_specific=self.job_specific.scheduling,
            job_git_specific=self.job_specific.git,
        )
        mock_update_perms.assert_called_once()
        # Ensure runAs-specific steps were NOT called
        mock_set_git_creds.assert_not_called()

    @patch.object(BaseWorkloadHandler, "update_job_permissions")
    @patch.object(BaseWorkloadHandler, "set_service_principal_git_credentials")
    @patch.object(BaseWorkloadHandler, "create_repository_with_permissions")
    @patch.object(BaseWorkloadHandler, "map_principals")
    @patch("src.service.provision.handler.job_workload_handler.WorkspaceManager")
    @patch("src.service.provision.handler.job_workload_handler.JobManager")
    def test_provision_workload_success_with_run_as(
        self,
        MockJobManager,
        MockWorkspaceManager,
        mock_map_principals,
        mock_create_repo,
        mock_set_git_creds,
        mock_update_perms,
    ):
        """Test the successful provisioning flow with a 'runAs' principal."""
        # Arrange
        run_as_name = "test-sp"
        run_as_app_id = "app-id-123"
        self.job_component.specific.runAsPrincipalName = run_as_name

        mock_map_principals.return_value = {
            self.owner_principal: self.owner_principal,
            self.dev_group_principal: self.dev_group_principal,
        }
        mock_job_manager = MockJobManager.return_value
        mock_job_manager.create_or_update_job_with_new_cluster.return_value = 456
        mock_workspace_manager = MockWorkspaceManager.return_value
        mock_workspace_manager.get_service_principal_from_name.return_value = ServicePrincipal(
            application_id=run_as_app_id
        )

        # Act
        result_id = self.handler.provision_workload(
            self.data_product, self.job_component, self.mock_workspace_client, self.workspace_info
        )

        # Assert
        self.assertEqual(result_id, "456")
        # Ensure runAs-specific steps were called
        mock_set_git_creds.assert_called_once_with(
            self.mock_workspace_client, self.workspace_info.databricks_host, self.workspace_info.name, run_as_name
        )
        mock_job_manager.create_or_update_job_with_new_cluster.assert_called_once_with(
            job_name="test_job",
            description="",
            task_key="Task1",
            run_as=run_as_app_id,
            job_cluster_specific=self.job_specific.cluster,
            scheduling_specific=self.job_specific.scheduling,
            job_git_specific=self.job_specific.git,
        )

    def test_provision_fails_if_run_as_principal_not_found(self):
        """Test that provisioning fails if the specified 'runAs' principal does not exist."""
        # Arrange
        with patch(
            "src.service.provision.handler.job_workload_handler.WorkspaceManager"
        ) as MockWorkspaceManager, patch.object(
            BaseWorkloadHandler,
            "map_principals",
            return_value={self.owner_principal: "owner", self.dev_group_principal: "dev"},
        ), patch.object(BaseWorkloadHandler, "create_repository_with_permissions"), patch.object(
            BaseWorkloadHandler, "set_service_principal_git_credentials"
        ):
            mock_workspace_manager = MockWorkspaceManager.return_value
            # Simulate the principal not being found
            mock_workspace_manager.get_service_principal_from_name.return_value = None
            self.job_component.specific.runAsPrincipalName = "non-existent-sp"

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "Service principal not found"):
                self.handler.provision_workload(
                    self.data_product, self.job_component, self.mock_workspace_client, self.workspace_info
                )

    def test_unprovision_workload_with_remove_data(self):
        """Test unprovisioning that includes deleting both the job and the repo."""
        # Arrange
        with patch("src.service.provision.handler.job_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.job_workload_handler.JobManager"
        ) as MockJobManager:
            mock_job_manager = MockJobManager.return_value
            mock_repo_manager = MockRepoManager.return_value
            # Simulate finding one job to delete
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=789)]

            # Act
            self.handler.unprovision_workload(
                self.data_product,
                self.job_component,
                remove_data=True,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

            # Assert
            mock_job_manager.list_jobs_with_given_name.assert_called_once_with(self.job_specific.jobName)
            mock_job_manager.delete_job.assert_called_once_with(789)
            mock_repo_manager.delete_repo.assert_called_once_with(
                self.job_specific.git.gitRepoUrl, f"/{self.job_specific.repoPath}"
            )

    def test_unprovision_workload_without_remove_data(self):
        """Test unprovisioning that deletes the job but skips the repo."""
        # Arrange
        with patch("src.service.provision.handler.job_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.job_workload_handler.JobManager"
        ) as MockJobManager:
            mock_job_manager = MockJobManager.return_value
            mock_repo_manager = MockRepoManager.return_value
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=789)]

            # Act
            self.handler.unprovision_workload(
                self.data_product,
                self.job_component,
                remove_data=False,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

            # Assert
            mock_job_manager.delete_job.assert_called_once_with(789)
            mock_repo_manager.delete_repo.assert_not_called()

    def test_provision_fails_if_mapping_fails(self):
        """Test that provisioning fails if principal mapping does not return a required ID."""
        # Arrange
        # We only need to patch the inherited map_principals method for this test.
        with patch.object(BaseWorkloadHandler, "map_principals") as mock_map_principals, patch.object(
            BaseWorkloadHandler, "create_repository_with_permissions"
        ) as mock_create_repo:
            # Simulate a failure where the owner's ID is not found in the mapped results
            mock_map_principals.return_value = {
                self.dev_group_principal: self.dev_group_principal,
            }

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "Failed to retrieve outcome of mapping"):
                self.handler.provision_workload(
                    self.data_product, self.job_component, self.mock_workspace_client, self.workspace_info
                )

            # Verify that downstream operations like repo creation were not attempted.
            mock_create_repo.assert_not_called()

    def test_unprovision_fails_if_listing_jobs_fails(self):
        """Test that unprovisioning fails correctly if the initial job listing fails."""
        # Arrange
        with patch("src.service.provision.handler.job_workload_handler.JobManager") as MockJobManager:
            mock_job_manager = MockJobManager.return_value
            # Simulate an API error during the listing of jobs
            original_error = Exception("List API is down")
            mock_job_manager.list_jobs_with_given_name.side_effect = original_error

            # Act & Assert
            with self.assertRaises(ProvisioningError) as cm:
                self.handler.unprovision_workload(
                    self.data_product,
                    self.job_component,
                    remove_data=False,
                    workspace_client=self.mock_workspace_client,
                    workspace_info=self.workspace_info,
                )

            # Check that the error message from the original exception is in the final error
            self.assertIn("List API is down", str(cm.exception))
            # Verify no delete calls were made
            mock_job_manager.delete_job.assert_not_called()

    def test_unprovision_fails_if_deleting_job_fails(self):
        """Test that unprovisioning fails if a job deletion call fails mid-process."""
        # Arrange
        with patch("src.service.provision.handler.job_workload_handler.JobManager") as MockJobManager:
            mock_job_manager = MockJobManager.return_value
            # Simulate finding two jobs to delete
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=111), BaseJob(job_id=222)]

            # Simulate an API error during the deletion of the second job
            original_error = Exception("Delete API is down")

            def delete_side_effect(job_id):
                if job_id == 222:
                    raise original_error
                return None  # Succeed for job_id 111

            mock_job_manager.delete_job.side_effect = delete_side_effect

            # Act & Assert
            with self.assertRaises(ProvisioningError) as cm:
                self.handler.unprovision_workload(
                    self.data_product,
                    self.job_component,
                    remove_data=False,
                    workspace_client=self.mock_workspace_client,
                    workspace_info=self.workspace_info,
                )

            # Check that the error message from the original exception is in the final error
            self.assertIn("Delete API is down", str(cm.exception))

            # Verify that the loop continued and attempted to delete both jobs
            self.assertEqual(mock_job_manager.delete_job.call_count, 2)
