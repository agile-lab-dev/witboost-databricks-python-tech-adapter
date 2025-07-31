import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sdk.service.jobs import BaseJob, Job, JobRunAs, JobSettings

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import WorkflowWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.workload.databricks_workflow_specific import (
    DatabricksWorkflowWorkloadSpecific,
    WorkflowTasksInfo,
)
from src.models.databricks.workload.databricks_workload_specific import GitSpecific
from src.models.exceptions import ProvisioningError
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler
from src.service.provision.handler.workflow_workload_handler import WorkflowWorkloadHandler


class TestWorkflowWorkloadHandler(unittest.TestCase):
    """Unit tests for the WorkflowWorkloadHandler class."""

    def setUp(self):
        """Set up the test environment with minimal dependencies."""
        self.mock_account_client = MagicMock()
        self.mock_workspace_client = MagicMock()

        # The handler under test
        self.handler = WorkflowWorkloadHandler(self.mock_account_client)

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

        self.workflow_job = Job(settings=JobSettings(name="test-workflow"))
        self.workflow_specific = DatabricksWorkflowWorkloadSpecific.model_validate(
            {
                "workspace": "test-workspace",
                "workflow": self.workflow_job.as_dict(),
                "git": GitSpecific(gitRepoUrl="https://test.repo"),
                "repoPath": "Repos/test/repo",
                "override": False,
            }
        )
        self.workflow_component = WorkflowWorkload(
            id="comp-id",
            name="test-workflow-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=self.workflow_specific,
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
    @patch.object(BaseWorkloadHandler, "create_repository_with_permissions")
    @patch.object(BaseWorkloadHandler, "map_principals")
    @patch("src.service.provision.handler.workflow_workload_handler.WorkflowManager")
    def test_provision_workflow_success_without_run_as(
        self, MockWorkflowManager, mock_map_principals, mock_create_repo, mock_update_perms
    ):
        """Test the successful provisioning flow without a 'runAs' principal."""
        # Arrange
        mock_workflow_manager = MockWorkflowManager.return_value
        mock_workflow_manager.create_or_update_workflow.return_value = 123
        # The reconstructed job should be the same as the original in this case
        mock_workflow_manager.reconstruct_job_with_correct_ids.return_value = self.workflow_job

        mock_map_principals.return_value = {
            self.owner_principal: self.owner_principal,
            self.dev_group_principal: self.dev_group_principal,
        }
        self.workflow_component.specific.runAsPrincipalName = None

        # Act
        result_id = self.handler.provision_workflow(
            self.data_product, self.workflow_component, self.mock_workspace_client, self.workspace_info
        )

        # Assert
        self.assertEqual(result_id, "123")
        # Verify orchestration steps
        mock_map_principals.assert_called_once_with(self.data_product, self.workflow_component)
        mock_create_repo.assert_called_once()
        mock_workflow_manager.create_or_update_workflow.assert_called_once_with(self.workflow_job)
        mock_update_perms.assert_called_once_with(
            self.mock_workspace_client, 123, self.owner_principal, self.dev_group_principal
        )

    @patch.object(BaseWorkloadHandler, "update_job_permissions")
    @patch.object(BaseWorkloadHandler, "create_repository_with_permissions")
    @patch.object(BaseWorkloadHandler, "map_principals")
    @patch("src.service.provision.handler.workflow_workload_handler.WorkspaceManager")
    @patch("src.service.provision.handler.workflow_workload_handler.WorkflowManager")
    def test_provision_workflow_success_with_run_as(
        self, MockWorkflowManager, MockWorkspaceManager, mock_map_principals, mock_create_repo, mock_update_perms
    ):
        """Test the successful provisioning flow with a 'runAs' principal."""
        # Arrange
        run_as_name = "test-sp"
        run_as_app_id = "app-id-123"
        self.workflow_component.specific.runAsPrincipalName = run_as_name

        mock_map_principals.return_value = {self.owner_principal: "owner", self.dev_group_principal: "dev"}

        mock_workflow_manager = MockWorkflowManager.return_value
        mock_workflow_manager.create_or_update_workflow.return_value = 456
        mock_workflow_manager.reconstruct_job_with_correct_ids.return_value = self.workflow_job

        mock_workspace_manager = MockWorkspaceManager.return_value
        mock_workspace_manager.get_service_principal_from_name.return_value = ServicePrincipal(
            application_id=run_as_app_id
        )

        # Act
        result_id = self.handler.provision_workflow(
            self.data_product, self.workflow_component, self.mock_workspace_client, self.workspace_info
        )

        # Assert
        self.assertEqual(result_id, "456")
        mock_workspace_manager.get_service_principal_from_name.assert_called_once_with(run_as_name)
        # Verify the run_as field was correctly set on the job settings before creation
        self.assertEqual(self.workflow_job.settings.run_as, JobRunAs(service_principal_name=run_as_app_id))
        mock_workflow_manager.create_or_update_workflow.assert_called_once_with(self.workflow_job)
        mock_update_perms.assert_called_once()

    def test_provision_workflow_fails_if_run_as_principal_not_found(self):
        """Test that provisioning fails if the specified 'runAs' principal does not exist."""
        # Arrange
        with patch(
            "src.service.provision.handler.workflow_workload_handler.WorkspaceManager"
        ) as MockWorkspaceManager, patch.object(
            BaseWorkloadHandler,
            "map_principals",
            return_value={self.owner_principal: "owner", self.dev_group_principal: "dev"},
        ), patch.object(BaseWorkloadHandler, "create_repository_with_permissions"):
            mock_workspace_manager = MockWorkspaceManager.return_value
            mock_workspace_manager.get_service_principal_from_name.return_value = None  # Principal not found
            self.workflow_component.specific.runAsPrincipalName = "non-existent-sp"

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "doesn't exist on target workspace"):
                self.handler.provision_workflow(
                    self.data_product, self.workflow_component, self.mock_workspace_client, self.workspace_info
                )

    def test_unprovision_workload_success(self):
        """Test successful unprovisioning of a workflow job."""
        # Arrange
        with patch("src.service.provision.handler.workflow_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.workflow_workload_handler.JobManager"
        ) as MockJobManager:
            mock_job_manager = MockJobManager.return_value
            mock_repo_manager = MockRepoManager.return_value
            # Simulate finding one job to delete
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=789)]
            self.workflow_component.specific.workflow_tasks_info = [MagicMock()]  # Ensure this list is not empty

            # Act
            self.handler.unprovision_workload(
                self.data_product,
                self.workflow_component,
                remove_data=True,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

            # Assert
            job_name = self.workflow_specific.workflow.settings.name
            mock_job_manager.list_jobs_with_given_name.assert_called_once_with(job_name)
            mock_job_manager.delete_job.assert_called_once_with(789)
            mock_repo_manager.delete_repo.assert_called_once()

    def test_unprovision_workload_fails_on_empty_job_name(self):
        """Test that unprovisioning fails if the workflow definition has no name."""
        # Arrange
        self.workflow_component.specific.workflow.settings.name = None
        self.workflow_component.specific.workflow_tasks_info = [WorkflowTasksInfo()]

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "Name is required to unprovision"):
            self.handler.unprovision_workload(
                self.data_product,
                self.workflow_component,
                remove_data=False,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

    # (These tests should be added inside the existing TestWorkflowWorkloadHandler class)

    def test_provision_fails_if_mapping_fails(self):
        """Test that provisioning fails if principal mapping does not return a required ID."""
        # Arrange
        with patch.object(BaseWorkloadHandler, "map_principals") as mock_map_principals, patch.object(
            BaseWorkloadHandler, "create_repository_with_permissions"
        ) as mock_create_repo:
            # Simulate a failure where the owner's ID is not found in the mapped results
            mock_map_principals.return_value = {self.dev_group_principal: self.dev_group_principal}

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "Failed to retrieve outcome of mapping"):
                self.handler.provision_workflow(
                    self.data_product, self.workflow_component, self.mock_workspace_client, self.workspace_info
                )

            # Verify that downstream operations like repo creation were not attempted.
            mock_create_repo.assert_not_called()

    def test_unprovision_fails_if_workflow_tasks_info_is_empty(self):
        """Test that unprovisioning fails if the specific model lacks workflow tasks info."""
        # Arrange
        # Set the required info to None or empty list to trigger the guard clause
        self.workflow_component.specific.workflow_tasks_info = []

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "received empty specific.workflowTasksInfoList"):
            self.handler.unprovision_workload(
                self.data_product,
                self.workflow_component,
                remove_data=False,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

    def test_unprovision_fails_if_job_deletion_fails(self):
        """Test that unprovisioning fails if a job deletion call fails mid-process."""
        # Arrange
        with patch("src.service.provision.handler.workflow_workload_handler.JobManager") as MockJobManager:
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

            self.workflow_component.specific.workflow_tasks_info = [WorkflowTasksInfo()]  # Satisfy initial check

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "Delete API is down"):
                self.handler.unprovision_workload(
                    self.data_product,
                    self.workflow_component,
                    remove_data=False,
                    workspace_client=self.mock_workspace_client,
                    workspace_info=self.workspace_info,
                )

            # Verify that the loop continued and attempted to delete both jobs
            self.assertEqual(mock_job_manager.delete_job.call_count, 2)

    def test_unprovision_skips_repo_deletion_if_remove_data_is_false(self):
        """Test that repository deletion is skipped when remove_data is False."""
        # Arrange
        with patch("src.service.provision.handler.workflow_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.workflow_workload_handler.JobManager"
        ) as MockJobManager:
            mock_job_manager = MockJobManager.return_value
            mock_repo_manager = MockRepoManager.return_value
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=789)]
            self.workflow_component.specific.workflow_tasks_info = [WorkflowTasksInfo()]

            # Act
            self.handler.unprovision_workload(
                self.data_product,
                self.workflow_component,
                remove_data=False,  # Key flag set to False
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

            # Assert
            # Job should still be deleted
            mock_job_manager.delete_job.assert_called_once_with(789)
            # The key assertion: repo deletion should NOT be called.
            mock_repo_manager.delete_repo.assert_not_called()
