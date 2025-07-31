import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.iam import ServicePrincipal
from databricks.sdk.service.jobs import BaseJob, Job, JobSettings, Task

from src.models.api_models import ReverseProvisioningRequest
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.workload.databricks_workflow_specific import WorkflowTasksInfo
from src.models.exceptions import ReverseProvisioningError
from src.service.reverse_provision.workflow_reverse_provision_handler import WorkflowReverseProvisionHandler


class TestWorkflowReverseProvisionHandler(unittest.TestCase):
    """Unit tests for the WorkflowReverseProvisionHandler class."""

    def setUp(self):
        self.mock_account_client = MagicMock()
        self.mock_workspace_handler = MagicMock()

        self.handler = WorkflowReverseProvisionHandler(self.mock_account_client, self.mock_workspace_handler)

        self.workspace_name = "test-workspace"
        self.workflow_name = "test_workflow"
        self.workflow_id = 123
        self.run_as_name = "test-sp"

        self.request = ReverseProvisioningRequest(
            useCaseTemplateId="urn:dmb:utm:databricks-workload-workflow-template:0.0.0",
            environment="development",
            catalogInfo={
                "spec": {
                    "instanceOf": "componenttype:default/workload",
                    "type": "workload",
                    "lifecycle": "experimental",
                    "owner": "user:john.doe_test.com",
                    "system": "system:domain.dp-name.0",
                    "domain": "domain:test-domain",
                    "mesh": {
                        "name": "test-component",
                        "specific": {
                            "workspace": "test-workspace",
                            "metastore": "test_metastore",
                            "repoPath": "Repos/test/repo",
                            "jobName": "test_job",
                            "git": {
                                "gitRepoUrl": "https://test.repo",
                                "gitReference": "gitReference",
                                "gitPath": "gitPath",
                                "gitReferenceType": "branch",
                            },
                            "cluster": {"clusterSparkVersion": "14.4.5", "nodeTypeId": "nodeTypeId", "numWorkers": 1},
                            "runAsPrincipalName": self.run_as_name,
                        },
                    },
                }
            },
            params={
                "environmentSpecificConfig": {
                    "specific": {
                        "workspace": self.workspace_name,
                        "workflow": {"settings": {"name": self.workflow_name}},
                    }
                }
            },
        )
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name=self.workspace_name,
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    def test_reverse_provision_success(self):
        """Test the successful reverse provisioning flow."""
        # Arrange
        with patch(
            "src.service.reverse_provision.workflow_reverse_provision_handler.JobManager"
        ) as MockJobManager, patch(
            "src.service.reverse_provision.workflow_reverse_provision_handler.WorkflowManager"
        ) as MockWorkflowManager, patch(
            "src.service.reverse_provision.workflow_reverse_provision_handler.WorkspaceManager"
        ) as MockWorkspaceManager:
            self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
            mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value

            # 1. Validation mocks
            mock_job_manager = MockJobManager.return_value
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=self.workflow_id)]

            # 2. Full workflow and task info mocks
            mock_workflow_manager = MockWorkflowManager.return_value
            task_info = WorkflowTasksInfo(task_key="task1", referenced_task_type="pipeline")
            mock_workflow_manager.get_workflow_task_info_from_task.return_value = task_info

            full_workflow = Job(
                job_id=self.workflow_id, settings=JobSettings(name=self.workflow_name, tasks=[Task(task_key="task1")])
            )
            mock_workspace_client.jobs.get.return_value = full_workflow

            # 3. 'run_as' validation mock (no change)
            full_workflow.run_as_user_name = "app-id-123"
            mock_workspace_manager = MockWorkspaceManager.return_value
            mock_workspace_manager.get_service_principal.return_value = ServicePrincipal(display_name=self.run_as_name)

            # Act
            updates = self.handler.reverse_provision(self.request)

            # Assert
            # Verify orchestration
            self.mock_workspace_handler.get_workspace_info_by_name.assert_called_once_with(self.workspace_name)
            mock_job_manager.list_jobs_with_given_name.assert_called_once_with(self.workflow_name)
            mock_workspace_client.jobs.get.assert_called_once_with(job_id=self.workflow_id)
            mock_workflow_manager.get_workflow_task_info_from_task.assert_called_once()
            mock_workspace_manager.get_service_principal.assert_called_once_with("app-id-123")

            # Verify the structure of the final updates dictionary
            self.assertIn("spec.mesh.specific.workflow.settings", updates)
            self.assertEqual(updates["spec.mesh.specific.workflow.job_id"], self.workflow_id)
            self.assertEqual(updates["spec.mesh.specific.workflowTasksInfoList"][0]["taskKey"], "task1")
            self.assertIn("witboost.parameters.updatedDate", updates)

    def test_reverse_provision_fails_if_workspace_not_found(self):
        """Test failure when the target workspace does not exist."""
        # Arrange
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = None

        # Act & Assert
        with self.assertRaisesRegex(ReverseProvisioningError, "Workspace 'test-workspace' not found"):
            self.handler.reverse_provision(self.request)

    def test_reverse_provision_fails_if_workflow_not_unique(self):
        """Test failure during validation if the workflow name is not unique."""
        # Arrange
        with patch("src.service.reverse_provision.workflow_reverse_provision_handler.JobManager") as MockJobManager:
            self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
            mock_job_manager = MockJobManager.return_value
            # Simulate finding multiple jobs with the same name
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=1), BaseJob(job_id=2)]

            # Act & Assert
            with self.assertRaisesRegex(ReverseProvisioningError, "is not unique"):
                self.handler.reverse_provision(self.request)

    def test_reverse_provision_fails_if_run_as_principal_changed(self):
        """Test failure if the 'run_as' principal on Databricks differs from the descriptor."""
        # Arrange
        with patch(
            "src.service.reverse_provision.workflow_reverse_provision_handler.JobManager"
        ) as MockJobManager, patch(
            "src.service.reverse_provision.workflow_reverse_provision_handler.WorkflowManager"
        ), patch(
            "src.service.reverse_provision.workflow_reverse_provision_handler.WorkspaceManager"
        ) as MockWorkspaceManager:
            self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
            mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value

            mock_job_manager = MockJobManager.return_value
            mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=self.workflow_id)]

            # Simulate that the fetched job has a 'run_as' user
            full_workflow = Job(
                job_id=self.workflow_id, settings=JobSettings(name=self.workflow_name), run_as_user_name="app-id-123"
            )
            mock_workspace_client.jobs.get.return_value = full_workflow

            # Simulate that the SP on Databricks now has a different display name than the one in the request
            mock_workspace_manager = MockWorkspaceManager.return_value
            mock_workspace_manager.get_service_principal.return_value = ServicePrincipal(
                display_name="a-different-sp-name"
            )

            # Act & Assert
            with self.assertRaisesRegex(ReverseProvisioningError, "doesn't allow the workflow Run As to be modified"):
                self.handler.reverse_provision(self.request)

    def test_prepare_updates_formats_timestamp_correctly(self):
        """Test the private helper to ensure the timestamp is formatted correctly."""
        # Arrange
        workflow = Job(job_id=123, created_time=12345, creator_user_name="user", settings=JobSettings(name="test"))
        now_fixed = datetime(2024, 7, 30, 10, 30, 15, 123456, tzinfo=timezone.utc)

        # We patch 'datetime.now' to control the timestamp
        with patch("src.service.reverse_provision.workflow_reverse_provision_handler.datetime") as mock_datetime:
            mock_datetime.now.return_value = now_fixed

            # Act
            updates = self.handler._prepare_updates(workflow, [])

            # Assert
            # Verify the specific ISO 8601 format with milliseconds and 'Z'
            self.assertEqual(updates["witboost.parameters.updatedDate"], "2024-07-30T10:30:15.123Z")
