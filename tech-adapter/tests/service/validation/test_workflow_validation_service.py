import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.jobs import BaseJob, Format, Job, JobEmailNotifications, JobSettings, WebhookNotifications

from src.models.databricks.databricks_models import WorkflowWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.workload.databricks_workflow_specific import DatabricksWorkflowWorkloadSpecific
from src.models.databricks.workload.databricks_workload_specific import GitSpecific
from src.models.exceptions import ProvisioningError
from src.service.validation.workflow_validation_service import validate_workflow_for_provisioning


class TestWorkflowValidationService(unittest.TestCase):
    """Unit tests for the validate_workflow_for_provisioning function."""

    def setUp(self):
        """Set up common test data and mocks."""
        self.mock_job_manager = MagicMock()
        self.mock_workspace_client = MagicMock()

        # Common test data
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace-op",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

        # A base workflow definition from a provisioning request
        self.workflow_job = Job(settings=JobSettings(name="test-workflow", format=Format.SINGLE_TASK))
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

    def test_validation_succeeds_if_no_workflow_exists(self):
        """Test that validation passes if no workflow with the same name exists."""
        # Arrange
        self.mock_job_manager.list_jobs_with_given_name.return_value = []

        # Act
        try:
            validate_workflow_for_provisioning(
                self.mock_job_manager, self.mock_workspace_client, self.workflow_component, "prod", self.workspace_info
            )
        except ProvisioningError:
            self.fail("ProvisioningError was raised unexpectedly.")

        # Assert
        self.mock_job_manager.list_jobs_with_given_name.assert_called_once_with(self.workflow_job.settings.name)

    def test_validation_fails_if_multiple_workflows_exist(self):
        """Test that validation fails if more than one workflow with the same name exists."""
        # Arrange
        self.mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=1), BaseJob(job_id=2)]

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "Found more than one workflow"):
            validate_workflow_for_provisioning(
                self.mock_job_manager, self.mock_workspace_client, self.workflow_component, "prod", self.workspace_info
            )

    def test_validation_succeeds_if_workflows_are_identical(self):
        """Test that validation passes if the existing workflow is identical to the request."""
        # Arrange
        existing_job_summary = BaseJob(job_id=123, settings=JobSettings(name=self.workflow_job.settings.name))
        self.mock_job_manager.list_jobs_with_given_name.return_value = [existing_job_summary]

        self.mock_workspace_client.jobs.get.return_value = self.workflow_component.specific.workflow

        # Act
        try:
            validate_workflow_for_provisioning(
                self.mock_job_manager, self.mock_workspace_client, self.workflow_component, "prod", self.workspace_info
            )
        except ProvisioningError:
            self.fail("ProvisioningError was raised unexpectedly for identical workflows.")

        # Assert
        self.mock_workspace_client.jobs.get.assert_called_once_with(job_id=123)

    def test_validation_succeeds_if_workflows_differ_but_override_is_true(self):
        """Test that validation passes for different workflows if the override flag is set."""
        # Arrange
        self.workflow_component.specific.override = True
        existing_job_summary = BaseJob(job_id=123)
        self.mock_job_manager.list_jobs_with_given_name.return_value = [existing_job_summary]

        # Simulate a different existing workflow (e.g., different format)
        existing_job_full = Job(settings=JobSettings(name=self.workflow_job.settings.name, format=Format.MULTI_TASK))
        self.mock_workspace_client.jobs.get.return_value = existing_job_full

        # Act
        try:
            validate_workflow_for_provisioning(
                self.mock_job_manager, self.mock_workspace_client, self.workflow_component, "prod", self.workspace_info
            )
        except ProvisioningError:
            self.fail("ProvisioningError was raised unexpectedly when override was true.")

    @patch("src.service.validation.workflow_validation_service.settings")
    def test_validation_fails_if_workflows_differ_in_dev_environment(self, mock_settings):
        """Test that validation fails for different workflows in a dev environment without override."""
        # Arrange
        mock_settings.misc.development_environment_name = "development"
        existing_job_summary = BaseJob(job_id=123)
        self.mock_job_manager.list_jobs_with_given_name.return_value = [existing_job_summary]
        existing_job_full = Job(settings=JobSettings(name=self.workflow_job.settings.name, format=Format.MULTI_TASK))
        self.mock_workspace_client.jobs.get.return_value = existing_job_full

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "is different from that on Databricks"):
            validate_workflow_for_provisioning(
                self.mock_job_manager,
                self.mock_workspace_client,
                self.workflow_component,
                "development",
                self.workspace_info,
            )

    def test_validation_fails_on_overwrite_with_empty_workflow(self):
        """Test that validation fails if a non-empty workflow would be overwritten by a default empty one."""
        # Arrange
        # This is a non-empty, real workflow that exists in Databricks
        existing_job_full = Job(
            job_id=123,
            settings=JobSettings(name=self.workflow_job.settings.name, format=Format.MULTI_TASK, tasks=[MagicMock()]),
        )
        existing_job_summary = BaseJob(job_id=123)
        self.mock_job_manager.list_jobs_with_given_name.return_value = [existing_job_summary]
        self.mock_workspace_client.jobs.get.return_value = existing_job_full

        # This is the default, "empty" workflow from the request
        self.workflow_component.specific.workflow = Job(
            created_time=self.workflow_job.created_time,
            creator_user_name=self.workflow_job.creator_user_name,
            job_id=self.workflow_job.job_id,
            run_as_user_name=self.workflow_job.run_as_user_name,
            settings=JobSettings(
                name=self.workflow_job.settings.name,
                email_notifications=JobEmailNotifications(),
                webhook_notifications=WebhookNotifications(),
                format=Format.MULTI_TASK,
                timeout_seconds=0,
                max_concurrent_runs=1,
                run_as=self.workflow_job.settings.run_as,
            ),
        )

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "not permitted to replace a NON-empty workflow"):
            validate_workflow_for_provisioning(
                self.mock_job_manager, self.mock_workspace_client, self.workflow_component, "prod", self.workspace_info
            )
