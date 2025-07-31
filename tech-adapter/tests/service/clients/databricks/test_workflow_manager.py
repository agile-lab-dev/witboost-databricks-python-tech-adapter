import unittest
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import (
    BaseJob,
    CreateResponse,
    Job,
    JobSettings,
    NotebookTask,
    PipelineTask,
    RunJobTask,
    Task,
)
from databricks.sdk.service.pipelines import PipelineStateInfo
from databricks.sdk.service.sql import GetWarehouseResponse

from src.models.databricks.exceptions import WorkflowManagerError
from src.models.databricks.workload.databricks_workflow_specific import WorkflowTasksInfo
from src.service.clients.databricks.workflow_manager import WorkflowManager


class TestWorkflowManager(unittest.TestCase):
    """Unit tests for the WorkflowManager."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_account_client = MagicMock()
        self.mock_workspace_client = MagicMock()
        self.workspace_name = "test-workspace"

        self.patcher_job_manager = patch("src.service.clients.databricks.workflow_manager.JobManager")
        self.patcher_dlt_manager = patch("src.service.clients.databricks.workflow_manager.DLTManager")
        self.patcher_workspace_manager = patch("src.service.clients.databricks.workflow_manager.WorkspaceManager")

        # Get the mock classes from the patchers
        mock_job_manager = self.patcher_job_manager.start()  # noqa: F841
        mock_dlt_manager = self.patcher_dlt_manager.start()  # noqa: F841
        mock_workspace_manager = self.patcher_workspace_manager.start()  # noqa: F841

        # Ensure the patchers are stopped after the test, even if it fails
        self.addCleanup(self.patcher_job_manager.stop)
        self.addCleanup(self.patcher_dlt_manager.stop)
        self.addCleanup(self.patcher_workspace_manager.stop)

        # Instantiate the class under test.
        # Its __init__ method will now use the mocked manager classes.
        self.workflow_manager = WorkflowManager(
            self.mock_account_client, self.mock_workspace_client, self.workspace_name
        )

        self.mock_job_manager = self.workflow_manager.job_manager
        self.mock_dlt_manager = self.workflow_manager.dlt_manager
        self.mock_workspace_manager = self.workflow_manager.workspace_manager

        # Common test data
        self.job_name = "test-workflow"
        self.job_id = 123
        self.job_settings = JobSettings(name=self.job_name, tasks=[])
        self.job = Job(settings=self.job_settings, job_id=self.job_id)

    def test_create_or_update_workflow_creates_new(self):
        """Test that a new workflow is created when none exists."""
        # Arrange
        self.mock_job_manager.list_jobs_with_given_name.return_value = []
        self.mock_workspace_client.jobs.create.return_value = CreateResponse(job_id=self.job_id)

        # Act
        result_id = self.workflow_manager.create_or_update_workflow(self.job)

        # Assert
        self.assertEqual(result_id, self.job_id)
        self.mock_job_manager.list_jobs_with_given_name.assert_called_once_with(self.job_name)
        self.mock_workspace_client.jobs.create.assert_called_once_with(
            continuous=self.job.settings.continuous,
            deployment=self.job.settings.deployment,
            email_notifications=self.job.settings.email_notifications,
            environments=self.job.settings.environments,
            format=self.job.settings.format,
            git_source=self.job.settings.git_source,
            job_clusters=self.job.settings.job_clusters,
            max_concurrent_runs=self.job.settings.max_concurrent_runs,
            name=self.job.settings.name,
            notification_settings=self.job.settings.notification_settings,
            parameters=self.job.settings.parameters,
            queue=self.job.settings.queue,
            run_as=self.job.settings.run_as,
            schedule=self.job.settings.schedule,
            tags=self.job.settings.tags,
            tasks=self.job.settings.tasks,
            timeout_seconds=self.job.settings.timeout_seconds,
            trigger=self.job.settings.trigger,
            webhook_notifications=self.job.settings.webhook_notifications,
            edit_mode=self.job.settings.edit_mode,
            health=self.job.settings.health,
        )
        self.mock_workspace_client.jobs.reset.assert_not_called()

    def test_create_or_update_workflow_updates_existing(self):
        """Test that an existing workflow is updated when one is found."""
        # Arrange
        existing_job = BaseJob(job_id=self.job_id, settings=JobSettings(name=self.job_name))
        self.mock_job_manager.list_jobs_with_given_name.return_value = [existing_job]

        # Act
        result_id = self.workflow_manager.create_or_update_workflow(self.job)

        # Assert
        self.assertEqual(result_id, self.job_id)
        self.mock_job_manager.list_jobs_with_given_name.assert_called_once_with(self.job_name)
        self.mock_workspace_client.jobs.reset.assert_called_once_with(
            job_id=self.job_id, new_settings=self.job_settings
        )
        self.mock_workspace_client.jobs.create.assert_not_called()

    def test_create_or_update_workflow_fails_on_multiple_jobs(self):
        """Test failure when multiple workflows exist with the same name."""
        # Arrange
        self.mock_job_manager.list_jobs_with_given_name.return_value = [BaseJob(job_id=1), BaseJob(job_id=2)]

        # Act & Assert
        with self.assertRaisesRegex(WorkflowManagerError, "The workflow name is not unique"):
            self.workflow_manager.create_or_update_workflow(self.job)

    def test_get_workflow_task_info_for_pipeline_task(self):
        """Test retrieving info from a DLT pipeline task."""
        # Arrange
        pipeline_id = "dlt-id-123"
        pipeline_name = "test-dlt-pipeline"
        task = Task(task_key="dlt_task", pipeline_task=PipelineTask(pipeline_id=pipeline_id))
        self.mock_workspace_client.pipelines.get.return_value = PipelineStateInfo(
            pipeline_id=pipeline_id, name=pipeline_name
        )

        # Act
        info = self.workflow_manager.get_workflow_task_info_from_task(task)

        # Assert
        self.assertIsNotNone(info)
        self.assertEqual(info.referenced_task_type, "pipeline")
        self.assertEqual(info.referenced_task_name, pipeline_name)
        self.assertEqual(info.referenced_task_id, pipeline_id)
        self.mock_workspace_client.pipelines.get.assert_called_once_with(pipeline_id)

    def test_get_workflow_task_info_for_job_task(self):
        """Test retrieving info from a Run Job task."""
        # Arrange
        run_job_id = 456
        run_job_name = "test-run-job"
        task = Task(task_key="run_job_task", run_job_task=RunJobTask(job_id=run_job_id))
        self.mock_workspace_client.jobs.get.return_value = Job(
            job_id=run_job_id, settings=JobSettings(name=run_job_name)
        )

        # Act
        info = self.workflow_manager.get_workflow_task_info_from_task(task)

        # Assert
        self.assertIsNotNone(info)
        self.assertEqual(info.referenced_task_type, "job")
        self.assertEqual(info.referenced_task_name, run_job_name)
        self.assertEqual(info.referenced_task_id, str(run_job_id))
        self.mock_workspace_client.jobs.get.assert_called_once_with(run_job_id)

    def test_get_workflow_task_info_for_notebook_with_warehouse(self):
        """Test retrieving info from a notebook task running on a SQL warehouse."""
        # Arrange
        warehouse_id = "wh-id-789"
        warehouse_name = "test-sql-warehouse"
        task = Task(
            task_key="notebook_task", notebook_task=NotebookTask(warehouse_id=warehouse_id, notebook_path="/path")
        )
        self.mock_workspace_client.warehouses.get.return_value = GetWarehouseResponse(
            id=warehouse_id, name=warehouse_name
        )

        # Act
        info = self.workflow_manager.get_workflow_task_info_from_task(task)

        # Assert
        self.assertIsNotNone(info)
        self.assertEqual(info.referenced_task_type, "notebook_warehouse")
        self.assertEqual(info.referenced_cluster_name, warehouse_name)
        self.assertEqual(info.referenced_cluster_id, warehouse_id)
        self.mock_workspace_client.warehouses.get.assert_called_once_with(warehouse_id)

    def test_create_task_from_workflow_info_for_job(self):
        """Test updating a Run Job task with a new job ID."""
        # Arrange
        new_job_id = "999"
        task_info = WorkflowTasksInfo(
            task_key="run_job_task", referenced_task_type="job", referenced_task_name="test-run-job"
        )
        original_task = Task(task_key="run_job_task", run_job_task=RunJobTask(job_id=456))  # Old ID
        self.mock_job_manager.retrieve_job_id_from_name.return_value = new_job_id

        # Act
        updated_task = self.workflow_manager.create_task_from_workflow_task_info(task_info, original_task)

        # Assert
        self.assertEqual(updated_task.run_job_task.job_id, int(new_job_id))
        self.mock_job_manager.retrieve_job_id_from_name.assert_called_once_with(task_info.referenced_task_name)

    def test_create_task_from_workflow_info_for_dlt(self):
        """Test updating a DLT task with a new pipeline ID."""
        # Arrange
        new_pipeline_id = "new-dlt-id"
        task_info = WorkflowTasksInfo(
            task_key="dlt_task", referenced_task_type="pipeline", referenced_task_name="test-dlt-pipeline"
        )
        original_task = Task(task_key="dlt_task", pipeline_task=PipelineTask(pipeline_id="old-dlt-id"))
        self.mock_dlt_manager.retrieve_pipeline_id_from_name.return_value = new_pipeline_id

        # Act
        updated_task = self.workflow_manager.create_task_from_workflow_task_info(task_info, original_task)

        # Assert
        self.assertEqual(updated_task.pipeline_task.pipeline_id, new_pipeline_id)
        self.mock_dlt_manager.retrieve_pipeline_id_from_name.assert_called_once_with(task_info.referenced_task_name)

    def test_reconstruct_job_with_correct_ids(self):
        """Test the full reconstruction of a job with new dependency IDs."""
        # Arrange
        # Task 1: DLT task that needs a new ID
        old_dlt_id = "old-dlt-id"
        new_dlt_id = "new-dlt-id"
        dlt_task = Task(task_key="dlt_task", pipeline_task=PipelineTask(pipeline_id=old_dlt_id))
        dlt_task_info = WorkflowTasksInfo(
            task_key="dlt_task", referenced_task_type="pipeline", referenced_task_name="dlt-pipeline-name"
        )
        self.mock_dlt_manager.retrieve_pipeline_id_from_name.return_value = new_dlt_id

        # Task 2: A simple notebook task that should not be changed
        notebook_task = Task(task_key="notebook_task", new_cluster=MagicMock())

        # Original job with both tasks
        original_job = Job(settings=JobSettings(name=self.job_name, tasks=[dlt_task, notebook_task]))

        # Act
        reconstructed_job = self.workflow_manager.reconstruct_job_with_correct_ids(original_job, [dlt_task_info])

        # Assert
        reconstructed_tasks = reconstructed_job.settings.tasks
        self.assertEqual(len(reconstructed_tasks), 2)
        # Check that the DLT task was updated with the new ID
        self.assertEqual(reconstructed_tasks[0].pipeline_task.pipeline_id, new_dlt_id)
        # Check that the notebook task remains unchanged
        self.assertEqual(reconstructed_tasks[1].task_key, "notebook_task")
        self.assertIsNone(reconstructed_tasks[1].pipeline_task)
