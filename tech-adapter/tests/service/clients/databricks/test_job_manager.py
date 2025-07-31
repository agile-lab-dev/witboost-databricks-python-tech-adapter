import unittest
from typing import Any
from unittest.mock import MagicMock

from databricks.sdk.service.compute import AzureAttributes, ClusterSpec, DataSecurityMode
from databricks.sdk.service.jobs import (
    BaseJob,
    CreateResponse,
    CronSchedule,
    GitProvider,
    GitSource,
    JobRunAs,
    JobSettings,
    NotebookTask,
    Source,
    Task,
)

from src.models.databricks.exceptions import JobManagerError
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    GitReferenceType,
    JobClusterSpecific,
)
from src.service.clients.databricks.job_manager import JobManager


class TestJobManager(unittest.TestCase):
    """Unit tests for the JobManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_workspace_client = MagicMock()
        self.workspace_name = "test-workspace"
        self.job_manager = JobManager(self.mock_workspace_client, self.workspace_name)

        # Common Test Data
        self.job_name = "test-job"
        self.job_id = 123
        self.description = "A test job."
        self.task_key = "test-task"
        self.run_as = "service-principal"

        self.job_cluster_specific = JobClusterSpecific(
            clusterSparkVersion="13.3.x-scala2.12",
            nodeTypeId="Standard_DS3_v2",
            numWorkers=2,
        )

        self.scheduling_specific = DatabricksJobWorkloadSpecific.SchedulingSpecific(
            cronExpression="0 0 12 * * ?", javaTimezoneId="UTC"
        )

        self.job_git_specific_branch = DatabricksJobWorkloadSpecific.JobGitSpecific(
            gitRepoUrl="https://gitlab.com/test/repo.git",
            gitReference="main",
            gitReferenceType=GitReferenceType.BRANCH,
            gitPath="/path/to/notebook.py",
        )

        self.base_job_args: dict[str, Any] = {
            "job_name": self.job_name,
            "description": self.description,
            "task_key": self.task_key,
            "run_as": self.run_as,
            "job_cluster_specific": self.job_cluster_specific,
            "scheduling_specific": self.scheduling_specific,
            "job_git_specific": self.job_git_specific_branch,
        }

    def test_create_or_update_creates_new_job(self):
        """Test that a new job is created when none exists."""
        # Arrange
        self.mock_workspace_client.jobs.list.return_value = []
        self.mock_workspace_client.jobs.create.return_value = CreateResponse(job_id=self.job_id)

        # Act
        result_id = self.job_manager.create_or_update_job_with_new_cluster(**self.base_job_args)

        # Assert
        self.assertEqual(result_id, self.job_id)
        self.mock_workspace_client.jobs.list.assert_called_once_with(name=self.job_name)
        self.mock_workspace_client.jobs.create.assert_called_once()
        self.mock_workspace_client.jobs.update.assert_not_called()

        _, kwargs = self.mock_workspace_client.jobs.create.call_args
        self.assertEqual(kwargs["name"], self.job_name)
        self.assertIsNotNone(kwargs["schedule"])
        self.assertEqual(kwargs["schedule"].quartz_cron_expression, self.scheduling_specific.cronExpression)
        self.assertIsNotNone(kwargs["git_source"])
        self.assertEqual(kwargs["git_source"].git_branch, self.job_git_specific_branch.gitReference)

    def test_create_or_update_updates_existing_job(self):
        """Test that an existing job is updated if one is found."""
        # Arrange
        existing_job = BaseJob(job_id=self.job_id, settings=JobSettings(name=self.job_name))
        self.mock_workspace_client.jobs.list.return_value = [existing_job]

        # Act
        result_id = self.job_manager.create_or_update_job_with_new_cluster(**self.base_job_args)

        # Assert
        self.assertEqual(result_id, self.job_id)
        self.mock_workspace_client.jobs.list.assert_called_once_with(name=self.job_name)
        self.mock_workspace_client.jobs.update.assert_called_once_with(
            job_id=self.job_id,
            new_settings=JobSettings(
                name=self.job_name,
                tasks=[
                    Task(
                        description=self.description,
                        notebook_task=NotebookTask(
                            notebook_path=self.job_git_specific_branch.gitPath, source=Source.GIT, base_parameters={}
                        ),
                        task_key=self.task_key,
                        new_cluster=ClusterSpec(
                            spark_version=self.job_cluster_specific.clusterSparkVersion,
                            node_type_id=self.job_cluster_specific.nodeTypeId,
                            num_workers=self.job_cluster_specific.numWorkers,
                            azure_attributes=AzureAttributes(
                                first_on_demand=self.job_cluster_specific.firstOnDemand,
                                availability=self.job_cluster_specific.availability,
                                spot_bid_max_price=self.job_cluster_specific.spotBidMaxPrice,
                            ),
                            driver_node_type_id=self.job_cluster_specific.driverNodeTypeId,
                            spark_conf={conf.name: conf.value for conf in self.job_cluster_specific.sparkConf}
                            if self.job_cluster_specific.sparkConf
                            else {},
                            spark_env_vars={conf.name: conf.value for conf in self.job_cluster_specific.spark_env_vars}
                            if self.job_cluster_specific.spark_env_vars
                            else {},
                            data_security_mode=DataSecurityMode.SINGLE_USER,
                            runtime_engine=self.job_cluster_specific.runtimeEngine,
                        ),
                    )
                ],
                parameters=[],
                git_source=GitSource(
                    git_url=self.job_git_specific_branch.gitRepoUrl,
                    git_provider=GitProvider.GIT_LAB,
                    git_branch=self.job_git_specific_branch.gitReference,
                ),
                run_as=JobRunAs(service_principal_name=self.run_as),
                schedule=CronSchedule(
                    timezone_id=self.scheduling_specific.javaTimezoneId,
                    quartz_cron_expression=self.scheduling_specific.cronExpression,
                ),
            ),
        )
        self.mock_workspace_client.jobs.create.assert_not_called()

    def test_create_or_update_with_git_tag(self):
        """Test job creation with a git tag reference."""
        # Arrange
        git_spec_tag = DatabricksJobWorkloadSpecific.JobGitSpecific(
            gitRepoUrl="https://gitlab.com/test/repo.git",
            gitReference="v1.0",
            gitReferenceType=GitReferenceType.TAG,
            gitPath="/path/to/notebook.py",
        )
        job_args = {**self.base_job_args, "job_git_specific": git_spec_tag}
        self.mock_workspace_client.jobs.list.return_value = []
        self.mock_workspace_client.jobs.create.return_value = CreateResponse(job_id=self.job_id)

        # Act
        self.job_manager.create_or_update_job_with_new_cluster(**job_args)

        # Assert
        _, kwargs = self.mock_workspace_client.jobs.create.call_args
        git_source = kwargs["git_source"]
        self.assertEqual(git_source.git_tag, "v1.0")
        self.assertIsNone(getattr(git_source, "git_branch", None))

    def test_create_or_update_no_scheduling(self):
        """Test job creation without a schedule."""
        # Arrange
        job_args = self.base_job_args.copy()
        job_args["scheduling_specific"] = None
        self.mock_workspace_client.jobs.list.return_value = []
        self.mock_workspace_client.jobs.create.return_value = CreateResponse(job_id=self.job_id)

        # Act
        self.job_manager.create_or_update_job_with_new_cluster(**job_args)

        # Assert
        _, kwargs = self.mock_workspace_client.jobs.create.call_args
        self.assertIsNone(kwargs.get("schedule"))

    def test_create_or_update_fails_on_multiple_jobs(self):
        """Test failure when multiple jobs exist with the same name."""
        # Arrange
        existing_jobs = [BaseJob(job_id=1), BaseJob(job_id=2)]
        self.mock_workspace_client.jobs.list.return_value = existing_jobs

        # Act & Assert
        with self.assertRaisesRegex(JobManagerError, "The job name is not unique"):
            self.job_manager.create_or_update_job_with_new_cluster(**self.base_job_args)

    def test_create_job_fails_on_empty_response(self):
        """Test failure when the create API returns a response without a job_id."""
        # Arrange
        self.mock_workspace_client.jobs.list.return_value = []
        self.mock_workspace_client.jobs.create.return_value = CreateResponse(job_id=None)

        # Act & Assert
        with self.assertRaisesRegex(JobManagerError, "Received empty response from Databricks"):
            self.job_manager.create_or_update_job_with_new_cluster(**self.base_job_args)

    def test_delete_job_success(self):
        """Test successful deletion of a job."""
        # Act
        self.job_manager.delete_job(self.job_id)

        # Assert
        self.mock_workspace_client.jobs.delete.assert_called_once_with(job_id=self.job_id)

    def test_delete_job_fails_on_api_error(self):
        """Test that deletion raises JobManagerError on API failure."""
        # Arrange
        self.mock_workspace_client.jobs.delete.side_effect = Exception("API Delete Error")

        # Act & Assert
        with self.assertRaisesRegex(JobManagerError, f"Error deleting job {self.job_id}"):
            self.job_manager.delete_job(self.job_id)

    def test_retrieve_job_id_success(self):
        """Test successful retrieval of a unique job ID."""
        # Arrange
        existing_job = BaseJob(job_id=self.job_id, settings=JobSettings(name=self.job_name))
        self.mock_workspace_client.jobs.list.return_value = [existing_job]

        # Act
        result_id = self.job_manager.retrieve_job_id_from_name(self.job_name)

        # Assert
        self.assertEqual(result_id, str(self.job_id))

    def test_retrieve_job_id_fails_when_not_found(self):
        """Test failure to retrieve ID when no job is found."""
        # Arrange
        self.mock_workspace_client.jobs.list.return_value = []

        # Act & Assert
        with self.assertRaisesRegex(JobManagerError, "No job found with name"):
            self.job_manager.retrieve_job_id_from_name(self.job_name)

    def test_retrieve_job_id_fails_when_not_unique(self):
        """Test failure to retrieve ID when multiple jobs are found."""
        # Arrange
        existing_jobs = [BaseJob(job_id=1), BaseJob(job_id=2)]
        self.mock_workspace_client.jobs.list.return_value = existing_jobs

        # Act & Assert
        with self.assertRaisesRegex(JobManagerError, "More than one job found with name"):
            self.job_manager.retrieve_job_id_from_name(self.job_name)
