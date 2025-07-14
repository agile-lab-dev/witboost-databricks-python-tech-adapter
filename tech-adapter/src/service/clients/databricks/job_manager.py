from typing import List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import AzureAttributes, ClusterSpec, DataSecurityMode
from databricks.sdk.service.jobs import (
    BaseJob,
    CronSchedule,
    GitProvider,
    GitSource,
    JobParameterDefinition,
    JobRunAs,
    JobSettings,
    NotebookTask,
    Source,
    Task,
)
from loguru import logger

from src.models.databricks.exceptions import JobManagerError
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    GitReferenceType,
    JobClusterSpecific,
)


class JobManager:
    """Class responsible for managing Databricks jobs."""

    def __init__(self, workspace_client: WorkspaceClient, workspace_name: str):
        self.workspace_client = workspace_client
        self.workspace_name = workspace_name

    def create_or_update_job_with_new_cluster(
        self,
        job_name: str,
        description: str,
        task_key: str,
        run_as: Optional[str],
        job_cluster_specific: JobClusterSpecific,
        scheduling_specific: Optional[DatabricksJobWorkloadSpecific.SchedulingSpecific],
        job_git_specific: DatabricksJobWorkloadSpecific.JobGitSpecific,
    ) -> int:
        """
        Creates or updates a Databricks job with a new dedicated cluster.

        If no job with the name exists, a new one is created.
        If exactly one job exists, it is updated.
        If multiple jobs exist, an error is raised.

        Returns:
            The ID of the created or updated job.

        Raises:
            JobManagerError: If the operation fails or the job name is not unique.
        """
        existing_jobs = self.list_jobs_with_given_name(job_name)

        if not existing_jobs:
            return self._create_job_with_new_cluster(
                job_name,
                description,
                task_key,
                run_as,
                job_cluster_specific,
                scheduling_specific,
                job_git_specific,
            )

        if len(existing_jobs) > 1:
            error_msg = (
                f"Error trying to update the job '{job_name}'. " f"The job name is not unique in {self.workspace_name}."
            )
            logger.error(error_msg)
            raise JobManagerError(error_msg)

        job_id = existing_jobs[0].job_id
        if not job_id:
            error_msg = (
                f"Error updating job '{job_name}' in {self.workspace_name}. " f"Received empty response from Databricks"
            )
            logger.error(error_msg)
            logger.debug("Response returned by Databricks for '{}': {}", job_name, existing_jobs[0])
            raise JobManagerError(error_msg)

        return self._update_job_with_new_cluster(
            job_id,
            job_name,
            description,
            task_key,
            run_as,
            job_cluster_specific,
            scheduling_specific,
            job_git_specific,
        )

    def _create_job_with_new_cluster(
        self,
        job_name: str,
        description: str,
        task_key: str,
        run_as: Optional[str],
        job_cluster_specific: JobClusterSpecific,
        scheduling_specific: Optional[DatabricksJobWorkloadSpecific.SchedulingSpecific],
        job_git_specific: DatabricksJobWorkloadSpecific.JobGitSpecific,
    ) -> int:
        logger.info("Creating job with name '{}' in workspace '{}'", job_name, self.workspace_name)
        try:
            job_settings = self._build_job_settings(
                job_name, description, task_key, run_as, job_cluster_specific, scheduling_specific, job_git_specific
            )
            response = self.workspace_client.jobs.create(
                name=job_name,
                description=description,
                run_as=job_settings.run_as,
                parameters=job_settings.parameters,
                git_source=job_settings.git_source,
                schedule=job_settings.schedule,
                tasks=job_settings.tasks,
            )
            if not response.job_id:
                error_msg = (
                    f"Error creating job '{job_name}' in {self.workspace_name}. "
                    f"Received empty response from Databricks"
                )
                logger.error(error_msg)
                logger.debug("Response returned by Databricks for '{}': {}", job_name, response)
                raise JobManagerError(error_msg)

            logger.info(
                "Created new job in '{}' with name: '{}' and ID: {}.", self.workspace_name, job_name, response.job_id
            )
            return response.job_id
        except JobManagerError:
            raise
        except Exception as e:
            error_msg = f"Error creating job '{job_name}' in '{self.workspace_name}'"
            logger.error(error_msg)
            raise JobManagerError(error_msg) from e

    def _update_job_with_new_cluster(
        self,
        job_id: int,
        job_name: str,
        description: str,
        task_key: str,
        run_as: Optional[str],
        job_cluster_specific: JobClusterSpecific,
        scheduling_specific: Optional[DatabricksJobWorkloadSpecific.SchedulingSpecific],
        job_git_specific: DatabricksJobWorkloadSpecific.JobGitSpecific,
    ) -> int:
        logger.info("Updating job '{}' (ID: {}) in workspace '{}'", job_name, job_id, self.workspace_name)
        try:
            job_settings = self._build_job_settings(
                job_name,
                description,
                task_key,
                run_as,
                job_cluster_specific,
                scheduling_specific,
                job_git_specific,
            )
            self.workspace_client.jobs.update(job_id=job_id, new_settings=job_settings)
            logger.info("Updated job in '{}' with name: '{}' and ID: {}.", self.workspace_name, job_name, job_id)
            return job_id
        except Exception as e:
            error_msg = f"Error updating job '{job_name}' (ID: {job_id}) in '{self.workspace_name}'"
            logger.error(error_msg)
            raise JobManagerError(error_msg) from e

    def delete_job(self, job_id: int) -> None:
        """Deletes a Databricks job by its ID."""
        logger.info("Deleting job with ID: {} in workspace '{}'", job_id, self.workspace_name)
        try:
            self.workspace_client.jobs.delete(job_id=job_id)
            logger.info("Successfully deleted job {}", job_id)
        except Exception as e:
            error_msg = f"Error deleting job {job_id} from workspace '{self.workspace_name}'"
            logger.error(error_msg)
            raise JobManagerError(error_msg) from e

    def retrieve_job_id_from_name(self, job_name: str) -> str:
        """Retrieves a job's ID by its name, failing if not unique."""
        jobs = self.list_jobs_with_given_name(job_name)
        if not jobs:
            error_msg = f"No job found with name '{job_name}' in workspace '{self.workspace_name}'."
            logger.error(error_msg)
            raise JobManagerError(error_msg)
        if len(jobs) > 1:
            error_msg = f"More than one job found with name '{job_name}' in workspace '{self.workspace_name}'."
            logger.error(error_msg)
            raise JobManagerError(error_msg)
        return str(jobs[0].job_id)

    def list_jobs_with_given_name(self, job_name: str) -> List[BaseJob]:
        """
        Lists all Databricks jobs matching a given name

        Args:
            job_name: The name of the job to search for

        Returns:
            The list of jobs with said name

        Raises:
            JobManagerError: If an error occurred while querying the workspace

        """
        try:
            return list(self.workspace_client.jobs.list(name=job_name))
        except Exception as e:
            error_msg = f"Error listing jobs named '{job_name}' in '{self.workspace_name}'"
            logger.error(error_msg)
            raise JobManagerError(error_msg) from e

    def _build_job_settings(
        self,
        job_name: str,
        description: str,
        task_key: str,
        run_as: Optional[str],
        job_cluster_specific: JobClusterSpecific,
        scheduling_specific: Optional[DatabricksJobWorkloadSpecific.SchedulingSpecific],
        job_git_specific: DatabricksJobWorkloadSpecific.JobGitSpecific,
    ) -> JobSettings:
        cluster_spec = self._get_cluster_spec_from_specific(job_cluster_specific)
        tasks = [
            Task(
                description=description,
                notebook_task=NotebookTask(
                    notebook_path=job_git_specific.gitPath,
                    source=Source.GIT,
                    base_parameters={},
                ),
                task_key=task_key,
                new_cluster=cluster_spec,
            )
        ]

        job_parameters = (
            [
                JobParameterDefinition(name=property.name, default=property.value)
                for property in job_cluster_specific.spark_env_vars
            ]
            if job_cluster_specific.spark_env_vars
            else []
        )

        job_settings = JobSettings(
            name=job_name,
            tasks=tasks,
            parameters=job_parameters,
            git_source=self._get_git_source_from_specific(job_git_specific),
        )

        if run_as:
            job_settings.run_as = JobRunAs(service_principal_name=run_as)

        if scheduling_specific:
            job_settings.schedule = CronSchedule(
                timezone_id=scheduling_specific.javaTimezoneId,
                quartz_cron_expression=scheduling_specific.cronExpression,
            )
        return job_settings

    def _get_git_source_from_specific(
        self, job_git_specific: DatabricksJobWorkloadSpecific.JobGitSpecific
    ) -> GitSource:
        source = GitSource(
            git_url=job_git_specific.gitRepoUrl, git_provider=job_git_specific.gitProvider or GitProvider.GIT_LAB
        )
        if job_git_specific.gitReferenceType == GitReferenceType.BRANCH:
            source.git_branch = job_git_specific.gitReference
        elif job_git_specific.gitReferenceType == GitReferenceType.TAG:
            source.git_tag = job_git_specific.gitReference
        return source

    def _get_cluster_spec_from_specific(self, job_cluster_specific: JobClusterSpecific) -> ClusterSpec:
        return ClusterSpec(
            spark_version=job_cluster_specific.clusterSparkVersion,
            node_type_id=job_cluster_specific.nodeTypeId,
            num_workers=job_cluster_specific.numWorkers,
            azure_attributes=AzureAttributes(
                first_on_demand=job_cluster_specific.firstOnDemand,
                availability=job_cluster_specific.availability,
                spot_bid_max_price=job_cluster_specific.spotBidMaxPrice,
            ),
            driver_node_type_id=job_cluster_specific.driverNodeTypeId,
            spark_conf={conf.name: conf.value for conf in job_cluster_specific.sparkConf}
            if job_cluster_specific.sparkConf
            else {},
            spark_env_vars={conf.name: conf.value for conf in job_cluster_specific.spark_env_vars}
            if job_cluster_specific.spark_env_vars
            else {},
            data_security_mode=DataSecurityMode.SINGLE_USER,
            runtime_engine=job_cluster_specific.runtimeEngine,
        )
