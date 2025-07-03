from typing import List, Optional

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.jobs import Job, Task
from loguru import logger

from src.models.databricks.exceptions import WorkflowManagerError
from src.models.databricks.workload.databricks_workflow_specific import WorkflowTasksInfo
from src.service.clients.databricks.dlt_manager import DLTManager
from src.service.clients.databricks.job_manager import JobManager
from src.service.clients.databricks.workspace_manager import WorkspaceManager


class WorkflowManager:
    """
    Manages the creation, update, and deployment of Databricks Jobs (Workflows).
    """

    def __init__(
        self,
        account_client: AccountClient,
        workspace_client: WorkspaceClient,
        workspace_name: str,
    ):
        self.workspace_client = workspace_client
        self.workspace_name = workspace_name
        self.job_manager = JobManager(workspace_client, workspace_name)
        self.dlt_manager = DLTManager(workspace_client, workspace_name)
        self.workspace_manager = WorkspaceManager(workspace_client, account_client)

    def create_or_update_workflow(self, job: Job) -> int:
        """
        Creates a new workflow or updates an existing one with the same name.

        Args:
            job: Job definition with all settings to be used during provision

        Returns:
            Job id
        """
        if not (job.settings and job.settings.name):
            error_msg = "Error, received empty workflow name. Name is a field required"
            logger.error(error_msg)
            raise WorkflowManagerError(error_msg)
        logger.info(f"Checking for existing workflow named '{job.settings.name}' in workspace '{self.workspace_name}'")

        existing_jobs = self.job_manager.list_jobs_with_given_name(job.settings.name)

        if not existing_jobs:
            return self.create_workflow(job)

        if len(existing_jobs) > 1:
            error_msg = (
                f"Error trying to update the workflow '{job.settings.name}'. "
                f"The workflow name is not unique in {self.workspace_name}"
            )
            logger.error(error_msg)
            raise WorkflowManagerError(error_msg)

        job.job_id = existing_jobs[0].job_id
        return self.update_workflow(job)

    def create_workflow(self, job: Job) -> int:
        """Creates a new workflow in Databricks.

        Args:
            job: Job definition with all settings to be used during provision

        Returns:
            Job id
        """

        if not (job.settings and job.settings.name):
            error_msg = "Error, received empty workflow name. Name is a field required"
            logger.error(error_msg)
            raise WorkflowManagerError(error_msg)

        logger.info("Creating workflow '{}' in workspace '{}'", job.settings.name, self.workspace_name)
        try:
            response = self.workspace_client.jobs.create(
                continuous=job.settings.continuous,
                deployment=job.settings.deployment,
                email_notifications=job.settings.email_notifications,
                environments=job.settings.environments,
                format=job.settings.format,
                git_source=job.settings.git_source,
                job_clusters=job.settings.job_clusters,
                max_concurrent_runs=job.settings.max_concurrent_runs,
                name=job.settings.name,
                notification_settings=job.settings.notification_settings,
                parameters=job.settings.parameters,
                queue=job.settings.queue,
                run_as=job.settings.run_as,
                schedule=job.settings.schedule,
                tags=job.settings.tags,
                tasks=job.settings.tasks,
                timeout_seconds=job.settings.timeout_seconds,
                trigger=job.settings.trigger,
                webhook_notifications=job.settings.webhook_notifications,
                edit_mode=job.settings.edit_mode,
                health=job.settings.health,
            )
            if not response.job_id:
                error_msg = (
                    f"Error creating workflow '{job.settings.name}' in {self.workspace_name}. "
                    f"Received empty response from Databricks"
                )
                logger.error(error_msg)
                logger.debug("Response returned by Databricks for '{}': {}", job.settings.name, response)
                raise WorkflowManagerError(error_msg)
            logger.success("Successfully created workflow '{}' with ID: {}.", job.settings.name, response.job_id)
            return response.job_id
        except Exception as e:
            error_msg = f"Failed to create workflow '{job.settings.name}'"
            logger.error("Failed to create workflow '{}': {}", job.settings.name, e)
            raise WorkflowManagerError(error_msg) from e

    def update_workflow(self, job: Job) -> int:
        """Updates an existing workflow by resetting it with new settings."""
        if not (job.settings and job.settings.name and job.job_id):
            error_msg = "Error, received empty workflow name. Name is a field required"
            logger.error(error_msg)
            raise WorkflowManagerError(error_msg)
        logger.info(
            "Updating workflow '{}' (ID: {}) in workspace '{}'", job.settings.name, job.job_id, self.workspace_name
        )
        try:
            self.workspace_client.jobs.reset(job_id=job.job_id, new_settings=job.settings)
            logger.success("Successfully updated workflow '{}'", job.settings.name)
            return job.job_id
        except Exception as e:
            error_msg = f"Failed to update workflow '{job.settings.name}'"
            logger.error("Failed to update workflow '{}': {}", job.settings.name, e)
            raise WorkflowManagerError(error_msg) from e

    def get_workflow_task_info_from_task(self, task: Task) -> Optional[WorkflowTasksInfo]:
        """
        Inspects a task to find workspace-dependent IDs and retrieves their names.
        This captures the state of a task before deploying to a new workspace.
        """
        task_info = WorkflowTasksInfo(task_key=task.task_key)

        try:
            task_info.referenced_task_type = self._retrieve_task_type(task)
            if not task_info.referenced_task_type:
                return None  # Task has no managed dependencies

            if task_info.referenced_task_type == "pipeline" and task.pipeline_task:
                pipeline_id = task.pipeline_task.pipeline_id
                pipeline = self.workspace_client.pipelines.get(pipeline_id)
                task_info.referenced_task_name = pipeline.name
                task_info.referenced_task_id = pipeline_id
            elif task_info.referenced_task_type == "job" and task.run_job_task and task.run_job_task.job_id:
                job_id = task.run_job_task.job_id
                job = self.workspace_client.jobs.get(job_id)
                if not job.settings or not job.settings.name:
                    error_msg = (
                        f"Unexpected error while creating task information."
                        f"Received empty response from Databricks for job {job_id}"
                    )
                    logger.error(error_msg)
                    logger.debug("Response returned by Databricks for '{}': {}", job_id, job)
                    raise WorkflowManagerError(error_msg)
                task_info.referenced_task_name = job.settings.name
                task_info.referenced_task_id = str(job_id)
            elif (
                task_info.referenced_task_type == "notebook_warehouse"
                and task.notebook_task
                and task.notebook_task.warehouse_id
            ):
                warehouse_id = task.notebook_task.warehouse_id
                warehouse = self.workspace_client.warehouses.get(warehouse_id)
                task_info.referenced_cluster_name = warehouse.name
                task_info.referenced_cluster_id = warehouse_id
            elif task_info.referenced_task_type == "notebook_compute" and task.existing_cluster_id:
                cluster_id = task.existing_cluster_id
                cluster = self.workspace_client.clusters.get(cluster_id)
                task_info.referenced_cluster_name = cluster.cluster_name
                task_info.referenced_cluster_id = cluster_id
            else:
                error_msg = (
                    f"Unexpected error while creating task information. "
                    f"Task is of type {task_info.referenced_task_type} but required values are not present in"
                    f"task {task}"
                )
                logger.error(error_msg)
                raise WorkflowManagerError(error_msg)

            return task_info

        except Exception as e:
            error_msg = f"Failed to get info for task '{task.task_key}'"
            logger.error("Failed to get info for task '{}: {}", task.task_key, e)
            raise WorkflowManagerError(error_msg) from e

    def _retrieve_task_type(self, task: Task) -> Optional[str]:
        """Determines the type of dependency a task has, if any."""
        if task.pipeline_task:
            return "pipeline"
        if task.run_job_task:
            return "job"
        if task.notebook_task:
            if task.notebook_task.warehouse_id:
                return "notebook_warehouse"
            if task.existing_cluster_id:
                return "notebook_compute"
        return None

    def create_task_from_workflow_task_info(self, wf_info: WorkflowTasksInfo, task: Task) -> Task:
        """
        Updates a task object with the correct entity IDs for the current workspace.
        """
        task_type = wf_info.referenced_task_type
        logger.info(f"Updating task '{task.task_key}' of type '{task_type}' with new workspace IDs.")

        if task_type == "pipeline":
            if not wf_info.referenced_task_name:
                error_msg = f"Error, cannot construct task, received empty task name {wf_info}"
                logger.error(error_msg)
                raise WorkflowManagerError(error_msg)
            new_id = self.dlt_manager.retrieve_pipeline_id_from_name(wf_info.referenced_task_name)
            task.pipeline_task.pipeline_id = new_id  # type:ignore[union-attr]
        elif task_type == "job":
            if not wf_info.referenced_task_name:
                error_msg = f"Error, cannot construct task, received empty task name {wf_info}"
                logger.error(error_msg)
                raise WorkflowManagerError(error_msg)
            new_id = self.job_manager.retrieve_job_id_from_name(wf_info.referenced_task_name)
            task.run_job_task.job_id = int(new_id)  # type:ignore[union-attr]
        elif task_type == "notebook_warehouse":
            if not wf_info.referenced_cluster_name:
                error_msg = f"Error, cannot construct task, received empty cluster name {wf_info}"
                logger.error(error_msg)
                raise WorkflowManagerError(error_msg)
            sql_warehouse_id = self.workspace_manager.get_sql_warehouse_id_from_name(wf_info.referenced_cluster_name)
            task.notebook_task.warehouse_id = sql_warehouse_id  # type:ignore[union-attr]
        elif task_type == "notebook_compute":
            if not wf_info.referenced_cluster_name:
                error_msg = f"Error, cannot construct task, received empty cluster name {wf_info}"
                logger.error(error_msg)
                raise WorkflowManagerError(error_msg)
            new_id = self.workspace_manager.get_compute_cluster_id_from_name(wf_info.referenced_cluster_name)
            task.existing_cluster_id = new_id

        return task

    def reconstruct_job_with_correct_ids(self, original_job: Job, workflow_info_list: List[WorkflowTasksInfo]) -> Job:
        """
        Rebuilds a job definition by replacing stale entity IDs in its tasks
        with fresh IDs from the target workspace.
        """
        if not (original_job.settings and original_job.settings.tasks):
            return original_job
        tasks = original_job.settings.tasks or []
        info_map = {info.task_key: info for info in workflow_info_list}
        updated_tasks: list[Task] = []

        for task in tasks:
            if task.task_key in info_map:
                wf_info = info_map[task.task_key]
                updated_task = self.create_task_from_workflow_task_info(wf_info, task)
                updated_tasks.append(updated_task)
            else:
                updated_tasks.append(task)

        original_job.settings.tasks = updated_tasks
        return original_job
