from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.jobs import BaseJob
from loguru import logger

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.exceptions import ProvisioningError
from src.service.clients.databricks.job_manager import JobManager
from src.service.clients.databricks.repo_manager import RepoManager
from src.service.clients.databricks.workspace_manager import WorkspaceManager
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler


class JobWorkloadHandler(BaseWorkloadHandler):
    """
    Handles the provisioning and unprovisioning of Databricks Job workloads.
    """

    def __init__(self, account_client: AccountClient):
        super().__init__(account_client)

    def provision_workload(
        self,
        data_product: DataProduct,
        component: JobWorkload,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> str:
        """
        Provisions a new Databricks job for the given component.

        Args:
            data_product: The data product related to the job to provision
            component: The component storing the definition of the job to provision
            workspace_client: The authenticated Databricks workspace client.
            workspace_info: Information about the target Databricks workspace.

        Returns:
            The ID of the provisioned job as a string.

        Raises:
            ProvisioningError: If any step in the provisioning process fails.
        """
        component_name = component.name
        try:
            # 1. Map principals
            principals_mapping = self.map_principals(data_product, component)
            owner_id = principals_mapping.get(data_product.dataProductOwner)

            # TODO: This is a temporary solution
            dev_group = data_product.devGroup
            if not dev_group.startswith("group:"):
                dev_group = f"group:{dev_group}"
            dev_group_id = principals_mapping.get(dev_group)

            if not owner_id or not dev_group_id:
                error_msg = "Error while mapping principals. Failed to retrieve outcome of mapping"
                logger.error(error_msg)
                raise ProvisioningError([error_msg])

            # 2. Create Git repository with initial permissions
            self.create_repository_with_permissions(
                component.specific, workspace_client, workspace_info, owner_id, dev_group_id
            )

            # 3. Set Git credentials for the 'runAs' principal, if specified
            run_as_principal = component.specific.runAsPrincipalName
            if run_as_principal and run_as_principal.strip():
                self.set_service_principal_git_credentials(
                    workspace_client,
                    workspace_info.databricks_host,
                    workspace_info.name,
                    run_as_principal,
                )

            # 4. Create or update the Databricks Job
            job_id = self._create_job(component, workspace_client, workspace_info.name)

            # 5. Update job permissions
            self.update_job_permissions(workspace_client, job_id, owner_id, dev_group_id)

            job_url = f"https://{workspace_info.databricks_host}/jobs/{job_id}"
            logger.info("New job for component '{}' available at: {}", component_name, job_url)

            return str(job_id)

        except Exception as e:
            error_msg = f"Error provisioning component '{component_name}'"
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def unprovision_workload(
        self,
        data_product: DataProduct,
        component: JobWorkload,
        remove_data: bool,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> None:
        """
        Unprovisions a Databricks job, deleting the job and optionally the repository.

        Args:
            data_product: The data product related to the job to unprovision
            component: The component storing the definition of the job to unprovision
            remove_data: Flag to remove underlying repository
            workspace_client: The authenticated Databricks workspace client.
            workspace_info: Information about the target Databricks workspace.

        Raises:
            ProvisioningError: If any step in the unprovisioning process fails.
        """
        component_name = component.name
        try:
            specific = component.specific
            job_manager = JobManager(workspace_client, workspace_info.name)

            # Delete the job(s) matching the name
            errors: list[Exception] = []
            jobs_to_delete: list[BaseJob] = []
            try:
                jobs_to_delete = job_manager.list_jobs_with_given_name(specific.jobName)
            except Exception as e:
                # Log error but continue to allow repo deletion if needed
                errors.append(e)

            for job in jobs_to_delete:
                try:
                    if job.job_id:
                        job_manager.delete_job(job.job_id)
                except Exception as e:
                    errors.append(e)

            if errors:
                logger.error("{} errors found while deleting jobs: {}", len(errors), errors)
                raise ProvisioningError([str(error) for error in errors])

            # Delete the repository if requested
            if remove_data:
                repo_manager = RepoManager(workspace_client, workspace_info.name)
                repo_path = specific.repoPath
                if not repo_path.startswith("/"):
                    repo_path = f"/{repo_path}"

                repo_manager.delete_repo(specific.git.gitRepoUrl, repo_path)
            else:
                logger.info(
                    "Repository with URL '{}' for component '{}' will not be removed as 'removeData' is false.",
                    specific.git.gitRepoUrl,
                    component_name,
                )
        except Exception as e:
            error_msg = f"Error unprovisioning component '{component_name}'"
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def _create_job(
        self,
        component: JobWorkload,
        workspace_client: WorkspaceClient,
        workspace_name: str,
    ) -> int:
        """
        Private helper to create or update the Databricks job.

        Returns:
            The ID of the created or updated job.
        """
        try:
            specific = component.specific
            job_manager = JobManager(workspace_client, workspace_name)
            run_as_app_id = None

            run_as_principal_name = specific.runAsPrincipalName
            if run_as_principal_name and run_as_principal_name.strip():
                workspace_manager = WorkspaceManager(workspace_client, self.account_client)
                run_as_principal = workspace_manager.get_service_principal_from_name(run_as_principal_name)
                run_as_app_id = run_as_principal.application_id

            return job_manager.create_or_update_job_with_new_cluster(
                job_name=specific.jobName,
                description=specific.description or "",
                task_key="Task1",
                run_as=run_as_app_id,
                job_cluster_specific=specific.cluster,
                scheduling_specific=specific.scheduling,
                job_git_specific=specific.git,
            )
        except Exception as e:
            error_msg = f"Error creating Databricks job for component '{component.name}'"
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e
