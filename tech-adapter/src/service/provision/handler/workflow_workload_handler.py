from typing import List

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.jobs import Job, JobRunAs
from loguru import logger

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import WorkflowWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.exceptions import ProvisioningError
from src.service.clients.databricks.job_manager import JobManager
from src.service.clients.databricks.repo_manager import RepoManager
from src.service.clients.databricks.workflow_manager import WorkflowManager
from src.service.clients.databricks.workspace_manager import WorkspaceManager
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler


class WorkflowWorkloadHandler(BaseWorkloadHandler):
    """
    Handles the provisioning and unprovisioning of Databricks Workflow workloads.

    This class orchestrates the entire lifecycle of a workflow, including
    mapping principals, creating repositories, deploying the job definition,
    and cleaning up resources.
    """

    def __init__(self, account_client: AccountClient):
        """
        Initializes the WorkflowWorkloadHandler.

        Args:
            account_client: An authenticated Databricks AccountClient for mapping principals.
        """
        super().__init__(account_client)

    def provision_workflow(
        self,
        data_product: DataProduct,
        component: WorkflowWorkload,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> str:
        """
        Provisions a new Databricks workflow based on the request.

        This method performs the following steps:
        1. Maps the data product owner and dev group to their Databricks representations.
        2. Creates a corresponding Git repository in the workspace with appropriate permissions.
        3. Deploys the workflow (job) definition to the workspace.
        4. Updates the job permissions

        Args:
            data_product: The data product related to the job to provision
            component: The component storing the definition of the workflow to provision
            workspace_client: The client for the target Databricks workspace.
            workspace_info: Information about the target Databricks workspace.

        Returns:
            The ID of the provisioned workflow as a string.

        Raises:
            ProvisioningError: If any step in the provisioning process fails.
        """
        component_name = component.name
        try:
            # 1. Map principals
            principals_mapping = self.map_principals(data_product, component)
            owner_id = principals_mapping.get(data_product.dataProductOwner)

            # Temporary solution to handle group prefix, as in the original code
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

            # 3. Provision the workflow job
            workflow_id = self._provision_workflow_internal(
                data_product,
                component,
                workspace_client,
                workspace_info.name,
            )

            # 4. Update job permissions
            self.update_job_permissions(workspace_client, workflow_id, owner_id, dev_group_id)

            logger.info("Workspace available at: {}", workspace_info.databricks_host)
            workflow_url = f"https://{workspace_info.databricks_host}/jobs/{workflow_id}"
            logger.info(
                "New workflow linked to component {} available at {}",
                component_name,
                workflow_url,
            )

            return str(workflow_id)

        except ProvisioningError:
            raise
        except Exception as e:
            error_msg = (
                f"An error occurred while provisioning component {component_name}. Please try again and "
                "if the error persists contact the platform team"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def _provision_workflow_internal(
        self,
        data_product: DataProduct,
        component: WorkflowWorkload,
        workspace_client: WorkspaceClient,
        workspace_name: str,
    ) -> int:
        """
        Internal method to create or update the workflow (job) in Databricks.
        """
        component_name = component.name
        specific = component.specific

        try:
            workflow_manager = WorkflowManager(self.account_client, workspace_client, workspace_name)

            original_workflow: Job = specific.workflow

            # Reconstruct the job with correct entity IDs for the target workspace
            updated_workflow = workflow_manager.reconstruct_job_with_correct_ids(
                original_workflow, specific.workflow_tasks_info or []
            )

            logger.info("({}) Original workflow definition: {}", component_name, original_workflow)
            logger.info("({}) Updated workflow definition: {}", component_name, updated_workflow)

            # We always set the run_as to the one stored on runAsPrincipalName, since we have different SPs
            # for each environment, so we cannot use the ones stored on the workflow settings (which anyway should be
            # null)
            if specific.runAsPrincipalName and not specific.runAsPrincipalName.isspace():
                logger.info("Run As Service Principal is defined, configuring workflow settings")
                logger.info(
                    "Retrieving service principal application id for '{}' in workspace '{}'",
                    specific.runAsPrincipalName,
                    workspace_name,
                )
                workspace_manager = WorkspaceManager(workspace_client, self.account_client)
                service_principal = workspace_manager.get_service_principal_from_name(specific.runAsPrincipalName)
                if not service_principal:
                    error_msg = (
                        f"Run As Service Principal {specific.runAsPrincipalName} doesn't exist "
                        f"on target workspace. Only Service Principals are allowed to run workflows"
                    )
                    logger.error(error_msg)
                    raise ProvisioningError([error_msg])
                updated_workflow.settings.run_as = JobRunAs(service_principal_name=service_principal.application_id)  # type:ignore

            # Create or update the job in Databricks
            return workflow_manager.create_or_update_workflow(updated_workflow)
        except ProvisioningError:
            raise
        except Exception as e:
            error_msg = (
                f"An error occurred while creating the new Databricks workflow for component {component_name}. "
                "Please try again and if the error persists contact the platform team"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def unprovision_workload(
        self,
        data_product: DataProduct,
        component: WorkflowWorkload,
        remove_data: bool,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> None:
        """
        Unprovisions a Databricks workflow, deleting the job and optionally the repo.

        This method will:
        1. Find and delete the Databricks job associated with the workload.
        2. If the 'remove_data' flag is set, delete the associated Git repository.

        Args:
            provision_request: The request containing component and unprovisioning specifics.
            workspace_client: The client for the target Databricks workspace.
            workspace_info: Information about the target Databricks workspace.

        Raises:
            ProvisioningError: If the unprovisioning process fails.
        """
        component_name = component.name
        specific = component.specific
        if not (specific.workflow_tasks_info and len(specific.workflow_tasks_info) > 0):
            error_msg = "Error, received empty specific.workflowTasksInfoList"
            logger.error(error_msg)
            raise ProvisioningError([error_msg])
        if not (specific.workflow.settings and specific.workflow.settings.name):
            error_msg = (
                "Error, received empty specific.workflow.settings.name. " "Name is required to unprovision the workload"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])

        try:
            job_manager = JobManager(workspace_client, workspace_info.name)
            # 1. Find and delete the job
            job_name = specific.workflow.settings.name
            logger.info("Searching for jobs named '{}' to delete.", job_name)

            jobs_to_delete = job_manager.list_jobs_with_given_name(specific.workflow.settings.name)

            errors: List[Exception] = []
            for job in jobs_to_delete:
                try:
                    if job.job_id:
                        job_manager.delete_job(job.job_id)
                except Exception as e:
                    errors.append(e)

            if errors:
                logger.error("{} errors found while deleting jobs: {}", len(errors), errors)
                raise ProvisioningError([str(error) for error in errors])

            # 2. Optionally delete the repository
            if remove_data:
                repo_manager = RepoManager(workspace_client, workspace_info.name)
                repo_path = specific.repoPath
                if not repo_path.startswith("/"):
                    repo_path = f"/{repo_path}"

                repo_manager.delete_repo(specific.git.gitRepoUrl, repo_path)
            else:
                logger.info(
                    "The repository '{}' associated with component '{}' will not be removed because the 'remove_data' "
                    "flag is false.",
                    specific.git.gitRepoUrl,
                    component_name,
                )
        except ProvisioningError:
            raise
        except Exception as e:
            error_msg = (
                f"An error occurred while unprovisioning component {component_name}. "
                "Please try again and if the error persists contact the platform team."
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e
