from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.jobs import Job
from loguru import logger

from src.models.api_models import ReverseProvisioningRequest
from src.models.databricks.catalog_info import CatalogInfo
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.reverse_provision.workflow_reverse_provisioning_params import (
    WorkflowReverseProvisioningParams,
)
from src.models.databricks.workload.databricks_workflow_specific import WorkflowTasksInfo
from src.models.databricks.workload.databricks_workload_specific import DatabricksWorkloadSpecific
from src.models.exceptions import ReverseProvisioningError
from src.service.clients.azure.azure_workspace_handler import WorkspaceHandler
from src.service.clients.databricks.job_manager import JobManager
from src.service.clients.databricks.workflow_manager import WorkflowManager
from src.service.clients.databricks.workspace_manager import WorkspaceManager


class WorkflowReverseProvisionHandler:
    """
    Handles the reverse provisioning process for Databricks Workflow workloads.
    """

    def __init__(self, account_client: AccountClient, workspace_handler: WorkspaceHandler):
        """
        Initializes the WorkflowReverseProvisionHandler.

        Args:
            workspace_handler: A handler to interact with Databricks workspaces.
        """
        self.account_client = account_client
        self.workspace_handler = workspace_handler

    def reverse_provision(self, reverse_provisioning_request: ReverseProvisioningRequest) -> Dict[str, Any]:
        """
        Performs reverse provisioning for a Databricks workflow.

        This method extracts the workflow definition and its task dependencies from
        a Databricks workspace and prepares a set of updates to be applied to a
        descriptor file (e.g., in a data catalog).

        Args:
            reverse_provisioning_request: The request containing parameters and catalog info.

        Returns:
            A dictionary of updates with dot-separated keys representing the paths
            to be updated in the descriptor.

        Raises:
            ReverseProvisioningError: If validation fails or any error occurs during the process.
        """
        try:
            # 1. Parse input request using Pydantic models
            catalog_info = CatalogInfo(**reverse_provisioning_request.catalogInfo)
            params = WorkflowReverseProvisioningParams(**reverse_provisioning_request.params)
            component_name = catalog_info.spec.mesh.name
            logger.info("({}) Started reverse provisioning.", component_name)

            # 2. Extract key information
            env_config = params.environment_specific_config.specific
            workspace_name = env_config.workspace

            if not (env_config.workflow.settings and env_config.workflow.settings.name):
                error_msg = (
                    "Error, received empty specific workflow.settings.name. " "Name is required to manage the workload"
                )
                logger.error(error_msg)
                raise ReverseProvisioningError(error_msg)
            workflow_name = env_config.workflow.settings.name

            # 3. Get workspace client and validate the request
            workspace_info = self.workspace_handler.get_workspace_info_by_name(workspace_name)
            if not workspace_info:
                error_msg = f"Validation failed. Workspace '{workspace_name}' not found."
                logger.error(error_msg)
                raise ReverseProvisioningError(error_msg)

            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)
            workflow_id = self._validate_provision_request(workspace_client, workspace_info, workflow_name)

            # 4. Validation successful, start reverse provisioning
            logger.info("Validation successful. Starting reverse provisioning for workflow ID: {}", workflow_id)
            workflow = workspace_client.jobs.get(job_id=workflow_id)
            tasks = workflow.settings.tasks if (workflow.settings and workflow.settings.tasks) else []

            # 5. Collect workspace-dependent task info
            workflow_manager = WorkflowManager(self.account_client, workspace_client, workspace_name)
            workflow_tasks_info_list: List[WorkflowTasksInfo] = []
            for task in tasks:
                task_info_optional = workflow_manager.get_workflow_task_info_from_task(task)
                if task_info_optional:
                    workflow_tasks_info_list.append(task_info_optional)

            # 6. We check that the user didn't change the Run As
            run_as_name = workflow.run_as_user_name
            input_run_as: Optional[str] = None
            # Get it from env specific config, fallback to catalog info
            if params.environment_specific_config.specific.run_as_principal_name:
                input_run_as = params.environment_specific_config.specific.run_as_principal_name
            elif isinstance(catalog_info.spec.mesh.specific, DatabricksWorkloadSpecific):
                input_run_as = catalog_info.spec.mesh.specific.runAsPrincipalName

            if input_run_as and run_as_name and not run_as_name.isspace():
                workspace_manager = WorkspaceManager(workspace_client, self.account_client)
                service_principal = workspace_manager.get_service_principal(run_as_name)
                if service_principal and service_principal.display_name:
                    if service_principal.display_name.lower() != input_run_as:
                        error_msg = (
                            f"The current version of Witboost doesn't allow the workflow Run As to be modified. "
                            f"Please revert changes by redeploying the workflow or by manually setting "
                            f"the Run As with the appropriate Service Principal "
                            f"'{input_run_as}' "
                        )
                        logger.error(error_msg)
                        raise ReverseProvisioningError(error_msg)
                else:
                    error_msg = (
                        f"Run As Service Principal '{run_as_name}' doesn't exist on target workspace. "
                        f"Only Service Principals are allowed to run workflows. "
                        f"Please revert changes by redeploying the workflow or by manually setting "
                        f"the Run As with the appropriate Service Principal"
                    )
                    logger.error(error_msg)
                    raise ReverseProvisioningError(error_msg)
            # 7. Prepare and return the final updates
            updates = self._prepare_updates(workflow, workflow_tasks_info_list)
            logger.info("({}) Reverse Provision updates are ready: {}", component_name, updates)
            return updates

        except ReverseProvisioningError:
            raise
        except Exception as e:
            error_msg = f"An unexpected error occurred during reverse provisioning: {e}"
            logger.error(error_msg)
            raise ReverseProvisioningError(error_msg) from e

    def _validate_provision_request(
        self,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
        workflow_name: str,
    ) -> int:
        """
        Validates that exactly one workflow with the given name exists.
        """
        try:
            job_manager = JobManager(workspace_client, workspace_info.name)
            workflow_list = job_manager.list_jobs_with_given_name(workflow_name)

            if not workflow_list:
                error_msg = f"Workflow {workflow_name} not found in {workspace_info.name}"
                logger.error(error_msg)
                raise ReverseProvisioningError(error_msg)

            if len(workflow_list) > 1:
                error_msg = f"Workflow {workflow_name} is not unique in {workspace_info.name}."
                logger.error(error_msg)
                raise ReverseProvisioningError(error_msg)

            if not workflow_list[0].job_id:
                error_msg = (
                    f"Error during reverse provision of workflow '{workflow_name}' in {workspace_info.name}. "
                    f"Received empty response from Databricks"
                )
                logger.error(error_msg)
                logger.debug("Response returned by Databricks for '{}': {}", workflow_name, workflow_list[0])
                raise ReverseProvisioningError(error_msg)

            return workflow_list[0].job_id
        except Exception as e:
            error_msg = f"Failed to list jobs named '{workflow_name}' in workspace '{workspace_info.name}'"
            logger.error(error_msg)
            raise ReverseProvisioningError(error_msg) from e

    def _prepare_updates(self, workflow: Job, workflow_tasks_info_list: List[WorkflowTasksInfo]) -> Dict[str, Any]:
        """
        Constructs the dictionary of updates to be applied to the descriptor.
        """
        tasks_info_as_dicts = [info.model_dump(by_alias=True) for info in workflow_tasks_info_list]
        settings_as_dict = workflow.settings.as_dict() if workflow.settings else {}

        # The timestamp format 'yyyy-MM-dd'T'HH:mm:ss.SSS'Z'' is ISO 8601 with milliseconds.
        # Python's isoformat() is very close. We slice to get 3 decimal places for seconds
        # and append 'Z' for UTC.
        now_utc = datetime.now(timezone.utc)
        timestamp = now_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        updates = {
            "spec.mesh.specific.workflowTasksInfoList": tasks_info_as_dicts,
            "spec.mesh.specific.workflow.job_id": workflow.job_id,
            "spec.mesh.specific.workflow.created_time": workflow.created_time,
            "spec.mesh.specific.workflow.creator_user_name": workflow.creator_user_name,
            "spec.mesh.specific.workflow.settings": settings_as_dict,
            "spec.mesh.specific.workflow.settings.name": "IGNORED",
            "witboost.parameters.modifiedByRef": "databricks-workload-workflow-reverse-provisioning-template.1",
            "witboost.parameters.updatedDate": timestamp,
        }
        return updates
