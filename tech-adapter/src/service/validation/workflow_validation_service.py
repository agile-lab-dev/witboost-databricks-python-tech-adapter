from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Format,
    Job,
    JobEmailNotifications,
    JobSettings,
    WebhookNotifications,
)
from loguru import logger

from src import settings
from src.models.databricks.databricks_models import WorkflowWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.exceptions import ProvisioningError
from src.service.clients.databricks.job_manager import JobManager


def validate_workflow_for_provisioning(
    job_manager: JobManager,
    workspace_client: WorkspaceClient,
    component: WorkflowWorkload,
    environment: str,
    databricks_workspace_info: DatabricksWorkspaceInfo,
) -> None:
    """
    Validates a workflow definition against the state of a Databricks workspace.

    This method performs several checks:
    1.  Ensures the workflow name is unique or does not exist.
    2.  If a workflow with the same name exists, it compares the definitions.
    3.  If definitions differ, it checks for an 'override' flag.
    4.  In a development environment, it enforces that changes require reverse provisioning.
    5.  It prevents overwriting a non-empty workflow with a default empty one.

    Args:
        component: The component being provisioned.
        specific: The specific configuration for the workflow workload.
        environment: The name of the deployment environment (e.g., 'development').
        databricks_workspace_info: Information about the target Databricks workspace.

    Raises:
        ProvisioningError: If any validation check fails.
    """
    workflow = component.specific.workflow
    if not (workflow.settings and workflow.settings.name):
        error_msg = "Error, received empty specificworkflow.settings.name. Name is required to manage the workload"
        logger.error(error_msg)
        raise ProvisioningError([error_msg])

    workspace_name = databricks_workspace_info.name
    workflow_name = workflow.settings.name

    # Check if a workflow with the same name already exists
    workflow_list = job_manager.list_jobs_with_given_name(workflow_name)

    # Case 1: No workflow with the same name exists. Validation passes.
    if not workflow_list:
        logger.info("Validation for deployment of {} succeeded.", component.name)
        return

    # Case 2: More than one workflow with the same name. This is an error.
    if len(workflow_list) > 1:
        error_msg = (
            f"Error during validation for deployment of {component.name}. Found more than one workflow "
            f"named {workflow_name} in workspace {workspace_name}."
            f"Please leave this name only to the workflow linked to the Witboost component."
        )
        logger.error(error_msg)
        raise ProvisioningError([error_msg])

    # Case 3: Exactly one workflow with the same name exists. Compare them.
    existing_workflow_summary = workflow_list[0]
    if not existing_workflow_summary.job_id:
        error_msg = (
            f"Error validating workflow '{workflow_name}' in {workspace_name}. "
            f"Received empty response from Databricks"
        )
        logger.error(error_msg)
        logger.debug("Response returned by Databricks for '{}': {}", workflow_name, workflow_list[0])
        raise ProvisioningError([error_msg])

    existing_workflow = workspace_client.jobs.get(job_id=existing_workflow_summary.job_id)

    # Create a comparable version of the request workflow by aligning metadata fields
    # that are set by Databricks, not by the user.
    request_workflow = workflow
    request_workflow.created_time = existing_workflow.created_time
    request_workflow.creator_user_name = existing_workflow.creator_user_name
    request_workflow.job_id = existing_workflow.job_id
    request_workflow.run_as_user_name = existing_workflow.run_as_user_name

    # If the definitions are different, further checks are needed
    if existing_workflow != request_workflow:
        # If override is true, validation passes.
        if component.specific.override:
            logger.info("Validation for deployment of {} succeeded (override is true).", component.name)
            return

        # In a development environment, any difference requires reverse provisioning.
        if environment.lower() == settings.misc.development_environment_name.lower():
            error_msg = (
                f"Error during validation for deployment of {component.name}. "
                f"The request workflow [name: {workflow_name}, "
                f"id: {existing_workflow.job_id}, workspace: {workspace_name}] is different from that on Databricks. "
                "Kindly perform reverse provisioning and try again."
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])

        # Check if the request is trying to overwrite a real workflow with an empty one.
        # This prevents accidental deletion of a configured job.
        empty_workflow_to_compare = Job(
            created_time=request_workflow.created_time,
            creator_user_name=request_workflow.creator_user_name,
            job_id=request_workflow.job_id,
            run_as_user_name=request_workflow.run_as_user_name,
            settings=JobSettings(
                name=workflow_name,
                email_notifications=JobEmailNotifications(),
                webhook_notifications=WebhookNotifications(),
                format=Format.MULTI_TASK,
                timeout_seconds=0,
                max_concurrent_runs=1,
                run_as=existing_workflow.settings.run_as if existing_workflow.settings else None,
            ),
        )

        if request_workflow == empty_workflow_to_compare:
            error_msg = (
                f"An error occurred during validation for deployment of {component.name}. It is not "
                f"permitted to replace a NON-empty workflow [name: {workflow_name}, id: {existing_workflow.job_id}, "
                f"workspace: {workspace_name}] with an empty one. Kindly perform reverse provisioning "
                "and try again."
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])

    # If workflows are identical or checks passed, validation is successful.
    logger.info("Validation for deployment of {} succeeded.", component.name)
