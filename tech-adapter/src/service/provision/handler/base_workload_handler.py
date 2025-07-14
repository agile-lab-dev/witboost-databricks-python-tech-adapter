import os
import threading
from typing import Dict, Mapping, Set, Union

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel
from databricks.sdk.service.pipelines import (
    PipelineAccessControlRequest,
    PipelinePermissionLevel,
)
from databricks.sdk.service.workspace import RepoPermissionLevel
from loguru import logger

from src import settings
from src.models.data_product_descriptor import DataProduct, Workload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import DatabricksMapperError
from src.models.databricks.workload.databricks_workload_specific import DatabricksWorkloadSpecific
from src.models.exceptions import ProvisioningError
from src.service.clients.azure.azure_workspace_handler import OAuthWorkspaceClientConfigParams, create_workspace_client
from src.service.clients.databricks.repo_manager import RepoManager
from src.service.clients.databricks.workspace_manager import WorkspaceManager
from src.service.principals_mapping.databricks_mapper import DatabricksMapper


class BaseWorkloadHandler:
    def __init__(
        self,
        account_client: AccountClient,
    ):
        self.account_client = account_client
        # Lock to ensure thread-safety for repository creation
        self._repo_creation_lock = threading.Lock()

    def create_repository_with_permissions(
        self,
        specific: DatabricksWorkloadSpecific,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
        owner_name: str,
        developer_group_name: str,
    ) -> None:
        with self._repo_creation_lock:
            try:
                git_repo_url = specific.git.gitRepoUrl
                repo_path = specific.repoPath

                workspace_manager = WorkspaceManager(workspace_client, self.account_client)

                # Set git credentials for the provisioning principal

                workspace_manager.set_git_credentials(settings.git)

                folder_path = os.path.dirname(repo_path)
                logger.info("Creating parent directory '/{}'", folder_path)
                workspace_client.workspace.mkdirs(path=f"/{folder_path}")

                repo_path_full = f"/{repo_path}"

                logger.info(
                    "Creating repository in workspace '{}' from git repo '{}' at path '{}'",
                    workspace_info.name,
                    git_repo_url,
                    repo_path_full,
                )

                repo_manager = RepoManager(workspace_client, workspace_info.name)
                repo_id = repo_manager.create_repo(git_repo_url, settings.git.provider, repo_path_full)

                # If workspace is managed, ensure users/groups exist with correct permissions
                if workspace_info.is_managed:
                    # TODO Manage workspace
                    error_msg = "Witboost managed workspace are not yet supported on this version"
                    logger.error(error_msg)
                    raise ProvisioningError([error_msg])
                else:
                    logger.info(
                        "Skipping upsert of project owner and development group to workspace since workspace "
                        "is not set to be managed by the Tech Adapter."
                    )

                # --- Update Repo Permissions ---
                repo_config_permissions = settings.databricks.permissions.workload.repo
                repo_id_str = str(repo_id)

                # Set Owner Permissions
                logger.info("Updating repo permissions for owner '{}' for repository with id '{}'", owner_name, repo_id)
                if repo_config_permissions.owner.upper() == "NO_PERMISSIONS":
                    repo_manager.remove_permissions_from_user(repo_id_str, owner_name)
                else:
                    permission = RepoPermissionLevel[repo_config_permissions.owner.upper()]
                    repo_manager.assign_permissions_to_user(repo_id_str, owner_name, permission)

                # Set Developer Group Permissions
                logger.info(
                    "Updating repo permissions for group '{}' for repository with id '{}'",
                    developer_group_name,
                    repo_id,
                )
                if repo_config_permissions.developer.upper() == "NO_PERMISSIONS":
                    repo_manager.remove_permissions_from_group(repo_id_str, developer_group_name)
                else:
                    permission = RepoPermissionLevel[repo_config_permissions.developer.upper()]
                    repo_manager.assign_permissions_to_group(repo_id_str, developer_group_name, permission)

            except Exception as e:
                error_msg = f"Error creating repository {git_repo_url} in {workspace_info.name}. Details: {e}"
                logger.error(error_msg)
                raise ProvisioningError([error_msg]) from e

    def map_principals(self, data_product: DataProduct, component: Workload) -> Dict[str, str]:
        """
        Maps principals from a provision request to Databricks-recognized formats.

        This method takes the data product owner and development group from the
        request, ensures they are correctly formatted, and uses the DatabricksMapper
        to resolve them to their canonical names (e.g., case-sensitive group names).

        Args:
            data_product: An object containing data product information,
                          like `data_product_owner` and `dev_group` fields.
            component: An object containing the workload component information

        Returns:
            A dictionary mapping the original principal identifiers to their
            validated and formatted representations.

        Raises:
            ProvisioningError: If any principal fails to be mapped or an
                               unexpected error occurs during the process.
        """

        try:
            databricks_mapper = DatabricksMapper(self.account_client)

            owner = data_product.dataProductOwner
            dev_group = data_product.devGroup

            # This logic is a temporary solution
            # Ensure the group subject is correctly prefixed.
            if not dev_group.startswith("group:"):
                dev_group = f"group:{dev_group}"

            subjects_to_map: Set[str] = {owner, dev_group}

            # The mapper returns a dictionary with either a mapped string or an Exception
            mapped_principals: Mapping[str, Union[str, DatabricksMapperError]] = databricks_mapper.map(subjects_to_map)

            # Validate the results and build the final dictionary of successful mappings
            successful_mappings: Dict[str, str] = {}
            errors: list[Exception] = []
            for original_subject, result in mapped_principals.items():
                if isinstance(result, Exception):
                    # If any mapping failed, raise an error with details
                    error_msg = f"Failed to map principal '{original_subject}': {result}"
                    logger.error(error_msg)
                    errors.append(result)
                else:
                    successful_mappings[original_subject] = result

            if len(errors) > 0:
                raise ProvisioningError(errors=[str(error) for error in errors])

            logger.info("Successfully mapped all principals.")
            return successful_mappings

        except Exception as e:
            # Catch any other unexpected exception and wrap it
            error_msg = (
                "An unexpected error occurred while mapping principals for component "
                f"'{component.name}'. Details: {e}"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def update_job_permissions(
        self,
        workspace_client: WorkspaceClient,
        job_id: int,
        dp_owner_id: str,
        dp_dev_group_id: str,
    ) -> None:
        try:
            job_perms_config = settings.databricks.permissions.workload.job
            access_control_list = []

            if job_perms_config.owner.upper() != "NO_PERMISSIONS":
                access_control_list.append(
                    JobAccessControlRequest(
                        user_name=dp_owner_id,
                        permission_level=JobPermissionLevel[job_perms_config.owner.upper()],
                    )
                )

            if job_perms_config.developer.upper() != "NO_PERMISSIONS":
                access_control_list.append(
                    JobAccessControlRequest(
                        group_name=dp_dev_group_id,
                        permission_level=JobPermissionLevel[job_perms_config.developer.upper()],
                    )
                )

            if not access_control_list:
                logger.info("No permissions to update for job {}", job_id)
                return

            logger.info(
                "Updating permissions for job {}: Owner gets '{}', Dev Group gets '{}'",
                job_id,
                job_perms_config.owner,
                job_perms_config.developer,
            )

            workspace_client.jobs.update_permissions(job_id=str(job_id), access_control_list=access_control_list)
            logger.info("Permissions for job {} updated successfully", job_id)

        except Exception as e:
            error_msg = f"Error updating permissions for job {job_id}. Details: {e}"
            logger.error(error_msg, exc_info=True)
            raise ProvisioningError([error_msg]) from e

    def update_pipeline_permissions(
        self,
        workspace_client: WorkspaceClient,
        pipeline_id: str,
        dp_owner_id: str,
        dp_dev_group_id: str,
    ) -> None:
        try:
            pipeline_perms_config = settings.databricks.permissions.workload.pipeline
            access_control_list = []

            if pipeline_perms_config.owner.upper() != "NO_PERMISSIONS":
                access_control_list.append(
                    PipelineAccessControlRequest(
                        user_name=dp_owner_id,
                        permission_level=PipelinePermissionLevel[pipeline_perms_config.owner.upper()],
                    )
                )

            if pipeline_perms_config.developer.upper() != "NO_PERMISSIONS":
                access_control_list.append(
                    PipelineAccessControlRequest(
                        group_name=dp_dev_group_id,
                        permission_level=PipelinePermissionLevel[pipeline_perms_config.developer.upper()],
                    )
                )

            if not access_control_list:
                logger.info("No permissions to update for DLT pipeline {}", pipeline_id)
                return

            logger.info(
                "Updating permissions for DLT pipeline {}: Owner gets '{}', Dev Group gets '{}'",
                pipeline_id,
                pipeline_perms_config.owner,
                pipeline_perms_config.developer,
            )

            workspace_client.pipelines.update_permissions(
                pipeline_id=pipeline_id, access_control_list=access_control_list
            )
            logger.info("Permissions for DLT pipeline {} updated successfully", pipeline_id)

        except Exception as e:
            error_msg = f"Error updating permissions for DLT pipeline {pipeline_id}. Details: {e}"
            logger.error(error_msg, exc_info=True)
            raise ProvisioningError([error_msg]) from e

    def _get_workspace_client_for_service_principal(
        self, workspace_url: str, sp_client_id: str, sp_client_secret: str, workspace_name: str
    ) -> WorkspaceClient:
        try:
            logger.info("Creating workspace client for workspace '{}' as principal '{}'", workspace_name, sp_client_id)
            params = OAuthWorkspaceClientConfigParams(
                databricks_client_id=sp_client_id,
                databricks_client_secret=sp_client_secret,
                workspace_host=workspace_url,
                workspace_name=workspace_name,
            )
            ws_client = create_workspace_client(params)
            logger.info("Successfully created workspace client for principal {}", sp_client_id)
            return ws_client
        except Exception as e:
            error_msg = (
                f"Error creating workspace client for workspace '{workspace_name}' as principal '{sp_client_id}'"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def set_service_principal_git_credentials(
        self,
        workspace_client: WorkspaceClient,
        workspace_host: str,
        workspace_name: str,
        principal_name: str,
    ) -> None:
        try:
            workspace_manager = WorkspaceManager(workspace_client, self.account_client)

            logger.info(
                "Setting Git credentials for service principal '{}' in workspace '{}'", principal_name, workspace_name
            )

            principal = workspace_manager.get_service_principal_from_name(principal_name)
            if not principal or not principal.id or not principal.application_id:
                error_msg = f"Cannot find service principal information for {principal_name}"
                logger.error(error_msg)
                raise ProvisioningError([error_msg])
            principal_id = int(principal.id)
            principal_app_id = principal.application_id

            logger.info("Generating temporary secret for principal '{}'", principal_name)
            secret_id, secret_value = workspace_manager.generate_secret_for_service_principal(principal_id, 900)

            try:
                run_as_client = self._get_workspace_client_for_service_principal(
                    workspace_host, principal_app_id, secret_value, workspace_name
                )

                # Re-create manager with the new client to perform the action
                run_as_workspace_manager = WorkspaceManager(run_as_client, self.account_client)
                run_as_workspace_manager.set_git_credentials(settings.git)
            finally:
                # Always clean up the secret
                logger.info("Cleaning up temporary secret for principal '{}'", principal_name)
                workspace_manager.delete_service_principal_secret(principal_id, secret_id)

            logger.info(
                "Successfully set Git credentials for service principal '{}' in workspace '{}'",
                principal_name,
                workspace_name,
            )

        except Exception as e:
            error_msg = f"Failed to set Git credentials for principal '{principal_name}'"
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e
