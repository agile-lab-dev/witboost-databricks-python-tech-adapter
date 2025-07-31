from typing import List

from databricks.sdk import AccountClient, WorkspaceClient
from loguru import logger

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import DLTWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.exceptions import ProvisioningError, build_error_message_from_chained_exception
from src.service.clients.databricks.dlt_manager import DLTManager
from src.service.clients.databricks.repo_manager import RepoManager
from src.service.clients.databricks.unity_catalog_manager import UnityCatalogManager
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler


class DLTWorkloadHandler(BaseWorkloadHandler):
    """
    Handles the provisioning and unprovisioning of Databricks DLT pipeline workloads.
    """

    def __init__(self, account_client: AccountClient):
        """
        Initializes the DLTWorkloadHandler.

        Args:
            account_client: An authenticated Databricks AccountClient for mapping principals.
        """
        super().__init__(account_client)

    def provision_workload(
        self,
        data_product: DataProduct,
        component: DLTWorkload,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> str:
        """
        Provisions a Databricks Delta Live Tables (DLT) pipeline workload.

        This method orchestrates the setup of a DLT pipeline, including:
        1. Attaching the workspace to a metastore (if managed).
        2. Ensuring the target catalog exists.
        3. Mapping principals and creating a Git repository with permissions.
        4. Creating or updating the DLT pipeline definition in the workspace.

        Args:
            data_product: Data Product containing the owner and development group information
            component: Component containing the information defining the DLT pipeline to be provisioned
            workspace_client: The client for the target Databricks workspace.
            workspace_info: Information about the target Databricks workspace.

        Returns:
            The ID of the provisioned DLT pipeline as a string.

        Raises:
            DLTProvisioningError: If any step in the provisioning process fails.
        """
        component_name = component.name
        try:
            specific = component.specific
            unity_catalog_manager = UnityCatalogManager(workspace_client, workspace_info)

            # 1. Attach metastore if workspace is managed
            if workspace_info.is_managed:
                if not specific.metastore:
                    error_msg = "Can't attach metastore as metastore name is not provided on the component specific"
                    logger.error(error_msg)
                    raise ProvisioningError([error_msg])
                unity_catalog_manager.attach_metastore(specific.metastore)
            else:
                logger.info("Skipping metastore attachment as workspace is not managed.")

            # 2. Ensure catalog exists
            unity_catalog_manager.create_catalog_if_not_exists(specific.catalog)

            # 3. Map principals
            mapped_principals = self.map_principals(data_product, component)
            owner_id = mapped_principals.get(data_product.dataProductOwner)

            # TODO: This is a temporary solution
            dev_group = data_product.devGroup
            if not dev_group.startswith("group:"):
                dev_group = f"group:{dev_group}"
            dev_group_id = mapped_principals.get(dev_group)

            if not owner_id or not dev_group_id:
                error_msg = "Error while mapping principals. Failed to retrieve outcome of mapping"
                logger.error(error_msg)
                raise ProvisioningError([error_msg])

            logger.debug("Creating repo with permissions to {} and {}", owner_id, dev_group_id)
            # 4. Create Git repository with initial permissions
            self.create_repository_with_permissions(
                specific,
                workspace_client,
                workspace_info,
                owner_id,
                dev_group_id,
            )

            # 5. Create or update the DLT pipeline
            dlt_manager = DLTManager(workspace_client, workspace_info.name)

            notebook_paths = [f"/Workspace/{nb}" for nb in specific.notebooks] if specific.notebooks else []

            notifications = {n.mail: n.alert for n in specific.notifications} if specific.notifications else {}

            pipeline_id = dlt_manager.create_or_update_dlt_pipeline(
                pipeline_name=specific.pipeline_name,
                product_edition=specific.product_edition,
                continuous=specific.continuous,
                notebooks=notebook_paths,
                files=specific.files or [],
                catalog=specific.catalog,
                target=specific.target,
                photon=specific.photon,
                notifications=notifications,
                channel=specific.channel,
                cluster_specific=specific.cluster,
            )

            pipeline_url = f"https://{workspace_info.databricks_host}/pipelines/{pipeline_id}"
            logger.info("New pipeline linked to component {} available at {}", component_name, pipeline_url)

            return pipeline_id

        except ProvisioningError:
            raise
        except Exception as e:
            error_msg = f"Error provisioning component '{component_name}'"
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def unprovision_workload(
        self,
        data_product: DataProduct,
        component: DLTWorkload,
        remove_data: bool,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> None:
        """
        Unprovisions a Databricks DLT pipeline workload.

        This method will:
        1. Find and delete all DLT pipelines matching the workload's pipeline name.
        2. If the 'remove_data' flag is set, delete the associated Git repository.

        Args:
            data_product: Data Product containing the owner and development group information
            component: Component containing the information defining the DLT pipeline to be unprovisioned
            workspace_client: The client for the target Databricks workspace.
            workspace_info: Information about the target Databricks workspace.

        Raises:
            ProvisioningError: If the unprovisioning process fails.
        """
        component_name = component.name
        try:
            specific = component.specific
            dlt_manager = DLTManager(workspace_client, workspace_info.name)

            # 1. Find and delete all matching pipelines
            pipelines_to_delete = dlt_manager.list_pipelines_with_given_name(specific.pipeline_name)

            problems: List[Exception] = []
            for pipeline in pipelines_to_delete:
                try:
                    if pipeline.pipeline_id:
                        dlt_manager.delete_pipeline(pipeline.pipeline_id)
                except Exception as e:
                    logger.exception(
                        "Error while deleting pipeline {} on {}", pipeline.pipeline_id, workspace_info.name
                    )
                    problems.append(e)

            if problems:
                logger.error("{} errors found while deleting pipelines: {}", len(problems), problems)
                raise ProvisioningError([build_error_message_from_chained_exception(error) for error in problems])

            # 2. Optionally delete the repository
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
        except ProvisioningError:
            raise
        except Exception as e:
            error_msg = f"Error unprovisioning component '{component_name}'"
            logger.error("Error unprovisioning component '{}'. Details: {}", component_name, e)
            raise ProvisioningError([error_msg]) from e
