from typing import Dict, Iterable, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceConflict, ResourceDoesNotExist
from databricks.sdk.service.pipelines import (
    FileLibrary,
    NotebookLibrary,
    Notifications,
    PipelineCluster,
    PipelineClusterAutoscale,
    PipelineClusterAutoscaleMode,
    PipelineLibrary,
    PipelineStateInfo,
)
from loguru import logger

from src.models.databricks.exceptions import DLTManagerError
from src.models.databricks.workload.databricks_dlt_workload_specific import (
    DLTClusterSpecific,
    PipelineChannel,
    ProductEdition,
)


class DLTManager:
    """
    Manages operations on Databricks Delta Live Table (DLT) pipelines.
    """

    def __init__(self, workspace_client: WorkspaceClient, workspace_name: str):
        """
        Initializes the DLTManager.

        Args:
            workspace_client: An authenticated Databricks WorkspaceClient.
            workspace_name: The name of the workspace to operate within.
        """
        self.workspace_client = workspace_client
        self.workspace_name = workspace_name

    def create_or_update_dlt_pipeline(
        self,
        pipeline_name: str,
        product_edition: ProductEdition,
        continuous: bool,
        notebooks: List[str],
        files: List[str],
        catalog: str,
        target: str,
        photon: bool,
        notifications: Dict[str, list[str]],
        channel: PipelineChannel,
        cluster_specific: DLTClusterSpecific,
    ) -> str:
        """
        Creates or updates a Delta Live Table (DLT) pipeline.

        If no pipeline with the given name exists, a new one is created.
        If a pipeline already exists, it updates the existing one.

        Args:
            pipeline_name: The name of the pipeline.
            product_edition: The product edition (ADVANCED, CORE, PRO).
            continuous: Indicates if the pipeline runs continuously.
            notebooks: List of notebook paths for the pipeline.
            files: List of file paths for the pipeline.
            catalog: The Unity Catalog name.
            target: The target schema or database.
            photon: Indicates if Photon is enabled.
            notifications: A dictionary of email recipients to alert types.
            channel: The pipeline channel ('current' or 'preview').
            cluster_specific: Cluster-specific configurations.

        Returns:
            The ID of the created or updated pipeline.

        Raises:
            DLTManagerError: For creation or update failures.
        """
        existing_pipelines = self.list_pipelines_with_given_name(pipeline_name)

        if not existing_pipelines:
            return self._create_dlt_pipeline(
                pipeline_name,
                product_edition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                cluster_specific,
            )

        if len(existing_pipelines) > 1:
            error_msg = (
                f"Error trying to update pipeline '{pipeline_name}'. The name is not unique in {self.workspace_name}."
            )
            logger.error(error_msg)
            raise DLTManagerError(error_msg)

        if not existing_pipelines[0].pipeline_id:
            error_msg = (
                f"Error creating job '{pipeline_name}' in {self.workspace_name}. "
                f"Received empty response from Databricks"
            )
            logger.error(error_msg)
            logger.debug("Response returned by Databricks for '{}': {}", pipeline_name, existing_pipelines[0])
            raise DLTManagerError(error_msg)

        pipeline_id = existing_pipelines[0].pipeline_id
        return self._update_dlt_pipeline(
            pipeline_id,
            pipeline_name,
            product_edition,
            continuous,
            notebooks,
            files,
            catalog,
            target,
            photon,
            notifications,
            channel,
            cluster_specific,
        )

    def delete_pipeline(self, pipeline_id: str) -> None:
        """
        Deletes an existing Delta Live Table (DLT) pipeline by its ID.

        If the pipeline does not exist, the method skips deletion and returns success.

        Args:
            pipeline_id: The ID of the pipeline to be deleted.

        Raises:
            DLTManagerError: If the deletion fails for reasons other than not existing.
        """
        try:
            logger.info("Deleting pipeline with ID: {} in {}", pipeline_id, self.workspace_name)
            self.workspace_client.pipelines.delete(pipeline_id=pipeline_id)
            logger.success("Successfully deleted pipeline with ID: {}", pipeline_id)
        except ResourceDoesNotExist:
            logger.info("Pipeline with ID {} not found. Deletion skipped.", pipeline_id)
        except Exception as e:
            error_msg = f"An error occurred while deleting DLT Pipeline {pipeline_id} in {self.workspace_name}."
            logger.error(
                "An error occurred while deleting DLT Pipeline {} in {}. Details: {}",
                pipeline_id,
                self.workspace_name,
                e,
            )
            raise DLTManagerError(error_msg) from e

    def list_pipelines_with_given_name(self, pipeline_name: str) -> List[PipelineStateInfo]:
        """
        Retrieves a list of DLT pipelines that match a specified name.

        Args:
            pipeline_name: The name of the pipeline(s) to search for

        Returns:
            A list of matching pipelines

        Raises:
            DLTManagerError: If the API call to list pipelines fails.
        """
        try:
            filter_str = f"name LIKE '{pipeline_name}'"
            pipelines_iterator: Iterable[PipelineStateInfo] = self.workspace_client.pipelines.list_pipelines(
                filter=filter_str
            )
            return list(pipelines_iterator)
        except Exception as e:
            error_msg = (
                f"An error occurred while getting the list of DLT Pipelines "
                f"named '{pipeline_name}' in '{self.workspace_name}'. "
                f"Please try again and if the error persists contact the platform team"
            )
            logger.error(error_msg)
            raise DLTManagerError(error_msg) from e

    def retrieve_pipeline_id_from_name(self, pipeline_name: str) -> str:
        """
        Retrieves the unique pipeline ID for a DLT pipeline with the given name.

        This method queries the Databricks workspace for pipelines matching the provided
        name. It ensures that exactly one pipeline is found to prevent ambiguity.

        Args:
            pipeline_name: The name of the pipeline whose ID needs to be retrieved.

        Returns:
            The unique pipeline ID as a string.

        Raises:
            DLTManagerError: If no DLT pipeline is found with the specified name,
            more than one DLT pipeline is found with the name or any other unexpected errors during the API call.
        """
        pipeline_list = self.list_pipelines_with_given_name(pipeline_name)

        if not pipeline_list:
            error_msg = (
                f"An error occurred while searching pipeline '{pipeline_name}' in {self.workspace_name}:"
                f" no DLT found with that name."
            )
            logger.error(error_msg)
            raise DLTManagerError(error_msg)

        if len(pipeline_list) > 1:
            error_msg = (
                f"An error occurred while searching pipeline '{pipeline_name}' in {self.workspace_name}: "
                "more than 1 DLT found with that name."
            )
            logger.error(error_msg)
            raise DLTManagerError(error_msg)

        pipeline_id = pipeline_list[0].pipeline_id
        if not pipeline_id:
            error_msg = (
                f"Error retrieving pipeline '{pipeline_name}' in {self.workspace_name}. "
                f"Received empty response from Databricks"
            )
            logger.error(error_msg)
            logger.debug("Response returned by Databricks for '{}': {}", pipeline_name, pipeline_list[0])
            raise DLTManagerError(error_msg)
        logger.info("Found unique pipeline '{}' with ID '{}'.", pipeline_name, pipeline_id)
        return pipeline_id

    def _create_dlt_pipeline(
        self,
        pipeline_name: str,
        product_edition: ProductEdition,
        continuous: bool,
        notebooks: List[str],
        files: List[str],
        catalog: str,
        target: str,
        photon: bool,
        notifications: Dict[str, list[str]],
        channel: PipelineChannel,
        cluster_specific: DLTClusterSpecific,
    ) -> str:
        logger.info("Creating pipeline {} in {}", pipeline_name, self.workspace_name)
        try:
            libraries = self._build_pipeline_libraries(notebooks, files)
            if not libraries:
                error_msg = "A DLT pipeline requires at least one notebook or file."
                logger.error(error_msg)
                raise DLTManagerError(error_msg)

            response = self.workspace_client.pipelines.create(
                name=pipeline_name,
                edition=product_edition,
                continuous=continuous,
                libraries=libraries,
                catalog=catalog,
                target=target,
                clusters=self._build_clusters(cluster_specific),
                photon=photon,
                channel=channel,
                notifications=self._build_notifications(notifications),
                configuration={var.name: var.value for var in cluster_specific.spark_env_vars}
                if cluster_specific.spark_env_vars
                else {},
            )
            if not response.pipeline_id:
                error_msg = (
                    f"Error retrieving pipeline '{pipeline_name}' in {self.workspace_name}. "
                    f"Received empty response from Databricks"
                )
                logger.error(error_msg)
                logger.debug("Response returned by Databricks for '{}': {}", pipeline_name, response.pipeline_id)
                raise DLTManagerError(error_msg)
            return response.pipeline_id
        except DLTManagerError:
            raise
        except ResourceConflict as e:
            error_msg = f"Error creating pipeline '{pipeline_name}'. A pipeline with this name may already exist."
            logger.error(
                "Error creating pipeline '{}'. A pipeline with this name may already exist. Details: {}",
                pipeline_name,
                e,
            )
            raise DLTManagerError(error_msg) from e
        except Exception as e:
            error_msg = f"An error occurred while creating DLT Pipeline {pipeline_name} in {self.workspace_name}."
            logger.error(
                "An error occurred while creating DLT Pipeline {} in {}. Details: {}",
                pipeline_name,
                self.workspace_name,
                e,
            )
            raise DLTManagerError(error_msg) from e

    def _update_dlt_pipeline(
        self,
        pipeline_id: str,
        pipeline_name: str,
        product_edition: ProductEdition,
        continuous: bool,
        notebooks: List[str],
        files: List[str],
        catalog: str,
        target: str,
        photon: bool,
        notifications: Dict[str, list[str]],
        channel: PipelineChannel,
        cluster_specific: DLTClusterSpecific,
    ) -> str:
        logger.info("Updating pipeline {} in {}", pipeline_name, self.workspace_name)
        try:
            libraries = self._build_pipeline_libraries(notebooks, files)
            if not libraries:
                error_msg = (
                    f"Pipeline {pipeline_name} doesn't contain neither notebooks or files to execute."
                    f"A DLT pipeline requires at least one notebook or file."
                )
                logger.error(error_msg)
                raise DLTManagerError(error_msg)

            self.workspace_client.pipelines.update(
                pipeline_id=pipeline_id,
                name=pipeline_name,
                edition=product_edition,
                continuous=continuous,
                libraries=libraries,
                catalog=catalog,
                target=target,
                clusters=self._build_clusters(cluster_specific),
                photon=photon,
                channel=channel,
                notifications=self._build_notifications(notifications),
                configuration={var.name: var.value for var in cluster_specific.spark_env_vars}
                if cluster_specific.spark_env_vars
                else {},
            )
            return pipeline_id
        except DLTManagerError:
            raise
        except Exception as e:
            error_msg = f"An error occurred while updating DLT Pipeline {pipeline_name} in {self.workspace_name}."
            logger.error(
                "An error occurred while updating DLT Pipeline {} in {}. Details: {}",
                pipeline_name,
                self.workspace_name,
                e,
            )
            raise DLTManagerError(error_msg) from e

    def _build_notifications(self, notifications: Dict[str, list[str]]) -> List[Notifications]:
        if not notifications:
            return []
        return [Notifications(email_recipients=[email], alerts=alerts) for email, alerts in notifications.items()]

    def _build_clusters(self, cluster_specific: DLTClusterSpecific) -> List[PipelineCluster]:
        spark_conf: dict[str, str] = (
            {c.name: c.value for c in cluster_specific.spark_conf} if cluster_specific.spark_conf else {}
        )

        cluster = PipelineCluster(
            policy_id=cluster_specific.policy_id,
            custom_tags=cluster_specific.tags,
            driver_node_type_id=cluster_specific.driver_type,
            node_type_id=cluster_specific.worker_type,
            spark_conf=spark_conf,
        )

        if cluster_specific.mode in (PipelineClusterAutoscaleMode.ENHANCED, PipelineClusterAutoscaleMode.LEGACY):
            cluster.autoscale = PipelineClusterAutoscale(
                min_workers=cluster_specific.min_workers,  # type:ignore[arg-type] # Already checked on validator
                max_workers=cluster_specific.max_workers,  # type:ignore[arg-type] # Already checked on validator
                mode=cluster_specific.mode,
            )
        else:
            cluster.num_workers = cluster_specific.num_workers

        return [cluster]

    def _build_pipeline_libraries(self, notebooks: List[str], files: List[str]) -> List[PipelineLibrary]:
        libraries = []
        if notebooks:
            libraries.extend([PipelineLibrary(notebook=NotebookLibrary(path=nb)) for nb in notebooks])
        if files:
            libraries.extend([PipelineLibrary(file=FileLibrary(path=f)) for f in files])
        return libraries
