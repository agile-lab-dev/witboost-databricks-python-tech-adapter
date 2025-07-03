from typing import Iterable, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineStateInfo
from loguru import logger

from src.models.databricks.exceptions import DLTManagerError


class DLTManager:
    """
    Manages operations on Databricks Delta Live Table (DLT) pipelines.

    TODO Implement all creation methods
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

    def list_pipelines_with_given_name(self, pipeline_name: str) -> List[PipelineStateInfo]:
        """
        Retrieves a list of DLT pipelines that match a specified name.

        Args:
            pipeline_name: The name of the pipeline(s) to search for

        Returns:
            List of matching pipelines
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
            PipelineNotFoundError: If no DLT pipeline is found with the specified name.
            MultiplePipelinesFoundError: If more than one DLT pipeline is found with the name.
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
