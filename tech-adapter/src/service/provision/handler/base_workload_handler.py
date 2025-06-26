from typing import Any, Dict, Mapping, Set, Union

from databricks.sdk import AccountClient, WorkspaceClient
from loguru import logger

from src.models.data_product_descriptor import DataProduct, Workload
from src.models.databricks.exceptions import DatabricksMapperError
from src.models.exceptions import ProvisioningError
from src.service.principals_mapping.databricks_mapper import DatabricksMapper


class BaseWorkloadHandler:
    """
    Handles high-level provisioning logic for Databricks workloads.

    This class orchestrates interactions with Databricks, including repository
    creation, permission management, and principal mapping.
    """

    def __init__(self, account_client: AccountClient):
        """
        Initializes the handler with necessary clients and configurations.

        Args:
            account_client: An authenticated Databricks SDK AccountClient.
        """
        self.account_client = account_client

    def create_repository_with_permissions(
        self,
        provision_request: Any,
        workspace_client: WorkspaceClient,
        databricks_workspace_info: Any,
        owner_name: str,
        developer_group_name: str,
    ) -> None:
        """
        Creates a repository in Databricks and assigns permissions.
        TODO method
        """
        pass

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
            logger.exception(error_msg)
            raise ProvisioningError([error_msg]) from e

    def get_service_principal_workspace_client(
        self,
        workspace_url: str,
        sp_client_id: str,
        sp_client_secret: str,
        workspace_name: str,
    ) -> WorkspaceClient:
        """
        Creates and returns a WorkspaceClient using service principal credentials.
        TODO method
        """
        raise NotImplementedError()

    def set_service_principal_git_credentials(
        self,
        workspace_client: WorkspaceClient,
        workspace_host: str,
        workspace_name: str,
        principal_name: str,
    ) -> None:
        """
        Sets Git credentials for a service principal in a Databricks workspace.
        TODO method
        """
        pass
