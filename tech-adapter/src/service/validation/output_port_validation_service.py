from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound
from loguru import logger

from src import settings
from src.models.data_product_descriptor import OpenMetadataColumn, OutputPort
from src.models.databricks.databricks_models import DatabricksOutputPort
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.exceptions import ProvisioningError
from src.service.clients.azure.azure_workspace_handler import AzureWorkspaceHandler
from src.service.clients.databricks.unity_catalog_manager import UnityCatalogManager


class OutputPortValidation:
    """
    A service dedicated to validating provisioning requests for Databricks Output Ports.
    """

    def __init__(self, workspace_handler: AzureWorkspaceHandler):
        """
        Initializes the validation service.

        Args:
            workspace_handler: A handler to interact with Databricks workspaces.
        """
        self.workspace_handler = workspace_handler

    def validate(
        self,
        component: DatabricksOutputPort,
        environment: str,
    ) -> None:
        """
        Validates the entire provisioning request for an Output Port.

        This is the main entry point for validation. It checks for the existence
        of the workspace and metastore before delegating to more specific checks
        for the source table and schema compatibility.

        Args:
            component: The Output Port component to be validated.
            environment: The name of the deployment environment (e.g., 'development').

        Raises:
            ProvisioningError: If any validation check fails.
        """
        logger.info("Started validation of Output Port {} (id: {}).", component.name, component.id)

        workspace_name = component.specific.workspace

        try:
            workspace_info = self.workspace_handler.get_workspace_info_by_name(workspace_name)

            # If workspace doesn't exist, we can't perform deeper checks. Validation succeeds with a warning.
            if not workspace_info:
                logger.warning(
                    "Validation of Output Port {} (id: {}) completed. "
                    "ATTENTION: couldn't check source table as workspace doesn't exist yet.",
                    component.name,
                    component.id,
                )
                return

            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)

            # Check metastore existence, if metastore is not found, we skip the rest of the validation
            # and return an OK status. Only if the metastore exists we do further checks
            try:
                workspace_client.metastores.current()
            except NotFound:
                if workspace_info.is_managed:
                    # This is considered a successful validation with a warning, so we return
                    logger.warning(
                        "Validation of Output Port {} (id: {}) completed. "
                        "ATTENTION: couldn't check source table as metastore is not attached yet to the workspace",
                        component.name,
                        component.id,
                    )
                    return
                else:
                    error_msg = (
                        f"Validation of Output Port {component.name} (id: {component.id}) failed. "
                        f"No metastore assigned for the current workspace"
                    )
                    logger.error(error_msg)
                    raise ProvisioningError([error_msg])
            except Exception as e:
                error_msg = (
                    f"An unexpected error occurred while retrieving current metastore for workspace '{workspace_name}'."
                )
                logger.error(
                    "An unexpected error occurred while retrieving current metastore for workspace '{}'. Details: {}",
                    workspace_name,
                    e,
                )
                raise ProvisioningError([error_msg]) from e

            # Perform detailed validation of the table and schema.
            self.validate_table_existence_and_schema(workspace_client, component, environment, workspace_info)

            logger.success(
                "Validation of Output Port {} (id: {}) completed successfully.", component.name, component.id
            )

        except ProvisioningError:
            # Re-raise our specific validation errors directly
            raise
        except Exception as e:
            # Catch any other unexpected exception and wrap it
            error_msg = f"An unexpected error occurred during validation for component '{component.name}'"
            logger.error(
                "An unexpected error occurred during validation for component '{}. Details: {}'", component.name, e
            )
            raise ProvisioningError([error_msg]) from e

    def validate_table_existence_and_schema(
        self,
        workspace_client: WorkspaceClient,
        component: DatabricksOutputPort,
        environment: str,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> None:
        """
        Validates if the source table exists and if the view's schema is a valid subset.
        """
        unity_catalog_manager = UnityCatalogManager(workspace_client, workspace_info)
        specific = component.specific

        logger.info(
            "Checking if the table provided in Output Port {} already exists. "
            "Details: Catalog: {}, Schema: {}, Table: {}",
            component.name,
            specific.catalog_name,
            specific.schema_name,
            specific.table_name,
        )

        table_full_name = f"{specific.catalog_name}.{specific.schema_name}.{specific.table_name}"

        # 1. Validate that the source table exists
        if not unity_catalog_manager.check_table_existence(
            specific.catalog_name, specific.schema_name, specific.table_name
        ):
            if environment.lower() == settings.misc.development_environment_name.lower():
                hint = (
                    "Be sure that the table exists by either running the "
                    "DLT Workload that creates it or creating the table manually."
                )
            else:
                hint = (
                    "Be sure that the DLT Workload that creates it is being "
                    "deployed correctly and that the table name is correct."
                )

            error_msg = (
                f"The table '{table_full_name}', provided in Output Port {component.name}, " f"does not exist. {hint}"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])

        logger.info("The table '{}', provided in Output Port {}, exists.", table_full_name, component.name)

        # 2. Validate the view schema against the table's schema
        logger.info(
            "Checking if the schema in Output Port {} is a subset of the schema of table '{}'.",
            component.name,
            table_full_name,
        )
        table_column_names = unity_catalog_manager.retrieve_table_columns_names(
            specific.catalog_name, specific.schema_name, specific.table_name
        )
        self._check_view_schema(component, table_column_names, table_full_name)

    def _check_view_schema(
        self,
        component: OutputPort,
        table_column_names: list[str],
        table_full_name: str,
    ) -> None:
        """
        Verifies that every column in the proposed view schema exists in the source table.
        """
        view_columns: list[OpenMetadataColumn] = component.dataContract.schema_ or []
        table_columns_set = set(table_column_names)

        errors: list[str] = []
        for view_column in view_columns:
            if view_column.name not in table_columns_set:
                error_msg = (
                    f"Check for Output Port {component.name}: the column '{view_column.name}' "
                    f"cannot be found in the table '{table_full_name}'"
                )
                logger.error(error_msg)
                errors.append(error_msg)
        if len(errors) > 0:
            raise ProvisioningError(errors)
