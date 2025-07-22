import re
from typing import Any, Dict, List, Mapping

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnTypeName, TableConstraint, TableInfo
from loguru import logger

from src.models.api_models import ReverseProvisioningRequest
from src.models.data_product_descriptor import OpenMetadataColumn
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.reverse_provision.output_port_reverse_provisioning_params import (
    OutputPortReverseProvisioningParams,
)
from src.models.exceptions import ReverseProvisioningError
from src.service.clients.azure.azure_workspace_handler import AzureWorkspaceHandler
from src.service.clients.databricks.unity_catalog_manager import UnityCatalogManager

# Module-level constants
SCHEMA_AND_DETAILS = "SCHEMA_AND_DETAILS"
VIEW = "VIEW"


class OutputPortReverseProvisionHandler:
    """
    Handles the reverse provisioning process for Databricks Output Ports.
    """

    def __init__(self, workspace_handler: AzureWorkspaceHandler):
        """
        Initializes the handler.

        Args:
            workspace_handler: A handler to interact with Databricks workspaces.
        """
        self.workspace_handler = workspace_handler
        self._data_types_map = self._get_data_types_map()

    def reverse_provision(self, reverse_provisioning_request: ReverseProvisioningRequest) -> Dict[str, Any]:
        """
        Performs reverse provisioning for a Databricks Output Port (table or view).

        This method reads the metadata of an existing table or view from a Databricks
        workspace, including its schema, and generates a set of updates to be applied
        to a descriptor file.

        Args:
            reverse_provisioning_request: The request containing parameters for the
                                          reverse provisioning operation.

        Returns:
            A dictionary of updates with dot-separated keys representing the paths
            to be updated in the descriptor.
        """
        try:
            params = OutputPortReverseProvisioningParams.model_validate(reverse_provisioning_request.params)
            logger.info("Start Output Port reverse provisioning with parameters: {}", params)

            workspace_name = params.environment_specific_config.specific.workspace
            workspace_info = self.workspace_handler.get_workspace_info_by_name(workspace_name)
            if not workspace_info:
                error_msg = f"Validation failed. Workspace '{workspace_name}' not found."
                logger.error(error_msg)
                raise ReverseProvisioningError([error_msg])

            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)

            # 1. Validate the request against the state in Databricks
            self._validate_provision_request(workspace_client, workspace_info, params)

            # 2. Retrieve the schema and construct the column list
            table_full_name = f"{params.catalog_name}.{params.schema_name}.{params.table_name}"
            columns_list = self._retrieve_columns_list(workspace_client, table_full_name)

            # 3. Prepare the updates dictionary
            columns = [col.model_dump(by_alias=True) for col in columns_list]
            updates: dict[str, Any] = {
                "spec.mesh.dataContract.schema": columns,
                "witboost.parameters.schemaDefinition.schemaColumns": columns,
            }
            if params.reverse_provisioning_option.upper() == SCHEMA_AND_DETAILS:
                updates.update(
                    {
                        "spec.mesh.specific.catalogName": params.catalog_name,
                        "spec.mesh.specific.schemaName": params.schema_name,
                        "spec.mesh.specific.tableName": params.table_name,
                        "witboost.parameters.catalogName": params.catalog_name,
                        "witboost.parameters.schemaName": params.schema_name,
                        "witboost.parameters.tableName": params.table_name,
                    }
                )

            return updates
        except ReverseProvisioningError:
            raise
        except Exception as e:
            error_msg = (
                "An unexpected error occurred during reverse provisioning. "
                "Contact the platform team for more information"
            )
            logger.error("An unexpected error occurred during reverse provisioning. Details: {}", e)
            raise ReverseProvisioningError([error_msg])

    def _validate_provision_request(
        self,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
        params: OutputPortReverseProvisioningParams,
    ):
        """Validates that the target table exists and meets the requirements."""
        unity_catalog_manager = UnityCatalogManager(workspace_client, workspace_info)
        table_full_name = f"{params.catalog_name}.{params.schema_name}.{params.table_name}"

        if not unity_catalog_manager.check_table_existence(params.catalog_name, params.schema_name, params.table_name):
            error_msg = f"The table '{table_full_name}', provided in the Reverse Provisioning request, does not exist."
            logger.error(error_msg)
            raise ReverseProvisioningError([error_msg])

        logger.info("The table '{}', provided in the Reverse Provisioning request, exists.", table_full_name)

        if params.reverse_provisioning_option.upper() == SCHEMA_AND_DETAILS:
            table_info = workspace_client.tables.get(table_full_name)
            if table_info.table_type and table_info.table_type.value.upper() == VIEW:
                error_msg = (
                    "It's not possible to inherit Table Details from a VIEW, only the Schema. "
                    "Please try again choosing to inherit the Schema only."
                )
                logger.error(error_msg)
                raise ReverseProvisioningError([error_msg])

    def _retrieve_columns_list(
        self, workspace_client: WorkspaceClient, table_full_name: str
    ) -> List[OpenMetadataColumn]:
        """Retrieves and maps the column schema of a Databricks table."""
        try:
            table_info: TableInfo = workspace_client.tables.get(table_full_name)

            pk_constraints: list[TableConstraint] = table_info.table_constraints or []
            primary_key_cols = {
                col
                for c in pk_constraints
                if c.primary_key_constraint
                for col in c.primary_key_constraint.child_columns
            }
            if primary_key_cols:
                logger.info("Primary key cols of the table: '{}'", ", ".join(primary_key_cols))
            else:
                logger.info("No primary key cols in table '{}'", table_full_name)

            columns_list = []
            problems: list[str] = []
            for col in table_info.columns or []:
                try:
                    if col.type_name and col.name:
                        open_metadata_type = self._map_databricks_to_open_metadata(col.type_name.value)

                        column_data: dict[str, Any] = {
                            "name": col.name,
                            "dataType": open_metadata_type,
                            "description": col.comment or col.type_text,
                        }

                        if col.type_name == ColumnTypeName.DECIMAL and col.type_precision and col.type_scale:
                            column_data.update({"precision": col.type_precision, "scale": col.type_scale})
                        elif col.type_name == ColumnTypeName.STRING:
                            column_data["dataLength"] = 65535

                        if col.name in primary_key_cols:
                            column_data["constraint"] = "PRIMARY_KEY"
                        elif not col.nullable:
                            column_data["constraint"] = "NOT_NULL"

                        columns_list.append(OpenMetadataColumn.model_validate(column_data))
                    else:
                        error_msg = (
                            f"Error retrieving column name and/or type for '{col.name}' in {table_full_name}. "
                            f"Received empty response from Databricks"
                        )
                        logger.error(error_msg)
                        logger.debug("Response returned by Databricks for '{}': {}", col.name, col)
                        problems.append(error_msg)

                except Exception as e:
                    logger.exception("Error while mapping Databricks column to OpenMetadata column")
                    problems.append(str(e))

            if len(problems) > 0:
                logger.error("Error mapping columns to Databricks. Details: {}", ",".join(problems))
                raise ReverseProvisioningError(problems)

            logger.info("Column list correctly retrieved for table '{}'", table_full_name)
            return columns_list
        except Exception as e:
            error_msg = f"An error occurred while retrieving column list from Databricks table '{table_full_name}'"
            logger.error(
                "An error occurred while retrieving column list from Databricks table '{}'. Details: {}",
                table_full_name,
                e,
            )
            raise ReverseProvisioningError([error_msg]) from e

    def _get_data_types_map(self) -> Mapping[re.Pattern, str]:
        """Returns a compiled regex map for Databricks to OpenMetadata type conversion."""
        return {
            re.compile(r"^DECIMAL.*"): "DECIMAL",
            re.compile(r"^ARRAY.*"): "ARRAY",
            re.compile(r"^MAP.*"): "MAP",
            re.compile(r"^STRUCT.*"): "STRUCT",
            re.compile(r"^VARCHAR.*"): "VARCHAR",
            re.compile(r"^CHAR.*"): "CHAR",
            re.compile(r"^TIMESTAMP_NTZ$"): "TIMESTAMPZ",
            re.compile(r"^BIGINT$"): "BIGINT",
            re.compile(r"^BINARY$"): "BINARY",
            re.compile(r"^BOOLEAN$"): "BOOLEAN",
            re.compile(r"^DATE$"): "DATE",
            re.compile(r"^DOUBLE$"): "DOUBLE",
            re.compile(r"^FLOAT$"): "FLOAT",
            re.compile(r"^INT$"): "INT",
            re.compile(r"^INTERVAL$"): "INTERVAL",
            re.compile(r"^SMALLINT$"): "SMALLINT",
            re.compile(r"^STRING$"): "STRING",
            re.compile(r"^TIMESTAMP$"): "TIMESTAMP",
            re.compile(r"^TINYINT$"): "TINYINT",
            re.compile(r"^VARIANT$"): "VARIANT",
            re.compile(r"^LONG$"): "LONG",
        }

    def _map_databricks_to_open_metadata(self, databricks_type: str) -> str:
        """Maps a Databricks data type string to its OpenMetadata equivalent."""
        for pattern, open_metadata_type in self._data_types_map.items():
            if pattern.match(databricks_type):
                logger.info(
                    "Mapped '{}' Databricks data type into '{}' Open Metadata data type",
                    databricks_type,
                    open_metadata_type,
                )
                return open_metadata_type

        error_msg = f"Not able to convert data type '{databricks_type}' to Open Metadata"
        logger.error(error_msg)
        raise ReverseProvisioningError([error_msg])
