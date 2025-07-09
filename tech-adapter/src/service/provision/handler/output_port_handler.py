from typing import Dict, Mapping, Set, Tuple, Union

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.catalog import Privilege, TableInfo
from loguru import logger

from src import settings
from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import DatabricksOutputPort
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import DatabricksMapperError
from src.models.databricks.object.db_objects import View
from src.models.exceptions import ProvisioningError, build_error_message_from_chained_exception
from src.service.clients.databricks.statement_execution_manager import StatementExecutionManager
from src.service.clients.databricks.unity_catalog_manager import UnityCatalogManager
from src.service.clients.databricks.workspace_manager import WorkspaceManager
from src.service.principals_mapping.databricks_mapper import DatabricksMapper


class OutputPortHandler:
    """
    Handles provisioning and management of Databricks Views as Output Ports.
    """

    def __init__(
        self,
        account_client: AccountClient,
    ):
        self.account_client = account_client

    def provision_output_port(
        self,
        data_product: DataProduct,
        component: DatabricksOutputPort,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> TableInfo:
        """
        Provisions a Databricks View as an Output Port.

        This method orchestrates the creation of a view, including attaching the metastore,
        creating the catalog and schema, creating the view itself, and setting permissions
        and descriptions for the view and its columns.

        Args:
            data_product: Data product containing the data product owner and development group information
            component: Databricks Output Port containing the target view information
            workspace_client: The client for the target Databricks workspace.
            workspace_info: Metadata about the Databricks workspace.

        Returns:
            A TableInfo object for the newly created view.

        Raises:
            ProvisioningError: If any step in the provisioning process fails.
        """
        component_name = component.name
        try:
            specific = component.specific
            unity_catalog_manager = UnityCatalogManager(workspace_client, workspace_info)
            workspace_manager = WorkspaceManager(workspace_client, self.account_client)

            # 1. Ensure catalog and schema exist
            unity_catalog_manager.create_catalog_if_not_exists(specific.catalog_name_op)
            unity_catalog_manager.create_schema_if_not_exists(specific.catalog_name_op, specific.schema_name_op)

            # 2. Get SQL Warehouse ID
            sql_warehouse_id = workspace_manager.get_sql_warehouse_id_from_name(specific.sql_warehouse_name)

            # 3. Create the view
            statement_execution_manager = StatementExecutionManager()
            statement_id = statement_execution_manager.execute_statement_create_or_replace_view(
                specific=specific,
                schema=component.dataContract.schema_ or [],
                sql_warehouse_id=sql_warehouse_id,
                workspace_client=workspace_client,
            )
            statement_execution_manager.poll_on_statement_execution(workspace_client, statement_id)

            view_full_name = f"`{specific.catalog_name_op}`.`{specific.schema_name_op}`.`{specific.view_name_op}`"
            logger.info("Output Port '{}' is now available. Setting permissions.", view_full_name)

            # 4. Set permissions
            self._set_initial_permissions(data_product, component, unity_catalog_manager)

            # 5. Add column and view descriptions
            statement_execution_manager.add_all_descriptions(
                component=component,
                sql_warehouse_id=sql_warehouse_id,
                workspace_client=workspace_client,
            )

            # 6. Return final info
            return unity_catalog_manager.get_table_info(
                specific.catalog_name_op, specific.schema_name_op, specific.view_name_op
            )

        except Exception as e:
            error_msg = f"An error occurred while provisioning component {component_name}."
            logger.error("An error occurred while provisioning component {}. Details: {}", component_name, e)
            raise ProvisioningError([error_msg]) from e

    def unprovision_output_port(
        self,
        data_product: DataProduct,
        component: DatabricksOutputPort,
        workspace_client: WorkspaceClient,
        workspace_info: DatabricksWorkspaceInfo,
    ) -> None:
        """
        Unprovisions a Databricks Output Port by dropping the view.

        Args:
            data_product: Data product containing the data product owner and development group information
            component: Databricks Output Port containing the target view information
            workspace_client: The client for the target Databricks workspace.
            workspace_info: Metadata about the Databricks workspace.

        Raises:
            ProvisioningError: If any step in the unprovisioning process fails.
        """
        specific = component.specific
        view_full_name = f"{specific.catalog_name_op}.{specific.schema_name_op}.{specific.view_name_op}"

        try:
            unity_catalog_manager = UnityCatalogManager(workspace_client, workspace_info)
            if not unity_catalog_manager.check_catalog_existence(specific.catalog_name_op):
                logger.info(
                    "Unprovision of '{}' skipped. Catalog '{}' not found.",
                    view_full_name,
                    specific.catalog_name_op,
                )
                return

            if not unity_catalog_manager.check_schema_existence(specific.catalog_name_op, specific.schema_name_op):
                logger.info(
                    "Unprovision of '{}' skipped. Schema '{}' in catalog '{}' not found.",
                    view_full_name,
                    specific.schema_name_op,
                    specific.catalog_name_op,
                )
                return

            unity_catalog_manager.drop_table_if_exists(
                specific.catalog_name_op, specific.schema_name_op, specific.view_name_op
            )
            logger.info("Unprovision of '{}' terminated correctly.", view_full_name)

        except Exception as e:
            error_msg = f"An error occurred while unprovisioning '{view_full_name}'"
            logger.error("An error occurred while unprovisioning '{}'. Details: {}", view_full_name, e)
            raise ProvisioningError([error_msg]) from e

    def update_acl(
        self,
        data_product: DataProduct,
        component: DatabricksOutputPort,
        witboost_identities: list[str],
        unity_catalog_manager: UnityCatalogManager,
    ) -> None:
        """
        Updates the Access Control List (ACL) for a Databricks Output Port.

        This method synchronizes the permissions on a view with the provided list of
        references (principals), granting SELECT to new ones and revoking it from old ones.

        Args:
            data_product: Data product containing the data product owner and development group information
            component: Databricks Output Port containing the target view information
            witboost_identities: List of witboost users and groups to be granted access to target view
            unity_catalog_manager: The manager for Unity Catalog operations.

        Raises:
            ProvisioningError: If any step of the ACL update fails.
        """
        specific = component.specific
        view_full_name = f"{specific.catalog_name_op}.{specific.schema_name_op}.{specific.view_name_op}"
        logger.info("Start updating Access Control List for {}", view_full_name)

        try:
            dp_owner_mapped, dev_group_mapped = self.map_principals(data_product, component)
            # 1. Map all principals involved
            databricks_mapper = DatabricksMapper(self.account_client)
            mapped_principals = databricks_mapper.map(set(witboost_identities))

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
                raise ProvisioningError(errors=[build_error_message_from_chained_exception(error) for error in errors])

            logger.info("Retrieving current permissions on Output Port")
            view_op = View(specific.catalog_name_op, specific.schema_name_op, specific.view_name_op)
            current_permissions = unity_catalog_manager.retrieve_databricks_permissions(view_op.securable_type, view_op)

            # 2. Remove grants for principals no longer in the ACL
            revoking_errors: list[str] = []
            for permission in current_permissions:
                principal = permission.principal
                if not principal:
                    error_msg = (
                        f"Error updating Access Control for view '{view_full_name}'. "
                        f"Received empty response from Databricks"
                    )
                    logger.error(error_msg)
                    logger.debug("Response returned by Databricks for '{}': {}", view_full_name, permission)
                    raise ProvisioningError([error_msg])
                # In dev, don't remove owner/dev group permissions
                if data_product.environment.lower() == settings.misc.development_environment_name.lower() and (
                    principal in [dp_owner_mapped, dev_group_mapped]
                ):
                    logger.info(
                        "Environment is {} and so, privileges of {} (Data Product Owner or "
                        "Development Group) are not removed",
                        data_product.environment,
                        principal,
                    )
                elif principal not in successful_mappings.values():
                    logger.info("Principal {} no longer has SELECT permission. Revoking.", principal)
                    try:
                        unity_catalog_manager.update_databricks_permissions(
                            principal, Privilege.SELECT, is_grant_added=False, db_object=view_op
                        )
                        logger.info(
                            "SELECT permission removed from {} for principal {} successfully",
                            view_op.fully_qualified_name,
                            principal,
                        )
                    except Exception as e:
                        revoking_errors.append(build_error_message_from_chained_exception(e))

            if revoking_errors:
                logger.error(
                    "An error occurred while removing permissions on Databricks entities. "
                    "Please try again and if the problem persists contact the platform team"
                )
                for error in revoking_errors:
                    logger.error("  - Error: {}", error)
                raise ProvisioningError(revoking_errors)

            granting_errors: list[str] = []
            # 3. Add grants for all principals in the new ACL
            for databricks_id in successful_mappings.values():
                logger.info("Assigning SELECT permission to Databricks entity: {}", databricks_id)
                try:
                    unity_catalog_manager.assign_databricks_permission_to_table_or_view(
                        databricks_id, Privilege.SELECT, view_op
                    )
                    logger.info(
                        "SELECT permission on '{}' added for Databricks identity '{}' successfully",
                        view_op.fully_qualified_name,
                        databricks_id,
                    )
                except Exception as e:
                    granting_errors.append(build_error_message_from_chained_exception(e))

            if granting_errors:
                logger.error(
                    "An error occurred while granting permissions on Databricks entities. "
                    "Please try again and if the problem persists contact the platform team"
                )
                for error in granting_errors:
                    logger.error("  - Error: {}", error)
                raise ProvisioningError(granting_errors)

        except Exception as e:
            error_msg = f"An error occurred while updating ACL for '{view_full_name}'"
            logger.error("An error occurred while updating ACL for '{}'. Details: {}", view_full_name, e)
            raise ProvisioningError([error_msg]) from e

    # --- Private Helper Methods ---
    def map_principals(self, data_product: DataProduct, component: DatabricksOutputPort) -> Tuple[str, str]:
        """
        Maps principals from a provision request to Databricks-recognized formats.

        This method takes the data product owner and development group from the
        request, ensures they are correctly formatted, and uses the DatabricksMapper
        to resolve them to their canonical names (e.g., case-sensitive group names).

        Args:
            data_product: An object containing data product information,
                          like `data_product_owner` and `dev_group` fields.
            component: An object containing the output port component information

        Returns:
            A tuple {mappedDpOwner, mappedDevGroup} with the mapped identities of the data product owner,
            and development group

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

            owner_id = successful_mappings.get(data_product.dataProductOwner)
            dev_group_id = successful_mappings.get(dev_group)

            if not owner_id or not dev_group_id:
                error_msg = "Error while mapping principals. Failed to retrieve outcome of mapping"
                logger.error(error_msg)
                raise ProvisioningError([error_msg])

            logger.info("Successfully mapped all principals.")
            return owner_id, dev_group_id

        except Exception as e:
            # Catch any other unexpected exception and wrap it
            error_msg = (
                "An unexpected error occurred while mapping principals for component "
                f"'{component.name}'. Details: {e}"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg]) from e

    def _set_initial_permissions(
        self, data_product: DataProduct, component: DatabricksOutputPort, unity_catalog_manager: UnityCatalogManager
    ):
        """Sets owner and developer permissions in development environment."""
        if data_product.environment.lower() != settings.misc.development_environment_name.lower():
            logger.info(
                "Skipping permission assignment for component '{}' as environment "
                "is not the configured development environment"
            )
            return

        view = View(
            component.specific.catalog_name_op, component.specific.schema_name_op, component.specific.view_name_op
        )
        owner_id, dev_group_id = self.map_principals(data_product, component)

        # Assign Owner permissions
        owner_privilege = Privilege(settings.databricks.permissions.output_port.owner)
        unity_catalog_manager.assign_databricks_permission_to_table_or_view(owner_id, owner_privilege, view)

        # Assign Developer permissions
        dev_privilege = Privilege(settings.databricks.permissions.output_port.developer)
        unity_catalog_manager.assign_databricks_permission_to_table_or_view(dev_group_id, dev_privilege, view)
