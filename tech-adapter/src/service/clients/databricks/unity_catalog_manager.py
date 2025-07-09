import time
from typing import Collection, List

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    PermissionsChange,
    Privilege,
    PrivilegeAssignment,
    SecurableType,
    TableInfo,
)
from loguru import logger

from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import UnityCatalogError
from src.models.databricks.object.db_objects import Catalog, DBObject, Schema, View


class UnityCatalogManager:
    """
    Manages entities within Databricks Unity Catalog, such as catalogs,
    schemas, tables, and permissions.
    """

    def __init__(self, workspace_client: WorkspaceClient, workspace_info: DatabricksWorkspaceInfo):
        self.workspace_client = workspace_client
        self.workspace_info = workspace_info

    def attach_metastore(self, metastore_name: str) -> None:
        """
        Attaches the current workspace to a specified metastore.

        Args:
            metastore_name: The name of the metastore to attach to.

        Raises:
            UnityCatalogError: If the metastore name is empty or if the attachment fails.
        """
        if not metastore_name or metastore_name.isspace():
            error_msg = (
                "Provided metastore name is empty. "
                "Please ensure it's present if you're managing the workspace via the Tech Adapter."
            )
            logger.error(error_msg)
            raise UnityCatalogError(error_msg)

        try:
            logger.info("Attaching workspace {} to metastore {}", self.workspace_info.name, metastore_name)
            metastore_id = self._get_metastore_id(metastore_name)
            self.workspace_client.metastores.assign(
                workspace_id=int(self.workspace_info.id),
                metastore_id=metastore_id,
                default_catalog_name=None,  # type:ignore[arg-type]
            )
            logger.info("Successfully attached workspace to metastore '{}'.", metastore_name)
        except Exception as e:
            error_msg = f"Error linking workspace {self.workspace_info.name} to metastore {metastore_name}"
            logger.error(error_msg)
            raise UnityCatalogError(error_msg) from e

    def create_catalog_if_not_exists(self, catalog_name: str) -> None:
        """
        Ensures a catalog with the specified name exists, creating it if necessary.

        Args:
            catalog_name: The name of the catalog.

        Raises:
            UnityCatalogError: If checking for or creating the catalog fails.
        """
        if not self.check_catalog_existence(catalog_name):
            logger.info("Catalog '{}' doesn't exist, creating.", catalog_name)
            self._create_catalog(catalog_name)
        else:
            logger.info("Catalog '{}' already exists, skipping creation.", catalog_name)

    def create_schema_if_not_exists(self, catalog_name: str, schema_name: str) -> None:
        """
        Ensures a schema exists within a catalog, creating it if necessary.

        Args:
            catalog_name: The name of the parent catalog.
            schema_name: The name of the schema.

        Raises:
            UnityCatalogError: If checking for or creating the schema fails.
        """
        if not self.check_schema_existence(catalog_name, schema_name):
            logger.info("Schema '{}' in catalog '{}' doesn't exist, creating.", schema_name, catalog_name)
            self._create_schema(catalog_name, schema_name)
        else:
            logger.info("Schema '{}' in catalog '{}' already exists, skipping creation.", schema_name, catalog_name)

    def get_table_info(self, catalog_name: str, schema_name: str, table_name: str) -> TableInfo:
        """
        Retrieves detailed information about a table.

        Args:
            catalog_name: The table's parent catalog name.
            schema_name: The table's parent schema name.
            table_name: The name of the table.

        Returns:
            A TableInfo object with details about the table.

        Raises:
            UnityCatalogError: If the table cannot be found or an API error occurs.
        """
        full_name = self._retrieve_table_full_name(catalog_name, schema_name, table_name)
        try:
            return self.workspace_client.tables.get(full_name)
        except Exception as e:
            error_msg = f"Failed to get info for table '{full_name}'"
            logger.error("Failed to get info for table '{}'. Details: {}", full_name, e)
            raise UnityCatalogError(error_msg) from e

    def check_table_existence(self, catalog_name: str, schema_name: str, table_name: str) -> bool:
        """
        Checks if a table or view exists in the Unity Catalog.

        Args:
            catalog_name: The table's parent catalog name.
            schema_name: The table's parent schema name.
            table_name: The name of the table or view.

        Returns:
            True if the table exists, False otherwise.

        Raises:
            UnityCatalogError: If the API call to check for existence fails.
        """
        full_name = self._retrieve_table_full_name(catalog_name, schema_name, table_name)
        try:
            response = self.workspace_client.tables.exists(full_name)
            return response.table_exists or False
        except Exception as e:
            error_msg = f"An error occurred while searching for table {full_name}."
            logger.error("An error occurred while searching for table {}. Details: {}", full_name, e)
            raise UnityCatalogError(error_msg)  # Not chaining as this exception has been seen to show Bearer tokens

    def drop_table_if_exists(self, catalog_name: str, schema_name: str, table_name: str) -> None:
        """
        Deletes a table from the Unity Catalog if it exists.

        Args:
            catalog_name: The table's parent catalog name.
            schema_name: The table's parent schema name.
            table_name: The name of the table to delete.

        Raises:
            UnityCatalogError: If checking for or deleting the table fails.
        """
        full_name = self._retrieve_table_full_name(catalog_name, schema_name, table_name)
        try:
            if not self.check_table_existence(catalog_name, schema_name, table_name):
                logger.info("Drop table skipped. Table '{}' does not exist.", full_name)
                return

            logger.info("Dropping table '{}'.", full_name)
            self.workspace_client.tables.delete(full_name)
            logger.info("Table '{}' correctly dropped.", full_name)
        except Exception as e:
            error_msg = f"An error occurred while dropping table '{full_name}'"
            logger.error("An error occurred while dropping table '{}'. Details: {}", full_name, e)
            raise UnityCatalogError(error_msg) from e

    def retrieve_table_columns_names(self, catalog_name: str, schema_name: str, table_name: str) -> List[str]:
        """
        Retrieves a list of column names for a given table.

        Args:
            catalog_name: The table's parent catalog name.
            schema_name: The table's parent schema name.
            table_name: The name of the table.

        Returns:
            A list of column name strings.

        Raises:
            UnityCatalogError: If the table info or columns cannot be retrieved.
        """
        full_name = self._retrieve_table_full_name(catalog_name, schema_name, table_name)
        logger.info("Retrieving columns for table '{}' in workspace {}", full_name, self.workspace_info.name)
        try:
            table_info = self.get_table_info(catalog_name, schema_name, table_name)
            if not table_info.columns:
                return []
            return [col.name for col in table_info.columns if col.name is not None]
        except Exception as e:
            error_msg = f"An error occurred while retrieving columns for table '{full_name}'"
            logger.error("An error occurred while retrieving columns for table '{}'. Details: {}", full_name, e)
            raise UnityCatalogError(error_msg) from e

    def assign_databricks_permission_to_table_or_view(self, principal: str, privilege: Privilege, view: View) -> None:
        """
        Assigns a specific privilege on a view, plus USE_CATALOG and USE_SCHEMA.

        Args:
            principal: The user, group, or service principal to grant permissions to.
            privilege: The main privilege to grant on the view (e.g., SELECT).
            view: The View object representing the target.

        Raises:
            UnityCatalogError: If any permission grant fails.
        """
        self.update_databricks_permissions(principal, privilege, is_grant_added=True, db_object=view)
        self.update_databricks_permissions(
            principal, Privilege.USE_CATALOG, is_grant_added=True, db_object=Catalog(view.catalog_name)
        )
        self.update_databricks_permissions(
            principal, Privilege.USE_SCHEMA, is_grant_added=True, db_object=Schema(view.catalog_name, view.schema_name)
        )

    def update_databricks_permissions(
        self, principal: str, privilege: Privilege, is_grant_added: bool, db_object: DBObject
    ) -> None:
        """
        Updates permissions (grants or revokes) for a principal on a securable object.

        Args:
            principal: The user, group, or service principal.
            privilege: The privilege to add or remove (e.g., SELECT, MODIFY).
            is_grant_added: True to grant the privilege, False to revoke it.
            db_object: The securable object (e.g., Catalog, Schema, View).

        Raises:
            UnityCatalogError: If the permission update fails.
        """
        object_full_name = db_object.fully_qualified_name
        securable_type = db_object.securable_type
        action = "Adding" if is_grant_added else "Removing"

        logger.info("{} permission {} on object '{}' for principal {}", action, privilege, object_full_name, principal)

        try:
            permission_change = PermissionsChange(principal=principal)
            if is_grant_added:
                permission_change.add = [privilege]
            else:
                permission_change.remove = [privilege]

            self.workspace_client.grants.update(
                securable_type=securable_type.value, full_name=object_full_name, changes=[permission_change]
            )

            action_past_tense = "added" if is_grant_added else "removed"
            logger.info(
                "Permission {} {} on object '{}' for principal {}",
                privilege,
                action_past_tense,
                object_full_name,
                principal,
            )
        except Exception as e:
            error_msg = (
                f"Error {action.lower()} permission {privilege} "
                f"on object '{object_full_name}' for principal {principal}"
            )
            logger.error(error_msg)
            raise UnityCatalogError(error_msg) from e

    def retrieve_databricks_permissions(
        self, securable_type: SecurableType, db_object: DBObject
    ) -> Collection[PrivilegeAssignment]:
        """
        Retrieves all permission assignments for a given securable object.

        Args:
            securable_type: The type of the object (e.g., SecurableType.TABLE).
            db_object: The securable object itself.

        Returns:
            A collection of PrivilegeAssignment objects, or an empty list if none exist.

        Raises:
            UnityCatalogError: If retrieving permissions fails.
        """
        full_name = db_object.fully_qualified_name
        try:
            current_privilege_assignments = self.workspace_client.grants.get(securable_type.value, full_name)
            return current_privilege_assignments.privilege_assignments or []
        except Exception as e:
            error_msg = (
                f"An error occurred while retrieving current permission on '{full_name}'. "
                f"Please try again and if the error persists contact the platform team."
            )
            logger.error(
                "An error occurred while retrieving current permission on '{}'. "
                "Please try again and if the error persists contact the platform team. Details: {}",
                full_name,
                e,
            )
            raise UnityCatalogError(error_msg) from e

    # --- Private Helper Methods ---

    def _get_metastore_id(self, metastore_name: str) -> str:
        metastores = self.workspace_client.metastores.list()
        for metastore in metastores:
            if metastore.name and metastore.metastore_id and metastore.name.lower() == metastore_name.lower():
                return metastore.metastore_id

        error_msg = (
            f"An error occurred while searching metastore '{metastore_name}' details. "
            f"Please try again and if the error persists contact the platform team. "
            f"Details: Metastore not found"
        )
        logger.error(error_msg)
        raise UnityCatalogError(error_msg)

    def _create_catalog(self, catalog_name: str) -> None:
        try:
            logger.info("Creating Unity Catalog '{}' in workspace {}", catalog_name, self.workspace_info.name)
            self.workspace_client.catalogs.create(name=catalog_name)
        except ResourceAlreadyExists:
            logger.warning("Catalog '{}' already created by another provisioning process.", catalog_name)
            time.sleep(3)  # Wait to allow for eventual consistency
            return
        except Exception as e:
            error_msg = f"An error occurred while creating Unity Catalog '{catalog_name}'"
            logger.error("An error occurred while creating Unity Catalog '{}'. Details: {}", catalog_name, e)
            raise UnityCatalogError([error_msg]) from e

    def check_catalog_existence(self, catalog_name: str) -> bool:
        try:
            catalogs_list = list(self.workspace_client.catalogs.list())
            return any(cat.name and cat.name.lower() == catalog_name.lower() for cat in catalogs_list)
        except Exception as e:
            error_msg = f"An error occurred trying to search for catalog {catalog_name}."
            logger.error("An error occurred trying to search for catalog {}. Details: {}", catalog_name, e)
            raise UnityCatalogError(error_msg) from e

    def _create_schema(self, catalog_name: str, schema_name: str) -> None:
        try:
            logger.info("Creating schema '{}' in catalog '{}'", schema_name, catalog_name)
            self.workspace_client.schemas.create(name=schema_name, catalog_name=catalog_name)
        except ResourceAlreadyExists:
            logger.warning(
                "Schema '{}' in catalog '{}' already created by another provisioning process.",
                schema_name,
                catalog_name,
            )
            time.sleep(3)  # Wait to allow for eventual consistency
            return
        except Exception as e:
            error_msg = "An error occurred while creating schema '{schema_name}' in catalog '{catalog_name}'"
            logger.error(
                "An error occurred while creating schema '{}' in catalog '{}'. Details: {}",
                schema_name,
                catalog_name,
                e,
            )
            raise UnityCatalogError(error_msg) from e

    def check_schema_existence(self, catalog_name: str, schema_name: str) -> bool:
        try:
            if not self.check_catalog_existence(catalog_name):
                error_msg = f"Cannot check for schema '{schema_name}' because catalog '{catalog_name}' does not exist."
                logger.error(error_msg)
                raise UnityCatalogError(error_msg)

            schemas_list = list(self.workspace_client.schemas.list(catalog_name=catalog_name))
            return any(s.name and s.name.lower() == schema_name.lower() for s in schemas_list)
        except UnityCatalogError:
            raise
        except Exception as e:
            error_msg = f"An error occurred trying to search schema '{schema_name}' in catalog '{catalog_name}'"
            logger.error(
                "An error occurred trying to search schema '{}' in catalog '{}'. Details: {}",
                schema_name,
                catalog_name,
                e,
            )
            raise UnityCatalogError(error_msg) from e

    def _retrieve_table_full_name(self, catalog_name: str, schema_name: str, table_name: str) -> str:
        return f"{catalog_name}.{schema_name}.{table_name}"
