import unittest
from unittest.mock import MagicMock, call, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    MetastoreInfo,
    PermissionsChange,
    Privilege,
    SecurableType,
    TableExistsResponse,
    TableInfo,
)

from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.object.db_objects import Catalog, Schema, View
from src.service.clients.databricks.unity_catalog_manager import UnityCatalogManager


class TestUnityCatalogManager(unittest.TestCase):
    """
    Unit tests for the UnityCatalogManager class.

    This suite tests the class by only mocking the external `workspace_client` dependency.
    It does not patch internal methods, ensuring that the interactions between methods
    are tested correctly.
    """

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_workspace_client = MagicMock()
        """Set up the test environment before each test."""
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )
        self.manager = UnityCatalogManager(self.mock_workspace_client, self.workspace_info)

        # Common test data
        self.catalog_name = "test_catalog"
        self.schema_name = "test_schema"
        self.table_name = "test_table"
        self.metastore_name = "test_metastore"
        self.metastore_id = "meta-id-123"
        self.principal = "test-principal"
        self.table_full_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
        self.view = View(self.catalog_name, self.schema_name, self.table_name)

    def test_attach_metastore_success(self):
        """Test successful attachment of a workspace to a metastore."""
        # Arrange: Configure the client to find the metastore by name.
        self.mock_workspace_client.metastores.list.return_value = [
            MetastoreInfo(name=self.metastore_name, metastore_id=self.metastore_id)
        ]

        # Act
        self.manager.attach_metastore(self.metastore_name)

        # Assert: Verify the correct SDK methods were called with the right data.
        self.mock_workspace_client.metastores.list.assert_called_once()
        self.mock_workspace_client.metastores.assign.assert_called_once_with(
            workspace_id=int(self.workspace_info.id), metastore_id=self.metastore_id, default_catalog_name=None
        )

    def test_create_catalog_if_not_exists_creates_when_missing(self):
        """Test that a catalog is created when it does not exist."""
        # Arrange: Configure the client to report the catalog does not exist.
        self.mock_workspace_client.catalogs.list.return_value = []

        # Act
        self.manager.create_catalog_if_not_exists(self.catalog_name)

        # Assert: Verify the check and the create call were made.
        self.mock_workspace_client.catalogs.list.assert_called_once()
        self.mock_workspace_client.catalogs.create.assert_called_once_with(name=self.catalog_name)

    def test_create_catalog_if_not_exists_skips_when_present(self):
        """Test that catalog creation is skipped when it already exists."""
        # Arrange: Configure the client to report the catalog already exists.
        self.mock_workspace_client.catalogs.list.return_value = [CatalogInfo(name=self.catalog_name)]

        # Act
        self.manager.create_catalog_if_not_exists(self.catalog_name)

        # Assert: Verify the check was made but the create call was not.
        self.mock_workspace_client.catalogs.list.assert_called_once()
        self.mock_workspace_client.catalogs.create.assert_not_called()

    @patch("time.sleep")
    def test_create_catalog_if_not_exists_handles_race_condition(self, mock_sleep):
        """Test that a race condition (ResourceAlreadyExists) is handled gracefully."""
        # Arrange: The catalog doesn't exist on check, but exists on create.
        self.mock_workspace_client.catalogs.list.return_value = []
        self.mock_workspace_client.catalogs.create.side_effect = ResourceAlreadyExists("exists")

        # Act
        self.manager.create_catalog_if_not_exists(self.catalog_name)

        # Assert: No exception is raised and a sleep is triggered.
        self.mock_workspace_client.catalogs.create.assert_called_once()
        mock_sleep.assert_called_once()

    def test_create_schema_if_not_exists_creates_when_missing(self):
        """Test that a schema is created when it does not exist."""
        # Arrange: Catalog exists, but schema does not.
        self.mock_workspace_client.catalogs.list.return_value = [CatalogInfo(name=self.catalog_name)]
        self.mock_workspace_client.schemas.list.return_value = []

        # Act
        self.manager.create_schema_if_not_exists(self.catalog_name, self.schema_name)

        # Assert
        self.mock_workspace_client.catalogs.list.assert_called_once()
        self.mock_workspace_client.schemas.list.assert_called_once_with(catalog_name=self.catalog_name)
        self.mock_workspace_client.schemas.create.assert_called_once_with(
            name=self.schema_name, catalog_name=self.catalog_name
        )

    def test_drop_table_if_exists_deletes(self):
        """Test that a table is dropped when it exists."""
        # Arrange: Configure the client to report the table exists.
        self.mock_workspace_client.tables.exists.return_value = TableExistsResponse(table_exists=True)

        # Act
        self.manager.drop_table_if_exists(self.catalog_name, self.schema_name, self.table_name)

        # Assert
        self.mock_workspace_client.tables.exists.assert_called_once_with(self.table_full_name)
        self.mock_workspace_client.tables.delete.assert_called_once_with(self.table_full_name)

    def test_drop_table_if_exists_skips(self):
        """Test that table deletion is skipped when the table does not exist."""
        # Arrange: Configure the client to report the table does not exist.
        self.mock_workspace_client.tables.exists.return_value = TableExistsResponse(table_exists=False)

        # Act
        self.manager.drop_table_if_exists(self.catalog_name, self.schema_name, self.table_name)

        # Assert
        self.mock_workspace_client.tables.exists.assert_called_once_with(self.table_full_name)
        self.mock_workspace_client.tables.delete.assert_not_called()

    def test_retrieve_table_columns_names_success(self):
        """Test successful retrieval of table column names."""
        # Arrange
        columns = [ColumnInfo(name="id"), ColumnInfo(name="value", type_text="STRING")]
        self.mock_workspace_client.tables.get.return_value = TableInfo(columns=columns)

        # Act
        column_names = self.manager.retrieve_table_columns_names(self.catalog_name, self.schema_name, self.table_name)

        # Assert
        self.assertEqual(column_names, ["id", "value"])
        self.mock_workspace_client.tables.get.assert_called_once_with(self.table_full_name)

    def test_assign_databricks_permission_to_table_or_view(self):
        """Test the orchestration of assigning hierarchical permissions to a view."""
        # Act
        self.manager.assign_databricks_permission_to_table_or_view(self.principal, Privilege.SELECT, self.view)

        # Assert: Verify that grants.update was called three times with the correct args.
        expected_changes_view = PermissionsChange(principal=self.principal, add=[Privilege.SELECT])
        expected_changes_catalog = PermissionsChange(principal=self.principal, add=[Privilege.USE_CATALOG])
        expected_changes_schema = PermissionsChange(principal=self.principal, add=[Privilege.USE_SCHEMA])

        expected_calls = [
            call(
                securable_type=SecurableType.TABLE.value,
                full_name=self.view.fully_qualified_name,
                changes=[expected_changes_view],
            ),
            call(
                securable_type=SecurableType.CATALOG.value,
                full_name=self.catalog_name,
                changes=[expected_changes_catalog],
            ),
            call(
                securable_type=SecurableType.SCHEMA.value,
                full_name=f"{self.catalog_name}.{self.schema_name}",
                changes=[expected_changes_schema],
            ),
        ]
        self.mock_workspace_client.grants.update.assert_has_calls(expected_calls, any_order=False)

    def test_update_databricks_permissions_grant(self):
        """Test granting a single privilege directly."""
        # Arrange
        db_object = Catalog(self.catalog_name)

        # Act
        self.manager.update_databricks_permissions(self.principal, Privilege.CREATE_SCHEMA, True, db_object)

        # Assert
        expected_change = PermissionsChange(principal=self.principal, add=[Privilege.CREATE_SCHEMA])
        self.mock_workspace_client.grants.update.assert_called_once_with(
            securable_type=db_object.securable_type.value,
            full_name=db_object.fully_qualified_name,
            changes=[expected_change],
        )

    def test_update_databricks_permissions_revoke(self):
        """Test revoking a single privilege directly."""
        # Arrange
        db_object = Schema(self.catalog_name, self.schema_name)

        # Act
        self.manager.update_databricks_permissions(self.principal, Privilege.CREATE_TABLE, False, db_object)

        # Assert
        expected_change = PermissionsChange(principal=self.principal, remove=[Privilege.CREATE_TABLE])
        self.mock_workspace_client.grants.update.assert_called_once_with(
            securable_type=db_object.securable_type.value,
            full_name=db_object.fully_qualified_name,
            changes=[expected_change],
        )
