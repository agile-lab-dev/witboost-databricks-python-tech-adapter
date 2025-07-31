import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.catalog import Privilege, PrivilegeAssignment, TableInfo
from loguru import logger

from src.models.data_product_descriptor import DataContract, DataProduct
from src.models.databricks.databricks_models import DatabricksOutputPort
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import MapperError
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.models.exceptions import ProvisioningError
from src.service.provision.handler.output_port_handler import OutputPortHandler


class TestOutputPortHandler(unittest.TestCase):
    """Unit tests for the OutputPortHandler class."""

    def setUp(self):
        """Set up the test environment with minimal dependencies."""
        self.mock_account_client = MagicMock()
        self.mock_workspace_client = MagicMock()

        # The handler under test
        self.handler = OutputPortHandler(self.mock_account_client)

        # --- Common Test Data ---
        self.owner_principal = "user:owner_test.com"
        self.dev_group_principal = "group:dev-group"
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner=self.owner_principal,
            devGroup=self.dev_group_principal,
            name="dp-name",
            description="description",
            kind="dataproduct",
            domain="domain:domain",
            version="0.0.0",
            environment="development",
            ownerGroup="group:dev-group",
            specific={},
            components=[],
            tags=[],
        )

        self.output_port_specific = DatabricksOutputPortSpecific(
            catalog_name="input_catalog",
            schema_name="input_schema",
            table_name="input_table",
            sql_warehouse_name="test-wh",
            catalog_name_op="output_catalog",
            schema_name_op="output_schema",
            view_name_op="output_view",
            workspace="test-workspace",
            workspace_op="test-workspace-op",
        )
        self.output_port_component = DatabricksOutputPort(
            id="comp-id",
            name="test-output-port",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            tags=[],
            specific=self.output_port_specific,
            kind="outputport",
            outputPortType="SQL",
            semanticLinking=[],
            dataContract=DataContract(),
        )
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace-op",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    @patch("src.service.provision.handler.output_port_handler.DatabricksMapper")
    @patch("src.service.provision.handler.output_port_handler.StatementExecutionManager")
    @patch("src.service.provision.handler.output_port_handler.WorkspaceManager")
    @patch("src.service.provision.handler.output_port_handler.UnityCatalogManager")
    @patch("src.service.provision.handler.output_port_handler.settings")
    def test_provision_output_port_success(
        self, mock_settings, MockUnityCatalogManager, MockWorkspaceManager, MockStatementManager, MockDatabricksMapper
    ):
        """Test the successful provisioning flow for an output port."""
        # Arrange
        mock_settings.misc.development_environment_name = "development"
        mock_settings.databricks.permissions.output_port.owner = "SELECT"
        mock_settings.databricks.permissions.output_port.developer = "SELECT"

        mock_uc_manager = MockUnityCatalogManager.return_value
        mock_ws_manager = MockWorkspaceManager.return_value
        mock_statement_manager = MockStatementManager.return_value
        mock_mapper = MockDatabricksMapper.return_value

        mock_ws_manager.get_sql_warehouse_id_from_name.return_value = "wh-id-123"
        mock_statement_manager.execute_statement_create_or_replace_view.return_value = "stmt-id-456"
        mock_mapper.map.return_value = {
            self.owner_principal: "mapped-owner",
            self.dev_group_principal: "mapped-dev-group",
        }
        table_info = TableInfo(name="created-table")
        mock_uc_manager.get_table_info.return_value = table_info

        # Act
        result_info = self.handler.provision_output_port(
            self.data_product, self.output_port_component, self.mock_workspace_client, self.workspace_info
        )

        # Assert
        self.assertIsInstance(result_info, TableInfo)
        self.assertEqual(result_info.name, table_info.name)
        # Verify orchestration steps in order
        mock_uc_manager.create_catalog_if_not_exists.assert_called_once_with(self.output_port_specific.catalog_name_op)
        mock_uc_manager.create_schema_if_not_exists.assert_called_once_with(
            self.output_port_specific.catalog_name_op, self.output_port_specific.schema_name_op
        )
        mock_ws_manager.get_sql_warehouse_id_from_name.assert_called_once_with(
            self.output_port_specific.sql_warehouse_name
        )
        mock_statement_manager.execute_statement_create_or_replace_view.assert_called_once()
        mock_statement_manager.poll_on_statement_execution.assert_called_once_with(
            self.mock_workspace_client, "stmt-id-456"
        )
        mock_uc_manager.assign_databricks_permission_to_table_or_view.assert_any_call(
            "mapped-owner", Privilege.SELECT, unittest.mock.ANY
        )
        mock_uc_manager.assign_databricks_permission_to_table_or_view.assert_any_call(
            "mapped-dev-group", Privilege.SELECT, unittest.mock.ANY
        )
        mock_statement_manager.add_all_descriptions.assert_called_once()
        mock_uc_manager.get_table_info.assert_called_once()

    @patch("src.service.provision.handler.output_port_handler.UnityCatalogManager")
    @patch("src.service.provision.handler.output_port_handler.WorkspaceManager")
    @patch("src.service.provision.handler.output_port_handler.StatementExecutionManager")
    def test_provision_fails_when_poll_on_statement_execution_fails(
        self, MockStatementManager, MockWorkspaceManager, MockUnityCatalogManager
    ):
        """Test that provisioning fails if polling for view creation fails."""
        # Arrange
        mock_statement_manager = MockStatementManager.return_value
        mock_workspace_manager = MockWorkspaceManager.return_value

        mock_workspace_manager.get_sql_warehouse_id_from_name.return_value = "wh-id-123"
        mock_statement_manager.execute_statement_create_or_replace_view.return_value = "stmt-id-456"

        # Simulate a failure during the polling step
        original_error = ProvisioningError(["Statement failed"])
        mock_statement_manager.poll_on_statement_execution.side_effect = original_error

        # Act & Assert
        with self.assertRaises(ProvisioningError) as cm:
            self.handler.provision_output_port(
                self.data_product, self.output_port_component, self.mock_workspace_client, self.workspace_info
            )

        # Verify the original exception was propagated
        self.assertIn("Statement failed", str(cm.exception.__context__))
        # Verify that downstream steps like setting permissions were not attempted
        MockUnityCatalogManager.return_value.assign_databricks_permission_to_table_or_view.assert_not_called()

    def test_unprovision_output_port_success(self):
        """Test successful unprovisioning of a view."""
        # Arrange
        with patch("src.service.provision.handler.output_port_handler.UnityCatalogManager") as MockUnityCatalogManager:
            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_catalog_existence.return_value = True
            mock_uc_manager.check_schema_existence.return_value = True

            # Act
            self.handler.unprovision_output_port(
                self.data_product, self.output_port_component, self.mock_workspace_client, self.workspace_info
            )

            # Assert
            mock_uc_manager.check_catalog_existence.assert_called_once()
            mock_uc_manager.check_schema_existence.assert_called_once()
            mock_uc_manager.drop_table_if_exists.assert_called_once_with(
                self.output_port_specific.catalog_name_op,
                self.output_port_specific.schema_name_op,
                self.output_port_specific.view_name_op,
            )

    def test_unprovision_output_port_skips_if_schema_not_found(self):
        """Test that unprovisioning is skipped if the schema does not exist."""
        # Arrange
        with patch("src.service.provision.handler.output_port_handler.UnityCatalogManager") as MockUnityCatalogManager:
            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_catalog_existence.return_value = True
            mock_uc_manager.check_schema_existence.return_value = False  # Schema not found

            # Act
            self.handler.unprovision_output_port(
                self.data_product, self.output_port_component, self.mock_workspace_client, self.workspace_info
            )

            # Assert
            # The key assertion: drop_table_if_exists should NOT be called.
            mock_uc_manager.drop_table_if_exists.assert_not_called()

    @patch("src.service.provision.handler.output_port_handler.DatabricksMapper")
    @patch("src.service.provision.handler.output_port_handler.settings")
    def test_update_acl_success_for_dev_env(self, mock_settings, MockDatabricksMapper):
        """Test ACL update logic, including adding, removing, and preserving permissions."""
        # Arrange
        mock_settings.misc.development_environment_name = "development"
        mock_uc_manager = MagicMock()

        # Principals setup
        mapped_owner = "mapped-owner"
        mapped_dev_group = "mapped-dev-group"
        user_to_keep = "user-to-keep@test.com"
        user_to_add = "user-to-add@test.com"
        user_to_remove = "user-to-remove@test.com"

        # Mock the two calls to the mapper
        mock_mapper_instance = MockDatabricksMapper.return_value
        mock_mapper_instance.map.side_effect = [
            {
                self.owner_principal: mapped_owner,
                self.dev_group_principal: mapped_dev_group,
            },  # First call in map_principals
            {f"user:{user_to_keep}": user_to_keep, f"user:{user_to_add}": user_to_add},  # Second call for ACL
        ]

        # Current permissions on the view
        current_perms = [
            PrivilegeAssignment(principal=user_to_keep),
            PrivilegeAssignment(principal=user_to_remove),
            PrivilegeAssignment(principal=mapped_dev_group),  # Dev group should be preserved
        ]
        mock_uc_manager.retrieve_databricks_permissions.return_value = current_perms

        # Act
        self.handler.update_acl(
            self.data_product,
            self.output_port_component,
            [f"user:{user_to_keep}", f"user:{user_to_add}"],
            mock_uc_manager,
        )

        # Assert
        # 1. Verify revoking permissions for the removed user
        mock_uc_manager.update_databricks_permissions.assert_called_once_with(
            user_to_remove, Privilege.SELECT, is_grant_added=False, db_object=unittest.mock.ANY
        )

        # 2. Verify granting permissions for the new user
        grant_calls = mock_uc_manager.assign_databricks_permission_to_table_or_view.call_args_list
        self.assertEqual(len(grant_calls), 2)
        logger.info(grant_calls)
        mock_uc_manager.assign_databricks_permission_to_table_or_view.assert_any_call(
            user_to_add, Privilege.SELECT, unittest.mock.ANY
        )
        mock_uc_manager.assign_databricks_permission_to_table_or_view.assert_any_call(
            user_to_keep, Privilege.SELECT, unittest.mock.ANY
        )

    @patch("src.service.provision.handler.output_port_handler.DatabricksMapper")
    def test_update_acl_fails_if_mapping_fails(self, MockDatabricksMapper):
        """Test that ACL update fails if any principal mapping fails."""
        # Arrange
        mock_uc_manager = MagicMock()
        mock_mapper_instance = MockDatabricksMapper.return_value

        # Mock the first call (owner/dev group) to succeed
        mock_mapper_instance.map.side_effect = [
            {"owner": "mapped-owner", "group:dev-group": "mapped-dev-group"},
            # Mock the second call (ACL list) to fail for one user
            {"user:good@user.com": "good-user-id", "user:bad@user.com": MapperError("User not found")},
        ]

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "An error occurred while updating ACL"):
            self.handler.update_acl(
                self.data_product,
                self.output_port_component,
                ["user:good@user.com", "user:bad@user.com"],
                mock_uc_manager,
            )

        # Verify no permission changes were attempted
        mock_uc_manager.update_databricks_permissions.assert_not_called()
        mock_uc_manager.assign_databricks_permission_to_table_or_view.assert_not_called()

    @patch("src.service.provision.handler.output_port_handler.DatabricksMapper")
    @patch("src.service.provision.handler.output_port_handler.settings")
    def test_update_acl_for_non_dev_env_removes_owner_and_dev_group(self, mock_settings, MockDatabricksMapper):
        """Test that in a non-dev env, owner/dev group are removed if not in the new ACL."""
        # Arrange
        self.data_product.environment = "production"  # Set non-dev environment
        mock_settings.misc.development_environment_name = "development"
        mock_uc_manager = MagicMock()

        mapped_owner = "mapped-owner"
        mapped_dev_group = "mapped-dev-group"
        user_in_acl = "user-in-acl"

        mock_mapper_instance = MockDatabricksMapper.return_value
        mock_mapper_instance.map.side_effect = [
            {self.owner_principal: mapped_owner, self.dev_group_principal: mapped_dev_group},
            {f"user:{user_in_acl}": user_in_acl},  # The new ACL only contains one user
        ]

        # Current permissions include the owner and dev group
        current_perms = [
            PrivilegeAssignment(principal=mapped_owner),
            PrivilegeAssignment(principal=mapped_dev_group),
        ]
        mock_uc_manager.retrieve_databricks_permissions.return_value = current_perms

        # Act
        self.handler.update_acl(self.data_product, self.output_port_component, [f"user:{user_in_acl}"], mock_uc_manager)

        # Assert
        # Verify that both the owner and dev group are removed because the environment is not dev
        # and they are not in the new ACL.
        revoke_calls = mock_uc_manager.update_databricks_permissions.call_args_list
        self.assertEqual(len(revoke_calls), 2)
        mock_uc_manager.update_databricks_permissions.assert_any_call(
            mapped_owner, Privilege.SELECT, is_grant_added=False, db_object=unittest.mock.ANY
        )
        mock_uc_manager.update_databricks_permissions.assert_any_call(
            mapped_dev_group, Privilege.SELECT, is_grant_added=False, db_object=unittest.mock.ANY
        )

        # Verify the new user is granted access
        mock_uc_manager.assign_databricks_permission_to_table_or_view.assert_called_once_with(
            user_in_acl, Privilege.SELECT, unittest.mock.ANY
        )

    def test_map_principals_success(self):
        """
        Test the map_principals helper method for the successful case where all
        principals are mapped correctly.
        """
        # Arrange
        with patch("src.service.provision.handler.output_port_handler.DatabricksMapper") as MockDatabricksMapper:
            mock_mapper = MockDatabricksMapper.return_value
            mock_mapper.map.return_value = {
                self.owner_principal: "mapped-owner",
                self.dev_group_principal: "mapped-dev-group",
            }

            # Act
            owner_id, dev_group_id = self.handler.map_principals(self.data_product, self.output_port_component)

            # Assert
            self.assertEqual(owner_id, "mapped-owner")
            self.assertEqual(dev_group_id, "mapped-dev-group")
            mock_mapper.map.assert_called_once_with({self.owner_principal, self.dev_group_principal})

    def test_map_principals_fails_if_mapper_returns_error(self):
        """
        Test that map_principals raises a ProvisioningError if the underlying
        mapper returns an error for one of the principals.
        """
        # Arrange
        with patch("src.service.provision.handler.output_port_handler.DatabricksMapper") as MockDatabricksMapper:
            mock_mapper = MockDatabricksMapper.return_value
            mock_mapper.map.return_value = {
                self.owner_principal: MapperError("User not found"),
                self.dev_group_principal: "mapped-dev-group",
            }

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "User not found"):
                self.handler.map_principals(self.data_product, self.output_port_component)

    def test_map_principals_fails_if_result_is_missing(self):
        """
        Test that map_principals raises a ProvisioningError if the mapping result
        does not contain one of the required principals.
        """
        # Arrange
        with patch("src.service.provision.handler.output_port_handler.DatabricksMapper") as MockDatabricksMapper:
            mock_mapper = MockDatabricksMapper.return_value
            # The result is missing the dev_group principal
            mock_mapper.map.return_value = {
                self.owner_principal: "mapped-owner",
            }

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "Failed to retrieve outcome of mapping"):
                self.handler.map_principals(self.data_product, self.output_port_component)
