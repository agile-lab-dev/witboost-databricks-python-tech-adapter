import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    PrimaryKeyConstraint,
    TableConstraint,
    TableInfo,
    TableType,
)

from src.models.api_models import ReverseProvisioningRequest
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.exceptions import ReverseProvisioningError
from src.service.reverse_provision.output_port_reverse_provision_handler import OutputPortReverseProvisionHandler


class TestOutputPortReverseProvisionHandler(unittest.TestCase):
    """Unit tests for the OutputPortReverseProvisionHandler class."""

    def setUp(self):
        self.mock_workspace_handler = MagicMock()

        self.handler = OutputPortReverseProvisionHandler(self.mock_workspace_handler)

        # Common test data
        self.workspace_name = "test-workspace"
        self.catalog_name = "test_cat"
        self.schema_name = "test_sch"
        self.table_name = "test_tbl"
        self.table_full_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

        self.request = ReverseProvisioningRequest(
            useCaseTemplateId="urn:dmb:utm:databricks-output-port-template:0.0.0",
            environment="development",
            catalogInfo={},
            params={
                "environmentSpecificConfig": {"specific": {"workspace": self.workspace_name}},
                "catalogName": self.catalog_name,
                "schemaName": self.schema_name,
                "tableName": self.table_name,
                "reverseProvisioningOption": "SCHEMA_AND_DETAILS",
            },
        )
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name=self.workspace_name,
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    def test_reverse_provision_success_with_schema_and_details(self):
        """
        Test the successful reverse provisioning flow with the SCHEMA_AND_DETAILS option,
        verifying the full internal call chain.
        """
        # Arrange
        with patch(
            "src.service.reverse_provision.output_port_reverse_provision_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            # 1. Configure mocks for dependencies at all levels
            self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
            mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value

            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_table_existence.return_value = True

            table_info_response = TableInfo(
                table_type=TableType.MANAGED,
                columns=[
                    ColumnInfo(name="id", type_name=ColumnTypeName.INT),
                    ColumnInfo(name="string_field", type_name=ColumnTypeName.STRING),
                    ColumnInfo(name="decimal_field", type_name=ColumnTypeName.DECIMAL, type_precision=10, type_scale=4),
                ],
            )
            mock_workspace_client.tables.get.return_value = table_info_response

            # Act
            updates = self.handler.reverse_provision(self.request)

            # Assert
            # Verify orchestration
            self.mock_workspace_handler.get_workspace_info_by_name.assert_called_once_with(self.workspace_name)
            mock_uc_manager.check_table_existence.assert_called_once()
            # `tables.get` is called once in validation and once in retrieval
            self.assertEqual(mock_workspace_client.tables.get.call_count, 2)
            # Verify the structure of the final updates dictionary
            self.assertIn("spec.mesh.dataContract.schema", updates)
            self.assertEqual(
                updates["spec.mesh.dataContract.schema"],
                [
                    {
                        "name": "id",
                        "dataType": "INT",
                        "dataLength": None,
                        "constraint": "NOT_NULL",
                        "precision": None,
                        "scale": None,
                        "description": None,
                        "tags": None,
                    },
                    {
                        "name": "string_field",
                        "dataType": "STRING",
                        "dataLength": 65535,
                        "constraint": "NOT_NULL",
                        "precision": None,
                        "scale": None,
                        "description": None,
                        "tags": None,
                    },
                    {
                        "name": "decimal_field",
                        "dataType": "DECIMAL",
                        "dataLength": None,
                        "constraint": "NOT_NULL",
                        "precision": 10,
                        "scale": 4,
                        "description": None,
                        "tags": None,
                    },
                ],
            )
            self.assertIn("spec.mesh.specific.tableName", updates)
            self.assertEqual(updates["spec.mesh.specific.tableName"], self.table_name)

    def test_reverse_provision_success_schema_only(self):
        """
        Test the successful reverse provisioning flow with the schema-only option
        """
        # Arrange
        self.request.params["reverseProvisioningOption"] = "SCHEMA_ONLY"
        with patch(
            "src.service.reverse_provision.output_port_reverse_provision_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
            mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value

            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_table_existence.return_value = True

            # `tables.get` is only needed for column retrieval in this path
            mock_workspace_client.tables.get.return_value = TableInfo(
                columns=[
                    ColumnInfo(name="id", type_name=ColumnTypeName.INT),
                    ColumnInfo(name="id", type_name=ColumnTypeName.STRING),
                    ColumnInfo(name="id", type_name=ColumnTypeName.DECIMAL, type_precision=10, type_scale=4),
                ]
            )

            # Act
            updates = self.handler.reverse_provision(self.request)

            # Assert
            self.assertIn("spec.mesh.dataContract.schema", updates)
            # The key check: table details should NOT be present in the updates
            self.assertNotIn("spec.mesh.specific.tableName", updates)

            # `tables.get` should only be called once from _retrieve_columns_list, not from validation
            mock_workspace_client.tables.get.assert_called_once()

    def test_validate_fails_if_table_does_not_exist(self):
        """Test that the validation helper fails if the target table cannot be found."""
        # Arrange
        with patch(
            "src.service.reverse_provision.output_port_reverse_provision_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_table_existence.return_value = False
            params = MagicMock()

            # Act & Assert
            with self.assertRaisesRegex(ReverseProvisioningError, "does not exist"):
                self.handler._validate_provision_request(MagicMock(), MagicMock(), params)

    def test_validate_fails_for_details_on_a_view(self):
        """Test that the validation helper fails if SCHEMA_AND_DETAILS is requested for a VIEW."""
        # Arrange
        mock_workspace_client = MagicMock()
        mock_workspace_client.tables.get.return_value = TableInfo(table_type=TableType.VIEW)

        with patch(
            "src.service.reverse_provision.output_port_reverse_provision_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_table_existence.return_value = True
            params = MagicMock(reverse_provisioning_option="SCHEMA_AND_DETAILS")

            # Act & Assert
            with self.assertRaisesRegex(ReverseProvisioningError, "not possible to inherit Table Details from a VIEW"):
                self.handler._validate_provision_request(mock_workspace_client, MagicMock(), params)

    def test_retrieve_columns_list_maps_types_and_constraints(self):
        """Test the column retrieval and mapping logic with various data types and constraints."""
        # Arrange
        mock_workspace_client = MagicMock()

        table_columns = [
            ColumnInfo(name="pk_col", type_name=ColumnTypeName.INT, nullable=False),
            ColumnInfo(name="not_null_col", type_name=ColumnTypeName.STRING, nullable=False),
            ColumnInfo(name="decimal_col", type_name=ColumnTypeName.DECIMAL, type_precision=10, type_scale=2),
            ColumnInfo(name="date_col", type_name=ColumnTypeName.DATE, comment="A date column"),
            ColumnInfo(name="array_col", type_name=ColumnTypeName.ARRAY, type_text="array<string>"),
        ]
        pk_constraint = TableConstraint(
            primary_key_constraint=PrimaryKeyConstraint(child_columns=["pk_col"], name="pk_col_constraint")
        )
        mock_workspace_client.tables.get.return_value = TableInfo(
            columns=table_columns, table_constraints=[pk_constraint]
        )

        # Act
        columns_list = self.handler._retrieve_columns_list(mock_workspace_client, self.table_full_name)

        # Assert
        self.assertEqual(len(columns_list), 5)
        cols_by_name = {c.name: c for c in columns_list}
        self.assertEqual(cols_by_name["pk_col"].constraint, "PRIMARY_KEY")
        self.assertEqual(cols_by_name["not_null_col"].constraint, "NOT_NULL")
        self.assertEqual(cols_by_name["decimal_col"].precision, 10)
        self.assertEqual(cols_by_name["decimal_col"].scale, 2)
        self.assertEqual(cols_by_name["date_col"].description, "A date column")
        self.assertEqual(cols_by_name["array_col"].dataType, "ARRAY")

    def test_map_databricks_to_open_metadata_unsupported_type(self):
        """Test that the type mapper fails for an unsupported Databricks type."""
        # Act & Assert
        with self.assertRaisesRegex(ReverseProvisioningError, "Not able to convert data type"):
            self.handler._map_databricks_to_open_metadata("UNSUPPORTED_TYPE")
