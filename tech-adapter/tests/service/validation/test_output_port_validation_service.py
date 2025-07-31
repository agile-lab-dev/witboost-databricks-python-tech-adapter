import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.errors.platform import NotFound

from src.models.data_product_descriptor import DataContract, OpenMetadataColumn
from src.models.databricks.databricks_models import DatabricksOutputPort
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.models.exceptions import ProvisioningError
from src.service.validation.output_port_validation_service import OutputPortValidation


class TestOutputPortValidation(unittest.TestCase):
    """Unit tests for the OutputPortValidation service."""

    def setUp(self):
        """Set up mocks for all injected dependencies."""
        self.mock_workspace_handler = MagicMock()

        # The service under test
        self.validation_service = OutputPortValidation(self.mock_workspace_handler)

        # Common test data
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
        self.environment = "development"

    def test_validate_success_full_check(self):
        """
        Test the main validation success path where workspace, metastore, and table
        all exist and are valid
        """
        # Arrange
        # 1. Mock the top-level dependency
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
        mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value

        # 2. Mock the dependencies of the inner `validate_table_existence_and_schema` method
        with patch(
            "src.service.validation.output_port_validation_service.UnityCatalogManager"
        ) as MockUnityCatalgoManager:
            mock_uc_manager = MockUnityCatalgoManager.return_value
            # Simulate that the source table exists
            mock_uc_manager.check_table_existence.return_value = True
            # Simulate a valid schema (view schema is a subset of table schema)
            self.output_port_component.dataContract.schema_ = [OpenMetadataColumn(name="id", dataType="STRING")]
            mock_uc_manager.retrieve_table_columns_names.return_value = ["id", "value"]

            # Act
            # This should now execute the full logic of both methods without error
            try:
                self.validation_service.validate(self.output_port_component, self.environment)
            except ProvisioningError:
                self.fail("ProvisioningError was raised unexpectedly on a successful validation path.")

            # Assert
            self.mock_workspace_handler.get_workspace_info_by_name.assert_called_once_with("test-workspace")
            self.mock_workspace_handler.get_workspace_client.assert_called_once_with(self.workspace_info)
            mock_workspace_client.metastores.current.assert_called_once()

            MockUnityCatalgoManager.assert_called_once_with(mock_workspace_client, self.workspace_info)
            mock_uc_manager.check_table_existence.assert_called_once()
            mock_uc_manager.retrieve_table_columns_names.assert_called_once()

    def test_validate_skips_if_workspace_does_not_exist(self):
        """Test that validation completes with a warning if the workspace doesn't exist yet."""
        # Arrange
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = None

        # Act
        try:
            self.validation_service.validate(self.output_port_component, self.environment)
        except ProvisioningError:
            self.fail("ProvisioningError should not be raised when the workspace does not exist.")

        # Assert
        self.mock_workspace_handler.get_workspace_client.assert_not_called()

    def test_validate_skips_if_metastore_not_found_on_managed_workspace(self):
        """Test that validation completes with a warning if the metastore is not attached to a managed workspace."""
        # Arrange
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
        mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value
        # Simulate the metastore not being attached
        mock_workspace_client.metastores.current.side_effect = NotFound("Metastore not found")

        # Act
        try:
            self.validation_service.validate(self.output_port_component, self.environment)
        except ProvisioningError:
            self.fail("ProvisioningError should not be raised when metastore is missing on a managed workspace.")

    def test_validate_fails_if_metastore_not_found_on_unmanaged_workspace(self):
        """Test that validation fails if the metastore is not found on an unmanaged workspace."""
        # Arrange
        workspace_info_unmanaged = self.workspace_info.model_copy(update={"is_managed": False})
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = workspace_info_unmanaged
        mock_workspace_client = self.mock_workspace_handler.get_workspace_client.return_value
        mock_workspace_client.metastores.current.side_effect = NotFound("Metastore not found")

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "No metastore assigned for the current workspace"):
            self.validation_service.validate(self.output_port_component, self.environment)

    def test_validate_table_existence_fails_if_table_not_found(self):
        """Test that validation fails if the source table does not exist."""
        # Arrange
        with patch(
            "src.service.validation.output_port_validation_service.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            mock_uc_manager = MockUnityCatalogManager.return_value
            # Simulate the table not being found
            mock_uc_manager.check_table_existence.return_value = False

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "does not exist"):
                self.validation_service.validate_table_existence_and_schema(
                    MagicMock(), self.output_port_component, self.environment, self.workspace_info
                )

            # Verify no attempt was made to check the schema
            mock_uc_manager.retrieve_table_columns_names.assert_not_called()

    def test_validate_table_schema_fails_if_view_column_not_in_table(self):
        """Test that validation fails if the view schema contains a column not present in the source table."""
        # Arrange
        # Define a view schema with a column that won't be in the table's schema
        view_schema = [
            OpenMetadataColumn(name="id", dataType="STRING"),
            OpenMetadataColumn(name="extra_column", dataType="STRING"),
        ]
        self.output_port_component.dataContract.schema_ = view_schema

        with patch(
            "src.service.validation.output_port_validation_service.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            mock_uc_manager = MockUnityCatalogManager.return_value
            mock_uc_manager.check_table_existence.return_value = True
            # The source table only has 'id' and 'value'
            mock_uc_manager.retrieve_table_columns_names.return_value = ["id", "value"]

            # Act & Assert
            with self.assertRaises(ProvisioningError) as cm:
                self.validation_service.validate_table_existence_and_schema(
                    MagicMock(), self.output_port_component, self.environment, self.workspace_info
                )

            # Check the error message
            self.assertIn("cannot be found in the table", str(cm.exception))
            self.assertIn("extra_column", str(cm.exception))
