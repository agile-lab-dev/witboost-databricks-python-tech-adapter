import unittest
from unittest.mock import MagicMock

from azure.mgmt.databricks.models import ProvisioningState

from src.models.api_models import RequestValidationError, Status1
from src.models.data_product_descriptor import DataContract, DataProduct
from src.models.databricks.databricks_models import DatabricksOutputPort, JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    JobClusterSpecific,
)
from src.models.exceptions import ProvisioningError
from src.service.provision.update_acl_service import UpdateAclService


class TestUpdateAclService(unittest.TestCase):
    """Unit tests for the UpdateAclService class."""

    def setUp(self):
        """Set up mocks for all injected dependencies."""
        self.mock_workspace_handler = MagicMock()
        self.mock_output_port_handler = MagicMock()

        # The service under test, with mocked dependencies
        self.service = UpdateAclService(
            workspace_handler=self.mock_workspace_handler,
            output_port_handler=self.mock_output_port_handler,
        )

        # Common test data
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner="user:john.doe_test.com",
            devGroup="group:test-group",
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
        self.users = ["user:jane.doe_test.com"]
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    def test_update_acl_success(self):
        """Test the successful ACL update flow for a supported component."""
        # Arrange
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
        mock_workspace_client = MagicMock()
        self.mock_workspace_handler.get_workspace_client.return_value = mock_workspace_client

        # Act
        result = self.service.update_acl(self.data_product, self.output_port_component, self.users)

        # Assert
        # Verify the orchestration steps
        self.mock_workspace_handler.get_workspace_info_by_name.assert_called_once_with("test-workspace-op")
        self.mock_workspace_handler.get_workspace_client.assert_called_once_with(self.workspace_info)

        # Verify the final delegation to the output port handler
        self.mock_output_port_handler.update_acl.assert_called_once()
        # Inspect the call arguments to ensure the UnityCatalogManager was created and passed correctly
        _, kwargs = self.mock_output_port_handler.update_acl.call_args
        self.assertEqual(kwargs["data_product"], self.data_product)
        self.assertEqual(kwargs["component"], self.output_port_component)
        self.assertEqual(kwargs["witboost_identities"], self.users)

        # Check the final status
        self.assertEqual(result.status, Status1.COMPLETED)
        self.assertIn("Update of Acl completed", result.result)

    def test_update_acl_unsupported_component_type(self):
        """Test that an unsupported component type returns a RequestValidationError."""
        # Arrange
        # Create a generic component that is not an OutputPort
        unsupported_component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace="test-workspace",
                metastore="test_metastore",
                repoPath="Repos/test/repo",
                jobName="test_job",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="https://test.repo",
                    gitReference="gitReference",
                    gitPath="gitPath",
                    gitReferenceType="branch",
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )

        # Act
        result = self.service.update_acl(self.data_product, unsupported_component, self.users)

        # Assert
        self.assertIsInstance(result, RequestValidationError)
        self.assertIn("is not supported for updating access control", result.moreInfo.problems[0])
        # Verify no handler methods were called
        self.mock_workspace_handler.get_workspace_info_by_name.assert_not_called()
        self.mock_output_port_handler.update_acl.assert_not_called()

    def test_update_acl_workspace_not_found(self):
        """Test that a FAILED status is returned if the workspace cannot be found."""
        # Arrange
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = None

        # Act
        result = self.service.update_acl(self.data_product, self.output_port_component, self.users)

        # Assert
        self.assertEqual(result.status, Status1.FAILED)
        self.assertIn("Unable to retrieve info for workspace", result.result)
        # Verify the handler was called but the process stopped
        self.mock_workspace_handler.get_workspace_info_by_name.assert_called_once_with("test-workspace-op")
        self.mock_output_port_handler.update_acl.assert_not_called()

    def test_update_acl_handler_failure(self):
        """Test that a FAILED status is returned if the output_port_handler raises an exception."""
        # Arrange
        self.mock_workspace_handler.get_workspace_info_by_name.return_value = self.workspace_info
        self.mock_workspace_handler.get_workspace_client.return_value = MagicMock()
        # Simulate a failure during the final ACL update step
        original_error = ProvisioningError(["Mapping failed for user 'jane.doe'"])
        self.mock_output_port_handler.update_acl.side_effect = original_error

        # Act
        result = self.service.update_acl(self.data_product, self.output_port_component, self.users)

        # Assert
        self.assertEqual(result.status, Status1.FAILED)
        self.assertIn("Mapping failed for user 'jane.doe'", result.result)
        self.mock_output_port_handler.update_acl.assert_called_once()
