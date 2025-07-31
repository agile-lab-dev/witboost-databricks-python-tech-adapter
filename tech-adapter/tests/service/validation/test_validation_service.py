import unittest
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import Job, JobSettings

from src.models.api_models import RequestValidationError
from src.models.data_product_descriptor import DataContract, DataProduct, OutputPort, Workload
from src.models.databricks.databricks_models import DatabricksOutputPort, DLTWorkload, JobWorkload, WorkflowWorkload
from src.models.exceptions import ProvisioningError
from src.service.validation.validation_service import _parse_component, validate, validate_update_acl


class TestValidationService(unittest.TestCase):
    """Unit tests for the validation service functions."""

    def setUp(self):
        """Set up common test data and mocks."""
        self.mock_workspace_handler = MagicMock()
        self.mock_output_port_validation_service = MagicMock()

        self.output_port = OutputPort(
            id="comp-id",
            name="test-output-port",
            description="description",
            useCaseTemplateId="urn:dmb:utm:databricks-workload-outputport-template:0.0.0",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            tags=[],
            kind="outputport",
            outputPortType="SQL",
            semanticLinking=[],
            dataContract=DataContract(),
            specific={
                "catalog_name": "input_catalog",
                "schema_name": "input_schema",
                "table_name": "input_table",
                "sql_warehouse_name": "test-wh",
                "catalog_name_op": "output_catalog",
                "schema_name_op": "output_schema",
                "view_name_op": "output_view",
                "workspace": "test-workspace",
                "workspace_op": "test-workspace-op",
                "metastore": "test-metastore",
            },
        )

        self.job_workload = Workload(
            id="job-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="urn:dmb:utm:databricks-workload-job-template:0.0.0",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific={
                "workspace": "test-workspace",
                "metastore": "test_metastore",
                "repoPath": "Repos/test/repo",
                "jobName": "test_job",
                "git": {
                    "gitRepoUrl": "https://test.repo",
                    "gitReference": "gitReference",
                    "gitPath": "gitPath",
                    "gitReferenceType": "branch",
                },
                "cluster": {"clusterSparkVersion": "14.4.5", "nodeTypeId": "nodeTypeId", "numWorkers": 1},
            },
        )

        self.dlt_workload = Workload(
            id="dlt-id",
            name="test-dlt-component",
            description="description",
            useCaseTemplateId="urn:dmb:utm:databricks-workload-dlt-template:0.0.0",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific={
                "pipeline_name": "test_dlt_pipeline",
                "catalog": "test_catalog",
                "product_edition": "pro",
                "continuous": False,
                "photon": True,
                "channel": "current",
                "cluster": {"num_workers": 1, "worker_type": "Standard_F4s", "driver_type": "Standard_F4s"},
                "notebooks": ["/path/to/notebook.py"],
                "target": "test_target_schema",
                "workspace": "test-workspace",
                "metastore": "test_metastore",
                "git": {"gitRepoUrl": "https://test.repo"},
                "repoPath": "Repos/test/repo",
            },
        )
        self.workflow_job = Job(settings=JobSettings(name="test-workflow"))

        self.workflow_workload = Workload(
            id="workflow-id",
            name="test-workflow-component",
            description="description",
            useCaseTemplateId="urn:dmb:utm:databricks-workload-workflow-template:0.0.0",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific={
                "workspace": "test-workspace",
                "workflow": self.workflow_job.as_dict(),
                "git": {"gitRepoUrl": "https://test.repo"},
                "repoPath": "Repos/test/repo",
                "override": False,
            },
        )
        # Create a sample DataProduct object for testing
        self.sample_data_product = DataProduct(
            id="1",
            name="Sample Data Product",
            description="A test data product",
            kind="dataproduct",
            domain="Sample Domain",
            version="1.0",
            environment="Development",
            dataProductOwner="John Doe",
            ownerGroup="Data Owners",
            devGroup="Development Team",
            specific={},
            components=[self.output_port, self.job_workload, self.dlt_workload, self.workflow_workload],
            tags=[],
        )

    @patch("src.service.validation.validation_service.settings")
    def test_parse_component_job_workload_success(self, mock_settings):
        """Test that a component with a job use case template ID is parsed as a JobWorkload."""
        # Arrange
        mock_settings.usecasetemplateid.workload.job = ["urn:dmb:utm:databricks-workload-job-template"]

        # Act
        parsed_component = _parse_component(self.job_workload)

        # Assert
        self.assertIsInstance(parsed_component, JobWorkload)

    @patch("src.service.validation.validation_service.settings")
    def test_parse_component_workflow_workload_success(self, mock_settings):
        """Test that a component with a workflow use case template ID is parsed as a JobWorkload."""
        # Arrange
        mock_settings.usecasetemplateid.workload.workflow = ["urn:dmb:utm:databricks-workload-workflow-template"]

        # Act
        parsed_component = _parse_component(self.workflow_workload)

        # Assert
        self.assertIsInstance(parsed_component, WorkflowWorkload)

    @patch("src.service.validation.validation_service.settings")
    def test_parse_component_dlt_workload_success(self, mock_settings):
        """Test that a component with a dlt use case template ID is parsed as a JobWorkload."""
        # Arrange
        mock_settings.usecasetemplateid.workload.dlt = ["urn:dmb:utm:databricks-workload-dlt-template"]

        # Act
        parsed_component = _parse_component(self.dlt_workload)

        # Assert
        self.assertIsInstance(parsed_component, DLTWorkload)

    @patch("src.service.validation.validation_service.settings")
    def test_parse_component_outputport_success(self, mock_settings):
        """Test that a component with a job use case template ID is parsed as a JobWorkload."""
        # Arrange
        mock_settings.usecasetemplateid.outputPort = ["urn:dmb:utm:databricks-workload-outputport-template"]

        # Act
        parsed_component = _parse_component(self.output_port)

        # Assert
        self.assertIsInstance(parsed_component, DatabricksOutputPort)

    @patch("src.service.validation.validation_service.settings")
    def test_parse_component_unsupported_template_id(self, mock_settings):
        """Test that an unsupported use case template ID raises a ProvisioningError."""
        # Arrange
        mock_settings.usecasetemplateid.workload.job = []
        mock_settings.usecasetemplateid.workload.workflow = []
        mock_settings.usecasetemplateid.outputPort = []
        mock_settings.usecasetemplateid.workload.dlt = []

        # Act & Assert
        with self.assertRaisesRegex(ProvisioningError, "unsupported use case template id"):
            _parse_component(self.job_workload)

    def test_validate_success_for_workload(self):
        """Test the main validation function for a valid JobWorkload component."""
        # Arrange
        request = (self.sample_data_product, self.job_workload.id, False)
        with patch("src.service.validation.validation_service._parse_component") as mock_parse:
            parsed_workload = JobWorkload.model_validate(self.job_workload.model_dump())
            mock_parse.return_value = parsed_workload

            # Act
            result = validate(request, self.mock_workspace_handler)

            # Assert
            self.assertNotIsInstance(result, RequestValidationError)
            dp_res, comp_res, remove_data_res = result
            self.assertEqual(dp_res, self.sample_data_product)
            self.assertEqual(comp_res, parsed_workload)
            self.assertFalse(remove_data_res)

    @patch("src.service.validation.validation_service.OutputPortValidation")
    def test_validate_success_for_output_port(self, MockOutputPortValidation):
        """Test the main validation function for a valid OutputPort, ensuring its specific validation is called."""
        # Arrange

        request = (self.sample_data_product, self.output_port.id, False)

        mock_op_validator = MockOutputPortValidation.return_value

        with patch("src.service.validation.validation_service._parse_component") as mock_parse:
            parsed_output_port = DatabricksOutputPort.model_validate(self.output_port.model_dump())
            mock_parse.return_value = parsed_output_port

            # Act
            result = validate(request, self.mock_workspace_handler)

            # Assert
            self.assertNotIsInstance(result, RequestValidationError)
            MockOutputPortValidation.assert_called_once_with(self.mock_workspace_handler)
            mock_op_validator.validate.assert_called_once_with(parsed_output_port, self.sample_data_product.environment)

    def test_validate_component_not_found(self):
        """Test that validation returns an error if the component ID is not in the data product."""
        # Arrange
        request = (self.sample_data_product, "non-existent-id", False)

        # Act
        result = validate(request, self.mock_workspace_handler)

        # Assert
        self.assertIsInstance(result, RequestValidationError)
        self.assertIn("Component with ID non-existent-id not found", result.errors[0])

    def test_validate_update_acl_success(self):
        """Test that validate_update_acl correctly reuses the main validate function."""
        # Arrange
        users = ["user:test@user.com"]
        request = (self.sample_data_product, self.output_port.id, users)

        # We can patch the inner validate function to test the wrapper's logic
        with patch("src.service.validation.validation_service.validate") as mock_validate:
            parsed_component = DatabricksOutputPort.model_validate(self.output_port.model_dump())
            # Simulate a successful validation result
            mock_validate.return_value = (self.sample_data_product, parsed_component, False)

            # Act
            result = validate_update_acl(request, self.mock_workspace_handler)

            # Assert
            self.assertNotIsInstance(result, RequestValidationError)
            dp_res, comp_res, users_res = result
            self.assertEqual(dp_res, self.sample_data_product)
            self.assertEqual(comp_res, parsed_component)
            self.assertEqual(users_res, users)
            # Verify that the inner validation was called correctly
            mock_validate.assert_called_once_with(
                (self.sample_data_product, self.output_port.id, False), self.mock_workspace_handler
            )

    def test_validate_update_acl_forwards_validation_error(self):
        """Test that validate_update_acl forwards errors from the main validate function."""
        # Arrange
        request = (self.sample_data_product, self.output_port.id, [])
        validation_error = RequestValidationError(errors=["Inner validation failed"])

        with patch("src.service.validation.validation_service.validate") as mock_validate:
            mock_validate.return_value = validation_error

            # Act
            result = validate_update_acl(request, self.mock_workspace_handler)

            # Assert
            self.assertIs(result, validation_error)
