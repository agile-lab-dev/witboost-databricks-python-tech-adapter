import unittest
from unittest.mock import MagicMock, patch

from src.models.api_models import ReverseProvisioningRequest, Status1
from src.models.exceptions import ReverseProvisioningError
from src.service.reverse_provision.reverse_provision_service import ReverseProvisionService


class TestReverseProvisionService(unittest.TestCase):
    """Unit tests for the ReverseProvisionService class."""

    def setUp(self):
        self.mock_workflow_handler = MagicMock()
        self.mock_output_port_handler = MagicMock()

        self.service = ReverseProvisionService(
            workflow_reverse_provision_handler=self.mock_workflow_handler,
            output_port_reverse_provision_handler=self.mock_output_port_handler,
        )

        self.base_request = ReverseProvisioningRequest(
            useCaseTemplateId="urn:dmb:utm:databricks-workload-workflow-template:0.0.0",
            environment="development",
            catalogInfo={},
            params={},
        )

    def test_run_reverse_provisioning_dispatches_to_workflow_handler(self):
        """Test that a request with a workflow use case ID is routed to the correct handler."""
        # Arrange
        with patch("src.service.reverse_provision.reverse_provision_service.settings") as mock_settings:
            # Configure settings to recognize the workflow ID
            mock_settings.usecasetemplateid.workload.workflow = ["urn:dmb:utm:databricks-workload-workflow-template"]
            mock_settings.usecasetemplateid.outputPort = []

            # Mock the handler's return value
            expected_updates = {"workflow.job_id": 123}
            self.mock_workflow_handler.reverse_provision.return_value = expected_updates

            # Act
            result = self.service.run_reverse_provisioning(self.base_request)

            # Assert
            self.mock_workflow_handler.reverse_provision.assert_called_once_with(self.base_request)
            self.mock_output_port_handler.reverse_provision.assert_not_called()

            self.assertEqual(result.status, Status1.COMPLETED)
            self.assertEqual(result.updates, expected_updates)
            self.assertEqual(result.logs[0].message, "Reverse provisioning successfully completed.")

    def test_run_reverse_provisioning_dispatches_to_output_port_handler(self):
        """Test that a request with an output port use case ID is routed to the correct handler."""
        # Arrange
        request = self.base_request.model_copy(
            update={"useCaseTemplateId": "urn:dmb:utm:dataproduct-provisioner:outputport:0"}
        )
        with patch("src.service.reverse_provision.reverse_provision_service.settings") as mock_settings:
            mock_settings.usecasetemplateid.workload.workflow = []
            mock_settings.usecasetemplateid.outputPort = ["urn:dmb:utm:dataproduct-provisioner:outputport"]

            expected_updates = {"dataContract.schema": [{"name": "id"}]}
            self.mock_output_port_handler.reverse_provision.return_value = expected_updates

            # Act
            result = self.service.run_reverse_provisioning(request)

            # Assert
            self.mock_output_port_handler.reverse_provision.assert_called_once_with(request)
            self.mock_workflow_handler.reverse_provision.assert_not_called()

            self.assertEqual(result.status, Status1.COMPLETED)
            self.assertEqual(result.updates, expected_updates)

    def test_run_reverse_provisioning_unsupported_component(self):
        """Test that a request with an unsupported use case ID returns a FAILED status."""
        # Arrange
        request = self.base_request.model_copy(update={"useCaseTemplateId": "urn:unsupported:id"})
        with patch("src.service.reverse_provision.reverse_provision_service.settings") as mock_settings:
            # Configure settings so the ID is not in any list
            mock_settings.usecasetemplateid.workload.workflow = []
            mock_settings.usecasetemplateid.outputPort = []

            # Act
            result = self.service.run_reverse_provisioning(request)

            # Assert
            self.mock_workflow_handler.reverse_provision.assert_not_called()
            self.mock_output_port_handler.reverse_provision.assert_not_called()

            self.assertEqual(result.status, Status1.FAILED)
            self.assertEqual(result.updates, {})
            self.assertIn("not yet supported by this Tech Adapter", result.logs[0].message)

    def test_run_reverse_provisioning_handler_raises_exception(self):
        """Test that an exception from a handler is caught and returns a FAILED status."""
        # Arrange
        with patch("src.service.reverse_provision.reverse_provision_service.settings") as mock_settings:
            mock_settings.usecasetemplateid.workload.workflow = ["urn:dmb:utm:databricks-workload-workflow-template"]
            mock_settings.usecasetemplateid.outputPort = []

            # Configure the handler to raise a specific error
            error_message = "Workspace not found"
            self.mock_workflow_handler.reverse_provision.side_effect = ReverseProvisioningError([error_message])

            # Act
            result = self.service.run_reverse_provisioning(self.base_request)

            # Assert
            self.mock_workflow_handler.reverse_provision.assert_called_once_with(self.base_request)

            self.assertEqual(result.status, Status1.FAILED)
            self.assertEqual(result.updates, {})
            self.assertEqual(len(result.logs), 1)
            self.assertEqual(result.logs[0].message, error_message)
