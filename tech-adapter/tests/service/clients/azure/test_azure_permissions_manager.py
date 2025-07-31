import unittest
from unittest.mock import MagicMock

from azure.core.exceptions import ResourceExistsError
from azure.mgmt.authorization.models import PrincipalType, RoleAssignment, RoleAssignmentCreateParameters

from src.models.databricks.exceptions import AzurePermissionsError
from src.service.clients.azure.azure_permissions_manager import AzurePermissionsManager


class TestAzurePermissionsManager(unittest.TestCase):
    """Unit tests for the AzurePermissionsManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        # The main external dependency to mock
        self.mock_auth_client = MagicMock()
        # Instantiate the class under test
        self.manager = AzurePermissionsManager(self.mock_auth_client)

        # Common test data
        self.resource_id = (
            "/subscriptions/sub-id/resourceGroups/rg-name/providers/Microsoft.Databricks/workspaces/ws-name"
        )
        self.permission_id = "a-unique-guid-for-the-assignment"
        self.role_definition_id = "/subscriptions/sub-id/providers/Microsoft.Authorization/roleDefinitions/role-def-id"
        self.principal_id = "principal-object-id"
        self.principal_type = PrincipalType.SERVICE_PRINCIPAL
        self.workspace_name = "ws-name"
        self.role_assignment_id = (
            f"{self.resource_id}/providers/Microsoft.Authorization/roleAssignments/{self.permission_id}"
        )

    def test_assign_permissions_success(self):
        """Test successful assignment of a new role."""
        # Act
        self.manager.assign_permissions(
            self.resource_id, self.permission_id, self.role_definition_id, self.principal_id, self.principal_type
        )

        # Assert
        # Check that the 'create' method on the SDK client was called once.
        self.mock_auth_client.role_assignments.create.assert_called_once()
        # Inspect the arguments passed to the mock call to verify correctness.
        _, kwargs = self.mock_auth_client.role_assignments.create.call_args
        self.assertEqual(kwargs["scope"], self.resource_id)
        self.assertEqual(kwargs["role_assignment_name"], self.permission_id)

        # Verify the parameters object was constructed correctly.
        params: RoleAssignmentCreateParameters = kwargs["parameters"]
        self.assertEqual(params.role_definition_id, self.role_definition_id)
        self.assertEqual(params.principal_id, self.principal_id)
        self.assertEqual(params.principal_type, self.principal_type)

    def test_assign_permissions_already_exists(self):
        """Test that a ResourceExistsError is handled gracefully and does not raise an exception."""
        # Arrange
        self.mock_auth_client.role_assignments.create.side_effect = ResourceExistsError(
            "The role assignment already exists."
        )

        # Act
        try:
            self.manager.assign_permissions(
                self.resource_id, self.permission_id, self.role_definition_id, self.principal_id, self.principal_type
            )
        except AzurePermissionsError:
            self.fail("AzurePermissionsError was raised unexpectedly for a pre-existing role assignment.")

        # Assert
        self.mock_auth_client.role_assignments.create.assert_called_once()

    def test_assign_permissions_fails_on_api_error(self):
        """Test that a generic API error is wrapped in AzurePermissionsError."""
        # Arrange
        self.mock_auth_client.role_assignments.create.side_effect = Exception("Generic API failure")

        # Act & Assert
        with self.assertRaisesRegex(AzurePermissionsError, "Error assigning permissions"):
            self.manager.assign_permissions(
                self.resource_id, self.permission_id, self.role_definition_id, self.principal_id, self.principal_type
            )

    def test_get_principal_role_assignments_on_resource_success(self):
        """Test successful retrieval and filtering of role assignments."""
        # Arrange
        matching_assignment = RoleAssignment(principal_id=self.principal_id)
        # Use a different case to test case-insensitive comparison
        matching_assignment_case = RoleAssignment(principal_id=self.principal_id.upper())
        non_matching_assignment = RoleAssignment(principal_id="a-different-principal-id")

        all_assignments = [matching_assignment, non_matching_assignment, matching_assignment_case]
        self.mock_auth_client.role_assignments.list_for_resource.return_value = all_assignments

        # Act
        result = self.manager.get_principal_role_assignments_on_resource(
            "rg-name", "Microsoft.Databricks", "workspaces", "ws-name", self.principal_id
        )

        # Assert
        # Verify the underlying SDK method was called correctly
        self.mock_auth_client.role_assignments.list_for_resource.assert_called_once_with(
            resource_group_name="rg-name",
            resource_provider_namespace="Microsoft.Databricks",
            parent_resource_path="",
            resource_type="workspaces",
            resource_name="ws-name",
        )
        # Verify the in-memory filtering worked correctly
        self.assertEqual(len(result), 2)
        self.assertIn(matching_assignment, result)
        self.assertIn(matching_assignment_case, result)
        self.assertNotIn(non_matching_assignment, result)

    def test_get_principal_role_assignments_on_resource_api_error(self):
        """Test that an API error during retrieval is wrapped in AzurePermissionsError."""
        # Arrange
        self.mock_auth_client.role_assignments.list_for_resource.side_effect = Exception("API list failure")

        # Act & Assert
        with self.assertRaisesRegex(AzurePermissionsError, "Error retrieving role assignments"):
            self.manager.get_principal_role_assignments_on_resource(
                "rg-name", "Microsoft.Databricks", "workspaces", "ws-name", self.principal_id
            )

    def test_delete_role_assignment_success(self):
        """Test successful deletion of a role assignment by its ID."""
        # Act
        self.manager.delete_role_assignment(self.workspace_name, self.role_assignment_id)

        # Assert
        self.mock_auth_client.role_assignments.delete_by_id.assert_called_once_with(self.role_assignment_id)

    def test_delete_role_assignment_fails_on_api_error(self):
        """Test that an API error during deletion is wrapped in AzurePermissionsError."""
        # Arrange
        self.mock_auth_client.role_assignments.delete_by_id.side_effect = Exception("API delete failure")

        # Act & Assert
        with self.assertRaisesRegex(AzurePermissionsError, "Error deleting role assignment"):
            self.manager.delete_role_assignment(self.workspace_name, self.role_assignment_id)
