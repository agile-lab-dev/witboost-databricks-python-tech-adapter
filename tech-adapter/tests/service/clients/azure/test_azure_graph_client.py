import unittest
from unittest.mock import AsyncMock, MagicMock

from src.models.databricks.exceptions import AzureGraphClientError
from src.service.clients.azure.azure_graph_client import AzureGraphClient


class TestAzureGraphClient(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the AzureGraphClient class."""

    def setUp(self):
        """Set up the test environment before each test."""
        # The main external dependency is the GraphServiceClient
        self.mock_graph_service_client = MagicMock()
        # The methods we call on its builders are async, so they need AsyncMocks
        self.mock_graph_service_client.users.get = AsyncMock()
        self.mock_graph_service_client.groups.get = AsyncMock()

        # Instantiate the class under test
        self.client = AzureGraphClient(self.mock_graph_service_client)

        # Common test data
        self.user_mail = "test.user@example.com"
        self.user_id = "user-id-12345"
        self.group_name = "Test-Group"
        self.group_id = "group-id-67890"

    async def test_get_user_id_success(self):
        """Test successful retrieval of a user ID."""
        # Arrange: Create a mock response structure that mimics the real API response.
        mock_user = MagicMock()
        mock_user.id = self.user_id
        mock_response = MagicMock()
        mock_response.value = [mock_user]
        self.mock_graph_service_client.users.get.return_value = mock_response

        # Act
        result_id = await self.client.get_user_id(self.user_mail)

        # Assert
        self.assertEqual(result_id, self.user_id)
        # Verify the mock was called correctly
        self.mock_graph_service_client.users.get.assert_awaited_once()
        # Check the arguments passed to the mock call
        _, kwargs = self.mock_graph_service_client.users.get.await_args
        request_config = kwargs.get("request_configuration")
        self.assertIsNotNone(request_config)
        self.assertEqual(request_config.query_parameters.filter, f"mail eq '{self.user_mail}'")
        self.assertEqual(request_config.query_parameters.select, ["id"])

    async def test_get_user_id_not_found(self):
        """Test failure when the user is not found (empty response value)."""
        # Arrange
        mock_response = MagicMock()
        mock_response.value = []  # No user in the response list
        self.mock_graph_service_client.users.get.return_value = mock_response

        # Act & Assert
        with self.assertRaisesRegex(AzureGraphClientError, f"User {self.user_mail} not found"):
            await self.client.get_user_id(self.user_mail)

    async def test_get_user_id_empty_id(self):
        """Test failure when the user is found but has no ID."""
        # Arrange
        mock_user = MagicMock()
        mock_user.id = None
        mock_response = MagicMock()
        mock_response.value = [mock_user]
        self.mock_graph_service_client.users.get.return_value = mock_response

        # Act & Assert
        with self.assertRaisesRegex(AzureGraphClientError, f"User {self.user_mail} not found"):
            await self.client.get_user_id(self.user_mail)

    async def test_get_user_id_api_exception(self):
        """Test failure when the Graph API call raises an exception."""
        # Arrange
        self.mock_graph_service_client.users.get.side_effect = Exception("Graph API is down")

        # Act & Assert
        with self.assertRaisesRegex(AzureGraphClientError, "An unexpected error occurred"):
            await self.client.get_user_id(self.user_mail)

    async def test_get_group_id_success(self):
        """Test successful retrieval of a group ID."""
        # Arrange
        mock_group = MagicMock()
        mock_group.id = self.group_id
        mock_response = MagicMock()
        mock_response.value = [mock_group]
        self.mock_graph_service_client.groups.get.return_value = mock_response

        # Act
        result_id = await self.client.get_group_id(self.group_name)

        # Assert
        self.assertEqual(result_id, self.group_id)
        self.mock_graph_service_client.groups.get.assert_awaited_once()
        _, kwargs = self.mock_graph_service_client.groups.get.await_args
        request_config = kwargs.get("request_configuration")
        self.assertIsNotNone(request_config)
        self.assertEqual(request_config.query_parameters.filter, f"displayName eq '{self.group_name}'")
        self.assertEqual(request_config.query_parameters.select, ["id"])

    async def test_get_group_id_not_found(self):
        """Test failure when the group is not found (empty response value)."""
        # Arrange
        mock_response = MagicMock()
        mock_response.value = []
        self.mock_graph_service_client.groups.get.return_value = mock_response

        # Act & Assert
        with self.assertRaisesRegex(AzureGraphClientError, f"Group {self.group_name} not found"):
            await self.client.get_group_id(self.group_name)

    async def test_get_group_id_api_exception(self):
        """Test failure when the Graph API call for a group raises an exception."""
        # Arrange
        self.mock_graph_service_client.groups.get.side_effect = Exception("Graph API is down")

        # Act & Assert
        with self.assertRaisesRegex(AzureGraphClientError, "An unexpected error occurred"):
            await self.client.get_group_id(self.group_name)
