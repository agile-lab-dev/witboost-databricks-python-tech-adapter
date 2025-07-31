import unittest
from unittest.mock import AsyncMock, MagicMock

from src.models.databricks.exceptions import AzureMapperError
from src.service.principals_mapping.azure_mapper import GROUP_PREFIX, USER_PREFIX, AzureMapper


class TestAzureMapper(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the AzureMapper class."""

    def setUp(self):
        """Set up the test environment with a mocked AzureGraphClient."""
        self.mock_azure_client = MagicMock()
        # The methods we call are async, so they need to be mocked with AsyncMock
        self.mock_azure_client.get_user_id = AsyncMock()
        self.mock_azure_client.get_group_id = AsyncMock()

        # Instantiate the class under test with the mocked client
        self.mapper = AzureMapper(self.mock_azure_client)

        # Common test data
        self.user_subject_witboost_format = f"{USER_PREFIX}john.doe_company.com"
        self.user_subject_email_format = f"{USER_PREFIX}jane.doe@company.com"
        self.group_subject = f"{GROUP_PREFIX}Test Developers"

        self.user_id = "user-id-123"
        self.group_id = "group-id-456"

    async def test_map_all_subjects_successfully(self):
        """Test mapping a set of subjects where all lookups succeed."""
        # Arrange
        self.mock_azure_client.get_user_id.return_value = self.user_id
        self.mock_azure_client.get_group_id.return_value = self.group_id
        subjects_to_map = {self.user_subject_witboost_format, self.group_subject}

        # Act
        results = await self.mapper.map(subjects_to_map)

        # Assert
        # Verify the returned dictionary is correct
        self.assertEqual(len(results), 2)
        self.assertEqual(results[self.user_subject_witboost_format], self.user_id)
        self.assertEqual(results[self.group_subject], self.group_id)

        # Verify the client methods were called with correctly formatted arguments
        self.mock_azure_client.get_user_id.assert_awaited_once_with("john.doe@company.com")
        self.mock_azure_client.get_group_id.assert_awaited_once_with("Test Developers")

    async def test_map_with_partial_failure(self):
        """Test mapping where one subject succeeds and another fails."""
        # Arrange
        # User lookup succeeds
        self.mock_azure_client.get_user_id.return_value = self.user_id
        # Group lookup fails with a specific error from the client
        client_error = AzureMapperError("Group not found in Azure AD")
        self.mock_azure_client.get_group_id.side_effect = client_error
        subjects_to_map = {self.user_subject_email_format, self.group_subject}

        # Act
        results = await self.mapper.map(subjects_to_map)

        # Assert
        self.assertEqual(len(results), 2)
        # Check the successful mapping
        self.assertEqual(results[self.user_subject_email_format], self.user_id)
        # Check that the failed mapping contains the error object
        self.assertIsInstance(results[self.group_subject], AzureMapperError)
        self.assertIs(results[self.group_subject], client_error)

    async def test_map_with_invalid_subject_prefix(self):
        """Test that a subject with an unrecognized prefix results in an error."""
        # Arrange
        invalid_subject = "service_principal:some-sp"
        subjects_to_map = {invalid_subject}

        # Act
        results = await self.mapper.map(subjects_to_map)

        # Assert
        self.assertIsInstance(results[invalid_subject], AzureMapperError)
        self.assertIn("neither a Witboost user nor a group", str(results[invalid_subject]))
        # The client should not have been called
        self.mock_azure_client.get_user_id.assert_not_awaited()
        self.mock_azure_client.get_group_id.assert_not_awaited()

    async def test_get_and_map_user_with_witboost_format(self):
        """Test the internal user mapping logic for the underscore format."""
        # Arrange
        witboost_user_string = "first.last_my-domain.com"
        expected_email = "first.last@my-domain.com"
        self.mock_azure_client.get_user_id.return_value = self.user_id

        # Act
        result_id = await self.mapper._get_and_map_user(witboost_user_string)

        # Assert
        self.assertEqual(result_id, self.user_id)
        self.mock_azure_client.get_user_id.assert_awaited_once_with(expected_email)

    async def test_map_empty_set_returns_empty_dict(self):
        """Test that mapping an empty set of subjects returns an empty dictionary."""
        # Act
        results = await self.mapper.map(set())

        # Assert
        self.assertEqual(results, {})
        self.mock_azure_client.get_user_id.assert_not_awaited()
        self.mock_azure_client.get_group_id.assert_not_awaited()
