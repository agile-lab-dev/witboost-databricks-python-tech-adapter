import unittest
from unittest.mock import Mock

from databricks.sdk.service.iam import Group

from src.models.databricks.exceptions import DatabricksMapperError
from src.service.principals_mapping.databricks_mapper import DatabricksMapper


class TestDatabricksMapper(unittest.TestCase):
    def setUp(self):
        self.mock_account_client = Mock()
        self.mapper = DatabricksMapper(account_client=self.mock_account_client)

    def test_retrieve_group_ok(self):
        """
        Test retrieve_case_sensitive_group_display_name returns the correct name on success.
        """
        group_name_insensitive = "test-group"
        group_name_sensitive = "Test-Group"
        mock_group = Group(display_name=group_name_sensitive, id="123")
        self.mock_account_client.groups.list.return_value = [mock_group]

        result = self.mapper.retrieve_case_sensitive_group_display_name(group_name_insensitive)

        self.assertEqual(result, group_name_sensitive)
        self.mock_account_client.groups.list.assert_called_once_with(
            filter=f"displayName eq '{group_name_insensitive}'"
        )

    def test_retrieve_group_not_found_ko(self):
        """
        Test retrieve_case_sensitive_group_display_name raises GroupNotFoundError when no group is found.
        """
        group_name_insensitive = "non-existent-group"
        self.mock_account_client.groups.list.return_value = []

        with self.assertRaises(DatabricksMapperError) as context:
            self.mapper.retrieve_case_sensitive_group_display_name(group_name_insensitive)

        self.assertIn(f"Group '{group_name_insensitive}' not found", str(context.exception))

    def test_retrieve_group_multiple_found_ko(self):
        """
        Test retrieve_case_sensitive_group_display_name raises MultipleGroupsFoundError when multiple groups are found.
        """
        group_name_insensitive = "ambiguous-group"
        mock_group1 = Group(display_name="Ambiguous-Group", id="123")
        mock_group2 = Group(display_name="ambiguous-group", id="456")
        self.mock_account_client.groups.list.return_value = [mock_group1, mock_group2]

        with self.assertRaises(DatabricksMapperError) as context:
            self.mapper.retrieve_case_sensitive_group_display_name(group_name_insensitive)

        self.assertIn(f"More than one group with name '{group_name_insensitive}'", str(context.exception))

    def test_map_ok(self):
        """
        Test the map method successfully maps a mix of users and groups.
        """
        # Arrange
        subjects = {"user:john.doe_example.com", "group:data-engineers"}

        # Mock the group lookup
        mock_group = Group(display_name="Data-Engineers", id="grp1")
        self.mock_account_client.groups.list.return_value = [mock_group]

        result = self.mapper.map(subjects)

        expected = {"user:john.doe_example.com": "john.doe@example.com", "group:data-engineers": "Data-Engineers"}
        self.assertEqual(result, expected)
        self.mock_account_client.groups.list.assert_called_once_with(filter="displayName eq 'data-engineers'")

    def test_map_with_failures_ko(self):
        """
        Test the map method handles both successful and failed mappings in the same call.
        """
        subjects = {
            "user:jane.doe_example.com",  # Will succeed
            "group:non-existent-group",  # Will fail (not found)
            "invalid:subject",  # Will fail (bad format)
        }

        # Mock the group lookup to return an empty list
        self.mock_account_client.groups.list.return_value = []

        result = self.mapper.map(subjects)

        # Check for successful mapping
        self.assertEqual(result["user:jane.doe_example.com"], "jane.doe@example.com")

        # Check for failed group mapping
        self.assertIsInstance(result["group:non-existent-group"], DatabricksMapperError)
        self.assertIn("not found", str(result["group:non-existent-group"]))

        # Check for invalid format mapping
        self.assertIsInstance(result["invalid:subject"], DatabricksMapperError)
        self.assertIn("neither a Witboost user nor a group", str(result["invalid:subject"]))

        # Verify the client was called for the group
        self.mock_account_client.groups.list.assert_called_once_with(filter="displayName eq 'non-existent-group'")
