from typing import Dict, Mapping, Set, Union

from databricks.sdk import AccountClient
from loguru import logger

from src.models.databricks.exceptions import DatabricksMapperError
from src.service.principals_mapping.mapper import Mapper

# Constants for subject prefixes
USER_PREFIX = "user:"
GROUP_PREFIX = "group:"


class DatabricksMapper(Mapper):
    """
    Maps Witboost user and group principals to their corresponding Databricks-compatible names.
    - Users are converted from a specific format to an email address.
    - Groups are resolved to their case-sensitive display name in the Databricks account.
    """

    def __init__(self, account_client: AccountClient):
        self.account_client = account_client

    def retrieve_case_sensitive_group_display_name(self, group_name_case_insensitive: str) -> str:
        """
        Finds the exact, case-sensitive display name for a group in Databricks.

        Databricks group display names are case-sensitive, but identity providers might not be.
        This method queries the Databricks Account API with a case-insensitive filter
        to find the single, correct group name.

        Args:
            group_name_case_insensitive: The group name to resolve.

        Returns:
            The case-sensitive group display name as stored in Databricks.

        Raises:
            GroupNotFoundError: If no group with that name is found.
            MultipleGroupsFoundError: If the filter matches more than one group.
            DatabricksError: For any other API-related errors from the SDK.
        """
        logger.debug("Searching for group with case-insensitive name: '{}'", group_name_case_insensitive)

        try:
            groups = list(self.account_client.groups.list(filter=f"displayName eq '{group_name_case_insensitive}'"))
        except Exception as e:
            error_msg = f"Databricks API call failed while retrieving group '{group_name_case_insensitive}': {e}"
            logger.error(error_msg)
            raise DatabricksMapperError(error_msg) from e

        if not groups:
            error_msg = f"Group '{group_name_case_insensitive}' not found at Databricks account level."
            logger.error(error_msg)
            raise DatabricksMapperError(error_msg)

        if len(groups) > 1:
            error_msg = f"More than one group with name '{group_name_case_insensitive}' has been found."
            logger.error(error_msg)
            raise DatabricksMapperError(error_msg)

        group_name_case_sensitive = groups[0].display_name
        if not group_name_case_sensitive:
            error_msg = f"Group '{group_name_case_insensitive}' has been found but doesn't contain a display name."
            logger.error(error_msg)
            raise DatabricksMapperError(error_msg)

        logger.info("Group found: correct displayName is '{}'", group_name_case_sensitive)
        return group_name_case_sensitive

    def map(self, subjects: Set[str]) -> Mapping[str, Union[str, DatabricksMapperError]]:
        """
        Maps a set of subject identifiers to their target representation.

        For each subject, it attempts a mapping. If it fails, the corresponding
        value in the returned dictionary will be the exception object that was raised.

        Args:
            subjects: A set of strings, each prefixed with "user:" or "group:".

        Returns:
            A dictionary mapping each original subject to its successfully mapped
            string or to the Exception object detailing the failure.
        """
        results: Dict[str, Union[str, DatabricksMapperError]] = {}
        for ref in subjects:
            try:
                results[ref] = self._map_subject(ref)
            except DatabricksMapperError as e:
                # Catch our custom, expected errors
                logger.warning("Failed to map subject '{}': {}", ref, e)
                results[ref] = e
        return results

    def _map_subject(self, ref: str) -> str:
        if ref.startswith(USER_PREFIX):
            user_part = ref[len(USER_PREFIX) :]
            mapped_user = self._get_and_map_user(user_part)
            logger.debug("Mapped successfully '{}' to '{}'", ref, mapped_user)
            return mapped_user
        if ref.startswith(GROUP_PREFIX):
            group_part = ref[len(GROUP_PREFIX) :]
            mapped_group = self.retrieve_case_sensitive_group_display_name(group_part)
            logger.debug("Mapped successfully '{}' to '{}'", ref, mapped_group)
            return mapped_group
        error_msg = f"The subject '{ref}' is neither a Witboost user nor a group."
        logger.error(error_msg)
        raise DatabricksMapperError(error_msg)

    def _get_and_map_user(self, user: str) -> str:
        try:
            underscore_index = user.rfind("_")
            if underscore_index == -1:
                return user
            mail = f"{user[:underscore_index]}@{user[underscore_index + 1:]}"
            return mail
        except Exception as e:
            error_msg = f"An unexpected error occurred while mapping the Witboost user '{user}'. Details: {e}"
            logger.error(error_msg)
            raise DatabricksMapperError(error_msg) from e
