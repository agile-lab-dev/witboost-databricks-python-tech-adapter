from typing import Dict, Mapping, Set, Union

from loguru import logger

from src.models.databricks.exceptions import AzureMapperError, MapperError
from src.service.clients.azure.azure_graph_client import AzureGraphClient
from src.service.principals_mapping.mapper import AsyncMapper

# Constants for subject prefixes, can be shared or redefined
USER_PREFIX = "user:"
GROUP_PREFIX = "group:"


class AzureMapper(AsyncMapper):
    """
    Maps Witboost user and group principals to their corresponding Azure AD Object IDs.

    - Users are converted from a specific format to an email address and looked up.
    - Groups are looked up by their display name.
    """

    def __init__(self, client: AzureGraphClient):
        """
        Initializes the AzureMapper.

        Args:
            client: An authenticated AzureGraphClient instance.
        """
        self.client = client

    async def map(self, subjects: Set[str]) -> Mapping[str, Union[str, MapperError]]:
        """
        Maps a set of subject identifiers to their Azure AD Object ID.

        For each subject, it attempts a mapping. If it fails, the corresponding
        value in the returned dictionary will be the exception object that was raised.

        Args:
            subjects: A set of strings, each prefixed with "user:" or "group:".

        Returns:
            A dictionary mapping each original subject to its successfully mapped
            Azure Object ID (str) or to the Exception object detailing the failure.
        """
        results: Dict[str, Union[str, MapperError]] = {}
        for ref in subjects:
            try:
                results[ref] = await self._map_subject(ref)
            except AzureMapperError as e:
                logger.warning("Failed to map subject '{}': {}", ref, e)
                results[ref] = e
            except Exception as e:
                logger.warning("Failed to map subject '{}': {}", ref, e)
                results[ref] = AzureMapperError(str(e))
        return results

    async def _map_subject(self, ref: str) -> str:
        """
        Internal helper to map a single subject string.

        Args:
            ref: The subject string to map.

        Returns:
            The mapped Azure Object ID string.

        Raises:
            InvalidSubjectFormatError: If the subject prefix is not recognized.
            AzureGraphClientError: Propagated from the client if lookup fails.
        """
        if ref.startswith(USER_PREFIX):
            user_part = ref[len(USER_PREFIX) :]
            return await self._get_and_map_user(user_part)

        if ref.startswith(GROUP_PREFIX):
            group_part = ref[len(GROUP_PREFIX) :]
            return await self._get_and_map_group(group_part)

        error_msg = f"The subject {ref} is neither a Witboost user nor a group"
        logger.error(error_msg)
        raise AzureMapperError(error_msg)

    async def _get_and_map_user(self, user: str) -> str:
        """
        Converts a Witboost user identifier to an email and retrieves its Azure Object ID.
        """
        underscore_index = user.rfind("_")
        if underscore_index == -1:
            mail = user
        else:
            mail = f"{user[:underscore_index]}@{user[underscore_index + 1:]}"

        return await self.client.get_user_id(mail)

    async def _get_and_map_group(self, group: str) -> str:
        """
        Retrieves the Azure Object ID for a group by its display name.
        """
        return await self.client.get_group_id(group)
