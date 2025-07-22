from loguru import logger
from msgraph.generated.groups.groups_request_builder import GroupsRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph.graph_service_client import GraphServiceClient

from src.models.databricks.exceptions import AzureGraphClientError


class AzureGraphClient:
    """
    A client to interact with the Microsoft Graph API to find users and groups.
    """

    def __init__(self, graph_service_client: GraphServiceClient):
        """
        Initializes the AzureGraphClient.

        Args:
            graph_service_client: An authenticated Microsoft GraphServiceClient instance.
        """
        self.graph_service_client = graph_service_client

    async def get_user_id(self, mail: str) -> str:
        """
        Retrieves the unique object ID for a user based on their email address.

        Args:
            mail: The email address of the user to find.

        Returns:
            The unique object ID of the user as a string.

        Raises:
            AzureGraphClientError: If no user is found with the specified email address
            or any other API errors or unexpected issues.
        """
        logger.info("Searching for user with mail: {}", mail)
        try:
            query_params = UsersRequestBuilder.UsersRequestBuilderGetQueryParameters(
                filter=f"mail eq '{mail}'",
                select=["id"],  # Select only the ID field to be efficient
            )
            request_configuration = UsersRequestBuilder.UsersRequestBuilderGetRequestConfiguration(
                query_parameters=query_params,
            )
            response = await self.graph_service_client.users.get(request_configuration=request_configuration)

            if not response or not response.value or len(response.value) == 0 or not response.value[0].id:
                error_msg = f"User {mail} not found on the configured Azure tenant"
                logger.error(error_msg)
                raise AzureGraphClientError(error_msg)

            return response.value[0].id
        except AzureGraphClientError:
            raise
        except Exception as e:
            error_msg = f"An unexpected error occurred while getting user ID for {mail}."
            logger.error("An unexpected error occurred while getting user ID for {}. Details: {}", mail, e)
            raise AzureGraphClientError(error_msg) from e

    async def get_group_id(self, group_name: str) -> str:
        """
        Retrieves the unique object ID for a group based on its display name.

        Args:
            group_name: The display name of the group to find.

        Returns:
            The unique object ID of the group as a string.

        Raises:
            AzureGraphClientError: If no group is found with the specified display name
            or any other API errors or unexpected issues.
        """
        logger.info("Searching for group with name: {}", group_name)
        try:
            query_params = GroupsRequestBuilder.GroupsRequestBuilderGetQueryParameters(
                filter=f"displayName eq '{group_name}'",
                select=["id"],  # Select only the ID field
            )
            request_configuration = GroupsRequestBuilder.GroupsRequestBuilderGetRequestConfiguration(
                query_parameters=query_params,
            )
            response = await self.graph_service_client.groups.get(request_configuration=request_configuration)

            if not response or not response.value or len(response.value) == 0 or not response.value[0].id:
                error_msg = f"Group {group_name} not found on the configured Azure tenant"
                logger.error(error_msg)
                raise AzureGraphClientError(error_msg)

            return response.value[0].id
        except AzureGraphClientError:
            raise
        except Exception as e:
            error_msg = f"An unexpected error occurred while getting group ID for {group_name}."
            logger.error("An unexpected error occurred while getting group ID for {}. Details: {}", group_name, e)
            raise AzureGraphClientError(error_msg) from e
