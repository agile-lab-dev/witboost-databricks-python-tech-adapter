import os
from typing import List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, ResourceAlreadyExists, ResourceDoesNotExist
from databricks.sdk.service.workspace import (
    ObjectType,
    RepoAccessControlRequest,
    RepoAccessControlResponse,
    RepoPermissionLevel,
)
from loguru import logger

from src.models.databricks.exceptions import RepoManagerError


class RepoManager:
    """Manages repository operations in a Databricks workspace."""

    def __init__(self, workspace_client: WorkspaceClient, workspace_name: str):
        self.workspace_client = workspace_client
        self.workspace_name = workspace_name

    def create_repo(self, git_url: str, provider: str, absolute_repo_path: str) -> int:
        """
        Creates a new repository in the Databricks workspace.

        If the repository already exists, it retrieves and returns its ID instead of failing.

        Args:
            git_url: The URL of the Git repository.
            provider: The Git provider (e.g., 'GITLAB', 'GITHUB').
            absolute_repo_path: The absolute path for the repository in the workspace.

        Returns:
            The ID of the created or existing repository.

        Raises:
            RepoManagerError: If creation fails for a reason other than the repo already existing,
                              or if the existing repo cannot be found.
        """
        try:
            logger.info("Creating repo with URL {} at {} in {}", git_url, absolute_repo_path, self.workspace_name)
            repo_info = self.workspace_client.repos.create(
                url=git_url,
                provider=provider.upper(),
                path=absolute_repo_path,
            )
            if repo_info.id:
                logger.info(
                    "Repo with URL {} created successfully at {} in {}",
                    git_url,
                    absolute_repo_path,
                    self.workspace_name,
                )
                return repo_info.id

            error_msg = (
                f"An unexpected error occurred creating repo {git_url} in {self.workspace_name}: "
                f"Response has empty ID"
            )
            logger.error(error_msg)
            raise RepoManagerError(error_msg)
        except ResourceAlreadyExists:
            logger.info(f"Repo {git_url} in {self.workspace_name} already exists, handling...")
            return self._handle_existing_repository(git_url, absolute_repo_path)
        except BadRequest as e:
            if "RESOURCE_ALREADY_EXISTS" in str(e):
                return self._handle_existing_repository(git_url, absolute_repo_path)

            error_msg = f"Error creating repo {git_url} in {self.workspace_name}: {e}"
            logger.error(error_msg)
            raise RepoManagerError(error_msg) from e
        except Exception as e:
            error_msg = f"An unexpected error occurred creating repo {git_url} in {self.workspace_name}: {e}"
            logger.error(error_msg)
            raise RepoManagerError(error_msg) from e

    def _handle_existing_repository(self, git_url: str, absolute_repo_path: str) -> int:
        """
        Finds and returns the ID of a repository that is known to exist.
        """
        logger.warning(
            "Creation of repo with URL {} skipped. It already exists in the folder {} (workspace {}).",
            git_url,
            absolute_repo_path,
            self.workspace_name,
        )

        parent_folder = os.path.dirname(absolute_repo_path)
        if not parent_folder:
            error_msg = f"Cannot determine parent folder for repo path {absolute_repo_path}"
            logger.error(error_msg)
            raise RepoManagerError(error_msg)

        folder_contents = self.workspace_client.workspace.list(parent_folder)
        for item in folder_contents:
            if item.object_id and item.object_type == ObjectType.REPO:
                repo_info = self.workspace_client.repos.get(item.object_id)
                if repo_info.id and repo_info.path and repo_info.path.lower() == absolute_repo_path.lower():
                    return repo_info.id

        error_msg = f"Repo at {absolute_repo_path} exists but could not be retrieved."
        logger.error(error_msg)
        raise RepoManagerError(error_msg)

    def delete_repo(self, git_url: str, absolute_repo_path: str) -> None:
        """
        Deletes a repository from the Databricks workspace.

        If the repository does not exist, the operation is considered successful and completes silently.

        Args:
            git_url: The URL of the Git repository to match.
            absolute_repo_path: The absolute path of the repository to delete.

        Raises:
            RepoManagerError: If deletion fails for an unexpected reason.
        """
        try:
            logger.info(
                "Attempting to delete repo with path {} and URL {} in {}",
                absolute_repo_path,
                git_url,
                self.workspace_name,
            )
            parent_folder = os.path.dirname(absolute_repo_path)
            if not parent_folder:
                error_msg = f"Cannot determine parent folder for repo path {absolute_repo_path}"
                logger.error(error_msg)
                raise RepoManagerError(error_msg)

            folder_contents = self.workspace_client.workspace.list(parent_folder)
            for item in folder_contents:
                if item.object_id and item.object_type == ObjectType.REPO:
                    repo_info = self.workspace_client.repos.get(item.object_id)
                    if repo_info.id and repo_info.url and repo_info.url.lower() == git_url.lower():
                        self.workspace_client.repos.delete(repo_id=repo_info.id)
                        logger.info(
                            "Repo {} (ID: {}) in {} deleted successfully.",
                            absolute_repo_path,
                            repo_info.id,
                            self.workspace_name,
                        )
                        return
        except ResourceDoesNotExist:
            logger.info("Repo at path {} not found. Deletion skipped.", absolute_repo_path)
        except Exception as e:
            error_msg = f"Error deleting repo at {absolute_repo_path} in {self.workspace_name}: {e}"
            logger.error(error_msg)
            raise RepoManagerError(error_msg) from e

    def assign_permissions_to_user(self, repo_id: str, username: str, permission_level: RepoPermissionLevel) -> None:
        self._assign_permissions(repo_id, permission_level, username=username)

    def remove_permissions_from_user(self, repo_id: str, username: str) -> None:
        self._remove_permissions(repo_id, username=username)

    def assign_permissions_to_group(self, repo_id: str, group_name: str, permission_level: RepoPermissionLevel) -> None:
        self._assign_permissions(repo_id, permission_level, group_name=group_name)

    def remove_permissions_from_group(self, repo_id: str, group_name: str) -> None:
        self._remove_permissions(repo_id, group_name=group_name)

    def _convert_to_access_control_requests(
        self, responses: List[RepoAccessControlResponse]
    ) -> List[RepoAccessControlRequest]:
        """Converts a list of permission responses to requests."""
        if not responses:
            return []

        requests = []
        for res in responses:
            # A principal can have multiple permission levels; we take the first as per the original logic.
            if res.all_permissions and res.all_permissions[0].permission_level:
                requests.append(
                    RepoAccessControlRequest(
                        user_name=res.user_name,
                        group_name=res.group_name,
                        service_principal_name=res.service_principal_name,
                        permission_level=res.all_permissions[0].permission_level,
                    )
                )
            else:
                error_msg = "Error while mapping repository permissions. Repository has no permission levels"
                logger.error(error_msg)
                logger.debug(
                    "Error raised when mapping permissions {} present on access control list {}", res, responses
                )
                raise RepoManagerError(error_msg)
        return requests

    def _assign_permissions(
        self,
        repo_id: str,
        permission_level: RepoPermissionLevel,
        *,
        username: Optional[str] = None,
        group_name: Optional[str] = None,
    ) -> None:
        principal_name = username or group_name
        if not principal_name:
            raise ValueError("You must provide either a username or a group name as parameter for assign permissions")
        try:
            logger.info(
                "Assigning permission {} to {} for repo {} in '{}'",
                permission_level.value,
                principal_name,
                repo_id,
                self.workspace_name,
            )

            current_permissions = self.workspace_client.repos.get_permissions(repo_id)
            access_control_list = self._convert_to_access_control_requests(
                current_permissions.access_control_list or []
            )

            # Add the new permission
            access_control_list.append(
                RepoAccessControlRequest(user_name=username, group_name=group_name, permission_level=permission_level)
            )

            self.workspace_client.repos.set_permissions(repo_id=repo_id, access_control_list=access_control_list)
            logger.info("Successfully assigned permissions to {} for repo {}", principal_name, repo_id)
        except Exception as e:
            error_msg = f"Error assigning permission {permission_level.value} to {principal_name} for repo {repo_id}"
            logger.error(error_msg)
            raise RepoManagerError(
                f"Error assigning permission {permission_level.value} to "
                f"{principal_name} for repo {repo_id}. Details: {e}"
            ) from e

    def _remove_permissions(
        self, repo_id: str, *, username: Optional[str] = None, group_name: Optional[str] = None
    ) -> None:
        principal_name = username or group_name
        if not principal_name:
            raise ValueError("You must provide either a username or a group name as parameter for remove permissions")
        try:
            logger.info("Removing permissions for {} from repo {} in {}", principal_name, repo_id, self.workspace_name)

            current_permissions = self.workspace_client.repos.get_permissions(repo_id)
            access_control_list = self._convert_to_access_control_requests(
                current_permissions.access_control_list or []
            )

            # Create a new list excluding the principal to be removed
            updated_acl = [
                req
                for req in access_control_list
                if not (username and req.user_name == username) and not (group_name and req.group_name == group_name)
            ]

            # If the list is unchanged, there was nothing to remove
            if len(updated_acl) == len(access_control_list):
                logger.info("No permissions found for {} on repo {}. Nothing to remove.", principal_name, repo_id)
                return

            self.workspace_client.repos.set_permissions(repo_id=repo_id, access_control_list=updated_acl)
            logger.info("Successfully removed permissions for {} from repo {}", principal_name, repo_id)
        except Exception as e:
            error_msg = f"Error removing permissions for {principal_name} from repo {repo_id}"
            logger.error(error_msg)
            raise RepoManagerError(
                f"Error removing permissions for {principal_name} from repo {repo_id}. Details: {e}"
            ) from e
