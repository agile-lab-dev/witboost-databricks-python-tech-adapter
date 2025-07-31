import threading
from functools import lru_cache
from typing import Optional, Tuple

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service.iam import ServicePrincipal
from loguru import logger

from src.models.databricks.exceptions import DatabricksWorkspaceManagerError
from src.settings.databricks_tech_adapter_settings import GitSettings


class WorkspaceManager:
    """
    Manages workspace-level operations for a Databricks workspace.

    This includes retrieving cluster and SQL warehouse details, handling Git
    credentials, and managing service principals.
    """

    def __init__(self, workspace_client: WorkspaceClient, account_client: AccountClient):
        self.workspace_client = workspace_client
        self.account_client = account_client
        # A lock to replicate the 'synchronized' behavior of set_git_credentials
        self._git_lock = threading.Lock()

    @lru_cache(maxsize=1)
    def get_workspace_name(self) -> str:
        """
        Retrieves the name of the workspace associated with the current configuration.

        The result is cached to avoid repeated API calls.

        Returns:
            The name of the workspace.

        Raises:
            WorkspaceManagerError: If the workspace cannot be found or an API error occurs.
        """
        workspace_host = self.workspace_client.config.host
        if not workspace_host:
            raise DatabricksWorkspaceManagerError("Workspace client host is not configured.")

        logger.debug("Searching for workspace with host: {}", workspace_host)
        try:
            for workspaces in self.account_client.workspaces.list():
                expected_host = f"https://{workspaces.deployment_name}.azuredatabricks.net"
                if expected_host.lower() == workspace_host.lower() and workspaces.workspace_name:
                    logger.info("Found workspace '{}' for host {}", workspaces.workspace_name, workspace_host)
                    return workspaces.workspace_name
            error_msg = f"No workspace found for host: {workspace_host}"
            logger.error(error_msg)
            raise DatabricksWorkspaceManagerError(error_msg)
        except Exception as e:
            logger.error("Failed to list Databricks workspaces")
            raise DatabricksWorkspaceManagerError(f"Failed to list Databricks workspaces: {e}") from e

    def get_sql_warehouse_id_from_name(self, sql_warehouse_name: str) -> str:
        """
        Retrieves the SQL Warehouse ID corresponding to the given name.

        Args:
            sql_warehouse_name: The name of the SQL Warehouse to find.

        Returns:
            The ID of the SQL Warehouse.

        Raises:
            WorkspaceManagerError: If the warehouse is not found or an API error occurs.
        """
        try:
            for warehouse in self.workspace_client.warehouses.list():
                if warehouse.name and warehouse.id and warehouse.name.lower() == sql_warehouse_name.lower():
                    logger.info("SQL Warehouse '{}' found. ID: {}", sql_warehouse_name, warehouse.id)
                    return warehouse.id
            error_msg = f"SQL Warehouse '{sql_warehouse_name}' not found in workspace '{self.get_workspace_name()}'"
            logger.error(error_msg)
            raise DatabricksWorkspaceManagerError(error_msg)
        except Exception as e:
            logger.error("Failed to list SQL warehouses")
            raise DatabricksWorkspaceManagerError(f"Failed to list SQL warehouses: {e}") from e

    def get_compute_cluster_id_from_name(self, cluster_name: str) -> str:
        """
        Retrieves the compute cluster ID from its name in the current workspace.

        Args:
            cluster_name: The name of the compute cluster to find.

        Returns:
            The ID of the compute cluster.

        Raises:
            WorkspaceManagerError: If the cluster is not found or an API error occurs.
        """
        try:
            for cluster in self.workspace_client.clusters.list():
                if cluster.cluster_id and cluster.cluster_name and cluster.cluster_name.lower() == cluster_name.lower():
                    logger.info("Cluster '{}' found. ID: '{}'", cluster_name, cluster.cluster_id)
                    return cluster.cluster_id
            error_msg = f"Cluster '{cluster_name}' not found in workspace '{self.get_workspace_name()}'"
            logger.error(error_msg)
            raise DatabricksWorkspaceManagerError(error_msg)
        except Exception as e:
            logger.error("Failed to list compute clusters")
            raise DatabricksWorkspaceManagerError(f"Failed to list compute clusters: {e}") from e

    def set_git_credentials(self, git_config: GitSettings) -> None:
        """
        Sets or updates Git credentials for the workspace.

        This method is thread-safe.

        Args:
            git_config: A dictionary containing 'username', 'token', and 'provider'.

        Raises:
            WorkspaceManagerError: If the operation fails.
        """
        with self._git_lock:
            try:
                workspace_name = self.get_workspace_name()
                username = git_config.username
                token = git_config.token
                provider = git_config.provider

                logger.info(
                    "Setting {} credentials on workspace '{}' with username {}", provider, workspace_name, username
                )

                existing_credential = None
                credentials_list = self.workspace_client.git_credentials.list()
                if credentials_list:
                    for cred in credentials_list:
                        if cred.git_provider and cred.git_provider.lower() == provider.lower():
                            existing_credential = cred
                            break

                if existing_credential:
                    logger.warning(
                        "Credentials for {} already exist in workspace '{}'. Updating them.", provider, workspace_name
                    )
                    self.workspace_client.git_credentials.update(
                        credential_id=existing_credential.credential_id,
                        git_username=username,
                        personal_access_token=token,
                        git_provider=provider,
                    )
                else:
                    logger.info("Creating new Git credentials for provider {}.", provider)
                    self.workspace_client.git_credentials.create(
                        personal_access_token=token,
                        git_username=username,
                        git_provider=provider,
                    )

                logger.info(
                    "Git credentials successfully set on workspace '{}' with username {}", workspace_name, username
                )
            except Exception as e:
                logger.error(f"Error setting Git credentials in workspace '{self.get_workspace_name()}'")
                raise DatabricksWorkspaceManagerError(
                    f"Error setting Git credentials in workspace '{self.get_workspace_name()}': {e}"
                ) from e

    def get_service_principal_from_name(self, principal_name: str) -> Optional[ServicePrincipal]:
        """
        Retrieves a service principal by its display name from the workspace.

        Args:
            principal_name: The display name of the service principal.

        Returns:
            The found ServicePrincipal object or None if Service Principal doesn't exist

        Raises:
            WorkspaceManagerError: If the an API error occurs.
        """
        try:
            workspace_name = self.get_workspace_name()
            logger.info("Looking up service principal '{}' in workspace '{}'", principal_name, workspace_name)

            principals = self.workspace_client.service_principals.list()

            for sp in principals:
                if sp.display_name and sp.display_name.strip().lower() == principal_name.strip().lower():
                    logger.info("Found principal with Application ID '{}' for '{}'", sp.application_id, principal_name)
                    return sp

            error_msg = f"No principal named '{principal_name}' found in workspace '{workspace_name}'"
            logger.error(error_msg)
            return None
        except DatabricksWorkspaceManagerError:
            raise
        except Exception as e:
            logger.error("Error getting info for service principal '{}'", principal_name)
            raise DatabricksWorkspaceManagerError(
                f"Error getting info for service principal '{principal_name}': {e}"
            ) from e

    def get_service_principal(self, application_id: str) -> Optional[ServicePrincipal]:
        """
        Retrieves a service principal by its display name from the workspace.

        Args:
            application_id: The display name of the service principal.

        Returns:
            The found ServicePrincipal object or None if Service Principal doesn't exist

        Raises:
            WorkspaceManagerError: If an API error occurs.
        """
        try:
            workspace_name = self.get_workspace_name()
            logger.info("Looking up service principal '{}' in workspace '{}'", application_id, workspace_name)

            principals = self.workspace_client.service_principals.list()

            for sp in principals:
                if sp.application_id and sp.application_id.strip().lower() == application_id.strip().lower():
                    logger.info("Found principal with name '{}' for '{}'", sp.display_name, application_id)
                    return sp

            error_msg = f"No principal with ID '{application_id}' found in workspace '{workspace_name}'"
            logger.error(error_msg)
            return None
        except DatabricksWorkspaceManagerError:
            raise
        except Exception as e:
            logger.error("Error getting info for service principal '{}': {}", application_id, e)
            raise DatabricksWorkspaceManagerError(f"Error getting info for service principal '{application_id}'") from e

    def generate_secret_for_service_principal(
        self, service_principal_id: int, lifetime_seconds: int
    ) -> Tuple[str, str]:
        """
        Generates a secret for a given service principal.

        Args:
            service_principal_id: The ID of the service principal.
            lifetime_seconds: The validity duration of the secret in seconds.

        Returns:
            A tuple containing the secret ID and the secret value.

        Raises:
            WorkspaceManagerError: If secret generation fails.
        """
        try:
            logger.info(
                "Generating secret for service principal '{}' with lifetime {}s", service_principal_id, lifetime_seconds
            )
            response = self.account_client.service_principal_secrets.create(
                service_principal_id=service_principal_id,
                lifetime=f"{lifetime_seconds}s",  # API requires a string representing seconds
            )
            if response.id and response.secret:
                logger.info("Successfully generated secret for service principal '{}'", service_principal_id)
                return response.id, response.secret
            else:
                error_msg = (
                    f"Error, received empty response when generating "
                    f"a secret for service principal '{service_principal_id}'"
                )
                logger.error(error_msg)
                logger.debug("Received response: {}", response)
                raise DatabricksWorkspaceManagerError(error_msg)
        except Exception as e:
            logger.error("Failed to generate secret for service principal '{}'", service_principal_id)
            raise DatabricksWorkspaceManagerError(
                f"Failed to generate secret for service principal '{service_principal_id}': {e}"
            ) from e

    def delete_service_principal_secret(self, service_principal_id: int, secret_id: str) -> None:
        """
        Deletes a secret from a specified service principal.

        Args:
            service_principal_id: The ID of the service principal.
            secret_id: The ID of the secret to delete.

        Raises:
            WorkspaceManagerError: If secret deletion fails.
        """
        try:
            logger.info("Deleting secret {} for service principal '{}'", secret_id, service_principal_id)
            self.account_client.service_principal_secrets.delete(
                service_principal_id=service_principal_id, secret_id=secret_id
            )
            logger.info("Successfully deleted secret {} for service principal {}", secret_id, service_principal_id)
        except Exception as e:
            logger.error("Failed to delete secret {} for service principal {}", secret_id, service_principal_id)
            raise DatabricksWorkspaceManagerError(
                f"Failed to delete secret {secret_id} for service principal {service_principal_id}: {e}"
            ) from e
