from typing import Optional

from azure.mgmt.databricks.models import ProvisioningState
from pydantic import BaseModel, ConfigDict


class DatabricksWorkspaceInfo(BaseModel):
    """
    Pydantic model representing essential information about a Databricks workspace.
    """

    model_config = ConfigDict(populate_by_name=True)

    name: str
    id: str
    databricks_host: str
    azure_resource_id: Optional[str]
    azure_resource_url: str
    provisioning_state: ProvisioningState

    # This field represents whether the provisioner should actively manage the workspace.
    is_managed: bool

    @classmethod
    def build_managed(
        cls,
        name: str,
        id: str,
        databricks_host: str,
        azure_resource_id: str,
        azure_resource_url: str,
        provisioning_state: ProvisioningState,
    ) -> "DatabricksWorkspaceInfo":
        """
        Factory method to create a managed workspace instance.

        In a managed workspace, the name is distinct from the host, and the
        provisioner is expected to manage users, groups, and permissions.
        """
        return cls(
            name=name,
            id=id,
            databricks_host=databricks_host,
            azure_resource_id=azure_resource_id,
            azure_resource_url=azure_resource_url,
            provisioning_state=provisioning_state,
            is_managed=True,
        )

    @classmethod
    def build_unmanaged(
        cls,
        databricks_host: str,
        id: str,
        azure_resource_url: str,
        provisioning_state: ProvisioningState,
    ) -> "DatabricksWorkspaceInfo":
        """
        Factory method to create an unmanaged workspace instance.

        In an unmanaged workspace, the name defaults to the host URL, and the
        provisioner does not manage users, groups, or permissions.
        """
        return cls(
            name=databricks_host,  # Name is set to the host for unmanaged workspaces
            id=id,
            databricks_host=databricks_host,
            azure_resource_id=None,
            azure_resource_url=azure_resource_url,
            provisioning_state=provisioning_state,
            is_managed=False,
        )
