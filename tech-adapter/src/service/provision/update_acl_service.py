from typing import List

from loguru import logger

from src.models.api_models import ProvisioningStatus, RequestValidationError, Status1
from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import DatabricksComponent, DatabricksOutputPort
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.models.exceptions import build_error_message_from_chained_exception
from src.service.clients.azure.azure_workspace_handler import AzureWorkspaceHandler
from src.service.clients.databricks.unity_catalog_manager import UnityCatalogManager
from src.service.provision.handler.output_port_handler import OutputPortHandler
from src.utility.error_builder import build_request_validation_error


class UpdateAclService:
    """
    A service responsible for handling ACL update requests for various components.
    """

    def __init__(
        self,
        workspace_handler: AzureWorkspaceHandler,
        output_port_handler: OutputPortHandler,
    ):
        """
        Initializes the UpdateAclService.

        Args:
            workspace_handler: A handler to get workspace clients and info.
            output_port_handler: A handler specifically for Output Port operations.
        """
        self.workspace_handler = workspace_handler
        self.output_port_handler = output_port_handler

    def update_acl(
        self, data_product: DataProduct, component: DatabricksComponent, witboost_users: List[str]
    ) -> ProvisioningStatus | RequestValidationError:
        """
        Updates the Access Control List (ACL) for a specific component.

        This method acts as a dispatcher. It validates the component kind and,
        if supported, retrieves the necessary Databricks clients and delegates
        the ACL update logic to the appropriate handler.

        Args:
            data_product: The parent data product of the component.
            component: The component whose ACL is to be updated.
            witboost_users: The list of user/group references for the new ACL.

        Returns:
            Either a ProvisioningStatus object indicating the outcome of the operation or a
            RequestValidationError.

        """
        if not isinstance(component, DatabricksOutputPort):
            error_msg = (
                f"The component '{component.name}' is not supported for "
                f"updating access control by this Tech Adapter"
            )
            logger.error(error_msg)
            return build_request_validation_error(problems=[error_msg])

        try:
            specific: DatabricksOutputPortSpecific = component.specific
            workspace_name = specific.workspace_op

            workspace_info = self.workspace_handler.get_workspace_info_by_name(workspace_name)
            if not workspace_info:
                error_msg = f"Update Acl failed: Unable to retrieve info for workspace '{workspace_name}'"
                logger.error(error_msg)
                return ProvisioningStatus(status=Status1.FAILED, message=error_msg, result=None)

            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)

            unity_catalog_manager = UnityCatalogManager(workspace_client, workspace_info)

            self.output_port_handler.update_acl(
                data_product=data_product,
                component=component,
                witboost_identities=witboost_users,
                unity_catalog_manager=unity_catalog_manager,
            )
            return ProvisioningStatus(status=Status1.COMPLETED, result="Update of Acl completed!")
        except Exception as e:
            # Catch any unexpected error during the process
            error_msg = build_error_message_from_chained_exception(e)
            logger.exception(
                "Caught exception {} during update ACL of component '{}'",
                e,
                component.id,
            )
            return ProvisioningStatus(status=Status1.FAILED, result=error_msg)
