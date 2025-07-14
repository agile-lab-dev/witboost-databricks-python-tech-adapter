from typing import Annotated, Tuple

import yaml
from azure.identity import DefaultAzureCredential
from azure.mgmt.databricks import AzureDatabricksManagementClient
from fastapi import BackgroundTasks, Depends

from src import settings
from src.models.api_models import (
    DescriptorKind,
    ProvisioningRequest,
    RequestValidationError,
    UpdateAclRequest,
)
from src.models.data_product_descriptor import DataProduct
from src.service.clients.azure.azure_workspace_handler import WorkspaceHandler
from src.service.clients.azure.azure_workspace_manager import AzureWorkspaceManager
from src.service.clients.databricks.account_client import get_account_client
from src.service.provision.handler.dlt_workload_handler import DLTWorkloadHandler
from src.service.provision.handler.job_workload_handler import JobWorkloadHandler
from src.service.provision.handler.output_port_handler import OutputPortHandler
from src.service.provision.handler.workflow_workload_handler import WorkflowWorkloadHandler
from src.service.provision.provision_service import ProvisionService
from src.service.provision.task_repository import MemoryTaskRepository, get_task_repository
from src.service.provision.update_acl_service import UpdateAclService
from src.service.reverse_provision.output_port_reverse_provision_handler import OutputPortReverseProvisionHandler
from src.service.reverse_provision.reverse_provision_service import ReverseProvisionService
from src.service.reverse_provision.workflow_reverse_provision_handler import WorkflowReverseProvisionHandler
from src.utility.error_builder import build_request_validation_error
from src.utility.parsing_pydantic_models import parse_yaml_with_model


def unpack_provisioning_request(
    provisioning_request: ProvisioningRequest,
) -> Tuple[DataProduct, str, bool] | RequestValidationError:
    """
    Unpacks a Provisioning Request.

    This function takes a `ProvisioningRequest` object and extracts relevant information
    to perform provisioning or unprovisioning for a data product component.

    Args:
        provisioning_request (ProvisioningRequest): The request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, str, bool], RequestValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product for provisioning.
                - `str`: The component ID to provision.
                - `bool`: The value of the removeData field.
            - If unsuccessful, returns a `RequestValidationError` object with error details.

    Note:
        - This function expects the `provisioning_request` to have a descriptor kind of `DescriptorKind.COMPONENT_DESCRIPTOR`.
        - It will attempt to parse the descriptor and return the relevant information. If parsing fails or the descriptor kind is unexpected, a `RequestValidationError` will be returned.

    """  # noqa: E501

    if not provisioning_request.descriptorKind == DescriptorKind.COMPONENT_DESCRIPTOR:
        error = (
            "Expecting a COMPONENT_DESCRIPTOR but got a "
            f"{provisioning_request.descriptorKind} instead; please check with the "
            f"platform team."
        )
        return build_request_validation_error(problems=[error])
    try:
        descriptor_dict = yaml.safe_load(provisioning_request.descriptor)
        data_product = parse_yaml_with_model(descriptor_dict.get("dataProduct"), DataProduct)
        component_to_provision = descriptor_dict.get("componentIdToProvision")
        remove_data = provisioning_request.removeData if provisioning_request.removeData is not None else False

        if isinstance(data_product, DataProduct):
            return data_product, component_to_provision, remove_data
        elif isinstance(data_product, RequestValidationError):
            return data_product

        else:
            return build_request_validation_error(
                problems=[
                    "An unexpected error occurred while parsing the provisioning request."  # noqa: E501
                ]
            )

    except Exception as ex:
        return build_request_validation_error(problems=["Unable to parse the descriptor.", str(ex)])


UnpackedProvisioningRequestDep = Annotated[
    Tuple[DataProduct, str, bool] | RequestValidationError,
    Depends(unpack_provisioning_request),
]


def unpack_update_acl_request(
    update_acl_request: UpdateAclRequest,
) -> Tuple[DataProduct, str, list[str]] | RequestValidationError:
    """
    Unpacks an Update ACL Request.

    This function takes an `UpdateAclRequest` object and extracts relevant information
    to update access control lists (ACL) for a data product.

    Args:
        update_acl_request (UpdateAclRequest): The update ACL request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, str, List[str]], RequestValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product to update ACL for.
                - `str`: The component ID to provision.
                - `List[str]`: A list of references.
            - If unsuccessful, returns a `RequestValidationError` object with error details.

    Note:
        This function expects the `update_acl_request` to contain a valid YAML string
        in the 'provisionInfo.request' field. It will attempt to parse the YAML and
        return the relevant information. If parsing fails, a `RequestValidationError` will
        be returned.

    """  # noqa: E501

    try:
        request = yaml.safe_load(update_acl_request.provisionInfo.request)
        data_product = parse_yaml_with_model(request.get("dataProduct"), DataProduct)
        component_to_provision = request.get("componentIdToProvision")
        if isinstance(data_product, DataProduct):
            return (
                data_product,
                component_to_provision,
                update_acl_request.refs,
            )
        elif isinstance(data_product, RequestValidationError):
            return data_product
        else:
            return build_request_validation_error(
                problems=["An unexpected error occurred while parsing the update acl request."]
            )
    except Exception as ex:
        return build_request_validation_error(problems=["Unable to parse the descriptor.", str(ex)])


UnpackedUpdateAclRequestDep = Annotated[
    Tuple[DataProduct, str, list[str]] | RequestValidationError,
    Depends(unpack_update_acl_request),
]


def get_workspace_handler() -> WorkspaceHandler:
    azure_workspace_manager = AzureWorkspaceManager(
        AzureDatabricksManagementClient(
            credential=DefaultAzureCredential(),  # type:ignore[arg-type]
            subscription_id=settings.azure.auth.subscription_id,
        )
    )
    workspace_handler = WorkspaceHandler(azure_workspace_manager)
    return workspace_handler


WorkspaceHandlerDep = Annotated[WorkspaceHandler, Depends(get_workspace_handler)]


def create_provision_service(
    background_tasks: BackgroundTasks,
    task_repository: Annotated[MemoryTaskRepository, Depends(get_task_repository)],
    workspace_handler: WorkspaceHandlerDep,
) -> ProvisionService:
    account_client = get_account_client(settings)

    return ProvisionService(
        workspace_handler,
        JobWorkloadHandler(account_client),
        WorkflowWorkloadHandler(account_client),
        DLTWorkloadHandler(account_client),
        OutputPortHandler(account_client),
        task_repository,
        background_tasks,
    )


ProvisionServiceDep = Annotated[ProvisionService, Depends(create_provision_service)]


def create_reverse_provision_service(workspace_handler: WorkspaceHandlerDep) -> ReverseProvisionService:
    account_client = get_account_client(settings)
    return ReverseProvisionService(
        WorkflowReverseProvisionHandler(account_client, workspace_handler),
        OutputPortReverseProvisionHandler(workspace_handler),
    )


ReverseProvisionServiceDep = Annotated[ReverseProvisionService, Depends(create_reverse_provision_service)]


def create_update_acl_service(workspace_handler: WorkspaceHandlerDep) -> UpdateAclService:
    account_client = get_account_client(settings)
    return UpdateAclService(workspace_handler, OutputPortHandler(account_client))


UpdateAclServiceDep = Annotated[UpdateAclService, Depends(create_update_acl_service)]
