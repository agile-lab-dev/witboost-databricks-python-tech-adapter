from typing import Annotated, Tuple

import yaml
from azure.identity import DefaultAzureCredential
from azure.mgmt.databricks import AzureDatabricksManagementClient
from fastapi import BackgroundTasks, Depends

from src import settings
from src.models.api_models import (
    DescriptorKind,
    ProvisioningRequest,
    UpdateAclRequest,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.service.clients.azure.azure_workspace_handler import WorkspaceHandler
from src.service.clients.azure.azure_workspace_manager import AzureWorkspaceManager
from src.service.clients.databricks.account_client import get_account_client
from src.service.provision.handler.job_workload_handler import JobWorkloadHandler
from src.service.provision.provision_service import ProvisionService
from src.service.provision.task_repository import MemoryTaskRepository, get_task_repository
from src.utility.parsing_pydantic_models import parse_yaml_with_model


async def unpack_provisioning_request(
    provisioning_request: ProvisioningRequest,
) -> Tuple[DataProduct, str, bool] | ValidationError:
    """
    Unpacks a Provisioning Request.

    This function takes a `ProvisioningRequest` object and extracts relevant information
    to perform provisioning or unprovisioning for a data product component.

    Args:
        provisioning_request (ProvisioningRequest): The request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, str, bool], ValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product for provisioning.
                - `str`: The component ID to provision.
                - `bool`: The value of the removeData field.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        - This function expects the `provisioning_request` to have a descriptor kind of `DescriptorKind.COMPONENT_DESCRIPTOR`.
        - It will attempt to parse the descriptor and return the relevant information. If parsing fails or the descriptor kind is unexpected, a `ValidationError` will be returned.

    """  # noqa: E501

    if not provisioning_request.descriptorKind == DescriptorKind.COMPONENT_DESCRIPTOR:
        error = (
            "Expecting a COMPONENT_DESCRIPTOR but got a "
            f"{provisioning_request.descriptorKind} instead; please check with the "
            f"platform team."
        )
        return ValidationError(errors=[error])
    try:
        descriptor_dict = yaml.safe_load(provisioning_request.descriptor)
        data_product = parse_yaml_with_model(descriptor_dict.get("dataProduct"), DataProduct)
        component_to_provision = descriptor_dict.get("componentIdToProvision")
        remove_data = provisioning_request.removeData if provisioning_request.removeData is not None else False

        if isinstance(data_product, DataProduct):
            return data_product, component_to_provision, remove_data
        elif isinstance(data_product, ValidationError):
            return data_product

        else:
            return ValidationError(
                errors=[
                    "An unexpected error occurred while parsing the provisioning request."  # noqa: E501
                ]
            )

    except Exception as ex:
        return ValidationError(errors=["Unable to parse the descriptor.", str(ex)])


UnpackedProvisioningRequestDep = Annotated[
    Tuple[DataProduct, str, bool] | ValidationError,
    Depends(unpack_provisioning_request),
]


async def unpack_update_acl_request(
    update_acl_request: UpdateAclRequest,
) -> Tuple[DataProduct, str, list[str]] | ValidationError:
    """
    Unpacks an Update ACL Request.

    This function takes an `UpdateAclRequest` object and extracts relevant information
    to update access control lists (ACL) for a data product.

    Args:
        update_acl_request (UpdateAclRequest): The update ACL request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, str, List[str]], ValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product to update ACL for.
                - `str`: The component ID to provision.
                - `List[str]`: A list of references.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        This function expects the `update_acl_request` to contain a valid YAML string
        in the 'provisionInfo.request' field. It will attempt to parse the YAML and
        return the relevant information. If parsing fails, a `ValidationError` will
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
        elif isinstance(data_product, ValidationError):
            return data_product
        else:
            return ValidationError(errors=["An unexpected error occurred while parsing the update acl request."])
    except Exception as ex:
        return ValidationError(errors=["Unable to parse the descriptor.", str(ex)])


UnpackedUpdateAclRequestDep = Annotated[
    Tuple[DataProduct, str, list[str]] | ValidationError,
    Depends(unpack_update_acl_request),
]


async def create_provision_service(
    background_tasks: BackgroundTasks, task_repository: Annotated[MemoryTaskRepository, Depends(get_task_repository)]
) -> ProvisionService:
    account_client = get_account_client(settings)
    azure_workspace_manager = AzureWorkspaceManager(
        AzureDatabricksManagementClient(
            credential=DefaultAzureCredential(),  # type:ignore[arg-type]
            subscription_id=settings.azure.auth.subscription_id,
        )
    )
    workspace_handler = WorkspaceHandler(azure_workspace_manager)

    return ProvisionService(workspace_handler, JobWorkloadHandler(account_client), task_repository, background_tasks)


ProvisionServiceDep = Annotated[ProvisionService, Depends(create_provision_service)]
