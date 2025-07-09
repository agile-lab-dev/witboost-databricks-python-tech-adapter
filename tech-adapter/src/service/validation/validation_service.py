from typing import Annotated, Tuple

import pydantic
from fastapi import Depends
from loguru import logger

from src import settings
from src.dependencies import UnpackedProvisioningRequestDep, UnpackedUpdateAclRequestDep, WorkspaceHandlerDep
from src.models.api_models import RequestValidationError
from src.models.data_product_descriptor import Component, ComponentKind, DataProduct
from src.models.databricks.databricks_models import (
    DatabricksComponent,
    DatabricksOutputPort,
    JobWorkload,
    WorkflowWorkload,
)
from src.models.exceptions import get_error_list_from_chained_exception
from src.service.validation.output_port_validation_service import OutputPortValidation
from src.utility.use_case_template_id_utils import get_use_case_template_id


def _parse_component(component: Component) -> DatabricksComponent:
    use_case_template_id = get_use_case_template_id(component.useCaseTemplateId)
    if use_case_template_id in settings.usecasetemplateid.workload.job:
        return JobWorkload.parse_obj(component.dict(by_alias=True))
    elif use_case_template_id in settings.usecasetemplateid.workload.workflow:
        return WorkflowWorkload.parse_obj(component.dict(by_alias=True))
    elif use_case_template_id in settings.usecasetemplateid.outputPort:
        return DatabricksOutputPort.parse_obj(component.dict(by_alias=True))
    else:
        raise NotImplementedError("Other components are not yet supported by this Tech Adapter")  # TODO


def validate(
    request: UnpackedProvisioningRequestDep, workspace_handler: WorkspaceHandlerDep
) -> Tuple[DataProduct, DatabricksComponent, bool] | RequestValidationError:
    if isinstance(request, RequestValidationError):
        return request

    data_product, component_id, remove_data = request

    try:
        component_to_provision = data_product.get_component_by_id(component_id)
        if component_to_provision is None:
            error_msg = f"Component with ID {component_id} not found in descriptor"
            logger.error(error_msg)
            return RequestValidationError(errors=[error_msg])
        component_kind_to_provision = component_to_provision.kind
        if component_kind_to_provision == ComponentKind.WORKLOAD:
            logger.info("Parsing Workload Component [id {}]", component_id)
            try:
                component_to_provision = _parse_component(component_to_provision)
            except pydantic.ValidationError:
                raise
            except Exception as e:
                return RequestValidationError(
                    errors=[f"Failed to parse or validate workload component {component_id}: {e}"]
                )

        elif component_kind_to_provision == ComponentKind.OUTPUTPORT:
            logger.info("Parsing Output Port Component [id {}]", component_id)
            try:
                component_to_provision = _parse_component(component_to_provision)
                if isinstance(component_to_provision, DatabricksOutputPort):
                    op_validation_service = OutputPortValidation(workspace_handler)
                    op_validation_service.validate(component_to_provision, data_product.environment)
                else:
                    error_msg = (
                        "Error while parsing Output Port component, component is of type 'outputport' but body "
                        "is not a valid Databricks Output Port"
                    )
                    logger.error(error_msg)
                    return RequestValidationError(errors=[error_msg])
            except pydantic.ValidationError:
                raise
            except Exception as e:
                logger.exception("Error while parsing or validating Output Port component {}", component_id)
                errors = get_error_list_from_chained_exception(e)
                return RequestValidationError(
                    errors=[f"Failed to parse or validate output port component {component_id}"] + errors
                )
        else:
            error_message = "The kind '{}' of the component to provision is not supported by this Tech Adapter"
            logger.error(error_message, component_kind_to_provision)
            return RequestValidationError(errors=[error_message])
    except pydantic.ValidationError as ve:
        error_msg = f"Failed to parse the component {component_id} as one of the supported component types:"
        logger.exception(error_msg)
        combined = [error_msg]
        combined.extend(
            map(
                str,
                ve.errors(include_url=False, include_context=False, include_input=False),
            )
        )
        return RequestValidationError(errors=combined)

    return data_product, component_to_provision, remove_data


ValidatedDatabricksComponentDep = Annotated[
    Tuple[DataProduct, DatabricksComponent, bool] | RequestValidationError,
    Depends(validate),
]


def validate_update_acl(
    request: UnpackedUpdateAclRequestDep, workspace_handler: WorkspaceHandlerDep
) -> Tuple[DataProduct, DatabricksComponent, list[str]] | RequestValidationError:
    if isinstance(request, RequestValidationError):
        return request
    data_product, component_id, witboost_users = request
    result = validate((data_product, component_id, False), workspace_handler)
    if isinstance(result, RequestValidationError):
        return result
    data_product, databricks_component, _ = result
    return data_product, databricks_component, witboost_users


ValidatedUpdateACLDatabricksComponentDep = Annotated[
    Tuple[DataProduct, DatabricksComponent, list[str]] | RequestValidationError,
    Depends(validate_update_acl),
]
