from typing import Annotated, Tuple

import pydantic
from fastapi import Depends
from loguru import logger

from src import settings
from src.dependencies import UnpackedProvisioningRequestDep
from src.models.api_models import ValidationError
from src.models.data_product_descriptor import Component, ComponentKind, DataProduct
from src.models.databricks.databricks_models import DatabricksComponent, JobWorkload
from src.utility.use_case_template_id_utils import get_use_case_template_id


def _parse_component(component: Component) -> DatabricksComponent:
    use_case_template_id = get_use_case_template_id(component.useCaseTemplateId)
    if use_case_template_id in settings.usecasetemplateid.workload.job:
        return JobWorkload.parse_obj(component.dict(by_alias=True))
    else:
        raise NotImplementedError("Other components are not yet supported by this Tech Adapter")  # TODO


def validate(
    request: UnpackedProvisioningRequestDep,
) -> Tuple[DataProduct, DatabricksComponent, bool] | ValidationError:
    if isinstance(request, ValidationError):
        return request

    data_product, component_id, remove_data = request

    try:
        component_to_provision = data_product.get_component_by_id(component_id)
        if component_to_provision is None:
            error_msg = f"Component with ID {component_id} not found in descriptor"
            logger.error(error_msg)
            return ValidationError(errors=[error_msg])
        component_kind_to_provision = component_to_provision.kind
        if component_kind_to_provision == ComponentKind.WORKLOAD:
            logger.info("Parsing Workload Component [id {}]", component_id)
            try:
                component_to_provision = _parse_component(component_to_provision)
            except Exception as e:
                return ValidationError(errors=[f"Failed to parse or validate workload component {component_id}: {e}"])

        elif component_kind_to_provision == ComponentKind.OUTPUTPORT:
            return ValidationError(errors=["Output Port is not yet supported"])  # TODO
        else:
            error_message = "The kind '{}' of the component to provision is not supported by this Tech Adapter"
            logger.error(error_message, component_kind_to_provision)
            return ValidationError(errors=[error_message])
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
        return ValidationError(errors=combined)

    return data_product, component_to_provision, remove_data


ValidatedDatabricksComponentDep = Annotated[
    Tuple[DataProduct, DatabricksComponent, bool] | ValidationError,
    Depends(validate),
]
