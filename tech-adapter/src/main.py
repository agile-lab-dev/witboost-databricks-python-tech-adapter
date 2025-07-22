from __future__ import annotations

import uuid

from fastapi import Request
from loguru import logger
from starlette.background import BackgroundTask
from starlette.responses import Response

from src.app_config import app
from src.check_return_type import check_response
from src.dependencies import (
    ProvisionServiceDep,
    ReverseProvisionServiceDep,
    UpdateAclServiceDep,
)
from src.models.api_models import (
    ProvisioningStatus,
    RequestValidationError,
    ReverseProvisioningRequest,
    ReverseProvisioningStatus,
    SystemErr,
    ValidationError,
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
)
from src.service.validation.validation_service import (
    ValidatedDatabricksComponentDep,
    ValidatedUpdateACLDatabricksComponentDep,
)


def log_info(req_body, res_code, res_body):
    id = str(uuid.uuid4())
    logger.info("[{}] REQUEST: {}", id, req_body.decode("utf-8"))
    logger.info("[{}] RESPONSE({}): {}", id, res_code, res_body.decode("utf-8"))


@app.middleware("http")
async def log_request_response_middleware(request: Request, call_next):
    req_body = await request.body()
    response = await call_next(request)
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk)
    res_body = b"".join(chunks)
    task = BackgroundTask(log_info, req_body, response.status_code, res_body)
    return Response(
        content=res_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
        background=task,
    )


@app.post(
    "/v1/provision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": RequestValidationError},
        "500": {"model": SystemErr},
    },
    tags=["TechAdapter"],
)
async def provision(request: ValidatedDatabricksComponentDep, provision_service: ProvisionServiceDep) -> Response:
    """
    Deploy a data product or a single component starting from a provisioning descriptor
    """

    if isinstance(request, RequestValidationError):
        return check_response(out_response=request)

    data_product, component, remove_data = request

    logger.info("Provisioning component with id: {}", component)

    provisioning_response = provision_service.provision(data_product, component, remove_data)
    logger.info("Provisioning started. Response: {}", provisioning_response)

    return check_response(out_response=provisioning_response)


@app.get(
    "/v1/provision/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "400": {"model": RequestValidationError},
        "500": {"model": SystemErr},
    },
    tags=["TechAdapter"],
)
def get_status(token: str, provision_service: ProvisionServiceDep) -> Response:
    """
    Get the status for a provisioning request
    """

    resp = provision_service.get_provisioning_status(token)

    return check_response(out_response=resp)


@app.post(
    "/v1/unprovision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": RequestValidationError},
        "500": {"model": SystemErr},
    },
    tags=["TechAdapter"],
)
async def unprovision(request: ValidatedDatabricksComponentDep, provision_service: ProvisionServiceDep) -> Response:
    """
    Undeploy a data product or a single component
    given the provisioning descriptor relative to the latest complete provisioning request
    """  # noqa: E501

    if isinstance(request, RequestValidationError):
        return check_response(out_response=request)

    data_product, component, remove_data = request

    logger.info("Unprovisioning component with id: {}", component)

    provisioning_response = provision_service.unprovision(data_product, component, remove_data)
    logger.info("Unprovisioning started. Response: {}", provisioning_response)

    return check_response(out_response=provisioning_response)


@app.post(
    "/v1/updateacl",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": RequestValidationError},
        "500": {"model": SystemErr},
    },
    tags=["TechAdapter"],
)
def updateacl(request: ValidatedUpdateACLDatabricksComponentDep, update_acl_service: UpdateAclServiceDep) -> Response:
    """
    Request the access to a tech adapter component
    """

    if isinstance(request, RequestValidationError):
        return check_response(out_response=request)

    data_product, component, witboost_users = request

    resp = update_acl_service.update_acl(data_product, component, witboost_users)

    return check_response(out_response=resp)


@app.post(
    "/v1/validate",
    response_model=None,
    responses={"200": {"model": ValidationResult}, "500": {"model": SystemErr}},
    tags=["TechAdapter"],
)
def validate(request: ValidatedDatabricksComponentDep) -> Response:
    """
    Validate a provisioning request
    """

    if isinstance(request, RequestValidationError):
        return check_response(ValidationResult(valid=False, error=ValidationError(errors=request.errors)))

    return check_response(out_response=ValidationResult(valid=True))


@app.post(
    "/v2/validate",
    response_model=None,
    responses={
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["TechAdapter"],
)
def async_validate(
    body: ValidationRequest,
) -> Response:
    """
    Validate a deployment request
    """

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.get(
    "/v2/validate/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ValidationStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["TechAdapter"],
)
def get_validation_status(
    token: str,
) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/reverse-provisioning",
    response_model=None,
    responses={
        "200": {"model": ReverseProvisioningStatus},
        "202": {"model": str},
        "400": {"model": RequestValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def run_reverse_provisioning(
    body: ReverseProvisioningRequest, reverse_provision_service: ReverseProvisionServiceDep
) -> Response:
    """
    Execute a reverse provisioning operation
    """
    resp = reverse_provision_service.run_reverse_provisioning(body)

    return check_response(out_response=resp)


@app.get(
    "/v1/reverse-provisioning/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ReverseProvisioningStatus},
        "400": {"model": RequestValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_reverse_provisioning_status(
    token: str,
) -> Response:
    """
    Get status and results of a reverse provisioning operation
    """
    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)
