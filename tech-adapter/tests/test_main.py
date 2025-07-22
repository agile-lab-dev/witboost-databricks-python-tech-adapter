from pathlib import Path
from unittest.mock import Mock

from fastapi.encoders import jsonable_encoder
from starlette.testclient import TestClient

from src.dependencies import (
    create_provision_service,
    create_reverse_provision_service,
    create_update_acl_service,
    get_workspace_handler,
)
from src.main import app
from src.models.api_models import (
    DescriptorKind,
    ProvisionInfo,
    ProvisioningRequest,
    ProvisioningStatus,
    ReverseProvisioningStatus,
    Status1,
    UpdateAclRequest,
)
from src.service.clients.azure.azure_workspace_handler import AzureWorkspaceHandler
from src.service.provision.provision_service import ProvisionService
from src.service.provision.update_acl_service import UpdateAclService
from src.service.reverse_provision.reverse_provision_service import ReverseProvisionService

client = TestClient(app)

# TODO refine tests
expected_global_response = "abcdef-1234-abcd"


def override_dependency() -> ProvisionService:
    mock = Mock()
    mock.provision.return_value = expected_global_response
    mock.unprovision.return_value = expected_global_response
    return mock


def override_update_acl_dependency() -> UpdateAclService:
    mock = Mock()
    mock.update_acl.return_value = ProvisioningStatus(status=Status1.COMPLETED, result="")
    return mock


def override_reverse_provision_dependency() -> ReverseProvisionService:
    mock = Mock()
    mock.run_reverse_provisioning.return_value = ReverseProvisioningStatus(
        status=Status1.COMPLETED, updates={}, logs=[]
    )
    return mock


def override_get_workspace_handler() -> AzureWorkspaceHandler:
    mock = Mock()
    mock.get_workspace_info_by_name.return_value = None
    return mock


app.dependency_overrides[create_provision_service] = override_dependency
app.dependency_overrides[create_reverse_provision_service] = override_reverse_provision_dependency
app.dependency_overrides[create_update_acl_service] = override_update_acl_dependency
app.dependency_overrides[get_workspace_handler] = override_get_workspace_handler


def test_provisioning_invalid_descriptor():
    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor"
    )

    resp = client.post("/v1/provision", json=dict(provisioning_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_provisioning_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str
    )

    resp = client.post("/v1/provision", json=dict(provisioning_request))

    assert resp.status_code == 202


def test_unprovisioning_invalid_descriptor():
    unprovisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor"
    )

    resp = client.post("/v1/unprovision", json=dict(unprovisioning_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_unprovisioning_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    unprovisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str
    )

    resp = client.post("/v1/unprovision", json=dict(unprovisioning_request))

    assert resp.status_code == 202


def test_validate_invalid_descriptor():
    validate_request = ProvisioningRequest(descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor="descriptor")

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert "Unable to parse the descriptor." in resp.json().get("error").get("errors")


def test_validate_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    validate_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.COMPONENT_DESCRIPTOR, descriptor=descriptor_str
    )

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert resp.json().get("valid") is True


def test_updateacl_invalid_descriptor():
    updateacl_request = UpdateAclRequest(
        provisionInfo=ProvisionInfo(request="descriptor", result=""),
        refs=["user:alice", "user:bob"],
    )

    resp = client.post("/v1/updateacl", json=jsonable_encoder(updateacl_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_updateacl_valid_descriptor():
    descriptor_str = Path("tests/descriptors/descriptor_output_port_valid.yaml").read_text()

    updateacl_request = UpdateAclRequest(
        provisionInfo=ProvisionInfo(request=descriptor_str, result=""),
        refs=["user:alice", "user:bob"],
    )

    resp = client.post("/v1/updateacl", json=jsonable_encoder(updateacl_request))

    print(resp)

    assert resp.status_code == 200
