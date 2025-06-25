from pathlib import Path

from fastapi.encoders import jsonable_encoder
from starlette.testclient import TestClient

from src.main import app
from src.models.api_models import (
    DescriptorKind,
    ProvisionInfo,
    ProvisioningRequest,
    UpdateAclRequest,
)

client = TestClient(app)


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

    assert resp.status_code == 500
    assert "Response not yet implemented" in resp.json().get("error")


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

    assert resp.status_code == 500
    assert "Response not yet implemented" in resp.json().get("error")


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

    assert resp.status_code == 500
    assert "Response not yet implemented" in resp.json().get("error")


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

    assert resp.status_code == 500
    assert "Response not yet implemented" in resp.json().get("error")
