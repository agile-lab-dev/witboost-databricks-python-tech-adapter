from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field, ValidationError

from src.models.databricks.databricks_component_specific import DatabricksComponentSpecific
from src.models.databricks.workload.databricks_workload_specific import DatabricksWorkloadSpecific


def try_parse_as_workload(data: Any) -> DatabricksComponentSpecific:
    try:
        return DatabricksWorkloadSpecific.model_validate(data)
    except ValidationError:
        return DatabricksComponentSpecific.model_validate(data)


class Mesh(BaseModel):
    """Represents the mesh configuration within the catalog spec."""

    name: str
    specific: Annotated[DatabricksComponentSpecific, BeforeValidator(try_parse_as_workload)]


class Spec(BaseModel):
    """
    Represents the main specification of the catalog entity.
    """

    instance_of: str = Field(alias="instanceOf")
    type: str
    lifecycle: str
    owner: str
    system: str
    domain: str
    mesh: Mesh


# --- Top-Level CatalogInfo Model ---
class CatalogInfo(BaseModel):
    """
    Represents the top-level structure for catalog information, typically
    retrieved from a data catalog system.
    """

    spec: Spec
