from typing import Any, Optional

from databricks.sdk.service.jobs import Job
from pydantic import BaseModel, ConfigDict, Field, field_validator


class WorkflowReverseProvisioningSpecific(BaseModel):
    """
    Holds the specific details for a workflow to be reverse provisioned,
    including the workspace and the job definition itself.
    """

    # This configuration allows the model to hold a non-Pydantic object
    # like the `Job` class from the Databricks SDK.
    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    workspace: str
    workflow: Job
    run_as_principal_name: Optional[str] = Field(default=None, alias="runAsPrincipalName")

    @field_validator("workflow", mode="before")
    @classmethod
    def parse_job(cls, data: Any) -> Any:
        if data is not None:
            if isinstance(data, dict):
                return Job.from_dict(data)
            raise ValueError("specific.workflow field is not an object")
        return None


class EnvironmentSpecificConfig(BaseModel):
    specific: WorkflowReverseProvisioningSpecific


class WorkflowReverseProvisioningParams(BaseModel):
    """
    Represents the parameters for a reverse provisioning request for a
    Databricks workflow.
    """

    model_config = ConfigDict(populate_by_name=True)

    environment_specific_config: EnvironmentSpecificConfig = Field(alias="environmentSpecificConfig")
