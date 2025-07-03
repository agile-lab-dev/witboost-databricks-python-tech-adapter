from typing import Any, List, Optional

from databricks.sdk.service.jobs import Job
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.models.databricks.workload.databricks_workload_specific import DatabricksWorkloadSpecific


class WorkflowTasksInfo(BaseModel):
    """
    Holds workspace-dependent information for a specific task within a workflow.

    This model captures entity names and types so their IDs can be looked up
    and replaced when deploying a workflow to a new workspace.
    """

    task_key: Optional[str] = Field(None, alias="taskKey")
    referenced_task_type: Optional[str] = Field(None, alias="referencedTaskType")
    referenced_task_id: Optional[str] = Field(None, alias="referencedTaskId")
    referenced_task_name: Optional[str] = Field(None, alias="referencedTaskName")
    referenced_cluster_name: Optional[str] = Field(None, alias="referencedClusterName")
    referenced_cluster_id: Optional[str] = Field(None, alias="referencedClusterId")

    model_config = ConfigDict(populate_by_name=True)


class DatabricksWorkflowWorkloadSpecific(DatabricksWorkloadSpecific):
    """
    Represents the specific configuration for a Databricks Workflow (Job) workload.
    """

    # This allows the model to accept non-Pydantic types like the SDK's `Job` class.
    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    @field_validator("workflow", mode="before")
    @classmethod
    def parse_job(cls, data: Any) -> Any:
        if data is not None:
            if isinstance(data, dict):
                return Job.from_dict(data)
            raise ValueError("specific.workflow field is not an object")
        return None

    workflow: Job

    workflow_tasks_info: Optional[List[WorkflowTasksInfo]] = Field(default=None, alias="workflowTasksInfoList")

    override: bool
