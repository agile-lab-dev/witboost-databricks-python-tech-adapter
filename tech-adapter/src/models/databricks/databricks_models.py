from typing import Union

from src.models.data_product_descriptor import OutputPort, Workload
from src.models.databricks.databricks_component_specific import DatabricksComponentSpecific
from src.models.databricks.workload.databricks_workflow_specific import DatabricksWorkflowWorkloadSpecific
from src.models.databricks.workload.databricks_workload_specific import DatabricksJobWorkloadSpecific

# --- Base and Common Models ---


class JobWorkload(Workload):
    specific: DatabricksJobWorkloadSpecific


class WorkflowWorkload(Workload):
    specific: DatabricksWorkflowWorkloadSpecific


class DLTWorkload(Workload):
    specific: DatabricksComponentSpecific  # TODO refine definition


class DatabricksOutputPort(OutputPort):
    specific: DatabricksComponentSpecific  # TODO refine definition


DatabricksWorkload = Union[JobWorkload, WorkflowWorkload, DLTWorkload]
DatabricksComponent = Union[DatabricksWorkload, DatabricksOutputPort]
