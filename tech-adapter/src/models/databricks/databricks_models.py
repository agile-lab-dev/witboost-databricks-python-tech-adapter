from typing import Union

from src.models.data_product_descriptor import OutputPort, Workload
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.models.databricks.workload.databricks_dlt_workload_specific import DatabricksDLTWorkloadSpecific
from src.models.databricks.workload.databricks_workflow_specific import DatabricksWorkflowWorkloadSpecific
from src.models.databricks.workload.databricks_workload_specific import DatabricksJobWorkloadSpecific

# --- Base and Common Models ---


class JobWorkload(Workload):
    specific: DatabricksJobWorkloadSpecific


class WorkflowWorkload(Workload):
    specific: DatabricksWorkflowWorkloadSpecific


class DLTWorkload(Workload):
    specific: DatabricksDLTWorkloadSpecific


class DatabricksOutputPort(OutputPort):
    specific: DatabricksOutputPortSpecific


DatabricksWorkload = Union[JobWorkload, WorkflowWorkload, DLTWorkload]
DatabricksComponent = Union[DatabricksWorkload, DatabricksOutputPort]
