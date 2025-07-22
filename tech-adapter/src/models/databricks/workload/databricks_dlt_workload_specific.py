from typing import Dict, List, Optional

from databricks.sdk.service.pipelines import PipelineClusterAutoscaleMode
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.models.data_product_descriptor import CaseInsensitiveEnum
from src.models.databricks.workload.databricks_workload_specific import DatabricksWorkloadSpecific, KeyValueProperty


class ProductEdition(CaseInsensitiveEnum):
    """Represents the edition of a product for a DLT pipeline."""

    ADVANCED = "advanced"
    CORE = "core"
    PRO = "pro"


class PipelineChannel(CaseInsensitiveEnum):
    """Represents the channel type for a DLT pipeline."""

    PREVIEW = "preview"
    CURRENT = "current"


class PipelineNotification(BaseModel):
    """Represents a notification configuration for a Databricks pipeline."""

    mail: str
    alert: List[str]


class DLTClusterSpecific(BaseModel):
    """
    Represents the cluster-specific configuration for a DLT pipeline.
    Includes a custom validator to ensure scaling parameters are consistent.
    """

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate_cluster_sizing(self) -> "DLTClusterSpecific":
        """
        Validates that sizing parameters (min/max_workers vs num_workers)
        are consistent with the selected autoscaling mode.
        """
        is_autoscale = self.mode in (
            PipelineClusterAutoscaleMode.ENHANCED,
            PipelineClusterAutoscaleMode.LEGACY,
        )

        if is_autoscale:
            if self.min_workers is None or self.max_workers is None:
                raise ValueError("For autoscale modes, 'min_workers' and 'max_workers' must be set.")
            if self.num_workers is not None:
                raise ValueError("For autoscale modes, 'num_workers' must not be set.")
        else:  # Fixed-size cluster
            if self.num_workers is None:
                raise ValueError("For fixed-size clusters, 'num_workers' must be set.")
            if self.min_workers is not None or self.max_workers is not None:
                raise ValueError("For fixed-size clusters, 'min_workers' and 'max_workers' must not be set.")

        return self

    policy_id: Optional[str] = None
    mode: Optional[PipelineClusterAutoscaleMode] = None
    min_workers: Optional[int] = Field(None, alias="minWorkers")
    max_workers: Optional[int] = Field(None, alias="maxWorkers")
    num_workers: Optional[int] = Field(None, alias="numWorkers")

    worker_type: str = Field(alias="workerType", min_length=1)
    driver_type: str = Field(alias="driverType", min_length=1)

    spark_conf: Optional[List[KeyValueProperty]] = Field(None, alias="sparkConf")
    # This has been ported back to the original version not discriminated by environment
    spark_env_vars: Optional[List[KeyValueProperty]] = Field(None, alias="sparkEnvVars")
    tags: Optional[Dict[str, str]] = None


class DatabricksDLTWorkloadSpecific(DatabricksWorkloadSpecific):
    """
    Represents the specific configuration for a Databricks Delta Live Tables (DLT) workload.
    """

    model_config = ConfigDict(populate_by_name=True)

    pipeline_name: str = Field(alias="pipelineName", min_length=1)
    catalog: str = Field(min_length=1)
    product_edition: ProductEdition = Field(alias="productEdition")
    continuous: bool
    photon: bool
    channel: PipelineChannel
    cluster: DLTClusterSpecific
    notebooks: List[str] = Field(..., min_length=1)
    target: str

    files: Optional[List[str]] = None
    notifications: Optional[List[PipelineNotification]] = None
