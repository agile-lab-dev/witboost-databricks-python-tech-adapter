from typing import List, Optional

from databricks.sdk.service.compute import (
    AzureAvailability,
    RuntimeEngine,
)
from databricks.sdk.service.jobs import GitProvider
from pydantic import BaseModel, Field

from src.models.data_product_descriptor import CaseInsensitiveEnum
from src.models.databricks.databricks_component_specific import DatabricksComponentSpecific


class GitSpecific(BaseModel):
    """Base model for Git repository configuration."""

    gitRepoUrl: str


class KeyValueProperty(BaseModel):
    """Represents a single key-value pair configuration."""

    name: str
    value: str


# --- Enums ---


class GitReferenceType(CaseInsensitiveEnum):
    """Defines the type of a Git reference."""

    BRANCH = "BRANCH"
    TAG = "TAG"


# --- Job-Specific Cluster Configuration ---


class JobClusterSpecific(BaseModel):
    """
    Defines the configuration for a job's compute cluster.
    """

    clusterSparkVersion: str
    nodeTypeId: str
    numWorkers: int = Field(ge=1)

    # Optional fields
    spotBidMaxPrice: Optional[float] = None
    firstOnDemand: Optional[int] = None
    spotInstances: Optional[bool] = None
    availability: Optional[AzureAvailability] = None
    driverNodeTypeId: Optional[str] = None
    sparkConf: Optional[List[KeyValueProperty]] = None

    # This has been ported back to the original version not discriminated by environment
    spark_env_vars: Optional[List[KeyValueProperty]] = Field(None, alias="sparkEnvVars")

    runtimeEngine: Optional[RuntimeEngine] = None


# --- Main Workload-Specific Models ---


class DatabricksWorkloadSpecific(DatabricksComponentSpecific):
    """
    Base model for Databricks workload-specific configurations.
    """

    git: GitSpecific
    repoPath: str
    runAsPrincipalName: Optional[str] = None


class DatabricksJobWorkloadSpecific(DatabricksWorkloadSpecific):
    """
    Defines the specific configuration for a Databricks Job workload.
    """

    class JobGitSpecific(GitSpecific):
        """Extended Git configuration specific to a Job workload."""

        gitReference: str
        gitReferenceType: GitReferenceType
        gitPath: str
        gitProvider: Optional[GitProvider] = None

    class SchedulingSpecific(BaseModel):
        """Configuration for scheduling a job with a cron expression."""

        cronExpression: str
        javaTimezoneId: str

        def __str__(self) -> str:
            return f"SchedulingSpecific(cronExpression={self.cronExpression}, javaTimezoneId={self.javaTimezoneId})"

    # --- Fields for DatabricksJobWorkloadSpecific ---
    jobName: str
    description: Optional[str] = None
    git: JobGitSpecific
    cluster: JobClusterSpecific
    scheduling: Optional[SchedulingSpecific] = None
