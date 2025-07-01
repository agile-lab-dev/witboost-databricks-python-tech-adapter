import os
from enum import StrEnum
from typing import List, Optional

from loguru import logger
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource


# --- Azure Configuration ---
class SkuType(StrEnum):
    PREMIUM = "PREMIUM"
    TRIAL = "TRIAL"


class AzureAuthSettings(BaseSettings):
    """Azure authentication settings for the main application principal."""

    model_config = SettingsConfigDict(env_prefix="auth_", extra="ignore")

    client_id: str  # = Field(..., alias="AZURE_CLIENT_ID")
    tenant_id: str  # = Field(..., alias="AZURE_TENANT_ID")
    client_secret: str  # = Field(..., alias="AZURE_CLIENT_SECRET")
    subscription_id: str

    sku_type: SkuType = SkuType.PREMIUM


class AzurePermissionsSettings(BaseSettings):
    """
    Azure authentication settings for the principal responsible for managing
    permissions, and resource identifiers.
    """

    model_config = SettingsConfigDict(env_prefix="permissions_", extra="ignore")

    auth_client_id: str
    auth_tenant_id: str
    auth_client_secret: str
    resource_group: str
    dp_owner_role_definition_id: str = ""
    dev_group_role_definition_id: str = ""


class AzureSettings(BaseSettings):
    """Root configuration for Azure services."""

    model_config = SettingsConfigDict(env_prefix="azure_", extra="ignore")

    auth: AzureAuthSettings
    permissions: Optional[AzurePermissionsSettings] = None


# --- Databricks Configuration ---


class DatabricksAuthSettings(BaseSettings):
    """Databricks account authentication settings."""

    model_config = SettingsConfigDict(env_prefix="auth_", extra="ignore")

    account_id: str


class RepositoryPermissions(StrEnum):
    CAN_EDIT = "CAN_EDIT"
    CAN_MANAGE = "CAN_MANAGE"
    CAN_READ = "CAN_READ"
    CAN_RUN = "CAN_RUN"
    NO_PERMISSIONS = "NO_PERMISSIONS"


class DatabricksRepoPermissionsSettings(BaseSettings):
    """Permission levels for Databricks Git Repos."""

    model_config = SettingsConfigDict(env_prefix="databricks_permissions_workload_repo_", extra="ignore")
    owner: RepositoryPermissions = RepositoryPermissions.CAN_MANAGE
    developer: RepositoryPermissions = RepositoryPermissions.CAN_MANAGE


class JobPermissions(StrEnum):
    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_RUN = "CAN_MANAGE_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"
    NO_PERMISSIONS = "NO_PERMISSIONS"


class DatabricksJobPermissionsSettings(BaseSettings):
    """Permission levels for Databricks Jobs."""

    model_config = SettingsConfigDict(env_prefix="databricks_permissions_workload_job_", extra="ignore")
    owner: JobPermissions = JobPermissions.CAN_MANAGE
    developer: JobPermissions = JobPermissions.CAN_MANAGE


class PipelinePermissions(StrEnum):
    CAN_MANAGE = "CAN_MANAGE"
    CAN_RUN = "CAN_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"
    NO_PERMISSIONS = "NO_PERMISSIONS"


class DatabricksPipelinePermissionsSettings(BaseSettings):
    """Permission levels for Databricks DLT Pipelines."""

    model_config = SettingsConfigDict(env_prefix="databricks_permissions_workload_pipeline_", extra="ignore")
    owner: PipelinePermissions = PipelinePermissions.CAN_MANAGE
    developer: PipelinePermissions = PipelinePermissions.CAN_MANAGE


class DatabricksWorkloadPermissionsSettings(BaseSettings):
    """Groups all workload-related permission settings."""

    model_config = SettingsConfigDict(env_prefix="databricks_permissions_workload_", extra="ignore")

    repo: DatabricksRepoPermissionsSettings
    job: DatabricksJobPermissionsSettings
    pipeline: DatabricksPipelinePermissionsSettings


class TablePermissions(StrEnum):
    ALL_PRIVILEGES = "ALL_PRIVILEGES"
    APPLY_TAG = "APPLY_TAG"
    SELECT = "SELECT"


class DatabricksOutputPortPermissionsSettings(BaseSettings):
    """Default permission levels for Databricks output port tables/views."""

    model_config = SettingsConfigDict(env_prefix="databricks_permissions_outputport_", extra="ignore")

    owner: str = TablePermissions.SELECT
    developer: str = TablePermissions.SELECT


class DatabricksPermissionsSettings(BaseSettings):
    """Root for Databricks permission configurations."""

    model_config = SettingsConfigDict(env_prefix="databricks_permissions_", extra="ignore")

    workload: DatabricksWorkloadPermissionsSettings
    output_port: DatabricksOutputPortPermissionsSettings = Field(alias="outputPort")


class DatabricksSettings(BaseSettings):
    """Root configuration for Databricks."""

    model_config = SettingsConfigDict(env_prefix="databricks_", extra="ignore")

    auth: DatabricksAuthSettings
    permissions: DatabricksPermissionsSettings


# --- Other Configurations ---


class GitSettings(BaseSettings):
    """Git provider credentials."""

    model_config = SettingsConfigDict(env_prefix="git_", extra="ignore")

    username: str
    token: str
    provider: str = "GITLAB"


class UseCaseTemplateWorkloadSettings(BaseSettings):
    """URNs for workload-related use case templates."""

    model_config = SettingsConfigDict(env_prefix="usecasetemplateid_workload_", extra="ignore")

    job: List[str] = ["urn:dmb:utm:databricks-workload-job-template"]
    dlt: List[str] = ["urn:dmb:utm:databricks-workload-dlt-template"]
    workflow: List[str] = ["urn:dmb:utm:databricks-workload-workflow-template"]


class UseCaseTemplateIdSettings(BaseSettings):
    """URNs for various use case templates."""

    model_config = SettingsConfigDict(env_prefix="usecasetemplateid_", extra="ignore")

    workload: UseCaseTemplateWorkloadSettings
    outputPort: List[str] = ["urn:dmb:utm:databricks-outputport-template"]


class MiscSettings(BaseSettings):
    """Miscellaneous application settings."""

    model_config = SettingsConfigDict(env_prefix="misc_", extra="ignore")

    development_environment_name: str = Field(alias="developmentEnvironmentName")


# --- Main Application Settings ---


class AppSettings(BaseSettings):
    """
    Main application settings class that aggregates all configuration models.
    It reads from a .env file and environment variables.
    """

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls, ["config/application.yaml"]),
        )

    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__", extra="ignore")

    azure: AzureSettings
    databricks: DatabricksSettings
    git: GitSettings
    usecasetemplateid: UseCaseTemplateIdSettings
    misc: MiscSettings


# Example of how to load and use the settings
def load_settings() -> AppSettings:
    """Loads the application settings and returns the populated model."""
    try:
        os.environ["AZURE__AUTH__CLIENT_ID"] = os.environ["AZURE_CLIENT_ID"]
        os.environ["AZURE__AUTH__TENANT_ID"] = os.environ["AZURE_TENANT_ID"]
        os.environ["AZURE__AUTH__CLIENT_SECRET"] = os.environ["AZURE_CLIENT_SECRET"]
        settings = AppSettings()
        logger.info("Application settings loaded successfully.")
        return settings
    except ValidationError:
        logger.exception("Failed to load application settings")
        raise
