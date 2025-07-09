from pydantic import BaseModel, ConfigDict, Field


class OutputPortReverseProvisioningSpecific(BaseModel):
    workspace: str


class EnvironmentSpecificConfig(BaseModel):
    specific: OutputPortReverseProvisioningSpecific


class OutputPortReverseProvisioningParams(BaseModel):
    """
    Represents the parameters for a reverse provisioning request for a
    Databricks Output Port.
    """

    model_config = ConfigDict(populate_by_name=True)

    environment_specific_config: EnvironmentSpecificConfig = Field(alias="environmentSpecificConfig")
    catalog_name: str = Field(alias="catalogName")
    schema_name: str = Field(alias="schemaName")
    table_name: str = Field(alias="tableName")
    reverse_provisioning_option: str = Field(alias="reverseProvisioningOption")
