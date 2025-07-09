from typing import Optional

from pydantic import ConfigDict, Field

from src.models.databricks.databricks_component_specific import DatabricksComponentSpecific


class DatabricksOutputPortSpecific(DatabricksComponentSpecific):
    """
    Represents the specific configuration for a Databricks Output Port,
    which is typically a Unity Catalog view.
    """

    # Pydantic v2 configuration to allow populating fields by their alias.
    model_config = ConfigDict(populate_by_name=True)

    # A nullable field, can be None if workspace is not managed
    metastore: Optional[str] = None

    catalog_name: str = Field(alias="catalogName", min_length=1)
    schema_name: str = Field(alias="schemaName", min_length=1)
    table_name: str = Field(alias="tableName", min_length=1)
    sql_warehouse_name: str = Field(alias="sqlWarehouseName", min_length=1)
    workspace_op: str = Field(alias="workspaceOP", min_length=1)
    catalog_name_op: str = Field(alias="catalogNameOP", min_length=1)
    schema_name_op: str = Field(alias="schemaNameOP", min_length=1)
    view_name_op: str = Field(alias="viewNameOP", min_length=1)
