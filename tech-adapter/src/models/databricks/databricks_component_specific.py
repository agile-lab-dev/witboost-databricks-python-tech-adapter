from pydantic import BaseModel, ConfigDict


class DatabricksComponentSpecific(BaseModel):
    model_config = ConfigDict(extra="ignore")

    workspace: str
