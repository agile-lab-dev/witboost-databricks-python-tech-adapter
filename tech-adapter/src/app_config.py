from fastapi import FastAPI

app = FastAPI(
    title="Databricks Tech Adapter",
    description="Microservice responsible to handle provisioning and access control requests for one or more data product components.",  # noqa: E501
    version="2.2.0",
)
