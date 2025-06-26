from databricks.sdk import AccountClient

from src.settings.databricks_tech_adapter_settings import AppSettings


def get_account_client(settings: AppSettings) -> AccountClient:
    return AccountClient(
        auth_type="azure-client-secret",
        host="https://accounts.azuredatabricks.net/",
        account_id=settings.databricks.auth.account_id,
        azure_tenant_id=settings.azure.auth.tenant_id,
        azure_client_id=settings.azure.auth.client_id,
        azure_client_secret=settings.azure.auth.client_secret,
    )
