# Configuration

This tech adapter is configured via an `application.yaml` file, used to configure the non-sensitive values, and a set of environment variables to store credentials for integration with Databricks and Azure. Below is a detailed explanation of each section.

## `azure` Section

This section of the configuration file manages the integration with Azure and includes two main parts: **Authentication** and **Permissions**.

### Authentication

This section handles the data required for authentication with Azure and for creating and managing resources such as the Databricks Workspace.

Authentication is done using the triplet Tenant ID, Client ID, Client Secret which should be stored in the following environment variables following Azure naming conventions:

```bash
AZURE_TENANT_ID=<my_tenant_id>
AZURE_CLIENT_ID=<my_client_id>
AZURE_CLIENT_SECRET=<my_client_secret>
AZURE__AUTH__SUBSCRIPTION_ID=<my_subscription_id>
```

* **AZURE__AUTH__SUBSCRIPTION_ID**: The Azure subscription ID. Used to construct the resource ID.

For further details on these variables, refer to [azure_databricks_config.md](azure_databricks_config.md).

Other configurations are defined on the `application.yaml` file:

```yaml
azure:
  auth:
    skuType: PREMIUM | TRIAL
```

* **auth.skuType**: Specifies the SKU type of the new Workspace that will be created, allowed values are `PREMIUM` or `TRIAL`. If left blank, it defaults to PREMIUM.

### Permissions

This section manages the permissions associated with the Workspace creation during provisioning. It includes the authentication details needed to map groups and users to Azure identities and to assign permissions. Thus, this section is only needed when workspace creation is set to be managed by the Tech Adapter

The provisioner uses a service principal to authenticate against Microsoft Graph API. ClientId, tenantId and clientSecret of the service principal are required. The following permissions are required for the service principal:
- `User.Read.All`
- `GroupMember.Read.All`

Credentials are to be stored on the following environment variables:

```bash
AZURE__PERMISSIONS__RESOURCE_GROUP=<my_resource_group>
AZURE__PERMISSIONS__AUTH_TENANT_ID=<my_tenant_id>
AZURE__PERMISSIONS__AUTH_CLIENT_ID=<my_client_id>
AZURE__PERMISSIONS__AUTH_CLIENT_SECRET=<my_client_secret>
```

* **AZURE__PERMISSIONS__RESOURCE_GROUP**: The Azure resource group name. Used to construct the resource ID. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.
* **AZURE__PERMISSIONS__AUTH_TENANT_ID**: The Azure tenant ID of the service principal.
* **AZURE__PERMISSIONS__AUTH_CLIENT_ID**: The client ID of the service principal.
* **AZURE__PERMISSIONS__AUTH_CLIENT_SECRET**: The client secret of the service principal.

Other configurations are defined on the `application.yaml` file:

```yaml
azure:
  permissions:
    dpOwnerRoleDefinitionId: ToBeFilled
    devGroupRoleDefinitionId: ToBeFilled
```

* **permissions.dpOwnerRoleDefinitionId**: Specifies the role for the Data Product owner. It can be set to `"no_permissions"` or filled with an ID from Azure RBAC roles. If set to `"no_permissions"`, all direct permissions on the resource (not inherited ones) will be removed. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.
* **permissions.devGroupRoleDefinitionId**: Specifies the role for the Developer group. It can be set to `"no_permissions"` or filled with an ID from Azure RBAC roles. If set to `"no_permissions"`, all direct permissions on the resource (not inherited ones) will be removed. If the Workspace already exists and should not be managed by the Tech Adapter, this value can be omitted as it's not used.


## `databricks` Section

This section handles authentication and permissions for Databricks.

The Tech Adapter is configured to create a Workspace as part of its deployment process, unless it receives as input in the `specific.workspace` field a [Databricks Workspace URL](https://learn.microsoft.com/en-us/azure/databricks/workspace/workspace-details#per-workspace-url) rather than the Workspace name.

The Databricks Account ID is set through the following environment variable:

```bash
DATABRICKS__AUTH__ACCOUNT_ID=<my_databricks_account_id>
```

Permissions to be set at provision time to the data product owners and developer groups is set on the `application.yaml` file as follows:

```yaml
databricks:
  permissions:
    workload:
      repo:
        owner: "CAN_MANAGE"       #CAN_EDIT, CAN_MANAGE, CAN_READ, CAN_RUN, NO_PERMISSIONS
        developer: "CAN_MANAGE"   #CAN_EDIT, CAN_MANAGE, CAN_READ, CAN_RUN, NO_PERMISSIONS
      job:
        owner: "CAN_MANAGE"       #CAN_MANAGE, CAN_MANAGE_RUN, CAN_VIEW, IS_OWNER
        developer: "CAN_MANAGE"   #CAN_MANAGE, CAN_MANAGE_RUN, CAN_VIEW, IS_OWNER
      pipeline:
        owner: "CAN_MANAGE"       #CAN_MANAGE, CAN_RUN, CAN_VIEW, IS_OWNER
        developer: "CAN_MANAGE"   #CAN_MANAGE, CAN_RUN, CAN_VIEW, IS_OWNER
    outputPort:
      owner: "SELECT"         #ALL_PRIVILEGES, APPLY_TAG, SELECT
      developer: "SELECT"     #ALL_PRIVILEGES, APPLY_TAG, SELECT
```

* **permissions.workload.[repo|job|pipeline].owner**: Defines the permission level for the data product owner for the workload-related Databricks repo. Options vary depending on the object.
* **permissions.workload.[repo|job|pipeline].developer**: Defines the permission level for the development group for the workload-related Databricks repo. Options vary depending on the object.
* **permissions.outputPort.owner**: Defines the permission level for the data product owner for the Databricks output port. Options: `ALL_PRIVILEGES`, `APPLY_TAG`, `SELECT`.
* **permissions.outputPort.developer**: Defines the permission level for the developer group for the Databricks output port. Options: `ALL_PRIVILEGES`, `APPLY_TAG`, `SELECT`.


## `git` Section

This section is used for configure the Git integration with the Databricks workspace. Authentication is managed through the following environment variables:

```bash
GIT__USERNAME=<git_username>
GIT__TOKEN=<git_token>
```

The git username and token must have `read` and `write` access to the target groups or organizations where repositories to be linked to workloads are stored. These permissions vary among Git providers.

Other configurations can be set on the `application.yaml`:

```yaml
git:
  provider: gitLab

```

* **git.provider**: The Git provider, in this case set to `gitLab`. The allowed values are: `gitHub`, `bitbucketCloud`, `gitLab`, `azureDevOpsServices`, `gitHubEnterprise`, `bitbucketServer`, `gitLabEnterpriseEdition` and `awsCodeCommit`


## `usecasetemplateid` Section

Expected useCaseTemplateId values in request bodies to identify the type of component that sent the request. The use case template id must be added without the version section of the id. These are set on the `application.yaml` file as follows:

```yaml
usecasetemplateid:
  workload:
    job: ["urn:dmb:utm:databricks-workload-job-template"]
    dlt: ["urn:dmb:utm:databricks-workload-dlt-template"]
    workflow: ["urn:dmb:utm:databricks-workload-workflow-template"]
  outputport: ["urn:dmb:utm:databricks-outputport-template"]
```

* **workload.job**: useCaseTemplateId for Databricks Jobs.
* **workload.dlt**: useCaseTemplateId for Databricks Delta Live Tables (DLT).
* **workload.workflow**: useCaseTemplateId for Databricks Workflows.
* **outputport**: useCaseTemplateId for Databricks Output Port.


## `misc` Section

These configurations are set on the `application.yaml` file as follows:

```yaml
misc:
  developmentEnvironmentName: ToBeFilled
```

* **misc.developmentEnvironmentName**: The name of the Witboost development environment.
