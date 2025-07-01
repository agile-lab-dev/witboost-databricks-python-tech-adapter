from typing import Optional

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk import WorkspaceClient
from fastapi import BackgroundTasks
from loguru import logger

from src.models.api_models import Info, ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import ComponentKind, DataProduct
from src.models.databricks.databricks_models import DatabricksComponent, JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import DatabricksError
from src.models.exceptions import ProvisioningError, build_error_message_from_chained_exception
from src.service.clients.azure.azure_workspace_handler import WorkspaceHandler
from src.service.provision.handler.job_workload_handler import JobWorkloadHandler
from src.service.provision.task_repository import MemoryTaskRepository


class ProvisionService:
    def __init__(
        self,
        workspace_handler: WorkspaceHandler,
        job_workload_handler: JobWorkloadHandler,
        task_repository: MemoryTaskRepository,
        background_tasks: BackgroundTasks,
    ):
        self.workspace_handler = workspace_handler
        self.job_workload_handler = job_workload_handler
        self.task_repository = task_repository
        self.background_tasks = background_tasks

    def _start_provisioning(
        self,
        task_id: str,
        data_product: DataProduct,
        component: DatabricksComponent,
        remove_data: bool,
        is_provisioning: bool,
    ) -> None:
        try:
            if component.kind == ComponentKind.WORKLOAD:
                self._handle_workload(task_id, data_product, component, remove_data, is_provisioning)
            elif component.kind == ComponentKind.OUTPUTPORT:
                self._handle_output_port(task_id, data_product, component, remove_data, is_provisioning)
            else:
                error_msg = (
                    f"The kind '{component.kind}' of the component '{component.id}' "
                    f"is not supported by this Tech Adapter"
                )
                self.task_repository.update_task(id=task_id, status=Status1.FAILED, result=error_msg)
        except (ProvisioningError, DatabricksError) as e:
            logger.exception(
                "Caught expected exception {} during provision of component '{}', handling appropriately",
                e,
                component.id,
            )
            error_msg = build_error_message_from_chained_exception(e)
            logger.error(error_msg)
            self.task_repository.update_task(id=task_id, status=Status1.FAILED, result=error_msg)
        except Exception as e:
            logger.exception(
                "Caught unexpected exception {} during provision of component '{}', storing failed result",
                e,
                component.id,
            )
            error_msg = build_error_message_from_chained_exception(e)
            logger.error(error_msg)
            self.task_repository.update_task(id=task_id, status=Status1.FAILED, result=error_msg)

    def provision(
        self, data_product: DataProduct, component: DatabricksComponent, remove_data: bool
    ) -> str | SystemErr:
        """
        Performs the provisioning of a databricks component
        Args:
            data_product: Data product containing the metadata for the whole data product
            component: Component containing the information of the specific databricks resources to provision
            remove_data: Provision request flag containing information about data removal

        Returns:
            String containing the ID of the asynchronous request to be polled for completion

        """
        task_id, task = self.task_repository.create_task()
        self.background_tasks.add_task(
            self._start_provisioning, str(task_id), data_product, component, remove_data, True
        )
        return str(task_id)

    def unprovision(
        self,
        data_product: DataProduct,
        component: DatabricksComponent,
        remove_data: bool,
    ) -> str | SystemErr:
        task_id, task = self.task_repository.create_task()
        self.background_tasks.add_task(
            self._start_provisioning, str(task_id), data_product, component, remove_data, False
        )
        return str(task_id)

    def get_provisioning_status(self, id: str) -> ProvisioningStatus | SystemErr:
        status: Optional[ProvisioningStatus] = self.task_repository.get_task(id)
        if status:
            return status

        error_msg = f"Cannot find Provisioning Status for id '{id}'"
        logger.error(error_msg)
        return SystemErr(error=error_msg)

    def _handle_workload(
        self,
        task_id: str,
        data_product: DataProduct,
        component: DatabricksComponent,
        remove_data: bool,
        is_provisioning: bool,
    ) -> None:
        if is_provisioning:
            workspace_info = self.workspace_handler.provision_workspace(data_product, component)
            self._raise_if_workspace_not_ready(workspace_info)
            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)
            if isinstance(component, JobWorkload):
                result = self._provision_job(data_product, component, workspace_info, workspace_client)
                self.task_repository.update_task(
                    id=task_id, status=result.status, info=result.info, result=result.result
                )
                logger.success("Job provisioning successful with resulting Provisioning Status {}", result)
        else:
            existing_workspace_info = self.workspace_handler.get_workspace_info(component)
            if not existing_workspace_info:
                message = (
                    f"Unprovision skipped for component {component.name}, "
                    f"Workspace {component.specific.workspace} not found"
                )
                logger.info(message)
                self.task_repository.update_task(id=task_id, status=Status1.COMPLETED, result=message)
                return
            self._raise_if_workspace_not_ready(existing_workspace_info)
            workspace_client = self.workspace_handler.get_workspace_client(existing_workspace_info)
            if isinstance(component, JobWorkload):
                result = self._unprovision_job(
                    data_product, component, remove_data, existing_workspace_info, workspace_client
                )
                self.task_repository.update_task(
                    id=task_id, status=result.status, info=result.info, result=result.result
                )
                logger.success("Job unprovisioning successful with resulting Provisioning Status {}", result)

    def _handle_output_port(
        self,
        task_id: str,
        data_product: DataProduct,
        component: DatabricksComponent,
        remove_data: bool,
        is_provisioning: bool,
    ) -> None:
        # TODO method
        error_msg = (
            f"The kind '{component.kind}' of the component '{component.id}' is not supported by this Tech Adapter"
        )
        self.task_repository.update_task(id=task_id, status=Status1.FAILED, result=error_msg)

    def _provision_job(
        self,
        data_product: DataProduct,
        component: JobWorkload,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        job_id = self.job_workload_handler.provision_workload(data_product, component, workspace_client, workspace_info)
        job_url = f"https://{workspace_info.databricks_host}/jobs/{job_id}"
        info = {
            "workspaceURL": {
                "type": "string",
                "label": "Databricks workspace URL",
                "value": "Open Azure Databricks Workspace",
                "href": workspace_info.azure_resource_url,
            },
            "jobURL": {
                "type": "string",
                "label": "Job URL",
                "value": "Open job details in Databricks",
                "href": job_url,
            },
        }
        return ProvisioningStatus(status=Status1.COMPLETED, result="", info=Info(publicInfo=info, privateInfo=info))

    def _unprovision_job(
        self,
        data_product: DataProduct,
        component: JobWorkload,
        remove_data: bool,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        self.job_workload_handler.unprovision_workload(
            data_product, component, remove_data, workspace_client, workspace_info
        )
        return ProvisioningStatus(status=Status1.COMPLETED, result="")

    def _raise_if_workspace_not_ready(self, workspace_info: DatabricksWorkspaceInfo) -> None:
        if not workspace_info.provisioning_state == ProvisioningState.SUCCEEDED:
            error_msg = (
                f"The status of {workspace_info.name} workspace is different from 'ACTIVE'. "
                f"Please try again and if the error persists contact the platform team."
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])
