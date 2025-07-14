from typing import Optional

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk import WorkspaceClient
from fastapi import BackgroundTasks
from loguru import logger

from src.models.api_models import Info, ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import ComponentKind, DataProduct
from src.models.databricks.databricks_models import (
    DatabricksComponent,
    DatabricksOutputPort,
    DLTWorkload,
    JobWorkload,
    WorkflowWorkload,
)
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.exceptions import DatabricksError
from src.models.exceptions import ProvisioningError, build_error_message_from_chained_exception
from src.service.clients.azure.azure_workspace_handler import WorkspaceHandler
from src.service.clients.databricks.job_manager import JobManager
from src.service.provision.handler.dlt_workload_handler import DLTWorkloadHandler
from src.service.provision.handler.job_workload_handler import JobWorkloadHandler
from src.service.provision.handler.output_port_handler import OutputPortHandler
from src.service.provision.handler.workflow_workload_handler import WorkflowWorkloadHandler
from src.service.provision.task_repository import MemoryTaskRepository
from src.service.validation.output_port_validation_service import OutputPortValidation
from src.service.validation.workflow_validation_service import validate_workflow_for_provisioning


class ProvisionService:
    def __init__(
        self,
        workspace_handler: WorkspaceHandler,
        job_workload_handler: JobWorkloadHandler,
        workflow_workload_handler: WorkflowWorkloadHandler,
        dlt_workload_handler: DLTWorkloadHandler,
        output_port_handler: OutputPortHandler,
        task_repository: MemoryTaskRepository,
        background_tasks: BackgroundTasks,
    ):
        self.workspace_handler = workspace_handler
        self.job_workload_handler = job_workload_handler
        self.workflow_workload_handler = workflow_workload_handler
        self.dlt_workload_handler = dlt_workload_handler
        self.output_port_handler = output_port_handler
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
        if (
            not isinstance(component, JobWorkload)
            and not isinstance(component, WorkflowWorkload)
            and not isinstance(component, DLTWorkload)
        ):
            error_msg = (
                f"The component {component.name} is of type 'workload' but"
                f"doesn't have the expected structure for any supported Databricks workload"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])

        result: ProvisioningStatus = ProvisioningStatus(status=Status1.FAILED, result="No action done")
        if is_provisioning:
            workspace_info = self.workspace_handler.provision_workspace(data_product, component)
            self._raise_if_workspace_not_ready(workspace_info)
            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)

            if isinstance(component, JobWorkload):
                result = self._provision_job(data_product, component, workspace_info, workspace_client)
                logger.success("Job provisioning successful with resulting Provisioning Status {}", result)
            elif isinstance(component, WorkflowWorkload):
                result = self._provision_workflow(data_product, component, workspace_info, workspace_client)
                logger.success("Workflow provisioning successful with resulting Provisioning Status {}", result)
            elif isinstance(component, DLTWorkload):
                result = self._provision_dlt(data_product, component, workspace_info, workspace_client)
                logger.success("DLT provisioning successful with resulting Provisioning Status {}", result)

            self.task_repository.update_task(id=task_id, status=result.status, info=result.info, result=result.result)
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
                logger.success("Job unprovisioning successful with resulting Provisioning Status {}", result)
            elif isinstance(component, WorkflowWorkload):
                result = self._unprovision_workflow(
                    data_product, component, remove_data, existing_workspace_info, workspace_client
                )
                logger.success("Workflow unprovisioning successful with resulting Provisioning Status {}", result)
            elif isinstance(component, DLTWorkload):
                result = self._unprovision_dlt(
                    data_product, component, remove_data, existing_workspace_info, workspace_client
                )
                logger.success("DLT unprovisioning successful with resulting Provisioning Status {}", result)
            self.task_repository.update_task(id=task_id, status=result.status, info=result.info, result=result.result)

    def _handle_output_port(
        self,
        task_id: str,
        data_product: DataProduct,
        component: DatabricksComponent,
        remove_data: bool,
        is_provisioning: bool,
    ) -> None:
        if not isinstance(component, DatabricksOutputPort):
            error_msg = (
                f"The component {component.name} is of type 'outputport' but"
                f"doesn't have the expected structure for any supported Databricks Output Port"
            )
            logger.error(error_msg)
            raise ProvisioningError([error_msg])

        result: ProvisioningStatus = ProvisioningStatus(status=Status1.FAILED, result="No action done")
        if is_provisioning:
            workspace_info = self.workspace_handler.provision_workspace(data_product, component)
            self._raise_if_workspace_not_ready(workspace_info)
            workspace_client = self.workspace_handler.get_workspace_client(workspace_info)
            result = self._provision_output_port(data_product, component, workspace_info, workspace_client)
            logger.success("Output Port provisioning successful with resulting Provisioning Status {}", result)
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
            if isinstance(component, DatabricksOutputPort):
                result = self._unprovision_output_port(
                    data_product, component, existing_workspace_info, workspace_client
                )
                logger.success("Output Port unprovisioning successful with resulting Provisioning Status {}", result)

        self.task_repository.update_task(id=task_id, status=result.status, info=result.info, result=result.result)

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

    def _provision_workflow(
        self,
        data_product: DataProduct,
        component: WorkflowWorkload,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        job_manager = JobManager(workspace_client, workspace_info.name)
        validate_workflow_for_provisioning(
            job_manager, workspace_client, component, data_product.environment, workspace_info
        )
        workflow_id = self.workflow_workload_handler.provision_workflow(
            data_product, component, workspace_client, workspace_info
        )
        wf_url = f"https://{workspace_info.databricks_host}/jobs/{workflow_id}"
        info = {
            "workspaceURL": {
                "type": "string",
                "label": "Databricks Workspace URL",
                "value": "Open Azure Databricks Workspace",
                "href": workspace_info.azure_resource_url,
            },
            "jobURL": {"type": "string", "label": "Job URL", "value": "Open job details in Databricks", "href": wf_url},
        }
        return ProvisioningStatus(status=Status1.COMPLETED, result="", info=Info(publicInfo=info, privateInfo=info))

    def _unprovision_workflow(
        self,
        data_product: DataProduct,
        component: WorkflowWorkload,
        remove_data: bool,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        self.workflow_workload_handler.unprovision_workload(
            data_product, component, remove_data, workspace_client, workspace_info
        )
        return ProvisioningStatus(status=Status1.COMPLETED, result="")

    def _provision_output_port(
        self,
        data_product: DataProduct,
        component: DatabricksOutputPort,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        # 1. Attach metastore if workspace is managed
        if workspace_info.is_managed:
            # TODO uncomment and test
            # unity_catalog_manager.attach_metastore(specific.metastore)
            error_msg = "Witboost managed workspace are not yet supported on this version"
            logger.error(error_msg)
            raise ProvisioningError([error_msg])
        else:
            logger.info("Skipping metastore attachment as workspace is not managed.")

        # After attaching the metastore, we check again the existence before actually provisioning
        op_validation_service = OutputPortValidation(self.workspace_handler)
        op_validation_service.validate_table_existence_and_schema(
            workspace_client, component, data_product.environment, workspace_info
        )

        table_info = self.output_port_handler.provision_output_port(
            data_product, component, workspace_client, workspace_info
        )

        table_url = (
            f"https://{workspace_info.databricks_host}/explore/data/"
            f"{table_info.catalog_name}/{table_info.schema_name}/{table_info.name}"
        )
        info = {
            "tableID": {"type": "string", "label": "Table ID", "value": table_info.table_id},
            "tableFullName": {"type": "string", "label": "Table full name", "value": table_info.full_name},
            "tableUrl": {
                "type": "string",
                "label": "Table URL",
                "value": "Open table in Databricks",
                "href": table_url,
            },
        }
        return ProvisioningStatus(status=Status1.COMPLETED, result="", info=Info(publicInfo=info, privateInfo=info))

    def _unprovision_output_port(
        self,
        data_product: DataProduct,
        component: DatabricksOutputPort,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        self.output_port_handler.unprovision_output_port(data_product, component, workspace_client, workspace_info)
        return ProvisioningStatus(status=Status1.COMPLETED, result="")

    def _provision_dlt(
        self,
        data_product: DataProduct,
        component: DLTWorkload,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ):
        pipeline_id = self.dlt_workload_handler.provision_workload(
            data_product, component, workspace_client, workspace_info
        )
        pipeline_url = f"https://{workspace_info.databricks_host}/pipelines/{pipeline_id}"
        info = {
            "workspaceURL": {
                "type": "string",
                "label": "Databricks workspace URL",
                "value": "Open Azure Databricks Workspace",
                "href": workspace_info.azure_resource_url,
            },
            "pipelineURL": {
                "type": "string",
                "label": "Pipeline URL",
                "value": "Open pipeline details in Databricks",
                "href": pipeline_url,
            },
        }
        return ProvisioningStatus(status=Status1.COMPLETED, result="", info=Info(publicInfo=info, privateInfo=info))

    def _unprovision_dlt(
        self,
        data_product: DataProduct,
        component: DLTWorkload,
        remove_data: bool,
        workspace_info: DatabricksWorkspaceInfo,
        workspace_client: WorkspaceClient,
    ) -> ProvisioningStatus:
        self.dlt_workload_handler.unprovision_workload(
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
