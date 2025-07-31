import unittest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState

from src.models.api_models import ProvisioningStatus, Status1
from src.models.data_product_descriptor import DataContract, DataProduct
from src.models.databricks.databricks_models import DatabricksOutputPort, DLTWorkload, JobWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.models.databricks.workload.databricks_dlt_workload_specific import (
    DatabricksDLTWorkloadSpecific,
    DLTClusterSpecific,
)
from src.models.databricks.workload.databricks_workload_specific import (
    DatabricksJobWorkloadSpecific,
    GitSpecific,
    JobClusterSpecific,
)
from src.models.exceptions import ProvisioningError
from src.service.provision.provision_service import ProvisionService


class TestProvisionService(unittest.IsolatedAsyncioTestCase):
    """Unit tests for the ProvisionService class."""

    def setUp(self):
        """Set up mocks for all injected dependencies."""
        self.mock_workspace_handler = MagicMock(provision_workspace=AsyncMock(), get_workspace_info=MagicMock())
        self.mock_job_handler = MagicMock()
        self.mock_workflow_handler = MagicMock()
        self.mock_dlt_handler = MagicMock()
        self.mock_output_port_handler = MagicMock()
        self.mock_task_repo = MagicMock()
        self.mock_background_tasks = MagicMock()

        # The service under test
        self.service = ProvisionService(
            workspace_handler=self.mock_workspace_handler,
            job_workload_handler=self.mock_job_handler,
            workflow_workload_handler=self.mock_workflow_handler,
            dlt_workload_handler=self.mock_dlt_handler,
            output_port_handler=self.mock_output_port_handler,
            task_repository=self.mock_task_repo,
            background_tasks=self.mock_background_tasks,
        )

        # Common test data
        self.task_id = str(uuid.uuid4())
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner="user:john.doe_test.com",
            devGroup="group:test-group",
            name="dp-name",
            description="description",
            kind="dataproduct",
            domain="domain:domain",
            version="0.0.0",
            environment="development",
            ownerGroup="group:dev-group",
            specific={},
            components=[],
            tags=[],
        )
        self.workspace_info = DatabricksWorkspaceInfo(
            id="12345",
            name="test-workspace",
            azure_resource_id="res-id-123",
            azure_resource_url="https://portal.azure.com",
            databricks_host="https://test.azuredatabricks.net",
            provisioning_state=ProvisioningState.SUCCEEDED,
            is_managed=True,
        )

    async def test_provision_dispatches_to_job_workload_handler(self):
        """Test that a provision request for a JobWorkload is correctly handled."""
        # Arrange
        component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace="test-workspace",
                metastore="test_metastore",
                repoPath="Repos/test/repo",
                jobName="test_job",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="https://test.repo",
                    gitReference="gitReference",
                    gitPath="gitPath",
                    gitReferenceType="branch",
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )

        self.mock_workspace_handler.provision_workspace.return_value = self.workspace_info
        self.mock_job_handler.provision_workload.return_value = "job-123"

        # Act
        await self.service._start_provisioning(self.task_id, self.data_product, component, False, is_provisioning=True)

        # Assert
        self.mock_workspace_handler.provision_workspace.assert_awaited_once_with(self.data_product, component)
        self.mock_job_handler.provision_workload.assert_called_once()
        # Verify the task is updated with a COMPLETED status and correct info
        self.mock_task_repo.update_task.assert_called_once()
        _, kwargs = self.mock_task_repo.update_task.call_args
        self.assertEqual(kwargs["id"], self.task_id)
        self.assertEqual(kwargs["status"], Status1.COMPLETED)
        self.assertIn("jobURL", kwargs["info"].publicInfo)

    async def test_unprovision_dispatches_to_dlt_workload_handler(self):
        """Test that an unprovision request for a DLTWorkload is correctly handled."""
        # Arrange
        component = DLTWorkload(
            id="comp-id",
            name="test-dlt-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksDLTWorkloadSpecific(
                pipeline_name="test_dlt_pipeline",
                catalog="test_catalog",
                product_edition="pro",
                continuous=False,
                photon=True,
                channel="current",
                cluster=DLTClusterSpecific(num_workers=1, worker_type="Standard_F4s", driver_type="Standard_F4s"),
                notebooks=["/path/to/notebook.py"],
                target="test_target_schema",
                workspace="test-workspace",
                metastore="test_metastore",
                git=GitSpecific(gitRepoUrl="https://test.repo"),
                repoPath="Repos/test/repo",
            ),
        )
        self.mock_workspace_handler.get_workspace_info.return_value = self.workspace_info

        # Act
        await self.service._start_provisioning(self.task_id, self.data_product, component, True, is_provisioning=False)

        # Assert
        self.mock_workspace_handler.get_workspace_info.assert_called_once_with(component)
        self.mock_dlt_handler.unprovision_workload.assert_called_once()
        # Verify the final status update
        self.mock_task_repo.update_task.assert_called_once_with(
            id=self.task_id, status=Status1.COMPLETED, info=None, result=""
        )

    @patch("src.service.provision.provision_service.UnityCatalogManager")
    async def test_provision_dispatches_to_output_port_handler(self, UnityCatalogManagerMock):
        """Test that a provision request for an OutputPort is correctly handled."""
        unity_catalog_manager_mock = UnityCatalogManagerMock.return_value
        # Arrange
        component = DatabricksOutputPort(
            id="comp-id",
            name="test-output-port",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            tags=[],
            kind="outputport",
            outputPortType="SQL",
            semanticLinking=[],
            dataContract=DataContract(),
            specific=DatabricksOutputPortSpecific(
                catalog_name="input_catalog",
                schema_name="input_schema",
                table_name="input_table",
                sql_warehouse_name="test-wh",
                catalog_name_op="output_catalog",
                schema_name_op="output_schema",
                view_name_op="output_view",
                workspace="test-workspace",
                workspace_op="test-workspace-op",
                metastore="test-metastore",
            ),
        )
        self.mock_workspace_handler.provision_workspace.return_value = self.workspace_info
        self.mock_output_port_handler.provision_output_port.return_value = MagicMock(
            table_id="t-id", full_name="cat.sch.tbl", catalog_name="cat", schema_name="sch", name="tbl"
        )
        unity_catalog_manager_mock.check_table_existence.return_value = True
        # Act
        await self.service._start_provisioning(self.task_id, self.data_product, component, False, is_provisioning=True)

        # Assert
        self.mock_output_port_handler.provision_output_port.assert_called_once()
        self.mock_task_repo.update_task.assert_called_once()
        _, kwargs = self.mock_task_repo.update_task.call_args
        self.assertEqual(kwargs["status"], Status1.COMPLETED)
        self.assertIn("tableFullName", kwargs["info"].publicInfo)

    def test_provision_adds_task_to_background(self):
        """Test that the main provision method creates a task and adds it to background tasks."""
        # Arrange
        component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace="test-workspace",
                metastore="test_metastore",
                repoPath="Repos/test/repo",
                jobName="test_job",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="https://test.repo",
                    gitReference="gitReference",
                    gitPath="gitPath",
                    gitReferenceType="branch",
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )
        self.mock_task_repo.create_task.return_value = (self.task_id, MagicMock())

        # Act
        result_id = self.service.provision(self.data_product, component, False)

        # Assert
        self.assertEqual(result_id, self.task_id)
        self.mock_task_repo.create_task.assert_called_once()
        self.mock_background_tasks.add_task.assert_called_once_with(
            self.service._start_provisioning, self.task_id, self.data_product, component, False, True
        )

    async def test_unprovision_skips_if_workspace_not_found(self):
        """Test that unprovisioning completes gracefully if the target workspace doesn't exist."""
        # Arrange
        component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace="test-workspace",
                metastore="test_metastore",
                repoPath="Repos/test/repo",
                jobName="test_job",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="https://test.repo",
                    gitReference="gitReference",
                    gitPath="gitPath",
                    gitReferenceType="branch",
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )
        # Simulate the workspace not being found
        self.mock_workspace_handler.get_workspace_info.return_value = None

        # Act
        await self.service._start_provisioning(self.task_id, self.data_product, component, False, is_provisioning=False)

        # Assert
        # Verify no handler was called
        self.mock_job_handler.unprovision_workload.assert_not_called()
        # Verify the task was marked as COMPLETED with an informational message
        self.mock_task_repo.update_task.assert_called_once()
        _, kwargs = self.mock_task_repo.update_task.call_args
        self.assertEqual(kwargs["status"], Status1.COMPLETED)
        self.assertIn("Unprovision skipped", kwargs["result"])

    async def test_provision_fails_if_workspace_not_ready(self):
        """Test that provisioning fails if the workspace is found but not in a SUCCEEDED state."""
        # Arrange
        not_ready_workspace = self.workspace_info.model_copy(update={"provisioning_state": ProvisioningState.UPDATING})
        component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace="test-workspace",
                metastore="test_metastore",
                repoPath="Repos/test/repo",
                jobName="test_job",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="https://test.repo",
                    gitReference="gitReference",
                    gitPath="gitPath",
                    gitReferenceType="branch",
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )
        self.mock_workspace_handler.provision_workspace.return_value = not_ready_workspace

        # Act
        await self.service._start_provisioning(self.task_id, self.data_product, component, False, is_provisioning=True)

        # Assert
        # Verify no handler was called
        self.mock_job_handler.provision_workload.assert_not_called()
        # Verify the task was marked as FAILED
        self.mock_task_repo.update_task.assert_called_once()
        _, kwargs = self.mock_task_repo.update_task.call_args
        self.assertEqual(kwargs["status"], Status1.FAILED)
        self.assertIn("status of test-workspace workspace is different from 'ACTIVE'", kwargs["result"])

    async def test_start_provisioning_handles_provisioning_error(self):
        """Test that a ProvisioningError from a handler is caught and the task is marked as FAILED."""
        # Arrange
        component = JobWorkload(
            id="comp-id",
            name="test-job-component",
            description="description",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            connectionType="HOUSEKEEPING",
            kind="workload",
            tags=[],
            specific=DatabricksJobWorkloadSpecific(
                workspace="test-workspace",
                metastore="test_metastore",
                repoPath="Repos/test/repo",
                jobName="test_job",
                git=DatabricksJobWorkloadSpecific.JobGitSpecific(
                    gitRepoUrl="https://test.repo",
                    gitReference="gitReference",
                    gitPath="gitPath",
                    gitReferenceType="branch",
                ),
                cluster=JobClusterSpecific(clusterSparkVersion="14.4.5", nodeTypeId="nodeTypeId", numWorkers=1),
            ),
        )
        self.mock_workspace_handler.provision_workspace.return_value = self.workspace_info
        # Simulate a failure in the handler
        error_message = "Mapping failed for principal"
        self.mock_job_handler.provision_workload.side_effect = ProvisioningError([error_message])

        # Act
        await self.service._start_provisioning(self.task_id, self.data_product, component, False, is_provisioning=True)

        # Assert
        self.mock_task_repo.update_task.assert_called_once()
        _, kwargs = self.mock_task_repo.update_task.call_args
        self.assertEqual(kwargs["status"], Status1.FAILED)
        self.assertIn(error_message, kwargs["result"])

    def test_get_provisioning_status(self):
        """Test retrieving the status of an existing task."""
        # Arrange
        expected_status = ProvisioningStatus(status=Status1.RUNNING, result="")
        self.mock_task_repo.get_task.return_value = expected_status

        # Act
        result = self.service.get_provisioning_status(self.task_id)

        # Assert
        self.assertEqual(result, expected_status)
        self.mock_task_repo.get_task.assert_called_once_with(self.task_id)
