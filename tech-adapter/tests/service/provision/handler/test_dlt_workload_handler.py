import unittest
from unittest.mock import MagicMock, patch

from azure.mgmt.databricks.models import ProvisioningState
from databricks.sdk.service.pipelines import PipelineStateInfo

from src.models.data_product_descriptor import DataProduct
from src.models.databricks.databricks_models import DLTWorkload
from src.models.databricks.databricks_workspace_info import DatabricksWorkspaceInfo
from src.models.databricks.workload.databricks_dlt_workload_specific import (
    DatabricksDLTWorkloadSpecific,
    DLTClusterSpecific,
)
from src.models.databricks.workload.databricks_workload_specific import GitSpecific
from src.models.exceptions import ProvisioningError
from src.service.provision.handler.base_workload_handler import BaseWorkloadHandler
from src.service.provision.handler.dlt_workload_handler import DLTWorkloadHandler


class TestDLTWorkloadHandler(unittest.TestCase):
    """Unit tests for the DLTWorkloadHandler class."""

    def setUp(self):
        """Set up the test environment with minimal dependencies."""
        self.mock_account_client = MagicMock()
        self.mock_workspace_client = MagicMock()

        # The handler under test
        self.handler = DLTWorkloadHandler(self.mock_account_client)

        # --- Common Test Data ---
        self.owner_principal = "user:owner_test.com"
        self.dev_group_principal = "group:dev-group"
        self.data_product = DataProduct(
            id="dp-id",
            dataProductOwner=self.owner_principal,
            devGroup=self.dev_group_principal,
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

        # Create a realistic component object using the Pydantic models
        self.dlt_specific = DatabricksDLTWorkloadSpecific(
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
        )
        self.dlt_component = DLTWorkload(
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
            specific=self.dlt_specific,
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

    @patch.object(BaseWorkloadHandler, "create_repository_with_permissions", new_callable=MagicMock)
    @patch.object(BaseWorkloadHandler, "map_principals", new_callable=MagicMock)
    @patch("src.service.provision.handler.dlt_workload_handler.DLTManager")
    @patch("src.service.provision.handler.dlt_workload_handler.UnityCatalogManager")
    def test_provision_workload_success_for_managed_workspace(
        self, MockUnityCatalogManager, MockDLTManager, mock_map_principals, mock_create_repo
    ):
        """Test the successful provisioning flow for a managed workspace."""
        # Arrange
        mock_uc_manager = MockUnityCatalogManager.return_value
        mock_dlt_manager = MockDLTManager.return_value
        mock_dlt_manager.create_or_update_dlt_pipeline.return_value = "pipeline-id-123"
        mock_map_principals.return_value = {
            self.owner_principal: "owner@test.com",
            self.dev_group_principal: self.dev_group_principal,
        }

        # Act
        result_id = self.handler.provision_workload(
            self.data_product, self.dlt_component, self.mock_workspace_client, self.workspace_info
        )

        # Assert
        self.assertEqual(result_id, "pipeline-id-123")
        # Verify orchestration steps
        mock_uc_manager.attach_metastore.assert_called_once_with(self.dlt_specific.metastore)
        mock_uc_manager.create_catalog_if_not_exists.assert_called_once_with(self.dlt_specific.catalog)
        mock_map_principals.assert_called_once_with(self.data_product, self.dlt_component)
        mock_create_repo.assert_called_once()
        mock_dlt_manager.create_or_update_dlt_pipeline.assert_called_once()
        # Check a few key parameters passed to the DLT manager
        _, kwargs = mock_dlt_manager.create_or_update_dlt_pipeline.call_args
        self.assertEqual(kwargs["pipeline_name"], self.dlt_specific.pipeline_name)
        self.assertEqual(kwargs["catalog"], self.dlt_specific.catalog)
        self.assertEqual(kwargs["target"], self.dlt_specific.target)

    @patch.object(BaseWorkloadHandler, "create_repository_with_permissions", new_callable=MagicMock)
    @patch.object(BaseWorkloadHandler, "map_principals", new_callable=MagicMock)
    @patch("src.service.provision.handler.dlt_workload_handler.DLTManager")
    @patch("src.service.provision.handler.dlt_workload_handler.UnityCatalogManager")
    def test_provision_workload_skips_metastore_for_unmanaged_workspace(
        self, MockUnityCatalogManager, MockDLTManager, mock_map_principals, mock_create_repo
    ):
        """Test that metastore attachment is skipped for an unmanaged workspace."""
        # Arrange
        unmanaged_workspace_info = self.workspace_info.model_copy(update={"is_managed": False})
        mock_uc_manager = MockUnityCatalogManager.return_value
        mock_dlt_manager = MockDLTManager.return_value
        mock_map_principals.return_value = {
            self.owner_principal: "owner@test.com",
            self.dev_group_principal: self.dev_group_principal,
        }

        # Act
        self.handler.provision_workload(
            self.data_product, self.dlt_component, self.mock_workspace_client, unmanaged_workspace_info
        )

        # Assert
        # The key assertion: attach_metastore should NOT be called.
        mock_uc_manager.attach_metastore.assert_not_called()
        # Other steps should still proceed.
        mock_uc_manager.create_catalog_if_not_exists.assert_called_once()
        mock_create_repo.assert_called_once()
        mock_dlt_manager.create_or_update_dlt_pipeline.assert_called_once()

    def test_provision_workload_fails_if_mapping_fails(self):
        """Test that provisioning fails if principal mapping returns an empty result."""
        # Arrange
        with patch.object(BaseWorkloadHandler, "map_principals") as mock_map_principals:
            # Simulate a failure where the owner is not found in the mapped results
            mock_map_principals.return_value = {self.dev_group_principal: self.dev_group_principal}

            self.workspace_info.is_managed = False

            # Act & Assert
            with self.assertRaisesRegex(ProvisioningError, "Failed to retrieve outcome of mapping"):
                self.handler.provision_workload(
                    self.data_product, self.dlt_component, self.mock_workspace_client, self.workspace_info
                )

    def test_unprovision_workload_with_remove_data(self):
        """Test unprovisioning that includes deleting both the pipeline and the repo."""
        # Arrange
        with patch("src.service.provision.handler.dlt_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.dlt_workload_handler.DLTManager"
        ) as MockDLTManager:
            mock_dlt_manager = MockDLTManager.return_value
            mock_repo_manager = MockRepoManager.return_value
            # Simulate finding one pipeline to delete
            mock_dlt_manager.list_pipelines_with_given_name.return_value = [
                PipelineStateInfo(pipeline_id="p-id-1", name=self.dlt_specific.pipeline_name)
            ]

            # Act
            self.handler.unprovision_workload(
                self.data_product,
                self.dlt_component,
                remove_data=True,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

            # Assert
            mock_dlt_manager.list_pipelines_with_given_name.assert_called_once_with(self.dlt_specific.pipeline_name)
            mock_dlt_manager.delete_pipeline.assert_called_once_with("p-id-1")
            mock_repo_manager.delete_repo.assert_called_once_with(
                self.dlt_specific.git.gitRepoUrl, f"/{self.dlt_specific.repoPath}"
            )

    def test_unprovision_workload_without_remove_data(self):
        """Test unprovisioning that deletes the pipeline but skips deleting the repo."""
        # Arrange
        with patch("src.service.provision.handler.dlt_workload_handler.RepoManager") as MockRepoManager, patch(
            "src.service.provision.handler.dlt_workload_handler.DLTManager"
        ) as MockDLTManager:
            mock_dlt_manager = MockDLTManager.return_value
            mock_repo_manager = MockRepoManager.return_value
            mock_dlt_manager.list_pipelines_with_given_name.return_value = [
                PipelineStateInfo(pipeline_id="p-id-1", name=self.dlt_specific.pipeline_name)
            ]

            # Act
            self.handler.unprovision_workload(
                self.data_product,
                self.dlt_component,
                remove_data=False,
                workspace_client=self.mock_workspace_client,
                workspace_info=self.workspace_info,
            )

            # Assert
            mock_dlt_manager.delete_pipeline.assert_called_once_with("p-id-1")
            # The key assertion: repo deletion should NOT be called.
            mock_repo_manager.delete_repo.assert_not_called()

    def test_unprovision_workload_handles_partial_deletion_failure(self):
        """
        Test that unprovisioning continues if one pipeline fails to delete,
        and the error is correctly reported.
        """
        # Arrange
        # We only need to patch the managers instantiated inside the method
        with patch("src.service.provision.handler.dlt_workload_handler.RepoManager"), patch(
            "src.service.provision.handler.dlt_workload_handler.DLTManager"
        ) as MockDLTManager:
            mock_dlt_manager = MockDLTManager.return_value

            # Simulate finding two pipelines that match the name
            pipelines_to_delete = [
                PipelineStateInfo(pipeline_id="p-id-success", name=self.dlt_specific.pipeline_name),
                PipelineStateInfo(pipeline_id="p-id-fail", name=self.dlt_specific.pipeline_name),
            ]
            mock_dlt_manager.list_pipelines_with_given_name.return_value = pipelines_to_delete

            # Configure the mock to raise an error only for the "p-id-fail" pipeline
            original_error = RuntimeError("API delete failed!")

            def delete_side_effect(pipeline_id):
                if pipeline_id == "p-id-fail":
                    raise original_error
                # For any other ID (e.g., "p-id-success"), do nothing (succeed).
                return None

            mock_dlt_manager.delete_pipeline.side_effect = delete_side_effect

            # Act & Assert
            # We expect a ProvisioningError that wraps the original failure.
            with self.assertRaises(ProvisioningError) as cm:
                self.handler.unprovision_workload(
                    self.data_product,
                    self.dlt_component,
                    remove_data=False,  # Set to False to focus the test on pipeline deletion logic
                    workspace_client=self.mock_workspace_client,
                    workspace_info=self.workspace_info,
                )

            # Assert that the final error message contains the original error's message
            self.assertIn("API delete failed!", str(cm.exception))

            # CRITICAL: Assert that the loop continued and attempted to delete BOTH pipelines,
            # not just stopping after the first failure.
            self.assertEqual(mock_dlt_manager.delete_pipeline.call_count, 2)
            mock_dlt_manager.delete_pipeline.assert_any_call("p-id-success")
            mock_dlt_manager.delete_pipeline.assert_any_call("p-id-fail")

    def test_provision_workload_fails_on_managed_workspace_without_metastore(self):
        """
        Test that provisioning fails for a managed workspace if the component specific
        lacks a metastore name.
        """
        # Arrange
        # We start with a valid component and then invalidate it by removing the metastore.
        self.dlt_component.specific.metastore = None

        with patch.object(BaseWorkloadHandler, "create_repository_with_permissions"), patch.object(
            BaseWorkloadHandler, "map_principals"
        ), patch("src.service.provision.handler.dlt_workload_handler.DLTManager"), patch(
            "src.service.provision.handler.dlt_workload_handler.UnityCatalogManager"
        ) as MockUnityCatalogManager:
            mock_unity_catalog_manager = MockUnityCatalogManager.return_value

            # Act & Assert
            # Expect a ProvisioningError with a specific message about the missing metastore.
            with self.assertRaisesRegex(ProvisioningError, "metastore name is not provided"):
                self.handler.provision_workload(
                    self.data_product, self.dlt_component, self.mock_workspace_client, self.workspace_info
                )

            # Assert that the metastore attachment was NOT called, as the failure
            # should happen before this step.
            mock_unity_catalog_manager.attach_metastore.assert_not_called()
