import unittest
from unittest.mock import MagicMock

from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.pipelines import (
    CreatePipelineResponse,
    NotebookLibrary,
    Notifications,
    PipelineCluster,
    PipelineClusterAutoscaleMode,
    PipelineLibrary,
    PipelineStateInfo,
)

from src.models.databricks.exceptions import DLTManagerError
from src.models.databricks.workload.databricks_dlt_workload_specific import (
    DLTClusterSpecific,
    PipelineChannel,
    ProductEdition,
)
from src.service.clients.databricks.dlt_manager import DLTManager


class TestDLTManager(unittest.TestCase):
    """Unit tests for the DLTManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_workspace_client = MagicMock()
        self.workspace_name = "test-workspace"
        self.dlt_manager = DLTManager(self.mock_workspace_client, self.workspace_name)

        # Common test data
        self.pipeline_name = "test_pipeline"
        self.pipeline_id = "pipeline_id_123"
        self.base_cluster_specific = DLTClusterSpecific(
            policy_id="policy-123",
            driver_type="driver-type",
            worker_type="worker-type",
            num_workers=2,
            tags={"tag_key": "tag_value"},
        )
        self.base_pipeline_args = {
            "pipeline_name": self.pipeline_name,
            "product_edition": ProductEdition.PRO,
            "continuous": False,
            "notebooks": ["/path/to/notebook.py"],
            "files": [],
            "catalog": "test_catalog",
            "target": "test_target",
            "photon": True,
            "notifications": {"test@email.com": ["on_failure"]},
            "channel": PipelineChannel.CURRENT,
            "cluster_specific": self.base_cluster_specific,
        }

    def test_create_or_update_creates_new_pipeline(self):
        """Test that a new pipeline is created if none exists."""
        # Arrange
        self.mock_workspace_client.pipelines.list_pipelines.return_value = []
        self.mock_workspace_client.pipelines.create.return_value = CreatePipelineResponse(pipeline_id=self.pipeline_id)

        # Act
        result_id = self.dlt_manager.create_or_update_dlt_pipeline(**self.base_pipeline_args)

        # Assert
        self.assertEqual(result_id, self.pipeline_id)
        self.mock_workspace_client.pipelines.list_pipelines.assert_called_once_with(
            filter=f"name LIKE '{self.pipeline_name}'"
        )
        self.mock_workspace_client.pipelines.create.assert_called_once()
        self.mock_workspace_client.pipelines.update.assert_not_called()

    def test_create_or_update_updates_existing_pipeline(self):
        """Test that an existing pipeline is updated if found."""
        # Arrange
        existing_pipeline = PipelineStateInfo(pipeline_id=self.pipeline_id, name=self.pipeline_name)
        self.mock_workspace_client.pipelines.list_pipelines.return_value = [existing_pipeline]

        # Act
        result_id = self.dlt_manager.create_or_update_dlt_pipeline(**self.base_pipeline_args)

        # Assert
        self.assertEqual(result_id, self.pipeline_id)
        self.mock_workspace_client.pipelines.list_pipelines.assert_called_once()
        self.mock_workspace_client.pipelines.update.assert_called_once_with(
            pipeline_id=self.pipeline_id,
            name=self.pipeline_name,
            edition=self.base_pipeline_args["product_edition"],
            continuous=self.base_pipeline_args["continuous"],
            libraries=[
                PipelineLibrary(notebook=NotebookLibrary(path=nb)) for nb in self.base_pipeline_args["notebooks"]
            ],
            catalog=self.base_pipeline_args["catalog"],
            target=self.base_pipeline_args["target"],
            clusters=[
                PipelineCluster(
                    policy_id=self.base_cluster_specific.policy_id,
                    custom_tags=self.base_cluster_specific.tags,
                    driver_node_type_id=self.base_cluster_specific.driver_type,
                    node_type_id=self.base_cluster_specific.worker_type,
                    num_workers=self.base_cluster_specific.num_workers,
                    spark_conf={},
                )
            ],
            photon=self.base_pipeline_args["photon"],
            channel=self.base_pipeline_args["channel"],
            notifications=[
                Notifications(email_recipients=[email], alerts=alerts)
                for email, alerts in self.base_pipeline_args["notifications"].items()
            ],
            configuration={},
        )
        self.mock_workspace_client.pipelines.create.assert_not_called()

    def test_create_or_update_autoscale_cluster(self):
        """Test pipeline creation/update with an autoscale cluster configuration."""
        # Arrange
        autoscale_cluster_specific = DLTClusterSpecific(
            policy_id="policy-123",
            driver_type="driver-type",
            worker_type="worker-type",
            min_workers=1,
            max_workers=5,
            mode=PipelineClusterAutoscaleMode.ENHANCED,
        )
        pipeline_args = {**self.base_pipeline_args, "cluster_specific": autoscale_cluster_specific}
        self.mock_workspace_client.pipelines.list_pipelines.return_value = []
        self.mock_workspace_client.pipelines.create.return_value = CreatePipelineResponse(pipeline_id=self.pipeline_id)

        # Act
        self.dlt_manager.create_or_update_dlt_pipeline(**pipeline_args)

        # Assert
        self.mock_workspace_client.pipelines.create.assert_called_once()
        _, kwargs = self.mock_workspace_client.pipelines.create.call_args
        created_clusters = kwargs.get("clusters", [])
        self.assertEqual(len(created_clusters), 1)
        self.assertIsNotNone(created_clusters[0].autoscale)
        self.assertEqual(created_clusters[0].autoscale.min_workers, 1)
        self.assertEqual(created_clusters[0].autoscale.max_workers, 5)
        self.assertEqual(created_clusters[0].autoscale.mode, PipelineClusterAutoscaleMode.ENHANCED)
        self.assertIsNone(created_clusters[0].num_workers)

    def test_create_or_update_fails_on_multiple_pipelines(self):
        """Test failure when multiple pipelines exist with the same name."""
        # Arrange
        pipelines = [PipelineStateInfo(pipeline_id="id1"), PipelineStateInfo(pipeline_id="id2")]
        self.mock_workspace_client.pipelines.list_pipelines.return_value = pipelines

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "The name is not unique"):
            self.dlt_manager.create_or_update_dlt_pipeline(**self.base_pipeline_args)

    def test_create_fails_with_no_libraries(self):
        """Test that pipeline creation fails if no notebooks or files are provided."""
        # Arrange
        pipeline_args = {**self.base_pipeline_args, "notebooks": [], "files": []}
        self.mock_workspace_client.pipelines.list_pipelines.return_value = []

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "requires at least one notebook or file"):
            self.dlt_manager.create_or_update_dlt_pipeline(**pipeline_args)

    def test_update_fails_with_no_libraries(self):
        """Test that pipeline update fails if no notebooks or files are provided."""
        # Arrange
        pipeline_args = {**self.base_pipeline_args, "notebooks": [], "files": []}
        existing_pipeline = PipelineStateInfo(pipeline_id=self.pipeline_id, name=self.pipeline_name)
        self.mock_workspace_client.pipelines.list_pipelines.return_value = [existing_pipeline]

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "requires at least one notebook or file"):
            self.dlt_manager.create_or_update_dlt_pipeline(**pipeline_args)

    def test_delete_pipeline_successfully(self):
        """Test successful deletion of a pipeline."""
        # Act
        self.dlt_manager.delete_pipeline(self.pipeline_id)

        # Assert
        self.mock_workspace_client.pipelines.delete.assert_called_once_with(pipeline_id=self.pipeline_id)

    def test_delete_pipeline_skips_if_not_exists(self):
        """Test that deletion is skipped if the pipeline does not exist."""
        # Arrange
        self.mock_workspace_client.pipelines.delete.side_effect = ResourceDoesNotExist("Not Found")

        # Act
        try:
            self.dlt_manager.delete_pipeline(self.pipeline_id)
        except DLTManagerError:
            self.fail("DLTManagerError raised unexpectedly on ResourceDoesNotExist")

        # Assert
        self.mock_workspace_client.pipelines.delete.assert_called_once_with(pipeline_id=self.pipeline_id)

    def test_delete_pipeline_fails_on_api_error(self):
        """Test that deletion raises DLTManagerError on other API errors."""
        # Arrange
        self.mock_workspace_client.pipelines.delete.side_effect = Exception("API Error")

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "An error occurred while deleting DLT Pipeline"):
            self.dlt_manager.delete_pipeline(self.pipeline_id)

    def test_retrieve_pipeline_id_from_name_success(self):
        """Test successful retrieval of a unique pipeline ID."""
        # Arrange
        existing_pipeline = PipelineStateInfo(pipeline_id=self.pipeline_id, name=self.pipeline_name)
        self.mock_workspace_client.pipelines.list_pipelines.return_value = [existing_pipeline]

        # Act
        result_id = self.dlt_manager.retrieve_pipeline_id_from_name(self.pipeline_name)

        # Assert
        self.assertEqual(result_id, self.pipeline_id)
        self.mock_workspace_client.pipelines.list_pipelines.assert_called_once()

    def test_retrieve_pipeline_id_fails_if_not_found(self):
        """Test failure to retrieve ID when no pipeline is found."""
        # Arrange
        self.mock_workspace_client.pipelines.list_pipelines.return_value = []

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "no DLT found with that name"):
            self.dlt_manager.retrieve_pipeline_id_from_name(self.pipeline_name)

    def test_retrieve_pipeline_id_fails_if_multiple_found(self):
        """Test failure to retrieve ID when multiple pipelines are found."""
        # Arrange
        pipelines = [PipelineStateInfo(pipeline_id="id1"), PipelineStateInfo(pipeline_id="id2")]
        self.mock_workspace_client.pipelines.list_pipelines.return_value = pipelines

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "more than 1 DLT found with that name"):
            self.dlt_manager.retrieve_pipeline_id_from_name(self.pipeline_name)

    def test_retrieve_pipeline_id_fails_if_id_is_empty(self):
        """Test failure when the found pipeline has an empty ID."""
        # Arrange
        pipeline = PipelineStateInfo(pipeline_id=None, name=self.pipeline_name)
        self.mock_workspace_client.pipelines.list_pipelines.return_value = [pipeline]

        # Act & Assert
        with self.assertRaisesRegex(DLTManagerError, "Received empty response from Databricks"):
            self.dlt_manager.retrieve_pipeline_id_from_name(self.pipeline_name)
