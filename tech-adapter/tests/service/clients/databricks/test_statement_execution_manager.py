import unittest
from unittest.mock import MagicMock, call, patch

from databricks.sdk.service.sql import (
    ServiceError,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from src.models.data_product_descriptor import DataContract, OpenMetadataColumn
from src.models.databricks.databricks_models import DatabricksOutputPort
from src.models.databricks.exceptions import StatementExecutionError
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific
from src.service.clients.databricks.statement_execution_manager import StatementExecutionManager


class TestStatementExecutionManager(unittest.TestCase):
    """Unit tests for the StatementExecutionManager class."""

    def setUp(self):
        """Set up the test environment before each test."""
        self.mock_workspace_client = MagicMock()
        self.manager = StatementExecutionManager()
        self.sql_warehouse_id = "warehouse-123"
        self.statement_id = "statement-abc"

        # Mock data models
        self.specific = DatabricksOutputPortSpecific(
            workspace="workspace_name",
            workspace_op="workspace_name_op",
            sql_warehouse_name="sql_warehouse",
            catalog_name="dev_catalog",
            schema_name="dev_schema",
            table_name="input_table",
            catalog_name_op="prod_catalog",
            schema_name_op="prod_schema",
            view_name_op="output_view",
        )
        schema = [
            OpenMetadataColumn(name="id", dataType="STRING", description="The unique identifier."),
            OpenMetadataColumn(name="value", dataType="STRING", description="The measurement value."),
            OpenMetadataColumn(name="notes", dataType="STRING", description="A user's notes."),
        ]
        self.schema = schema
        self.component = DatabricksOutputPort(
            id="dp-id",
            kind="outputport",
            useCaseTemplateId="useCaseTemplateId",
            infrastructureTemplateId="infrastructureTemplateId",
            version="0.0.0",
            dependsOn=[],
            outputPortType="SQL",
            tags=[],
            semanticLinking=[],
            name="Test Output Port",
            description="A view for testing purposes.",
            specific=self.specific,
            dataContract=DataContract(schema=schema),
        )

    def test_add_all_descriptions_full_success(self):
        """Test adding all descriptions for a component with schema and description."""
        # Arrange
        with patch.object(
            self.manager, "_execute_statement_comment_on_column", return_value="stmt-col"
        ) as mock_comment_col, patch.object(
            self.manager, "_execute_statement_alter_view_set_description", return_value="stmt-view"
        ) as mock_set_desc, patch.object(self.manager, "poll_on_statement_execution") as mock_poll:
            # Act
            self.manager.add_all_descriptions(self.component, self.sql_warehouse_id, self.mock_workspace_client)

            # Assert
            self.assertEqual(mock_comment_col.call_count, len(self.schema))
            mock_comment_col.assert_has_calls(
                [call(self.specific, col, self.sql_warehouse_id, self.mock_workspace_client) for col in self.schema]
            )
            mock_set_desc.assert_called_once_with(
                self.specific, self.component.description, self.sql_warehouse_id, self.mock_workspace_client
            )
            self.assertEqual(mock_poll.call_count, len(self.schema) + 1)
            mock_poll.assert_has_calls(
                [call(self.mock_workspace_client, "stmt-col")] * 3 + [call(self.mock_workspace_client, "stmt-view")]
            )

    def test_add_all_descriptions_skips_when_no_description(self):
        """Test that polling is skipped if execute methods return None."""
        # Arrange
        with patch.object(
            self.manager, "_execute_statement_comment_on_column", return_value=None
        ) as mock_comment_col, patch.object(
            self.manager, "_execute_statement_alter_view_set_description", return_value=None
        ) as mock_set_desc, patch.object(self.manager, "poll_on_statement_execution") as mock_poll:
            # Act
            self.manager.add_all_descriptions(self.component, self.sql_warehouse_id, self.mock_workspace_client)

            # Assert
            self.assertEqual(mock_comment_col.call_count, len(self.schema))
            mock_set_desc.assert_called_once()
            mock_poll.assert_not_called()  # Crucial check

    def test_execute_query_success(self):
        """Test successful execution of a query."""
        # Arrange
        query = "SELECT 1"
        self.mock_workspace_client.statement_execution.execute_statement.return_value = StatementResponse(
            statement_id=self.statement_id
        )

        # Act
        result_id = self.manager._execute_query(query, "cat", "sch", self.sql_warehouse_id, self.mock_workspace_client)

        # Assert
        self.assertEqual(result_id, self.statement_id)
        self.mock_workspace_client.statement_execution.execute_statement.assert_called_once()

    def test_execute_query_fails_on_empty_response(self):
        """Test query execution failure due to an empty response from Databricks."""
        # Arrange
        self.mock_workspace_client.statement_execution.execute_statement.return_value = StatementResponse(
            statement_id=None
        )

        # Act & Assert
        with self.assertRaisesRegex(StatementExecutionError, "Received empty response"):
            self.manager._execute_query("SELECT 1", "cat", "sch", self.sql_warehouse_id, self.mock_workspace_client)

    def test_poll_on_statement_execution_success(self):
        """Test polling until a statement succeeds."""
        # Arrange
        pending_response = StatementResponse(status=StatementStatus(state=StatementState.PENDING))
        succeeded_response = StatementResponse(status=StatementStatus(state=StatementState.SUCCEEDED))
        self.mock_workspace_client.statement_execution.get_statement.side_effect = [
            pending_response,
            succeeded_response,
        ]

        # Act
        try:
            self.manager.poll_on_statement_execution(self.mock_workspace_client, self.statement_id)
        except StatementExecutionError:
            self.fail("StatementExecutionError was raised unexpectedly.")

        # Assert
        self.assertEqual(self.mock_workspace_client.statement_execution.get_statement.call_count, 2)

    def test_poll_on_statement_execution_failure(self):
        """Test polling when a statement fails."""
        # Arrange
        error_message = "Syntax error"
        failed_response = StatementResponse(
            status=StatementStatus(state=StatementState.FAILED, error=ServiceError(message=error_message))
        )
        self.mock_workspace_client.statement_execution.get_statement.return_value = failed_response

        # Act & Assert
        with self.assertRaisesRegex(
            StatementExecutionError, f"failed with state StatementState\\.FAILED. Details: {error_message}"
        ):
            self.manager.poll_on_statement_execution(self.mock_workspace_client, self.statement_id)

    def test_execute_statement_create_or_replace_view(self):
        """Test the construction and execution of a CREATE OR REPLACE VIEW statement."""
        # Arrange
        with patch.object(self.manager, "_execute_query", return_value=self.statement_id) as mock_execute:
            # Act
            result_id = self.manager.execute_statement_create_or_replace_view(
                self.specific, self.schema, self.sql_warehouse_id, self.mock_workspace_client
            )

            # Assert
            self.assertEqual(result_id, self.statement_id)
            mock_execute.assert_called_once()
            called_query = mock_execute.call_args[0][0]
            self.assertIn("CREATE OR REPLACE VIEW `prod_catalog`.`prod_schema`.`output_view`", called_query)
            self.assertIn("SELECT `id`,`value`,`notes` FROM", called_query)
            self.assertIn("`dev_catalog`.`dev_schema`.`input_table`", called_query)

    def test_execute_statement_comment_on_column(self):
        """Test construction of a COMMENT ON COLUMN statement."""
        # Arrange
        column_with_quotes = OpenMetadataColumn(name="col", description="It's a test.", dataType="STRING")
        with patch.object(self.manager, "_execute_query", return_value=self.statement_id) as mock_execute:
            # Act
            self.manager._execute_statement_comment_on_column(
                self.specific, column_with_quotes, self.sql_warehouse_id, self.mock_workspace_client
            )
            # Assert
            mock_execute.assert_called_once()
            called_query = mock_execute.call_args[0][0]
            self.assertIn("COMMENT ON COLUMN `prod_catalog`.`prod_schema`.`output_view`.`col`", called_query)
            self.assertIn("IS 'It''s a test.'", called_query)  # Check for escaped single quote

    def test_execute_statement_comment_on_column_skips_if_no_description(self):
        """Test that COMMENT ON COLUMN is skipped if description is missing."""
        # Arrange
        column_no_desc = OpenMetadataColumn(name="col", description="", dataType="STRING")
        with patch.object(self.manager, "_execute_query") as mock_execute:
            # Act
            result = self.manager._execute_statement_comment_on_column(
                self.specific, column_no_desc, self.sql_warehouse_id, self.mock_workspace_client
            )
            # Assert
            self.assertIsNone(result)
            mock_execute.assert_not_called()
