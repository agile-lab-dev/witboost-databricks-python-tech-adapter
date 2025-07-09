from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from loguru import logger

from src.models.data_product_descriptor import OpenMetadataColumn
from src.models.databricks.databricks_models import DatabricksOutputPort
from src.models.databricks.exceptions import StatementExecutionError
from src.models.databricks.outputport.databricks_outputport_specific import DatabricksOutputPortSpecific


class StatementExecutionManager:
    def add_all_descriptions(
        self, component: DatabricksOutputPort, sql_warehouse_id: str, workspace_client: WorkspaceClient
    ):
        """
        Adds comments to view columns and the view itself based on the data contract schema descriptions
        and the description of the component itself

        Args:
              component: Databricks Output Port storing the data contract schema and the component description
              sql_warehouse_id: ID of the SQL warehouse to be used to execute the statements
              workspace_client: The client for the target Databricks workspace

        Raises:
        """
        # Add column comments
        if component.dataContract.schema_:
            for col in component.dataContract.schema_:
                statement_id = self._execute_statement_comment_on_column(
                    component.specific, col, sql_warehouse_id, workspace_client
                )
                if statement_id:
                    self.poll_on_statement_execution(workspace_client, statement_id)

        # Add view comment
        statement_id = self._execute_statement_alter_view_set_description(
            component.specific, component.description, sql_warehouse_id, workspace_client
        )
        if statement_id:
            self.poll_on_statement_execution(workspace_client, statement_id)

    def _create_columns_list_for_select(self, schema: list[OpenMetadataColumn]) -> str:
        """Creates a comma-separated list of columns, or '*' if empty."""
        if not schema:
            return "*"
        return ",".join(f"`{c.name}`" for c in schema)

    def _execute_query(self, query: str, catalog: str, schema: str, warehouse_id: str, client: WorkspaceClient) -> str:
        """Executes a SQL query and returns the statement ID."""
        try:
            logger.debug("Executing query: {}", query)
            response = client.statement_execution.execute_statement(
                catalog=catalog, schema=schema, statement=query, warehouse_id=warehouse_id
            )
            if response.statement_id:
                return response.statement_id
            else:
                error_msg = "Error executing query. Received empty response from Databricks"
                logger.error(error_msg)
                logger.debug("Response returned by Databricks for query '{}': {}", query, response)
                raise StatementExecutionError(error_msg)
        except StatementExecutionError:
            raise
        except Exception as e:
            error_msg = f"An error occurred while running query '{query}'"
            logger.error("An error occurred while running query '{}'. Details: {}", query, e)
            raise StatementExecutionError(error_msg) from e

    def poll_on_statement_execution(self, client: WorkspaceClient, statement_id: str) -> None:
        """Polls a statement until it succeeds or fails."""
        while True:
            response = client.statement_execution.get_statement(statement_id)
            if not response.status or not response.status.state:
                error_msg = (
                    f"Error waiting on statement '{statement_id}'  execution outcome. "
                    f"Received empty response from Databricks"
                )
                logger.error(error_msg)
                logger.debug("Response returned by Databricks for '{}': {}", statement_id, response)
                raise StatementExecutionError(error_msg)
            state = response.status.state
            log_msg = "Status of statement (id: {}): {}."

            if state in (StatementState.SUCCEEDED,):
                logger.info(log_msg, statement_id, state)
                break
            if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                error_details = (
                    response.status.error.message
                    if response.status.error
                    else f"Received empty error message: {response.status}"
                )
                log_msg += " Details: {}"
                logger.error(log_msg, statement_id, state, error_details)
                raise StatementExecutionError(
                    f"Statement {statement_id} failed with state {state}. Details: {error_details}"
                )

            logger.info(log_msg + " Still polling.", statement_id, state)
            # time.sleep(2) # Avoid busy-waiting

    def execute_statement_create_or_replace_view(
        self,
        specific: DatabricksOutputPortSpecific,
        schema: list[OpenMetadataColumn],
        sql_warehouse_id: str,
        workspace_client: WorkspaceClient,
    ) -> str:
        """Builds and executes the CREATE OR REPLACE VIEW statement."""
        columns_list = self._create_columns_list_for_select(schema)
        table_full_name = f"`{specific.catalog_name}`.`{specific.schema_name}`.`{specific.table_name}`"
        view_full_name = f"`{specific.catalog_name_op}`.`{specific.schema_name_op}`.`{specific.view_name_op}`"
        query = f"CREATE OR REPLACE VIEW {view_full_name} AS SELECT {columns_list} FROM {table_full_name};"

        return self._execute_query(
            query, specific.catalog_name_op, specific.schema_name_op, sql_warehouse_id, workspace_client
        )

    def _execute_statement_comment_on_column(
        self,
        specific: DatabricksOutputPortSpecific,
        column: OpenMetadataColumn,
        sql_warehouse_id: str,
        workspace_client: WorkspaceClient,
    ) -> Optional[str]:
        """Builds and executes a COMMENT ON COLUMN statement."""
        if not column.description or column.description.isspace():
            return None

        view_full_name = f"`{specific.catalog_name_op}`.`{specific.schema_name_op}`.`{specific.view_name_op}`"
        query = f"""COMMENT ON COLUMN {view_full_name}.`{column.name}` IS '{column.description.replace("'", "''")}'"""
        return self._execute_query(
            query, specific.catalog_name_op, specific.schema_name_op, sql_warehouse_id, workspace_client
        )

    def _execute_statement_alter_view_set_description(
        self, specific, view_description, sql_warehouse_id, workspace_client
    ) -> Optional[str]:
        """Builds and executes an ALTER VIEW statement to set the comment."""
        if not view_description or view_description.isspace():
            return None

        view_full_name = f"`{specific.catalog_name_op}`.`{specific.schema_name_op}`.`{specific.view_name_op}`"
        query = (
            f"""ALTER VIEW {view_full_name} SET TBLPROPERTIES ('comment' = '{view_description.replace("'", "''")}')"""
        )
        return self._execute_query(
            query, specific.catalog_name_op, specific.schema_name_op, sql_warehouse_id, workspace_client
        )
