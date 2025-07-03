from datetime import datetime, timezone

from loguru import logger

from src import settings
from src.models.api_models import Level, Log, ReverseProvisioningRequest, ReverseProvisioningStatus, Status1
from src.models.exceptions import get_error_list_from_chained_exception
from src.service.reverse_provision.workflow_reverse_provision_handler import WorkflowReverseProvisionHandler
from src.utility.use_case_template_id_utils import get_use_case_template_id


class ReverseProvisionService:
    def __init__(self, workflow_reverse_provision_handler: WorkflowReverseProvisionHandler):
        self.workflow_reverse_provision_handler = workflow_reverse_provision_handler

    def run_reverse_provisioning(
        self, reverse_provision_request: ReverseProvisioningRequest
    ) -> ReverseProvisioningStatus:
        use_case_template_id = get_use_case_template_id(reverse_provision_request.useCaseTemplateId)
        if use_case_template_id in settings.usecasetemplateid.workload.workflow:
            try:
                updates = self.workflow_reverse_provision_handler.reverse_provision(reverse_provision_request)
                success_msg = "Reverse provisioning successfully completed."
                logger.success(success_msg)
                return ReverseProvisioningStatus(
                    status=Status1.COMPLETED,
                    updates=updates,
                    logs=[Log(timestamp=datetime.now(timezone.utc), level=Level.INFO, message=success_msg)],
                )
            except Exception as e:
                return self._handle_status_failed(e)
        else:
            return self._handle_status_failed("Other components are not yet supported by this Tech Adapter")  # TODO

    def _handle_status_failed(self, error: str | Exception) -> ReverseProvisioningStatus:
        if isinstance(error, Exception):
            errors = get_error_list_from_chained_exception(error)
            logger.exception(error)
            return ReverseProvisioningStatus(
                status=Status1.FAILED,
                updates={},
                logs=[Log(timestamp=datetime.now(timezone.utc), level=Level.ERROR, message=msg) for msg in errors],
            )
        else:
            logger.exception(error)
            return ReverseProvisioningStatus(
                status=Status1.FAILED,
                updates={},
                logs=[Log(timestamp=datetime.now(timezone.utc), level=Level.ERROR, message=error)],
            )
