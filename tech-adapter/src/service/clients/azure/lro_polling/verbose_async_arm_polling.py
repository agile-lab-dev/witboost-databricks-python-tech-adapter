from azure.core.pipeline.policies._utils import get_retry_after
from azure.core.polling.base_polling import OperationFailed, _failed, _raise_if_bad_http_status_and_method
from azure.mgmt.core.polling.async_arm_polling import (
    AsyncARMPolling,
)
from loguru import logger


class VerboseAsyncARMPolling(AsyncARMPolling):
    """
    Custom implementation of AsyncARMPolling where we add a debug logging message during the polling.
    The rest of the _poll implementation is left as-is.
    https://github.com/Azure/azure-sdk-for-python/blob/main/doc/dev/customize_long_running_operation.md
    """

    def __init__(
        self,
        operation_description: str,
        timeout: float = 30,
    ):
        self.operation_description = operation_description
        super().__init__(timeout)

    async def _poll(self) -> None:
        """Poll status of operation so long as operation is incomplete and
        we have an endpoint to query.

        Logs a debug message on each poll cycle

        :raises: OperationFailed if operation status 'Failed' or 'Canceled'.
        :raises: BadStatus if response status invalid.
        :raises: BadResponse if response invalid.
        """
        if not self.finished():
            await self.update_status()
        while not self.finished():
            logger.debug(
                "Long Running Operation '{}' is not yet finished. Sleeping for {}s...",
                self.operation_description,
                get_retry_after(self._pipeline_response),
            )
            await self._delay()
            await self.update_status()

        if _failed(self.status()):
            raise OperationFailed("Operation failed or canceled")

        final_get_url = self._operation.get_final_get_url(self._pipeline_response)
        if final_get_url:
            self._pipeline_response = await self.request_status(final_get_url)
            _raise_if_bad_http_status_and_method(self._pipeline_response.http_response)
