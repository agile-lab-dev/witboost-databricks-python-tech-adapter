"""
Custom exceptions for handling Provisioning level errors.
"""


class ProvisioningError(Exception):
    """
    Base exception for all provisioning errors.

    This exception serves as a common parent for all other provisioning exceptions
    in this module.
    """

    def __init__(self, errors: list[str]):
        if not errors:
            raise ValueError("Error while creating exception. You must provide at least one error message")
        self.errors = errors
        super().__init__(",".join(self.errors))


class AsyncHandlingError(ProvisioningError):
    pass


def build_error_message_from_chained_exception(e: BaseException) -> str:
    ex = e
    result = f"{e}"
    while ex.__context__:
        result += f'\n caused by: "{ex.__context__}"'
        ex = ex.__context__
    return result
