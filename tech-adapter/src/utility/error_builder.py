# --- Found in: common/error_builder.py ---

from typing import Optional

from src.models.api_models import ErrorMoreInfo, RequestValidationError


def build_request_validation_error(
    problems: Optional[list[str]] = None,
    solutions: Optional[list[str]] = None,
    message: Optional[str] = None,
    input_str: Optional[str] = None,
    input_error_field: Optional[str] = None,
) -> RequestValidationError:
    """
    Builds a detailed RequestValidationError object from a FailedOperation.

    This function aggregates problem descriptions and solutions from a failed
    operation to construct a user-friendly and informative validation error response.

    Args:
        problems: A list of problems encountered.
        solutions: A list of hints to the user to solve the encountered problems.
        message: An optional custom user-facing message. If not provided, a
                 default message is used.
        input_str: An optional string representing the input that caused the error.
        input_error_field: An optional string specifying the field within the input
                           that is invalid.

    Returns:
        A fully constructed RequestValidationError object.
    """

    # Collect all solutions from all problems
    if not solutions:
        solutions = []
    solutions.append("If the problem persists, contact the platform team")

    user_msg = message or "Validation on the received descriptor failed, check the error details for more information"

    more_info = ErrorMoreInfo(problems=problems, solutions=solutions)

    return RequestValidationError(
        user_message=user_msg,
        errors=problems,
        input_str=input_str,
        input_error_field=input_error_field,
        more_info=more_info,
    )
