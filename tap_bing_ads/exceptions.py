"""
Custom exception classes for tap-bing-ads.
"""
import time


class BingAdsError(Exception):
    """Base exception class for Bing Ads tap."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class BingAdsBackoffError(BingAdsError):
    """Raised for errors that should trigger a backoff retry."""
    pass


class BingAdsBadRequestError(BingAdsError):
    """HTTP 400 Bad Request."""
    pass


class BingAdsUnauthorizedError(BingAdsError):
    """HTTP 401 Unauthorized — token may need refresh."""
    pass


class BingAdsForbiddenError(BingAdsError):
    """HTTP 403 Forbidden — insufficient permissions."""
    pass


class BingAdsNotFoundError(BingAdsError):
    """HTTP 404 Not Found."""
    pass


class BingAdsRateLimitError(BingAdsBackoffError):
    """HTTP 429 Too Many Requests."""

    def __init__(self, message=None, response=None):
        self.response = response
        self.retry_after = 60

        if response is not None:
            headers = response.headers or {}
            retry_after_val = headers.get("Retry-After") or headers.get("retry-after")
            if retry_after_val:
                try:
                    self.retry_after = int(retry_after_val)
                except (ValueError, TypeError):
                    self.retry_after = 60

        full_message = f"{message or 'Rate limit exceeded.'} (Retry after {self.retry_after} seconds.)"
        super().__init__(full_message, response=response)


class BingAdsServerError(BingAdsBackoffError):
    """HTTP 5xx Server Error."""
    pass


class BingAdsReportError(BingAdsError):
    """Raised when a report request returns an Error status."""
    pass


class BingAdsInvalidDateRangeEnd(BingAdsError):
    """Raised when report date range end is outside data retention window."""
    pass


class BingAdsNoMeasureSelected(BingAdsError):
    """Raised when report request contains no metric (measure) columns."""
    pass


class BingAdsInvalidFieldSelection(BingAdsError):
    """Raised when mutually exclusive fields are selected together."""
    pass


class OAuthTokenError(BingAdsError):
    """Raised when OAuth token refresh fails."""
    pass


# HTTP status → exception mapping
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {"raise_exception": BingAdsBadRequestError, "message": "Bad request."},
    401: {"raise_exception": BingAdsUnauthorizedError, "message": "Unauthorized. Check credentials."},
    403: {"raise_exception": BingAdsForbiddenError, "message": "Forbidden. Insufficient permissions."},
    404: {"raise_exception": BingAdsNotFoundError, "message": "Resource not found."},
    429: {"raise_exception": BingAdsRateLimitError, "message": "Rate limit exceeded."},
    500: {"raise_exception": BingAdsServerError, "message": "Internal server error."},
    502: {"raise_exception": BingAdsServerError, "message": "Bad gateway."},
    503: {"raise_exception": BingAdsServerError, "message": "Service unavailable."},
    504: {"raise_exception": BingAdsServerError, "message": "Gateway timeout."},
}
