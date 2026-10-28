"""
REST HTTP client for the Microsoft Advertising (Bing Ads) API.

Handles:
 - OAuth 2.0 refresh-token flow (no SDK dependency)
 - Authorization headers for all API services
 - HTTP error handling and backoff retries
 - Token auto-refresh on 401
"""
from typing import Any, Dict, Mapping, Optional
from datetime import datetime, timedelta
import json

import backoff
import requests
from requests import Session
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
from singer import get_logger, metrics

from tap_bing_ads.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    BingAdsBackoffError,
    BingAdsError,
    BingAdsRateLimitError,
    BingAdsUnauthorizedError,
    OAuthTokenError,
)

LOGGER = get_logger()

REQUEST_TIMEOUT = 300
DEFAULT_TOKEN_EXPIRY_SECONDS = 3600

# Production base URLs for Microsoft Advertising REST API v13
CAMPAIGN_BASE_URL = "https://campaign.api.bingads.microsoft.com/CampaignManagement/v13"
CUSTOMER_BASE_URL = "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13"
REPORTING_BASE_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13"

# OAuth endpoints
OAUTH_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
OAUTH_SCOPE = "https://ads.microsoft.com/msads.manage offline_access"


def raise_for_error(response: requests.Response) -> None:
    """Raise an appropriate exception based on the HTTP response status code."""
    if response.status_code in (200, 201, 204):
        return

    try:
        body = response.json()
    except Exception:
        body = {}

    error_message = None
    # Microsoft Advertising REST API wraps errors in various structures
    if isinstance(body, dict):
        LOGGER.info("Error response body: %s", body)
        # Top-level message field
        error_message = body.get("message") or body.get("Message")
        # ApiFaultDetail: OperationErrors / BatchErrors
        if not error_message:
            for key in ("OperationErrors", "BatchErrors", "errors", "Errors"):
                errors = body.get(key) or []
                if errors and isinstance(errors, list):
                    error_message = "; ".join(
                        str(
                            e.get("Message") or e.get("message")
                            or e.get("ErrorCode") or e
                        )
                        for e in errors[:3]
                    )
                    break

    if not error_message:
        error_message = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
            "message", "Unknown API error."
        )

    full_message = f"HTTP {response.status_code}: {error_message}"
    exc_class = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
        "raise_exception", BingAdsError
    )

    if 500 <= response.status_code < 600 and response.status_code not in ERROR_CODE_EXCEPTION_MAPPING:
        exc_class = BingAdsBackoffError

    raise exc_class(full_message, response) from None


def _get_retry_after(exception_info) -> int:
    """Return the Retry-After wait time in seconds for rate-limit backoff."""
    exc = exception_info.get("exception") if isinstance(exception_info, dict) else exception_info
    if exc and isinstance(exc, BingAdsRateLimitError):
        wait = exc.retry_after or 60
        LOGGER.info("Rate limited by Microsoft Advertising API. Waiting %s seconds.", wait)
        return wait
    return 60


class Client:
    """
    REST HTTP client for the Microsoft Advertising API.

    Manages OAuth token lifecycle and exposes a ``request()`` method used by
    stream classes to fetch data.
    """

    def __init__(self, config: Mapping[str, Any], config_path: Optional[str] = None) -> None:
        self.config = config
        self._config_path = config_path
        self._session = Session()

        # Restore persisted token from config when available
        self._access_token: Optional[str] = config.get("access_token")
        self._expires_at: Optional[datetime] = None
        saved_expiry = config.get("token_expires_at")
        if saved_expiry:
            try:
                expiry_str = saved_expiry
                if isinstance(expiry_str, str) and expiry_str.endswith("Z"):
                    expiry_str = expiry_str[:-1] + "+00:00"
                self._expires_at = datetime.fromisoformat(expiry_str)
            except (ValueError, TypeError):
                self._expires_at = None

        timeout_cfg = config.get("request_timeout")
        try:
            t = float(timeout_cfg) if timeout_cfg else 0.0
            self.request_timeout = t if t > 0 else REQUEST_TIMEOUT
        except (ValueError, TypeError):
            self.request_timeout = REQUEST_TIMEOUT

    def __enter__(self):
        # Reuse the saved token if it is still valid; only refresh when missing or expired
        if self._access_token and self._expires_at and self._expires_at > datetime.now(tz=self._expires_at.tzinfo):
            LOGGER.info("Reusing existing access token from config (expires at %s).", self._expires_at)
        else:
            self._refresh_access_token()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _refresh_access_token(self) -> None:
        """Exchange the refresh_token for a fresh access_token and persist it."""
        LOGGER.info("Refreshing Microsoft Advertising access token.")
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.config["oauth_client_id"],
            "client_secret": self.config["oauth_client_secret"],
            "refresh_token": self.config["refresh_token"],
            "scope": OAUTH_SCOPE,
        }

        try:
            resp = self._session.post(OAUTH_TOKEN_URL, data=payload, timeout=30)
        except Exception as exc:
            raise OAuthTokenError(f"Failed to contact OAuth token endpoint: {exc}") from exc

        if resp.status_code != 200:
            raise OAuthTokenError(
                f"OAuth token refresh failed ({resp.status_code}): {resp.text}"
            )

        token_data = resp.json()
        self._access_token = token_data.get("access_token")
        if not self._access_token:
            raise OAuthTokenError("OAuth response did not contain an access_token.")

        expires_in = token_data.get("expires_in", DEFAULT_TOKEN_EXPIRY_SECONDS)
        self._expires_at = datetime.now() + timedelta(seconds=int(expires_in))
        self._write_config()
        LOGGER.info("Access token refreshed (expires at %s).", self._expires_at)

    def _write_config(self) -> None:
        """Persist the current access token and expiry back to the config file."""
        if not self._config_path:
            return
        try:
            LOGGER.info("Persisting refreshed credentials to config file.")
            with open(self._config_path) as fh:
                config_data = json.load(fh)
            config_data["access_token"] = self._access_token
            config_data["token_expires_at"] = self._expires_at.isoformat()
            with open(self._config_path, "w") as fh:
                json.dump(config_data, fh, indent=2)
        except Exception as exc:
            LOGGER.warning("Failed to persist access token to config file: %s", exc)

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if expired."""
        if self._access_token and self._expires_at and self._expires_at > datetime.now(tz=self._expires_at.tzinfo):
            return self._access_token
        self._refresh_access_token()
        return self._access_token

    def _build_headers(self, account_id: Optional[str] = None) -> Dict[str, str]:
        """Build the standard HTTP headers for Microsoft Advertising REST API calls."""
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "DeveloperToken": self.config["developer_token"],
            "CustomerId": str(self.config.get("customer_id", "")),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if account_id:
            headers["CustomerAccountId"] = str(account_id)
        return headers

    # ------------------------------------------------------------------
    # HTTP request with retry/backoff
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.expo,
        (BingAdsBackoffError, ConnectionError, Timeout, ChunkedEncodingError),
        max_tries=5,
        factor=2,
        on_backoff=lambda details: LOGGER.info(
            "Backoff retry #%d for %s", details["tries"], details.get("args")
        ),
    )
    def _execute_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict] = None,
        json_body: Optional[Dict] = None,
        account_id: Optional[str] = None,
    ) -> requests.Response:
        headers = self._build_headers(account_id)
        # LOGGER.info(
        #     "REQUEST %s %s | params=%s | body=%s",
        #     method.upper(), url, params, json_body,
        #     headers["DeveloperToken"], headers["CustomerId"], headers.get("CustomerAccountId")
        # )
        resp = self._session.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=self.request_timeout,
        )
        return resp

    def make_request(
        self,
        method: str,
        url: str,
        params: Optional[Dict] = None,
        json_body: Optional[Dict] = None,
        account_id: Optional[str] = None,
    ) -> Any:
        """
        Make an authenticated REST API call using the given HTTP method.
        Handles token refresh on 401 and raises typed exceptions for errors.

        Args:
            method:     HTTP method string — "GET" or "POST".
            url:        Full endpoint URL.
            params:     URL query parameters (GET requests).
            json_body:  JSON request body (POST requests).
            account_id: When provided, sets the CustomerAccountId header.

        Returns the parsed JSON body (dict), or {} for 204 responses.
        """
        with metrics.http_request_timer(url):
            resp = self._execute_request(method, url, params, json_body, account_id)

        # Refresh token and retry once on 401
        if resp.status_code == 401:
            LOGGER.warning("Received 401 — refreshing access token and retrying.")
            self._refresh_access_token()
            with metrics.http_request_timer(url):
                resp = self._execute_request(method, url, params, json_body, account_id)
                LOGGER.info(resp.json())
        raise_for_error(resp)

        if resp.status_code == 204:
            return {}

        try:
            return resp.json()
        except Exception as exc:
            raise BingAdsError(
                f"Failed to parse JSON response from {url} "
                f"(HTTP {resp.status_code}): {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Download helpers (for report ZIP files)
    # ------------------------------------------------------------------

    @backoff.on_exception(
        backoff.constant,
        (ConnectionError, Timeout),
        max_tries=5,
        interval=10,
    )
    def download_file(self, url: str) -> bytes:
        """Download a binary file (e.g. report ZIP) from the given URL."""
        with metrics.http_request_timer("download_report"):
            resp = self._session.get(url, timeout=self.request_timeout)

        if resp.status_code != 200:
            raise BingAdsError(
                f"Failed to download report file. HTTP {resp.status_code}: {url}"
            )
        return resp.content
