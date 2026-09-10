"""Unit tests for the tap-bing-ads REST client."""
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

import requests
from parameterized import parameterized

from tap_bing_ads.client import Client
from tap_bing_ads.exceptions import (
    BingAdsBadRequestError,
    BingAdsBackoffError,
    BingAdsForbiddenError,
    BingAdsNotFoundError,
    BingAdsRateLimitError,
    BingAdsServerError,
    BingAdsUnauthorizedError,
    OAuthTokenError,
)

DEFAULT_CONFIG = {
    "oauth_client_id": "test_client_id",
    "oauth_client_secret": "test_client_secret",
    "refresh_token": "test_refresh_token",
    "developer_token": "test_dev_token",
    "customer_id": "12345",
    "account_ids": "67890",
    "start_date": "2024-01-01T00:00:00Z",
}


class MockResponse:
    def __init__(self, status_code, json_data=None, headers=None, content=b""):
        self.status_code = status_code
        self._json = json_data or {}
        self.headers = headers or {}
        self.content = content
        self.text = str(json_data)

    def json(self):
        return self._json


class TestClientInitialization(unittest.TestCase):
    """Tests for Client.__init__ timeout handling."""

    @parameterized.expand([
        ("empty_string", "", 300.0),
        ("string_number", "30", 30.0),
        ("int_value", 60, 60.0),
        ("float_value", 45.5, 45.5),
        ("zero_int", 0, 300.0),
        ("zero_string", "0", 300.0),
        ("none_value", None, 300.0),
    ])
    def test_request_timeout_parsing(self, name, timeout_input, expected):
        config = {**DEFAULT_CONFIG, "request_timeout": timeout_input}
        client = Client(config)
        self.assertEqual(client.request_timeout, expected)

    def test_default_timeout_when_key_missing(self):
        client = Client(DEFAULT_CONFIG)
        self.assertEqual(client.request_timeout, 300.0)


class TestClientOAuthRefresh(unittest.TestCase):
    """Tests for OAuth token refresh logic."""

    @patch("tap_bing_ads.client.Session")
    def test_successful_token_refresh(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.return_value = MockResponse(200, {"access_token": "new_token_abc"})

        client = Client(DEFAULT_CONFIG)
        client._session = mock_session
        client._refresh_access_token()

        self.assertEqual(client._access_token, "new_token_abc")

    @patch("tap_bing_ads.client.Session")
    def test_token_refresh_raises_on_non_200(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.return_value = MockResponse(400, {"error": "invalid_grant"})

        client = Client(DEFAULT_CONFIG)
        client._session = mock_session

        with self.assertRaises(OAuthTokenError):
            client._refresh_access_token()

    @patch("tap_bing_ads.client.Session")
    def test_token_refresh_raises_when_no_access_token_in_response(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.return_value = MockResponse(200, {"not_a_token": "x"})

        client = Client(DEFAULT_CONFIG)
        client._session = mock_session

        with self.assertRaises(OAuthTokenError):
            client._refresh_access_token()

    @patch("tap_bing_ads.client.Session")
    def test_token_refresh_raises_on_connection_error(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.post.side_effect = ConnectionError("timeout")

        client = Client(DEFAULT_CONFIG)
        client._session = mock_session

        with self.assertRaises(OAuthTokenError):
            client._refresh_access_token()


class TestClientBuildHeaders(unittest.TestCase):
    """Tests for header construction."""

    def test_headers_without_account_id(self):
        client = Client(DEFAULT_CONFIG)
        with patch.object(client, "get_access_token", return_value="tok123"):
            headers = client._build_headers()
        self.assertEqual(headers["Authorization"], "Bearer tok123")
        self.assertEqual(headers["DeveloperToken"], DEFAULT_CONFIG["developer_token"])
        self.assertEqual(headers["CustomerId"], DEFAULT_CONFIG["customer_id"])
        self.assertNotIn("CustomerAccountId", headers)

    def test_headers_with_account_id(self):
        client = Client(DEFAULT_CONFIG)
        with patch.object(client, "get_access_token", return_value="tok123"):
            headers = client._build_headers(account_id="99999")
        self.assertEqual(headers["CustomerAccountId"], "99999")


class TestRaiseForError(unittest.TestCase):
    """Tests for raise_for_error function."""

    from tap_bing_ads.client import raise_for_error

    @parameterized.expand([
        ("400", 400, BingAdsBadRequestError),
        ("401", 401, BingAdsUnauthorizedError),
        ("403", 403, BingAdsForbiddenError),
        ("404", 404, BingAdsNotFoundError),
        ("429", 429, BingAdsRateLimitError),
        ("500", 500, BingAdsServerError),
        ("503", 503, BingAdsServerError),
    ])
    def test_raises_correct_exception_for_status(self, name, status_code, expected_exc):
        from tap_bing_ads.client import raise_for_error
        resp = MockResponse(status_code, {"message": "Error"})
        with self.assertRaises(expected_exc):
            raise_for_error(resp)

    def test_no_exception_for_200(self):
        from tap_bing_ads.client import raise_for_error
        resp = MockResponse(200, {"data": "ok"})
        # Should not raise
        raise_for_error(resp)

    def test_no_exception_for_201(self):
        from tap_bing_ads.client import raise_for_error
        resp = MockResponse(201, {})
        raise_for_error(resp)


class TestClientRequest(unittest.TestCase):
    """Tests for Client.make_request method."""

    def _make_client(self):
        client = Client(DEFAULT_CONFIG)
        client._access_token = "test_token"
        client._expires_at = None  # force get_access_token to use _access_token
        return client

    @patch("tap_bing_ads.client.Client._execute_request")
    def test_successful_get_request(self, mock_execute):
        mock_execute.return_value = MockResponse(200, {"Campaigns": []})
        client = self._make_client()
        result = client.make_request("GET", "https://example.com/api", account_id="123")
        self.assertEqual(result, {"Campaigns": []})

    @patch("tap_bing_ads.client.Client._execute_request")
    @patch("tap_bing_ads.client.Client._refresh_access_token")
    def test_retries_on_401_and_succeeds(self, mock_refresh, mock_execute):
        mock_execute.side_effect = [
            MockResponse(401, {"message": "Unauthorized"}),
            MockResponse(200, {"Campaigns": [{"Id": 1}]}),
        ]
        client = self._make_client()
        result = client.make_request("GET", "https://example.com/api")
        mock_refresh.assert_called_once()
        self.assertEqual(result["Campaigns"][0]["Id"], 1)

    @patch("tap_bing_ads.client.Client._execute_request")
    def test_raises_on_403(self, mock_execute):
        mock_execute.return_value = MockResponse(403, {"message": "Forbidden"})
        client = self._make_client()
        with self.assertRaises(BingAdsForbiddenError):
            client.make_request("GET", "https://example.com/api")

    @patch("tap_bing_ads.client.Client._execute_request")
    def test_returns_empty_dict_on_204(self, mock_execute):
        mock_execute.return_value = MockResponse(204, content=b"")
        client = self._make_client()
        result = client.make_request("GET", "https://example.com/api")
        self.assertEqual(result, {})
