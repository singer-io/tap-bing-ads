"""
Supplementary unit tests targeting lines not covered by the primary test suite.
Each test class is focused on a specific module / code path.
"""
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, mock_open, call

import arrow
import singer
from singer.catalog import CatalogEntry, Schema
from singer import metadata

# ── shared helpers ─────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "oauth_client_id": "id",
    "oauth_client_secret": "secret",
    "refresh_token": "refresh",
    "developer_token": "dev",
    "customer_id": "12345",
    "account_ids": "67890",
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T00:00:00Z",
}


class MockResponse:
    def __init__(self, status_code, json_data=None, headers=None, content=b""):
        self.status_code = status_code
        self._json_data = json_data
        self.headers = headers or {}
        self.content = content
        self.text = str(json_data)

    def json(self):
        if self._json_data is None:
            raise ValueError("No JSON content")
        return self._json_data


def _mock_client():
    from tap_bing_ads.client import Client
    client = MagicMock(spec=Client)
    client.config = dict(DEFAULT_CONFIG)
    return client


def _make_report_entry(stream_cls, selected_columns=None):
    selected_columns = selected_columns or ["TimePeriod", "AccountId", "Clicks", "Impressions"]
    schema_dict = {
        "type": "object",
        "properties": {col: {"type": ["null", "string"]} for col in selected_columns},
    }
    schema_dict["properties"]["_sdc_report_datetime"] = {"type": ["null", "string"]}
    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema_dict, key_properties=[], valid_replication_keys=["TimePeriod"],
        replication_method="INCREMENTAL",
    )
    mdata = metadata.to_map(mdata)
    mdata = metadata.write(mdata, (), "selected", True)
    for col in selected_columns:
        inc = "automatic" if col in ("TimePeriod", "AccountId") else "available"
        mdata = metadata.write(mdata, ("properties", col), "inclusion", inc)
        if inc == "available":
            mdata = metadata.write(mdata, ("properties", col), "selected", True)
    return CatalogEntry(
        stream=stream_cls.tap_stream_id,
        tap_stream_id=stream_cls.tap_stream_id,
        key_properties=[],
        schema=Schema.from_dict(schema_dict),
        metadata=metadata.to_list(mdata),
    )


# =============================================================================
# discover.py
# =============================================================================

class TestDiscoverWithoutClient(unittest.TestCase):
    """discover() without a client returns the full catalog (no access checks)."""

    def test_returns_full_catalog(self):
        from tap_bing_ads.discover import discover
        catalog = discover()
        ids = {e.tap_stream_id for e in catalog.streams}
        self.assertIn("accounts", ids)
        self.assertIn("campaign_performance_report", ids)

    def test_catalog_entries_have_schemas(self):
        from tap_bing_ads.discover import discover
        catalog = discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                self.assertEqual(entry.schema.to_dict().get("type"), "object")

    def test_catalog_is_json_serialisable(self):
        from tap_bing_ads.discover import discover
        catalog = discover()
        dumped = json.dumps(catalog.to_dict())
        self.assertIn("streams", json.loads(dumped))


class TestDoDiscover(unittest.TestCase):
    """do_discover() emits JSON to stdout."""

    def test_do_discover_no_client_writes_json(self):
        from tap_bing_ads.discover import do_discover
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            do_discover()
        data = json.loads(buf.getvalue())
        self.assertIn("streams", data)

    def test_do_discover_with_client_writes_json(self):
        from tap_bing_ads.discover import do_discover
        client = _mock_client()
        buf = io.StringIO()
        with patch("tap_bing_ads.discover._apply_access_checks"), \
             patch("sys.stdout", buf):
            do_discover(client)
        data = json.loads(buf.getvalue())
        self.assertIn("streams", data)


class TestDiscoverCatalogBuildError(unittest.TestCase):
    """discover() propagates exceptions from Schema.from_dict."""

    def test_schema_error_propagates(self):
        from tap_bing_ads.discover import discover
        with patch("singer.catalog.Schema.from_dict", side_effect=ValueError("bad")):
            with self.assertRaises(ValueError):
                discover()


class TestApplyAccessChecksSkipsAbsentStreams(unittest.TestCase):
    """Streams in STREAMS but not in schemas dict are silently skipped."""

    def test_stream_not_in_schemas_is_not_probed(self):
        from tap_bing_ads.discover import _apply_access_checks
        from tap_bing_ads.schema import get_schemas
        from tap_bing_ads.streams import STREAMS

        schemas, field_metadata = get_schemas()
        client = _mock_client()
        probed = []

        class ExtraStream:
            parent = None
            def __init__(self, client=None, catalog_entry=None): pass
            def check_access(self):
                probed.append("extra")
                return True

        patched = {k: type("S", (), {"parent": v.parent,
                                     "__init__": lambda self, client=None, catalog_entry=None: None,
                                     "check_access": lambda self: True})
                   for k, v in STREAMS.items()}
        patched["__nonexistent__"] = ExtraStream

        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("extra", probed)


# =============================================================================
# exceptions.py
# =============================================================================

class TestBingAdsRateLimitRetryAfter(unittest.TestCase):
    """BingAdsRateLimitError parses Retry-After header."""

    def test_parses_integer_header(self):
        from tap_bing_ads.exceptions import BingAdsRateLimitError
        resp = MockResponse(429, headers={"Retry-After": "45"})
        exc = BingAdsRateLimitError("limited", response=resp)
        self.assertEqual(exc.retry_after, 45)

    def test_parses_lowercase_header(self):
        from tap_bing_ads.exceptions import BingAdsRateLimitError
        resp = MockResponse(429, headers={"retry-after": "30"})
        exc = BingAdsRateLimitError("limited", response=resp)
        self.assertEqual(exc.retry_after, 30)

    def test_invalid_header_falls_back_to_60(self):
        from tap_bing_ads.exceptions import BingAdsRateLimitError
        resp = MockResponse(429, headers={"Retry-After": "not-a-number"})
        exc = BingAdsRateLimitError("limited", response=resp)
        self.assertEqual(exc.retry_after, 60)

    def test_no_response_defaults_to_60(self):
        from tap_bing_ads.exceptions import BingAdsRateLimitError
        exc = BingAdsRateLimitError("limited")
        self.assertEqual(exc.retry_after, 60)


# =============================================================================
# schema.py
# =============================================================================

class TestGetReportRequiredFields(unittest.TestCase):
    def test_returns_base_and_metric_columns(self):
        from tap_bing_ads.schema import get_report_required_fields
        fields = get_report_required_fields("CampaignPerformanceReport")
        self.assertIn("TimePeriod", fields)
        self.assertIn("AccountId", fields)
        self.assertGreater(len(fields), 3)

    def test_includes_report_specific_fields(self):
        from tap_bing_ads.schema import get_report_required_fields
        fields = get_report_required_fields("AdExtensionDetailReport")
        self.assertIn("AdExtensionId", fields)

    def test_unknown_report_returns_base_fields(self):
        from tap_bing_ads.schema import get_report_required_fields
        fields = get_report_required_fields("UnknownReport")
        self.assertIn("TimePeriod", fields)


class TestLoadSchemaReferences(unittest.TestCase):
    def test_returns_dict(self):
        from tap_bing_ads.schema import load_schema_references
        refs = load_schema_references()
        self.assertIsInstance(refs, dict)


# =============================================================================
# client.py
# =============================================================================

class TestClientInitTokenExpiry(unittest.TestCase):
    """__init__ restores token_expires_at from config."""

    def test_parses_iso_expiry_from_config(self):
        from tap_bing_ads.client import Client
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        config = {**DEFAULT_CONFIG, "access_token": "tok", "token_expires_at": future}
        with patch("tap_bing_ads.client.Session"):
            client = Client(config)
        self.assertIsNotNone(client._expires_at)
        self.assertEqual(client._access_token, "tok")

    def test_parses_expiry_with_Z_suffix(self):
        from tap_bing_ads.client import Client
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        config = {**DEFAULT_CONFIG, "access_token": "tok", "token_expires_at": future}
        with patch("tap_bing_ads.client.Session"):
            client = Client(config)
        self.assertIsNotNone(client._expires_at)

    def test_invalid_expiry_sets_none(self):
        from tap_bing_ads.client import Client
        config = {**DEFAULT_CONFIG, "token_expires_at": "not-a-date"}
        with patch("tap_bing_ads.client.Session"):
            client = Client(config)
        self.assertIsNone(client._expires_at)


class TestClientInitTimeoutInvalid(unittest.TestCase):
    """__init__ falls back to REQUEST_TIMEOUT when timeout_cfg is invalid."""

    def test_invalid_string_falls_back_to_default(self):
        from tap_bing_ads.client import Client, REQUEST_TIMEOUT
        config = {**DEFAULT_CONFIG, "request_timeout": "not-a-number"}
        with patch("tap_bing_ads.client.Session"):
            client = Client(config)
        self.assertEqual(client.request_timeout, REQUEST_TIMEOUT)


class TestClientContextManager(unittest.TestCase):
    """__enter__ and __exit__ coverage."""

    def test_enter_reuses_valid_token(self):
        from tap_bing_ads.client import Client
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        config = {**DEFAULT_CONFIG, "access_token": "valid_tok",
                  "token_expires_at": future.isoformat()}
        with patch("tap_bing_ads.client.Session"):
            client = Client(config)
        with patch.object(client, "_refresh_access_token") as mock_refresh:
            result = client.__enter__()
        mock_refresh.assert_not_called()
        self.assertIs(result, client)

    def test_enter_refreshes_when_no_token(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        with patch.object(client, "_refresh_access_token") as mock_refresh:
            client.__enter__()
        mock_refresh.assert_called_once()

    def test_exit_closes_session(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        client._session = MagicMock()
        client.__exit__(None, None, None)
        client._session.close.assert_called_once()

    def test_context_manager_usage(self):
        from tap_bing_ads.client import Client
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        config = {**DEFAULT_CONFIG, "access_token": "tok",
                  "token_expires_at": future.isoformat()}
        with patch("tap_bing_ads.client.Session") as mock_sess:
            mock_sess.return_value = MagicMock()
            with Client(config) as c:
                self.assertIsNotNone(c)


class TestClientWriteConfig(unittest.TestCase):
    """_write_config persists token to file."""

    def test_skips_when_no_config_path(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG, config_path=None)
        client._access_token = "tok"
        client._expires_at = datetime.now()
        # Should return without doing anything
        client._write_config()

    def test_writes_token_to_file(self):
        from tap_bing_ads.client import Client
        existing = {"access_token": "old", "key": "val"}
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG, config_path="/tmp/cfg.json")
        client._access_token = "new_tok"
        client._expires_at = datetime.now()
        with patch("builtins.open", mock_open(read_data=json.dumps(existing))), \
             patch("json.load", return_value=dict(existing)), \
             patch("json.dump") as mock_dump:
            client._write_config()
        mock_dump.assert_called_once()

    def test_logs_warning_on_file_error(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG, config_path="/bad/path.json")
        client._access_token = "tok"
        client._expires_at = datetime.now()
        with patch("builtins.open", side_effect=IOError("no permission")):
            # Should not raise; logs warning instead
            client._write_config()


class TestGetAccessToken(unittest.TestCase):
    def test_returns_valid_existing_token(self):
        from tap_bing_ads.client import Client
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        client._access_token = "tok"
        client._expires_at = future
        self.assertEqual(client.get_access_token(), "tok")

    def test_refreshes_when_token_expired(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        client._access_token = None
        with patch.object(client, "_refresh_access_token") as mock_ref:
            mock_ref.side_effect = lambda: setattr(client, "_access_token", "new")
            tok = client.get_access_token()
        mock_ref.assert_called_once()
        self.assertEqual(tok, "new")


class TestRaiseForErrorAdditional(unittest.TestCase):
    """Additional raise_for_error branches not covered elsewhere."""

    def test_json_decode_error_falls_back_to_mapping_message(self):
        from tap_bing_ads.client import raise_for_error
        from tap_bing_ads.exceptions import BingAdsBadRequestError
        resp = MockResponse(400)  # json() raises ValueError
        with self.assertRaises(BingAdsBadRequestError):
            raise_for_error(resp)

    def test_operation_errors_list_extracted(self):
        from tap_bing_ads.client import raise_for_error
        from tap_bing_ads.exceptions import BingAdsBadRequestError
        body = {"OperationErrors": [{"Message": "Op error msg"}]}
        resp = MockResponse(400, json_data=body)
        with self.assertRaises(BingAdsBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("Op error msg", str(ctx.exception))

    def test_batch_errors_list_extracted(self):
        from tap_bing_ads.client import raise_for_error
        from tap_bing_ads.exceptions import BingAdsBadRequestError
        body = {"BatchErrors": [{"ErrorCode": "BatchErrCode"}]}
        resp = MockResponse(400, json_data=body)
        with self.assertRaises(BingAdsBadRequestError):
            raise_for_error(resp)

    def test_unmapped_5xx_raises_backoff_error(self):
        from tap_bing_ads.client import raise_for_error
        from tap_bing_ads.exceptions import BingAdsBackoffError
        # 505 is not in ERROR_CODE_EXCEPTION_MAPPING, triggers the fallback BingAdsBackoffError
        resp = MockResponse(505, json_data={"message": "HTTP Version Not Supported"})
        with self.assertRaises(BingAdsBackoffError):
            raise_for_error(resp)

    def test_no_message_field_uses_mapping_default(self):
        from tap_bing_ads.client import raise_for_error
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        resp = MockResponse(403, json_data={"unrelated": "data"})
        with self.assertRaises(BingAdsForbiddenError):
            raise_for_error(resp)


class TestGetRetryAfter(unittest.TestCase):
    def test_returns_wait_for_rate_limit_error(self):
        from tap_bing_ads.client import _get_retry_after
        from tap_bing_ads.exceptions import BingAdsRateLimitError
        exc = BingAdsRateLimitError("limited")
        exc.retry_after = 42
        result = _get_retry_after({"exception": exc})
        self.assertEqual(result, 42)

    def test_returns_60_for_non_rate_limit(self):
        from tap_bing_ads.client import _get_retry_after
        result = _get_retry_after({"exception": ValueError("other")})
        self.assertEqual(result, 60)

    def test_returns_60_when_exception_is_none(self):
        from tap_bing_ads.client import _get_retry_after
        result = _get_retry_after({"exception": None})
        self.assertEqual(result, 60)


class TestExecuteRequest(unittest.TestCase):
    """_execute_request actually calls session.request."""

    def test_builds_headers_and_calls_session(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        mock_session = MagicMock()
        mock_session.request.return_value = MockResponse(200, {"ok": True})
        client._session = mock_session
        with patch.object(client, "get_access_token", return_value="tok"):
            resp = client._execute_request("GET", "https://example.com/api",
                                            account_id="12345")
        mock_session.request.assert_called_once()
        args, kwargs = mock_session.request.call_args
        self.assertEqual(args[0], "GET")
        self.assertEqual(resp.status_code, 200)


class TestMakeRequestAdditional(unittest.TestCase):
    """make_request edge cases not covered elsewhere."""

    def test_401_retry_logs_and_retries(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        r1 = MockResponse(401, {"message": "Unauthorized"})
        r2 = MockResponse(200, {"data": "ok"})
        with patch.object(client, "_execute_request", side_effect=[r1, r2]), \
             patch.object(client, "_refresh_access_token"):
            result = client.make_request("GET", "https://example.com")
        self.assertEqual(result, {"data": "ok"})

    def test_json_parse_error_raises_bing_ads_error(self):
        from tap_bing_ads.client import Client
        from tap_bing_ads.exceptions import BingAdsError
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        bad_resp = MagicMock()
        bad_resp.status_code = 200
        bad_resp.json.side_effect = ValueError("bad json")
        with patch.object(client, "_execute_request", return_value=bad_resp):
            with self.assertRaises(BingAdsError):
                client.make_request("GET", "https://example.com")


class TestDownloadFile(unittest.TestCase):
    def test_returns_binary_content_on_200(self):
        from tap_bing_ads.client import Client
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        mock_session = MagicMock()
        mock_session.get.return_value = MockResponse(200, content=b"ZIPDATA")
        client._session = mock_session
        result = client.download_file("https://example.com/report.zip")
        self.assertEqual(result, b"ZIPDATA")

    def test_raises_on_non_200(self):
        from tap_bing_ads.client import Client
        from tap_bing_ads.exceptions import BingAdsError
        with patch("tap_bing_ads.client.Session"):
            client = Client(DEFAULT_CONFIG)
        mock_session = MagicMock()
        mock_session.get.return_value = MockResponse(403, content=b"")
        client._session = mock_session
        with self.assertRaises(BingAdsError):
            client.download_file("https://example.com/report.zip")


# =============================================================================
# streams/abstracts.py — BaseStream
# =============================================================================

class TestBaseStreamWriteSchemaOSError(unittest.TestCase):
    """write_schema re-raises OSError."""

    def test_reraises_os_error(self):
        from tap_bing_ads.streams.accounts import Accounts
        client = _mock_client()
        stream = Accounts(client=client)
        with patch("tap_bing_ads.streams.abstracts.write_schema",
                   side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                stream.write_schema()


class TestBaseStreamGetRecords(unittest.TestCase):
    """get_records branches: no data_key, non-list response, empty."""

    def _make_campaigns_stream(self):
        """Campaigns uses base get_records and has data_key='Campaigns'."""
        from tap_bing_ads.streams.campaigns import Campaigns
        client = _mock_client()
        stream = Campaigns(client=client)
        stream.get_url_endpoint()  # sets url_endpoint
        return stream

    def test_yields_list_records(self):
        stream = self._make_campaigns_stream()
        client = stream.client
        client.make_request.return_value = {"Campaigns": [{"Id": 1}, {"Id": 2}]}
        records = list(stream.get_records())
        self.assertEqual(len(records), 2)

    def test_yields_single_dict_when_not_list(self):
        stream = self._make_campaigns_stream()
        client = stream.client
        client.make_request.return_value = {"Campaigns": {"Id": 99}}
        records = list(stream.get_records())
        self.assertEqual(records, [{"Id": 99}])

    def test_yields_nothing_when_empty(self):
        stream = self._make_campaigns_stream()
        client = stream.client
        client.make_request.return_value = {"Campaigns": None}
        records = list(stream.get_records())
        self.assertEqual(records, [])

    def test_yields_response_directly_when_no_data_key(self):
        """When data_key is empty, raw response is yielded."""
        from tap_bing_ads.streams.campaigns import Campaigns
        client = _mock_client()
        stream = Campaigns(client=client)
        stream.data_key = ""
        stream.get_url_endpoint()
        client.make_request.return_value = [{"Id": 1}, {"Id": 2}]
        records = list(stream.get_records())
        self.assertEqual(len(records), 2)


class TestBaseStreamCheckAccessFalse(unittest.TestCase):
    """check_access returns False on 403."""

    def test_base_stream_returns_false_on_forbidden(self):
        from tap_bing_ads.streams.accounts import Accounts
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        client = _mock_client()
        stream = Accounts(client=client)
        client.make_request.side_effect = BingAdsForbiddenError("403")
        self.assertFalse(stream.check_access())

    def test_base_stream_returns_true_on_success(self):
        from tap_bing_ads.streams.accounts import Accounts
        client = _mock_client()
        stream = Accounts(client=client)
        client.make_request.return_value = {"Account": {"Id": 1}}
        self.assertTrue(stream.check_access())


class TestIncrementalStreamWriteBookmarkEarlyReturn(unittest.TestCase):
    """write_bookmark returns early when no key and no replication_keys."""

    def test_returns_state_unchanged(self):
        from tap_bing_ads.streams.abstracts import IncrementalStream

        class MinimalIncremental(IncrementalStream):
            tap_stream_id = "minimal"
            key_properties = ["Id"]
            replication_keys = []
            valid_replication_keys = []
            def get_url_endpoint(self, parent_obj=None): return "https://x.com"
            def sync(self, state, transformer, parent_obj=None, **kw): return 0

        client = _mock_client()
        stream = MinimalIncremental(client=client)
        state = {"bookmarks": {}}
        result = stream.write_bookmark(state, "minimal", key=None, value="2024-01-01")
        self.assertIs(result, state)


# =============================================================================
# streams/abstracts.py — FullTableStream
# =============================================================================

class TestFullTableStreamSync(unittest.TestCase):
    """FullTableStream.sync coverage."""

    def _make_stream(self):
        from tap_bing_ads.streams.abstracts import FullTableStream
        schema_dict = {"type": "object", "properties": {"Id": {"type": ["null", "integer"]}}}
        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema_dict, key_properties=["Id"],
            valid_replication_keys=[], replication_method="FULL_TABLE",
        )
        mdata = metadata.to_map(mdata)
        mdata = metadata.write(mdata, (), "selected", True)
        mdata = metadata.write(mdata, ("properties", "Id"), "selected", True)
        entry = CatalogEntry(
            stream="fake_ft", tap_stream_id="fake_ft",
            key_properties=["Id"], schema=Schema.from_dict(schema_dict),
            metadata=metadata.to_list(mdata),
        )

        class FakeFullTable(FullTableStream):
            tap_stream_id = "fake_ft"
            key_properties = ["Id"]
            replication_keys = []
            def get_url_endpoint(self, parent_obj=None): return "https://x.com"

        client = _mock_client()
        client.make_request.return_value = [{"Id": 1}, {"Id": 2}]
        stream = FakeFullTable(client, entry)
        stream.data_key = ""
        return stream

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_writes_all_records(self, mock_wr, mock_ws):
        stream = self._make_stream()
        count = stream.sync({}, singer.Transformer())
        self.assertEqual(count, 2)
        self.assertEqual(mock_wr.call_count, 2)

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_drives_children(self, mock_wr, mock_ws):
        stream = self._make_stream()
        mock_child = MagicMock()
        stream.child_to_sync = [mock_child]
        stream.sync({}, singer.Transformer())
        mock_child.sync.assert_called()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_returns_zero_for_empty_response(self, mock_wr, mock_ws):
        stream = self._make_stream()
        stream.client.make_request.return_value = []
        count = stream.sync({}, singer.Transformer())
        self.assertEqual(count, 0)


# =============================================================================
# streams — concrete check_access overrides
# =============================================================================

class TestCampaignsCheckAccess(unittest.TestCase):
    def test_returns_true_on_success(self):
        from tap_bing_ads.streams.campaigns import Campaigns
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        client = _mock_client()
        stream = Campaigns(client=client)
        client.make_request.return_value = {"Campaigns": []}
        self.assertTrue(stream.check_access())

    def test_returns_false_on_403(self):
        from tap_bing_ads.streams.campaigns import Campaigns
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        client = _mock_client()
        stream = Campaigns(client=client)
        client.make_request.side_effect = BingAdsForbiddenError("403")
        self.assertFalse(stream.check_access())


class TestAdGroupsAdsCheckAccess(unittest.TestCase):
    def test_ad_groups_always_true(self):
        from tap_bing_ads.streams.ad_groups import AdGroups
        client = _mock_client()
        self.assertTrue(AdGroups(client=client).check_access())

    def test_ads_always_true(self):
        from tap_bing_ads.streams.ads import Ads
        client = _mock_client()
        self.assertTrue(Ads(client=client).check_access())


# =============================================================================
# streams/abstracts.py — BaseReport.check_access
# =============================================================================

class TestBaseReportCheckAccess(unittest.TestCase):
    """BaseReport.check_access all return paths."""

    def _stream(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        client = _mock_client()
        return CampaignPerformanceReport(client=client)

    def test_returns_true_on_200(self):
        stream = self._stream()
        stream.client.make_request.return_value = {"ReportRequestId": "abc"}
        self.assertTrue(stream.check_access())

    def test_returns_false_on_403(self):
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        stream = self._stream()
        stream.client.make_request.side_effect = BingAdsForbiddenError("403")
        self.assertFalse(stream.check_access())

    def test_propagates_400(self):
        """400 errors are NOT swallowed — they propagate (probing a valid endpoint)."""
        from tap_bing_ads.exceptions import BingAdsBadRequestError
        stream = self._stream()
        stream.client.make_request.side_effect = BingAdsBadRequestError("400")
        with self.assertRaises(BingAdsBadRequestError):
            stream.check_access()


# =============================================================================
# streams/abstracts.py — BaseReport._check_field_exclusions
# =============================================================================

class TestCheckFieldExclusions(unittest.TestCase):
    """_check_field_exclusions raises on mutually-exclusive fields."""

    def test_raises_on_conflicting_fields(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        from tap_bing_ads.exceptions import BingAdsInvalidFieldSelection
        from tap_bing_ads.exclusions import EXCLUSIONS

        client = _mock_client()
        stream = CampaignPerformanceReport(client=client)
        stream.report_name = "CampaignPerformanceReport"

        exclusion_rules = EXCLUSIONS.get("CampaignPerformanceReport", [])
        if not exclusion_rules:
            self.skipTest("No exclusion rules for CampaignPerformanceReport")

        rule = exclusion_rules[0]
        attrs = rule.get("Attributes", [])
        share_stats = rule.get("ImpressionSharePerformanceStatistics", [])
        if not attrs or not share_stats:
            self.skipTest("No conflicting fields available in first rule")

        conflicting = [attrs[0], share_stats[0]]
        with self.assertRaises(BingAdsInvalidFieldSelection):
            stream._check_field_exclusions(conflicting)


# =============================================================================
# streams/abstracts.py — BaseReport.sync and _sync_interval
# =============================================================================

class TestBaseReportSync(unittest.TestCase):
    """BaseReport.sync covers date-window logic and error handling."""

    def _make_stream(self, start="2024-01-01T00:00:00Z", end="2024-01-03T00:00:00Z"):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        client = _mock_client()
        client.config = {**DEFAULT_CONFIG, "start_date": start, "end_date": end}
        return CampaignPerformanceReport(client, entry)

    def test_returns_zero_when_start_after_end(self):
        """When bookmark is ahead of end_date, no records are synced."""
        stream = self._make_stream(start="2024-01-01T00:00:00Z", end="2024-01-03T00:00:00Z")
        # Bookmark after end
        state = {}
        config = {**DEFAULT_CONFIG,
                  "start_date": "2024-01-10T00:00:00Z",
                  "end_date": "2024-01-05T00:00:00Z",
                  "report_max_days": 30, "conversion_window": -30}
        result = stream.sync(state, "67890", config)
        self.assertEqual(result, 0)

    def test_skips_when_no_columns_selected(self):
        """When get_selected_columns returns empty, returns 0."""
        stream = self._make_stream()
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        with patch.object(stream, "get_selected_columns", return_value=[]):
            result = stream.sync({}, "67890", config)
        self.assertEqual(result, 0)

    def test_raises_when_no_measure_columns(self):
        """When no metric columns selected, BingAdsNoMeasureSelected is raised."""
        from tap_bing_ads.exceptions import BingAdsNoMeasureSelected
        stream = self._make_stream()
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        # Only dimension columns, no measures
        with patch.object(stream, "get_selected_columns", return_value=["TimePeriod", "AccountId"]):
            with self.assertRaises(BingAdsNoMeasureSelected):
                stream.sync({}, "67890", config)

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    def test_sync_calls_sync_interval(self, mock_wb, mock_ws):
        """sync() delegates to _sync_interval for each window."""
        stream = self._make_stream()
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        with patch.object(stream, "get_selected_columns",
                          return_value=["TimePeriod", "AccountId", "Clicks"]), \
             patch.object(stream, "_sync_interval", return_value=5) as mock_si:
            result = stream.sync({}, "67890", config)
        self.assertGreater(result, 0)
        mock_si.assert_called()

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    def test_invalid_date_range_is_skipped(self, mock_wb, mock_ws):
        from tap_bing_ads.exceptions import BingAdsInvalidDateRangeEnd
        stream = self._make_stream()
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        with patch.object(stream, "get_selected_columns",
                          return_value=["TimePeriod", "AccountId", "Clicks"]), \
             patch.object(stream, "_sync_interval",
                          side_effect=BingAdsInvalidDateRangeEnd("out of range")):
            result = stream.sync({}, "67890", config)
        self.assertEqual(result, 0)

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    def test_report_error_is_skipped(self, mock_wb, mock_ws):
        from tap_bing_ads.exceptions import BingAdsReportError
        stream = self._make_stream()
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        with patch.object(stream, "get_selected_columns",
                          return_value=["TimePeriod", "AccountId", "Clicks"]), \
             patch.object(stream, "_sync_interval",
                          side_effect=BingAdsReportError("report failed")):
            result = stream.sync({}, "67890", config)
        self.assertEqual(result, 0)

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    def test_invalid_field_selection_breaks_loop(self, mock_wb, mock_ws):
        from tap_bing_ads.exceptions import BingAdsInvalidFieldSelection
        stream = self._make_stream()
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        with patch.object(stream, "get_selected_columns",
                          return_value=["TimePeriod", "AccountId", "Clicks"]), \
             patch.object(stream, "_sync_interval",
                          side_effect=BingAdsInvalidFieldSelection("conflict")):
            result = stream.sync({}, "67890", config)
        self.assertEqual(result, 0)


class TestBaseReportPollTimeout(unittest.TestCase):
    """_poll_report returns None when MAX_REPORT_POLLS is reached."""

    @patch("time.sleep")
    def test_returns_none_on_timeout(self, mock_sleep):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        from tap_bing_ads.streams.abstracts import MAX_REPORT_POLLS
        client = _mock_client()
        entry = _make_report_entry(CampaignPerformanceReport)
        stream = CampaignPerformanceReport(client, entry)
        # Always return Pending
        client.make_request.return_value = {
            "ReportRequestStatus": {"Status": "Pending"}
        }
        with patch("tap_bing_ads.streams.abstracts.MAX_REPORT_POLLS", 2):
            result = stream._poll_report("67890", "req-123")
        self.assertIsNone(result)


class TestBaseReportSyncInterval(unittest.TestCase):
    """_sync_interval: normal flow and regeneration on transient error."""

    def _make_stream(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        client = _mock_client()
        client.config = dict(DEFAULT_CONFIG)
        return CampaignPerformanceReport(client, entry)

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_sync_interval_no_data(self, mock_ws, mock_wb, mock_wst):
        """When download_url is None, returns 0 records."""
        stream = self._make_stream()
        stream.client.make_request.return_value = {"ReportRequestId": "req-1"}
        state = {}
        with patch.object(stream, "_poll_report", return_value=None), \
             patch.object(stream, "_submit_report", return_value="req-1"):
            count = stream._sync_interval(
                state, "67890_campaign", "67890",
                ["TimePeriod", "AccountId", "Clicks"],
                arrow.get("2024-01-01"), arrow.get("2024-01-01"),
            )
        self.assertEqual(count, 0)

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_sync_interval_streams_csv(self, mock_ws, mock_wb, mock_wst):
        """When download_url is provided, _stream_report_csv is called."""
        stream = self._make_stream()
        state = {}
        with patch.object(stream, "_submit_report", return_value="req-1"), \
             patch.object(stream, "_poll_report", return_value="https://example.com/r.zip"), \
             patch.object(stream, "_stream_report_csv", return_value=10) as mock_csv:
            count = stream._sync_interval(
                state, "67890_campaign", "67890",
                ["TimePeriod", "AccountId", "Clicks"],
                arrow.get("2024-01-01"), arrow.get("2024-01-01"),
            )
        self.assertEqual(count, 10)
        mock_csv.assert_called_once()

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_sync_interval_resumes_from_bookmark(self, mock_ws, mock_wb, mock_wst):
        """When request_id is in state, submit is skipped."""
        stream = self._make_stream()
        import singer
        state = {}
        singer.write_bookmark(state, "67890_campaign", "request_id", "saved-req")
        with patch.object(stream, "_submit_report") as mock_sub, \
             patch.object(stream, "_poll_report", return_value=None):
            stream._sync_interval(
                state, "67890_campaign", "67890",
                ["TimePeriod", "AccountId", "Clicks"],
                arrow.get("2024-01-01"), arrow.get("2024-01-01"),
            )
        mock_sub.assert_not_called()

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_sync_interval_regenerates_on_transient_error(self, mock_ws, mock_wb, mock_wst):
        """On non-BingAdsReportError poll failure, regenerates request."""
        from tap_bing_ads.exceptions import BingAdsReportError
        stream = self._make_stream()
        state = {}
        with patch.object(stream, "_submit_report", side_effect=["req-1", "req-2"]), \
             patch.object(stream, "_poll_report",
                          side_effect=[ConnectionError("transient"), None]):
            count = stream._sync_interval(
                state, "67890_campaign", "67890",
                ["TimePeriod", "AccountId", "Clicks"],
                arrow.get("2024-01-01"), arrow.get("2024-01-01"),
            )
        self.assertEqual(count, 0)

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_sync_interval_reraises_report_error(self, mock_ws, mock_wb, mock_wst):
        """BingAdsReportError from _poll_report propagates immediately."""
        from tap_bing_ads.exceptions import BingAdsReportError
        stream = self._make_stream()
        state = {}
        with patch.object(stream, "_submit_report", return_value="req-1"), \
             patch.object(stream, "_poll_report",
                          side_effect=BingAdsReportError("API error")):
            with self.assertRaises(BingAdsReportError):
                stream._sync_interval(
                    state, "67890_campaign", "67890",
                    ["TimePeriod", "AccountId", "Clicks"],
                    arrow.get("2024-01-01"), arrow.get("2024-01-01"),
                )


class TestTypeRow(unittest.TestCase):
    """_type_row casts column values to declared types."""

    def _make_stream(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        client = _mock_client()
        return CampaignPerformanceReport(client, entry)

    def test_integer_field_cast(self):
        from tap_bing_ads.reports import REPORTING_FIELD_TYPES
        stream = self._make_stream()
        int_field = next((f for f, t in REPORTING_FIELD_TYPES.items() if t == "integer"), None)
        if not int_field:
            self.skipTest("No integer fields in REPORTING_FIELD_TYPES")
        row = {int_field: "1,234"}
        stream._type_row(row)
        self.assertEqual(row[int_field], 1234)

    def test_number_field_cast(self):
        from tap_bing_ads.reports import REPORTING_FIELD_TYPES
        stream = self._make_stream()
        num_field = next((f for f, t in REPORTING_FIELD_TYPES.items() if t == "number"), None)
        if not num_field:
            self.skipTest("No number fields in REPORTING_FIELD_TYPES")
        row = {num_field: "12.5%"}
        stream._type_row(row)
        self.assertAlmostEqual(row[num_field], 12.5)

    def test_dash_value_becomes_zero(self):
        from tap_bing_ads.reports import REPORTING_FIELD_TYPES
        stream = self._make_stream()
        int_field = next((f for f, t in REPORTING_FIELD_TYPES.items() if t == "integer"), None)
        if not int_field:
            self.skipTest("No integer fields")
        row = {int_field: "--"}
        stream._type_row(row)
        self.assertEqual(row[int_field], 0)

    def test_empty_value_becomes_none(self):
        from tap_bing_ads.reports import REPORTING_FIELD_TYPES
        stream = self._make_stream()
        int_field = next((f for f, t in REPORTING_FIELD_TYPES.items() if t == "integer"), None)
        if not int_field:
            self.skipTest("No integer fields")
        row = {int_field: ""}
        stream._type_row(row)
        self.assertIsNone(row[int_field])


# =============================================================================
# sync.py
# =============================================================================

class TestIsAncestor(unittest.TestCase):
    def _cls(self, parent):
        class C:
            pass
        C.parent = parent
        return C

    def test_direct_parent_is_ancestor(self):
        from tap_bing_ads.sync import _is_ancestor
        streams = {"accounts": self._cls(None), "campaigns": self._cls("accounts")}
        self.assertTrue(_is_ancestor("accounts", "campaigns", streams))

    def test_grandparent_is_ancestor(self):
        from tap_bing_ads.sync import _is_ancestor
        streams = {
            "accounts": self._cls(None),
            "campaigns": self._cls("accounts"),
            "ad_groups": self._cls("campaigns"),
        }
        self.assertTrue(_is_ancestor("accounts", "ad_groups", streams))

    def test_non_ancestor_returns_false(self):
        from tap_bing_ads.sync import _is_ancestor
        streams = {"accounts": self._cls(None), "campaigns": self._cls("accounts")}
        self.assertFalse(_is_ancestor("campaigns", "accounts", streams))

    def test_missing_stream_returns_false(self):
        from tap_bing_ads.sync import _is_ancestor
        self.assertFalse(_is_ancestor("accounts", "ghost", {}))

    def test_no_parent_returns_false(self):
        from tap_bing_ads.sync import _is_ancestor
        streams = {"accounts": self._cls(None)}
        self.assertFalse(_is_ancestor("x", "accounts", streams))


class TestSyncWriteSchemaChildren(unittest.TestCase):
    """write_schema in sync.py wires up child streams."""

    def test_child_appended_to_parent_child_to_sync_when_selected(self):
        from tap_bing_ads.sync import write_schema as sw

        parent = MagicMock()
        parent.is_selected.return_value = True
        parent.children = ["campaigns"]
        parent.child_to_sync = []

        child_obj = MagicMock()
        child_obj.is_selected.return_value = True
        child_obj.children = []
        child_obj.child_to_sync = []

        child_cls = MagicMock(return_value=child_obj)
        mock_catalog = MagicMock()
        mock_catalog.get_stream.return_value = MagicMock()
        client = _mock_client()

        with patch("tap_bing_ads.sync.STREAMS", {"campaigns": child_cls}):
            sw(parent, client, ["campaigns"], mock_catalog)

        self.assertIn(child_obj, parent.child_to_sync)

    def test_child_not_appended_when_not_in_streams_to_sync(self):
        from tap_bing_ads.sync import write_schema as sw

        parent = MagicMock()
        parent.is_selected.return_value = True
        parent.children = ["campaigns"]
        parent.child_to_sync = []

        child_obj = MagicMock()
        child_obj.is_selected.return_value = True
        child_obj.children = []
        child_obj.child_to_sync = []

        child_cls = MagicMock(return_value=child_obj)
        mock_catalog = MagicMock()
        mock_catalog.get_stream.return_value = MagicMock()
        client = _mock_client()

        # campaigns NOT in streams_to_sync
        with patch("tap_bing_ads.sync.STREAMS", {"campaigns": child_cls}):
            sw(parent, client, [], mock_catalog)

        self.assertNotIn(child_obj, parent.child_to_sync)


# =============================================================================
# __init__.py  main()
# =============================================================================

class TestMain(unittest.TestCase):
    """main() entry point coverage."""

    def _mock_args(self, discover=False, catalog=None, state=None):
        args = MagicMock()
        args.config = {**DEFAULT_CONFIG, "end_date": "2024-01-31T00:00:00Z"}
        args.config_path = None
        args.discover = discover
        args.catalog = catalog
        args.state = state or {}
        return args

    @patch("tap_bing_ads.Client")
    @patch("singer.utils.parse_args")
    def test_discover_mode_calls_do_discover(self, mock_parse, mock_client_cls):
        mock_parse.return_value = self._mock_args(discover=True)
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        with patch("tap_bing_ads.do_discover") as mock_dd:
            from tap_bing_ads import main
            main()
        mock_dd.assert_called_once_with(mock_client)

    @patch("tap_bing_ads.Client")
    @patch("singer.utils.parse_args")
    def test_sync_mode_calls_sync(self, mock_parse, mock_client_cls):
        mock_catalog = MagicMock()
        mock_parse.return_value = self._mock_args(catalog=mock_catalog)
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        with patch("tap_bing_ads.sync") as mock_sync:
            from tap_bing_ads import main
            main()
        mock_sync.assert_called_once()

    @patch("tap_bing_ads.Client")
    @patch("singer.utils.parse_args")
    def test_no_catalog_no_discover_logs_info(self, mock_parse, mock_client_cls):
        mock_parse.return_value = self._mock_args(discover=False, catalog=None)
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        from tap_bing_ads import main
        # Should not raise
        main()

    @patch("tap_bing_ads.Client")
    @patch("singer.utils.parse_args")
    def test_injects_end_date_when_missing(self, mock_parse, mock_client_cls):
        args = self._mock_args(discover=True)
        args.config = dict(DEFAULT_CONFIG)  # no end_date
        args.config.pop("end_date", None)
        mock_parse.return_value = args
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        with patch("tap_bing_ads.do_discover"):
            from tap_bing_ads import main
            main()
        self.assertIn("end_date", args.config)


# =============================================================================
# Targeted tests for remaining missing lines
# =============================================================================

class TestBaseStreamCheckAccessBase(unittest.TestCase):
    """
    Cover BaseStream.check_access (lines 186-204) by using a stream that does NOT
    override the method — we create a minimal FullTableStream subclass.
    """

    def _make_stream(self):
        from tap_bing_ads.streams.abstracts import FullTableStream
        schema_dict = {"type": "object", "properties": {"Id": {"type": ["null", "integer"]}}}
        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema_dict, key_properties=["Id"],
            valid_replication_keys=[], replication_method="FULL_TABLE",
        )
        mdata = metadata.to_map(mdata)
        mdata = metadata.write(mdata, (), "selected", True)
        entry = CatalogEntry(
            stream="probe_ft", tap_stream_id="probe_ft",
            key_properties=["Id"], schema=Schema.from_dict(schema_dict),
            metadata=metadata.to_list(mdata),
        )

        class ProbeStream(FullTableStream):
            tap_stream_id = "probe_ft"
            key_properties = ["Id"]
            replication_keys = []
            def get_url_endpoint(self, parent_obj=None):
                self.url_endpoint = "https://example.com/probe"
                return self.url_endpoint

        client = _mock_client()
        stream = ProbeStream(client, entry)
        return stream

    def test_base_check_access_returns_true(self):
        stream = self._make_stream()
        stream.client.make_request.return_value = {"Id": 1}
        self.assertTrue(stream.check_access())

    def test_base_check_access_returns_false_on_403(self):
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        stream = self._make_stream()
        stream.client.make_request.side_effect = BingAdsForbiddenError("Forbidden")
        self.assertFalse(stream.check_access())


class TestBaseReportIsSelected(unittest.TestCase):
    """Cover BaseReport.is_selected (line 341)."""

    def test_selected_true(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        stream = CampaignPerformanceReport(_mock_client(), entry)
        result = stream.is_selected()
        self.assertTrue(result)

    def test_selected_false_without_catalog_entry(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        stream = CampaignPerformanceReport(_mock_client())
        result = stream.is_selected()
        self.assertFalse(result)


class TestPollReportEmptyLoop(unittest.TestCase):
    """Cover line 502 (return None after for loop when loop runs 0 iterations)."""

    @patch("time.sleep")
    def test_poll_returns_none_when_zero_polls(self, mock_sleep):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        client = _mock_client()
        stream = CampaignPerformanceReport(client, entry)
        # With MAX_REPORT_POLLS=0, range(1, 1) is empty → falls through to `return None`
        with patch("tap_bing_ads.streams.abstracts.MAX_REPORT_POLLS", 0):
            result = stream._poll_report("67890", "req-0")
        self.assertIsNone(result)
        mock_sleep.assert_not_called()


class TestTypeRowDatetimeException(unittest.TestCase):
    """Cover lines 541-542: except Exception: pass in _type_row for datetime field."""

    def test_invalid_datetime_value_is_silently_skipped(self):
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        from tap_bing_ads.reports import REPORTING_FIELD_TYPES
        entry = _make_report_entry(CampaignPerformanceReport)
        stream = CampaignPerformanceReport(_mock_client(), entry)
        dt_field = next((f for f, t in REPORTING_FIELD_TYPES.items() if t == "datetime"), None)
        if not dt_field:
            self.skipTest("No datetime fields in REPORTING_FIELD_TYPES")
        row = {dt_field: "not-a-real-date-!!$$%%"}
        # Should not raise — the except block silently passes
        stream._type_row(row)
        self.assertIn(dt_field, row)


class TestSyncIntervalNoMeasureFromInterval(unittest.TestCase):
    """Cover lines 602-603: BingAdsNoMeasureSelected raised from _sync_interval."""

    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    def test_no_measure_from_interval_breaks_loop(self, mock_wb, mock_wst):
        from tap_bing_ads.exceptions import BingAdsNoMeasureSelected
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        client = _mock_client()
        client.config = dict(DEFAULT_CONFIG)
        stream = CampaignPerformanceReport(client, entry)
        config = {**DEFAULT_CONFIG, "report_max_days": 30, "conversion_window": -30}
        with patch.object(stream, "get_selected_columns",
                          return_value=["TimePeriod", "AccountId", "Clicks"]), \
             patch.object(stream, "_sync_interval",
                          side_effect=BingAdsNoMeasureSelected("no measure")):
            result = stream.sync({}, "67890", config)
        self.assertEqual(result, 0)


class TestMainElseBranch(unittest.TestCase):
    """Cover __init__.py:59 — the else branch when no catalog and no discover."""

    @patch("tap_bing_ads.Client")
    @patch("singer.utils.parse_args")
    def test_logs_info_when_no_catalog_no_discover(self, mock_parse, mock_client_cls):
        args = MagicMock()
        args.config = {**DEFAULT_CONFIG, "end_date": "2024-01-31T00:00:00Z"}
        args.config_path = None
        args.discover = False
        args.catalog = None
        args.state = {}
        mock_parse.return_value = args
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        import tap_bing_ads
        with patch.object(tap_bing_ads.LOGGER, "info") as mock_log:
            tap_bing_ads.main()
        logged_messages = [str(c) for c in mock_log.call_args_list]
        self.assertTrue(any("No --catalog" in m for m in logged_messages))


class TestLoadSchemaReferencesWithFiles(unittest.TestCase):
    """Cover schema.py:37,45-46 — the list-comprehension and loop body."""

    def test_returns_refs_when_shared_files_exist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_shared = os.path.join(tmpdir, "integer.json")
            with open(fake_shared, "w") as f:
                json.dump({"type": "integer"}, f)
            with patch("tap_bing_ads.schema.get_abs_path", return_value=tmpdir):
                from tap_bing_ads.schema import load_schema_references
                refs = load_schema_references()
        self.assertIsInstance(refs, dict)
        self.assertGreater(len(refs), 0)


class TestAccountsGetRecordsEmptyAccountId(unittest.TestCase):
    """Cover accounts.py:49 — the `continue` when account_id is empty after strip."""

    def test_skips_empty_account_id_entries(self):
        from tap_bing_ads.streams.accounts import Accounts
        client = _mock_client()
        # Trailing comma produces an empty entry after split
        client.config = {**DEFAULT_CONFIG, "account_ids": "67890,"}
        stream = Accounts(client=client)
        stream.get_url_endpoint()
        client.make_request.return_value = {"Account": {"Id": 67890}}
        records = list(stream.get_records())
        self.assertEqual(len(records), 1)
        self.assertEqual(client.make_request.call_count, 1)


class TestStreamReportCsvActual(unittest.TestCase):
    """Cover abstracts.py:502+ by actually calling _stream_report_csv."""

    def test_parses_csv_from_zip(self):
        """Construct a real in-memory ZIP containing a CSV and call _stream_report_csv."""
        import zipfile
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        entry = _make_report_entry(CampaignPerformanceReport)
        client = _mock_client()
        stream = CampaignPerformanceReport(client, entry)

        csv_content = "TimePeriod,AccountId,Clicks\n2024-01-01,67890,100\n"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("report.csv", csv_content)
        zip_bytes = buf.getvalue()

        client.download_file.return_value = zip_bytes

        with patch("tap_bing_ads.streams.abstracts.write_record") as mock_wr:
            count = stream._stream_report_csv(
                "campaign_performance_report",
                "https://example.com/report.zip",
                "2024-01-01T00:00:00Z",
                {},
            )

        self.assertEqual(count, 1)
        mock_wr.assert_called_once()
        call_args = mock_wr.call_args[0]
        self.assertIn("Clicks", call_args[1])
        # Clicks is typed as integer by _type_row
        self.assertEqual(call_args[1]["Clicks"], 100)
