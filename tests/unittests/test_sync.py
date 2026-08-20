"""Unit tests for the sync orchestration logic â€” new parent-child architecture."""
import unittest
from unittest.mock import MagicMock, patch, call

import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata

from tap_bing_ads.sync import sync
from tap_bing_ads.client import Client

DEFAULT_CONFIG = {
    "oauth_client_id": "id",
    "oauth_client_secret": "secret",
    "refresh_token": "refresh",
    "developer_token": "dev",
    "customer_id": "12345",
    "account_ids": "67890,11111",
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T00:00:00Z",
    "report_max_days": 30,
    "conversion_window": -30,
}


def _make_entry(stream_name, selected=True, replication_method="FULL_TABLE"):
    schema_dict = {
        "type": "object",
        "properties": {"Id": {"type": ["null", "integer"]}},
    }
    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema_dict, key_properties=["Id"],
        valid_replication_keys=[], replication_method=replication_method
    )
    mdata = metadata.to_map(mdata)
    if selected:
        mdata = metadata.write(mdata, (), "selected", True)
    return CatalogEntry(
        stream=stream_name,
        tap_stream_id=stream_name,
        key_properties=["Id"],
        schema=Schema.from_dict(schema_dict),
        metadata=metadata.to_list(mdata),
    )


class TestGetSelectedStreams(unittest.TestCase):
    """Test that Singer catalog selection is respected."""

    def test_returns_only_selected_streams(self):
        catalog = Catalog([
            _make_entry("campaigns", selected=True),
            _make_entry("accounts", selected=False),
        ])
        selected = [s.stream for s in catalog.get_selected_streams({})]
        self.assertIn("campaigns", selected)
        self.assertNotIn("accounts", selected)

    def test_empty_catalog(self):
        catalog = Catalog([])
        selected = [s.stream for s in catalog.get_selected_streams({})]
        self.assertEqual(selected, [])


class TestSyncOrchestration(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.client.config = dict(DEFAULT_CONFIG)
        self.state = {}

    @patch("singer.write_state")
    def test_no_sync_when_no_selected_streams(self, mock_ws):
        """With an empty/unselected catalog, no stream sync should fire."""
        catalog = Catalog([_make_entry("campaigns", selected=False)])
        with patch("tap_bing_ads.sync.STREAMS") as mock_streams:
            mock_cls = MagicMock()
            mock_cls.parent = None
            mock_cls.children = []
            mock_streams.__getitem__ = MagicMock(return_value=mock_cls)
            mock_streams.__contains__ = MagicMock(return_value=True)
            mock_streams.items = MagicMock(return_value=[])
            mock_streams.keys = MagicMock(return_value=[])
            sync(self.client, DEFAULT_CONFIG, catalog, self.state)
            mock_cls.return_value.sync.assert_not_called()

    @patch("singer.write_state")
    def test_accounts_sync_called_when_selected(self, mock_ws):
        """When accounts is selected, its sync() should be called once."""
        catalog = Catalog([_make_entry("accounts", selected=True, replication_method="INCREMENTAL")])

        mock_accounts_obj = MagicMock()
        mock_accounts_obj.parent = None
        mock_accounts_obj.is_selected.return_value = True
        mock_accounts_obj.children = []
        mock_accounts_obj.child_to_sync = []
        mock_accounts_obj.sync.return_value = 3

        mock_accounts_cls = MagicMock(return_value=mock_accounts_obj)
        mock_accounts_cls.parent = None
        mock_accounts_cls.children = []

        with patch.dict("tap_bing_ads.sync.STREAMS", {"accounts": mock_accounts_cls}):
            with patch("tap_bing_ads.sync.REPORT_STREAMS", {}):
                sync(self.client, DEFAULT_CONFIG, catalog, self.state)

        mock_accounts_obj.sync.assert_called_once()

    @patch("singer.write_state")
    def test_report_streams_called_per_account(self, mock_ws):
        """Reports run per account without adding accounts to the core sync."""
        catalog = Catalog([_make_entry("keyword_performance_report", selected=True)])

        mock_report_obj = MagicMock()
        mock_report_obj.parent = "accounts"
        mock_report_obj.children = []
        mock_report_obj.child_to_sync = []
        mock_report_obj.sync.return_value = 10
        mock_report_cls = MagicMock(return_value=mock_report_obj)
        mock_report_cls.parent = "accounts"

        with patch("tap_bing_ads.sync.STREAMS", {"keyword_performance_report": mock_report_cls}):
            with patch("tap_bing_ads.sync.REPORT_STREAMS", {"keyword_performance_report": mock_report_cls}):
                sync(self.client, DEFAULT_CONFIG, catalog, self.state)

        # 2 account IDs in DEFAULT_CONFIG → should be called twice from report loop
        report_calls = [c for c in mock_report_obj.sync.call_args_list if c.kwargs.get("account_id")]
        self.assertEqual(len(report_calls), 2)

    @patch("singer.write_state")
    def test_account_ids_are_parsed_from_config(self, mock_ws):
        """Both account IDs from comma-separated config should be used for reports."""
        catalog = Catalog([_make_entry("keyword_performance_report", selected=True)])

        mock_report_obj = MagicMock()
        mock_report_obj.parent = None
        mock_report_obj.sync.return_value = 0
        mock_report_cls = MagicMock(return_value=mock_report_obj)
        mock_report_cls.parent = None

        with patch("tap_bing_ads.sync.STREAMS", {"keyword_performance_report": mock_report_cls}):
            with patch("tap_bing_ads.sync.REPORT_STREAMS", {"keyword_performance_report": mock_report_cls}):
                sync(self.client, DEFAULT_CONFIG, catalog, self.state)

        account_ids_used = [c.kwargs.get("account_id") for c in mock_report_obj.sync.call_args_list]
        self.assertIn("67890", account_ids_used)
        self.assertIn("11111", account_ids_used)
