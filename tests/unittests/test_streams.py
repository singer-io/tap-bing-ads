"""Unit tests for stream sync logic (core streams) — new parent-child architecture."""
import unittest
from unittest.mock import MagicMock, patch, call

import singer
from singer.catalog import CatalogEntry, Schema
from singer import metadata

from tap_bing_ads.streams.campaigns import Campaigns
from tap_bing_ads.streams.ad_groups import AdGroups
from tap_bing_ads.streams.ads import Ads
from tap_bing_ads.streams.accounts import Accounts
from tap_bing_ads.client import Client

DEFAULT_CONFIG = {
    "oauth_client_id": "test_id",
    "oauth_client_secret": "test_secret",
    "refresh_token": "test_refresh",
    "developer_token": "test_dev",
    "customer_id": "12345",
    "account_ids": "67890",
    "start_date": "2024-01-01T00:00:00Z",
}

MOCK_CAMPAIGNS = [
    {"Id": 1, "Name": "Campaign A", "Status": "Active", "CampaignType": "Search"},
    {"Id": 2, "Name": "Campaign B", "Status": "Paused", "CampaignType": "Shopping"},
]

MOCK_AD_GROUPS = [
    {"Id": 10, "Name": "AdGroup A", "Status": "Active"},
]

MOCK_ADS = [
    {"Id": 100, "Type": "ExpandedText", "Status": "Active"},
]


def _make_catalog_entry(stream_cls):
    """Build a minimal CatalogEntry with all fields selected."""
    base_props = ["Id", "Name", "Status"]
    # Include replication keys so they survive transformation
    for rk in (getattr(stream_cls, "replication_keys", None) or []):
        if rk not in base_props:
            base_props.append(rk)
    schema_dict = {
        "type": "object",
        "properties": {k: {"type": ["null", "string"]} for k in base_props},
    }
    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema_dict,
        key_properties=stream_cls.key_properties,
        valid_replication_keys=getattr(stream_cls, "replication_keys", []) or [],
        replication_method=stream_cls.replication_method,
    )
    mdata = metadata.to_map(mdata)
    mdata = metadata.write(mdata, (), "selected", True)
    for prop in schema_dict["properties"]:
        mdata = metadata.write(mdata, ("properties", prop), "selected", True)

    return CatalogEntry(
        stream=stream_cls.tap_stream_id,
        tap_stream_id=stream_cls.tap_stream_id,
        key_properties=stream_cls.key_properties,
        schema=Schema.from_dict(schema_dict),
        metadata=metadata.to_list(mdata),
    )


class TestCampaignsStream(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.client.config = dict(DEFAULT_CONFIG)
        self.catalog_entry = _make_catalog_entry(Campaigns)
        self.stream = Campaigns(self.client, self.catalog_entry)

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_writes_campaigns(self, mock_write_record, mock_write_schema):
        self.client.make_request.return_value = {"Campaigns": MOCK_CAMPAIGNS}
        transformer = singer.Transformer()
        parent_record = {"Id": "67890", "LastModifiedTime": "2024-06-01T00:00:00Z"}
        count = self.stream.sync({}, transformer, parent_obj=parent_record)

        self.assertEqual(count, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_handles_empty_response(self, mock_write_record, mock_write_schema):
        self.client.make_request.return_value = {"Campaigns": []}
        transformer = singer.Transformer()
        count = self.stream.sync({}, transformer, parent_obj={"Id": "67890"})
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_no_parent_record_yields_nothing(self, mock_write_record, mock_write_schema):
        """When no parent_obj is provided, no records should be written."""
        self.client.make_request.return_value = {"Campaigns": []}
        transformer = singer.Transformer()
        count = self.stream.sync({}, transformer, parent_obj=None)
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_injects_account_id_for_children(self, mock_write_record, mock_write_schema):
        """get_records should inject account_id into each campaign record."""
        self.client.make_request.return_value = {"Campaigns": [{"Id": 1, "Name": "C"}]}
        transformer = singer.Transformer()

        # Capture the records passed to a child stream
        child_records = []
        mock_child = MagicMock()
        mock_child.sync.side_effect = lambda state, tr, parent_obj=None, **kw: child_records.append(parent_obj)
        self.stream.child_to_sync = [mock_child]

        self.stream.sync({}, transformer, parent_obj={"Id": "67890", "LastModifiedTime": "2024-06-01T00:00:00Z"})
        self.assertTrue(len(child_records) > 0)
        self.assertEqual(child_records[0].get("account_id"), "67890")


class TestAdGroupsStream(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.client.config = dict(DEFAULT_CONFIG)
        self.catalog_entry = _make_catalog_entry(AdGroups)
        self.stream = AdGroups(self.client, self.catalog_entry)

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_with_parent_record(self, mock_write_record, mock_write_schema):
        self.client.make_request.return_value = {"AdGroups": MOCK_AD_GROUPS}
        transformer = singer.Transformer()
        parent_record = {"Id": "1", "account_id": "67890", "accounts_LastModifiedTime": "2024-06-01T00:00:00Z"}
        count = self.stream.sync({}, transformer, parent_obj=parent_record)
        self.assertEqual(count, 1)
        self.client.make_request.assert_called_once()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_no_parent_yields_nothing(self, mock_write_record, mock_write_schema):
        self.client.make_request.return_value = {"AdGroups": []}
        transformer = singer.Transformer()
        count = self.stream.sync({}, transformer, parent_obj=None)
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_injects_account_id_for_children(self, mock_write_record, mock_write_schema):
        """get_records should forward account_id from campaign to each ad_group."""
        self.client.make_request.return_value = {"AdGroups": [{"Id": 10, "Name": "AG"}]}
        transformer = singer.Transformer()

        child_records = []
        mock_child = MagicMock()
        mock_child.sync.side_effect = lambda state, tr, parent_obj=None, **kw: child_records.append(parent_obj)
        self.stream.child_to_sync = [mock_child]

        self.stream.sync({}, transformer, parent_obj={"Id": "1", "account_id": "67890", "accounts_LastModifiedTime": "2024-06-01T00:00:00Z"})
        self.assertTrue(len(child_records) > 0)
        self.assertEqual(child_records[0].get("account_id"), "67890")


class TestAdsStream(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.client.config = dict(DEFAULT_CONFIG)
        self.catalog_entry = _make_catalog_entry(Ads)
        self.stream = Ads(self.client, self.catalog_entry)

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_writes_ads(self, mock_write_record, mock_write_schema):
        self.client.make_request.return_value = {"Ads": MOCK_ADS}
        transformer = singer.Transformer()
        parent_record = {"Id": "10", "account_id": "67890", "campaigns_LastModifiedTime": "2024-06-01T00:00:00Z"}
        count = self.stream.sync({}, transformer, parent_obj=parent_record)
        self.assertEqual(count, 1)
        mock_write_record.assert_called_once()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_no_parent_yields_nothing(self, mock_write_record, mock_write_schema):
        self.client.make_request.return_value = {"Ads": []}
        transformer = singer.Transformer()
        count = self.stream.sync({}, transformer, parent_obj=None)
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()


class TestAccountsStream(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.client.config = dict(DEFAULT_CONFIG)
        self.catalog_entry = _make_catalog_entry(Accounts)
        self.stream = Accounts(self.client, self.catalog_entry)

    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_writes_account_records(
        self, mock_record, mock_schema, mock_state, mock_bookmark
    ):
        self.client.make_request.return_value = {
            "Account": {"Id": 67890, "Name": "Test Account", "LastModifiedTime": "2024-06-01T00:00:00Z"}
        }
        transformer = singer.Transformer()
        self.stream.sync({}, transformer)
        mock_record.assert_called_once()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_respects_bookmark(self, mock_record, mock_schema):
        """Records older than bookmark should be filtered out."""
        self.client.make_request.return_value = {
            "Account": {
                "Id": 67890,
                "Name": "Old Account",
                "LastModifiedTime": "2023-01-01T00:00:00Z",
            }
        }
        state = {"bookmarks": {"accounts": {"last_record": "2024-01-01T00:00:00Z"}}}
        transformer = singer.Transformer()
        self.stream.sync(state, transformer)
        mock_record.assert_not_called()

    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_sync_triggers_children(self, mock_record, mock_schema):
        """Each account record should be passed to child streams."""
        self.client.make_request.return_value = {
            "Account": {"Id": 67890, "Name": "Acct", "LastModifiedTime": "2025-01-01T00:00:00Z"}
        }
        mock_child = MagicMock()
        mock_child.sync.return_value = 0
        self.stream.child_to_sync = [mock_child]

        transformer = singer.Transformer()
        self.stream.sync({}, transformer)
        mock_child.sync.assert_called_once()
        # parent_obj passed to child should have account Id
        _, kwargs = mock_child.sync.call_args
        self.assertEqual(str(kwargs["parent_obj"].get("Id")), "67890")
