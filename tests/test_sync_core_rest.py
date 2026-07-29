"""
Integration tests for sync mode — core entity streams.

These tests mock the Microsoft Advertising REST API responses and verify
that the tap writes the correct Singer messages.
"""
import time
import unittest
from unittest.mock import MagicMock, patch, call

import singer
from singer.catalog import Catalog

from tap_bing_ads.client import Client
from tap_bing_ads.sync import sync
from tests.integration_base import (
    DEFAULT_CONFIG,
    BingAdsIntegrationBase,
    make_report_zip,
    mock_token_response,
)

MOCK_ACCOUNT_RESP = {
    "Account": {
        "Id": 188412305,
        "Name": "Test Account",
        "AccountLifeCycleStatus": "Active",
        "CurrencyCode": "USD",
        "TimeZone": "EasternTimeUSandCanada",
        "LastModifiedTime": "2024-06-15T10:00:00",
    }
}

MOCK_CAMPAIGNS_RESP = {
    "Campaigns": [
        {"Id": 1001, "Name": "Search Campaign", "Status": "Active", "CampaignType": "Search"},
        {"Id": 1002, "Name": "Shopping Campaign", "Status": "Paused", "CampaignType": "Shopping"},
    ]
}

MOCK_AD_GROUPS_RESP = {
    "AdGroups": [
        {"Id": 2001, "Name": "AdGroup A", "Status": "Active", "CampaignId": 1001},
        {"Id": 2002, "Name": "AdGroup B", "Status": "Active", "CampaignId": 1001},
    ]
}

MOCK_ADS_RESP = {
    "Ads": [
        {"Id": 3001, "Type": "ExpandedText", "Status": "Active"},
        {"Id": 3002, "Type": "ResponsiveSearchAd", "Status": "Active"},
    ]
}


class TestSyncCoreStreams(BingAdsIntegrationBase):
    """Sync tests for core entity streams (campaigns, ad_groups, ads)."""

    def _make_client(self):
        client = MagicMock(spec=Client)
        return client

    @patch("singer.write_record")
    @patch("singer.write_schema")
    def test_campaigns_syncs_records(self, mock_schema, mock_record):
        """All campaigns are written to the Singer output."""
        client = self._make_client()
        client.get.return_value = MOCK_CAMPAIGNS_RESP

        catalog = self.get_catalog(selected_streams=["campaigns"])
        with patch("tap_bing_ads.sync.STREAMS") as mock_streams:
            from tap_bing_ads.streams.campaigns import Campaigns
            # Use real Campaigns class but with mocked client
            mock_streams.__contains__ = lambda s, k: True
            mock_streams.__getitem__ = lambda s, k: (
                Campaigns if k in ("campaigns",) else MagicMock()
            )
            sync(client, self.config, catalog, self.state)

        self.assertGreaterEqual(mock_record.call_count, 1)

    @patch("singer.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_campaigns_writes_schema_once(self, mock_schema, mock_record):
        """write_schema should be called exactly once for campaigns."""
        client = self._make_client()
        client.get.return_value = MOCK_CAMPAIGNS_RESP

        catalog = self.get_catalog(selected_streams=["campaigns"])
        from tap_bing_ads.streams.campaigns import Campaigns
        stream_obj = Campaigns(client, catalog.streams[0])
        state = {}
        transformer = singer.Transformer()
        stream_obj.sync(state, transformer, account_id="188412305")
        mock_schema.assert_called_once_with("campaigns", unittest.mock.ANY, ["Id"])

    @patch("singer.write_record")
    @patch("singer.write_schema")
    def test_ad_groups_syncs_per_campaign(self, mock_schema, mock_record):
        """Ad groups are fetched for each campaign ID."""
        client = self._make_client()
        client.get.return_value = MOCK_AD_GROUPS_RESP

        catalog = self.get_catalog(selected_streams=["ad_groups"])
        from tap_bing_ads.streams.ad_groups import AdGroups
        stream_obj = AdGroups(client, catalog.streams[0])
        state = {}
        transformer = singer.Transformer()
        count = stream_obj.sync(
            state, transformer,
            account_id="188412305",
            campaign_ids=["1001", "1002"]
        )
        # 2 ad groups × 2 campaigns = 4 total records
        self.assertEqual(count, 4)
        self.assertEqual(client.get.call_count, 2)

    @patch("singer.write_record")
    @patch("singer.write_schema")
    def test_ads_syncs_per_ad_group(self, mock_schema, mock_record):
        """Ads are fetched for each ad group ID."""
        client = self._make_client()
        client.get.return_value = MOCK_ADS_RESP

        catalog = self.get_catalog(selected_streams=["ads"])
        from tap_bing_ads.streams.ads import Ads
        stream_obj = Ads(client, catalog.streams[0])
        state = {}
        transformer = singer.Transformer()
        count = stream_obj.sync(
            state, transformer,
            account_id="188412305",
            ad_group_ids=["2001", "2002"]
        )
        # 2 ads × 2 ad groups = 4 total records
        self.assertEqual(count, 4)


class TestSyncAccounts(BingAdsIntegrationBase):
    """Sync tests for the accounts stream."""

    @patch("tap_bing_ads.streams.accounts.write_bookmark")
    @patch("tap_bing_ads.streams.accounts.write_state")
    @patch("singer.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    def test_accounts_writes_records(self, mock_schema, mock_record, mock_state, mock_bookmark):
        client = MagicMock(spec=Client)
        client.get.return_value = MOCK_ACCOUNT_RESP

        catalog = self.get_catalog(selected_streams=["accounts"])
        from tap_bing_ads.streams.accounts import Accounts
        stream_obj = Accounts(client, catalog.streams[0])
        transformer = singer.Transformer()
        count = stream_obj.sync({}, transformer, account_ids=["188412305"])

        self.assertEqual(count, 1)
        mock_record.assert_called_once()
        mock_bookmark.assert_called_once()

    @patch("singer.write_record")
    @patch("singer.write_schema")
    def test_accounts_filters_by_bookmark(self, mock_schema, mock_record):
        """Accounts older than the bookmark are not emitted."""
        client = MagicMock(spec=Client)
        client.get.return_value = MOCK_ACCOUNT_RESP  # LastModifiedTime = 2024-06-15

        state = {"bookmarks": {"accounts": {"last_record": "2024-12-31T00:00:00"}}}
        catalog = self.get_catalog(selected_streams=["accounts"])
        from tap_bing_ads.streams.accounts import Accounts
        stream_obj = Accounts(client, catalog.streams[0])
        transformer = singer.Transformer()
        count = stream_obj.sync(state, transformer, account_ids=["188412305"])

        self.assertEqual(count, 0)
        mock_record.assert_not_called()


class TestSyncWithMultipleAccounts(BingAdsIntegrationBase):
    """Verify sync iterates over all configured account IDs."""

    @patch("tap_bing_ads.sync._sync_account")
    @patch("tap_bing_ads.sync.write_state")
    def test_sync_called_for_each_account(self, mock_ws, mock_sync_acct):
        client = MagicMock(spec=Client)
        catalog = self.get_catalog(selected_streams=["campaigns"])
        sync(client, self.config, catalog, self.state)

        # config has "188412305,188412306" — 2 accounts
        self.assertEqual(mock_sync_acct.call_count, 2)
        account_ids_processed = [c.args[4] for c in mock_sync_acct.call_args_list]
        self.assertIn("188412305", account_ids_processed)
        self.assertIn("188412306", account_ids_processed)


if __name__ == "__main__":
    unittest.main()
