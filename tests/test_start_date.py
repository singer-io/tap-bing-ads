"""Table-reset coverage for Bing Ads accounts."""

from copy import deepcopy

from tap_tester.base_suite_tests.table_reset_test import TableResetTest

from base import BingAdsBaseTest


class BingAdsAccountTableResetTest(TableResetTest, BingAdsBaseTest):
    """Verify resetting the REST accounts bookmark replays its records.

    Child core streams use parent-driven replication and separate bookmarks, so
    they are not selected for this accounts-only reset scenario.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._reset_results = None

    def setUp(self):
        cls = type(self)
        if cls._reset_results is None:
            super().setUp()
            cls._reset_results = deepcopy({
                'reset_records': self.reset_records,
                'synced_records': TableResetTest.synced_records,
                'second_sync_records': TableResetTest.second_sync_records,
                'second_sync_bookmarks': TableResetTest.second_sync_bookmarks,
            })
        for name, value in cls._reset_results.items():
            setattr(self, name, deepcopy(value))

    @staticmethod
    def name():
        return 'tt_bing_acct_reset'

    def streams_to_test(self):
        return {'accounts'}

    @property
    def reset_stream(self):
        return 'accounts'

    def manipulate_state(self, current_state):
        return current_state

    @staticmethod
    def set_replication_methods(conn_id, catalogs, replication_methods):
        """Bing Ads replication methods are fixed by stream and need no overrides."""

    def test_reset_records_replayed(self):
        """Verify record contents and duplicates, not just the inherited row count."""
        self.assertSetEqual(set(self.synced_records), self.streams_to_test())
        self.assertSetEqual(set(self.second_sync_records), self.streams_to_test())
        first_records = [
            message['data']
            for message in self.synced_records['accounts']['messages']
            if message.get('action') == 'upsert'
        ]
        self.assertEqual(len(self.reset_records), 1)
        replayed_records = self.reset_records[0]['filtered_second_sync_records']
        self.assertTrue(first_records, msg='Accounts test data is required to verify a reset')
        self.assertCountEqual(first_records, replayed_records)
        self.assertIsNone(self.second_sync_bookmarks.get('currently_syncing'))
