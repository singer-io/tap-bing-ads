"""Table-reset coverage for Bing Ads accounts."""

from tap_tester.base_suite_tests.table_reset_test import TableResetTest

from base_new_framework import BingAdsBaseTest


class BingAdsAccountTableResetTest(TableResetTest, BingAdsBaseTest):
    """Verify resetting the incremental accounts stream replays its records."""

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
    def get_bookmark_value(state, stream):
        return state.get('bookmarks', {}).get(stream, {}).get('last_record')

    @staticmethod
    def set_replication_methods(conn_id, catalogs, replication_methods):
        """Bing Ads replication methods are fixed by stream and need no overrides."""
