"""Core-stream record replay coverage, excluding recovery order and marker cleanup."""

import copy

from tap_tester import menagerie, runner
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest

from base_new_framework import BingAdsBaseTest


class CoreInterruptedSyncMixin:
    """Exercise completed and unstarted accounts alongside full-table recovery."""

    test_interrupted_sync_stream_order = None

    @classmethod
    def name(cls):
        return 'tt_bing_core_' + cls.accounts_role

    def streams_to_test(self):
        return {'accounts', 'campaigns', 'ad_groups', 'ads'}

    def setUp(self):
        cls = type(self)
        results = cls.__dict__.get('_sync_results')
        if results is None:
            conn_id = self.get_connection_id()
            catalogs = self.run_and_verify_check_mode(conn_id)
            selected = [catalog for catalog in catalogs
                        if catalog['stream_name'] in self.streams_to_test()]
            self.perform_and_verify_table_and_field_selection(conn_id, selected)
            menagerie.set_state(conn_id, {})
            self.run_and_verify_sync_mode(conn_id)
            self.first_sync_records = runner.get_records_from_target_output()
            self.first_sync_state = menagerie.get_state(conn_id)
            menagerie.set_state(conn_id, self.manipulate_state())
            self.record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
            self.resuming_sync_records = runner.get_records_from_target_output()
            self.resuming_sync_state = menagerie.get_state(conn_id)
            results = {
                key: getattr(self, key) for key in (
                    'first_sync_records', 'first_sync_state', 'record_count_by_stream',
                    'resuming_sync_records', 'resuming_sync_state')
            }
            cls._sync_results = copy.deepcopy(results)
        for key, value in results.items():
            setattr(self, key, copy.deepcopy(value))

    @staticmethod
    def records(sync_records, stream):
        return [message['data']
                for message in sync_records.get(stream, {}).get('messages', [])
                if message.get('action') == 'upsert']

    @staticmethod
    def get_bookmark_value(state, stream):
        return state.get('bookmarks', {}).get(stream, {}).get('last_record')

    def manipulate_state(self):
        state = copy.deepcopy(self.first_sync_state)
        state['currently_syncing'] = self.interrupted_stream
        if self.accounts_role == 'unstarted':
            state['bookmarks'].pop('accounts', None)
        return state

    def test_recovery_scenario_coverage(self):
        state = self.manipulate_state()
        self.assertSetEqual(set(self.first_sync_records), self.streams_to_test())
        self.assertSetEqual(set(self.resuming_sync_records), self.streams_to_test())
        self.assertSetEqual(set(self.first_sync_state['bookmarks']), {'accounts'})
        self.assertSetEqual(set(self.resuming_sync_state['bookmarks']), {'accounts'})
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                self.assertTrue(self.records(self.first_sync_records, stream),
                                msg=f'Populate test data for {stream}; coverage requires records')
        if self.accounts_role == 'unstarted':
            self.assertNotIn('accounts', state['bookmarks'])
        else:
            self.assertIn('accounts', state['bookmarks'])
            self.assertNotEqual(state['currently_syncing'], 'accounts')

    def test_not_yet_synced_start_date(self):
        if self.accounts_role != 'unstarted':
            self.assertIn('accounts', self.manipulate_state()['bookmarks'])
            return
        first = self.records(self.first_sync_records, 'accounts')
        resumed = self.records(self.resuming_sync_records, 'accounts')
        self.assertTrue(first)
        self.assertTrue(resumed)
        self.assertEqual(
            min(self.parse_date(record['LastModifiedTime']) for record in first),
            min(self.parse_date(record['LastModifiedTime']) for record in resumed))

    def test_full_replication_streams(self):
        for stream in self.streams_to_test() - {'accounts'}:
            with self.subTest(stream=stream):
                self.assertEqual(self.expected_replication_method(stream), self.FULL_TABLE)
                self.assertNotIn(stream, self.first_sync_state['bookmarks'])
                self.assertNotIn(stream, self.resuming_sync_state['bookmarks'])
                first = self.records(self.first_sync_records, stream)
                resumed = self.records(self.resuming_sync_records, stream)
                self.assertTrue(first, msg='Initial full-table sync needs records')
                self.assertTrue(resumed, msg='Recovery full-table sync needs records')
                self.assertEqual(len(first), len(resumed))
                self.assertCountEqual(
                    first, resumed,
                    msg='There is a difference in full-table records between syncs')

    def test_syncs_were_successful(self):
        """Verify bookmarks and other final state, excluding currently_syncing cleanup."""
        self.assertIsNotNone(self.first_sync_state.get('bookmarks'))
        self.assertIsNotNone(self.resuming_sync_state.get('bookmarks'))
        first = copy.deepcopy(self.first_sync_state)
        resumed = copy.deepcopy(self.resuming_sync_state)
        first.pop('currently_syncing', None)
        resumed.pop('currently_syncing', None)
        self.assertDictEqual(resumed, first)


class TestBingAdsCompletedAccountsSync(
        CoreInterruptedSyncMixin, InterruptedSyncTest, BingAdsBaseTest):
    """Verify core record replay with already-completed accounts."""

    interrupted_stream = 'campaigns'
    accounts_role = 'completed'


class TestBingAdsUnstartedAccountsSync(
        CoreInterruptedSyncMixin, InterruptedSyncTest, BingAdsBaseTest):
    """Verify core record replay with accounts not yet bookmarked."""

    interrupted_stream = 'campaigns'
    accounts_role = 'unstarted'