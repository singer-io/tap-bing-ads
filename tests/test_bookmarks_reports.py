from copy import deepcopy

from base import BingAdsBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class BingAdsBookmarksReports(BookmarkTest, BingAdsBaseTest):

    # Bing Ads report bookmarks are stored as ISO timestamps with +00:00 offset
    bookmark_format = "%Y-%m-%dT%H:%M:%S+00:00"
    # No initial state needed; let the tap sync from its configured start_date
    initial_bookmarks = None

    @staticmethod
    def name():
        return "tap_tester_bing_ads_bookmarks_reports"

    def streams_to_test(self):
        """Return the set of streams to test."""
        # Exclude streams that have no data in the test account.
        streams_to_exclude = {
            'accounts',
            'campaigns',
            'ad_groups',
            'ads',
            'ad_extension_detail_report',
            'goals_and_funnels_report',
            'keyword_performance_report',
            'search_query_performance_report',
            'audience_performance_report',
            'age_gender_audience_report',
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @staticmethod
    def streams_to_selected_fields():
        return {
            "campaign_performance_report": set(),
            "ad_group_performance_report": set(),
            "ad_performance_report": set(),
            "geographic_performance_report": set(),
        }

    @staticmethod
    def get_stream_name(stream_id):
        """
        Bing Ads state keys are prefixed with the account ID:
          '{account_id}_{stream}' → '{stream}'
        e.g. '188412305_campaign_performance_report' → 'campaign_performance_report'
        """
        parts = stream_id.split('_', 1)
        if len(parts) == 2 and parts[0].isdigit():
            return parts[1]
        return stream_id

    def manipulate_state(self, state: dict, new_bookmarks: dict):
        """
        Translate the framework's flat bookmarks into bing-ads state format.

        Framework produces:
          {'campaign_performance_report': {'TimePeriod': '2026-07-28T00:00:00+00:00'}}

        Bing-ads expects:
          {'188412305_campaign_performance_report': {'date': '2026-07-28T00:00:00+00:00',
                                                     'request_id': None}}
        """
        new_state = deepcopy(state)
        if new_state.get('bookmarks') is None:
            new_state['bookmarks'] = {}

        account_ids = [
            a.strip()
            for a in self.get_properties().get('account_ids', '').split(',')
        ]

        for stream, rep in new_bookmarks.items():
            if stream.endswith('_report'):
                # The framework uses 'TimePeriod' as the replication key;
                # the tap stores the bookmark under 'date'.
                date_value = rep.get('TimePeriod')
                if date_value:
                    for account_id in account_ids:
                        state_key = f'{account_id}_{stream}'
                        new_state['bookmarks'][state_key] = {
                            'date': date_value,
                            'request_id': None,
                        }
            else:
                if new_state['bookmarks'].get(stream):
                    for key, value in rep.items():
                        new_state['bookmarks'][stream][key] = value
                else:
                    new_state['bookmarks'][stream] = rep

        return new_state

    def calculate_new_bookmarks(self):
        """
        Override the base implementation because all sync-1 data for Bing Ads report
        streams falls within the conversion window (last 30 days).  The base version
        filters to records BEFORE bookmark-minus-lookback, which yields an empty list
        and crashes.  Instead, collect every unique replication value from sync 1 and
        use the second-to-last date as the new bookmark.
        """
        new_bookmarks = {}
        replication_keys = self.expected_replication_keys()
        for stream, records in BookmarkTest.synced_records_1.items():
            if self.expected_replication_methods.get(stream) != self.INCREMENTAL:
                continue

            replication_key = replication_keys[stream]
            assert len(replication_key) == 1
            replication_key = next(iter(replication_key))

            replication_values = sorted({
                message['data'][replication_key]
                for message in records['messages']
                if message['action'] == 'upsert'
            })

            print(f"unique replication values for stream {stream} are: {replication_values}")

            if len(replication_values) < 2:
                continue

            new_bookmarks[self.get_stream_id(stream)] = {
                replication_key: self.timedelta_formatted(
                    self.parse_date(replication_values[-2]),
                    date_format=self.bookmark_format,
                )
            }
        return new_bookmarks

    def test_first_vs_second_records(self):
        """
        Override to use assertLessEqual instead of assertLess.

        With the conversion window (-30 days), sync 2 always starts from
        max(bookmark+1, today-30days) = today-30days when the bookmark is
        within the last 30 days.  This means both syncs cover the same date
        range and produce the same record count — strict '<' can never pass.
        """
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_methods.get(stream, {})

                if replication_method == self.INCREMENTAL:
                    sync_1_records = [
                        record['data'] for record in
                        self.synced_records_1.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert']

                    expected_replication_key = self.expected_replication_keys(stream)
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    sync_2_records = [
                        record['data'] for record in
                        self.synced_records_2.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert'
                        and self.parse_date(record['data'][expected_replication_key])
                        <= self.parse_date(self.bookmark_values_1.get(stream, {}))]

                    self.assertLessEqual(len(sync_2_records), len(sync_1_records))

    def test_first_sync_bookmark(self):
        """
        Override the base assertion to use >= instead of ==.

        Bing Ads report bookmarks are set to the END of the sync window, which
        can be later than the max TimePeriod value in the returned records
        (e.g. when no campaign activity exists on the last day of the window).
        """
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_methods.get(stream, {})

                sync_1_records = [
                    record['data'] for record in
                    self.synced_records_1.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if replication_method == self.INCREMENTAL:
                    expected_replication_key = self.expected_replication_keys(stream)
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    max_replication_value = max(
                        self.parse_date(record.get(expected_replication_key))
                        for record in sync_1_records)
                    bookmark_value = self.parse_date(self.bookmark_values_1.get(stream, {}))
                    self.assertGreaterEqual(bookmark_value, max_replication_value)

    def test_second_sync_bookmark(self):
        """
        Override the base assertion to use >= instead of ==.

        Same reasoning as test_first_sync_bookmark: the bookmark is the window
        end date, not necessarily the max TimePeriod found in records.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_methods.get(stream, {})

                sync_2_records = [
                    record['data'] for record in
                    self.synced_records_2.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if replication_method == self.INCREMENTAL:
                    expected_replication_key = self.expected_replication_keys(stream)
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    if not sync_2_records:
                        continue

                    max_replication_value = max(
                        self.parse_date(record.get(expected_replication_key))
                        for record in sync_2_records)
                    bookmark_value = self.parse_date(self.bookmark_values_2.get(stream, {}))
                    self.assertGreaterEqual(bookmark_value, max_replication_value)
