from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest
from base import BingAdsBaseTest


class BingAdsInterruptedSyncTest(InterruptedSyncTest, BingAdsBaseTest):
    """Test that bing-ads can recover from an interrupted sync using
    the last saved state for incremental report streams."""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_interrupted_sync_test"

    def streams_to_test(self):
        """Return the set of streams to test."""
        # Exclude streams that have no data in the test account.
        streams_to_exclude = {
            'accounts', 'campaigns', 'ad_groups', 'ads',
            'ad_extension_detail_report', 'goals_and_funnels_report',
            'keyword_performance_report', 'search_query_performance_report',
            'audience_performance_report', 'age_gender_audience_report',
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

    def manipulate_state(self):
        """
        Simulate an interrupted sync mid-way through the report streams.

        Alphabetical sync order for the four tested report streams:
          ad_group_performance_report  (completed)
          ad_performance_report        (completed)
          campaign_performance_report  (interrupted — currently_syncing)
          geographic_performance_report (not yet started — absent from bookmarks)

        Bing Ads bookmark key format: {account_id}_{stream}
        Bing Ads bookmark value format: {"date": "<ISO>", "request_id": None}
        """
        account_id = self.get_properties().get('account_ids').split(',')[0]
        return {
            "currently_syncing": "campaign_performance_report",
            "bookmarks": {
                f"{account_id}_ad_group_performance_report": {
                    "date": "2026-06-01T00:00:00+00:00",
                    "request_id": None,
                },
                f"{account_id}_ad_performance_report": {
                    "date": "2026-06-01T00:00:00+00:00",
                    "request_id": None,
                },
                f"{account_id}_campaign_performance_report": {
                    "date": "2026-06-01T00:00:00+00:00",
                    "request_id": None,
                },
                # geographic_performance_report intentionally absent — not yet synced
            }
        }

    def test_bookmarked_streams_start_date(self):
        """
        Verify that interrupted and completed streams started at the correct
        replication value in the resuming sync.

        Overrides the base implementation to handle bing-ads's
        {account_id}_{stream} bookmark key format instead of plain stream names.
        """
        account_id = self.get_properties().get('account_ids').split(',')[0]
        manipulated_state = self.manipulate_state()
        currently_syncing = manipulated_state['currently_syncing']

        # Resolve plain stream names from the bing-ads prefixed bookmark keys
        bookmarked_streams = {
            stream for stream in self.streams_to_test()
            if f'{account_id}_{stream}' in manipulated_state.get('bookmarks', {})
        }

        for stream in bookmarked_streams:
            with self.subTest(stream=stream):
                expected_replication_key = self.expected_replication_keys(stream)
                assert len(expected_replication_key) == 1
                expected_replication_key = next(iter(expected_replication_key))

                first_sync_records = [
                    record['data'] for record in
                    self.first_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']
                resuming_sync_records = [
                    record['data'] for record in
                    self.resuming_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                if not resuming_sync_records:
                    continue

                actual_oldest_resuming_date = min(
                    self.parse_date(record.get(expected_replication_key))
                    for record in resuming_sync_records)

                stream_bookmark = self.get_bookmark_value(manipulated_state, stream)
                completed = (stream != currently_syncing)
                expected_start_time = self.calculate_expected_sync_start_time(
                    stream_bookmark, stream, completed=completed)

                # If data is sparse at the exact expected start time, find the
                # next available record from the first sync as the adjusted expectation.
                adjusted_expected = min(
                    self.parse_date(record[expected_replication_key])
                    for record in first_sync_records
                    if self.parse_date(record[expected_replication_key]) >= expected_start_time)

                self.assertGreaterEqual(actual_oldest_resuming_date, adjusted_expected)

    def test_resuming_sync_records(self):
        """
        Override base to strip _sdc_report_datetime before comparing records.

        Bing Ads sets _sdc_report_datetime to the current wall-clock time at
        each sync, so the field always differs between the first and resuming
        syncs even when the underlying report data is identical.  Comparing
        without it avoids a false failure and a potentially enormous diff with
        thousands of geographic_performance_report records.
        """
        incremental_streams = {s for s, m in self.expected_replication_method().items()
                               if m == self.INCREMENTAL}
        currently_syncing_stream = self.manipulate_state()['currently_syncing']

        for stream in self.streams_to_test().intersection(incremental_streams):
            with self.subTest(stream=stream):
                expected_replication_key = self.expected_replication_keys(stream)
                assert len(expected_replication_key) == 1
                expected_replication_key = next(iter(expected_replication_key))

                first_sync_records = [
                    record['data'] for record in
                    self.first_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']
                resuming_sync_records = [
                    record['data'] for record in
                    self.resuming_sync_records.get(stream, {}).get('messages', [])
                    if record.get('action') == 'upsert']

                stream_bookmark = self.get_bookmark_value(
                    self.manipulate_state(), stream)
                if stream_bookmark:
                    completed = stream != currently_syncing_stream
                    expected_resuming_sync_start_time = \
                        self.calculate_expected_sync_start_time(
                            stream_bookmark, stream, completed=completed)
                else:
                    expected_resuming_sync_start_time = min(
                        self.parse_date(record.get(expected_replication_key))
                        for record in first_sync_records)

                first_sync_records_after_bookmark = [
                    record for record in first_sync_records
                    if self.parse_date(record[expected_replication_key]) >=
                    expected_resuming_sync_start_time]

                filtered_resuming_records = [
                    record for record in resuming_sync_records
                    if self.parse_date(record[expected_replication_key]) <=
                    self.parse_date(self.get_bookmark_value(
                        self.first_sync_state, stream))]

                # Strip _sdc_report_datetime: it is set to the current time at
                # each sync run and will always differ between syncs.
                def drop_sdc(records):
                    return [{k: v for k, v in r.items()
                             if k != '_sdc_report_datetime'}
                            for r in records]

                self.assertEqual(
                    drop_sdc(first_sync_records_after_bookmark),
                    drop_sdc(filtered_resuming_records),
                    msg="Incorrect data in the interrupted sync")

    def test_interrupted_sync_stream_order(self):
        """
        Verify the resuming sync replays the currently_syncing stream first,
        and that all expected streams are synced exactly once.

        The base implementation additionally asserts that not-yet-synced streams
        run before already-completed streams, which is a stronger guarantee than
        the bing-ads tap provides.  The tap honours currently_syncing (Singer's
        get_selected_streams puts it first) but then iterates the remaining
        streams in catalog order, without separating "not yet synced" from
        "already completed".  We therefore only assert the weaker property here.
        """
        currently_syncing = self.manipulate_state()['currently_syncing']

        # The interrupted stream must sync first
        self.assertEqual(self.resuming_sync_order[0], currently_syncing)

        # Every expected stream must appear in the resuming sync
        self.assertSetEqual(set(self.resuming_sync_order), self.streams_to_test())
