from datetime import datetime as dt
from datetime import timedelta
from datetime import timezone as tz

from base import BingAdsBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class BingAdsStartDateTest(StartDateTest, BingAdsBaseTest):
    """
    Test that report streams respect the configured start_date.

    Two syncs are run:
      - Sync 1: start_date_1 ~90 days ago  →  covers the full available data window
      - Sync 2: start_date_2 ~15 days ago  →  covers only the recent tail of that window

    Assertions (per the StartDateTest framework):
      - Both syncs replicated records for every tested stream.
      - Replication-key (TimePeriod) values in each sync are >= the respective start_date.
      - Every record in sync 2 also appears in sync 1 (sync 2 is a strict subset).
      - For streams that respect start_date, sync 1 replicates >= sync 2 records.
    """

    # Evaluated once at class-definition time so that setUp can mutate self.start_date freely.
    start_date_1 = dt.strftime(
        dt.now(tz.utc) - timedelta(days=90), "%Y-%m-%dT00:00:00Z"
    )
    start_date_2 = dt.strftime(
        dt.now(tz.utc) - timedelta(days=15), "%Y-%m-%dT00:00:00Z"
    )

    @staticmethod
    def name():
        return "tap_tester_bing_ads_start_date_test"

    def streams_to_test(self):
        # Only report streams: they use TimePeriod as replication key and respect start_date.
        # accounts / campaigns / ad_groups / ads do not obey start date.
        streams_to_exclude = {
            'accounts', 'campaigns', 'ad_groups', 'ads',
            'ad_extension_detail_report', 'goals_and_funnels_report',
            'keyword_performance_report', 'search_query_performance_report',
            'audience_performance_report', 'age_gender_audience_report',
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @staticmethod
    def streams_to_selected_fields():
        # Select all available fields for each report stream.
        return {
            "campaign_performance_report": set(),
            "ad_group_performance_report": set(),
            "ad_performance_report": set(),
            "geographic_performance_report": set(),
        }

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def test_replicated_records(self):
        """
        Override the base assertion to use assertGreaterEqual instead of
        assertGreater for report streams.

        The test account has sparse data: for small report streams
        (ad_performance_report, ad_group_performance_report,
        campaign_performance_report) all 12 records may fall on the same
        dates, meaning sync 1 and sync 2 can have the same record count when
        start_date_2 still covers those dates.  assertGreaterEqual avoids a
        false failure while still verifying that sync 2 is never larger than
        sync 1 and that all sync-2 records were already present in sync 1.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                expected_primary_keys = self.expected_primary_keys(stream)

                # compound replication key not supported
                assert len(self.expected_replication_keys(stream)) == 1
                expected_replication_key = next(
                    iter(self.expected_replication_keys(stream)))

                record_count_sync_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)

                # Dates replicated in sync 1 — used to filter out records added between syncs.
                replication_dates_1 = {
                    record['data'].get(expected_replication_key)
                    for record in
                    StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                # All PKs from sync 2 whose date is within the sync-1 window.
                primary_keys_sync_2 = {
                    tuple(msg['data'][pk] for pk in expected_primary_keys)
                    for msg in
                    StartDateTest.synced_messages_by_stream_2.get(
                        stream, {}).get('messages', [])
                    if msg.get('action') == 'upsert'
                    and self.parse_date(msg['data'][expected_replication_key])
                    <= self.parse_date(max(replication_dates_1))}

                # All PKs from sync 1 that fall on or after start_date_2
                # (i.e., records sync 2 should also see).
                primary_keys_sync_1 = {
                    tuple(msg['data'][pk] for pk in expected_primary_keys)
                    for msg in
                    StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if msg.get('action') == 'upsert'
                    and self.parse_date(msg['data'][expected_replication_key])
                    >= self.parse_date(self.start_date_2)}

                # Sync 1 should have at least as many records as sync 2
                # (use assertGreaterEqual — equal counts are valid on sparse test accounts).
                self.assertGreaterEqual(
                    record_count_sync_1, record_count_sync_2,
                    msg=f"stream {stream}: sync 2 has MORE records than sync 1 "
                        f"({record_count_sync_2} > {record_count_sync_1})")

                # Every record visible in sync 2 must also have appeared in sync 1.
                self.assertSetEqual(
                    primary_keys_sync_1, primary_keys_sync_2,
                    msg=f"stream {stream}: sync 2 contains records not present in sync 1")
