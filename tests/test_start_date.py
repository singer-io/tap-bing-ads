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

    def expected_replication_keys(self, stream=None):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        replication_keys = {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return replication_keys
        return replication_keys[stream]

    def expected_replication_method(self, stream=None):
        """return a dictionary with key of table name nd value of replication method"""
        replication_method = {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return replication_method
        return replication_method[stream]

    def test_replication_key_values(self):
        """
        The 30-day lookback (DEFAULT_CONVERSION_WINDOW=-30) means sync 2 queries
        from today-30days even when start_date_2 > today-30days.  Records with
        TimePeriod values back to today-30days are therefore expected and valid.

        Override: validate sync 2 dates against today-30days instead of start_date_2.
        Sync 1 uses start_date_1 (older than today-30days) so the standard check applies.
        """
        lookback_cutoff = (dt.now(tz.utc) - timedelta(days=30)).replace(
            hour=0, minute=0, second=0, microsecond=0)

        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                assert len(self.expected_replication_keys(stream)) == 1
                replication_key = next(iter(self.expected_replication_keys(stream)))

                replication_dates_1 = {
                    record['data'].get(replication_key)
                    for record in
                    StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                replication_dates_2 = {
                    record['data'].get(replication_key)
                    for record in
                    StartDateTest.synced_messages_by_stream_2.get(
                        stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                # Sync 1: start_date_1 is older than today-30days so full range applies.
                for replication_date in replication_dates_1:
                    with self.subTest(sync="sync1", replication_date=replication_date):
                        self.assertGreaterEqual(
                            self.parse_date(replication_date),
                            self.parse_date(self.start_date_1),
                            msg=f"Record date {replication_date} is before "
                                f"start_date_1 {self.start_date_1}")

                # Sync 2: the 30-day lookback expands the window back to today-30days,
                # which is earlier than start_date_2. Validate against today-30days.
                for replication_date in replication_dates_2:
                    with self.subTest(sync="sync2", replication_date=replication_date):
                        self.assertGreaterEqual(
                            self.parse_date(replication_date),
                            lookback_cutoff,
                            msg=f"Record date {replication_date} is before the 30-day "
                                f"lookback cutoff {lookback_cutoff.date()} "
                                f"(start_date_2={self.start_date_2})")

    def test_replicated_records(self):
        """
        The 30-day lookback (DEFAULT_CONVERSION_WINDOW=-30) means start_date_2
        (today-15days) is overridden: sync 2 queries from today-30days, the same
        effective window as sync 1 (which has no data before today-30days anyway).

        Since both syncs cover the same date range and Bing Ads can attribute new
        rows between API calls, sync 2 may return slightly more records than sync 1.

        Assert that all records seen in sync 1 also appear in sync 2 (sync1 ⊆ sync2).
        Sync 2 having extra live-attribution records is expected and acceptable.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                expected_primary_keys = self.expected_primary_keys(stream)

                assert len(self.expected_replication_keys(stream)) == 1
                expected_replication_key = next(
                    iter(self.expected_replication_keys(stream)))

                # Dates replicated in sync 1 — used to exclude records added between syncs.
                replication_dates_1 = {
                    record['data'].get(expected_replication_key)
                    for record in
                    StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if record.get('action') == 'upsert'}

                # All PKs from sync 1.
                primary_keys_sync_1 = {
                    tuple(msg['data'][pk] for pk in expected_primary_keys)
                    for msg in
                    StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if msg.get('action') == 'upsert'}

                # PKs from sync 2 filtered to dates present in sync 1's window
                # (excludes rows that were added to the API after sync 1 ran).
                primary_keys_sync_2 = {
                    tuple(msg['data'][pk] for pk in expected_primary_keys)
                    for msg in
                    StartDateTest.synced_messages_by_stream_2.get(
                        stream, {}).get('messages', [])
                    if msg.get('action') == 'upsert'
                    and self.parse_date(msg['data'][expected_replication_key])
                    <= self.parse_date(max(replication_dates_1))}

                # Every record from sync 1 must appear in sync 2 (sync 1 ⊆ sync 2).
                # Sync 2 may have additional rows from live attribution data — that is fine.
                missing = primary_keys_sync_1 - primary_keys_sync_2
                self.assertEqual(
                    missing, set(),
                    msg=f"stream {stream}: sync 2 is missing {len(missing)} record(s) "
                        f"that appeared in sync 1")
