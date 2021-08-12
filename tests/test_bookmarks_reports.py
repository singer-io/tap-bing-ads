import datetime
import dateutil.parser
import pytz
import singer

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import BingAdsBaseTest

LOGGER = singer.get_logger()


class TestBingAdsBookmarksReports(BingAdsBaseTest):

    @staticmethod
    def name():
        return "tap_tester_bing_ads_bookmarking_reports"

    def expected_sync_streams(self):
        return {
            'accounts',
            'ad_extension_detail_report',
            'ad_group_performance_report',
            'ad_groups',
            'ad_performance_report',
            'ads',
            'age_gender_audience_report',
            'audience_performance_report',
            'campaign_performance_report',
            'campaigns',
            'geographic_performance_report',
            # 'goals_and_funnels_report',  # Unable to generate data for this stream
            'keyword_performance_report',
            'search_query_performance_report',
        }
    def get_delta_from_target_date(self, stream):
        stream_to_target_dates =  {
            'ad_extension_detail_report': "2020-11-18T00:00:00+00:00",
            'ad_group_performance_report': "2020-11-20T00:00:00+00:00",
            'ad_performance_report': "2020-11-20T00:00:00+00:00",
            'age_gender_audience_report': "2020-11-20T00:00:00+00:00",
            'audience_performance_report': "2020-11-18T00:00:00+00:00",
            'campaign_performance_report': "2020-11-20T00:00:00+00:00",
            'geographic_performance_report': "2020-11-20T00:00:00+00:00",
            'keyword_performance_report': "2020-11-18T00:00:00+00:00",
            'search_query_performance_report': "2020-11-18T00:00:00+00:00",
        }
        today = datetime.datetime.utcnow().date()

        _, stream_without_prefix = stream.split('_', 1)
        target_date_unformatted = dateutil.parser.parse(stream_to_target_dates[stream_without_prefix])
        target_date = target_date_unformatted.replace(tzinfo=None).date()
        conversion_window_delta = datetime.timedelta(days=self.DEFAULT_CONVERSION_WINDOW)

        bookmark_date = target_date - conversion_window_delta
        delta = today - bookmark_date

        return delta

    def stream_to_days_with_data(self):
        return {
            'ad_extension_detail_report': {
                "2020-11-18T00:00:00+00:00"
            },
            'ad_group_performance_report': {
                "2020-11-17T00:00:00+00:00"
                "2020-11-18T00:00:00+00:00"
                "2020-11-19T00:00:00+00:00"
                "2020-11-20T00:00:00+00:00"
                "2020-11-21T00:00:00+00:00"
                "2020-11-22T00:00:00+00:00"
            },
            'ad_performance_report': {
                "2020-11-17T00:00:00+00:00"
                "2020-11-18T00:00:00+00:00"
                "2020-11-19T00:00:00+00:00"
                "2020-11-20T00:00:00+00:00"
                "2020-11-21T00:00:00+00:00"
                "2020-11-22T00:00:00+00:00"
            },
            'age_gender_audience_report': {
                "2020-11-17T00:00:00+00:00"
                "2020-11-18T00:00:00+00:00"
                "2020-11-19T00:00:00+00:00"
                "2020-11-20T00:00:00+00:00"
                "2020-11-21T00:00:00+00:00"
                "2020-11-22T00:00:00+00:00"
            },
            'audience_performance_report': {
                "2020-11-18T00:00:00+00:00"
            },
            'campaign_performance_report': {
                "2020-11-17T00:00:00+00:00"
                "2020-11-18T00:00:00+00:00"
                "2020-11-19T00:00:00+00:00"
                "2020-11-20T00:00:00+00:00"
                "2020-11-21T00:00:00+00:00"
                "2020-11-22T00:00:00+00:00"
            },
            'geographic_performance_report': {
                "2020-11-17T00:00:00+00:00"
                "2020-11-18T00:00:00+00:00"
                "2020-11-19T00:00:00+00:00"
                "2020-11-20T00:00:00+00:00"
                "2020-11-21T00:00:00+00:00"
                "2020-11-22T00:00:00+00:00"
            },
            'keyword_performance_report': {
                "2020-11-18T00:00:00+00:00"
            },
            'search_query_performance_report': {
                "2020-11-18T00:00:00+00:00"
            },
        }

    def get_bookmark_key(self, stream):
        if self.is_report(stream):
            key = 'date'
        elif stream == 'accounts':  # BUG (https://stitchdata.atlassian.net/browse/SRCE-4609)
            key = 'last_record'
        else:
            raise NotImplementedError("{} is not accounted for in test.".format(stream))

        return key

    def get_account_ids(self):
        """
        Each report is prefixed with the account_id that is pertains to.
        Return those account_ids
        """
        config_value = self.get_properties().get('account_ids', '')
        account_ids = set(config_value.split(','))

        return account_ids

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-03-04T16:13:49.893000+00:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state):
        """
        Look at the bookmarks from a previous sync and set a new bookmark
        value that is 1 day prior. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.

        Formatting of state
           {'bookmarks': {
               '71086605_ad_extension_detail_report': {'date': '2020-12-08T00:00:00+00:00',
                                                       'request_id': None},
               'accounts': {'last_record': '2020-11-23T03:58:39.610000+00:00'}
               }
            }
        """
        stream_to_calculated_bookmark_value = {stream: "" for stream in current_state['bookmarks'].keys()}

        for stream, bookmark in current_state['bookmarks'].items():
            bookmark_value = bookmark.get(self.get_bookmark_key(stream))

            if not self.is_report(stream):  # non-report streams do not change
                stream_to_calculated_bookmark_value[stream] = bookmark_value
                continue  # skipping concersion

            # convert state value from string to datetime object
            value_as_datetime = dateutil.parser.parse(bookmark_value)

            # subtract the timedelta to get to the target date
            delta = self.get_delta_from_target_date(stream)
            calculated_value_as_datetime = value_as_datetime - delta

            # convert back to string and format
            calculated_bookmark_value = str(calculated_value_as_datetime).replace(' ', 'T')
            stream_to_calculated_bookmark_value[stream] = calculated_bookmark_value

        return stream_to_calculated_bookmark_value

    def test_run(self):
        """
        Test is parametrized to account for the exclusions in some report streams.
        By default we select all fields for a given stream for this test, however due to the
        exclusions (see base.py for groups)  we will be running the test multiple times.

        The first test run selects all fields for standard streams, and as many fields as allowed
        including the Impression Share Performance Statistics for streams with exclusions.

        The second test run selects all fields for standard streams, and as many fields as allowed
        including Attributes for streams with exclusions.

        Both runs account for uncategorized exclusion fields. See method in base.py.
        """

        # Test report bookmarks selecting all statistics fields for streams with exclusions
        streams_to_fields_with_statistics = dict()
        for stream in self.expected_streams_with_exclusions():
            streams_to_fields_with_statistics[stream] = self.get_as_many_fields_as_possbible_excluding_attributes(stream)

        self.bookmark_reports_test(streams_to_fields_with_statistics)

        # TODO UNCOMMENT
        # # Test report bookmarks selecting all attribute fields for streams with exclusions
        # streams_to_fields_with_attributes = dict()
        # for stream in self.expected_streams_with_exclusions():
        #     streams_to_fields_with_attributes[stream] = self.get_as_many_fields_as_possbible_excluding_statistics(stream)

        # self.bookmark_reports_test(streams_to_fields_with_attributes)

    def bookmark_reports_test(self, streams_to_fields_with_exclusions):
        """
        Verify for each stream that you can do a sync which records bookmarks.
        Verify that the bookmark is set to the day on which the sync is ran
        Verify that all data of the 2nd sync is >= the bookmark from the first sync - conversion_window
        Verify that the number of records in the 2nd sync is less then the first

        PREREQUISITE
        For EACH report stream there are multiple days in which a report can be generated.
        """

        self.START_DATE = self.get_properties().get('start_date')

        # Instantiate connection with default start
        conn_id = self.create_connection()

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # ensure our expectations are consistent for streams with exclusions
        self.assertSetEqual(self.expected_streams_with_exclusions(), set(self.get_all_attributes().keys()))
        self.assertSetEqual(self.expected_streams_with_exclusions(), set(self.get_all_statistics().keys()))

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in self.expected_sync_streams()
                                    and catalog.get('tap_stream_id') not in self.expected_streams_with_exclusions()]
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs, select_all_fields=True)
        self.select_all_streams_and_fields(conn_id, test_catalogs_all_fields, select_all_fields=True)
        found_catalogs = menagerie.get_catalogs(conn_id)
        test_catalogs_specific_fields = [catalog for catalog in found_catalogs
                                         if catalog.get('tap_stream_id') in self.expected_sync_streams()
                                         and catalog.get('tap_stream_id') in self.expected_streams_with_exclusions()]
        self.perform_and_verify_adjusted_selection(conn_id, test_catalogs_specific_fields,
                                                   select_all_fields=False, specific_fields=streams_to_fields_with_exclusions)

        # Run a sync job using orchestrator
        state = menagerie.get_state(conn_id)
        first_sync_record_count = self.run_and_verify_sync(conn_id, state)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        # UPDATE STATE BETWEEN SYNCS
        new_state = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, bookmark in simulated_states.items():
            new_state['bookmarks'][stream] = {self.get_bookmark_key(stream): bookmark}
        menagerie.set_state(conn_id, new_state)

        # Run a second sync job using orchestrator
        state = menagerie.get_state(conn_id)
        second_sync_record_count = self.run_and_verify_sync(conn_id, state)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        stream_to_days = self.stream_to_days_with_data()

        # Test by stream
        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                if not self.is_report(stream):
                    continue  # SKIPPING NON-REPORT STREAMS AS THEY ARE COVERED BY test_bookmarks.py

                days_with_data = len(stream_to_days.get(stream, []))

                # reports use an account_id prefix for bookmarking, use only the id
                # associated with the Stitch Account
                account_to_test = self.get_account_id_with_report_data()
                prefix = account_to_test + '_' if self.is_report(stream) else ''
                prefixed_stream = prefix + stream # prefixed_stream = streams for standard streams

                replication_method = self.expected_replication_method().get(stream)

                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                # bookmarked states (top level objects)
                first_sync_bookmark_key_value = first_sync_bookmarks.get('bookmarks').get(prefixed_stream)
                second_sync_bookmark_key_value = second_sync_bookmarks.get('bookmarks').get(prefixed_stream)

                if replication_method == self.INCREMENTAL:
                    replication_key = self.expected_replication_keys().get(stream).pop()
                    bookmark_key = self.get_bookmark_key(stream)

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_sync_bookmark_key_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_sync_bookmark_key_value)

                    # bookmarked states (actual values)
                    first_sync_bookmark_value = first_sync_bookmark_key_value.get(bookmark_key)
                    second_sync_bookmark_value = second_sync_bookmark_key_value.get(bookmark_key)
                    # bookmarked values as utc for comparing against records
                    first_sync_bookmark_value_utc = self.convert_state_to_utc(first_sync_bookmark_value)
                    second_sync_bookmark_value_utc = self.convert_state_to_utc(second_sync_bookmark_value)

                    today_utc = datetime.datetime.strftime(datetime.datetime.utcnow(), self.START_DATE_FORMAT)

                    # Verify that the first sync bookmark is set to the day on which the sync is ran
                    self.assertEqual(first_sync_bookmark_value_utc, today_utc)

                    # Verify that the second sync bookmark is set to the day on which the sync is ran
                    self.assertEqual(second_sync_bookmark_value_utc, today_utc)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_sync_bookmark_value, first_sync_bookmark_value) # assumes no data changes during test

                    # Verify the second sync records respect the previous (simulated) bookmark value and conversion window
                    simulated_bookmark_value = new_state['bookmarks'][prefixed_stream][bookmark_key]
                    conversion_window = self.DEFAULT_CONVERSION_WINDOW  # days ago
                    bookmark_minus_lookback = self.timedelta_formatted(simulated_bookmark_value, days=conversion_window)
                    for message in second_sync_messages:
                        replication_key_value = message.get('data').get(replication_key)
                        self.assertGreaterEqual(replication_key_value, bookmark_minus_lookback,
                                                msg="Second sync records do not repect the previous bookmark.")

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)


                    if days_with_data > 1:  # Reports with multiple days of data can be fully tested

                        # Verify at least 1 record was replicated in the second sync
                        self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))


                else:
                    raise NotImplementedError("invalid replication method: {}".format(replication_method))
