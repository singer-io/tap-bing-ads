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
        return {  # TODO get all these streams covered!!
            'accounts',
            # 'ad_extension_detail_report', # TODO has only one day of data
            # 'ad_group_performance_report', # TODO need to implement modified field selection
            'ad_groups',
            'ad_performance_report',
            'ads',
            # 'age_gender_demographic_report',
            # 'audience_performance_report',
            # 'campaign_performance_report', # TODO need to implement modified field selection
            'campaigns',
            # 'geographic_performance_report',
            # 'goals_and_funnels_report',
            # 'keyword_performance_report',
            # 'search_query_performance_report',
        }

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-03-04T16:13:49.893000+00:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def get_bookmark_key(self, stream):
        stream_to_keys ={
            'ad_extension_detail_report': 'date',
        }

        return stream_to_keys.get(stream)

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

        stream_to_current_state = dict()
        for stream, bookmark in current_state.items():
            key = 'date' # TODO why are these the rep keys??? BUG?
            stream_to_current_state[stream] = bookmark.get(key)

        stream_to_calculated_state = {stream: "" for stream in current_state.keys()}

        for stream, state in stream_to_current_state.items():
            # convert state from string to datetime object
            state_as_datetime = dateutil.parser.parse(state)
            # subtract 1 day from the state # TODO make num days a method?
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=1)
            # convert back to string and format
            calculated_state = str(calculated_state_as_datetime).replace(' ', 'T')
            stream_to_calculated_state[stream] = calculated_state

        return stream_to_calculated_state

    def get_account_ids(self):
        """
        Each report is prefixed with the account_id that is pertains to.
        Return those account_ids
        """
        config_value = self.get_properties().get('account_ids', '')
        account_ids = set(config_value.split(','))

        return account_ids

    def test_run(self):
        """
        Verify for each stream that you can do a sync which records bookmarks.
        Verify that the bookmark is the max value sent to the target for the `date` PK field
        Verify that the 2nd sync respects the bookmark
        Verify that all data of the 2nd sync is >= the bookmark from the first sync
        Verify that the number of records in the 2nd sync is less then the first
        Verify inclusivivity of bookmarks

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """

        self.START_DATE = self.get_properties().get('start_date')

        # Instantiate connection with default start
        conn_id = self.create_connection()

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select all testable streams and no fields within streams
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=True)
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(
        #     conn_id, test_catalogs, select_all_fields=True
        # )

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)


        # UPDATE STATE BETWEEN SYNCS
        new_state = {'bookmarks': dict()}
        max_replication_key_values = self.max_replication_key_values_by_stream(first_sync_records)
        for stream, bookmark in self.calculated_states_by_stream(max_replication_key_values).items():
            key = 'last_record' if stream == 'accounts' else 'date'  # TODO why are these the rep keys??? BUG?
            new_state['bookmarks'][stream] = {key: bookmark}
        # TODO account for report bookmarks for other account prefixed streams besides Stitch
        import pdb; pdb.set_trace()
        menagerie.set_state(conn_id, new_state)

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        # Test by stream
        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                if not self.is_report(stream):
                    continue  # SKIPPING NON-REPORT STREAMS AS THEY ARE COVERED BY test_bookmarks.py
                
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
                first_bookmark_key_value = first_sync_bookmarks.get('bookmarks').get(prefixed_stream)
                second_bookmark_key_value = second_sync_bookmarks.get('bookmarks').get(prefixed_stream)

                if replication_method == self.INCREMENTAL:
                    replication_key = self.expected_replication_keys().get(stream).pop()
                    bookmark_key = 'last_record' if stream == 'accounts' else 'date'# TODO | will have to adjust for reports

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)

                    # bookmarked states (actual values)
                    first_bookmark_value = first_bookmark_key_value.get(bookmark_key)
                    second_bookmark_value = second_bookmark_key_value.get(bookmark_key)
                    # bookmarked values as utc for comparing against records
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    # Verify the second sync records respect the previous (simulated) bookmark value
                    simulated_bookmark_value = new_state['bookmarks'][prefixed_stream][bookmark_key]
                    for message in second_sync_messages:
                        replication_key_value = message.get('data').get(replication_key)
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg="Second sync records do not repect the previous bookmark.")

                    # Verify the first sync bookmark value is the max replication key value for a given stream
                    for message in first_sync_messages:
                        replication_key_value = message.get('data').get(replication_key)
                        self.assertLessEqual(replication_key_value, first_bookmark_value_utc,
                                             msg="First sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                    # Verify the second sync bookmark value is the max replication key value for a given stream
                    for message in second_sync_messages:
                        replication_key_value = message.get('data').get(replication_key)
                        self.assertLessEqual(replication_key_value, second_bookmark_value_utc,
                                             msg="Second sync bookmark was set incorrectly, a record with a greater rep key value was synced")

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)

                    # Verify at least 1 record was replicated in the second sync
                    self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))

                elif replication_method == self.FULL_TABLE:
                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNone(first_bookmark_key_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNone(second_bookmark_key_value)

                else:
                    raise NotImplementedError("invalid replication method: {}".format(replication_method))
