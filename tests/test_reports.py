from datetime import datetime as dt
from datetime import timedelta

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import BingAdsBaseTest


class MySqlStartDateTest(BingAdsBaseTest):

    # TODO 
    #   There are exclusion rules that prevent us from selecting all fields for certain streams.
    #   There are measurement fields that must be selected for some reports to work. These are not
    #   accounted for as automatic fields (see BUG SRCE-4313). 

    @staticmethod
    def name():
        return "tap_tester_bing_ads_start_date_test"

    def expected_sync_streams(self):
        return {  # TODO get all these streams covered!!
            'accounts',
            # 'ad_extension_detail_report', #  FAILED in sync (missing pk)
            # 'ad_group_performance_report', # FAILED missing measurement column
            'ad_groups',
            # 'ad_performance_report', # FAILED missing measurement column
            'ads',
            # 'age_gender_demographic_report', # FAILED no records available 
            # 'audience_performance_report', # FAILED missing measurement column
            # 'campaign_performance_report', # FAILED missing measurement column
            'campaigns',
            # 'geographic_performance_report', # FAILED missing measurement column
            # 'goals_and_funnels_report', # FAILED missing measurement column
            # 'keyword_performance_report', # FAILED missing measurement column
            # 'search_query_performance_report', # FAILED missing measurement column
        }

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        # self.start_date = self.get_properties().get('start_date')
        self.start_date = '2019-12-31T00:00:00Z',
        # self.start_date_2 = self.timedelta_formatted(self.start_date, days=1)

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        # normally we'd want all fields but here this fails since there are exclusion rules not accounted for in our test
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=False)
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(
        #     conn_id, test_catalogs, select_all_fields=False
        # )

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)
                record_count = first_sync_record_count.get(stream, 0)

                if stream.endswith('_report'):
                    # Verify that all reports replicate at least 1 record
                    self.assertGreater(record_count, 0, msg="No records were replicated.")

                    # # Verify replication key is greater or equal to start_date
                    # replication_dates =[row.get('data').get(replication_key)
                    #                     for row in first_sync_records.get(stream, []).get('messages', [])]
                    # for date in replication_dates:
                    #     self.assertGreaterEqual(
                    #         self.parse_date(date), self.parse_date(self.start_date),
                    #         msg="Record was created prior to start date for this sync.\n" +
                    #         "Sync start_date: {}\n".format(self.start_date) +
                    #         "Record date: {} ".format(date)
                    #     )


                else:
                    # Ensure we are accounting for both report and parent (non-report) streams
                    non_report_streams = {'accounts', 'ad_groups', 'ads', 'campaigns'}
                    self.assertIn(stream, non_report_streams, msg="A non-report stream is unaccounted for.")
