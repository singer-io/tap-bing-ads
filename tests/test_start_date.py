from datetime import datetime as dt
from datetime import timedelta

import tap_tester.connections as connections
import tap_tester.runner      as runner

from base import BingAdsBaseTest


class MySqlStartDateTest(BingAdsBaseTest):
    start_date = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_start_date_test"

    def expected_sync_streams(self):
        return {  # TODO get all these streams covered!!
            'accounts',
            # 'ad_extension_detail_report',
            # 'ad_group_performance_report',
            'ad_groups',
            # 'ad_performance_report',
            'ads',
            # 'age_gender_demographic_report',
            # 'audience_performance_report',
            # 'campaign_performance_report',
            'campaigns',
            # 'geographic_performance_report',
            # 'goals_and_funnels_report',
            # 'keyword_performance_report', # TODO errors on pk during sync
            # 'search_query_performance_report', # TODO errors on pk during sync
        }

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        self.start_date = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date, days=1)

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=True)
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(
        #     conn_id, test_catalogs, select_all_fields=True
        # )

        # run initial sync
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)

        replicated_row_count_1 = sum(first_record_count_by_stream.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(first_record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count_1))
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=True)
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(
        #     conn_id, test_catalogs, select_all_fields=True
        # )

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id)

        replicated_row_count_2 = sum(record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        print("total replicated row count: {}".format(replicated_row_count_2))
        synced_records_2 = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)

                record_count_1 = first_record_count_by_stream.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                # Verify that the 2nd sync resutls in less records than the 1st sync.
                self.assertLessEqual(
                    record_count_2, record_count_1,
                    msg="\nStream '{}' is {}\n".format(stream, self.FULL_TABLE) +
                    "Second sync should result in fewer records\n" +
                    "Sync 1 start_date: {} ".format(self.start_date) +
                    "Sync 1 record_count: {}\n".format(record_count_1) +
                    "Sync 2 start_date: {} ".format(self.start_date_2) +
                    "Sync 2 record_count: {}".format(record_count_2))

                if replication_type == self.INCREMENTAL:
                    replication_key = next(iter(self.expected_replication_keys().get(stream)))

                    # Verify replication key is greater or equal to start_date
                    replication_dates_1 =[row.get('data').get(replication_key)
                                for row in synced_records_1.get(stream, []).get('messages', [])]
                    replication_dates_2 =[row.get('data').get(replication_key)
                                for row in synced_records_2.get(stream, []).get('messages', [])]
                    for replication_dates in [replication_dates_1, replication_dates_2]:
                        for date in replication_dates:
                            self.assertGreaterEqual(
                                self.parse_date(date), self.parse_date(self.start_date),
                                msg="Record was created prior to start date for this sync.\n" +
                                "Sync start_date: {}\n".format(self.start_date) +
                                "Record date: {} ".format(date)
                            )

                    # Verify 1st sync record count > 2nd sync record count for incremental streams
                    self.assertGreaterEqual(record_count_1, record_count_2,
                                            msg="Expected less records on 2nd sync.")

                elif replication_type != self.FULL_TABLE:
                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type))
