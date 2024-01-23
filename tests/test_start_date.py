from datetime import datetime as dt
from datetime import timedelta

from tap_tester import connections, menagerie, runner, LOGGER
from base import BingAdsBaseTest

import base


class BingAdsStartDateTest(BingAdsBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_start_date_test"

    def expected_sync_streams(self):
        return {
            'accounts',
            # 'ad_extension_detail_report',
            # 'ad_group_performance_report',
            'ad_groups',
            # 'ad_performance_report',
            'ads',
            # 'age_gender_audience_report',
            # 'audience_performance_report',
            # 'campaign_performance_report',
            'campaigns',
            # 'geographic_performance_report',
            # 'goals_and_funnels_report',  # cannot test no data available
            # 'keyword_performance_report',
            # 'search_query_performance_report',
        }

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

        TDL_24648_is_done = base.JIRA_CLIENT.get_status_category("TDL-24648") == "done"
        assert TDL_24648_is_done == False, ("TDL-24648 is done, Re-add report streams to "
                                            "expected_sync_streams")

        # Test start date selecting all fields for standard streams, and all statistic fields for
        #   streams with exclusions
        streams_to_fields_with_statistics = dict()
        for stream in self.expected_streams_with_exclusions():
            streams_to_fields_with_statistics[stream] = \
                self.get_as_many_fields_as_possbible_excluding_attributes(stream)

        self.start_date_test(streams_to_fields_with_statistics)

        # Test start date selecting all fields for standard streams and all attribute fields for
        #   streams with exclusions
        streams_to_fields_with_attributes = dict()
        for stream in self.expected_streams_with_exclusions():
            streams_to_fields_with_attributes[stream] = \
                self.get_as_many_fields_as_possbible_excluding_statistics(stream)

        self.start_date_test(streams_to_fields_with_attributes)

    def start_date_test(self, streams_to_fields_with_exclusions):
        """Instantiate start date according to the desired data set and run the test"""

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=1)

        self.start_date = self.start_date_1

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = self.create_connection()

        # run check mode was run when the connection was created, just get the catalog
        found_catalogs_1 = menagerie.get_catalogs(conn_id_1)

        # ensure our expectations are consistent for streams with exclusions
        self.assertSetEqual(self.expected_streams_with_exclusions(),
                            set(self.get_all_attributes().keys()))
        self.assertSetEqual(self.expected_streams_with_exclusions(),
                            set(self.get_all_statistics().keys()))

        # table and field selection
        test_catalogs_1_all_fields = [
            catalog for catalog in found_catalogs_1
            if catalog.get('tap_stream_id') in self.expected_sync_streams()
            and catalog.get('tap_stream_id') not in self.expected_streams_with_exclusions()]

        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_and_field_selection(conn_id_1,
        #                                             test_catalogs_1_all_fields,
        #                                             select_all_fields=True)
        self.select_all_streams_and_fields(conn_id_1,
                                           test_catalogs_1_all_fields,
                                           select_all_fields=True) # BUG_SRCE-4304

        test_catalogs_1_specific_fields = [
            catalog for catalog in found_catalogs_1
            if catalog.get('tap_stream_id') in self.expected_sync_streams()
            and catalog.get('tap_stream_id') in self.expected_streams_with_exclusions()]

        self.perform_and_verify_adjusted_selection(
            conn_id_1,
            test_catalogs_1_specific_fields,
            select_all_fields=False,
            specific_fields=streams_to_fields_with_exclusions)

        # run initial sync
        state = menagerie.get_state(conn_id_1)
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1, state)

        replicated_row_count_1 = sum(record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0,
                           msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        LOGGER.info("total replicated row count: %s", replicated_row_count_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info("REPLICATION START DATE CHANGE: %s ===>>> %s ", self.start_date,
                    self.start_date_2)
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = self.create_connection()

        # run check mode
        found_catalogs_2 = menagerie.get_catalogs(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [
            catalog for catalog in found_catalogs_2
            if catalog.get('tap_stream_id') in self.expected_sync_streams()
            and catalog.get('tap_stream_id') not in self.expected_streams_with_exclusions()]
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_and_field_selection(conn_id_2,
        #                                             test_catalogs_2_all_fields,
        #                                             select_all_fields=True)
        self.select_all_streams_and_fields(conn_id_2,
                                           test_catalogs_2_all_fields,
                                           select_all_fields=True) # BUG_SRCE-4304
        test_catalogs_2_specific_fields = [
            catalog for catalog in found_catalogs_2
            if catalog.get('tap_stream_id') in self.expected_sync_streams()
            and catalog.get('tap_stream_id') in self.expected_streams_with_exclusions()]
        self.perform_and_verify_adjusted_selection(
            conn_id_2,
            test_catalogs_2_specific_fields,
            select_all_fields=False,
            specific_fields=streams_to_fields_with_exclusions)

        # run sync
        state = menagerie.get_state(conn_id_2)
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2, state)

        replicated_row_count_2 = sum(record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        LOGGER.info("total replicated row count: %s", replicated_row_count_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)

                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                if replication_type == self.INCREMENTAL:
                    replication_key = next(iter(self.expected_replication_keys().get(stream)))

                    if self.is_report(stream):
                        # Verify replication key is greater or equal to start_date for sync 1
                        replication_dates_1 =[
                            row.get('data').get(replication_key)
                            for row in synced_records_1.get(stream, []).get('messages', [])]

                        for replication_date in replication_dates_1:
                            self.assertGreaterEqual(
                                self.parse_date(replication_date),
                                self.parse_date(self.start_date_1),
                                msg="Report pertains to a date prior to our start date.\n" +
                                    "Sync start_date: {}\n".format(self.start_date_1) +
                                    "Record date: {} ".format(replication_date)
                            )

                        # Verify replication key is greater or equal to start_date for sync 2
                        replication_dates_2 =[
                            row.get('data').get(replication_key)
                            for row in synced_records_2.get(stream, []).get('messages', [])]
                        for replication_date in replication_dates_2:
                            self.assertGreaterEqual(
                                self.parse_date(replication_date),
                                self.parse_date(self.start_date_2),
                                msg="Report pertains to a date prior to our start date.\n" +
                                    "Sync start_date: {}\n".format(self.start_date_2) +
                                    "Record date: {} ".format(replication_date)
                            )

                    elif stream == 'accounts':

                        # Verify that the 2nd sync with a later start date replicates the same
                        # number of records as the 1st sync.
                        self.assertEqual(
                            record_count_2, record_count_1,
                            msg="Second sync should result in fewer records\n" +
                            "Sync 1 start_date: {} ".format(self.start_date) +
                            "Sync 1 record_count: {}\n".format(record_count_1) +
                            "Sync 2 start_date: {} ".format(self.start_date_2) +
                            "Sync 2 record_count: {}".format(record_count_2))

                    else:
                        raise NotImplementedError("Stream is not report-based and incremental. "
                                                  "Must add assertion for it.")

                elif replication_type == self.FULL_TABLE:

                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(
                        record_count_2, record_count_1,
                        msg="Second sync should result in fewer records\n" +
                        "Sync 1 start_date: {} ".format(self.start_date) +
                        "Sync 1 record_count: {}\n".format(record_count_1) +
                        "Sync 2 start_date: {} ".format(self.start_date_2) +
                        "Sync 2 record_count: {}".format(record_count_2))

                else:

                    raise Exception("Expectations are set incorrectly. {} cannot have a "
                                    "replication method of {}".format(stream, replication_type)
                    )
