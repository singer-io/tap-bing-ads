"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie

from base import BingAdsBaseTest


class MinimumSelectionTest(BingAdsBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_no_fields_test"

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
            # 'goals_and_funnels_report',
            # 'keyword_performance_report',
            # 'search_query_performance_report',
        }

    def expected_pks(self):
        primary_keys = self.expected_primary_keys()
        return {
            stream: primary_keys.get(stream)
            for stream in self.expected_sync_streams()
        }

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        self.start_date = '2020-01-01T00:00:00Z'
        conn_id = self.create_connection(original_properties=False)

        # Select all streams and no fields within streams
        # IF THERE ARE NO AUTOMATIC FIELDS FOR A STREAM
        # WE WILL NEED TO UPDATE THE BELOW TO SELECT ONE
        found_catalogs = menagerie.get_catalogs(conn_id)
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('tap_stream_id') in self.expected_sync_streams()]

        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection( # UNCOMMENT HERE
        #     conn_id, test_catalogs, select_all_fields=False
        # )
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=False) # REMOVE ME when BUG fixed

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                # verify that you get some records for each stream
                # SKIP THIS ASSERTION FOR STREAMS WHERE YOU CANNOT GET
                # MORE THAN 1 PAGE OF DATA IN THE TEST ACCOUNT
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # verify that only the automatic fields are sent to the target
                actual = actual_fields_by_stream.get(stream) or set()
                expected = self.expected_automatic_fields().get(stream, set())
                self.assertEqual(
                    actual, expected,
                    msg=("The fields sent to the target are not the automatic fields. Expected: {}, Actual: {}"
                         .format(actual, expected))
                )
