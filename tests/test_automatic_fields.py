"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie

from base import BingAdsBaseTest


class MinimumSelectionTest(BingAdsBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_minimum_fields_test"

    def expected_sync_streams(self):
        return {
            'accounts',
            'ad_extension_detail_report',
            # 'ad_group_performance_report',
            'ad_groups',
            # 'ad_performance_report',
            'ads',
            # 'age_gender_audience_report',
            # 'audience_performance_report',
            # 'campaign_performance_report',
            'campaigns',
            # 'geographic_performance_report',
            # 'goals_and_funnels_report',  # BUG_1 https://stitchdata.atlassian.net/browse/SRCE-4549
            # 'keyword_performance_report',
            'search_query_performance_report',
        }

    def report_measure_fields(self):
        """
        Most reports inherit the minimum required fields and they do not need to be specified to run a sync.
        Others have several measures (fields) that MUST be selected to generate a valid report.

        These fields can be foound by looking at the Requirements specs withing the value-sets section of docs:
            https://docs.microsoft.com/en-us/advertising/reporting-service/reporting-value-sets?view=bingads-13

        TODO THIS METHOD IS REDUNDANT BUT SOURCE OF TRUTH IS UNCLEAR AND IT IS BEST TO BE EXPLICIT.
             Move to expected_metatdata in base.py once bugs are addressed. 
        """
        minimum_measure_fields = set()

        stream_to_fields = {
            stream: minimum_measure_fields
            for stream in self.expected_sync_streams()
        }

        # stream_to_fields['ad_group_performance_report'] = {'TimePeriod'}

        # stream_to_fields['goals_and_funnels_report'] = {'Goal', 'TimePeriod'}  # BUG_1

        stream_to_fields['ad_extension_detail_report'] = {'AdExtensionId', 'AdExtensionPropertyValue',
                                                          'AdExtensionType', 'AdExtensionTypeId', 'TimePeriod'}

        # stream_to_fields['age_gender_audience_report'] = {'AccountName', 'AdGroupName', 'AgeGroup', 'Gender', 'TimePeriod'}

        return stream_to_fields

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        self.start_date = '2020-11-10T00:00:00Z'
        conn_id = self.create_connection(original_properties=False)

        # Select all parent streams and no fields within streams
        # Select all (testable) report streams and only fields which are automatic and/or required by bing to genereate a report
        found_catalogs = menagerie.get_catalogs(conn_id)
        test_catalogs = [catalog for catalog in found_catalogs
                        if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.perform_and_verify_reports_selection(
            conn_id, test_catalogs, select_all_fields=False, report_measure_fields=self.report_measure_fields()
        )

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                # verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # BUG_2 | (TODO write this up)
                # if stream.endswith('_report'):
                #     continue  # SKIP REPORT STREAMS B/C BUG_2

                # verify that only the automatic fields are sent to the target
                actual = actual_fields_by_stream.get(stream) or set()
                expected = self.expected_automatic_fields().get(stream, set())
                self.assertEqual(
                    actual, expected,
                    msg=("The fields sent to the target are not the automatic fields. Expected: {}, Actual: {}"
                         .format(actual, expected))
                )
