"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie

from base import BingAdsBaseTest

import base


class MinimumSelectionTest(BingAdsBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_minimum_fields_test"

    def expected_sync_streams(self):
        # BUG_SRCE-4313 To reproduce grep jira id across tests dir and comment/uncomment
        #               corresponding lines. You will need to comment out all _report streams prior
        #               to running test as well. You will probably get an OperationError during the
        #               sync, that's because we aren't selecting any fields prior to the sync, this
        #               means only automatic fields will be replicated. Except we aren't marking the
        #               necessary fields as automatic. Hence this bug.
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
            # 'goals_and_funnels_report',  # Unable to generate data for this stream, workaround in test
            # 'keyword_performance_report',
            # 'search_query_performance_report',
        }

    def report_measure_fields(self):
        minimum_measure_fields = {'Clicks'}
        stream_to_fields = {
            stream: minimum_measure_fields
            for stream in self.expected_sync_streams() if stream.endswith('_report')
        }
        stream_to_fields['goals_and_funnels_report'] = {'Assists'}

        return stream_to_fields

    def report_automatic_fields(self):
        """
        Most reports inherit the minimum required fields and they do not need to be specified to run a sync.
        Others have several measures (fields) that MUST be selected to generate a valid report.
        All reports must have at least 1 performance statistic such as Clicks, Conversions, etc. selected.

        These fields can be foound by looking at the Requirements specs withing the value-sets section of docs:
            https://docs.microsoft.com/en-us/advertising/reporting-service/reporting-value-sets?view=bingads-13

        TODO Replace this method with the above commented method once BUG_SRCE-4313 is addressed
        """
        minimum_measure_fields = {'Clicks'}
        report_required_fields = self.expected_required_fields()
        stream_to_fields = {
            stream: minimum_measure_fields | report_required_fields.get(stream)
            for stream in self.expected_sync_streams() if stream.endswith('_report')
        }

        # TODO uncomment below two lines when TDL-24648 is resolved
        # stream_to_fields['goals_and_funnels_report'].remove('Clicks')
        # stream_to_fields['goals_and_funnels_report'].update({'Assists'})

        return stream_to_fields

    def parent_automatic_fields(self):
        """
        TODO This is only needed because of BUG_SRCE-4313 once that is addressed and metadata is
        updated this can be removed.
        """
        auto_fields = self.expected_automatic_fields()
        stream_to_fields = {
            stream: auto_fields.get(stream)
            for stream in self.expected_sync_streams() if not stream.endswith('_report')
        }

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

        TDL_24648_is_done = base.JIRA_CLIENT.get_status_category("TDL-24648") == "done"
        assert TDL_24648_is_done == False, ("TDL-24648 is done, Re-add report streams to "
                                            "expected_sync_streams and report_automatic_fields")

        conn_id = self.create_connection()

        # Select all parent streams and no fields within streams
        # Select all (testable) report streams and only fields which are automatic and/or required
        #   by bing to genereate a report
        found_catalogs = menagerie.get_catalogs(conn_id)
        test_catalogs = [catalog for catalog in found_catalogs
                       if catalog.get('tap_stream_id') in self.expected_sync_streams()]

        # BUG_SRCE-4313 (https://stitchdata.atlassian.net/browse/SRCE-4313)
        #   streams missing automatic fields
        # COMMENT out line below to reproduce
        specific_fields = {**self.report_automatic_fields(), **self.parent_automatic_fields()}
        # UNCOMMENT 2 lines below to reproduce
        # specific_fields = {**self.report_measure_fields(), **self.parent_automatic_fields()}
        # specific_fields = self.report_measure_fields()  # TODO Use this line once bugs addressed.

        self.perform_and_verify_adjusted_selection(
            conn_id, test_catalogs, select_all_fields=False, specific_fields=specific_fields
        )

        # COMMENT EVERYTHING DOWN FROM HERE TO ADDRESS BUG_SRCE-4313

        # Run a sync job using orchestrator
        state = menagerie.get_state(conn_id)
        record_count_by_stream = self.run_and_verify_sync(conn_id, state)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                if stream == 'goals_and_funnels_report':  # SKIP TESTING FOR THIS STREAM
                    # no data available, would need to implement a tracking script on singer's site
                    continue

                # verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # verify that only the automatic fields are sent to the target for parent streams,
                # and that automatic fields, _sdc_report_datetime, AND specific measure fields are
                # sent to target for report streams
                actual = actual_fields_by_stream.get(stream) or set()
                expected = self.expected_automatic_fields().get(stream, set())
                if stream.endswith('_report'):  # update expectations for report streams
                    expected_measure = 'Assists' if stream.startswith('goals') else 'Clicks'
                    expected.update({
                        '_sdc_report_datetime',  # tap applies sdc value as pk for all reports
                        # reports require a perf measure (which is intentionally not automatic)
                        expected_measure
                    })

                self.assertSetEqual(expected, actual)
