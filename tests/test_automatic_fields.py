from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest
from base import BingAdsBaseTest


class BingAdsAutomaticFields(MinimumSelectionTest, BingAdsBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_automatic_fields_test"

    def streams_to_test(self):
        """ Streams to test for automatic_fields test."""
        # excluding due to lack of test data
        streams_to_exclude = {
            'ad_extension_detail_report',
            'audience_performance_report',
            'goals_and_funnels_report',
            'keyword_performance_report',
            'search_query_performance_report',
            'geographic_performance_report',
            'ad_group_performance_report',
            'ad_performance_report',
            'campaign_performance_report',
            'age_gender_audience_report'

        }
        return self.expected_stream_names().difference(streams_to_exclude)
