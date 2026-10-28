from unittest import skip

from base import BingAdsBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


@skip("Temporarily skipping bookmark test due to lack of data")
class BingAdsBookMarkTest(BookmarkTest, BingAdsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            'accounts': {'LastModifiedTime': '2026-04-23T03:58:39.610000+00:00'}
        }
    }

    @staticmethod
    def name():
        return "tap_tester_bing_ads_bookmark_test"

    def streams_to_test(self):
        """Return the set of streams to test."""
        # Exclude streams that have no data in the test account and no reports.
        streams_to_exclude = {
            'ad_extension_detail_report',
            'ad_performance_report',
            'ad_group_performance_report',
            'age_gender_audience_report',
            'audience_performance_report',
            'campaign_performance_report',
            'geographic_performance_report',
            'goals_and_funnels_report',
            'keyword_performance_report',
            'search_query_performance_report'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
