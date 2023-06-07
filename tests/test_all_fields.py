

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base_new_framework import BingAdsBaseTest


class AllFieldsTest(AllFieldsTest,BingAdsBaseTest):
    """ Test the tap all_fields """

    @staticmethod
    def name():
        return "tap_tester_bing_ads_all_fields_test"
    def streams_to_test(self):
        streams_to_exclude={'ad_group_performance_report','campaign_performance_report','goals_and_funnels_report'}
        """
        TODO
        Excluded the ad_group and campaign report streams, has the Exclusion's file doesn't have the latest exclusions,
        to be removed after TDL-23223 is fixed
        Goals stream has no active data
        """
        return self.expected_stream_names().difference(streams_to_exclude)

    def missing_fields(self):
        return {
            'accounts':{
                'TaxCertificate',
                'AccountMode'
                },
            'ads':{
                'Descriptions',
                'LongHeadlineString',
                'BusinessName',
                'Videos',
                'LongHeadlines',
                'Images',
                'LongHeadline',
                'PromotionalText',
                'CallToAction',
                'AppStoreId',
                'Headlines',
                'ImpressionTrackingUrls',
                'CallToActionLanguage',
                'Headline',
                'AppPlatform'
                },
            'campaigns':{
                'MultimediaAdsBidAdjustment',
                'AdScheduleUseSearcherTimeZone',
                'BidStrategyId'
                 },
            'ad_groups':{
                'CpvBid',
                'AdGroupType',#Talend Data Loader TDL-23228 -- data present in fronend but not returned in synced records
                'MultimediaAdsBidAdjustment',
                'AdScheduleUseSearcherTimeZone',
                'CpmBid'
                }
            }
    def test_all_fields_for_streams_are_replicated(self):
        self.selected_fields = {k:v - self.missing_fields().get(k, set())
                                for k,v in AllFieldsTest.selected_fields.items()}
        super().test_all_fields_for_streams_are_replicated()
