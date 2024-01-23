from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base_new_framework import BingAdsBaseTest

from tap_tester.base_case import BaseCase as base
from tap_tester.jira_client import JiraClient as jira_client
from tap_tester.jira_client import CONFIGURATION_ENVIRONMENT as jira_config

JIRA_CLIENT = jira_client({**jira_config})


class AllFieldsTest(AllFieldsTest,BingAdsBaseTest):
    """ Test the tap all_fields """

    start_date = '2021-01-01T00:00:00Z'

    @staticmethod
    def name():
        return "tap_tester_bing_ads_all_fields_test"

    # update all tests in repo when JIRA cards are complete
    TDL_23223_is_done = JIRA_CLIENT.get_status_category("TDL-23223") == "done"
    assert TDL_23223_is_done == False, "TDL-23223 is done, Re-add streams with fixed exclusions"
    TDL_24648_is_done = JIRA_CLIENT.get_status_category("TDL-24648") == "done"
    assert TDL_24648_is_done == False, "TDL-24648 is done, Re-add streams that have data"

    def streams_to_test(self):
        # TODO Excluded ad_group and campaign report streams due to errors exclusions file errors
        #   current file doesn't appear to have the latest exclusions, to be removed after TDL-23223
        #   is fixed. Data has aged out of the 3 year retention window for other report streams.
        #   Work with marketing / dev to see if new data can be generated.
        streams_to_exclude = {'ad_extension_detail_report',
                              'ad_performance_report',
                              'ad_group_performance_report', # TDL-23223
                              'age_gender_audience_report',
                              'audience_performance_report',
                              'campaign_performance_report', # TDL-23223
                              'geographic_performance_report',
                              'goals_and_funnels_report',
                              'keyword_performance_report',
                              'search_query_performance_report'}

        return self.expected_stream_names().difference(streams_to_exclude)

    MISSING_FIELDS = {
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
            'AdGroupType',  # TDL-23228 -- data present in fronend but not returned in synced records
            'MultimediaAdsBidAdjustment',
            'AdScheduleUseSearcherTimeZone',
            'CpmBid'
        }
    }
