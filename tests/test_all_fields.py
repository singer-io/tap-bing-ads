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

    # # update all tests in repo when JIRA cards are complete
    # TDL_23223_is_done = JIRA_CLIENT.get_status_category("TDL-23223") == "done"
    # assert TDL_23223_is_done == False, "TDL-23223 is done, Re-add streams with fixed exclusions"
    # TDL_24648_is_done = JIRA_CLIENT.get_status_category("TDL-24648") == "done"
    # assert TDL_24648_is_done == False, "TDL-24648 is done, Re-add streams that have data"

    def streams_to_test(self):
        # Keep this test pinned to core object streams only.
        # Report streams are volatile (availability, retention, exclusions), and
        # stream inventory can change over time, which makes exclusion lists brittle.
        core_streams = {'accounts', 'ad_groups', 'ads', 'campaigns'}

        return self.expected_stream_names().intersection(core_streams)

    def test_no_unexpected_streams_replicated(self):
        # Some expected core streams can legitimately return no rows depending on
        # account state and date window. Keep this assertion focused on guarding
        # against extra/unselected streams being replicated.
        synced_stream_names = set(self.synced_records.keys())
        unexpected_streams = synced_stream_names.difference(self.test_streams)
        self.assertSetEqual(unexpected_streams, set())

    def test_all_streams_sync_records(self):
        # Core object streams may legitimately sync zero rows for a given account
        # and date window. Require at least one selected stream to have data.
        record_count_by_stream = {
            stream: self.record_count_by_stream.get(stream, 0)
            for stream in self.test_streams
        }
        non_empty_streams = {
            stream for stream, count in record_count_by_stream.items() if count > 0
        }
        self.assertGreater(
            len(non_empty_streams),
            0,
            msg=f"No selected streams synced rows. Counts: {record_count_by_stream}"
        )

    def test_all_fields_for_streams_are_replicated(self):
        # Validate all-fields behavior only for streams that actually produced rows.
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                if self.record_count_by_stream.get(stream, 0) <= 0:
                    continue

                expected_all_keys = self.selected_fields.get(stream, set()) \
                    - set(self.MISSING_FIELDS.get(stream, {})) \
                    - set(self.KEYS_WITH_NO_DATA.get(stream, {})) \
                    | set(self.EXTRA_FIELDS.get(stream, {}))

                fields_replicated = self.actual_fields.get(stream, set())
                self.fields_replicated = fields_replicated
                self.remove_bad_keys(stream)

                self.assertSetEqual(self.fields_replicated, expected_all_keys,
                                    logging=f"verify all fields are replicated for stream {stream}")

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
            'AppPlatform',
            'Path1',
            'EditorialStatus',
            'DevicePreference',
            'AdFormatPreference',
            'Domain',
            'Type',
            'Status',
            'DisplayUrl',
            'UrlCustomParameters',
            'FinalUrlSuffix',
            'TrackingUrlTemplate',
            'Title',
            'TitlePart2',
            'TextPart2',
            'FinalMobileUrls',
            'FinalUrls',
            'Id',
            'ForwardCompatibilityMap',
            'DestinationUrl',
            'Text',
            'FinalAppUrls',
            'TitlePart3',
            'TitlePart1',
            'Path2'
        },
        'campaigns':{
            'MultimediaAdsBidAdjustment',
            'AdScheduleUseSearcherTimeZone',
            'BidStrategyId',
            'Settings',
            'BudgetId',
            'ForwardCompatibilityMap',
            'Name',
            'Id',
            'TimeZone',
            'UrlCustomParameters',
            'BiddingScheme',
            'BudgetType',
            'FinalUrlSuffix',
            'AudienceAdsBidAdjustment',
            'Status',
            'DailyBudget',
            'ExperimentId',
            'Languages',
            'CampaignType',
            'SubType',
            'TrackingUrlTemplate'
        },
        'ad_groups':{
            'CpvBid',
            'AdGroupType',  # TDL-23228 -- data present in fronend but not returned in synced records
            'MultimediaAdsBidAdjustment',
            'AdScheduleUseSearcherTimeZone',
            'CpmBid',
            'Settings',
            'StartDate',
            'ForwardCompatibilityMap',
            'CpcBid',
            'EndDate',
            'Language',
            'AdRotation',
            'Name',
            'Id',
            'UrlCustomParameters',
            'BiddingScheme',
            'Network',
            'PrivacyStatus',
            'FinalUrlSuffix',
            'AudienceAdsBidAdjustment',
            'Status',
            'TrackingUrlTemplate'
        }
    }
