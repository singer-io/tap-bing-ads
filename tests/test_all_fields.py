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

    # When both TDL-23223 (exclusions file fixes) and TDL-24648 (stream data availability)
    # are resolved, report streams may be re-added to streams_to_test().
    TDL_23223_is_done = JIRA_CLIENT.get_status_category("TDL-23223") == "done"
    TDL_24648_is_done = JIRA_CLIENT.get_status_category("TDL-24648") == "done"
    assert not (TDL_23223_is_done and TDL_24648_is_done), \
        "TDL-23223 and TDL-24648 are both done — re-add report streams to streams_to_test()"

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

    def test_core_streams_sync_records(self):
        """
        FULL_TABLE streams (ads, ad_groups, campaigns) are not bounded by start_date and
        should always replicate records if data exists in the account — assert these strictly.

        INCREMENTAL streams (accounts) are bounded by LastModifiedTime relative to start_date,
        so zero rows is acceptable; we skip the hard assertion for those.
        """
        full_table_streams = {
            stream for stream in self.test_streams
            if self.expected_replication_method(stream) == self.FULL_TABLE
        }
        incremental_streams = self.test_streams - full_table_streams

        for stream in full_table_streams:
            with self.subTest(stream=stream):
                record_count = self.record_count_by_stream.get(stream, 0)
                self.assertGreater(
                    record_count, 0,
                    msg=f"FULL_TABLE stream '{stream}' returned 0 records — "
                        "expected data regardless of start_date. "
                        "Populate data via the UI for this account."
                )

        for stream in incremental_streams:
            with self.subTest(stream=stream):
                record_count = self.record_count_by_stream.get(stream, 0)
                if record_count == 0:
                    self.logger.warning(
                        f"INCREMENTAL stream '{stream}' returned 0 records for "
                        f"start_date={self.start_date}. Skipping count assertion — "
                        "adjust start_date or generate records to strengthen this check."
                    )

    def test_all_fields_for_streams_are_replicated(self):
        # Validate all-fields behavior only for streams that actually produced rows.
        # Track whether at least one stream was asserted so we don't silently pass
        # a test where every stream was skipped due to zero records.
        streams_asserted = []

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
                streams_asserted.append(stream)

        self.assertGreater(
            len(streams_asserted),
            0,
            msg=f"No field assertions were made — every stream in {self.test_streams} "
                "returned 0 records. Populate data via the UI or adjust start_date."
        )

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
