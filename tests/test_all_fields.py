from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base_new_framework import BingAdsBaseTest

from tap_tester.base_case import BaseCase as base
from tap_tester.jira_client import JiraClient as jira_client
from tap_tester.jira_client import CONFIGURATION_ENVIRONMENT as jira_config

JIRA_CLIENT = jira_client({**jira_config})


class AllFieldsTest(AllFieldsTest,BingAdsBaseTest):
    """ Test the tap all_fields """

    # Incremental streams can be harder to populate predictably. Keep this set
    # empty by default and add stream names (for example: {'accounts'}) once
    # data setup is stable enough to enforce field-assert coverage.
    EXPECTED_INCREMENTAL_STREAMS_ASSERTED = {'accounts'}

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

    @staticmethod
    def _sorted_fields(values):
        return sorted(values)

    def test_no_unexpected_streams_replicated(self):
        # Some expected core streams can legitimately return no rows depending on
        # account state and date window. Keep this assertion focused on guarding
        # against extra/unselected streams being replicated.
        synced_stream_names = set(self.synced_records.keys())
        unexpected_streams = synced_stream_names.difference(self.test_streams)
        self.assertSetEqual(unexpected_streams, set())

    def test_core_streams_sync_records(self):
        """
        Core streams are required coverage for this test. Each selected core stream
        must replicate at least one record.
        """
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                record_count = self.record_count_by_stream.get(stream, 0)
                self.assertGreater(
                    record_count, 0,
                    msg=f"Core stream '{stream}' returned 0 records. "
                        "Populate data for this stream in the test account before running."
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

                expected_key_set = set(expected_all_keys)
                replicated_key_set = set(self.fields_replicated)
                missing_from_replication = expected_key_set - replicated_key_set
                unexpected_in_replication = replicated_key_set - expected_key_set

                # Ads stream frequently varies by ad subtype. Emit explicit guidance
                # so MISSING_FIELDS updates are driven by observed data, not guesswork.
                replicated_but_marked_missing = set()
                suggested_missing_field_additions = set()
                if stream == 'ads':
                    configured_missing = set(self.MISSING_FIELDS.get('ads', {}))
                    replicated_but_marked_missing = replicated_key_set.intersection(configured_missing)
                    suggested_missing_field_additions = missing_from_replication - configured_missing

                self.assertSetEqual(self.fields_replicated, expected_all_keys,
                                    msg=(
                                        f"Field mismatch for stream '{stream}'. "
                                        f"Missing from replicated ({len(missing_from_replication)}): "
                                        f"{self._sorted_fields(missing_from_replication)}. "
                                        f"Unexpected in replicated ({len(unexpected_in_replication)}): "
                                        f"{self._sorted_fields(unexpected_in_replication)}. "
                                        f"ads recommendations -> remove from MISSING_FIELDS if now present: "
                                        f"{self._sorted_fields(replicated_but_marked_missing)}; "
                                        f"consider adding to MISSING_FIELDS only if consistently absent: "
                                        f"{self._sorted_fields(suggested_missing_field_additions)}"
                                    ),
                                    logging=f"verify all fields are replicated for stream {stream}")
                streams_asserted.append(stream)

        streams_asserted = set(streams_asserted)
        full_table_streams = {
            stream for stream in self.test_streams
            if self.expected_replication_method(stream) == self.FULL_TABLE
        }
        expected_incremental_streams = self.EXPECTED_INCREMENTAL_STREAMS_ASSERTED.intersection(
            self.test_streams.difference(full_table_streams)
        )

        self.assertTrue(
            full_table_streams.issubset(streams_asserted),
            msg=f"Required FULL_TABLE stream field assertions missing. "
                f"Expected: {full_table_streams}, "
                f"asserted: {streams_asserted}"
        )

        self.assertTrue(
            expected_incremental_streams.issubset(streams_asserted),
            msg=f"Expected incremental stream field assertions missing. "
                f"Expected: {expected_incremental_streams}, asserted: {streams_asserted}"
        )

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
            'LongHeadlineString',
            'BusinessName',
            'Videos',
            'LongHeadlines',
            'Images',
            'LongHeadline',
            'PromotionalText',
            'CallToAction',
            'AppStoreId',
            'ImpressionTrackingUrls',
            'CallToActionLanguage',
            'Headline',
            'AppPlatform',
            'DisplayUrl',
            'Title',
            'TitlePart2',
            'TextPart2',
            'DestinationUrl',
            'Text',
            'TitlePart3',
            'TitlePart1',
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
