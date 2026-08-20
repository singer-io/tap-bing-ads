from tap_tester import connections, menagerie, runner
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import BingAdsBaseTest


class BingAdsAllFields(AllFieldsTest, BingAdsBaseTest):
    """ Test the tap all_fields """

    MISSING_FIELDS = {
        'ads':{
            'Headline',
            'Text',
            'LongHeadline',
            'LongHeadlineString',
            'BusinessName',
            'CallToAction',
            'CallToActionLanguage',
            'Images'
        }
    }

    @staticmethod
    def name():
        return "tap_tester_bing_ads_all_fields_test"

    def streams_to_test(self):
        """ Streams to test for all_fields test."""
        # Exclude streams that have no data in the test account.
        streams_to_exclude = {
            'ad_extension_detail_report',
            'audience_performance_report',
            'goals_and_funnels_report',
            'keyword_performance_report',
            'search_query_performance_report'
        }

        return self.expected_stream_names().difference(streams_to_exclude)

    # Union of all ImpressionSharePerformanceStatistics fields from the Bing Ads exclusion rules
    # for campaign_performance_report and ad_group_performance_report.
    #
    # These fields are mutually exclusive with Attribute columns (BudgetName, BudgetStatus,
    # BudgetAssociationStatus, BidMatchType, TopVsOther, DeviceOS, Goal, GoalType, CustomerId,
    # CustomerName, DeliveredMatchType).  Selecting both groups triggers
    # BingAdsInvalidFieldSelection.  We keep the Attributes and drop all ImpressionShare
    # statistics so the maximum valid field set is selected.
    #
    # Source: tap_bing_ads/exclusions.py — CampaignPerformanceReport / AdGroupPerformanceReport
    _IMPRESSION_SHARE_FIELDS = {
        'AbsoluteTopImpressionRatePercent',
        'AbsoluteTopImpressionShareLostToBudgetPercent',
        'AbsoluteTopImpressionShareLostToRankPercent',
        'AbsoluteTopImpressionSharePercent',
        'AudienceImpressionLostToBudgetPercent',
        'AudienceImpressionLostToRankPercent',
        'AudienceImpressionSharePercent',
        'ClickSharePercent',
        'ExactMatchImpressionSharePercent',
        'ImpressionLostToAdRelevancePercent',
        'ImpressionLostToBidPercent',
        'ImpressionLostToBudgetPercent',
        'ImpressionLostToExpectedCtrPercent',
        'ImpressionLostToRankAggPercent',
        'ImpressionLostToRankPercent',
        'ImpressionSharePercent',
        'RelativeCtr',
        'TopImpressionRatePercent',
        'TopImpressionShareLostToBudgetPercent',
        'TopImpressionShareLostToRankPercent',
        'TopImpressionSharePercent',
    }
    _EXCLUSION_STREAMS = {'campaign_performance_report', 'ad_group_performance_report'}

    def setUp(self):
        """
        Override setUp to build streams_to_selected_fields dynamically from the live catalog.

        The framework's default behaviour (streams_to_selected_fields = {}) selects ALL fields
        for ALL streams.  For campaign_performance_report and ad_group_performance_report the
        Bing Ads API enforces a mutual-exclusion constraint that makes a full-field sync fail.
        We build a complete per-stream "fields to keep" dict here so that:
          - every other stream still gets all its fields selected, and
          - the two exclusion streams get everything except the four conflicting fields.
        """
        _cache = BingAdsAllFields

        if all([_cache.synced_records, _cache.record_count_by_stream,
                _cache.selected_fields, _cache.actual_fields]):
            return

        _cache.conn_id = conn_id = connections.ensure_connection(self)
        _cache.found_catalogs = found_catalogs = self.run_and_verify_check_mode(conn_id)
        _cache.test_streams = self.streams_to_test()

        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in _cache.test_streams]

        # Build a complete streams_to_selected_fields dict from the live catalog metadata.
        # "fields to keep" = all non-unsupported schema fields, minus the exclusions for the
        # two report streams that have a mutual-exclusion constraint.
        streams_to_selected = {}
        for catalog in test_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            all_props = {
                item['breadcrumb'][-1] for item in schema['metadata']
                if item['breadcrumb'] != []
                and item['metadata'].get('inclusion') != 'unsupported'
            }
            stream = catalog['stream_name']
            if stream in self._EXCLUSION_STREAMS:
                streams_to_selected[stream] = all_props - self._IMPRESSION_SHARE_FIELDS
            else:
                streams_to_selected[stream] = all_props

        # select_streams_and_fields computes non_selected = all_props - streams_to_selected[stream]
        # so passing all_props for a stream means nothing is deselected (all fields kept).
        self.select_streams_and_fields(conn_id, test_catalogs, streams_to_selected)
        _cache.selected_fields = streams_to_selected

        _cache.record_count_by_stream = self.run_and_verify_sync_mode(conn_id)
        _cache.synced_records = runner.get_records_from_target_output()
        _cache.actual_fields = runner.examine_target_output_for_fields()
