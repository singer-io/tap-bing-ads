from datetime import datetime as dt
from datetime import timedelta

import tap_tester.connections as connections
import tap_tester.runner      as runner
import tap_tester.menagerie   as menagerie


from base import BingAdsBaseTest


class BingAdsStartDateTest(BingAdsBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_bing_ads_start_date_test"

    def expected_sync_streams(self):
        return {  # TODO get all these streams covered!!
            'accounts',
            'ad_extension_detail_report',
            # 'ad_group_performance_report',  # TODO account for exclusions
            'ad_groups',
            'ad_performance_report',
            'ads',
            'age_gender_audience_report',
            'audience_performance_report',
            # 'campaign_performance_report',  # TODO account for exclusions
            'campaigns',
            'geographic_performance_report',
            # 'goals_and_funnels_report',  # cannot test no data available
            'keyword_performance_report',
            'search_query_performance_report',
        }

    def get_as_many_fields_as_possbible_accounting_for__exclusions(self):
        """
        TODO gotta figure out what is best,
         - should we just do all the stats fields? Don't we need one of those attributes thoguh to generate a report? 
         - should we do syncs, one with all attributes and one with stats? 

                                YES DO THIS 

                                HERE IS WHY 

              - I feel like this is the best thing to do.
               TODO need to look at if there are fields that do not fall into attributes or statistics.

               290             # TODO need to look at if there are fields that do not fall into attributes or statistics.
               291             # # DELETE_ME  #################
               292             test_streams = {'campaign_performance_report', 'ad_group_performance_report'}
               293  ->         for test_stream in test_streams:
               294                 all_fields = self.get_all_fields().get(test_stream)
               295                 all_stats = self.get_all_statistics().get(test_stream)
               296                 all_attrs = self.get_all_attributes().get(test_stream)
               297                 import pdb; pdb.set_trace()
               298             # ##############################
               (Pdb) len(all_stats) + len(all_attrs), len(all_fields)
               (18, 81)
               (Pdb) len(all_stats)
               13
        """
        return {
            'campaign_performance_report': {},
            'ad_group_performance_report': {},
        }

    def get_all_attributes(self):
        """A dictionary of reports to Attributes"""
        return {
            'campaign_performance_report': {
                'BidMatchType', 'BudgetAssociationStatus', 'BudgetName', 'BudgetStatus',
                'DeviceOS', 'Goal', 'GoalType', 'TopVsOther'
            },
            'ad_group_performance_report': {
                'BidMatchType','DeviceOS','Goal','GoalType','TopVsOther'
            },
        }

    def get_all_statistics(self):
        """A dictionary of reports to ImpressionSharePerformanceStatistics"""
        return {
            'campaign_performance_report': {
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'RelativeCtr',
                'TopImpressionRatePercent',
                'TopImpressionShareLostToBudgetPercent',
                'TopImpressionShareLostToRankPercent',
                'TopImpressionSharePercent'
            },
            'ad_group_performance_report': {
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'RelativeCtr'},
        }

    def get_all_fields(self):
        return {
            'campaign_performance_report': {
                'BudgetName',
                'TopImpressionRatePercent',
                'HistoricalQualityScore',
                'ImpressionLostToBudgetPercent',
                'LowQualityClicksPercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AdDistribution',
                'Assists',
                'Ctr',
                'HistoricalAdRelevance',
                'TopVsOther',
                'ExactMatchImpressionSharePercent',
                'CustomerName',
                'Goal',
                'QualityScore',
                'CurrencyCode',
                'CostPerAssist',
                'BidMatchType',
                'RevenuePerConversion',
                'DeviceType',
                'BaseCampaignId',
                'AllRevenuePerConversion',
                'CampaignStatus',
                'AccountNumber',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'Spend',
                'PhoneCalls',
                'ConversionRate',
                'BudgetStatus',
                'RelativeCtr',
                'LowQualityGeneralClicks',
                'AudienceImpressionLostToBudgetPercent',
                'ImpressionLostToRankAggPercent',
                'TopImpressionShareLostToRankPercent',
                'LowQualityConversionRate',
                'CustomerId',
                'AccountId',
                'AudienceImpressionSharePercent',
                'AbsoluteTopImpressionRatePercent',
                'HistoricalLandingPageExperience',
                'AllReturnOnAdSpend',
                'ReturnOnAdSpend',
                'GoalType',
                'CampaignName',
                'LowQualityImpressionsPercent',
                'Ptr',
                'DeliveredMatchType',
                'AllConversions',
                'ClickSharePercent',
                'TopImpressionShareLostToBudgetPercent',
                'BudgetAssociationStatus',
                'LandingPageExperience',
                'CustomParameters',
                'Conversions',
                'ImpressionSharePercent',
                'PhoneImpressions',
                'AdRelevance',
                'AllRevenue',
                'TrackingTemplate',
                'Revenue',
                'CostPerConversion',
                'AveragePosition',
                'Clicks',
                'LowQualitySophisticatedClicks',
                'TimePeriod',
                'AllConversionRate',
                'CampaignLabels',
                'Impressions',
                'FinalUrlSuffix',
                'LowQualityConversions',
                'LowQualityClicks',
                'RevenuePerAssist',
                'HistoricalExpectedCtr',
                'AccountStatus',
                'Network',
                'ExpectedCtr',
                'DeviceOS',
                'CampaignType',
                'LowQualityImpressions',
                'TopImpressionSharePercent',
                'AbsoluteTopImpressionSharePercent',
                'ViewThroughConversions',
                'AudienceImpressionLostToRankPercent',
                'AverageCpc',
                'AccountName',
                'CampaignId',
                'AllCostPerConversion'
            },
            'ad_group_performance_report': {
                'HistoricalExpectedCtr',
                'DeliveredMatchType',
                'AdGroupId',
                'AccountId',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'CampaignType',
                'AdGroupType',
                'Goal',
                'FinalUrlSuffix',
                'QualityScore',
                'AudienceImpressionSharePercent',
                'CostPerConversion',
                'AllConversionRate',
                'ConversionRate',
                'DeviceType',
                'Language',
                'AdRelevance',
                'DeviceOS',
                'ClickSharePercent',
                'CustomerId',
                'Assists',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AdGroupLabels',
                'Spend',
                'PhoneImpressions',
                'AllRevenue',
                'AdGroupName',
                'CurrencyCode',
                'ExpectedCtr',
                'TimePeriod',
                'AccountNumber',
                'Revenue',
                'AdDistribution',
                'AudienceImpressionLostToRankPercent',
                'BidMatchType',
                'ReturnOnAdSpend',
                'TopImpressionShareLostToRankPercent',
                'PhoneCalls',
                'CustomParameters',
                'ViewThroughConversions',
                'CampaignName',
                'ImpressionLostToRankAggPercent',
                'CampaignStatus',
                'Status',
                'RevenuePerAssist',
                'BaseCampaignId',
                'ImpressionLostToBudgetPercent',
                'Impressions',
                'RevenuePerConversion',
                'ExactMatchImpressionSharePercent',
                'Conversions',
                'LandingPageExperience',
                'TopVsOther',
                'ImpressionSharePercent',
                'Ctr',
                'TrackingTemplate',
                'TopImpressionShareLostToBudgetPercent',
                'CostPerAssist',
                'GoalType',
                'AllReturnOnAdSpend',
                'HistoricalQualityScore',
                'Clicks',
                'AllConversions',
                'AllCostPerConversion',
                'Network',
                'HistoricalLandingPageExperience',
                'RelativeCtr',
                'TopImpressionRatePercent',
                'HistoricalAdRelevance',
                'AveragePosition',
                'AccountName',
                'AccountStatus',
                'CustomerName',
                'Ptr',
                'AudienceImpressionLostToBudgetPercent',
                'AverageCpc',
                'TopImpressionSharePercent',
                'AbsoluteTopImpressionSharePercent',
                'AllRevenuePerConversion',
                'CampaignId',
                'AbsoluteTopImpressionRatePercent'
            }
        }

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, self.START_DATE_FORMAT)
        except ValueError:
            return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=1)

        self.start_date = self.start_date_1

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = self.create_connection()

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1 = [catalog for catalog in found_catalogs_1
                           if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id_1, test_catalogs_1, select_all_fields=True)
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(
        #     conn_id_1, test_catalogs_1, select_all_fields=True
        # )

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)

        replicated_row_count_1 = sum(record_count_by_stream_1.values())
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = self.create_connection(original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2 = [catalog for catalog in found_catalogs_2
                           if catalog.get('tap_stream_id') in self.expected_sync_streams()]
        self.select_all_streams_and_fields(conn_id_2, test_catalogs_2, select_all_fields=True)
        # BUG (https://stitchdata.atlassian.net/browse/SRCE-4304)
        # self.perform_and_verify_table_and_field_selection(
        #     conn_id_2, test_catalogs_2, select_all_fields=True
        # )

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)

        replicated_row_count_2 = sum(record_count_by_stream_2.values())
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data")
        print("total replicated row count: {}".format(replicated_row_count_2))
        synced_records_2 = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):
                replication_type = self.expected_replication_method().get(stream)

                record_count_1 = record_count_by_stream_1.get(stream, 0)
                record_count_2 = record_count_by_stream_2.get(stream, 0)

                if replication_type == self.INCREMENTAL:
                    replication_key = next(iter(self.expected_replication_keys().get(stream)))

                    if self.is_report(stream):
                        # Verify replication key is greater or equal to start_date for sync 1
                        replication_dates_1 =[row.get('data').get(replication_key)
                                              for row in synced_records_1.get(stream, []).get('messages', [])]

                        for replication_date in replication_dates_1:
                            self.assertGreaterEqual(
                                self.parse_date(replication_date), self.parse_date(self.start_date_1),
                                    msg="Report pertains to a date prior to our start date.\n" +
                                    "Sync start_date: {}\n".format(self.start_date_1) +
                                    "Record date: {} ".format(replication_date)
                            )

                        # Verify replication key is greater or equal to start_date for sync 2
                        replication_dates_2 =[row.get('data').get(replication_key)
                                              for row in synced_records_2.get(stream, []).get('messages', [])]
                        for replication_date in replication_dates_2:
                            self.assertGreaterEqual(
                                self.parse_date(replication_date), self.parse_date(self.start_date_2),
                                    msg="Report pertains to a date prior to our start date.\n" +
                                    "Sync start_date: {}\n".format(self.start_date_2) +
                                    "Record date: {} ".format(replication_date)
                            )

                    elif stream == 'accounts':

                        # Verify that the 2nd sync with a later start date replicates the same number of
                        # records as the 1st sync.
                        self.assertEqual(
                            record_count_2, record_count_1,
                            msg="Second sync should result in fewer records\n" +
                            "Sync 1 start_date: {} ".format(self.start_date) +
                            "Sync 1 record_count: {}\n".format(record_count_1) +
                            "Sync 2 start_date: {} ".format(self.start_date_2) +
                            "Sync 2 record_count: {}".format(record_count_2))

                    else:
                        raise NotImplementedError("Stream is not report-based and incremental. Must add assertion for it.")

                elif replication_type == self.FULL_TABLE:

                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(
                        record_count_2, record_count_1,
                        msg="Second sync should result in fewer records\n" +
                        "Sync 1 start_date: {} ".format(self.start_date) +
                        "Sync 1 record_count: {}\n".format(record_count_1) +
                        "Sync 2 start_date: {} ".format(self.start_date_2) +
                        "Sync 2 record_count: {}".format(record_count_2))

                else:

                    raise Exception(
                        "Expectations are set incorrectly. {} cannot have a replication method of {}".format(
                            stream, replication_type
                        )
                    )
