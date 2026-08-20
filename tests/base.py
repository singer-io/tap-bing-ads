import os
from datetime import timedelta
from datetime import datetime as dt

from tap_tester.base_suite_tests.base_case import BaseCase


class BingAdsBaseTest(BaseCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """
    REQUIRED_KEYS = "required_keys"
    PARENT_TAP_STREAM_ID = "parent-tap-stream-id"

    # respect tap-bing-ads data retention window by looking back a maximum of about 3 years
    start_date = dt.strftime(dt.now() - timedelta(days=365*3), "%Y-%m-%dT00:00:00Z")

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-bing-ads"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.bing-ads"

    def get_properties(self):
        """Configuration properties required for the tap."""
        # Disable the conversion-window lookback so that the second sync starts
        # strictly after the manipulated bookmark, making record-count assertions valid.
        return_value = {
            'start_date': self.start_date,
            # 'conversion_window': '-15',  # advanced option
        }

        return return_value

    def get_bookmark_value(self, state, stream):
        """
        Bing Ads uses a flat bookmark structure:
            - {account_ids}_{stream} → bookmarks → date
        Return the bookmark date for the stream across the configured account.
        """
        bookmarks = state.get('bookmarks', {})
        account_id = self.get_credentials().get('account_ids').split(',')[0]

        if stream.endswith('_report'):
            value = bookmarks.get(f'{account_id}_{stream}', {}).get('date', None)
            return value if value is not None else None

        return super().get_bookmark_value(state, stream)

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {
            "oauth_client_id": os.getenv('TAP_BING_ADS_OAUTH_CLIENT_ID'),
            "oauth_client_secret": os.getenv('TAP_BING_ADS_OAUTH_CLIENT_SECRET'),
            "refresh_token": os.getenv('TAP_BING_ADS_REFRESH_TOKEN'),
            "developer_token": os.getenv('TAP_BING_ADS_DEVELOPER_TOKEN'),
            "customer_id": os.getenv('TAP_BING_ADS_CUSTOMER_ID'),
            "account_ids": os.getenv('TAP_BING_ADS_ACCOUNT_IDS'),
        }

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams"""
        default_report = {
            cls.PRIMARY_KEYS: set(),
            cls.REPLICATION_METHOD: cls.INCREMENTAL,
            cls.REPLICATION_KEYS: {"TimePeriod"},
            cls.FOREIGN_KEYS: {"AccountId"},
            cls.PARENT_TAP_STREAM_ID: "accounts",
            cls.LOOK_BACK_WINDOW: timedelta(days=30),
        }
        return {
            "accounts": {
                cls.PRIMARY_KEYS: { "Id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "LastModifiedTime" },
                cls.OBEYS_START_DATE: False
            },
            "campaigns": {
                cls.PRIMARY_KEYS: { "Id", "account_id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "accounts_LastModifiedTime" },
                cls.OBEYS_START_DATE: False,
                cls.PARENT_TAP_STREAM_ID: "accounts"
            },
            "ad_groups": {
                cls.PRIMARY_KEYS: { "Id", "account_id", "campaign_id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "campaigns_LastModifiedTime" },
                cls.OBEYS_START_DATE: False,
                cls.PARENT_TAP_STREAM_ID: "campaigns"
            },
            "ads": {
                cls.PRIMARY_KEYS: { "Id", "account_id", "campaign_id", "ad_group_id" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "ad_groups_LastModifiedTime" },
                cls.OBEYS_START_DATE: False,
                cls.PARENT_TAP_STREAM_ID: "ad_groups"
            },
            "ad_extension_detail_report": {
                cls.REQUIRED_KEYS: {
                    "AdExtensionId",
                    "AdExtensionPropertyValue",
                    "AdExtensionType",
                    "AdExtensionTypeId",
                    "Clicks",
                    "Revenue",
                    "Ctr",
                    "Spend",
                    "AllConversions",
                    "ReturnOnAdSpend",
                    "Impressions",
                    "ConversionRate",
                    "Conversions",
                    "AverageCpm",
                    "Assists",
                    "AverageCpc",
                    "AllRevenue",
                    "TimePeriod"
                },
                **default_report
            },
            "ad_group_performance_report": {
                cls.REQUIRED_KEYS:{
                    "Clicks",
                    "Revenue",
                    "Ctr",
                    "PhoneCalls",
                    "Spend",
                    "AllConversions",
                    "ReturnOnAdSpend",
                    "Impressions",
                    "ViewThroughConversions",
                    "ConversionRate",
                    "Conversions",
                    "AverageCpm",
                    "Assists",
                    "AverageCpc",
                    "AllRevenue",
                    "TimePeriod"
                },
                **default_report
            },
            "ad_performance_report": {
                cls.REQUIRED_KEYS: {
                    "Revenue",
                    "Clicks",
                    "Ctr",
                    "Impressions",
                    "ReturnOnAdSpend",
                    "AverageCpm",
                    "Assists",
                    "ConversionRate",
                    "AverageCpc",
                    "Spend",
                    "AllRevenue",
                    "AllConversions",
                    "ViewThroughConversions",
                    "Conversions",
                    "TimePeriod"
                },
                **default_report
            },
            "age_gender_audience_report": {
                cls.REQUIRED_KEYS: {
                    "AccountName",
                    "AdGroupName",
                    "AgeGroup",
                    "Gender",
                    "Revenue",
                    "Clicks",
                    "Impressions",
                    "Assists",
                    "Spend",
                    "AllRevenue",
                    "AllConversions",
                    "ViewThroughConversions",
                    "Conversions",
                    "TimePeriod"
                },
                **default_report
            },
            "audience_performance_report": {
                cls.REQUIRED_KEYS: {
                    "AudienceId",
                    "Clicks",
                    "Revenue",
                    "Ctr",
                    "Spend",
                    "AllConversions",
                    "ReturnOnAdSpend",
                    "Impressions",
                    "ViewThroughConversions",
                    "AllRevenue",
                    "Conversions",
                    "AverageCpm",
                    "ConversionRate",
                    "AverageCpc",
                    "TimePeriod"
                },
                **default_report
            },
            "campaign_performance_report": {
                cls.REQUIRED_KEYS: {
                    "Clicks",
                    "Revenue",
                    "Ctr",
                    "PhoneCalls",
                    "Spend",
                    "AllConversions",
                    "ReturnOnAdSpend",
                    "Impressions",
                    "LowQualityClicks",
                    "ViewThroughConversions",
                    "ConversionRate",
                    "Conversions",
                    "AverageCpm",
                    "Assists",
                    "AverageCpc",
                    "AllRevenue",
                    "TimePeriod"
                },
                **default_report
            },
            "geographic_performance_report": {
                cls.REQUIRED_KEYS: {
                    "AccountName",
                    "Revenue",
                    "Clicks",
                    "Ctr",
                    "Impressions",
                    "ReturnOnAdSpend",
                    "AverageCpm",
                    "Assists",
                    "ConversionRate",
                    "AverageCpc",
                    "Spend",
                    "AllRevenue",
                    "AllConversions",
                    "ViewThroughConversions",
                    "Conversions",
                    "TimePeriod"
                },
                **default_report
            },
            "goals_and_funnels_report": {
                cls.REQUIRED_KEYS: {
                    "Goal",
                    "AllConversions",
                    "ViewThroughConversions",
                    "Assists",
                    "AllRevenue",
                    "TimePeriod"
                },
                **default_report
            },
            "keyword_performance_report": {
                cls.REQUIRED_KEYS: {
                    "Clicks",
                    "Revenue",
                    "Ctr",
                    "Spend",
                    "AllConversions",
                    "ReturnOnAdSpend",
                    "Impressions",
                    "ViewThroughConversions",
                    "ConversionRate",
                    "Conversions",
                    "AverageCpm",
                    "Assists",
                    "AverageCpc",
                    "AllRevenue",
                    "TimePeriod"
                },
                **default_report
            },
            "search_query_performance_report": {
                cls.REQUIRED_KEYS: {
                    "SearchQuery",
                    "Clicks",
                    "Revenue",
                    "Ctr",
                    "Spend",
                    "AllConversions",
                    "ReturnOnAdSpend",
                    "Impressions",
                    "ConversionRate",
                    "Conversions",
                    "AverageCpm",
                    "Assists",
                    "AverageCpc",
                    "AllRevenue",
                    "TimePeriod"
                },
                **default_report
            }
        }


    @classmethod
    def setUpClass(cls):
        super().setUpClass(logging="Ensuring environment variables are sourced.")
        missing_envs = [
            x for x in ['TAP_BING_ADS_OAUTH_CLIENT_ID', 'TAP_BING_ADS_OAUTH_CLIENT_SECRET',
                        'TAP_BING_ADS_REFRESH_TOKEN', 'TAP_BING_ADS_DEVELOPER_TOKEN',
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def expected_automatic_fields(self, stream=None):
        """
        Return a dictionary with key of table name and value as a set of automatic fields
        """
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.FOREIGN_KEYS, set()) | v.get(self.REQUIRED_KEYS, set()) | \
                {'_sdc_report_datetime'}
        for streams in auto_fields.keys():
            if streams in ['ads', 'ad_groups', 'campaigns', 'accounts']:
                auto_fields[streams] = auto_fields[streams] - {'_sdc_report_datetime'}
        if not stream:
            return auto_fields
        return auto_fields[stream]

    def expected_parent_tap_stream(self, stream=None):
        """return a dictionary with key of table name and value of parent stream"""
        parent_stream = {
            table: properties.get(self.PARENT_TAP_STREAM_ID, None)
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return parent_stream
        return parent_stream[stream]

    def expected_replication_method(self,stream=None):
        """ Return a dictionary with key of table name nd value of replication method
            TDL-15816 - Currently, in tap, all streams are FULL_TABLE except accounts. But as per
            the doc https://www.stitchdata.com/docs/integrations/saas/microsoft-advertising,
            only the below streams are FULL TABLE, all other streams are INCREMENTAL.
            ads
            ad_groups
            campaigns
            """

        rep_method = {}
        for table, properties in self.expected_metadata().items():
            rep_method[table] = properties.get(self.REPLICATION_METHOD, None)
        for streams in rep_method.keys():
            if streams in [ 'ad_extension_detail_report', 'ad_group_performance_report',
                            'ad_performance_report', 'age_gender_audience_report',
                            'audience_performance_report', 'campaign_performance_report',
                            'geographic_performance_report', 'goals_and_funnels_report',
                            'keyword_performance_report', 'search_query_performance_report']:
                rep_method[streams] = 'FULL_TABLE'
        if not stream:
            return rep_method
        return rep_method[stream]

    def expected_replication_keys(self,stream=None):
        """
        Return a dictionary with key of table name and value as a set of replication key fields
        """

        # As all streams are FULL TABLE according to the tap, there is no replication key specified
        # for any of the streams. TDL-15816, hence removing the "TimePeriod" key from expected
        # replication keys. Need to determine the correct replication menthod and replication keys
        # accordingly.

        replication_keys = {table: properties.get(self.REPLICATION_KEYS, set()) - {"TimePeriod"}
                            for table, properties
                            in self.expected_metadata().items()}
        if not stream:
            return replication_keys
        return replication_keys[stream]
