import backoff
import copy
import os
from datetime import timedelta
from datetime import datetime as dt
from datetime import timezone as tz

#from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase

def backoff_wait_times():
    """Create a generator of wait times as [30, 60, 120, 240, 480, ...]"""
    return backoff.expo(factor=30)


class RetryableTapError(Exception):
    def __init__(self, message):
        super().__init__(message)


class BingAdsBaseTest(BaseCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """
    REQUIRED_KEYS = "required_keys"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-bing-ads"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.bing-ads"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': '2020-10-01T00:00:00Z',
            'customer_id': '163875182',
            'account_ids': '163078754,140168565,71086605',
            # 'conversion_window': '-15',  # advanced option
        }
        # cid=42183085 aid=71086605  uid=71069166 (RJMetrics)
        # cid=42183085 aid=163078754 uid=71069166 (Stitch)
        # cid=42183085 aid=140168565 uid=71069166 (TestAccount)

        if original:
            return return_value

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > return_value["start_date"]

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {
            "oauth_client_id": os.getenv('TAP_BING_ADS_OAUTH_CLIENT_ID'),
            "oauth_client_secret": os.getenv('TAP_BING_ADS_OAUTH_CLIENT_SECRET'),
            "refresh_token": os.getenv('TAP_BING_ADS_REFRESH_TOKEN'),
            "developer_token": os.getenv('TAP_BING_ADS_DEVELOPER_TOKEN'),
        }

    @staticmethod
    def expected_metadata():
        """The expected streams and metadata about the streams"""

        default = {
            BaseCase.PRIMARY_KEYS: {"Id"},
            BaseCase.REPLICATION_METHOD: BaseCase.FULL_TABLE,
        }
        default_report = {
            BaseCase.PRIMARY_KEYS: set(), # "_sdc_report_datetime" is added by tap
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            BaseCase.REPLICATION_KEYS: {"TimePeriod"}, # It used in sync but not mentioned in catalog. Bug: TDL-15816
            BaseCase.FOREIGN_KEYS: {"AccountId"}
        }
        accounts_meta = {
            BaseCase.PRIMARY_KEYS: {"Id"},
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            BaseCase.REPLICATION_KEYS: {"LastModifiedTime"}
        }

        goals_report = copy.deepcopy(default_report)
        goals_report[BingAdsBaseTest.REQUIRED_KEYS] = {'Goal', 'TimePeriod'}

        audience_report = copy.deepcopy(default_report)
        audience_report[BingAdsBaseTest.REQUIRED_KEYS] = {'AudienceId'}

        geographic_report = copy.deepcopy(default_report)
        geographic_report[BingAdsBaseTest.REQUIRED_KEYS] = {'AccountName'}

        search_report = copy.deepcopy(default_report)
        search_report[BingAdsBaseTest.REQUIRED_KEYS] = {'SearchQuery'}

        # BUG_SRCE-4578 (https://stitchdata.atlassian.net/browse/SRCE-4578)
        #               'Impressions', 'Ctr', 'Clicks' shouldn't be automatic
        extension_report = copy.deepcopy(default_report)
        extension_report[BingAdsBaseTest.REQUIRED_KEYS] = {
            'AdExtensionId', 'AdExtensionPropertyValue', 'AdExtensionType', 'AdExtensionTypeId'
        }

        age_gender_report = copy.deepcopy(default_report)
        age_gender_report[BingAdsBaseTest.REQUIRED_KEYS] = {'AccountName', 'AdGroupName', 'AgeGroup', 'Gender'}

        return {
            "accounts": accounts_meta,
            "ad_extension_detail_report": extension_report, # BUG_DOC-1504 | https://stitchdata.atlassian.net/browse/DOC-1504
            "ad_group_performance_report": default_report, # BUG_DOC-1567 https://stitchdata.atlassian.net/browse/DOC-1567
            "ad_groups": default,
            "ad_performance_report": default_report,
            "ads": default,
            "age_gender_audience_report": age_gender_report, # BUG_DOC-1567
            "audience_performance_report": audience_report, # BUG_DOC-1504
            "campaign_performance_report": default_report,
            "campaigns": default,
            "geographic_performance_report": geographic_report,
            "goals_and_funnels_report": goals_report,
            "keyword_performance_report": default_report,
            "search_query_performance_report": search_report,
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass(logging="Ensuring environment variables are sourced.")
        missing_envs = [
            x for x in [
                'TAP_BING_ADS_OAUTH_CLIENT_ID','TAP_BING_ADS_OAUTH_CLIENT_SECRET','TAP_BING_ADS_REFRESH_TOKEN',
                'TAP_BING_ADS_DEVELOPER_TOKEN',
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def expected_replication_method(self,stream=None):
        """return a dictionary with key of table name nd value of replication method
            TDL-15816
            Currently, in tap, all streams are FULL_TABLE except accounts.
            But as per the doc https://www.stitchdata.com/docs/integrations/saas/microsoft-advertising,
            only the below streams are FULL TABLE, all other streams are INCREMENTAL.
            ads
            ad_groups
            campaigns
            """

        rep_method = {}
        for table, properties in self.expected_metadata().items():
            rep_method[table] = properties.get(self.REPLICATION_METHOD, None)
        for streams in rep_method.keys():
            if streams in [ 'ad_extension_detail_report', 'ad_group_performance_report', 'ad_performance_report',
                           'age_gender_audience_report', 'audience_performance_report', 'campaign_performance_report',                              'geographic_performance_report', 'goals_and_funnels_report', 'keyword_performance_report',
                           'search_query_performance_report']:
                rep_method[streams] = 'FULL_TABLE'
        if not stream:
            return rep_method
        return rep_method[stream]
    
    def expected_replication_keys(self,stream=None):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        """
        As all streams are FULL TABLE according to the tap, there is no replication key specified for any of
        the streams.TDL-15816, hence removing the "TimePeriod" key from expected replication keys.
        Need to determine the correct replication menthod and replication keys accordingly.
        """
        
        replication_keys = {table: properties.get(self.REPLICATION_KEYS, set())-{"TimePeriod"}
                            for table, properties
                            in self.expected_metadata().items()}
        if not stream:
            return replication_keys
        return replication_keys[stream]
    
    def expected_automatic_fields(self,stream=None):
        """
        return a dictionary with key of table name
        and value as a set of automatic fields
        """
        """
        Sdc_report_datetime is mentioned as primary key for most of the stream in docs,
        but is not returned as primary key by the tap, hence adding it explicitly to automatic fields TDL-15816
        """
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set())|v.get(self.REPLICATION_KEYS, set()) \
                |v.get(self.FOREIGN_KEYS, set())|v.get(self.REQUIRED_KEYS, set())|{'_sdc_report_datetime'}
        for streams in auto_fields.keys():
            if streams in ['ads', 'ad_groups', 'campaigns', 'accounts']:
                auto_fields[streams] = auto_fields[stream]-{'_sdc_report_datetime'}
        if not stream:
            return auto_fields
        return auto_fields[stream]
