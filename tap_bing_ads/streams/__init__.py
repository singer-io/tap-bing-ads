"""
Stream registry for tap-bing-ads.

All stream classes are imported here and exposed via the STREAMS dict,
keyed by tap_stream_id.
"""
from tap_bing_ads.streams.accounts import Accounts
from tap_bing_ads.streams.campaigns import Campaigns
from tap_bing_ads.streams.ad_groups import AdGroups
from tap_bing_ads.streams.ads import Ads
from tap_bing_ads.streams.keyword_performance_report import KeywordPerformanceReport
from tap_bing_ads.streams.ad_performance_report import AdPerformanceReport
from tap_bing_ads.streams.ad_group_performance_report import AdGroupPerformanceReport
from tap_bing_ads.streams.geographic_performance_report import GeographicPerformanceReport
from tap_bing_ads.streams.age_gender_audience_report import AgeGenderAudienceReport
from tap_bing_ads.streams.search_query_performance_report import SearchQueryPerformanceReport
from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
from tap_bing_ads.streams.goals_and_funnels_report import GoalsAndFunnelsReport
from tap_bing_ads.streams.audience_performance_report import AudiencePerformanceReport
from tap_bing_ads.streams.ad_extension_detail_report import AdExtensionDetailReport

# Core entity streams
CORE_STREAMS = {
    "accounts": Accounts,
    "campaigns": Campaigns,
    "ad_groups": AdGroups,
    "ads": Ads,
}

# Report streams
REPORT_STREAMS = {
    "keyword_performance_report": KeywordPerformanceReport,
    "ad_performance_report": AdPerformanceReport,
    "ad_group_performance_report": AdGroupPerformanceReport,
    "geographic_performance_report": GeographicPerformanceReport,
    "age_gender_audience_report": AgeGenderAudienceReport,
    "search_query_performance_report": SearchQueryPerformanceReport,
    "campaign_performance_report": CampaignPerformanceReport,
    "goals_and_funnels_report": GoalsAndFunnelsReport,
    "audience_performance_report": AudiencePerformanceReport,
    "ad_extension_detail_report": AdExtensionDetailReport,
}

# Combined registry
STREAMS = {**CORE_STREAMS, **REPORT_STREAMS}
