"""CampaignPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class CampaignPerformanceReport(BaseReport):
    tap_stream_id = "campaign_performance_report"
    report_name = "CampaignPerformanceReport"
    report_specific_columns = []
