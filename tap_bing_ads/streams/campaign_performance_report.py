"""CampaignPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class CampaignPerformanceReport(BaseReport):
    tap_stream_id = "campaign_performance_report"
    report_name = "CampaignPerformanceReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "CampaignId",
        "Network",
        "TopVsOther",
        "DeviceType",
        "DeliveredMatchType",
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = []
