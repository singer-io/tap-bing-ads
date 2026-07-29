"""GeographicPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class GeographicPerformanceReport(BaseReport):
    tap_stream_id = "geographic_performance_report"
    report_name = "GeographicPerformanceReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "CampaignId",
        "LocationId",
        "Network",
        "TopVsOther",
        "DeviceType",
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = ["AccountName"]
