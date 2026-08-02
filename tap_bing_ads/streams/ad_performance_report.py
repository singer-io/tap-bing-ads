"""AdPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AdPerformanceReport(BaseReport):
    tap_stream_id = "ad_performance_report"
    report_name = "AdPerformanceReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "AdId",
        "Network",
        "TopVsOther",
        "DeviceType",
        "DeliveredMatchType"
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = []
