"""KeywordPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class KeywordPerformanceReport(BaseReport):
    tap_stream_id = "keyword_performance_report"
    report_name = "KeywordPerformanceReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "KeywordId",
        "AdId",
        "Network",
        "TopVsOther",
        "DeviceType",
        "DeliveredMatchType",
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = []
