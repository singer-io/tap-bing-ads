"""SearchQueryPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class SearchQueryPerformanceReport(BaseReport):
    tap_stream_id = "search_query_performance_report"
    report_name = "SearchQueryPerformanceReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "AdGroupCriterionId",
        "SearchQuery",
        "Network",
        "TopVsOther",
        "DeviceType",
        "DeliveredMatchType"
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = ["SearchQuery"]
