"""AudiencePerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AudiencePerformanceReport(BaseReport):
    tap_stream_id = "audience_performance_report"
    report_name = "AudiencePerformanceReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "AssociationId",
        "AssociationLevel"
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = ["AudienceId"]
