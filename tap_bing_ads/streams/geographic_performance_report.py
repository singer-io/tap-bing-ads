"""GeographicPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class GeographicPerformanceReport(BaseReport):
    tap_stream_id = "geographic_performance_report"
    report_name = "GeographicPerformanceReport"
    report_specific_columns = ["AccountName"]
