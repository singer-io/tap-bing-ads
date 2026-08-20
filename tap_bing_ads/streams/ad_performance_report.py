"""AdPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AdPerformanceReport(BaseReport):
    tap_stream_id = "ad_performance_report"
    report_name = "AdPerformanceReport"
    report_specific_columns = []
