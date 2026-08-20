"""KeywordPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class KeywordPerformanceReport(BaseReport):
    tap_stream_id = "keyword_performance_report"
    report_name = "KeywordPerformanceReport"
    report_specific_columns = []
