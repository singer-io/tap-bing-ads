"""AdGroupPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AdGroupPerformanceReport(BaseReport):
    tap_stream_id = "ad_group_performance_report"
    report_name = "AdGroupPerformanceReport"
    report_specific_columns = []
