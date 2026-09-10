"""SearchQueryPerformanceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class SearchQueryPerformanceReport(BaseReport):
    tap_stream_id = "search_query_performance_report"
    report_name = "SearchQueryPerformanceReport"
    report_specific_columns = ["SearchQuery"]
