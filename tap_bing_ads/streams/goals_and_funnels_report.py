"""GoalsAndFunnelsReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class GoalsAndFunnelsReport(BaseReport):
    tap_stream_id = "goals_and_funnels_report"
    report_name = "GoalsAndFunnelsReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "AdGroupId",
        "GoalId",
        "DeviceType",
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = ["Goal"]
