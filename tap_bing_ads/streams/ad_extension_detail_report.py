"""AdExtensionDetailReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AdExtensionDetailReport(BaseReport):
    tap_stream_id = "ad_extension_detail_report"
    report_name = "AdExtensionDetailReport"
    key_properties = [
        "TimePeriod",
        "AccountId",
        "AdExtensionId",
        "AdExtensionVersion",
        "DeviceType",
        "Network",
        "TopVsOther",
    ]
    replication_method = "INCREMENTAL"
    replication_keys = ["TimePeriod"]
    report_specific_columns = [
        "AdExtensionId",
        "AdExtensionPropertyValue",
        "AdExtensionType",
        "AdExtensionTypeId",
    ]
