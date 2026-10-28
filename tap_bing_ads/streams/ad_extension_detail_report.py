"""AdExtensionDetailReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AdExtensionDetailReport(BaseReport):
    tap_stream_id = "ad_extension_detail_report"
    report_name = "AdExtensionDetailReport"
    report_specific_columns = [
        "AdExtensionId",
        "AdExtensionPropertyValue",
        "AdExtensionType",
        "AdExtensionTypeId",
    ]
