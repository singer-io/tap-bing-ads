"""AgeGenderAudienceReport stream."""
from tap_bing_ads.streams.abstracts import BaseReport


class AgeGenderAudienceReport(BaseReport):
    tap_stream_id = "age_gender_audience_report"
    report_name = "AgeGenderAudienceReport"
    report_specific_columns = ["AccountName", "AdGroupName", "AgeGroup", "Gender"]
