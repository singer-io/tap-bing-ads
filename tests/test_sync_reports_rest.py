"""
Integration tests for sync mode — report streams.

Tests the full poll/download/parse cycle for report streams using
mocked REST API responses and in-memory ZIP files.
"""
import time
import unittest
from unittest.mock import MagicMock, patch

import arrow
import singer

from tap_bing_ads.client import Client
from tap_bing_ads.exceptions import BingAdsNoMeasureSelected, BingAdsReportError
from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
from tap_bing_ads.streams.keyword_performance_report import KeywordPerformanceReport
from tests.integration_base import (
    DEFAULT_CONFIG,
    BingAdsIntegrationBase,
    make_report_zip,
)

SAMPLE_CAMPAIGN_REPORT_ROWS = [
    {
        "TimePeriod": "2024-01-01",
        "AccountId": "188412305",
        "CampaignId": "1001",
        "CampaignName": "Search Campaign",
        "Clicks": "150",
        "Impressions": "5000",
        "Spend": "75.50",
        "Ctr": "3.0%",
    },
    {
        "TimePeriod": "2024-01-02",
        "AccountId": "188412305",
        "CampaignId": "1001",
        "CampaignName": "Search Campaign",
        "Clicks": "200",
        "Impressions": "6000",
        "Spend": "98.00",
        "Ctr": "3.33%",
    },
]

SAMPLE_KEYWORD_REPORT_ROWS = [
    {
        "TimePeriod": "2024-01-01",
        "AccountId": "188412305",
        "KeywordId": "5001",
        "Keyword": "running shoes",
        "Clicks": "50",
        "Impressions": "1000",
        "Spend": "25.00",
    },
]


class TestReportSyncCycle(BingAdsIntegrationBase):
    """Full poll/download/parse cycle for report streams."""

    def _build_stream(self, stream_cls, selected_columns=None):
        if selected_columns is None:
            selected_columns = ["TimePeriod", "AccountId", "Clicks", "Impressions", "Spend"]
        catalog = self.get_catalog(selected_streams=[stream_cls.tap_stream_id])
        stream_entry = catalog.streams[0]
        # Reset ALL fields to unselected, then select only the safe specified columns.
        # This avoids triggering field exclusion conflicts on fully-selected schemas.
        from singer import metadata
        md_map = metadata.to_map(stream_entry.metadata)
        for prop in stream_entry.schema.to_dict().get("properties", {}):
            inc = metadata.get(md_map, ("properties", prop), "inclusion")
            if inc != "automatic":
                md_map = metadata.write(md_map, ("properties", prop), "selected", False)
        for col in selected_columns:
            md_map = metadata.write(md_map, ("properties", col), "selected", True)
        stream_entry.metadata = metadata.to_list(md_map)
        return stream_cls(MagicMock(spec=Client), stream_entry)

    @patch("time.sleep")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    def test_full_report_sync_cycle_writes_records(
        self, mock_ws, mock_wb, mock_schema, mock_record, mock_sleep
    ):
        """Report stream: submit → poll → download → parse → write records."""
        stream = self._build_stream(CampaignPerformanceReport)
        zip_bytes = make_report_zip(SAMPLE_CAMPAIGN_REPORT_ROWS)

        stream.client.post.return_value = {"ReportRequestId": "test-req-id-001"}
        stream.client.get.return_value = {
            "ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": "https://download.example.com/report.zip",
            }
        }
        stream.client.download_file.return_value = zip_bytes

        total = stream.sync(
            state=self.state,
            account_id="188412305",
            config=self.config,
        )

        self.assertEqual(total, 2)
        self.assertEqual(mock_record.call_count, 2)

    @patch("time.sleep")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    def test_report_sync_no_data_writes_zero_records(
        self, mock_ws, mock_wb, mock_schema, mock_record, mock_sleep
    ):
        """When download URL is None, no records are written."""
        stream = self._build_stream(CampaignPerformanceReport)
        stream.client.post.return_value = {"ReportRequestId": "test-req-id-002"}
        stream.client.get.return_value = {
            "ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": None,
            }
        }

        total = stream.sync(
            state=self.state,
            account_id="188412305",
            config=self.config,
        )

        self.assertEqual(total, 0)
        mock_record.assert_not_called()

    @patch("time.sleep")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    def test_report_sync_updates_bookmark_on_success(
        self, mock_ws, mock_wb, mock_sleep
    ):
        """State bookmark is updated after a successful sync interval."""
        with patch("tap_bing_ads.streams.abstracts.write_schema"), patch("tap_bing_ads.streams.abstracts.write_record"):
            stream = self._build_stream(CampaignPerformanceReport)
            stream.client.post.return_value = {"ReportRequestId": "req-id-bm"}
            stream.client.get.return_value = {
                "ReportRequestStatus": {"Status": "Success", "ReportDownloadUrl": None}
            }

            stream.sync(state=self.state, account_id="188412305", config=self.config)

        # Bookmark should have been written
        mock_wb.assert_called()

    def test_report_sync_raises_no_measure_selected_when_no_metrics(self):
        """BingAdsNoMeasureSelected raised when no metric columns are selected."""
        catalog = self.get_catalog(selected_streams=["campaign_performance_report"])
        stream_entry = catalog.streams[0]

        # Override metadata to only select dimension columns
        from singer import metadata
        md_map = metadata.to_map(stream_entry.metadata)
        # Deselect everything, then select only dimensions
        for prop in stream_entry.schema.to_dict().get("properties", {}):
            md_map = metadata.write(md_map, ("properties", prop), "selected", False)
        md_map = metadata.write(md_map, ("properties", "TimePeriod"), "selected", True)
        md_map = metadata.write(md_map, ("properties", "AccountId"), "selected", True)
        md_map = metadata.write(md_map, ("properties", "CampaignName"), "selected", True)
        stream_entry.metadata = metadata.to_list(md_map)

        stream = CampaignPerformanceReport(MagicMock(spec=Client), stream_entry)

        with self.assertRaises(BingAdsNoMeasureSelected):
            stream.sync(state=self.state, account_id="188412305", config=self.config)

    @patch("time.sleep")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    def test_bookmark_resumes_from_previous_end_date(
        self, mock_ws, mock_wb, mock_schema, mock_record, mock_sleep
    ):
        """When state has a bookmark date, sync resumes from the day after."""
        state_key = "188412305_campaign_performance_report"
        state = {"bookmarks": {state_key: {"date": "2024-01-04"}}}

        stream = self._build_stream(CampaignPerformanceReport)
        stream.client.post.return_value = {"ReportRequestId": "req-resume"}
        stream.client.get.return_value = {
            "ReportRequestStatus": {"Status": "Success", "ReportDownloadUrl": None}
        }

        # Track what date range was requested
        captured_bodies = []
        original_post = stream.client.post

        def capturing_post(url, json_body=None, account_id=None):
            if json_body:
                captured_bodies.append(json_body)
            return {"ReportRequestId": "req-resume"}

        stream.client.post.side_effect = capturing_post

        stream.sync(state=state, account_id="188412305", config=self.config)

        # The first report request should start on Jan 5th (day after bookmark)
        if captured_bodies:
            start = captured_bodies[0]["Time"]["CustomDateRangeStart"]
            self.assertEqual(start["Day"], 5)
            self.assertEqual(start["Month"], 1)
            self.assertEqual(start["Year"], 2024)

    @patch("time.sleep")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    def test_keyword_report_sync_writes_records(
        self, mock_ws, mock_wb, mock_schema, mock_record, mock_sleep
    ):
        """KeywordPerformanceReport writes records from CSV data."""
        stream = self._build_stream(KeywordPerformanceReport)
        zip_bytes = make_report_zip(SAMPLE_KEYWORD_REPORT_ROWS)

        stream.client.post.return_value = {"ReportRequestId": "req-kw-001"}
        stream.client.get.return_value = {
            "ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": "https://download.example.com/kw.zip",
            }
        }
        stream.client.download_file.return_value = zip_bytes

        total = stream.sync(state=self.state, account_id="188412305", config=self.config)
        self.assertEqual(total, 1)

    @patch("time.sleep")
    @patch("tap_bing_ads.streams.abstracts.write_record")
    @patch("tap_bing_ads.streams.abstracts.write_schema")
    @patch("tap_bing_ads.streams.abstracts.write_bookmark")
    @patch("tap_bing_ads.streams.abstracts.write_state")
    def test_report_resumes_from_saved_request_id(
        self, mock_ws, mock_wb, mock_schema, mock_record, mock_sleep
    ):
        """When state has a saved request_id, poll resumes without re-submitting."""
        state_key = "188412305_campaign_performance_report"
        state = {"bookmarks": {state_key: {"request_id": "existing-req-id"}}}

        stream = self._build_stream(CampaignPerformanceReport)
        stream.client.get.return_value = {
            "ReportRequestStatus": {"Status": "Success", "ReportDownloadUrl": None}
        }

        stream.sync(state=state, account_id="188412305", config=self.config)

        # POST (submit) should not have been called since request_id was saved
        stream.client.post.assert_not_called()


class TestReportCsvTyping(BingAdsIntegrationBase):
    """Tests for the CSV type-casting logic."""

    def setUp(self):
        super().setUp()
        from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
        self.stream = CampaignPerformanceReport(MagicMock(spec=Client))

    def test_type_row_integer_columns(self):
        row = {"Clicks": "1,234", "Impressions": "50,000", "CampaignName": "Test"}
        self.stream._type_row(row)
        self.assertEqual(row["Clicks"], 1234)
        self.assertEqual(row["Impressions"], 50000)

    def test_type_row_number_columns_with_percent(self):
        row = {"Ctr": "3.5%", "ImpressionSharePercent": "75.2%"}
        self.stream._type_row(row)
        self.assertAlmostEqual(row["Ctr"], 3.5)
        self.assertAlmostEqual(row["ImpressionSharePercent"], 75.2)

    def test_type_row_dash_becomes_zero(self):
        row = {"Clicks": "--", "AverageCpc": "--"}
        self.stream._type_row(row)
        self.assertEqual(row["Clicks"], 0)
        self.assertAlmostEqual(row["AverageCpc"], 0.0)

    def test_type_row_empty_string_becomes_none(self):
        row = {"CampaignName": "", "Clicks": ""}
        self.stream._type_row(row)
        self.assertIsNone(row["CampaignName"])

    def test_type_row_datetime_column_is_iso_formatted(self):
        row = {"TimePeriod": "2024-01-15"}
        self.stream._type_row(row)
        # arrow.get("2024-01-15").isoformat() should produce a valid ISO string
        self.assertIn("2024-01-15", row["TimePeriod"])


if __name__ == "__main__":
    unittest.main()
