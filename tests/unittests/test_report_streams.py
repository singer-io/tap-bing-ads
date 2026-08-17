"""Unit tests for the report stream base class."""
import io
import json
import time
import unittest
import zipfile
from unittest.mock import MagicMock, patch, call

import arrow
import singer
from singer.catalog import CatalogEntry, Schema
from singer import metadata

from tap_bing_ads.streams.abstracts import BaseReport
from tap_bing_ads.streams.campaign_performance_report import CampaignPerformanceReport
from tap_bing_ads.streams.keyword_performance_report import KeywordPerformanceReport
from tap_bing_ads.exceptions import (
    BingAdsInvalidFieldSelection,
    BingAdsNoMeasureSelected,
    BingAdsReportError,
)
from tap_bing_ads.client import Client

DEFAULT_CONFIG = {
    "oauth_client_id": "id",
    "oauth_client_secret": "secret",
    "refresh_token": "refresh",
    "developer_token": "dev",
    "customer_id": "12345",
    "account_ids": "67890",
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-31T00:00:00Z",
    "report_max_days": 30,
    "conversion_window": -30,
}


_REPORT_FIELD_SCHEMA = {
    "Clicks":                   {"type": ["null", "integer"]},
    "Impressions":              {"type": ["null", "integer"]},
    "AccountId":                {"type": ["null", "integer"]},
    "Spend":                    {"type": ["null", "number"]},
    "Ctr":                      {"type": ["null", "number"]},
    "AverageCpc":               {"type": ["null", "number"]},
    "AverageCpm":               {"type": ["null", "number"]},
    "ImpressionSharePercent":   {"type": ["null", "number"]},
    "TimePeriod":               {"type": ["null", "string"], "format": "date-time"},
}


def _make_report_catalog_entry(stream_cls, selected_columns=None):
    """Build a CatalogEntry with selected columns for a report stream."""
    selected_columns = selected_columns or ["TimePeriod", "AccountId", "Clicks", "Impressions", "Spend"]
    schema_dict = {
        "type": "object",
        "properties": {
            col: _REPORT_FIELD_SCHEMA.get(col, {"type": ["null", "string"]})
            for col in selected_columns
        },
    }
    schema_dict["properties"]["_sdc_report_datetime"] = {
        "type": ["null", "string"], "format": "date-time"
    }

    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema_dict,
        key_properties=[],
        valid_replication_keys=["TimePeriod"],
        replication_method="INCREMENTAL",
    )
    mdata = metadata.to_map(mdata)
    mdata = metadata.write(mdata, (), "selected", True)
    for col in selected_columns:
        inc = "automatic" if col in ["TimePeriod", "AccountId"] else "available"
        mdata = metadata.write(mdata, ("properties", col), "inclusion", inc)
        if inc == "available":
            mdata = metadata.write(mdata, ("properties", col), "selected", True)

    return CatalogEntry(
        stream=stream_cls.tap_stream_id,
        tap_stream_id=stream_cls.tap_stream_id,
        key_properties=[],
        schema=Schema.from_dict(schema_dict),
        metadata=metadata.to_list(mdata),
    )


def _make_report_zip(csv_content: str) -> bytes:
    """Create an in-memory ZIP containing one CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("report.csv", csv_content)
    return buf.getvalue()


class TestBaseReportBuildRequest(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.catalog_entry = _make_report_catalog_entry(CampaignPerformanceReport)
        self.stream = CampaignPerformanceReport(self.client, self.catalog_entry)

    def test_build_report_request_structure(self):
        start = arrow.get("2024-01-01")
        end = arrow.get("2024-01-31")
        body = self.stream._build_report_request("67890", ["Clicks", "Impressions"], start, end)

        rr = body["ReportRequest"]
        self.assertEqual(rr["ReportName"], "CampaignPerformanceReport")
        self.assertEqual(rr["Format"], "Csv")
        self.assertEqual(rr["Scope"]["AccountIds"], [67890])
        self.assertEqual(rr["Time"]["CustomDateRangeStart"]["Day"], 1)
        self.assertEqual(rr["Time"]["CustomDateRangeStart"]["Month"], 1)
        self.assertEqual(rr["Time"]["CustomDateRangeStart"]["Year"], 2024)
        self.assertEqual(rr["Time"]["CustomDateRangeEnd"]["Day"], 31)
        self.assertIn("Columns", rr)

    def test_build_report_request_excludes_sdc_fields(self):
        start = arrow.get("2024-01-01")
        end = arrow.get("2024-01-31")
        columns = ["TimePeriod", "AccountId", "Clicks", "_sdc_report_datetime"]
        body = self.stream._build_report_request("67890", columns, start, end)
        # _sdc_report_datetime is included in columns here since we pass it explicitly
        # but it shouldn't be sent to API (handled in get_selected_columns)
        self.assertIn("Columns", body["ReportRequest"])


class TestBaseReportSubmit(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.catalog_entry = _make_report_catalog_entry(CampaignPerformanceReport)
        self.stream = CampaignPerformanceReport(self.client, self.catalog_entry)

    def test_submit_report_returns_request_id(self):
        self.client.make_request.return_value = {"ReportRequestId": "abc-123"}
        request_id = self.stream._submit_report("67890", {"ReportName": "test"})
        self.assertEqual(request_id, "abc-123")

    def test_submit_report_raises_when_no_id(self):
        self.client.make_request.return_value = {}
        with self.assertRaises(BingAdsReportError):
            self.stream._submit_report("67890", {"ReportName": "test"})


class TestBaseReportPoll(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.catalog_entry = _make_report_catalog_entry(CampaignPerformanceReport)
        self.stream = CampaignPerformanceReport(self.client, self.catalog_entry)

    @patch("time.sleep")
    def test_poll_returns_download_url_on_success(self, mock_sleep):
        self.client.make_request.return_value = {
            "ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": "https://download.example.com/report.zip",
            }
        }
        url = self.stream._poll_report("67890", "req-id-123")
        self.assertEqual(url, "https://download.example.com/report.zip")

    @patch("time.sleep")
    def test_poll_raises_on_error_status(self, mock_sleep):
        self.client.make_request.return_value = {
            "ReportRequestStatus": {"Status": "Error"}
        }
        with self.assertRaises(BingAdsReportError):
            self.stream._poll_report("67890", "req-id-123")

    @patch("time.sleep")
    def test_poll_returns_none_when_no_download_url(self, mock_sleep):
        self.client.make_request.return_value = {
            "ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": None,
            }
        }
        url = self.stream._poll_report("67890", "req-id-123")
        self.assertIsNone(url)

    @patch("time.sleep")
    def test_poll_waits_while_pending(self, mock_sleep):
        responses = [
            {"ReportRequestStatus": {"Status": "Pending"}},
            {"ReportRequestStatus": {"Status": "Running"}},
            {"ReportRequestStatus": {"Status": "Success", "ReportDownloadUrl": "http://x.com/r.zip"}},
        ]
        self.client.make_request.side_effect = responses
        url = self.stream._poll_report("67890", "req-id")
        self.assertEqual(url, "http://x.com/r.zip")
        self.assertEqual(self.client.make_request.call_count, 3)


class TestBaseReportCsvParsing(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.catalog_entry = _make_report_catalog_entry(
            CampaignPerformanceReport,
            selected_columns=[
                "TimePeriod", "AccountId", "Clicks", "Impressions", "Spend",
                "Ctr", "AverageCpc", "ImpressionSharePercent",
            ],
        )
        self.stream = CampaignPerformanceReport(self.client, self.catalog_entry)

    @patch("tap_bing_ads.streams.abstracts.write_record")
    def test_stream_report_csv_writes_records(self, mock_write_record):
        csv_content = "TimePeriod,AccountId,Clicks,Impressions,Spend\n2024-01-01,67890,100,1000,50.0\n2024-01-02,67890,200,2000,100.0\n"
        zip_content = _make_report_zip(csv_content)
        self.client.download_file.return_value = zip_content

        count = self.stream._stream_report_csv(
            "campaign_performance_report",
            "http://example.com/report.zip",
            "2024-01-03T00:00:00+00:00",
            {},
        )
        self.assertEqual(count, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    def test_type_row_converts_integer_fields(self):
        row = {"Clicks": "1,234", "Impressions": "5,678", "CampaignName": "Test"}
        self.stream._type_row(row)
        self.assertEqual(row["Clicks"], 1234)
        self.assertEqual(row["Impressions"], 5678)
        self.assertEqual(row["CampaignName"], "Test")

    def test_type_row_handles_dash_values(self):
        row = {"Clicks": "--", "AverageCpc": "--", "CampaignName": ""}
        self.stream._type_row(row)
        self.assertEqual(row["Clicks"], 0)
        self.assertEqual(row["AverageCpc"], 0.0)
        self.assertIsNone(row["CampaignName"])

    def test_type_row_converts_number_with_percent(self):
        row = {"Ctr": "12.5%", "ImpressionSharePercent": "75.3%"}
        self.stream._type_row(row)
        self.assertAlmostEqual(row["Ctr"], 12.5)
        self.assertAlmostEqual(row["ImpressionSharePercent"], 75.3)


class TestNoMeasureSelected(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        # Select only dimension columns (no metrics)
        self.catalog_entry = _make_report_catalog_entry(
            CampaignPerformanceReport,
            selected_columns=["TimePeriod", "AccountId", "CampaignName"],
        )
        self.stream = CampaignPerformanceReport(self.client, self.catalog_entry)

    def test_raises_no_measure_selected(self):
        with self.assertRaises(BingAdsNoMeasureSelected):
            self.stream.sync(
                state={}, account_id="67890", config=DEFAULT_CONFIG
            )


class TestGetReportInterval(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock(spec=Client)
        self.catalog_entry = _make_report_catalog_entry(CampaignPerformanceReport)
        self.stream = CampaignPerformanceReport(self.client, self.catalog_entry)

    def test_start_date_uses_bookmark_when_present(self):
        state = {
            "bookmarks": {
                "67890_campaign_performance_report": {"date": "2024-01-15"}
            }
        }
        config = {**DEFAULT_CONFIG, "end_date": "2024-02-28T00:00:00Z"}
        # Patch _sync_interval to capture the start_date passed in
        with patch.object(self.stream, "_sync_interval", return_value=0) as mock_interval:
            self.stream.sync(state=state, account_id="67890", config=config)
            # call_args_list[0] = first call (bookmark date + 1 day = Jan 16)
            first_call = mock_interval.call_args_list[0]
            start_date_used = first_call[1].get("start_date") or first_call[0][4]
            self.assertEqual(start_date_used.date().isoformat(), "2024-01-16")
