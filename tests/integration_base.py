"""
Integration test base for tap-bing-ads (REST API refactor).

These tests exercise the full tap pipeline (discover → sync) against
mocked Microsoft Advertising REST API responses. They do NOT require
live API credentials.
"""
import io
import json
import os
import sys
import unittest
import zipfile
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata


# ──────────────────────────────────────────────────────────────────────────────
# Default test config (no real credentials needed for mock tests)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "oauth_client_id": "test_client_id",
    "oauth_client_secret": "test_client_secret",
    "refresh_token": "test_refresh_token",
    "developer_token": "test_dev_token",
    "customer_id": "254943312",
    "account_ids": "188412305,188412306",
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-07T00:00:00Z",
    "report_max_days": 7,
    "conversion_window": -7,
}

# ──────────────────────────────────────────────────────────────────────────────
# Canonical list of streams and their metadata
# ──────────────────────────────────────────────────────────────────────────────
EXPECTED_CORE_STREAMS = {
    "accounts": {
        "key_properties": ["Id"],
        "replication_method": "INCREMENTAL",
        "replication_key": "LastModifiedTime",
    },
    "campaigns": {
        "key_properties": ["Id"],
        "replication_method": "FULL_TABLE",
        "replication_key": None,
    },
    "ad_groups": {
        "key_properties": ["Id"],
        "replication_method": "FULL_TABLE",
        "replication_key": None,
    },
    "ads": {
        "key_properties": ["Id"],
        "replication_method": "FULL_TABLE",
        "replication_key": None,
    },
}

EXPECTED_REPORT_STREAMS = {
    "keyword_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "ad_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "ad_group_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "geographic_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "age_gender_audience_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "search_query_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "campaign_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "goals_and_funnels_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "audience_performance_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
    "ad_extension_detail_report": {"replication_method": "INCREMENTAL", "replication_key": "TimePeriod"},
}

ALL_EXPECTED_STREAMS = {**EXPECTED_CORE_STREAMS, **EXPECTED_REPORT_STREAMS}


def mock_token_response():
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {"access_token": "mock_access_token_xyz"}
    return m


def make_report_zip(csv_rows: List[Dict]) -> bytes:
    """Build a ZIP-wrapped CSV for report download."""
    if not csv_rows:
        return b""
    headers = list(csv_rows[0].keys())
    lines = [",".join(headers)]
    for row in csv_rows:
        lines.append(",".join(str(row.get(h, "")) for h in headers))
    csv_content = "\n".join(lines) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("report.csv", csv_content)
    return buf.getvalue()


class BingAdsIntegrationBase(unittest.TestCase):
    """
    Base class for tap-bing-ads integration tests.
    """

    def setUp(self):
        self.config = DEFAULT_CONFIG.copy()
        self.state = {}
        self._captured_records: Dict[str, List] = {}
        self._captured_schemas: Dict[str, Dict] = {}

    def _capture_record(self, stream_name, record, **kwargs):
        self._captured_records.setdefault(stream_name, []).append(record)

    def _capture_schema(self, stream_name, schema, key_properties, **kwargs):
        self._captured_schemas[stream_name] = schema

    def get_catalog(self, selected_streams: Optional[List[str]] = None) -> Catalog:
        """Build a catalog with all (or selected) streams fully selected."""
        from tap_bing_ads.schema import get_schemas
        schemas, field_metadata = get_schemas()
        entries = []
        for stream_name, schema_dict in schemas.items():
            if selected_streams and stream_name not in selected_streams:
                continue
            mdata = field_metadata[stream_name]
            md_map = metadata.to_map(mdata)
            md_map = metadata.write(md_map, (), "selected", True)
            for prop in schema_dict.get("properties", {}):
                inc = metadata.get(md_map, ("properties", prop), "inclusion")
                if inc != "automatic":
                    md_map = metadata.write(md_map, ("properties", prop), "selected", True)
            entries.append(
                CatalogEntry(
                    stream=stream_name,
                    tap_stream_id=stream_name,
                    key_properties=EXPECTED_CORE_STREAMS.get(stream_name, {}).get("key_properties", []),
                    schema=Schema.from_dict(schema_dict),
                    metadata=metadata.to_list(md_map),
                )
            )
        return Catalog(entries)
