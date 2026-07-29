"""
Integration tests for discovery mode (REST API refactor).
"""
import json
import unittest

from tests.integration_base import (
    ALL_EXPECTED_STREAMS,
    EXPECTED_CORE_STREAMS,
    EXPECTED_REPORT_STREAMS,
    BingAdsIntegrationBase,
)


class TestDiscoveryRESTRefactor(BingAdsIntegrationBase):
    """Tests for the discovery (catalog generation) mode."""

    def _run_discover(self):
        from tap_bing_ads.discover import discover
        return discover()

    def test_discover_returns_all_expected_streams(self):
        catalog = self._run_discover()
        found_streams = {entry.tap_stream_id for entry in catalog.streams}
        for stream_name in ALL_EXPECTED_STREAMS:
            self.assertIn(stream_name, found_streams,
                          f"Stream '{stream_name}' missing from catalog.")

    def test_discover_produces_exactly_expected_streams(self):
        catalog = self._run_discover()
        found_streams = {entry.tap_stream_id for entry in catalog.streams}
        expected = set(ALL_EXPECTED_STREAMS.keys())
        self.assertEqual(found_streams, expected)

    def test_core_streams_have_correct_key_properties(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            if entry.tap_stream_id not in EXPECTED_CORE_STREAMS:
                continue
            expected_keys = EXPECTED_CORE_STREAMS[entry.tap_stream_id]["key_properties"]
            self.assertEqual(sorted(entry.key_properties or []), sorted(expected_keys))

    def test_report_streams_have_time_period_as_automatic(self):
        from singer import metadata as md
        catalog = self._run_discover()
        for entry in catalog.streams:
            if entry.tap_stream_id not in EXPECTED_REPORT_STREAMS:
                continue
            md_map = md.to_map(entry.metadata)
            schema_props = entry.schema.to_dict().get("properties", {})
            if "TimePeriod" in schema_props:
                inclusion = md.get(md_map, ("properties", "TimePeriod"), "inclusion")
                self.assertEqual(inclusion, "automatic",
                                 f"TimePeriod should be automatic for {entry.tap_stream_id}")

    def test_report_streams_have_parent_tap_stream_id(self):
        from singer import metadata as md
        catalog = self._run_discover()
        for entry in catalog.streams:
            if entry.tap_stream_id not in EXPECTED_REPORT_STREAMS:
                continue
            md_map = md.to_map(entry.metadata)
            parent = md.get(md_map, (), "parent-tap-stream-id")
            self.assertEqual(parent, "accounts",
                             f"parent-tap-stream-id should be 'accounts' for {entry.tap_stream_id}")

    def test_accounts_replication_key_is_automatic(self):
        from singer import metadata as md
        catalog = self._run_discover()
        accounts = next(e for e in catalog.streams if e.tap_stream_id == "accounts")
        md_map = md.to_map(accounts.metadata)
        inclusion = md.get(md_map, ("properties", "LastModifiedTime"), "inclusion")
        self.assertEqual(inclusion, "automatic")

    def test_schemas_have_valid_structure(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            schema_dict = entry.schema.to_dict()
            self.assertEqual(schema_dict.get("type"), "object",
                             f"Type should be 'object' for {entry.tap_stream_id}")
            self.assertIn("properties", schema_dict)

    def test_catalog_serializes_to_json(self):
        catalog = self._run_discover()
        catalog_json = json.dumps(catalog.to_dict())
        reloaded = json.loads(catalog_json)
        self.assertIn("streams", reloaded)
        self.assertEqual(len(reloaded["streams"]), len(ALL_EXPECTED_STREAMS))


if __name__ == "__main__":
    unittest.main()
