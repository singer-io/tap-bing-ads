"""Unit tests for discovery mode — schema generation and access-check logic."""
import unittest
from unittest.mock import MagicMock, patch

from singer import metadata as md

from tap_bing_ads.client import Client
from tap_bing_ads.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    discover,
)
from tap_bing_ads.exceptions import BingAdsForbiddenError
from tap_bing_ads.schema import get_schemas
from tap_bing_ads.streams import STREAMS

# ── Expected stream constants ─────────────────────────────────────────────────

EXPECTED_CORE_STREAMS = {"accounts", "campaigns", "ad_groups", "ads"}
EXPECTED_REPORT_STREAMS = {
    "keyword_performance_report",
    "ad_performance_report",
    "ad_group_performance_report",
    "geographic_performance_report",
    "age_gender_audience_report",
    "search_query_performance_report",
    "campaign_performance_report",
    "goals_and_funnels_report",
    "audience_performance_report",
    "ad_extension_detail_report",
}
ALL_EXPECTED_STREAMS = EXPECTED_CORE_STREAMS | EXPECTED_REPORT_STREAMS


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_client():
    client = MagicMock(spec=Client)
    client.config = {
        "account_ids": "163078754",
        "start_date": "2024-01-01T00:00:00Z",
    }
    return client


# ── Schema / catalog structure tests ──────────────────────────────────────────

class TestGetSchemas(unittest.TestCase):
    """Tests for get_schemas() output."""

    def setUp(self):
        self.schemas, self.field_metadata = get_schemas()

    def test_returns_all_expected_streams(self):
        for stream in ALL_EXPECTED_STREAMS:
            with self.subTest(stream=stream):
                self.assertIn(stream, self.schemas,
                              f"Stream '{stream}' missing from get_schemas() output.")

    def test_schemas_are_object_type(self):
        for stream, schema in self.schemas.items():
            with self.subTest(stream=stream):
                self.assertEqual(schema.get("type"), "object",
                                 f"Schema for {stream} should have type 'object'.")

    def test_schemas_have_properties(self):
        for stream, schema in self.schemas.items():
            with self.subTest(stream=stream):
                self.assertIn("properties", schema,
                              f"Schema for {stream} missing 'properties' key.")
                self.assertGreater(len(schema["properties"]), 0,
                                   f"Schema for {stream} has no properties.")

    def test_field_metadata_has_replication_method(self):
        for stream, mdata_list in self.field_metadata.items():
            with self.subTest(stream=stream):
                mdata_map = md.to_map(mdata_list)
                rep_method = mdata_map.get((), {}).get("forced-replication-method")
                self.assertIn(rep_method, ("INCREMENTAL", "FULL_TABLE"),
                              f"Invalid replication-method for {stream}: {rep_method!r}")

    def test_accounts_has_last_modified_time_as_replication_key(self):
        mdata_map = md.to_map(self.field_metadata["accounts"])
        rep_keys = mdata_map.get((), {}).get("valid-replication-keys", [])
        self.assertIn("LastModifiedTime", rep_keys)

    def test_report_streams_have_time_period_automatic(self):
        for stream in EXPECTED_REPORT_STREAMS:
            with self.subTest(stream=stream):
                mdata_map = md.to_map(self.field_metadata[stream])
                schema_props = self.schemas[stream].get("properties", {})
                if "TimePeriod" in schema_props:
                    inclusion = mdata_map.get(("properties", "TimePeriod"), {}).get("inclusion")
                    self.assertEqual(inclusion, "automatic",
                                     f"TimePeriod should be automatic for {stream}.")

    def test_report_streams_have_parent_tap_stream_id(self):
        for stream in EXPECTED_REPORT_STREAMS:
            with self.subTest(stream=stream):
                mdata_map = md.to_map(self.field_metadata[stream])
                parent = mdata_map.get((), {}).get("parent-tap-stream-id")
                self.assertEqual(parent, "accounts",
                                 f"parent-tap-stream-id should be 'accounts' for {stream}.")

    def test_core_streams_have_key_properties(self):
        for stream in EXPECTED_CORE_STREAMS:
            with self.subTest(stream=stream):
                mdata_map = md.to_map(self.field_metadata[stream])
                key_props = mdata_map.get((), {}).get("table-key-properties", [])
                self.assertGreater(len(key_props), 0,
                                   f"No key-properties metadata for {stream}.")


# ── discover() — full catalog without access checks ───────────────────────────

class TestDiscoverNoCLient(unittest.TestCase):
    """discover() called without a client should return all streams."""

    def test_returns_all_streams_without_client(self):
        catalog = discover()
        found = {e.tap_stream_id for e in catalog.streams}
        self.assertSetEqual(found, ALL_EXPECTED_STREAMS)

    def test_catalog_entries_have_schemas(self):
        catalog = discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema_dict = entry.schema.to_dict()
                self.assertEqual(schema_dict.get("type"), "object")
                self.assertIn("properties", schema_dict)

    def test_catalog_serializes_to_json(self):
        import json
        catalog = discover()
        dumped = json.dumps(catalog.to_dict())
        reloaded = json.loads(dumped)
        self.assertIn("streams", reloaded)
        self.assertEqual(len(reloaded["streams"]), len(ALL_EXPECTED_STREAMS))


# ── _apply_access_checks ──────────────────────────────────────────────────────

class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks() and _prune_inaccessible_children()."""

    def _fresh_dicts(self):
        schemas, field_metadata = get_schemas()
        return dict(schemas), dict(field_metadata)

    def _make_stream_cls(self, accessible=True, parent_stream=None):
        """Return a fake stream class object with .parent and .check_access() on its instance."""
        class FakeStreamCls:
            parent = parent_stream
            def __init__(self, client=None, catalog_entry=None):
                pass
            def check_access(self):
                return accessible
        return FakeStreamCls

    # --- All streams accessible -------------------------------------------

    def test_all_accessible_leaves_schemas_unchanged(self):
        schemas, field_metadata = self._fresh_dicts()
        expected_keys = set(schemas.keys())

        client = _mock_client()
        patched = {k: self._make_stream_cls(accessible=True, parent_stream=v.parent)
                   for k, v in STREAMS.items()}
        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertSetEqual(set(schemas.keys()), expected_keys)

    # --- One parent inaccessible ------------------------------------------

    def test_inaccessible_parent_removed_from_schemas(self):
        """Campaigns being inaccessible removes it (and its children) from schemas."""
        schemas, field_metadata = self._fresh_dicts()
        client = _mock_client()

        patched = {k: self._make_stream_cls(accessible=(k != "campaigns"),
                                            parent_stream=v.parent)
                   for k, v in STREAMS.items()}
        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            _apply_access_checks(client, schemas, field_metadata)

        self.assertNotIn("campaigns", schemas)
        # accounts and report streams are unaffected
        self.assertIn("accounts", schemas)
        for report in EXPECTED_REPORT_STREAMS:
            self.assertIn(report, schemas)

    def test_inaccessible_parent_children_also_removed(self):
        """
        Campaigns being inaccessible cascades: ad_groups and ads are also removed.
        accounts and report streams (parent=accounts) are unaffected.
        """
        schemas, field_metadata = self._fresh_dicts()
        client = _mock_client()

        patched = {k: self._make_stream_cls(accessible=(k != "campaigns"),
                                            parent_stream=v.parent)
                   for k, v in STREAMS.items()}
        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            _apply_access_checks(client, schemas, field_metadata)

        for removed in ("campaigns", "ad_groups", "ads"):
            with self.subTest(removed=removed):
                self.assertNotIn(removed, schemas,
                                 f"'{removed}' should be pruned when campaigns is inaccessible.")

    def test_inaccessible_parent_keeps_unrelated_streams(self):
        """Report streams are independent of campaigns being inaccessible."""
        schemas, field_metadata = self._fresh_dicts()
        client = _mock_client()

        patched = {k: self._make_stream_cls(accessible=(k != "campaigns"),
                                            parent_stream=v.parent)
                   for k, v in STREAMS.items()}
        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            _apply_access_checks(client, schemas, field_metadata)

        # Report streams have no parent, so they should be unaffected
        for report in EXPECTED_REPORT_STREAMS:
            with self.subTest(stream=report):
                self.assertIn(report, schemas)

    # --- Complete denial --------------------------------------------------

    def test_accounts_inaccessible_raises_forbidden_error(self):
        """
        When accounts is inaccessible every other stream (all children of accounts)
        is also pruned, leaving an empty catalog — which raises BingAdsForbiddenError.
        """
        schemas, field_metadata = self._fresh_dicts()
        client = _mock_client()

        patched = {k: self._make_stream_cls(accessible=(k != "accounts"),
                                            parent_stream=v.parent)
                   for k, v in STREAMS.items()}
        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            with self.assertRaises(BingAdsForbiddenError):
                _apply_access_checks(client, schemas, field_metadata)

    def test_all_inaccessible_raises_forbidden_error(self):
        schemas, field_metadata = self._fresh_dicts()
        client = _mock_client()

        patched = {k: self._make_stream_cls(accessible=False, parent_stream=v.parent)
                   for k, v in STREAMS.items()}
        with patch.dict("tap_bing_ads.discover.STREAMS", patched, clear=True):
            with self.assertRaises(BingAdsForbiddenError):
                _apply_access_checks(client, schemas, field_metadata)


# ── _prune_inaccessible_children ─────────────────────────────────────────────

class TestPruneInaccessibleChildren(unittest.TestCase):

    def _fake_cls(self, parent_stream):
        class Cls:
            parent = parent_stream
        return Cls

    def test_direct_child_pruned_when_parent_missing(self):
        schemas = {"campaigns": {}}
        field_metadata = {"campaigns": []}

        with patch.dict("tap_bing_ads.discover.STREAMS", {
            "accounts":  self._fake_cls(None),
            "campaigns": self._fake_cls("accounts"),
        }, clear=True):
            _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("campaigns", schemas)

    def test_grandchild_pruned_transitively(self):
        """ad_groups (child of campaigns) must also be pruned if campaigns is removed."""
        schemas = {"campaigns": {}, "ad_groups": {}}
        field_metadata = {"campaigns": [], "ad_groups": []}

        with patch.dict("tap_bing_ads.discover.STREAMS", {
            "accounts":  self._fake_cls(None),
            "campaigns": self._fake_cls("accounts"),
            "ad_groups": self._fake_cls("campaigns"),
        }, clear=True):
            _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("campaigns", schemas)
        self.assertNotIn("ad_groups", schemas)

    def test_unrelated_streams_untouched(self):
        """Streams with no parent, or whose parent is still present, are not removed."""
        schemas = {"accounts": {}, "keyword_performance_report": {}}
        field_metadata = {"accounts": [], "keyword_performance_report": []}

        with patch.dict("tap_bing_ads.discover.STREAMS", {
            "accounts":                    self._fake_cls(None),
            "keyword_performance_report":  self._fake_cls(None),
        }, clear=True):
            _prune_inaccessible_children(schemas, field_metadata)

        self.assertIn("accounts", schemas)
        self.assertIn("keyword_performance_report", schemas)


# ── discover() — with client access checks ───────────────────────────────────

class TestDiscoverWithClient(unittest.TestCase):

    def _make_accessible_streams(self, inaccessible=()):
        patched = {}
        for k, real_cls in STREAMS.items():
            accessible = k not in inaccessible
            par = real_cls.parent

            class FakeCls:
                parent = par
                _accessible = accessible
                def __init__(self, client=None, catalog_entry=None):
                    pass
                def check_access(self):
                    return self.__class__._accessible

            FakeCls._accessible = accessible
            FakeCls.parent = par
            patched[k] = FakeCls
        return patched

    def test_discover_with_all_accessible_returns_all_streams(self):
        client = _mock_client()
        patched = self._make_accessible_streams()
        with patch("tap_bing_ads.discover.STREAMS", patched):
            catalog = discover(client)
        found = {e.tap_stream_id for e in catalog.streams}
        self.assertSetEqual(found, ALL_EXPECTED_STREAMS)

    def test_discover_excludes_inaccessible_parent(self):
        """When campaigns is inaccessible, campaigns/ad_groups/ads are excluded."""
        client = _mock_client()
        patched = self._make_accessible_streams(inaccessible=("campaigns",))
        with patch("tap_bing_ads.discover.STREAMS", patched):
            catalog = discover(client)
        stream_ids = {e.tap_stream_id for e in catalog.streams}
        # campaigns and its children must be gone
        for removed in ("campaigns", "ad_groups", "ads"):
            with self.subTest(removed=removed):
                self.assertNotIn(removed, stream_ids)
        # accounts and reports must remain
        self.assertIn("accounts", stream_ids)
        for report in EXPECTED_REPORT_STREAMS:
            with self.subTest(report=report):
                self.assertIn(report, stream_ids)
