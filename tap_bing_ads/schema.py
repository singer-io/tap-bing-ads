"""
Schema loading utilities for tap-bing-ads.

Loads JSON schema files from the ``schemas/`` directory and assembles
Singer catalog metadata using stream-level configuration defined on each
stream class.
"""
import json
import os
from typing import Dict, Tuple

import singer
from singer import metadata

from tap_bing_ads.exclusions import EXCLUSIONS
from tap_bing_ads.streams import STREAMS
from tap_bing_ads.reports import (
    REPORT_REQUIRED_FIELDS,
    REPORT_SPECIFIC_REQUIRED_FIELDS,
    METRIC_COLUMNS
)

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema_references() -> Dict:
    """
    Load the schema files from the schema folder and return the schema references.
    """
    shared_schema_path = get_abs_path("schemas/shared")

    shared_file_names = []
    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f
            for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs

def get_report_required_fields(report_name: str):
    base = list(REPORT_REQUIRED_FIELDS)
    extra = REPORT_SPECIFIC_REQUIRED_FIELDS.get(report_name, [])
    metric_fields = list(METRIC_COLUMNS)
    return base + extra + metric_fields

def _build_report_metadata(stream_cls, schema: Dict) -> list:
    """Build Singer metadata for a report stream with fieldExclusions."""
    report_name = getattr(stream_cls, "report_name", "")
    required_fields = get_report_required_fields(report_name)
    exclusion_rules = EXCLUSIONS.get(report_name, [])

    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema,
        key_properties=stream_cls.key_properties,
        valid_replication_keys=stream_cls.replication_keys or [],
        replication_method=stream_cls.replication_method,
    )
    mdata = metadata.to_map(mdata)

    # Mark replication key as automatic
    for key in (stream_cls.replication_keys or []):
        mdata = metadata.write(mdata, ("properties", key), "inclusion", "automatic")

    # Mark parent-tap-stream-id
    if getattr(stream_cls, "parent", None):
        mdata = metadata.write(mdata, (), "parent-tap-stream-id", stream_cls.parent)

    for prop in schema.get("properties", {}):
        if prop in required_fields:
            mdata = metadata.write(mdata, ("properties", prop), "inclusion", "automatic")

        # Field exclusions
        for rule in exclusion_rules:
            attrs = rule.get("Attributes", [])
            share_stats = rule.get("ImpressionSharePerformanceStatistics", [])

            if prop in attrs:
                current = list(metadata.get(mdata, ("properties", prop), "fieldExclusions") or [])
                current += [["properties", p] for p in share_stats]
                mdata = metadata.write(mdata, ("properties", prop), "fieldExclusions", current)

            if prop in share_stats:
                current = list(metadata.get(mdata, ("properties", prop), "fieldExclusions") or [])
                current += [["properties", p] for p in attrs]
                mdata = metadata.write(mdata, ("properties", prop), "fieldExclusions", current)

    return metadata.to_list(mdata)

def _build_core_metadata(stream_cls, schema: Dict) -> list:
    """Build Singer metadata for a core entity stream."""
    mdata = metadata.new()
    mdata = metadata.get_standard_metadata(
        schema=schema,
        key_properties=stream_cls.key_properties,
        valid_replication_keys=stream_cls.replication_keys or [],
        replication_method=stream_cls.replication_method,
    )
    mdata = metadata.to_map(mdata)

    for key in (stream_cls.replication_keys or []):
        mdata = metadata.write(mdata, ("properties", key), "inclusion", "automatic")

    if getattr(stream_cls, "parent", None):
        mdata = metadata.write(mdata, (), "parent-tap-stream-id", stream_cls.parent)

    return metadata.to_list(mdata)

def get_schemas() -> Tuple[Dict, Dict]:
    """
    Load all stream schemas and build metadata.

    Returns:
        (schemas, field_metadata) — dicts keyed by tap_stream_id.
    """
    from tap_bing_ads.streams import REPORT_STREAMS

    schemas = {}
    field_metadata = {}
    refs = load_schema_references()

    for stream_name, stream_cls in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path) as file:
            raw_schema = json.load(file)
        schemas[stream_name] = raw_schema

        resolved_schema = singer.resolve_schema_references(raw_schema, refs)

        schemas[stream_name] = resolved_schema

        if stream_name in REPORT_STREAMS:
            mdata = _build_report_metadata(stream_cls, resolved_schema)
        else:
            mdata = _build_core_metadata(stream_cls, resolved_schema)

        field_metadata[stream_name] = mdata

    return schemas, field_metadata
