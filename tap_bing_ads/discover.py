"""Discovery mode — build and emit the Singer catalog."""
import json
import sys

import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_bing_ads.schema import get_schemas

LOGGER = singer.get_logger()


def discover() -> Catalog:
    """Build and return the Singer catalog."""
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as exc:
            LOGGER.error("Error building catalog entry for %s: %s", stream_name, exc)
            raise

        from singer import metadata as md
        key_properties = md.to_map(mdata).get((), {}).get("table-key-properties", [])

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog


def do_discover() -> None:
    """Emit the catalog to stdout."""
    LOGGER.info("Starting discovery.")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Discovery complete.")
