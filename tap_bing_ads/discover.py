"""Discovery mode — build and emit the Singer catalog."""
import json
import sys

import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_bing_ads.schema import get_schemas
from tap_bing_ads.streams import STREAMS
from tap_bing_ads.exceptions import BingAdsForbiddenError

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """
    Remove child streams whose parent was excluded from the catalog.
    Mutates *schemas* and *field_metadata* in place and handles arbitrarily
    deep parent-child chains (grandchildren are also pruned).
    """
    pruned = True
    while pruned:
        pruned = False
        for name, stream_cls in list(STREAMS.items()):
            if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent stream '%s' "
                    "is not accessible.",
                    name,
                    stream_cls.parent,
                )
                schemas.pop(name, None)
                field_metadata.pop(name, None)
                pruned = True


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """
    Probe each parent stream for read access and remove inaccessible streams
    (plus their children) from *schemas* and *field_metadata* in place.

    Raises BingAdsForbiddenError if no parent streams remain accessible.
    """
    inaccessible_streams = []

    for name, stream_cls in STREAMS.items():
        if name not in schemas:
            continue
        if not stream_cls(client=client).check_access():
            inaccessible_streams.append(name)

    for name in inaccessible_streams:
        schemas.pop(name, None)
        field_metadata.pop(name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise BingAdsForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have " \
            "'read' access to any supported streams."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(inaccessible_streams),
        )


def discover(client=None) -> Catalog:
    """
    Build and return the Singer catalog.

    When *client* is provided, each parent stream is probed for read access
    and streams the credentials cannot access are excluded from the catalog.
    """
    schemas, field_metadata = get_schemas()

    if client is not None:
        _apply_access_checks(client, schemas, field_metadata)

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


def do_discover(client=None) -> None:
    """Emit the catalog to stdout."""
    LOGGER.info("Starting discovery.")
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Discovery complete.")
