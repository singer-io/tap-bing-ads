from typing import Dict, List

import singer
# from singer import Transformer, metadata, set_currently_syncing, get_currently_syncing, write_state

from tap_bing_ads.client import Client
from tap_bing_ads.streams import STREAMS, REPORT_STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """
    Write schema for stream and its children
    """
    if stream.is_selected():
        stream.write_schema()

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state: Dict) -> None:
    """
    Main sync entry point.

    Top-level core stream (accounts) cascades to campaigns → ad_groups → ads automatically.
    Report streams run per account.
    """
    selected_streams = [s.stream for s in catalog.get_selected_streams(state)]

    # Only top-level streams (no parent) are driven directly;
    # children are wired up inside write_schema via child_to_sync.
    top_level_streams = [name for name in selected_streams if STREAMS[name].parent is None]

    account_ids: List[str] = [a.strip() for a in config["account_ids"].split(",")]

    LOGGER.info("Selected streams: %s", selected_streams)
    LOGGER.info("Top-level streams to sync: %s", top_level_streams)

    with singer.Transformer() as transformer:
        for stream_name in top_level_streams:
            stream = STREAMS[stream_name](client, catalog.get_stream(stream_name))
            write_schema(stream, client, selected_streams, catalog)

            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(state=state, transformer=transformer)
            update_currently_syncing(state, None)
            LOGGER.info("FINISHED Syncing: %s, total_records: %s", stream_name, total_records)

        # ----------------------------------------------------------------
        # Report streams — run per account, unchanged
        # ----------------------------------------------------------------
        for account_id in account_ids:
            LOGGER.info("--- Report streams for account: %s ---", account_id)
            for entry in catalog.get_selected_streams(state):
                stream_name = entry.stream
                if stream_name not in REPORT_STREAMS:
                    continue

                update_currently_syncing(state, stream_name)
                LOGGER.info("Syncing report %s for account %s", stream_name, account_id)

                stream_obj = STREAMS[stream_name](client, entry)
                total = stream_obj.sync(state=state, account_id=account_id, config=config)
                LOGGER.info("Finished %s for account %s. Records: %d", stream_name, account_id, total)
                update_currently_syncing(state, None)

    update_currently_syncing(state, None)
    LOGGER.info("Sync complete.")


def _is_ancestor(ancestor_name: str, descendant_name: str, streams_map: Dict) -> bool:
    """Return True if ancestor_name is a transitive parent of descendant_name."""
    cls = streams_map.get(descendant_name)
    if cls is None:
        return False
    parent = cls.parent
    if parent is None:
        return False
    if parent == ancestor_name:
        return True
    return _is_ancestor(ancestor_name, parent, streams_map)
