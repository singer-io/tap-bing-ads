from typing import Dict, List

import singer
from tap_bing_ads.client import Client
from tap_bing_ads.streams import STREAMS, REPORT_STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """
    Update currently_syncing in state and write it
    """
    if not stream_name and "currently_syncing" in state:
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
        if child in streams_to_sync or any(_is_ancestor(child, s, STREAMS) for s in streams_to_sync):
            stream.child_to_sync.append(child_obj)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state: Dict) -> None:
    """
    Main sync entry point.

    Top-level core stream (accounts) cascades to campaigns → ad_groups → ads automatically.
    Report streams run per account.
    Syncing is done in the order of streams in the catalog, so that parent streams are synced before child streams.
    """
    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info("selected_streams: {}".format(streams_to_sync))

    account_ids: List[str] = [a.strip() for a in config["account_ids"].split(",")]

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            # Reports are synced independently per configured account below.
            # They do not need the accounts stream added to the core stream graph.
            if stream_name in REPORT_STREAMS:
                continue

            stream = STREAMS[stream_name](client, catalog.get_stream(stream_name))
            if stream.parent:
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            write_schema(stream, client, streams_to_sync, catalog)
            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(state=state, transformer=transformer)
            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: {}, total_records: {}".format(
                    stream_name, total_records
                )
            )
        # ----------------------------------------------------------------
        # Report streams — run per account, unchanged
        # ----------------------------------------------------------------
        for account_id in account_ids:
            LOGGER.info("--- Report streams for account: %s ---", account_id)
            for stream_name in streams_to_sync:
                entry = catalog.get_stream(stream_name)
                if stream_name not in REPORT_STREAMS:
                    continue

                update_currently_syncing(state, stream_name)
                LOGGER.info("Syncing report %s for account %s", stream_name, account_id)

                stream_obj = STREAMS[stream_name](client, entry)
                total_records = stream_obj.sync(state=state, account_id=account_id, config=config)
                update_currently_syncing(state, None)
                LOGGER.info(
                    "FINISHED Syncing: {}, total_records: {}".format(
                        stream_name, total_records
                    )
                )

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
