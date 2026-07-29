#!/usr/bin/env python3
"""
tap-bing-ads — Singer tap for Microsoft Advertising (Bing Ads) REST API.

Replaces the legacy SOAP/SDK implementation with direct REST API calls
using OAuth 2.0 refresh-token authentication.
"""
import sys
import json

import singer
from singer import utils

from tap_bing_ads.client import Client
from tap_bing_ads.discover import do_discover
from tap_bing_ads.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "customer_id",
    "account_ids",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "developer_token",
]


@singer.utils.handle_top_exception(LOGGER)
def main() -> None:
    """Entry point for the tap."""
    parsed_args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = parsed_args.config
    config_path = parsed_args.config_path

    # Default end_date to today when not supplied
    if not config.get("end_date"):
        import arrow
        config["end_date"] = arrow.now().isoformat()

    with Client(config, config_path=config_path) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            state = parsed_args.state or {}
            sync(
                client=client,
                config=config,
                catalog=parsed_args.catalog,
                state=state,
            )
        else:
            LOGGER.info("No --catalog provided. Run with --discover first.")


if __name__ == "__main__":
    main()


