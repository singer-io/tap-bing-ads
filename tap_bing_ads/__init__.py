#!/usr/bin/env python3
"""
tap-bing-ads — Singer tap for Microsoft Advertising (Bing Ads).

REST (OAuth 2.0 refresh-token authentication) is the default transport and
the foundation for new development. The legacy SOAP/SDK implementation
(tap_bing_ads/soap_tap.py) remains available behind the `api_type: "soap"`
config option for existing connections, since Microsoft continues to support
the Bing Ads SOAP API - and ship new features to both SOAP and REST - until
it is retired on 2027-01-31. New streams/fields should be added to REST
first and backported to SOAP only while it remains supported.
"""
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

# Bing Ads SOAP API sunset date announced by Microsoft. Once this date has
# passed, the `soap` api_type and tap_bing_ads/soap_tap.py can be removed.
SOAP_SUNSET_DATE = "2027-01-31"

VALID_API_TYPES = ("rest", "soap")
DEFAULT_API_TYPE = "rest"


def get_api_type(config: dict) -> str:
    """Resolve and validate the configured `api_type` (defaults to REST)."""
    api_type = str(config.get("api_type") or DEFAULT_API_TYPE).strip().lower()
    if api_type not in VALID_API_TYPES:
        raise ValueError(
            "Invalid 'api_type' config value: '{}'. Must be one of {}.".format(
                api_type, VALID_API_TYPES
            )
        )
    return api_type


def run_soap(config: dict, state: dict, catalog, is_discover: bool) -> None:
    """Dispatch discovery/sync to the legacy SOAP implementation."""
    LOGGER.warning(
        "api_type='soap' is configured. The Bing Ads SOAP API is scheduled "
        "to be retired on %s; new connections should use the REST default "
        "(omit 'api_type' or set it to 'rest').",
        SOAP_SUNSET_DATE,
    )
    try:
        from tap_bing_ads import soap_tap
    except ImportError as exc:
        raise ImportError(
            "api_type='soap' requires the optional SOAP dependencies "
            "(bingads, suds-community). Install them with: "
            "pip install 'tap-bing-ads[soap]'"
        ) from exc

    if is_discover:
        soap_tap.run_discover(config)
    else:
        soap_tap.run_sync(config, state, catalog)


def run_rest(config: dict, config_path, state: dict, catalog, is_discover: bool) -> None:
    """Dispatch discovery/sync to the REST implementation (default)."""
    # Default end_date to today when not supplied
    if not config.get("end_date"):
        import arrow
        config["end_date"] = arrow.now().isoformat()

    with Client(config, config_path=config_path) as client:
        if is_discover:
            do_discover(client)
        else:
            sync(client=client, config=config, catalog=catalog, state=state)


@singer.utils.handle_top_exception(LOGGER)
def main() -> None:
    """
    Entry point for the tap.

    Dispatches to the REST implementation by default, or to the legacy SOAP
    implementation when the connection is configured with `api_type: soap`.
    """
    parsed_args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = parsed_args.config
    state = parsed_args.state or {}
    api_type = get_api_type(config)

    if not parsed_args.discover and not parsed_args.catalog:
        LOGGER.info("No --catalog provided. Run with --discover first.")
        return

    if api_type == "soap":
        run_soap(config, state, parsed_args.catalog, parsed_args.discover)
    else:
        run_rest(config, parsed_args.config_path, state, parsed_args.catalog, parsed_args.discover)


if __name__ == "__main__":
    main()

