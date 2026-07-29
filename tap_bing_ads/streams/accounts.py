"""Accounts stream - Customer Management REST API."""
from typing import Dict, Iterator
import singer
from tap_bing_ads.client import CUSTOMER_BASE_URL
from tap_bing_ads.streams.abstracts import IncrementalStream

LOGGER = singer.get_logger()


class Accounts(IncrementalStream):
    tap_stream_id = "accounts"
    key_properties = ["Id"]
    replication_keys = ["LastModifiedTime"]
    valid_replication_keys = ["LastModifiedTime"]
    children = ["campaigns"]
    data_key = "Account"
    path = "Account/Query"

    def get_url_endpoint(self, parent_obj = None):
        self.url_endpoint = f"{CUSTOMER_BASE_URL}/{self.path}"
        return super().get_url_endpoint(parent_obj)

    def get_records(self, parent_obj: Dict = None) -> Iterator[Dict]:
        """Yield one account record per configured account ID."""
        for account_id in self.client.config.get("account_ids", "").split(","):
            account_id = account_id.strip()
            if not account_id:
                continue
            self.update_data_payload(parent_obj, AccountId=account_id)
            response = self.client.make_request(
                method=self.http_method,
                url=self.url_endpoint,
                json_body=self.data_payload,
                account_id=account_id
            )
            account = response.get(self.data_key) or response
            if account:
                yield account
