"""Campaigns stream - Campaign Management REST API."""
from typing import Dict, Optional
import singer
from tap_bing_ads.client import CAMPAIGN_BASE_URL
from tap_bing_ads.streams.abstracts import IncrementalStream

LOGGER = singer.get_logger()

CAMPAIGN_TYPES = "Search, Shopping, DynamicSearchAds"


class Campaigns(IncrementalStream):
    tap_stream_id = "campaigns"
    key_properties = ["Id", "account_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["accounts_LastModifiedTime"]
    valid_replication_keys = ["accounts_LastModifiedTime"]
    parent = "accounts"
    children = ["ad_groups"]
    data_key = "Campaigns"
    path = "Campaigns/QueryByAccountId"

    def get_url_endpoint(self, parent_obj: Optional[Dict] = None) -> str:
        self.url_endpoint = f"{CAMPAIGN_BASE_URL}/{self.path}"
        return super().get_url_endpoint(parent_obj)

    def check_access(self) -> bool:
        """
        Probe the campaigns endpoint using the first account_id from config as the
        parent AccountId.  A real account_id is required for a valid request.
        """
        from tap_bing_ads.exceptions import BingAdsForbiddenError
        account_id = self.client.config.get("account_ids", "").split(",")[0].strip()
        url = self.get_url_endpoint()
        # update_data_payload for Campaigns reads parent_obj.get("Id") as AccountId
        self.update_data_payload(parent_obj={"Id": account_id})
        try:
            self.client.make_request(
                method=self.http_method,
                url=url,
                json_body=self.data_payload,
                account_id=account_id or None,
            )
            return True
        except BingAdsForbiddenError as exc:
            LOGGER.warning(
                "Unauthorized stream: %s — excluding from catalog. Error: '%s'",
                self.tap_stream_id, str(exc),
            )
            return False

    def update_data_payload(self, parent_obj: Dict = None, **kwargs) -> Dict:
        """
        Constructs the JSON body payload for the API request.
        """
        account_id = str(parent_obj.get("Id", "")) if parent_obj else ""
        kwargs["AccountId"] = account_id
        kwargs["CampaignType"] = CAMPAIGN_TYPES
        return super().update_data_payload(parent_obj, **kwargs)

    def modify_object(self, record, parent_obj = None):
        record["account_id"] = parent_obj.get("Id", "") if parent_obj else ""
        record["accounts_LastModifiedTime"] = parent_obj.get("LastModifiedTime") if parent_obj else ""
        return super().modify_object(record, parent_obj)
