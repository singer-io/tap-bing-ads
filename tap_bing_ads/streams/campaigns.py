"""Campaigns stream - Campaign Management REST API."""
from typing import Dict, Optional
import singer
from tap_bing_ads.client import CAMPAIGN_BASE_URL
from tap_bing_ads.streams.abstracts import IncrementalStream

LOGGER = singer.get_logger()

CAMPAIGN_TYPES = "Search, Shopping, DynamicSearchAds, Audience, PerformanceMax"


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
