"""Ad Groups stream - Campaign Management REST API."""
from typing import Dict, Iterator, Optional
import singer
from tap_bing_ads.client import CAMPAIGN_BASE_URL
from tap_bing_ads.streams.abstracts import IncrementalStream

LOGGER = singer.get_logger()


class AdGroups(IncrementalStream):
    tap_stream_id = "ad_groups"
    key_properties = ["Id", "campaign_id", "account_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["campaigns_LastModifiedTime"]
    valid_replication_keys = ["campaigns_LastModifiedTime"]
    parent = "campaigns"
    children = ["ads"]
    path = "AdGroups/QueryByCampaignId"
    data_key = "AdGroups"

    def get_url_endpoint(self, parent_obj: Optional[Dict] = None) -> str:
        """
        Constructs the URL endpoint for the API request."""
        self.url_endpoint = f"{CAMPAIGN_BASE_URL}/{self.path}"
        return super().get_url_endpoint(parent_obj)

    def check_access(self) -> bool:
        """
        Ad groups require a CampaignId in the request which is only known at sync
        time — not at discovery.  Return True and rely on cascade: if the parent
        ``campaigns`` stream is inaccessible, ad_groups will be pruned automatically
        by ``_prune_inaccessible_children()`` in discover.py.
        """
        return True

    def update_data_payload(self, parent_obj: Dict = None, **kwargs) -> Dict:
        """
        Constructs the JSON body payload for the API request.
        """
        campaign_id = str(parent_obj.get("Id", "")) if parent_obj else ""
        kwargs["CampaignId"] = campaign_id
        return super().update_data_payload(parent_obj, **kwargs)

    def modify_object(self, record: Dict, parent_obj: Optional[Dict] = None) -> Dict:
        record["account_id"] = parent_obj.get("account_id", "") if parent_obj else ""
        record["campaign_id"] = parent_obj.get("Id", "") if parent_obj else ""
        record["campaigns_LastModifiedTime"] = parent_obj.get("accounts_LastModifiedTime") if parent_obj else ""
        return super().modify_object(record, parent_obj)
