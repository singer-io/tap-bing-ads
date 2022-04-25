# tap-bing-ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- `tap-bing-ads` works together with any other [Singer Target](https://singer.io) to move data from BingAds API to any target destination.
- Extracts the following resources from HubSpot
  - [Accounts](https://docs.microsoft.com/en-us/advertising/customer-management-service/getaccount?view=bingads-13)
  - [Campaigns](https://docs.microsoft.com/en-us/advertising/campaign-management-service/getcampaignsbyaccountid?view=bingads-13)
  - [Adgroups](https://docs.microsoft.com/en-us/advertising/campaign-management-service/getadgroupsbycampaignid?view=bingads-13)
  - [Ads](https://docs.microsoft.com/en-us/advertising/campaign-management-service/getadsbyadgroupid?view=bingads-13)
  - [Reporting](https://docs.microsoft.com/en-us/advertising/reporting-service/reporting-service-reference?view=bingads-13)
    - [Ad Extension Detail Report](https://docs.microsoft.com/en-us/advertising/reporting-service/adextensiondetailreportrequest?view=bingads-13)
    - [Ad Group Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/adgroupperformancereportrequest?view=bingads-13)
    - [Ad Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/adperformancereportrequest?view=bingads-13)
    - [Age Gender Audience Report](https://docs.microsoft.com/en-us/advertising/reporting-service/agegenderaudiencereportrequest?view=bingads-13)
    - [Audience Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/audienceperformancereportrequest?view=bingads-13)
    - [Campaign Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/campaignperformancereportrequest?view=bingads-13)
    - [Geographic Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/geographicperformancereportrequest?view=bingads-13)
    - [Goals and Funnels Report](https://docs.microsoft.com/en-us/advertising/reporting-service/goalsandfunnelsreportrequest?view=bingads-13)
    - [Keyword Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/keywordperformancereportrequest?view=bingads-13)
    - [Search Query Performance Report](https://docs.microsoft.com/en-us/advertising/reporting-service/searchqueryperformancereportrequest?view=bingads-13)

## Configuration

This tap requires a `config.json` which specifies details regarding [OAuth 2.0](https://docs.microsoft.com/en-us/advertising/guides/authentication-oauth?view=bingads-13) authentication and a cutoff date for syncing historical data. See [config.sample.json](config.sample.json) for an example.

To run the discover mode of `tap-bing-ads` with the configuration file, use this command:

```bash
$ tap-bing-ads -c my-config.json -d
```

To run he sync mode of `tap-bing-ads` with the catalog file, use the command:

```bash
$ tap-bing-ads -c my-config.json --catalog catalog.json
```

## Metadata Reference

tap-bing-ads uses some custom metadata keys:

* `fieldExclusions` - Indicates which other fields may not be selected when this field is selected. If you invoke the tap with selections that violate fieldExclusion rules, it is likely that the tap will fail.

---

Copyright &copy; 2017 Stitch
