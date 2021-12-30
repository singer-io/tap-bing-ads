# Changelog

## 2.20
  * Implement Timeout Request [#93](https://github.com/singer-io/tap-bing-ads/pull/93)
  * Added Top Level Breadcrumb [#91](https://github.com/singer-io/tap-bing-ads/pull/91)
  * Added Retry Logic for 500 errors [#90](https://github.com/singer-io/tap-bing-ads/pull/90)
  * Added required fields for age_gender_audience_report [#86](https://github.com/singer-io/tap-bing-ads/pull/86)
  * Removed automatic fields from ad_ext report [#85](https://github.com/singer-io/tap-bing-ads/pull/85)


## 2.1.0
  * Update the BingAds library to `13.0.11`

## 2.0.17
  * Remove null characters from incoming CSV [#78](https://github.com/singer-io/tap-bing-ads/pull/78)

## 2.0.16
  * Temporary fix for `wsdl_type_to_schema` [#71](https://github.com/singer-io/tap-bing-ads/pull/71)

## 2.0.15
  * Add Required Field for GoalsAndFunnels Report [#55](https://github.com/singer-io/tap-bing-ads/pull/55)
  * Update exclusions.py [#66](https://github.com/singer-io/tap-bing-ads/pull/66)
    * Adds several new fields to Attributes and ImpressionSharePerformanceStatistics for most reports.
    * Alphabetizes Attributes and ImpressionSharePerformanceStatistics objects in each report for easier updates / cross reference against Microsoft Documentation in the future.

## 2.0.14
  * Added optional `require_live_connect` config parameter [#63](https://github.com/singer-io/tap-bing-ads/pull/63)

## 2.0.13
  * Add exclusions for 3 fields missing from the Microsoft Ads documentation for the `ad_group_performance_report` [#58](https://github.com/singer-io/tap-bing-ads/pull/58)

## 2.0.12
  * Exclusions pattern update to include the second group of exclusions for `ad_group_performace_report` and `campaign_performance_report` mentioned in Microsoft's documentation above the exclusion tables. [#56](https://github.com/singer-io/tap-bing-ads/pull/56)

## 2.0.11
  * Adds `Goal` and `GoalType` as exclusions to the `AdGroupPerformanceReport` report [#53](https://github.com/singer-io/tap-bing-ads/pull/53)

## 2.0.10
  * Adds `TopImpressionRatePercent`, `TopImpressionShareLostToBudgetPercent`, `TopImpressionShareLostToRankPercent`, `TopImpressionSharePercent` to field exclusions [#51](https://github.com/singer-io/tap-bing-ads/pull/51)

## 2.0.9
  * Adds a CampaignType to the call to GetCampaignsByAccountId to bring back more than the default "Search" campaigns [#46](https://github.com/singer-io/tap-bing-ads/pull/46)

## 2.0.8
  * Updates bing-ads dependency to support v13 and implements the version migration [#37](https://github.com/singer-io/tap-bing-ads/pull/37)

## 2.0.7
  * Add logging when client is created, service message is sent, exception is caught [#41](https://github.com/singer-io/tap-bing-ads/pull/41)

## 2.0.6
  * Add retry up to 5 times for report retrieval [#39](https://github.com/singer-io/tap-bing-ads/pull/39)

## 2.0.5
  * Only attempt to resume prior requests from STATE if they are non-null [#38](https://github.com/singer-io/tap-bing-ads/pull/38/)

## 2.0.4
  * Add some simple retry logic to refresh stale `request_id`s [#33](https://github.com/singer-io/tap-bing-ads/pull/33)

## 2.0.3
  * Update field exclusion rules to match an API change [#30](https://github.com/singer-io/tap-bing-ads/pull/30)

## 2.0.2
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.0.1
  * Makes tap tolerant of `InvalidCustomDateRangeEnd` errors for reports. The tap will now just move past the ranges that receive this error. [#29](https://github.com/singer-io/tap-bing-ads/pull/29)

## 2.0.0
  * Adds support for v12 of the Bing API as v11 is sunset as of Oct 31 2018 [#28](https://github.com/singer-io/tap-bing-ads/pull/28)

## 1.2.0
  * Add `ad_extension_detail_report` stream [#27](https://github.com/singer-io/tap-bing-ads/pull/27)

## 1.1.0
  * Add `audience_performance_report` stream [#26](https://github.com/singer-io/tap-bing-ads/pull/26)

## 1.0.3
  * Handle AdApiFault in addition to ApiFault

## 1.0.2
  * Provide suds error messages to the tap so that we can see what went wrong [#21](https://github.com/singer-io/tap-bing-ads/pull/21)

## 1.0.1
  * Generate proper JSON schema for elements of type `unsignedByte` [#20](https://github.com/singer-io/tap-bing-ads/pull/20)

## 1.0.0
  * Initial Release to Stitch platform for production

## 0.4.0
  * Adds checking for stale request IDs [#18](https://github.com/singer-io/tap-bing-ads/pull/18)
  * Updates handling of incremental report downloading to check status and response URL [#19](https://github.com/singer-io/tap-bing-ads/pull/19)

## 0.3.0
  * Adds a feature to load 30 days of report data and skip syncing core streams unless they are selected [#15](https://github.com/singer-io/tap-bing-ads/pull/15)

## 0.2.0
  * Adds a feature to save an incomplete reports' request in state [#14](https://github.com/singer-io/tap-bing-ads/pull/14)

## 0.1.0
  * Adds asyncio support for concurrent polling of reports [#13](https://github.com/singer-io/tap-bing-ads/pull/13)

## 0.0.6
  * Adds CampaignPerformanceReport and GoalsAndFunnelsReport reports [#12](https://github.com/singer-io/tap-bing-ads/pull/12)
