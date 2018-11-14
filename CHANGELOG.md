# Changelog

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
