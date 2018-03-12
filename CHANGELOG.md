# Changelog

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
