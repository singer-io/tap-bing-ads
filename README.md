# tap-bing-ads

[Singer](https://www.singer.io/) tap for extracting data from the [Microsoft Advertising (Bing Ads) REST API v13](https://docs.microsoft.com/en-us/advertising/guides/).

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Streams](#streams)
  - [Core Entity Streams](#core-entity-streams)
  - [Report Streams](#report-streams)
- [Report Generation & Polling](#report-generation--polling)
- [Report Fields](#report-fields)
- [Field Exclusions](#field-exclusions)
- [Replication](#replication)
- [State](#state)

---

## Features

- Pure REST API implementation — no Bing Ads SDK dependency.
- OAuth 2.0 refresh-token authentication with automatic token refresh.
- Streams unauthorized (HTTP 403) streams are silently excluded from the catalog during discovery instead of failing.
- Asynchronous report generation with polling and CSV download.
- Configurable date windows and conversion window for reports.
- Automatic retry with back-off on rate limits (HTTP 429) and transient server errors.

---

## Requirements

- Python 3.12+
- A Microsoft Advertising account with API access
- OAuth 2.0 credentials (client ID, client secret, refresh token)
- A Developer Token from the [Microsoft Advertising Developer Portal](https://developers.ads.microsoft.com/)

---

## Installation

```bash
pip install tap-bing-ads
```

Or from source:

```bash
git clone https://github.com/singer-io/tap-bing-ads.git
cd tap-bing-ads
pip install -e .
```

---

## Configuration

Create a `config.json` file with the following keys:

```json
{
  "oauth_client_id": "YOUR_CLIENT_ID",
  "oauth_client_secret": "YOUR_CLIENT_SECRET",
  "refresh_token": "YOUR_REFRESH_TOKEN",
  "developer_token": "YOUR_DEVELOPER_TOKEN",
  "customer_id": "YOUR_CUSTOMER_ID",
  "account_ids": "ACCOUNT_ID_1,ACCOUNT_ID_2",
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-12-31T00:00:00Z",
  "report_max_days": 30,
  "conversion_window": -30,
  "request_timeout": 300
}
```

| Key | Required | Description |
|-----|----------|-------------|
| `oauth_client_id` | Yes | OAuth 2.0 application client ID |
| `oauth_client_secret` | Yes | OAuth 2.0 application client secret |
| `refresh_token` | Yes | OAuth 2.0 refresh token |
| `developer_token` | Yes | Microsoft Advertising developer token |
| `customer_id` | Yes | Microsoft Advertising customer (manager) ID |
| `account_ids` | Yes | Comma-separated list of advertiser account IDs to sync |
| `start_date` | Yes | Earliest date to sync, ISO 8601 format |
| `end_date` | No | Latest date to sync (defaults to today) |
| `report_max_days` | No | Max days per report request window (default: `30`) |
| `conversion_window` | No | Days back from today to re-sync for conversion attribution (default: `-30`) |
| `request_timeout` | No | HTTP request timeout in seconds (default: `300`) |

---

## Usage

**Discovery** (generate a catalog):

```bash
tap-bing-ads -c config.json --discover > catalog.json
```

**Sync** (stream data):

```bash
tap-bing-ads -c config.json --catalog catalog.json --state state.json
```

**Select streams** by editing `catalog.json` and setting `"selected": true` in the stream's metadata, then run the sync command above.

---

## Streams

### Core Entity Streams

These streams use the Microsoft Advertising Customer Management and Campaign Management REST APIs. They are synced incrementally using the `LastModifiedTime` field.

| Stream | Tap Stream ID | Primary Keys | Replication Key | API |
|--------|---------------|--------------|-----------------|-----|
| Accounts | `accounts` | `Id` | `LastModifiedTime` | [Customer Management](https://learn.microsoft.com/en-us/advertising/customer-management-service/getaccount?view=bingads-13&tabs=prod&pivots=rest) |
| Campaigns | `campaigns` | `Id`, `account_id` | `accounts_LastModifiedTime` | [Campaign Management](https://learn.microsoft.com/en-us/advertising/campaign-management-service/getcampaignsbyaccountid?view=bingads-13&tabs=prod&pivots=rest) |
| Ad Groups | `ad_groups` | `Id`, `account_id`, `campaign_id` | `campaigns_LastModifiedTime` | [Campaign Management](https://learn.microsoft.com/en-us/advertising/campaign-management-service/getadgroupsbycampaignid?view=bingads-13&tabs=prod&pivots=rest) |
| Ads | `ads` | `Id`, `account_id`, `ad_group_id` | `ad_groups_LastModifiedTime` | [Campaign Management](https://learn.microsoft.com/en-us/advertising/campaign-management-service/getadsbyadgroupid?view=bingads-13&tabs=prod&pivots=rest) |

**Hierarchy:** `accounts → campaigns → ad_groups → ads`

Child streams are driven by their parent — selecting `ad_groups` will automatically sync `accounts` and `campaigns` first.

---

### Report Streams

Report streams use the Microsoft Advertising Reporting REST API. Each report is generated asynchronously: the tap submits a report request, polls for completion, downloads a ZIP file, and streams the CSV rows as Singer records.

All report streams are **INCREMENTAL**, bookmarked on `TimePeriod` (daily aggregation).

| Stream | Tap Stream ID | Required Fields | Primary Keys |
|--------|---------------|-----------------|--------------|
| Keyword Performance | `keyword_performance_report` | `TimePeriod`, `AccountId` | `TimePeriod`, `AccountId`, `KeywordId`, `AdId`, `Network`, `TopVsOther`, `DeviceType`, `DeliveredMatchType` |
| Ad Performance | `ad_performance_report` | `TimePeriod`, `AccountId` | `TimePeriod`, `AccountId`, `AdId`, `AdGroupId`, `Network`, `TopVsOther`, `DeviceType`, `DeliveredMatchType` |
| Ad Group Performance | `ad_group_performance_report` | `TimePeriod`, `AccountId` | `TimePeriod`, `AccountId`, `AdGroupId`, `Network`, `TopVsOther`, `DeviceType`, `DeliveredMatchType` |
| Campaign Performance | `campaign_performance_report` | `TimePeriod`, `AccountId` | `TimePeriod`, `AccountId`, `CampaignId`, `Network`, `TopVsOther`, `DeviceType`, `DeliveredMatchType` |
| Geographic Performance | `geographic_performance_report` | `TimePeriod`, `AccountId`, `AccountName` | `TimePeriod`, `AccountId`, `AdGroupId`, `Country`, `State`, `MetroArea`, `City` |
| Age Gender Audience | `age_gender_audience_report` | `TimePeriod`, `AccountId`, `AccountName`, `AdGroupName`, `AgeGroup`, `Gender` | `TimePeriod`, `AccountId`, `AdGroupId`, `AgeGroup`, `Gender` |
| Search Query Performance | `search_query_performance_report` | `TimePeriod`, `AccountId`, `SearchQuery` | `TimePeriod`, `AccountId`, `AdGroupId`, `KeywordId`, `SearchQuery` |
| Goals and Funnels | `goals_and_funnels_report` | `TimePeriod`, `AccountId`, `Goal` | `TimePeriod`, `AccountId`, `AdGroupId`, `GoalId`, `DeviceType` |
| Audience Performance | `audience_performance_report` | `TimePeriod`, `AccountId`, `AudienceId` | `TimePeriod`, `AccountId`, `AdGroupId`, `AudienceId` |
| Ad Extension Detail | `ad_extension_detail_report` | `TimePeriod`, `AccountId`, `AdExtensionId`, `AdExtensionPropertyValue`, `AdExtensionType`, `AdExtensionTypeId` | `TimePeriod`, `AccountId`, `AdExtensionId`, `AdExtensionType` |

---

## Report Generation & Polling

Report streams follow a 3-step async flow:

```
1. Submit  →  POST /Reporting/v13/GenerateReport/Submit
                 Body: JSON report request with columns, date range, account ID
                 Response: { "ReportRequestId": "abc-123" }

2. Poll    →  POST /Reporting/v13/GenerateReport/Poll
                 Body: { "ReportRequestId": "abc-123" }
                 Response: { "ReportRequestStatus": { "Status": "Pending|Success|Error", "ReportDownloadUrl": "..." } }
                 ↻ Retries every 5 seconds, up to ~2 hours (1440 polls)

3. Download → GET <ReportDownloadUrl>
                 Response: ZIP file containing a CSV report
                 The tap extracts the CSV, parses each row, and emits Singer RECORD messages
```

**Date windowing:** The full date range (`start_date` → `end_date`) is split into windows of `report_max_days` days. Each window generates one report request. The `conversion_window` setting extends lookback from today by that many days to re-fetch recently attributed conversions.

**Request ID resumption:** If a sync is interrupted, the `ReportRequestId` is stored in state. On the next run the tap resumes polling the existing request instead of submitting a new one.

**Timeout:** If polling exceeds ~2 hours the tap logs a warning, skips the window, and continues to the next one.

**Error handling:**

| Condition | Behaviour |
|-----------|-----------|
| `Status: Error` from poll | Raises `BingAdsReportError` — window is skipped |
| Date range outside API retention | `BingAdsInvalidDateRangeEnd` — window is skipped |
| No metric column selected | `BingAdsNoMeasureSelected` — stream is skipped |
| Mutually exclusive fields selected | `BingAdsInvalidFieldSelection` — stream is skipped |

---

## Report Fields

Every report record includes these tap-added metadata fields regardless of column selection:

| Field | Type | Description |
|-------|------|-------------|
| `_sdc_report_datetime` | string (ISO 8601) | UTC timestamp when this report was downloaded |
| `TimePeriod` | datetime | Report date (daily aggregation) |
| `AccountId` | integer | Microsoft Advertising account ID |

### Column Types

Columns are typed as follows (all others default to `string`):

| Type | Example Columns |
|------|----------------|
| `integer` | `Clicks`, `Impressions`, `AccountId`, `AdGroupId`, `CampaignId`, `KeywordId`, `AdId`, `Assists`, `EstimatedClicks`, `EstimatedImpressions`, `LowQualityClicks`, `ManualCalls`, `PhoneCalls`, `PhoneImpressions` |
| `number` | `Spend`, `Ctr`, `AverageCpc`, `AverageCpp`, `AveragePosition`, `ConversionRate`, `Conversions`, `CostPerConversion`, `Revenue`, `ReturnOnAdSpend`, `ImpressionSharePercent`, `AbsoluteTopImpressionSharePercent`, `ClickSharePercent`, `AllConversions`, `ViewThroughConversions` |
| `datetime` | `TimePeriod` |

### Metric (Measure) Columns

At least one of the following **metric columns must be selected** per report, otherwise the Microsoft Advertising API rejects the request:

`Clicks`, `Impressions`, `Ctr`, `Spend`, `Conversions`, `ConversionRate`, `Revenue`, `Assists`, `ReturnOnAdSpend`, `AverageCpc`, `AverageCpm`, `AverageCpp`, `LowQualityClicks`, `PhoneCalls`, `AllConversions`, `AllRevenue`, `ViewThroughConversions`

> **Note:** `GoalsAndFunnelsReport` does not have a `Clicks` column. It uses `AllConversions` as its required measure column instead.

---

## Field Exclusions

Microsoft Advertising enforces mutual exclusion rules between **Attribute** columns and **Impression Share Performance Statistics** columns. Selecting a field from one group together with a field from the other group in the same report will cause the API to reject the request.

The tap raises `BingAdsInvalidFieldSelection` if conflicting fields are detected and skips the stream.

The rules below apply per report. For the full list see [`tap_bing_ads/exclusions.py`](tap_bing_ads/exclusions.py) and the [Microsoft Advertising documentation](https://docs.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#columnrestrictions).

### Common Exclusion Pattern

For most reports, the following **Attribute** columns **cannot** be selected together with any **Impression Share** column:

| Cannot combine | With any of |
|---------------|-------------|
| `BidMatchType`, `DeviceOS`, `Goal`, `GoalType`, `TopVsOther` | `AbsoluteTopImpressionSharePercent`, `AbsoluteTopImpressionShareLostToBudgetPercent`, `AbsoluteTopImpressionShareLostToRankPercent`, `AudienceImpressionSharePercent`, `AudienceImpressionLostToBudgetPercent`, `AudienceImpressionLostToRankPercent`, `ClickSharePercent`, `ExactMatchImpressionSharePercent`, `ImpressionSharePercent`, `ImpressionLostToBudgetPercent`, `ImpressionLostToRankAggPercent`, `ImpressionLostToRankPercent`, `RelativeCtr`, `TopImpressionSharePercent`, `TopImpressionShareLostToBudgetPercent`, `TopImpressionShareLostToRankPercent` |
| `CustomerId`, `CustomerName`, `DeliveredMatchType` | `AudienceImpressionSharePercent`, `AudienceImpressionLostToBudgetPercent`, `AudienceImpressionLostToRankPercent`, `RelativeCtr` |

### Reports with Exclusion Rules

The following reports have field exclusion rules defined:

- `AccountPerformanceReport`
- `AdGroupPerformanceReport`
- `AdPerformanceReport`
- `CampaignPerformanceReport`
- `GeographicPerformanceReport`
- `KeywordPerformanceReport`
- `SearchQueryPerformanceReport`

> Reports **without** exclusion rules: `AgeGenderAudienceReport`, `GoalsAndFunnelsReport`, `AudiencePerformanceReport`, `AdExtensionDetailReport`.

### `fieldExclusions` Metadata Key

Each field in the Singer catalog carries a `fieldExclusions` metadata key listing the other fields it cannot be selected with. Destination tools and Stitch use this to enforce valid column combinations.

---

## Replication

| Stream type | Method | Bookmark field |
|-------------|--------|----------------|
| `accounts` | INCREMENTAL | `LastModifiedTime` |
| `campaigns` | INCREMENTAL | `accounts_LastModifiedTime` |
| `ad_groups` | INCREMENTAL | `campaigns_LastModifiedTime` |
| `ads` | INCREMENTAL | `ad_groups_LastModifiedTime` |
| All report streams | INCREMENTAL | `TimePeriod` (per account) |

---

## State

State is stored per stream per account. Example:

```json
{
  "bookmarks": {
    "accounts": { "LastModifiedTime": "2024-06-01T00:00:00Z" },
    "67890_campaign_performance_report": {
      "date": "2024-06-01",
      "request_id": null
    }
  }
}
```

Pass state to the tap with `--state state.json`. The tap writes updated state to stdout as `STATE` messages after each sync window.

---

Copyright &copy; 2017 Stitch

