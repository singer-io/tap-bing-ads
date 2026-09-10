"""
Report column metadata for Microsoft Advertising reporting streams.

REPORTING_FIELD_TYPES maps column names to their JSON Schema types.
Columns not listed here default to 'string'.

METRIC_COLUMNS is the set of columns that count as measure/metric fields.
Microsoft Advertising requires at least one metric column per report request.

REPORT_REQUIRED_COLUMNS are always added to every report request regardless
of user selection (needed for bookmarking and record identification).

REPORT_SPECIFIC_REQUIRED_COLUMNS maps report names to their additional
required columns.
"""

# Backwards-compatibility aliases (used by existing integration tests)
REPORT_WHITELIST = [
    'KeywordPerformanceReport',
    'AdPerformanceReport',
    'AdGroupPerformanceReport',
    'GeographicPerformanceReport',
    'AgeGenderAudienceReport',
    'SearchQueryPerformanceReport',
    'CampaignPerformanceReport',
    'GoalsAndFunnelsReport',
    'AudiencePerformanceReport',
    'AdExtensionDetailReport',
]

REPORT_REQUIRED_FIELDS = ['_sdc_report_datetime', 'AccountId', 'TimePeriod']
REPORT_REQUIRED_COLUMNS = REPORT_REQUIRED_FIELDS

REPORT_SPECIFIC_REQUIRED_FIELDS = {
    'GeographicPerformanceReport': ['AccountName'],
    'AgeGenderDemographicReport': [
        'AccountName',
        'AdGroupName',
        'AgeGroup',
        'Gender'
    ],
    'SearchQueryPerformanceReport': ['SearchQuery'],
    'AudiencePerformanceReport': ['AudienceId'],
    'AdExtensionDetailReport': [
        'AdExtensionId',
        'AdExtensionPropertyValue',
        'AdExtensionType',
        'AdExtensionTypeId' # removed `Impressions`, `Ctr,`, and `Clicks` from the required fields of `AdExtensionDetailReport`
    ],
    'GoalsAndFunnelsReport': ['Goal'],
    # added required fields for `AgeGenderAudienceReport` as mentioned in the bing-ads docs
    'AgeGenderAudienceReport': [
        'AccountName',
        'AdGroupName',
        'AgeGroup',
        'Gender'
    ],
}

# Columns considered metrics/measures — at least one must be selected per report request.
METRIC_COLUMNS = {
    'Clicks', 'Impressions', 'Ctr', 'Spend', 'Conversions', 'ConversionRate',
    'Revenue', 'Assists', 'ReturnOnAdSpend', 'AverageCpc', 'AverageCpm',
    'AverageCpp', 'LowQualityClicks', 'PhoneCalls', 'AllConversions',
    'AllRevenue', 'ViewThroughConversions',
}

# Alias used by BaseReport._check_required_metrics
REPORT_SPECIFIC_REQUIRED_COLUMNS = REPORT_SPECIFIC_REQUIRED_FIELDS
