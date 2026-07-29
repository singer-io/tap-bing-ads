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

## Any not listed here are strings
REPORTING_FIELD_TYPES = {
    'AccountId': 'integer',
    'AdId': 'integer',
    'AdGroupCriterionId': 'integer',
    'AdGroupId': 'integer',
    'AdRelevance': 'number',
    'Assists': 'integer',
    'AverageCpc': 'number',
    'AverageCpp': 'number',
    'AveragePosition': 'number',
    'BusinessCategoryId': 'integer',
    'BusinessListingId': 'integer',
    'CampaignId': 'integer',
    'ClickCalls': 'integer',
    'Clicks': 'integer',
    'ConversionRate': 'number',
    'Conversions': 'number',
    'CostPerAssist': 'number',
    'CostPerConversion': 'number',
    'Ctr': 'number',
    'CurrentMaxCpc': 'number',
    'EstimatedClickPercent': 'number',
    'EstimatedClicks': 'integer',
    'EstimatedConversionRate': 'number',
    'EstimatedConversions': 'integer',
    'EstimatedCtr': 'number',
    'EstimatedImpressionPercent': 'number',
    'EstimatedImpressions': 'integer',
    'ExactMatchImpressionSharePercent': 'number',
    'ExpectedCtr': 'number',
    'GoalId': 'integer',
    'HistoricAdRelevance':  'number',
    'HistoricExpectedCtr': 'number',
    'HistoricLandingPageExperience': 'number',
    'HistoricQualityScore': 'number',
    'ImpressionLostToAdRelevancePercent': 'number',
    'ImpressionLostToBidPercent': 'number',
    'ImpressionLostToBudgetPercent': 'number',
    'ImpressionLostToExpectedCtrPercent': 'number',
    'ImpressionLostToRankPercent': 'number',
    'Impressions': 'integer',
    'ImpressionSharePercent': 'number',
    'KeywordId': 'integer',
    'LandingPageExperience': 'number',
    'LocationId': 'integer',
    'LowQualityClicks': 'integer',
    'LowQualityClicksPercent': 'number',
    'LowQualityConversionRate': 'number',
    'LowQualityConversions': 'integer',
    'LowQualityGeneralClicks': 'integer',
    'LowQualityImpressions': 'integer',
    'LowQualityImpressionsPercent': 'number',
    'LowQualitySophisticatedClicks': 'integer',
    'Mainline1Bid': 'number',
    'MainlineBid': 'number',
    'ManualCalls': 'integer',
    'PhoneCalls': 'integer',
    'PhoneImpressions': 'integer',
    'Ptr': 'number',
    'QualityImpact': 'number',
    'QualityScore': 'number',
    'Radius': 'number',
    'ReturnOnAdSpend': 'number',
    'Revenue': 'number',
    'RevenuePerAssist': 'number',
    'RevenuePerConversion': 'number',
    'SidebarBid': 'number',
    'Spend': 'number',
    'TimePeriod': 'datetime',
    'AbsoluteTopImpressionRatePercent': 'number',
    'AbsoluteTopImpressionShareLostToBudgetPercent': 'number',
    'AbsoluteTopImpressionShareLostToRankPercent': 'number',
    'AbsoluteTopImpressionSharePercent': 'number',
    'AudienceImpressionLostToBudgetPercent': 'number',
    'AudienceImpressionLostToRankPercent': 'number',
    'AudienceImpressionSharePercent': 'number',
    'ClickSharePercent': 'number',
    'ImpressionLostToRankAggPercent': 'number',
    'RelativeCtr': 'number',
    'ViewThroughConversions': 'number',
    'AllConversions': 'number',
    'AllConversionRate': 'number',
    'AllCostPerConversion': 'number',
    'AllReturnOnAdSpend': 'number',
    'AllRevenue': 'number',
    'AllRevenuePerConversion': 'number',
    'AudienceId': 'integer',
    'ConversionsQualified': 'number',
    'ViewThroughConversionsQualified': 'number',
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
