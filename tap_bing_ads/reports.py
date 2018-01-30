REPORT_WHITELIST = [
    'KeywordPerformanceReport',
    'AdPerformanceReport',
    'AdGroupPerformanceReport',
    'GeographicPerformanceReport',
    'AgeGenderDemographicReport',
    'SearchQueryPerformanceReport',
    'CampaignPerformanceReport',
    'GoalsAndFunnelsReport'
]

REPORT_REQUIRED_FIELDS = ['_sdc_report_datetime', 'AccountId', 'GregorianDate']

REPORT_SPECIFIC_REQUIRED_FIELDS = {
    'GeographicPerformanceReport': ['AccountName'],
    'AgeGenderDemographicReport': [
        'AccountName',
        'AdGroupName',
        'AgeGroup',
        'Gender'
    ],
    'SearchQueryPerformanceReport': ['SearchQuery']
}

ALIASES = {
    'BusinessCatName': 'BusinessCategoryName',
    'BusinessCatId': 'BusinessCategoryId',
    'AvgCPP': 'AverageCpp',
    'PTR': 'Ptr',
    'FinalAppUrl': 'FinalAppURL',
    'FinalMobileUrl': 'FinalMobileURL',
    'FinalUrl': 'FinalURL',
    'Bid strategy type': 'BidStrategyType'
}

# the bing reporting API just throws these in - they are not in the docs or WSDL
EXTRA_FIELDS = {
    'GeographicPerformanceReport': ['CountryOrRegion'],
    'SearchQueryPerformanceReport': ['Status']
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
    'GregorianDate': 'date',
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
    'Spend': 'number'
}
