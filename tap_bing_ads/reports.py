REPORT_WHITELIST = [
    'KeywordPerformanceReport',
    'AdPerformanceReport',
    'AdGroupPerformanceReport',
    'GeographicPerformanceReport',
    'AgeGenderDemographicReport',
    'SearchQueryPerformanceReport'
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
    'GregorianDate': 'date',
    'AccountId': 'integer',
    'CampaignId': 'integer',
    'AdGroupId': 'integer',
    'KeywordId': 'integer',
    'AdId': 'integer',
    'CurrentMaxCpc': 'number',
    'Impressions': 'integer',
    'Clicks': 'integer',
    'Ctr': 'number',
    'AverageCpc': 'number',
    'Spend': 'number',
    'AveragePosition': 'number',
    'Conversions': 'number',
    'ConversionRate': 'number',
    'CostPerConversion': 'number',
    'QualityScore': 'number',
    'ExpectedCtr': 'number',
    'AdRelevance': 'number',
    'HistoricQualityScore': 'number',
    'HistoricExpectedCtr': 'number',
    'HistoricAdRelevance': 'number',
    'QualityImpact': 'number',
    'BusinessListingId': 'integer',
    'BusinessCategoryId': 'integer',
    'BusinessCatId': 'integer',
    'Assists': 'integer',
    'Revenue': 'number',
    'ReturnOnAdSpend': 'number',
    'CostPerAssist': 'number',
    'RevenuePerConversion': 'number',
    'RevenuePerAssist': 'number',
    'Mainline1Bid': 'number',
    'MainlineBid': 'number',
    'SidebarBid': 'number'
}
