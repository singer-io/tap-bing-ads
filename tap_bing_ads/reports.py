REPORT_WHITELIST = [
    'KeywordPerformanceReport'
]

REPORT_REQUIRED_FIELDS = ['_sdc_report_datetime', 'AccountId', 'GregorianDate']

REPORT_SPECIFIC_REQUIRED_FIELDS = {
}

## Any not listed here are strings
REPORTING_FIELD_TYPES = {
    'GregorianDate': 'date',
    'AccountNumber': 'integer',
    'AccountId': 'integer',
    'CampaignId': 'integer',
    'AdGroupId': 'integer',
    'KeywordId': 'integer',
    'AdId': 'integer',
    'AdType': 'integer',
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
