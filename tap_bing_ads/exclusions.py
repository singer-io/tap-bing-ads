"""
Defines the field exclusion rules for Bing Ads/Microsoft Ads reports.
The structure is for each report, there are groups of fields that cannot
be selected with the associated group.

Docs: https://docs.microsoft.com/en-us/advertising/guides/reports?view=bingads-13#columnrestrictions

"""

EXCLUSIONS = {
    'AccountPerformanceReport': [
        {
            'Attributes': [
                'CustomerId',
                'CustomerName',
                'DeliveredMatchType'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent'
            ]
        },
        {
            'Attributes': [
                'BidMatchType',
                'DeviceOS',
                'Goal',
                'GoalType',
                'TopVsOther'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'AbsoluteTopImpressionSharePercent',
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToAdRelevancePercent',
                'ImpressionLostToBidPercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToExpectedCtrPercent',
                'ImpressionLostToRankPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'TopImpressionRatePercent',
                'TopImpressionShareLostToBudgetPercent',
                'TopImpressionShareLostToRankPercent',
                'TopImpressionSharePercent'
            ]
        }],
    'AdGroupPerformanceReport': [
        {
            'Attributes': [
                'CustomerId',
                'CustomerName',
                'DeliveredMatchType'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent'
            ]
        },{
            'Attributes': [
                'BidMatchType',
                'DeviceOS',
                'TopVsOther',
                'Goal',
                'GoalType'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AbsoluteTopImpressionSharePercent',
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToAdRelevancePercent',
                'ImpressionLostToBidPercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToExpectedCtrPercent',
                'ImpressionLostToRankPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'TopImpressionRatePercent',
                'TopImpressionShareLostToBudgetPercent',
                'TopImpressionShareLostToRankPercent',
                'TopImpressionSharePercent'
            ]
        }],
    'CampaignPerformanceReport': [
        {
            'Attributes': [
                'CustomerId',
                'CustomerName',
                'DeliveredMatchType'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent'
            ]
        },{
            'Attributes': [
                'BidMatchType',
                'DeviceOS',
                'Goal',
                'GoalType',
                'TopVsOther',
                'BudgetAssociationStatus',
                'BudgetName',
                'BudgetStatus'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'AbsoluteTopImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToAdRelevancePercent',
                'ImpressionLostToBidPercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToExpectedCtrPercent',
                'ImpressionLostToRankPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'TopImpressionRatePercent',
                'TopImpressionShareLostToBudgetPercent',
                'TopImpressionShareLostToRankPercent',
                'TopImpressionSharePercent'
            ]
        }],
    'ProductDimensionPerformanceReport': [{
        'Attributes': [
            'AdDistribution',
            'AdId',
            'AdStatus',
            'ClickType',
            'Language',
            'LocalStoreCode',
            'Network',
            'TopVsOther'
        ],
        'ImpressionSharePerformanceStatistics': [
            'AbsoluteTopImpressionSharePercent',
            'BenchmarkBid',
            'BenchmarkCtr',
            'ClickSharePercent',
            'ImpressionLostToBudgetPercent',
            'ImpressionLostToRankPercent',
            'ImpressionSharePercent'
        ]
    }],
    'ProductPartitionPerformanceReport': [{
        'Attributes': [
            'AdDistribution',
            'AdId',
            'AdStatus',
            'BidMatchType',
            'ClickType',
            'DeliveredMatchType',
            'Language',
            'LocalStoreCode',
            'Network',
            'TopVsOther'
        ],
        'ImpressionSharePerformanceStatistics': [
            'AbsoluteTopImpressionSharePercent',
            'BenchmarkBid',
            'BenchmarkCtr',
            'ClickSharePercent',
            'ImpressionLostToBudgetPercent',
            'ImpressionLostToRankPercent',
            'ImpressionSharePercent'
        ]
    }]
}
