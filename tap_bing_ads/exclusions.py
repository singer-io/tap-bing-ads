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
                'AudienceImpressionSharePercent',
                'RelativeCtr'
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
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToAdRelevancePercent',
                'ImpressionLostToBidPercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToExpectedCtrPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionLostToRankPercent',
                'ImpressionSharePercent',
                'RelativeCtr',
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
                'AudienceImpressionSharePercent',
                'RelativeCtr'
            ]
        },{
            'Attributes': [
                'BidMatchType',
                'DeviceOS',
                'Goal',
                'GoalType',
                'TopVsOther'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToAdRelevancePercent',
                'ImpressionLostToBidPercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToExpectedCtrPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionLostToRankPercent',
                'ImpressionSharePercent',
                'RelativeCtr',
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
                'AudienceImpressionSharePercent',
                'RelativeCtr'
            ]
        },{
            'Attributes': [
                'BidMatchType',
                'BudgetAssociationStatus',
                'BudgetName',
                'BudgetStatus',
                'DeviceOS',
                'Goal',
                'GoalType',
                'TopVsOther'
            ],
            'ImpressionSharePerformanceStatistics': [
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToAdRelevancePercent',
                'ImpressionLostToBidPercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToExpectedCtrPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionLostToRankPercent',
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
            'ClickTypeId',
            'Goal',
            'GoalType',
            'Language',
            'LocalStoreCode',
            'Network',
            'TopVsOther'
        ],
        'ImpressionSharePerformanceStatistics': [
            'AbsoluteTopImpressionSharePercent',
            'AbsoluteTopImpressionShareLostToBudgetPercent',
            'AbsoluteTopImpressionShareLostToRankPercent',
            'AbsoluteTopImpressionSharePercent',
            'BenchmarkBid',
            'BenchmarkCtr',
            'ClickSharePercent',
            'ImpressionLostToBudgetPercent',
            'ImpressionLostToRankPercent',
            'ImpressionSharePercent',
            'TopImpressionRatePercent',
            'TopImpressionShareLostToBudgetPercent',
            'TopImpressionShareLostToRankPercent',
            'TopImpressionSharePercent'
        ]
    }],
    'ProductPartitionPerformanceReport': [{
        'Attributes': [
            'AdDistribution',
            'AdId',
            'AdStatus',
            'BidMatchType',
            'ClickType',
            'ClickTypeId',
            'DeliveredMatchType',
            'Goal',
            'GoalType',
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
