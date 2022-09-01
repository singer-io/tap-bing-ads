
from integrations_testing_framework import *
import tap_bing_ads
import os
import pytest

config_path = os.path.abspath('config.json')

@pytest.mark.skip() # Currently failing due to a timezone issue
@assert_stdout_matches('tests/catalog.json')
@intercept_requests('tests/requests/discovery.txt', generate=False)
@with_sys_args(['--config', config_path, '--discover'])
def test_schema():
    tap_bing_ads.main()

# @write_stdout('tests/responses/accounts.txt')
@assert_stdout_matches('tests/responses/accounts.txt')
@intercept_requests('tests/requests/accounts.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'accounts' ,stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_accounts():
    tap_bing_ads.main()

# @write_stdout('tests/responses/campaigns.txt')
@assert_stdout_matches('tests/responses/campaigns.txt')
@intercept_requests('tests/requests/campaigns.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'campaigns',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_campaigns():
    tap_bing_ads.main()

# @write_stdout('tests/responses/ad_groups.txt')
@assert_stdout_matches('tests/responses/ad_groups.txt')
@intercept_requests('tests/requests/ad_groups.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'ad_groups',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_ad_groups():
    tap_bing_ads.main()

# @write_stdout('tests/responses/ads.txt')
@assert_stdout_matches('tests/responses/ads.txt')
@intercept_requests('tests/requests/ads.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'ads', stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_ads():
    tap_bing_ads.main()

# @write_stdout('tests/responses/ad_extension_detail_report.txt')
@assert_stdout_matches('tests/responses/ad_extension_detail_report.txt')
@intercept_requests('tests/requests/ad_extension_detail_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'ad_extension_detail_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_ad_extension_detail_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/ad_group_performance_report.txt')
@assert_stdout_matches('tests/responses/ad_group_performance_report.txt')
@intercept_requests('tests/requests/ad_group_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'ad_group_performance_report', stream_key = 'stream' , select_by_property=True,  exclude = 'fieldExclusions')])
def test_ad_group_performance_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/ad_performance_report.txt')
@assert_stdout_matches('tests/responses/ad_performance_report.txt')
@intercept_requests('tests/requests/ad_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'ad_performance_report',stream_key= 'stream' , select_by_property=True,exclude = 'fieldExclusions')])
def test_ad_performance_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/age_gender_audience_report.txt')
@assert_stdout_matches('tests/responses/age_gender_audience_report.txt')
@intercept_requests('tests/requests/age_gender_audience_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'age_gender_audience_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_age_gender_audience_report():
    tap_bing_ads.main()


# @write_stdout('tests/responses/audience_performace_report.txt')
@assert_stdout_matches('tests/responses/audience_performace_report.txt')
@intercept_requests('tests/requests/audience_performace_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'audience_performace_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_audience_performace_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/campaign_performance_report.txt')
@assert_stdout_matches('tests/responses/campaign_performance_report.txt')
@intercept_requests('tests/requests/campaign_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'campaign_performance_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_campaign_performance_report():
    tap_bing_ads.main()

@pytest.mark.skip() # Currently failing due to a timezone issue
# @write_stdout('tests/responses/geografic_performance_report.txt')
@assert_stdout_matches('tests/responses/geografic_performance_report.txt')
@intercept_requests('tests/requests/geografic_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'geografic_performance_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_geografic_performance_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/goals_and_funnels_report.txt')
@assert_stdout_matches('tests/responses/goals_and_funnels_report.txt')
@intercept_requests('tests/requests/goals_and_funnels_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'goals_and_funnels_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_goals_and_funnels_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/keyword_performance_report.txt')
@assert_stdout_matches('tests/responses/keyword_performance_report.txt')
@intercept_requests('tests/requests/keyword_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'keyword_performance_report',stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_keyword_performance_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/publisher_usage_performance_report.txt')
@assert_stdout_matches('tests/responses/publisher_usage_performance_report.txt')
@intercept_requests('tests/requests/publisher_usage_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'publisher_usage_performance_report', stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions')])
def test_publisher_usage_performance_report():
    tap_bing_ads.main()

# @write_stdout('tests/responses/search_query_performance_report.txt')
@assert_stdout_matches('tests/responses/search_query_performance_report.txt')
@intercept_requests('tests/requests/publisher_usage_performance_report.txt', generate=False)
@with_sys_args(['--config', config_path, '--catalog', utils.select_schema('tests/catalog.json', 'search_query_performance_report', stream_key= 'stream' , select_by_property=True, exclude = 'fieldExclusions') ])
def test_search_query_performance_report():
    tap_bing_ads.main()
