import unittest
from unittest import mock
import tap_bing_ads
from urllib.error import HTTPError, URLError
from suds.transport import TransportError
    
class MockClient():
    '''Mocked ServiceClient class and it's method to pass the test case'''
    def __init__(self, error):
        self.error = error
        self.count = 0

    def GetCampaignsByAccountId(self, AccountId, CampaignType):
        self.count = self.count + 1
        raise self.error

    def GetAdGroupsByCampaignId(self, CampaignId):
        self.count = self.count + 1
        raise self.error

    def GetAdsByAdGroupId(self, AdGroupId, AdTypes):
        self.count = self.count + 1
        raise self.error

    def PollGenerateReport(self):
        self.count = self.count + 1
        raise self.error

    def SubmitGenerateReport(self, report_request):
        self.count = self.count + 1
        raise self.error

    def PollGenerateReport(self, request_id):
        self.count = self.count + 1
        raise self.error

    @property
    def factory(self):
        self.count = self.count + 1
        raise self.error

    @property
    def soap_client(self):
        self.count = self.count + 1
        raise self.error

    @property
    def call_count(self):
        return self.count

@mock.patch("time.sleep")
@mock.patch("tap_bing_ads.filter_selected_fields_many", return_value = '')
@mock.patch("singer.write_records", return_value = '')
@mock.patch("singer.metrics", return_value = '')
@mock.patch("singer.write_bookmark", return_value = '')
@mock.patch("singer.write_state", return_value = '')
@mock.patch("tap_bing_ads.sobject_to_dict", return_value = '')
@mock.patch("singer.get_bookmark", return_value = {'b1': 'b1'})
@mock.patch("singer.write_schema", return_value = '')
@mock.patch("tap_bing_ads.get_core_schema", return_value = '')
@mock.patch("tap_bing_ads.get_selected_fields", return_value = '')
class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly. Mocked some common method to test the backoff including
    filter_selected_fields_many, write_records , metrics, write_bookmark, write_state, sobject_to_dict,
    get_bookmark, write_schema, get_core_schema, get_selected_fields, time.sleep.
    '''
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_url_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                    mock_get_selected_fields, mock_get_core_schema,
                                                    mock_write_schema, mock_get_bookmark,
                                                    mock_sobject_to_dict, mock_write_state,
                                                    mock_write_bookmark, mock_metrics, mock_write_records,
                                                    mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_get_account.side_effect = URLError('_ssl.c:1059: The handshake operation timed out')
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)

    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_url_error_no_timeout_get_account(self, mock_get_account, mock_create_sdk_client,
                                                    mock_get_selected_fields, mock_get_core_schema,
                                                    mock_write_schema, mock_get_bookmark,
                                                    mock_sobject_to_dict, mock_write_state,
                                                    mock_write_bookmark, mock_metrics, mock_write_records,
                                                    mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mock_get_account.side_effect = URLError('_ssl.c:1059: The handshake operation did not succeed')
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_get_account.call_count, 1)

    def test_url_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_url_error_no_timeout_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation did not succeed'))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)

    def test_url_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url error timeout 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_url_error_no_timeout_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation did not succeed'))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except URLError:
            pass
        # verify the code does not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)

    def test_url_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_url_error_no_timeout_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation did not succeed'))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except URLError:
            pass
        # verify the code does not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)    

    @mock.patch("tap_bing_ads.build_report_request")
    def test_url_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                                force_refresh = True)
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    @mock.patch("tap_bing_ads.build_report_request")
    def test_url_error_no_timeout_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation did not succeed'))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                                force_refresh = True)
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)


    def test_url_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except URLError:
            pass
        # verify the code back off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_url_error_no_timeout_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation did not succeed'))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)    

    def test_url_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 1 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)

    def test_url_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except URLError:
            pass
        # verify the codedid not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)

    async def test_url_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the connection reset error 5 times.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation timed out'))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    async def test_url_error_no_timeout_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mock_client = MockClient(URLError('_ssl.c:1059: The handshake operation did not succeed'))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_client.call_count, 1)

    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_url_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                        mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        mock_client.side_effect = URLError('_ssl.c:1059: The handshake operation timed out')
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)

    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_url_error_no_timeout_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                        mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        """
        Test that tap does not retry on the url error no timeout.
        """
        mock_oauth.return_value = ''
        mock_client.side_effect = URLError('_ssl.c:1059: The handshake operation did not succeed')
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mock_oauth.call_count, 1)

    @mock.patch('tap_bing_ads.requests.Session.get')
    def test_url_error_stream_report(self, mocked_get, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap retry on the url timeout error 5 times.
        '''
        mocked_get.side_effect = URLError('_ssl.c:1059: The handshake operation timed out')
        try:
            tap_bing_ads.stream_report('dummy_report', '', 'http://test.com', 'dummy_time')
        except URLError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mocked_get.call_count, 5)

    @mock.patch('tap_bing_ads.requests.Session.get')
    def test_url_error_no_timeout_stream_report(self, mocked_get, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        '''
        Test that tap does not retry on the url error no timeout.
        '''
        mocked_get.side_effect = URLError('_ssl.c:1059: The handshake operation did not succeed')
        try:
            tap_bing_ads.stream_report('dummy_report', '', 'http://test.com', 'dummy_time')
        except URLError:
            pass
        # verify the code did not back off and requested for 1 time
        self.assertEqual(mocked_get.call_count, 1)