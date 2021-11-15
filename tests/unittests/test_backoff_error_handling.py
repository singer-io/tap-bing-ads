import unittest
import socket
from unittest import mock
import tap_bing_ads
import json
from urllib.error import HTTPError
import ssl
from suds.transport import TransportError
import datetime

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
    def test_connection_reset_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                    mock_get_selected_fields, mock_get_core_schema,
                                                    mock_write_schema, mock_get_bookmark,
                                                    mock_sobject_to_dict, mock_write_state,
                                                    mock_write_bookmark, mock_metrics, mock_write_records,
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_get_account.side_effect = socket.error(104, 'Connection reset by peer')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except ConnectionResetError:
            pass
        
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_socket_timeout__error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                    mock_get_selected_fields, mock_get_core_schema,
                                                    mock_write_schema, mock_get_bookmark,
                                                    mock_sobject_to_dict, mock_write_state,
                                                    mock_write_bookmark, mock_metrics, mock_write_records,
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_get_account.side_effect = socket.timeout()
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_http_timeout__error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                    mock_get_selected_fields, mock_get_core_schema,
                                                    mock_write_schema, mock_get_bookmark,
                                                    mock_sobject_to_dict, mock_write_state,
                                                    mock_write_bookmark, mock_metrics, mock_write_records,
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_get_account.side_effect = HTTPError('url', 408, 'Request Timeout', {}, f)
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_internal_server_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                    mock_get_selected_fields, mock_get_core_schema,
                                                    mock_write_schema, mock_get_bookmark,
                                                    mock_sobject_to_dict, mock_write_state,
                                                    mock_write_bookmark, mock_metrics, mock_write_records,
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_get_account.side_effect = HTTPError('url', 500, 'Internal Server Error', {}, f)
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_transport_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                mock_get_selected_fields, mock_get_core_schema,
                                                mock_write_schema, mock_get_bookmark,
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_get_account.side_effect = TransportError('url', 500, 'Internal Server Error')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_400_error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                        mock_get_selected_fields, mock_get_core_schema, 
                                        mock_write_schema, mock_get_bookmark, 
                                        mock_sobject_to_dict, mock_write_state, 
                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                        mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_get_account.side_effect = HTTPError('url', 400, 'Bad Request', {}, f)
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_get_account.call_count, 1)
        
       
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_ssl_eof_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_get_account.side_effect = ssl.SSLEOFError('EOF occurred in violation of protocol')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_connection_reset_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_socket_timeout_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_http_timeout_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_internal_server_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_transport_server_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_400_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                            mock_write_schema, mock_get_bookmark, 
                                            mock_sobject_to_dict, mock_write_state, 
                                            mock_write_bookmark, mock_metrics, mock_write_records, 
                                            mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
    def test_connection_reset_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_socket_timeout_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_http_timeout_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_internal_server_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_transport_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_400_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)        
        
    def test_connection_reset_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_socket_timeout_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_http_timeout_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                smock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_internal_server_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)        

    def test_transport_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60) 

    def test_400_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1) 
        
    def test_ssl_eof_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                        mock_write_schema, mock_get_bookmark, 
                                        mock_sobject_to_dict, mock_write_state, 
                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)  
        
    @mock.patch("tap_bing_ads.build_report_request")
    def test_connection_reset_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                                force_refresh = True)
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.build_report_request")
    def test_socket_timeout_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
           tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key', 
                                              force_refresh = True)
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.build_report_request")
    def test_http_timeout_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
     
    @mock.patch("tap_bing_ads.build_report_request")   
    def test_internal_server_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)

    @mock.patch("tap_bing_ads.build_report_request")   
    def test_transport_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.build_report_request")   
    def test_400_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
        
    @mock.patch("tap_bing_ads.build_report_request")   
    def test_ssl_eof_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
                
    def test_connection_reset_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_socket_timeout_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
           tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_http_timeout_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
     
    def test_internal_server_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_transport_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_400_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
               
    def test_connection_reset_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_socket_timeout_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
           tap_bing_ads.get_report_schema(mock_client, '')
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_http_timeout_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
     
    def test_internal_server_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)

    def test_transport_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)

    def test_400_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
         
    def test_ssl_eof_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
                
    def test_connection_reset_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_socket_timeout_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
           tap_bing_ads.get_type_map(mock_client)
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_http_timeout_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
     
    def test_internal_server_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)

    def test_transport_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    def test_400_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                            mock_write_schema, mock_get_bookmark, 
                                            mock_sobject_to_dict, mock_write_state, 
                                            mock_write_bookmark, mock_metrics, mock_write_records, 
                                            mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)

    async def test_connection_reset_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the connection reset error.
        '''
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    async def test_socket_timeout_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_client = MockClient(socket.timeout())
        before_time = datetime.datetime.now()
        try:
           await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    async def test_http_timeout_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
     
    async def test_internal_server_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
            
    async def test_transport_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    async def test_400_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_client.call_count, 1)
        
    async def test_ssl_eof_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        before_time = datetime.datetime.now()
        try:
            s = await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_connection_reset_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                        mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        mock_oauth.return_value = ''
        mock_client.side_effect = socket.error(104, 'Connection reset by peer')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except ConnectionResetError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_socket_timeout_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the socket timeout error.
        '''
        mock_oauth.return_value = ''
        mock_client.side_effect = socket.timeout()
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except socket.timeout:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_http_timeout_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config, 
                                                        mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the http timeout error.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = HTTPError('url', 408, 'Request Timeout', {}, f)
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_internal_server_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 500 internal server error.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = HTTPError('url', 500, 'Internal Server Error', {}, f)
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except HTTPError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_transport_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = TransportError('url', 500, 'Internal Server Error')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except TransportError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_400_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 error.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = HTTPError('url', 400, 'Bad Request', {}, f)
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except HTTPError:
            pass
        # verify the code raise error without backoff
        self.assertEqual(mock_oauth.call_count, 1)
        
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_ssl_eof_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the SSLEOFError.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = ssl.SSLEOFError('EOF occurred in violation of protocol')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except ssl.SSLEOFError:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_408_exception_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the 408 Request Timeout Exception of.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = Exception((408, 'Request Timeout'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except Exception:
            pass
        after_time = datetime.datetime.now()
        time_difference = (after_time - before_time).total_seconds()
        # verify the code backed off for 60 seconds
        # time_difference should be greater or equall 60 as some time elapsed while calculating `after_time`
        self.assertGreaterEqual(time_difference, 60)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_400_exception_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap does not retry on the 400 Bad reques Exception.
        '''
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = Exception((400, 'Bad request'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except Exception:
            pass
        # verify the code backed off and requested for
        self.assertEqual(mock_oauth.call_count, 1)