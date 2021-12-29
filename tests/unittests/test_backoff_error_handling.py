import unittest
import socket
from unittest import mock
import tap_bing_ads
from urllib.error import HTTPError, URLError
import ssl
from suds.transport import TransportError
import datetime

class MockClient():
    '''Mocked ServiceClient class and it's method to pass the test case'''
    def __init__(self, error):
        self.error = error

    def GetCampaignsByAccountId(self, AccountId, CampaignType):
        """ Mocked GetCampaignsByAccountId to test the backoff in sync_campaigns method """
        raise self.error

    def GetAdGroupsByCampaignId(self, CampaignId):
        """ Mocked GetAdGroupsByCampaignId to test the backoff in sync_ad_groups method"""
        raise self.error

    def GetAdsByAdGroupId(self, AdGroupId, AdTypes):
        """ Mocked GetAdsByAdGroupId test to the backoff in sync_ads method"""
        raise self.error

    def SubmitGenerateReport(self, report_request):
        """ Mocked SubmitGenerateReport to test the backoff in get_report_request_id method"""
        raise self.error

    def PollGenerateReport(self, request_id):
        """ Mocked PollGenerateReport to test the backoff in poll_report method"""
        raise self.error

    @property
    def factory(self):
        """ Mocked factory to test backoff the in build_report_request method"""
        raise self.error

    @property
    def soap_client(self):
        """ Mocked soap_client to test backoff in get_type_map method"""
        raise self.error

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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
        mock_get_account.side_effect = TransportError('url', 500, 'Internal Server Error')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_url_error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                        mock_get_selected_fields, mock_get_core_schema, 
                                        mock_write_schema, mock_get_bookmark, 
                                        mock_sobject_to_dict, mock_write_state, 
                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_get_account.side_effect  = URLError("<urlopen error [Errno 104] Connection reset by peer>")
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
       
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_exception_408_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_get_account.side_effect = Exception((408, 'Request Timeout'))

        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
        
    def test_transport_server_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)
    
    def test_url_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    def test_exception_408_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
        
    def test_transport_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    def test_url_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
 
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)        

    def test_exception_408_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)        

    def test_transport_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    def test_url_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60) 

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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)  

    def test_exception_408_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    @mock.patch("tap_bing_ads.build_report_request")   
    def test_url_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
    
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    @mock.patch("tap_bing_ads.build_report_request")   
    def test_url_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
        
    def test_transport_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    def test_url_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)   
    
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    def test_408_exception_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    def test_transport_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    def test_url_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
    
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
    
    def test_exception_408_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        with open('tests/base.py') as f:
            mock_client = MockClient(Exception((408, 'Request Timeout')))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    def test_transport_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    def test_url_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.get_type_map(mock_client)
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
   
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    def test_exception_408_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        before_time = datetime.datetime.now()
        try:
           tap_bing_ads.get_type_map(mock_client)
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)
            
    async def test_transport_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)

    async def test_url_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the URLError.
        '''
        mock_client = MockClient(URLError("<urlopen error [Errno 104] Connection reset by peer>"))
        
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    async def test_exception_408_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_client = MockClient(Exception((408, 'Request Timeout')))
        
        before_time = datetime.datetime.now()
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
        mock_client.side_effect = TransportError('url', 500, 'Internal Server Error')
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except TransportError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code raise error without backoff
            # time_difference should be less or equal 1 as it directly raise the error without backoff
            self.assertGreaterEqual(1, time_difference)
        
        
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
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_url_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Transport error.
        '''
        mock_oauth.return_value = ''
        mock_client.side_effect = URLError("<urlopen error [Errno 104] Connection reset by peer>")
        
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except URLError:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_exception_408_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many):
        '''
        Test that tap retry for 60 seconds on the Exception with error code 408.
        '''
        mock_oauth.return_value = ''
        mock_client.side_effect = Exception((408, 'Request Timeout'))
        before_time = datetime.datetime.now()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except Exception:
            after_time = datetime.datetime.now()
            time_difference = (after_time - before_time).total_seconds()
            # verify the code backed off for 60 seconds
            # time_difference should be greater or equal 60 as some time elapsed while calculating `after_time`
            self.assertGreaterEqual(time_difference, 60)

