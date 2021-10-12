import unittest
import socket
from unittest import mock
import tap_bing_ads
import json
from urllib.error import HTTPError
import ssl
from suds.transport import TransportError
    
class MockClient():
    
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
class TestConnectionResetError(unittest.TestCase):
    
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_connection_reset_error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        mock_get_account.side_effect = socket.error(104, 'Connection reset by peer')
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_socket_timeout__error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        mock_get_account.side_effect = socket.timeout()
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_http_timeout__error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_get_account.side_effect = HTTPError('url', 408, 'Request Timeout', {}, f)
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_internal_server_error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_get_account.side_effect = HTTPError('url', 500, 'Internal Server Error', {}, f)
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_transport_error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                                
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_get_account.side_effect = TransportError('url', 500, 'Internal Server Error')
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)
        
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_400_error_get_account(self, mock_get_account, mock_create_sdk_client, 
                                        mock_get_selected_fields, mock_get_core_schema, 
                                        mock_write_schema, mock_get_bookmark, 
                                        mock_sobject_to_dict, mock_write_state, 
                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_get_account.side_effect = HTTPError('url', 400, 'Bad Request', {}, f)
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 1)
        
       
    @mock.patch("tap_bing_ads.create_sdk_client", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_ssl_eof_error_get_account(self, mock_get_account, mock_create_sdk_client,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_get_account.side_effect = ssl.SSLEOFError('EOF occurred in violation of protocol')
        try:
            tap_bing_ads.sync_accounts_stream(['i1'], {})
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_get_account.call_count, 5)
        
    def test_connection_reset_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_socket_timeout_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_http_timeout_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_internal_server_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_transport_server_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_400_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                            mock_write_schema, mock_get_bookmark, 
                                            mock_sobject_to_dict, mock_write_state, 
                                            mock_write_bookmark, mock_metrics, mock_write_records, 
                                            mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_sync_campaigns(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.sync_campaigns(mock_client, '', [])
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
    def test_connection_reset_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_socket_timeout_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_http_timeout_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_internal_server_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_transport_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_400_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_sync_ad_groups(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.sync_ad_groups(mock_client, '', ['dummy_campaign_id'], [])
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)        
        
    def test_connection_reset_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_socket_timeout_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_http_timeout_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                smock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_internal_server_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)        

    def test_transport_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5) 

    def test_400_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1) 
        
    def test_ssl_eof_error_sync_ads(self, mock_get_selected_fields, mock_get_core_schema, 
                                        mock_write_schema, mock_get_bookmark, 
                                        mock_sobject_to_dict, mock_write_state, 
                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.sync_ads(mock_client, ['dummy_stream'], ['dummy_ad_id'])
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)  
        
    @mock.patch("tap_bing_ads.build_report_request")
    def test_connection_reset_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                                force_refresh = True)
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    @mock.patch("tap_bing_ads.build_report_request")
    def test_socket_timeout_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
           tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key', 
                                              force_refresh = True)
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    @mock.patch("tap_bing_ads.build_report_request")
    def test_http_timeout_error_get_report_request_id(self, mock_build_report_request, 
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
     
    @mock.patch("tap_bing_ads.build_report_request")   
    def test_internal_server_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    @mock.patch("tap_bing_ads.build_report_request")   
    def test_transport_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    @mock.patch("tap_bing_ads.build_report_request")   
    def test_400_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
        
    @mock.patch("tap_bing_ads.build_report_request")   
    def test_ssl_eof_error_get_report_request_id(self, mock_build_report_request,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.get_report_request_id(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date', 'dummy_start_key',
                                               force_refresh = True)
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
                
    def test_connection_reset_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_socket_timeout_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
           tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_http_timeout_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
     
    def test_internal_server_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_transport_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_400_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_build_report_request(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.build_report_request(mock_client, '', '', '', 'dummy_start_date', 'dumy_end_date')
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
               
    def test_connection_reset_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_socket_timeout_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
           tap_bing_ads.get_report_schema(mock_client, '')
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_http_timeout_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
     
    def test_internal_server_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_transport_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_400_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
         
    def test_ssl_eof_error_get_report_schema(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.get_report_schema(mock_client, '')
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
                
    def test_connection_reset_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_socket_timeout_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
           tap_bing_ads.get_type_map(mock_client)
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_http_timeout_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
     
    def test_internal_server_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    def test_transport_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    def test_400_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
        
    def test_ssl_eof_error_get_type_map(self, mock_get_selected_fields, mock_get_core_schema, 
                                            mock_write_schema, mock_get_bookmark, 
                                            mock_sobject_to_dict, mock_write_state, 
                                            mock_write_bookmark, mock_metrics, mock_write_records, 
                                            mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            tap_bing_ads.get_type_map(mock_client)
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)

    async def test_connection_reset_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.error(104, 'Connection reset by peer'))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    async def test_socket_timeout_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_client = MockClient(socket.timeout())
        try:
           await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    async def test_http_timeout_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 408, 'Request Timeout', {}, f))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
     
    async def test_internal_server_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 500, 'Internal Server Error', {}, f))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
            
    async def test_transport_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(TransportError('url', 500, 'Internal Server Error'))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    async def test_400_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(HTTPError('url', 400, 'Bad Request', {}, f))
        try:
            await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 1)
        
    async def test_ssl_eof_error_poll_report(self, mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        with open('tests/base.py') as f:
            mock_client = MockClient(ssl.SSLEOFError('EOF occurred in violation of protocol'))
        try:
            s = await tap_bing_ads.poll_report(mock_client, '', '', '', '', '')
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_client.call_count, 5)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_connection_reset_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                        mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        mock_client.side_effect = socket.error(104, 'Connection reset by peer')
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except ConnectionResetError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_socket_timeout_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        mock_client.side_effect = socket.timeout()
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except socket.timeout:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_http_timeout_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config, 
                                                        mock_get_selected_fields, mock_get_core_schema, 
                                                        mock_write_schema, mock_get_bookmark, 
                                                        mock_sobject_to_dict, mock_write_state, 
                                                        mock_write_bookmark, mock_metrics, mock_write_records, 
                                                        mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = HTTPError('url', 408, 'Request Timeout', {}, f)
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_internal_server_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                           mock_get_selected_fields, mock_get_core_schema, 
                                                           mock_write_schema, mock_get_bookmark, 
                                                           mock_sobject_to_dict, mock_write_state, 
                                                           mock_write_bookmark, mock_metrics, mock_write_records, 
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = HTTPError('url', 500, 'Internal Server Error', {}, f)
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_transport_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                    mock_get_selected_fields, mock_get_core_schema, 
                                                    mock_write_schema, mock_get_bookmark, 
                                                    mock_sobject_to_dict, mock_write_state, 
                                                    mock_write_bookmark, mock_metrics, mock_write_records, 
                                                    mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = TransportError('url', 500, 'Internal Server Error')
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except TransportError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)
        
    @mock.patch("tap_bing_ads.CONFIG", return_value = {'oauth_client_id': '', 'oauth_client_secret': '', 'refresh_token': ''})            
    @mock.patch("bingads.OAuthWebAuthCodeGrant.request_oauth_tokens_by_refresh_token")
    @mock.patch("bingads.AuthorizationData", return_value = '')
    @mock.patch("tap_bing_ads.CustomServiceClient")
    def test_400_error_create_sdk_client(self, mock_client, mock_authorization_data, mock_oauth, mock_config,
                                                mock_get_selected_fields, mock_get_core_schema, 
                                                mock_write_schema, mock_get_bookmark, 
                                                mock_sobject_to_dict, mock_write_state, 
                                                mock_write_bookmark, mock_metrics, mock_write_records, 
                                                mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = HTTPError('url', 400, 'Bad Request', {}, f)
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except HTTPError:
            pass
        # verify the code backed off and requested for 5 times
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
                                                           mock_filter_selected_fields_many,mock_sleep):
        mock_oauth.return_value = ''
        with open('tests/base.py') as f:
            mock_client.side_effect = ssl.SSLEOFError('EOF occurred in violation of protocol')
        try:
            tap_bing_ads.create_sdk_client('dummy_service', {})
        except ssl.SSLEOFError:
            pass
        # verify the code backed off and requested for 5 times
        self.assertEqual(mock_oauth.call_count, 5)