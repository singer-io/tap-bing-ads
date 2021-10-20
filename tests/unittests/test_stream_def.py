import unittest
from unittest import mock
from tap_bing_ads import get_stream_def

class TestStreamDef(unittest.TestCase):
    """A set of unit tests to ensure that we are getting proper schema and metadata for each streams"""
    
    # Mocking metadata
    def mock_metadata(*args):
        return {(): {'table-key-properties': ['Id'], 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': 'LastModifiedTime', 'inclusion': 'available'}, ('properties', 'BillToCustomerId'): {'inclusion': 'available'}, ('properties', 'CurrencyCode'): {'inclusion': 'available'}, ('properties', 'AccountFinancialStatus'): {'inclusion': 'available'}, ('properties', 'Id'): {'inclusion': 'automatic'}, ('properties', 'Language'): {'inclusion': 'available'}, ('properties', 'LastModifiedByUserId'): {'inclusion': 'available'}, ('properties', 'LastModifiedTime'): {'inclusion': 'available'}, ('properties', 'Name'): {'inclusion': 'available'}}

    # Mocking metadata.write method
    def mock_write(mdata,tpl,incl='inclusion', val='automatic'):
        if isinstance(tpl[1],list):
            mdata[(tpl[0],tpl[1][0])] = {incl: val}
        else:
            mdata[tpl] = {incl: val}
        return mdata

    # Mocking metadata.to_list method
    def mock_to_list(mdata):
        return [{'breadcrumb': (), 'metadata': {'table-key-properties': ['Id'], 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': 'LastModifiedTime', 'inclusion': 'available'}}, {'breadcrumb': ('properties', 'BillToCustomerId'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'CurrencyCode'), 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ('properties', 'AccountFinancialStatus'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'Id'), 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ('properties', 'Language'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'LastModifiedByUserId'), 'metadata': {'inclusion': 'available'}}, {'breadcrumb': ('properties', 'LastModifiedTime'), 'metadata': {'inclusion': 'automatic'}}, {'breadcrumb': ('properties', 'Name'), 'metadata': {'inclusion': 'available'}}]

    @mock.patch("singer.metadata.get_standard_metadata")
    @mock.patch("singer.metadata.to_map",side_effect=mock_metadata)
    @mock.patch("singer.metadata.write", side_effect=mock_write)
    @mock.patch("singer.metadata.to_list", side_effect=mock_to_list)
    def test_get_stream_def_for_account_stream(self,mock_to_list,mock_write,mock_metadata, mock_get_standard_metadata):
        """ 
        Call get_stream_def function for a stream and validate primary keys, automatic keys and replication keys assignment
        """
        # Defined mock variables to call get_stream_def function for account stream
        mock_stream_name = 'test_accounts'
        mock_schema = {'type':['null','object'],'additionalProperties':False,'properties':{'BillToCustomerId':{'type':['null','integer']},'CurrencyCode':{'type':['null','string']},'AccountFinancialStatus':{'type':['null','string']},'Id':{'type':['null','integer']},'Language':{'type':['null','string']},'LastModifiedByUserId':{'type':['null','integer']},'LastModifiedTime':{'type':['null','string'],'format':'date-time'},'Name':{'type':['null','string']}}}
        mock_stream_metadata = [{'metadata': {'inclusion': 'automatic'}, 'breadcrumb': ['properties','CurrencyCode']}]
        mock_pks=['Id']
        mock_replication_key=['LastModifiedTime']
        stream_response = get_stream_def(mock_stream_name, mock_schema, mock_stream_metadata, mock_pks, mock_replication_key)

        # Verify top-level breadcrum is available
        self.assertEquals(stream_response.get('metadata')[0].get('breadcrumb'),())
        self.assertEquals(stream_response.get('metadata')[0].get('metadata'),{'table-key-properties': ['Id'], 'forced-replication-method': 'INCREMENTAL', 'valid-replication-keys': 'LastModifiedTime', 'inclusion': 'available'})

        # Verify pks, replication_keys and automatic_keys in mock_stream_metadata must be automatic in metadata
        for field in stream_response.get('metadata'):
            if field.get('breadcrumb') in [('properties', 'Id'),('properties', 'LastModifiedTime'),('properties', 'CurrencyCode')]:
                self.assertEquals(field.get('metadata').get('inclusion'),'automatic')
