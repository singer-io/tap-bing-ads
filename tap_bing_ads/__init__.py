#!/usr/bin/env python3

import asyncio
import json
import csv
import sys
import re
import io
from datetime import datetime
from zipfile import ZipFile

import socket
import ssl
import functools
from urllib.error import URLError
import singer
from singer import utils, metadata, metrics
from bingads import AuthorizationData, OAuthWebAuthCodeGrant, ServiceClient
import suds
from suds.sudsobject import asdict
import stringcase
import requests
import arrow
import backoff

from tap_bing_ads import reports
from tap_bing_ads.exclusions import EXCLUSIONS

LOGGER = singer.get_logger()
REQUEST_TIMEOUT = 300

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "customer_id",
    "account_ids",
    "oauth_client_id",
    "oauth_client_secret",
    "refresh_token",
    "developer_token",
]

# objects that are at the root level, with selectable fields in the Stitch UI
TOP_LEVEL_CORE_OBJECTS = [
    'AdvertiserAccount',
    'Campaign',
    'AdGroup',
    'Ad'
]

CONFIG = {}
STATE = {}

# ~2 hour polling timeout
MAX_NUM_REPORT_POLLS = 1440
REPORT_POLL_SLEEP = 5

SESSION = requests.Session()
DEFAULT_USER_AGENT = 'Singer.io Bing Ads Tap'

ARRAY_TYPE_REGEX = r'ArrayOf([A-Za-z0-9]+)'

def should_retry_httperror(exception):
    """ Return true if exception is required to retry otherwise return false """
    try:
        if isinstance(exception, ConnectionError) or isinstance(exception, ssl.SSLError) or isinstance(exception, suds.transport.TransportError) or isinstance(exception, socket.timeout) or type(exception) == URLError: # pylint: disable=consider-merging-isinstance,no-else-return)
            return True
        elif (type(exception) == Exception and exception.args[0][0] == 408) or exception.code == 408:
            # A 408 Request Timeout is an HTTP response status code that indicates the server didn't receive a complete
            # request message within the server's allotted timeout period. A suds SDK catches HTTPError with status code 408 and
            # raises Exception: (408, 'Request Timeout'). That's why we retrying this error also.
            return True
        return 500 <= exception.code < 600
    except AttributeError:
        return False

def bing_ads_error_handling(fnc):
    """
        Retry following errors until 60 seconds(7 or 8 times retry perform).
        socket.timeout, ConnectionError, internal server error(500-range), SSLError, URLError, HTTPError(408), Transport errors, Exception with 408 error code.
        If after passing 60 seconds the same error comes, then it will be raised. Raise the error directly for all errors except mentioned above errors.
    """
    @backoff.on_exception(backoff.expo,
                          (Exception),
                          giveup=lambda e: not should_retry_httperror(e),
                          max_time=60, # 60 seconds
                          factor=2)
    @functools.wraps(fnc)
    def wrapper(*args, **kwargs):
        return fnc(*args, **kwargs)
    return wrapper

def get_user_agent():
    return CONFIG.get('user_agent', DEFAULT_USER_AGENT)

class InvalidDateRangeEnd(Exception):
    pass

def log_service_call(service_method, account_id):
    def wrapper(*args, **kwargs): # pylint: disable=inconsistent-return-statements
        log_args = list(map(lambda arg: str(arg).replace('\n', '\\n'), args)) + list(map(lambda kv: '{}={}'.format(*kv), kwargs.items()))
        LOGGER.info('Calling: %s(%s) for account: %s',
                    service_method.name,
                    ','.join(log_args),
                    account_id)
        with metrics.http_request_timer(service_method.name):
            try:
                return service_method(*args, **kwargs)
            except suds.WebFault as e:
                #Raise SOAP exception
                if hasattr(e.fault.detail, 'ApiFaultDetail'):
                    # The Web fault structure is heavily nested. This is to be sure we catch the error we want.
                    operation_errors = e.fault.detail.ApiFaultDetail.OperationErrors
                    invalid_date_range_end_errors = [oe for (_, oe) in operation_errors
                                                     if oe.ErrorCode == 'InvalidCustomDateRangeEnd']
                    if any(invalid_date_range_end_errors):
                        raise InvalidDateRangeEnd(invalid_date_range_end_errors) from e
                    LOGGER.info('Caught exception for account: %s', account_id)
                    raise Exception(operation_errors) from e
                if hasattr(e.fault.detail, 'AdApiFaultDetail'):
                    raise Exception(e.fault.detail.AdApiFaultDetail.Errors) from e

    return wrapper

def get_request_timeout():
    request_timeout = CONFIG.get('request_timeout')
    # if request_timeout is other than 0, "0" or "" then use request_timeout else use default request timeout.
    if request_timeout and float(request_timeout):
        request_timeout = float(request_timeout)
    else: # If value is 0, "0" or "" then set default to 300 seconds.
        request_timeout = REQUEST_TIMEOUT
    return request_timeout

class CustomServiceClient(ServiceClient):
    # This class calling the methods of the specified Bing Ads service.
    @bing_ads_error_handling
    def __init__(self, name, **kwargs):
        # Initializes a new instance of this ServiceClient class.
        return super().__init__(name, 'v13', **kwargs)

    def __getattr__(self, name):
        # Log and return service call(suds client call) object
        service_method = super(CustomServiceClient, self).__getattr__(name)
        return log_service_call(service_method, self._authorization_data.account_id)

    def set_options(self, **kwargs):
        # Set suds options, these options will be passed to suds.
        self._options = kwargs # pylint: disable=attribute-defined-outside-init

        kwargs = ServiceClient._ensemble_header(self.authorization_data, **self._options)
        kwargs['headers']['User-Agent'] = get_user_agent()
        # setting the timeout parameter using the set_options which sets timeout in the _soap_client
        kwargs['timeout'] = get_request_timeout()
        self._soap_client.set_options(**kwargs)

@bing_ads_error_handling
def create_sdk_client(service, account_id):
    # Creates SOAP client with OAuth refresh credentials for services
    LOGGER.info('Creating SOAP client with OAuth refresh credentials for service: %s, account_id %s',
                service, account_id)

    if CONFIG.get('require_live_connect', 'True') == 'True':
        oauth_scope = 'bingads.manage'
    else:
        oauth_scope = 'msads.manage'

    # Represents an OAuth authorization object implementing the authorization code grant flow for use in a web application.
    authentication = OAuthWebAuthCodeGrant(
        CONFIG['oauth_client_id'],
        CONFIG['oauth_client_secret'],
        '',
        oauth_scope=oauth_scope) ## redirect URL not needed for refresh token

    # Retrieves OAuth access and refresh tokens from the Microsoft Account authorization service.
    authentication.request_oauth_tokens_by_refresh_token(CONFIG['refresh_token'])

    # Instance require to authenticate with Bing Ads
    authorization_data = AuthorizationData(
        account_id=account_id,
        customer_id=CONFIG['customer_id'],
        developer_token=CONFIG['developer_token'],
        authentication=authentication)

    return CustomServiceClient(service, authorization_data=authorization_data)

def sobject_to_dict(obj):
    # Convert response of soap to dictionary
    if not hasattr(obj, '__keylist__'):
        return obj

    out = {}
    for key, value in asdict(obj).items():
        if hasattr(value, '__keylist__'):
            out[key] = sobject_to_dict(value)
        elif isinstance(value, list):
            out[key] = []
            for item in value:
                out[key].append(sobject_to_dict(item))
        elif isinstance(value, datetime):
            out[key] = arrow.get(value).isoformat()
        else:
            out[key] = value
    return out

def xml_to_json_type(xml_type):
    # Convert xml type to json type
    if xml_type == 'boolean':
        return 'boolean'
    if xml_type in ['decimal', 'float', 'double']:
        return 'number'
    if xml_type in ['long', 'int', 'unsignedByte']:
        return 'integer'

    return 'string'

def get_json_schema(element):
    # Prepare json `type` for schema of streams e.g. "type": ["null","integer"]
    types = []
    _format = None

    if element.nillable:
        types.append('null')

    if element.root.name == 'simpleType':
        types.append('null')
        types.append('string')
    else:
        xml_type = element.type[0]

        _type = xml_to_json_type(xml_type)
        types.append(_type)

        if xml_type in ['dateTime', 'date']:
            _format = 'date-time'

    schema = {'type': types}

    if _format:
        schema['format'] = _format

    return schema

def get_array_type(array_type):
    #Return array type to prepare schema file for streams
    # e.g "res":{"type": ["null","integer"], "properties": {"id": "type": ["null","integer"]}}
    xml_type = re.match(ARRAY_TYPE_REGEX, array_type).groups()[0]
    json_type = xml_to_json_type(xml_type) # Convert xml to json type
    if json_type == 'string' and xml_type != 'string':
        # complex type
        items = xml_type # will be filled in fill_in_nested_types
    else:
        items = {
            'type': json_type
        }

    array_obj = {
        'type': ['null', 'object'],
        'properties': {}
    }

    array_obj['properties'][xml_type] = {
        'type': ['null', 'array'],
        'items': items
    }

    return array_obj

def get_complex_type_elements(inherited_types, wsdl_type):
    ## inherited type
    if isinstance(wsdl_type.rawchildren[0].rawchildren[0], suds.xsd.sxbasic.Extension): # pylint: disable=no-else-return
        abstract_base = wsdl_type.rawchildren[0].rawchildren[0].ref[0]
        if abstract_base not in inherited_types:
            inherited_types[abstract_base] = set()
        inherited_types[abstract_base].add(wsdl_type.name)

        elements = []
        for element_group in wsdl_type.rawchildren[0].rawchildren[0].rawchildren:
            for element in element_group:
                elements.append(element[0])
        return elements
    else:
        return wsdl_type.rawchildren[0].rawchildren

def wsdl_type_to_schema(inherited_types, wsdl_type):
    #Prepare schema from wsdl file
    if wsdl_type.root.name == 'simpleType':
        return get_json_schema(wsdl_type) # Return json schema for simpleType wsdl

    elements = get_complex_type_elements(inherited_types, wsdl_type) # Return json schema for complexType(recursive) wsdl

    properties = {}
    for element in elements:
        if element.root.name == 'enumeration':
            properties[element.name] = get_json_schema(element)
        elif element.type is None and element.ref:
            properties[element.name] = element.ref[0] ## set to service type name for now
        elif element.type[1] != 'http://www.w3.org/2001/XMLSchema': ## not a built-in XML type
            _type = element.type[0]
            if 'ArrayOf' in _type:
                properties[element.name] = get_array_type(_type)
            else:
                properties[element.name] = _type ## set to service type name for now
        else:
            properties[element.name] = get_json_schema(element)

    return {
        'type': ['null', 'object'],
        'additionalProperties': False,
        'properties': properties
    }

def combine_object_schemas(schemas):
    properties = {}
    for schema in schemas:
        for prop, prop_schema in schema['properties'].items():
            properties[prop] = prop_schema
    return {
        'type': ['object'],
        'properties': properties
    }

def normalize_abstract_types(inherited_types, type_map):
    for base_type, types in inherited_types.items():
        if base_type in type_map:
            schemas = []
            for inherited_type in types:
                if inherited_type in type_map:
                    schemas.append(type_map[inherited_type])
            schemas.append(type_map[base_type])

            if base_type in TOP_LEVEL_CORE_OBJECTS:
                type_map[base_type] = combine_object_schemas(schemas)
            else:
                type_map[base_type] = {'anyOf': schemas}

def fill_in_nested_types(type_map, schema):
    # Prepare nested schema for catalog
    if 'properties' in schema:
        for prop, descriptor in schema['properties'].items():
            schema['properties'][prop] = fill_in_nested_types(type_map, descriptor)
    elif 'items' in schema:
        schema['items'] = fill_in_nested_types(type_map, schema['items'])
    else:
        if isinstance(schema, str) and schema in type_map:
            return type_map[schema]
    return schema

@bing_ads_error_handling
def get_type_map(client):
    inherited_types = {}
    type_map = {}
    for type_tuple in client.soap_client.sd[0].types:
        _type = type_tuple[0]
        qname = _type.qname[1]
        if 'https://bingads.microsoft.com' not in qname and \
           'http://schemas.datacontract.org' not in qname:
            continue
        type_map[_type.name] = wsdl_type_to_schema(inherited_types, _type)

    # Temporary change to add a new wsdl property to the type map so it can be resolved below and filled in with correct JSON schema
    if not type_map.get('KeyValueOfstringbase64Binary'):
        type_map['KeyValueOfstringbase64Binary'] = {'additionalProperties': False, 'type': ['null', 'object'], 'properties': {'key': {'type': ['null', 'string']}, 'value': {'type': ['null', 'string']}}}
    normalize_abstract_types(inherited_types, type_map)

    for _type, schema in type_map.items():
        type_map[_type] = fill_in_nested_types(type_map, schema)

    return type_map

def get_stream_def(stream_name, schema, stream_metadata=None, pks=None, replication_keys=None):
    '''Generate schema with metadata for the given stream.'''

    stream_def = {
        'tap_stream_id': stream_name,
        'stream': stream_name,
        'schema': schema
    }

    if pks:
        stream_def['key_properties'] = pks

    # Defining standered matadata
    mdata = metadata.to_map(
            metadata.get_standard_metadata(
                schema = schema,
                key_properties = pks,
                valid_replication_keys = replication_keys,
                replication_method = 'INCREMENTAL' if replication_keys else 'FULL_TABLE'
            )
        )

    # Marking replication key as automatic
    if replication_keys:
        for replication_key in replication_keys:
            mdata = metadata.write(mdata, ('properties', replication_key), 'inclusion', 'automatic')

    # For the report streams, we have some stream_metadata which have list fields to make automatic and a list of file exclusions.
    if stream_metadata:
        for field in stream_metadata:
            if field.get('metadata').get('inclusion') == 'automatic':
                mdata = metadata.write(mdata, tuple(field.get('breadcrumb')), 'inclusion', 'automatic')
            if field.get('metadata').get('fieldExclusions'):
                mdata = metadata.write(mdata, tuple(field.get('breadcrumb')), 'fieldExclusions', field.get('metadata').get('fieldExclusions'))

    stream_def['metadata'] = metadata.to_list(mdata)

    return stream_def

def get_core_schema(client, obj):
    # Get object's schema
    type_map = get_type_map(client)
    return type_map[obj]

def discover_core_objects():
    core_object_streams = []

    LOGGER.info('Initializing CustomerManagementService client - Loading WSDL')
    client = CustomServiceClient('CustomerManagementService')

    # Load Account's schemas
    account_schema = get_core_schema(client, 'AdvertiserAccount')
    core_object_streams.append(
        # After new standard metadata changes we are getting Id as primary key only
        # while earlier we were getting Id and LastModifiedTime both because of the coding mistake
        # but we are writing Id only while writing the schema (func: sync_accounts_stream) in sync mode,
        # Hence we are keeping ID only in pks.
        get_stream_def('accounts', account_schema, pks=['Id'], replication_keys=['LastModifiedTime']))

    LOGGER.info('Initializing CampaignManagementService client - Loading WSDL')
    client = CustomServiceClient('CampaignManagementService')

    # Load Campaign's schemas
    campaign_schema = get_core_schema(client, 'Campaign')
    core_object_streams.append(get_stream_def('campaigns', campaign_schema, pks=['Id']))

    # Load AdGroup's schemas
    ad_group_schema = get_core_schema(client, 'AdGroup')
    core_object_streams.append(get_stream_def('ad_groups', ad_group_schema, pks=['Id']))

    # Load Ad's schemas
    ad_schema = get_core_schema(client, 'Ad')
    core_object_streams.append(get_stream_def('ads', ad_schema, pks=['Id']))

    return core_object_streams

@bing_ads_error_handling
def get_report_schema(client, report_name):
    # Load report's schemas
    column_obj_name = '{}Column'.format(report_name)

    report_columns_type = None
    for _type in client.soap_client.sd[0].types:
        if _type[0].name == column_obj_name:
            report_columns_type = _type[0]
            break

    report_columns = map(lambda x: x.name, report_columns_type.rawchildren[0].rawchildren)

    properties = {}
    for column in report_columns:
        # Prepare json `type` for schema of streams e.g. "type": ["null","integer"]
        if column in reports.REPORTING_FIELD_TYPES:
            _type = reports.REPORTING_FIELD_TYPES[column]
        else:
            _type = 'string'

        if _type == 'datetime':
            col_schema = {'type': ['null', 'string'], 'format': 'date-time'}
        else:
            col_schema = {'type': ['null', _type]}

        properties[column] = col_schema

    properties['_sdc_report_datetime'] = {
        'type': 'string',
        'format': 'date-time'
    }

    return {
        'properties': properties,
        'additionalProperties': False,
        'type': 'object'
    }

def metadata_fn(report_name, field, required_fields):
    if field in required_fields:
        # Set automatic inclusion for all required fields.
        mdata = {"metadata": {"inclusion": "automatic"}, "breadcrumb": ["properties", field]}
    else:
        mdata = {"metadata": {"inclusion": "available"}, "breadcrumb": ["properties", field]}

    if EXCLUSIONS.get(report_name):
        #'fieldExclusions' property that contain fields that cannot be selected with the associated group.
        for group_set in EXCLUSIONS[report_name]:
            if field in group_set['Attributes']:
                mdata['metadata']['fieldExclusions'] = [
                    *mdata['metadata'].get('fieldExclusions', []),
                    *[['properties', p] for p in group_set['ImpressionSharePerformanceStatistics']]
                ]
            if field in group_set['ImpressionSharePerformanceStatistics']:
                mdata['metadata']['fieldExclusions'] = [
                    *mdata['metadata'].get('fieldExclusions', []),
                    *[['properties', p] for p in group_set['Attributes']]
                ]

    return mdata

def get_report_metadata(report_name, report_schema):
    # Load metadata for report streams
    if report_name in reports.REPORT_SPECIFIC_REQUIRED_FIELDS:
        required_fields = (
            reports.REPORT_REQUIRED_FIELDS +
            reports.REPORT_SPECIFIC_REQUIRED_FIELDS[report_name])
    else:
        required_fields = reports.REPORT_REQUIRED_FIELDS

    return list(map(
        lambda field: metadata_fn(report_name, field, required_fields),
        report_schema['properties']))

def discover_reports():
    # Discover mode for report streams
    report_streams = []
    LOGGER.info('Initializing ReportingService client - Loading WSDL')
    client = CustomServiceClient('ReportingService')
    type_map = get_type_map(client)
    report_column_regex = r'^(?!ArrayOf)(.+Report)Column$'

    for type_name in type_map:
        match = re.match(report_column_regex, type_name)
        if match and match.groups()[0] in reports.REPORT_WHITELIST:
            report_name = match.groups()[0]
            stream_name = stringcase.snakecase(report_name)
            report_schema = get_report_schema(client, report_name)
            report_metadata = get_report_metadata(report_name, report_schema)
            report_stream_def = get_stream_def(
                stream_name,
                report_schema,
                stream_metadata=report_metadata)
            report_streams.append(report_stream_def)

    return report_streams

def test_credentials(account_ids):
    if not account_ids:
        raise Exception('At least one id in account_ids is required to test authentication')

    create_sdk_client('CustomerManagementService', account_ids[0]) # Create bingads sdk client

def do_discover(account_ids):
    # Discover schemas and dump in STDOUT
    LOGGER.info('Testing authentication')
    test_credentials(account_ids)# Test provided credentails

    LOGGER.info('Discovering core objects')
    core_object_streams = discover_core_objects()

    LOGGER.info('Discovering reports')
    report_streams = discover_reports()

    json.dump({'streams': core_object_streams + report_streams}, sys.stdout, indent=2)


def check_for_invalid_selections(prop, mdata, invalid_selections):
    # Check whether fields 'fieldExclusions' selected or not
    field_exclusions = metadata.get(mdata, ('properties', prop), 'fieldExclusions')
    is_prop_selected = metadata.get(mdata, ('properties', prop), 'selected')
    if field_exclusions and is_prop_selected:
        for exclusion in field_exclusions:
            is_exclusion_selected = metadata.get(mdata, tuple(exclusion), 'selected')
            if not is_exclusion_selected:
                continue
            if invalid_selections.get(prop):
                invalid_selections[prop].append(exclusion[1])
            else:
                invalid_selections[prop] = [exclusion[1]]


def get_selected_fields(catalog_item, exclude=None):
    # Get selected fields only
    if not catalog_item.metadata:
        return None

    if not exclude:
        exclude = []

    mdata = metadata.to_map(catalog_item.metadata)
    selected_fields = []
    invalid_selections = {}
    for prop in catalog_item.schema.properties:
        check_for_invalid_selections(prop, mdata, invalid_selections)
        if prop not in exclude and \
           ((catalog_item.key_properties and prop in catalog_item.key_properties) or \
            metadata.get(mdata, ('properties', prop), 'inclusion') == 'automatic' or \
            metadata.get(mdata, ('properties', prop), 'selected') is True):
            selected_fields.append(prop)

    # Raise Exception if incompatible fields are selected
    if any(invalid_selections):
        raise Exception("Invalid selections for field(s) - {{ FieldName: [IncompatibleFields] }}:\n{}".format(json.dumps(invalid_selections, indent=4)))
    return selected_fields

def filter_selected_fields(selected_fields, obj):
    # Return only selected fields
    if selected_fields:
        return {key:value for key, value in obj.items() if key in selected_fields}
    return obj

def filter_selected_fields_many(selected_fields, objs):
    if selected_fields:
        return [filter_selected_fields(selected_fields, obj) for obj in objs]
    return objs

@bing_ads_error_handling
def sync_accounts_stream(account_ids, catalog_item):
    selected_fields = get_selected_fields(catalog_item)
    accounts = []

    LOGGER.info('Initializing CustomerManagementService client - Loading WSDL')
    client = CustomServiceClient('CustomerManagementService')
    account_schema = get_core_schema(client, 'AdvertiserAccount')
    singer.write_schema('accounts', account_schema, ['Id'])

    for account_id in account_ids:
        # Loop over the multiple account_ids
        client = create_sdk_client('CustomerManagementService', account_id)
        # Get account data
        response = client.GetAccount(AccountId=account_id)
        accounts.append(sobject_to_dict(response))

    accounts_bookmark = singer.get_bookmark(STATE, 'accounts', 'last_record')
    if accounts_bookmark:
        accounts = list(
            filter(lambda x: x is not None and x['LastModifiedTime'] >= accounts_bookmark,
                   accounts))

    max_accounts_last_modified = max([x['LastModifiedTime'] for x in accounts])

    with metrics.record_counter('accounts') as counter:
        # Write only selected fields
        singer.write_records('accounts', filter_selected_fields_many(selected_fields, accounts))
        counter.increment(len(accounts))

    singer.write_bookmark(STATE, 'accounts', 'last_record', max_accounts_last_modified)
    singer.write_state(STATE)

@bing_ads_error_handling
def sync_campaigns(client, account_id, selected_streams): # pylint: disable=inconsistent-return-statements
    # CampaignType defaults to 'Search', but there are other types of campaigns
    response = client.GetCampaignsByAccountId(AccountId=account_id, CampaignType='Search Shopping DynamicSearchAds')
    response_dict = sobject_to_dict(response)
    if 'Campaign' in response_dict:
        campaigns = response_dict['Campaign']

        if 'campaigns' in selected_streams:
            selected_fields = get_selected_fields(selected_streams['campaigns'])
            singer.write_schema('campaigns', get_core_schema(client, 'Campaign'), ['Id'])
            with metrics.record_counter('campaigns') as counter:
                singer.write_records('campaigns',
                                     filter_selected_fields_many(selected_fields, campaigns))
                counter.increment(len(campaigns))

        return map(lambda x: x['Id'], campaigns)

@bing_ads_error_handling
def sync_ad_groups(client, account_id, campaign_ids, selected_streams):
    ad_group_ids = []
    for campaign_id in campaign_ids:
        response = client.GetAdGroupsByCampaignId(CampaignId=campaign_id)
        response_dict = sobject_to_dict(response)

        if 'AdGroup' in response_dict:
            ad_groups = sobject_to_dict(response)['AdGroup']

            if 'ad_groups' in selected_streams:
                LOGGER.info('Syncing AdGroups for Account: %s, Campaign: %s',
                    account_id, campaign_id)
                selected_fields = get_selected_fields(selected_streams['ad_groups'])
                singer.write_schema('ad_groups', get_core_schema(client, 'AdGroup'), ['Id'])
                with metrics.record_counter('ad_groups') as counter:
                    singer.write_records('ad_groups',
                                         filter_selected_fields_many(selected_fields, ad_groups))
                    counter.increment(len(ad_groups))

            ad_group_ids += list(map(lambda x: x['Id'], ad_groups))
    return ad_group_ids

@bing_ads_error_handling
def sync_ads(client, selected_streams, ad_group_ids):
    for ad_group_id in ad_group_ids:
        response = client.GetAdsByAdGroupId(
            AdGroupId=ad_group_id,
            AdTypes={
                'AdType': [
                    'AppInstall',
                    'DynamicSearch',
                    'ExpandedText',
                    'Product',
                    'Text',
                    'Image'
                ]
            })
        response_dict = sobject_to_dict(response)

        if 'Ad' in response_dict:
            selected_fields = get_selected_fields(selected_streams['ads'])
            singer.write_schema('ads', get_core_schema(client, 'Ad'), ['Id'])
            with metrics.record_counter('ads') as counter:
                ads = response_dict['Ad']
                singer.write_records('ads', filter_selected_fields_many(selected_fields, ads))
                counter.increment(len(ads))

def sync_core_objects(account_id, selected_streams):
    client = create_sdk_client('CampaignManagementService', account_id)

    LOGGER.info('Syncing Campaigns for Account: %s', account_id)
    campaign_ids = sync_campaigns(client, account_id, selected_streams)

    if campaign_ids and ('ad_groups' in selected_streams or 'ads' in selected_streams):
        ad_group_ids = sync_ad_groups(client, account_id, campaign_ids, selected_streams)
        if 'ads' in selected_streams:
            LOGGER.info('Syncing Ads for Account: %s', account_id)
            sync_ads(client, selected_streams, ad_group_ids)

def type_report_row(row):
    # Check and convert report's field to valid type
    for field_name, value in row.items():
        value = value.strip()
        if value == '':
            value = None

        if value is not None and field_name in reports.REPORTING_FIELD_TYPES:
            _type = reports.REPORTING_FIELD_TYPES[field_name]
            if _type == 'integer':
                if value == '--':
                    value = 0
                else:
                    value = int(value.replace(',', ''))
            elif _type == 'number':
                if value == '--':
                    value = 0.0
                else:
                    value = float(value.replace('%', '').replace(',', ''))
            elif _type in ['date', 'datetime']:
                value = arrow.get(value).isoformat()

        row[field_name] = value

@bing_ads_error_handling
def generate_poll_report(client, request_id):
    """
        Retry following errors for 60 seconds,
        socket.timeout, ConnectionError, internal server error(500-range), SSLError, HTTPError(408), Transport error.
        Raise the error directly for all errors except mentioned above errors.
    """
    return client.PollGenerateReport(request_id)

async def poll_report(client, account_id, report_name, start_date, end_date, request_id):
    # Get download_url of generated report
    download_url = None
    with metrics.job_timer('generate_report'):
        for i in range(1, MAX_NUM_REPORT_POLLS + 1):
            LOGGER.info('Polling report job %s/%s - %s - from %s to %s',
                i,
                MAX_NUM_REPORT_POLLS,
                report_name,
                start_date,
                end_date)
            # As in the async method backoff does not work directly we created a separate method to handle it.
            response = generate_poll_report(client, request_id)
            if response.Status == 'Error':
                LOGGER.warn('Error polling %s for account %s with request id %s',
                            report_name, account_id, request_id)
                return False, None
            if response.Status == 'Success':
                if response.ReportDownloadUrl:
                    download_url = response.ReportDownloadUrl
                else:
                    LOGGER.info('No results for report: %s - from %s to %s',
                                report_name,
                                start_date,
                                end_date)
                break

            if i == MAX_NUM_REPORT_POLLS:
                LOGGER.info('Generating report timed out: %s - from %s to %s',
                            report_name,
                            start_date,
                            end_date)
            else:
                await asyncio.sleep(REPORT_POLL_SLEEP)

    return True, download_url

def log_retry_attempt(details):
    LOGGER.info('Retrieving report timed out, triggering retry #%d', details.get('tries'))

# retry the request for 5 times when Timeout error occurs
@backoff.on_exception(backoff.constant,
                      (requests.exceptions.Timeout ,requests.exceptions.ConnectionError),
                      max_tries=5,
                      on_backoff=log_retry_attempt)
def stream_report(stream_name, report_name, url, report_time):
    # Write stream report with backoff of ConnectionError
    with metrics.http_request_timer('download_report'):
        # Set request timeout with config param `request_timeout`.
        request_timeout = get_request_timeout()
        response = SESSION.get(url, headers={'User-Agent': get_user_agent()}, timeout=request_timeout)

    if response.status_code != 200:
        raise Exception('Non-200 ({}) response downloading report: {}'.format(
            response.status_code, report_name))
    with ZipFile(io.BytesIO(response.content)) as zip_file:
        with zip_file.open(zip_file.namelist()[0]) as binary_file:
            with io.TextIOWrapper(binary_file, encoding='utf-8') as csv_file:
                # handle control character at the start of the file and extra next line
                header_line = next(csv_file)[1:-1]
                headers = header_line.replace('"', '').split(',')

                reader = csv.DictReader((line.replace('\0', '') for line in csv_file), fieldnames=headers)

                with metrics.record_counter(stream_name) as counter:
                    for row in reader:
                        type_report_row(row)
                        row['_sdc_report_datetime'] = report_time
                        singer.write_record(stream_name, row)
                        counter.increment()

def get_report_interval(state_key):
    # Return start_date and end_date for report interval
    report_max_days = int(CONFIG.get('report_max_days', 30)) # pylint: disable=unused-variable
    conversion_window = int(CONFIG.get('conversion_window', -30))

    config_start_date = arrow.get(CONFIG.get('start_date'))
    config_end_date = arrow.get(CONFIG.get('end_date')).floor('day')

    bookmark_end_date = singer.get_bookmark(STATE, state_key, 'date')
    conversion_min_date = arrow.get().floor('day').shift(days=conversion_window) # 30 days before the current date

    start_date = None
    if bookmark_end_date:
        start_date = arrow.get(bookmark_end_date).floor('day').shift(days=1)
    else:
        # Will default to today
        start_date = config_start_date.floor('day')

    start_date = min(start_date, conversion_min_date) # minimum of start_date or conversion_min_date

    end_date = min(config_end_date, arrow.get().floor('day')) # minimum of end_date or current date

    return start_date, end_date

async def sync_report(client, account_id, report_stream):
    report_max_days = int(CONFIG.get('report_max_days', 30)) # Date window size

    state_key = '{}_{}'.format(account_id, report_stream.stream)

    start_date, end_date = get_report_interval(state_key)

    LOGGER.info('Generating %s reports for account %s between %s - %s',
        report_stream.stream, account_id, start_date, end_date)

    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = min(
            current_start_date.shift(days=report_max_days),
            end_date
        )
        try:
            success = await sync_report_interval(client,
                                                 account_id,
                                                 report_stream,
                                                 current_start_date,
                                                 current_end_date)
        except InvalidDateRangeEnd as ex: # pylint: disable=unused-variable
            LOGGER.warn("Bing reported that the requested report date range ended outside of "
                        "their data retention period. Skipping to next range...")
            success = True

        if success:
            current_start_date = current_end_date.shift(days=1)

async def sync_report_interval(client, account_id, report_stream,
                               start_date, end_date):
    state_key = '{}_{}'.format(account_id, report_stream.stream)
    report_name = stringcase.pascalcase(report_stream.stream)

    report_schema = get_report_schema(client, report_name)
    singer.write_schema(report_stream.stream, report_schema, [])

    report_time = arrow.get().isoformat()

    # Get request id to retrieve report stream
    request_id = get_report_request_id(client, account_id, report_stream,
                                       report_name, start_date, end_date,
                                       state_key)

    singer.write_bookmark(STATE, state_key, 'request_id', request_id)
    singer.write_state(STATE)

    try:
        # Get success status and download url
        success, download_url = await poll_report(client, account_id, report_name,
                                                  start_date, end_date, request_id)

    except Exception as some_error: # pylint: disable=broad-except,unused-variable
        LOGGER.info('The request_id %s for %s is invalid, generating a new one',
                    request_id,
                    state_key)
        request_id = get_report_request_id(client, account_id, report_stream,
                                           report_name, start_date, end_date,
                                           state_key, force_refresh=True)

        singer.write_bookmark(STATE, state_key, 'request_id', request_id)
        singer.write_state(STATE)

        success, download_url = await poll_report(client, account_id, report_name,
                                                  start_date, end_date, request_id)

    if success and download_url: # pylint: disable=no-else-return
        LOGGER.info('Streaming report: %s for account %s - from %s to %s',
                    report_name, account_id, start_date, end_date)

        stream_report(report_stream.stream,
                      report_name,
                      download_url,
                      report_time)
        singer.write_bookmark(STATE, state_key, 'request_id', None)
        singer.write_bookmark(STATE, state_key, 'date', end_date.isoformat())
        singer.write_state(STATE)
        return True
    elif success and not download_url:
        LOGGER.info('No data for report: %s for account %s - from %s to %s',
                    report_name, account_id, start_date, end_date)
        singer.write_bookmark(STATE, state_key, 'request_id', None)
        singer.write_bookmark(STATE, state_key, 'date', end_date.isoformat())
        singer.write_state(STATE)
        return True
    else:
        LOGGER.info('Unsuccessful request for report: %s for account %s - from %s to %s',
                    report_name, account_id, start_date, end_date)
        singer.write_bookmark(STATE, state_key, 'request_id', None)
        singer.write_state(STATE)
        return False


@bing_ads_error_handling
def get_report_request_id(client, account_id, report_stream, report_name,
                          start_date, end_date, state_key, force_refresh=False):
    saved_request_id = singer.get_bookmark(STATE, state_key, 'request_id')
    if not force_refresh and saved_request_id is not None:
        LOGGER.info('Resuming polling for account %s: %s',
                    account_id, report_name)
        return saved_request_id

    report_request = build_report_request(client, account_id, report_stream,
                                          report_name, start_date, end_date)

    return client.SubmitGenerateReport(report_request)


@bing_ads_error_handling
def build_report_request(client, account_id, report_stream, report_name,
                         start_date, end_date):
    LOGGER.info('Syncing report for account %s: %s - from %s to %s',
                account_id, report_name, start_date, end_date)

    report_request = client.factory.create('{}Request'.format(report_name))
    report_request.Format = 'Csv'
    report_request.Aggregation = 'Daily'
    report_request.ExcludeReportHeader = True
    report_request.ExcludeReportFooter = True

    scope = client.factory.create('AccountThroughAdGroupReportScope')
    scope.AccountIds = {'long': [account_id]}
    report_request.Scope = scope

    excluded_fields = ['_sdc_report_datetime']

    selected_fields = get_selected_fields(report_stream,
                                          exclude=excluded_fields)

    report_columns = client.factory.create(
        'ArrayOf{}Column'.format(report_name)
    )
    getattr(report_columns, '{}Column'.format(report_name)) \
        .append(selected_fields)
    report_request.Columns = report_columns

    request_start_date = client.factory.create('Date')
    request_start_date.Day = start_date.day
    request_start_date.Month = start_date.month
    request_start_date.Year = start_date.year

    request_end_date = client.factory.create('Date')
    request_end_date.Day = end_date.day
    request_end_date.Month = end_date.month
    request_end_date.Year = end_date.year

    request_time_zone = client.factory.create('ReportTimeZone')

    report_time = client.factory.create('ReportTime')
    report_time.CustomDateRangeStart = request_start_date
    report_time.CustomDateRangeEnd = request_end_date
    report_time.PredefinedTime = None
    report_time.ReportTimeZone = request_time_zone.GreenwichMeanTimeDublinEdinburghLisbonLondon

    report_request.Time = report_time

    return report_request

async def sync_reports(account_id, catalog):
    # Sync report stream
    client = create_sdk_client('ReportingService', account_id)

    reports_to_sync = filter(lambda x: x.is_selected() and x.stream[-6:] == 'report',
                             catalog.streams)

    sync_report_tasks = [
        sync_report(client, account_id, report_stream)
        for report_stream in reports_to_sync
    ]
    await asyncio.gather(*sync_report_tasks)

async def sync_account_data(account_id, catalog, selected_streams):
    all_core_streams = {
        stringcase.snakecase(o) + 's' for o in TOP_LEVEL_CORE_OBJECTS
    }
    all_report_streams = {
        stringcase.snakecase(r) for r in reports.REPORT_WHITELIST
    }

    if len(all_core_streams & set(selected_streams)):
        # Sync all core objects streams
        LOGGER.info('Syncing core objects')
        sync_core_objects(account_id, selected_streams)

    if len(all_report_streams & set(selected_streams)):
        # Sync all report streams
        LOGGER.info('Syncing reports')
        await sync_reports(account_id, catalog)

# run sync mode
async def do_sync_all_accounts(account_ids, catalog):
    selected_streams = {}
    for stream in filter(lambda x: x.is_selected(), catalog.streams):
        selected_streams[stream.tap_stream_id] = stream

    if 'accounts' in selected_streams:
        LOGGER.info('Syncing Accounts')
        sync_accounts_stream(account_ids, selected_streams['accounts'])

    sync_account_data_tasks = [
        sync_account_data(account_id, catalog, selected_streams)
        for account_id in account_ids
    ]
    await asyncio.gather(*sync_account_data_tasks)

async def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE.update(args.state)
    account_ids = CONFIG['account_ids'].split(",")

    if args.discover: # Discover mode
        do_discover(account_ids)
        LOGGER.info("Discovery complete")
    elif args.catalog: # Sync mode
        await do_sync_all_accounts(account_ids, args.catalog)
        LOGGER.info("Sync Completed")
    else:
        LOGGER.info("No catalog was provided")

def main():
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main_impl())
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
