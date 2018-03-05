#!/usr/bin/env python3

import asyncio
import json
import csv
import sys
import re
import io
from datetime import datetime
from zipfile import ZipFile

import singer
from singer import utils, metadata, metrics
from bingads import AuthorizationData, OAuthWebAuthCodeGrant, ServiceClient
import suds
from suds.sudsobject import asdict
import stringcase
import requests
import arrow

from tap_bing_ads import reports

LOGGER = singer.get_logger()

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

ARRAY_TYPE_REGEX = r'ArrayOf([A-Za-z]+)'

def get_user_agent():
    return CONFIG.get('user_agent', DEFAULT_USER_AGENT)

def log_service_call(service_method):
    def wrapper(*args, **kwargs):
        log_args = list(map(lambda arg: str(arg).replace('\n', '\\n'), args)) + \
                   list(map(lambda kv: '{}={}'.format(*kv), kwargs.items()))
        LOGGER.info('Calling: {}({})'.format(service_method.name, ','.join(log_args)))
        with metrics.http_request_timer(service_method.name):
            return service_method(*args, **kwargs)
    return wrapper

class CustomServiceClient(ServiceClient):
    def __getattr__(self, name):
        service_method = super(CustomServiceClient, self).__getattr__(name)
        return log_service_call(service_method)

    def set_options(self, **kwargs):
        self._options = kwargs
        kwargs = ServiceClient._ensemble_header(self.authorization_data, **self._options)
        kwargs['headers']['User-Agent'] = get_user_agent()
        self._soap_client.set_options(**kwargs)

def create_sdk_client(service, account_id):
    LOGGER.info('Creating SOAP client with OAuth refresh credentials')

    authentication = OAuthWebAuthCodeGrant(
        CONFIG['oauth_client_id'],
        CONFIG['oauth_client_secret'],
        '') ## redirect URL not needed for refresh token

    authentication.request_oauth_tokens_by_refresh_token(CONFIG['refresh_token'])

    authorization_data = AuthorizationData(
        account_id=account_id,
        customer_id=CONFIG['customer_id'],
        developer_token=CONFIG['developer_token'],
        authentication=authentication)

    return CustomServiceClient(service, authorization_data)

def sobject_to_dict(obj):
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
    if xml_type == 'boolean':
        return 'boolean'
    if xml_type in ['decimal', 'float', 'double']:
        return 'number'
    if xml_type in ['long', 'int']:
        return 'integer'

    return 'string'

def get_json_schema(element):
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
    xml_type = re.match(ARRAY_TYPE_REGEX, array_type).groups()[0]
    json_type = xml_to_json_type(xml_type)
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
    if isinstance(wsdl_type.rawchildren[0].rawchildren[0], suds.xsd.sxbasic.Extension):
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
    if wsdl_type.root.name == 'simpleType':
        return get_json_schema(wsdl_type)

    elements = get_complex_type_elements(inherited_types, wsdl_type)

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
    if 'properties' in schema:
        for prop, descriptor in schema['properties'].items():
            schema['properties'][prop] = fill_in_nested_types(type_map, descriptor)
    elif 'items' in schema:
        schema['items'] = fill_in_nested_types(type_map, schema['items'])
    else:
        if isinstance(schema, str) and schema in type_map:
            return type_map[schema]
    return schema

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

    normalize_abstract_types(inherited_types, type_map)

    for _type, schema in type_map.items():
        type_map[_type] = fill_in_nested_types(type_map, schema)

    return type_map

def get_stream_def(stream_name, schema, stream_metadata=None, pks=None, replication_key=None):
    stream_def = {
        'tap_stream_id': stream_name,
        'stream': stream_name,
        'schema': schema
    }

    excluded_inclusion_fields = []
    if pks:
        stream_def['key_properties'] = pks
        excluded_inclusion_fields = pks

    if replication_key:
        stream_def['replication_key'] = replication_key
        stream_def['replication_method'] = 'INCREMENTAL'
        excluded_inclusion_fields += [replication_key]
    else:
        stream_def['replication_method'] = 'FULL_TABLE'

    if stream_metadata:
        stream_def['metadata'] = stream_metadata
    else:
        stream_def['metadata'] = list(map(
          lambda field: {"metadata": {"inclusion": "available"}, "breadcrumb": ["properties", field]},
          (schema['properties'].keys() - excluded_inclusion_fields)))

    return stream_def

def get_core_schema(client, obj):
    type_map = get_type_map(client)
    return type_map[obj]

def discover_core_objects():
    core_object_streams = []

    LOGGER.info('Initializing CustomerManagementService client - Loading WSDL')
    client = CustomServiceClient('CustomerManagementService')

    account_schema = get_core_schema(client, 'AdvertiserAccount')
    core_object_streams.append(
        get_stream_def('accounts', account_schema, pks=['Id'], replication_key='LastModifiedTime'))

    LOGGER.info('Initializing CampaignManagementService client - Loading WSDL')
    client = CustomServiceClient('CampaignManagementService')

    campaign_schema = get_core_schema(client, 'Campaign')
    core_object_streams.append(get_stream_def('campaigns', campaign_schema, pks=['Id']))

    ad_group_schema = get_core_schema(client, 'AdGroup')
    core_object_streams.append(get_stream_def('ad_groups', ad_group_schema, pks=['Id']))

    ad_schema = get_core_schema(client, 'Ad')
    core_object_streams.append(get_stream_def('ads', ad_schema, pks=['Id']))

    return core_object_streams

def get_report_schema(client, report_name):
    column_obj_name = '{}Column'.format(report_name)

    report_columns_type = None
    for _type in client.soap_client.sd[0].types:
        if _type[0].name == column_obj_name:
            report_columns_type = _type[0]
            break

    report_columns = map(lambda x: x.name, report_columns_type.rawchildren[0].rawchildren)

    if report_name in reports.EXTRA_FIELDS:
        report_columns = list(report_columns) + reports.EXTRA_FIELDS[report_name]

    properties = {}
    for column in report_columns:
        if column in reports.REPORTING_FIELD_TYPES:
            _type = reports.REPORTING_FIELD_TYPES[column]
        else:
            _type = 'string'

        # TimePeriod's column name changes depending on aggregation level
        # This tap always uses daily aggregation
        if column == 'TimePeriod':
            column = 'GregorianDate'
            _type = 'datetime'

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

def inclusion_fn(field, required_fields):
    if field in required_fields:
        return {"metadata": {"inclusion": "automatic"}, "breadcrumb": ["properties", field]}
    else:
        return {"metadata": {"inclusion": "available"}, "breadcrumb": ["properties", field]}

def get_report_metadata(report_name, report_schema):
    if report_name in reports.EXTRA_FIELDS and \
       report_name in reports.REPORT_SPECIFIC_REQUIRED_FIELDS:
        required_fields = (
            reports.REPORT_REQUIRED_FIELDS +
            reports.REPORT_SPECIFIC_REQUIRED_FIELDS[report_name] +
            reports.EXTRA_FIELDS[report_name])
    elif report_name in reports.REPORT_SPECIFIC_REQUIRED_FIELDS:
        required_fields = (
            reports.REPORT_REQUIRED_FIELDS +
            reports.REPORT_SPECIFIC_REQUIRED_FIELDS[report_name])
    else:
        required_fields = reports.REPORT_REQUIRED_FIELDS

    return list(map(
        lambda field: inclusion_fn(field, required_fields),
        report_schema['properties']))

def discover_reports():
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

    create_sdk_client('CustomerManagementService', account_ids[0])

def do_discover(account_ids):
    LOGGER.info('Testing authentication')
    test_credentials(account_ids)

    LOGGER.info('Discovering core objects')
    core_object_streams = discover_core_objects()

    LOGGER.info('Discovering reports')
    report_streams = discover_reports()

    json.dump({'streams': core_object_streams + report_streams}, sys.stdout, indent=2)

def get_selected_fields(catalog_item, exclude=None):
    if not catalog_item.metadata:
        return None

    if not exclude:
        exclude = []

    mdata = metadata.to_map(catalog_item.metadata)
    selected_fields = []
    for prop in catalog_item.schema.properties:
        if prop not in exclude and \
           ((catalog_item.key_properties and prop in catalog_item.key_properties) or \
            metadata.get(mdata, ('properties', prop), 'inclusion') == 'automatic' or \
            metadata.get(mdata, ('properties', prop), 'selected') is True):
            selected_fields.append(prop)
    return selected_fields

def filter_selected_fields(selected_fields, obj):
    if selected_fields:
        return {key:value for key, value in obj.items() if key in selected_fields}
    return obj

def filter_selected_fields_many(selected_fields, objs):
    if selected_fields:
        return [filter_selected_fields(selected_fields, obj) for obj in objs]
    return objs

def sync_accounts_stream(account_ids, catalog_item):
    selected_fields = get_selected_fields(catalog_item)
    accounts = []

    LOGGER.info('Initializing CustomerManagementService client - Loading WSDL')
    client = CustomServiceClient('CustomerManagementService')
    account_schema = get_core_schema(client, 'AdvertiserAccount')
    singer.write_schema('accounts', account_schema, ['Id'])

    for account_id in account_ids:
        client = create_sdk_client('CustomerManagementService', account_id)
        response = client.GetAccount(AccountId=account_id)
        accounts.append(sobject_to_dict(response))

    accounts_bookmark = singer.get_bookmark(STATE, 'accounts', 'last_record')
    if accounts_bookmark:
        accounts = list(
            filter(lambda x: x['LastModifiedTime'] >= accounts_bookmark,
                   accounts))

    max_accounts_last_modified = max([x['LastModifiedTime'] for x in accounts])

    with metrics.record_counter('accounts') as counter:
        singer.write_records('accounts', filter_selected_fields_many(selected_fields, accounts))
        counter.increment(len(accounts))

    singer.write_bookmark(STATE, 'accounts', 'last_record', max_accounts_last_modified)
    singer.write_state(STATE)

def sync_campaigns(client, account_id, selected_streams):
    response = client.GetCampaignsByAccountId(AccountId=account_id)
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

def sync_ad_groups(client, account_id, campaign_ids, selected_streams):
    ad_group_ids = []
    for campaign_id in campaign_ids:
        response = client.GetAdGroupsByCampaignId(CampaignId=campaign_id)
        response_dict = sobject_to_dict(response)

        if 'AdGroup' in response_dict:
            ad_groups = sobject_to_dict(response)['AdGroup']

            if 'ad_groups' in selected_streams:
                LOGGER.info('Syncing AdGroups for Account: {}, Campaign: {}'.format(
                    account_id, campaign_id))
                selected_fields = get_selected_fields(selected_streams['ad_groups'])
                singer.write_schema('ad_groups', get_core_schema(client, 'AdGroup'), ['Id'])
                with metrics.record_counter('ad_groups') as counter:
                    singer.write_records('ad_groups',
                                         filter_selected_fields_many(selected_fields, ad_groups))
                    counter.increment(len(ad_groups))

            ad_group_ids += list(map(lambda x: x['Id'], ad_groups))
    return ad_group_ids

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

    LOGGER.info('Syncing Campaigns for Account: {}'.format(account_id))
    campaign_ids = sync_campaigns(client, account_id, selected_streams)

    if campaign_ids and ('ad_groups' in selected_streams or 'ads' in selected_streams):
        ad_group_ids = sync_ad_groups(client, account_id, campaign_ids, selected_streams)
        if 'ads' in selected_streams:
            LOGGER.info('Syncing Ads for Account: {}'.format(account_id))
            sync_ads(client, selected_streams, ad_group_ids)

def normalize_report_column_names(row):
    for alias_name in reports.ALIASES:
        if alias_name in row:
            row[reports.ALIASES[alias_name]] = row[alias_name]
            del row[alias_name]

def type_report_row(row):
    for field_name, value in row.items():
        value = value.strip()
        if value == '':
            value = None

        if value is not None and field_name in reports.REPORTING_FIELD_TYPES:
            _type = reports.REPORTING_FIELD_TYPES[field_name]
            if _type == 'integer':
                value = int(value.replace(',', ''))
            elif _type == 'number':
                value = float(value.replace('%', '').replace(',', ''))
            elif _type in ['date', 'datetime']:
                value = arrow.get(value).isoformat()

        row[field_name] = value

async def poll_report(client, report_name, start_date, end_date, request_id):
    download_url = None
    with metrics.job_timer('generate_report'):
        for i in range(1, MAX_NUM_REPORT_POLLS + 1):
            LOGGER.info('Polling report job {}/{} - {} - from {} to {}'.format(
                i,
                MAX_NUM_REPORT_POLLS,
                report_name,
                start_date,
                end_date))
            response = client.PollGenerateReport(request_id)
            if response.Status == 'Error':
                raise Exception('Error running {} report'.format(report_name))
            if response.Status == 'Success':
                if response.ReportDownloadUrl:
                    download_url = response.ReportDownloadUrl
                else:
                    LOGGER.info('No results for report: {} - from {} to {}'.format(
                        report_name,
                        start_date,
                        end_date))
                break

            if i == MAX_NUM_REPORT_POLLS:
                LOGGER.info('Generating report timed out: {} - from {} to {}'.format(
                        report_name,
                        start_date,
                        end_date))
            else:
                await asyncio.sleep(REPORT_POLL_SLEEP)

    return download_url

def stream_report(stream_name, report_name, url, report_time):
    with metrics.http_request_timer('download_report'):
        response = SESSION.get(url, headers={'User-Agent': get_user_agent()})

    if response.status_code != 200:
        raise Exception('Non-200 ({}) response downloading report: {}'.format(
            response.status_code, report_name))

    with ZipFile(io.BytesIO(response.content)) as zip_file:
        with zip_file.open(zip_file.namelist()[0]) as binary_file:
            with io.TextIOWrapper(binary_file, encoding='utf-8') as csv_file:
                # handle control character at the start of the file and extra next line
                header_line = next(csv_file)[1:-1]
                headers = header_line.replace('"', '').split(',')

                reader = csv.DictReader(csv_file, fieldnames=headers)

                with metrics.record_counter(stream_name) as counter:
                    for row in reader:
                        normalize_report_column_names(row)
                        type_report_row(row)
                        row['_sdc_report_datetime'] = report_time
                        singer.write_record(stream_name, row)
                        counter.increment()

def get_report_interval(state_key):
    report_max_days = int(CONFIG.get('report_max_days', 30))
    conversion_window = int(CONFIG.get('conversion_window', -30))

    config_start_date = arrow.get(CONFIG.get('start_date'))
    config_end_date = arrow.get(CONFIG.get('end_date')).floor('day')

    bookmark_end_date = singer.get_bookmark(STATE, state_key, 'date')
    conversion_min_date = arrow.get().floor('day').shift(days=conversion_window)

    start_date = None
    if bookmark_end_date:
        start_date = arrow.get(bookmark_end_date).floor('day').shift(days=1)
    else:
        # Will default to today
        start_date = config_start_date.floor('day')

    start_date = min(start_date, conversion_min_date)

    end_date = min(config_end_date, arrow.get().floor('day'))

    return start_date, end_date

async def sync_report(client, account_id, report_stream):
    report_max_days = int(CONFIG.get('report_max_days', 30))

    state_key = '{}_{}'.format(account_id, report_stream.stream)

    start_date, end_date = get_report_interval(state_key)

    LOGGER.info('Generating {} reports for account {} between {} - {}'.format(
        report_stream.stream, account_id, start_date, end_date))

    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = min(
            current_start_date.shift(days=report_max_days),
            end_date
        )
        await sync_report_interval(client, account_id, report_stream,
                                   current_start_date, current_end_date)
        current_start_date = current_end_date.shift(days=1)

async def sync_report_interval(client, account_id, report_stream,
                               start_date, end_date):
    state_key = '{}_{}'.format(account_id, report_stream.stream)
    report_name = stringcase.pascalcase(report_stream.stream)

    report_schema = get_report_schema(client, report_name)
    singer.write_schema(report_stream.stream, report_schema, [])

    report_time = arrow.get().isoformat()

    request_id = get_report_request_id(client, account_id, report_stream,
                                       report_name, start_date, end_date,
                                       state_key)

    singer.write_bookmark(STATE, state_key, 'request_id', request_id)
    singer.write_state(STATE)

    download_url = await poll_report(client, report_name, start_date, end_date,
                                     request_id)

    if download_url:
        LOGGER.info('Streaming report: {} for account {} - from {} to {}'.format(
            report_name,
            account_id,
            start_date,
            end_date))

        stream_report(report_stream.stream,
                      report_name,
                      download_url,
                      report_time)
        singer.write_bookmark(STATE, state_key, 'request_id', None)
        singer.write_bookmark(STATE, state_key, 'date', end_date.isoformat())

    singer.write_state(STATE)

def get_report_request_id(client, account_id, report_stream, report_name,
                          start_date, end_date, state_key):
    saved_request_id = singer.get_bookmark(STATE, state_key, 'request_id')

    if saved_request_id:
        LOGGER.info(
            'Resuming polling for account {}: {}'
            .format(account_id, report_name)
        )
        return saved_request_id

    report_request = build_report_request(client, account_id, report_stream,
                                          report_name, start_date, end_date)
    return client.SubmitGenerateReport(report_request)


def build_report_request(client, account_id, report_stream, report_name,
                         start_date, end_date):
    LOGGER.info(
        'Syncing report for account {}: {} - from {} to {}'
        .format(account_id, report_name, start_date, end_date)
    )

    report_request = client.factory.create('{}Request'.format(report_name))
    report_request.Format = 'Csv'
    report_request.Aggregation = 'Daily'
    report_request.Language = 'English'
    report_request.ExcludeReportHeader = True
    report_request.ExcludeReportFooter = True

    scope = client.factory.create('AccountThroughAdGroupReportScope')
    scope.AccountIds = {'long': [account_id]}
    report_request.Scope = scope

    excluded_fields = ['GregorianDate', '_sdc_report_datetime']
    if report_name in reports.EXTRA_FIELDS:
        excluded_fields += reports.EXTRA_FIELDS[report_name]

    selected_fields = get_selected_fields(report_stream,
                                          exclude=excluded_fields)
    selected_fields.append('TimePeriod')

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

    report_time = client.factory.create('ReportTime')
    report_time.CustomDateRangeStart = request_start_date
    report_time.CustomDateRangeEnd = request_end_date
    report_time.PredefinedTime = None
    report_request.Time = report_time

    return report_request

async def sync_reports(account_id, catalog):
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
        LOGGER.info('Syncing core objects')
        sync_core_objects(account_id, selected_streams)

    if len(all_report_streams & set(selected_streams)):
        LOGGER.info('Syncing reports')
        await sync_reports(account_id, catalog)

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

    if args.discover:
        do_discover(account_ids)
        LOGGER.info("Discovery complete")
    elif args.catalog:
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
