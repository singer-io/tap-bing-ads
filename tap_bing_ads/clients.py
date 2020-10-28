import tempfile

import singer
import suds
from bingads import AuthorizationData, OAuthDesktopMobileAuthCodeGrant
from bingads.v13.bulk import (
    BulkFileReader,
    BulkServiceManager,
    DownloadParameters
)
from singer import metrics

from tap_bing_ads.transform import sobject_to_dict


LOGGER = singer.get_logger()


def log_service_call(service_method, account_id):
    def wrapper(*args, **kwargs):
        if hasattr(service_method, 'name'):
            service_method_name = getattr(service_method, 'name')
        else:
            service_method_name = service_method.__name__

        log_args = list(map(lambda arg: str(arg).replace('\n', '\\n'), args)) + \
                   list(map(lambda kv: '{}={}'.format(*kv), kwargs.items()))
        LOGGER.info('Calling: {}({}) for account: {}'.format(
            service_method_name,
            ','.join(log_args),
            account_id))
        with metrics.http_request_timer(service_method_name):
            try:
                return service_method(*args, **kwargs)
            except suds.WebFault as e:
                if hasattr(e.fault.detail, 'ApiFaultDetail'):
                    # The Web fault structure is heavily nested. This is to be sure we catch the error we want.
                    operation_errors = e.fault.detail.ApiFaultDetail.OperationErrors
                    invalid_date_range_end_errors = [oe for (_, oe) in operation_errors
                                                     if oe.ErrorCode == 'InvalidCustomDateRangeEnd']
                    if any(invalid_date_range_end_errors):
                        raise InvalidDateRangeEnd(invalid_date_range_end_errors) from e
                    LOGGER.info('Caught exception for account: {}'.format(account_id))
                    raise Exception(operation_errors) from e
                if hasattr(e.fault.detail, 'AdApiFaultDetail'):
                    raise Exception(e.fault.detail.AdApiFaultDetail.Errors) from e

    return wrapper


class BingBulkClient(object):
    def __init__(self, config, account_id):
        self._account_id = account_id

        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=config['oauth_client_id'])
        authentication.request_oauth_tokens_by_refresh_token(
            config['refresh_token'])

        authorization_data = AuthorizationData(
            account_id=self._account_id,
            customer_id=config['customer_id'],
            developer_token=config['developer_token'],
            authentication=authentication)

        self._bulk_service = BulkServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment=config.get('environment', 'production')
        )

        self._temp_path = None

    @property
    def temp_path(self):
        if self._temp_path is None:
            self._temp_path = tempfile.mkdtemp()
            # logging where the output dir is so we can find if when debugging
            LOGGER.info(
                "temp directory for Bing bulk download: %s", self._temp_path)
        return self._temp_path

    def entities_generator(self, entities, last_sync_time):
        download_parameters = DownloadParameters(
            campaign_ids=None,
            data_scope=['EntityData'],
            download_entities=entities,
            file_type='Csv',
            last_sync_time_in_utc=last_sync_time,
            result_file_directory=self.temp_path,
            result_file_name='temp.csv',
            overwrite_result_file = True
        )
        result_file_path = log_service_call(self._bulk_service.download_file, self._account_id)(download_parameters)

        with BulkFileReader(file_path=result_file_path) as reader:
            for entity in reader:
                yield entity
