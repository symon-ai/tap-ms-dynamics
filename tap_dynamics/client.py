import os
import sys
import math
import json
from datetime import datetime, timedelta

import backoff
import requests
import singer
from simplejson import JSONDecodeError

from tap_dynamics.transform import transform_metadata_xml
from tap_dynamics.symon_exception import SymonException


LOGGER = singer.get_logger()

API_VERSION = '9.2'
MAX_PAGESIZE = 5000
MAX_RETRIES = 5
MAX_SELECT_PARAM_SIZE = 1800


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def log_backoff_attempt(details):
    LOGGER.info(
        "ConnectionError detected, triggering backoff: %d try", details.get("tries"))


def retry_after_wait_gen():
    while True:
        # This is called in an except block so we can retrieve the exception
        # and check it.
        exc_info = sys.exc_info()
        resp = exc_info[1].response
        sleep_time_str = resp.headers.get('Retry-After')
        LOGGER.info(f'API rate limit exceeded -- sleeping for '
                    f'{sleep_time_str} seconds')
        yield math.floor(float(sleep_time_str))

# pylint: disable=missing-class-docstring


class DynamicsException(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response

# pylint: disable=missing-class-docstring


class DynamicsQuotaExceededException(DynamicsException):
    def __init__(self, message=None, response=None):
        super().__init__(message, response)

# pylint: disable=missing-class-docstring


class Dynamics5xxException(DynamicsException):
    def __init__(self, message=None, response=None):
        super().__init__(message, response)

# pylint: disable=missing-class-docstring


class Dynamics4xxException(DynamicsException):
    def __init__(self, message=None, response=None):
        super().__init__(message, response)

# pylint: disable=missing-class-docstring


class Dynamics429Exception(DynamicsException):
    def __init__(self, message=None, response=None):
        super().__init__(message, response)

# pylint: disable=too-many-instance-attributes


class DynamicsClient:
    def __init__(self,
                 organization_uri,
                 config_path,
                 max_pagesize,
                 api_version=None,
                 client_id=None,
                 client_secret=None,
                 user_agent=None,
                 redirect_uri=None,
                 refresh_token=None,
                 start_date=None):
        self.organization_uri = organization_uri
        self.api_version = api_version if api_version else API_VERSION
        # tap-tester was failing otherwise
        max_pagesize = MAX_PAGESIZE if max_pagesize is None else max_pagesize
        self.max_pagesize = max_pagesize if max_pagesize <= MAX_PAGESIZE else MAX_PAGESIZE
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.user_agent = user_agent
        self.refresh_token = refresh_token

        self.session = requests.Session()
        self.access_token = None
        self.expires_at = None

        self.start_date = start_date
        self.config_path = config_path

    def _write_config(self, refresh_token):
        LOGGER.info("Credentials Refreshed")
        self.refresh_token = refresh_token

        # Update config at config_path
        with open(self.config_path) as file:
            config = json.load(file)

        config['refresh_token'] = refresh_token

        with open(self.config_path, 'w') as file:
            json.dump(config, file, indent=2)

    def _ensure_access_token(self):
        if self.access_token is None or self.expires_at <= datetime.utcnow():
            response = self.session.post(
                'https://login.microsoftonline.com/common/oauth2/token',
                data={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'redirect_uri': self.redirect_uri,
                    'refresh_token': self.refresh_token,
                    'grant_type': 'refresh_token',
                    'resource': self.organization_uri
                })

            if response.status_code != 200:
                raise SymonException('Failed to connect to MS Dynamics. Please ensure the OAuth token is up to date.', 'dynamics.AuthInvalid')

            data = response.json()

            self.access_token = data.get('access_token')
            if self.refresh_token != data.get('refresh_token'):
                self._write_config(data.get('refresh_token'))

            # pad by 10 seconds for clock drift
            self.expires_at = datetime.utcnow() + \
                timedelta(seconds=int(data.get('expires_in')) - 10)

    def _get_standard_headers(self):
        return {
            "Authorization": "Bearer {}".format(self.access_token),
            "User-Agent": self.user_agent,
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "If-None-Match": "null"
        }

    @backoff.on_exception(retry_after_wait_gen,
                          Dynamics429Exception,
                          max_tries=MAX_RETRIES,
                          on_backoff=log_backoff_attempt)
    @backoff.on_exception(backoff.expo,
                          (Dynamics5xxException, Dynamics4xxException,
                           requests.ConnectionError),
                          max_tries=MAX_RETRIES,
                          factor=2,
                          on_backoff=log_backoff_attempt)
    def _make_request(self, method, endpoint, paging=False, headers=None, params=None, data=None):
        if not paging:
            full_url = f'{self.organization_uri}/api/data/v{self.api_version}/{endpoint}'
        else:
            full_url = endpoint

        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            full_url,
            method.upper(),
            endpoint if not paging else '@odata.nextLink',
            params,
        )

        self._ensure_access_token()

        default_headers = self._get_standard_headers()

        if headers:
            headers = {**default_headers, **headers}
        else:
            headers = {**default_headers}

        try:
            response = self.session.request(
                method, full_url, headers=headers, params=params, data=data)
        except requests.exceptions.ConnectionError as e:
            message = str(e)
            if 'nodename nor servname provided, or not known' in message or 'Name or service not known' in message:
                raise SymonException(f'Sorry, we couldn\'t connect to Dynamics URL "{self.organization_uri}". Please check the Dynamics URL and try again.', 'dynamics.InvalidUrl')
        
        # pylint: disable=no-else-raise
        if response.status_code >= 500:
            raise Dynamics5xxException(response.text, response)
        elif response.status_code == 429:
            raise Dynamics429Exception("rate limit exceeded", response)
        elif response.status_code >= 400:
            raise Dynamics4xxException(response.text, response)

        try:
            results = response.json()
        except JSONDecodeError:
            results = response.text

        return results

    def get(self, endpoint, paging=False, headers=None, params=None):
        try:
            return self._make_request("GET", endpoint, paging, headers=headers, params=params)
        except DynamicsException as e:
            message, error_code = None, None
            try:
                error_json = e.response.json()
                message = error_json["error"]["message"]
                error_code = error_json["error"]["code"]
            except:
                pass

            if message is not None and error_code is not None:
                raise SymonException(f'Import failed with the following MS Dynamics error: (error code: {error_code}) {message}','dynamics.DynamicsApiError')
            elif message is not None:
                raise SymonException(f'Import failed with the following MS Dynamics error: {message}','dynamics.DynamicsApiError')
            raise

    def call_entity_definitions(self, object: str):
        '''
        Calls the `EntityDefinitions` endpoint to get all entities.
        '''

        params = {
            "$select": "MetadataId,LogicalName,EntitySetName"
        }

        results = self.get(
            f'EntityDefinitions(LogicalName=\'{object}\')', params=params)

        return results

    def call_metadata(self) -> dict:
        '''
        Calls the `$metadata` endpoint to get entities, key field,
            properties, and corresponding datatypes.
        '''
        metadata = self.get('$metadata')

        return transform_metadata_xml(metadata)

    def build_entity_metadata(self, object: str):
        '''
        Builds entity metadata from the `EntityDefinitions` and `$metadata` endpoints.
        '''
        entity = self.call_entity_definitions(object)

        entity_metadata = self.call_metadata()

        entity_name = entity.get("LogicalName")
        if entity_name in entity_metadata:
            # checks that entity is in $metadata response
            entity_metadata[entity_name]["LogicalName"] = entity_name
            entity_metadata[entity_name]["EntitySetName"] = entity.get(
                "EntitySetName")

        yield from entity_metadata.values()

    @staticmethod
    def build_params(orderby_key: str = 'modifiedon',
                     replication_key: str = 'modifiedon',
                     filter_value: str = None) -> dict:
        orderby_param = f'{orderby_key} asc'

        if filter_value:
            filter_param = f'{replication_key} ge {filter_value}'
            return {"$orderby": orderby_param, "$filter": filter_param}
        return {"$orderby": orderby_param}

    
    @staticmethod
    def build_select_params(desired_columns: list):
        if desired_columns is None or len(desired_columns) == 0:
            return {}
        
        select_columns = ','.join(desired_columns)
        if len(select_columns) > MAX_SELECT_PARAM_SIZE:
            return {}
        return {'$select': select_columns}
