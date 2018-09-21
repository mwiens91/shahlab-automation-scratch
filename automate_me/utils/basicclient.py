""" Contains a Basic API class to make requests to a REST API.
"""

from __future__ import print_function
import os
import requests
import coreapi
from openapi_codec import OpenAPICodec
from coreapi.codecs import JSONCodec


class NotFoundError(Exception):
    pass


class BasicAPIClient(object):
    """ Basic API class. """

    def __init__(self, api_url, username=None, password=None):
        """ Set up authentication using basic authentication.
        """

        # Create session and give it with auth
        self.session = requests.Session()
        if username is not None and password is not None:
            self.session.auth = (username, password)

        # Tell Tantalus we're sending JSON
        self.session.headers.update({'content-type': 'application/json'})

        # Record the base API URL
        self.base_api_url = api_url

        self.document_url = self.base_api_url + 'swagger/?format=openapi'

        auth = None
        if username is not None and password is not None:
            auth = coreapi.auth.BasicAuthentication(
                username=username, password=password)

        decoders = [OpenAPICodec(), JSONCodec()]

        self.coreapi_client = coreapi.Client(auth=auth, decoders=decoders)
        self.coreapi_schema = self.coreapi_client.get(self.document_url, format='openapi')

    def get(self, table_name, **fields):
        ''' Check if a resource exists and if so return it. '''

        list_results = self.list(table_name, **fields)

        try:
            result = next(list_results)
        except StopIteration:
            raise NotFoundError('no object for {}, {}'.format(
                table_name, fields))

        try:
            next(list_results)
            raise Exception('more than 1 object for {}, {}'.format(
                table_name, fields))
        except StopIteration:
            pass

        return result

    def list(self, table_name, **fields):
        ''' List resources in from endpoint with given filter fields. '''

        get_params = {}

        for field in self.coreapi_schema[table_name]['list'].fields:
            if field.name in ('limit', 'offset'):
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        if 'limit' not in get_params:
            get_params['limit'] = 100

        if 'offset' not in get_params:
            get_params['offset'] = 0

        while True:
            list_results = self.coreapi_client.action(self.coreapi_schema, [table_name, 'list'], params=get_params)

            for result in list_results['results']:
                for field_name, field_value in fields.iteritems():
                    # Currently no support for checking related model fields
                    if '__' in field_name:
                        continue

                    if field_name not in result:
                        raise Exception('field {} not in {}'.format(
                            field_name, table_name))

                    if result[field_name] != field_value:
                        raise Exception('field {} mismatches, set to {} not {}'.format(
                            field_name, result[field_name], field_value))

                yield result

            if list_results.get('next') is None:
                break

            get_params['offset'] += get_params['limit']

    def get_or_create(self, table_name, **fields):
        ''' Check if a resource exists in and if so return it.
        If it does not exist, create the resource and return it. '''

        try:
            return self.get(table_name, **fields)
        except NotFoundError:
            pass

        return self.coreapi_client.action(self.coreapi_schema, [table_name, 'create'], params=fields)
