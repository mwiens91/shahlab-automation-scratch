"""Contains a Tantalus API class to make requests to Tantalus.

This class makes no attempt at being all-encompassing. It covers a
subset of Tantalus API features to meet the needs of the automation
scripts.
"""

from __future__ import print_function
import json
import os
import sys
from django.core.serializers.json import DjangoJSONEncoder
import requests
import coreapi
from openapi_codec import OpenAPICodec
from coreapi.codecs import JSONCodec


TANTALUS_API_URL = 'http://tantalus.bcgsc.ca/api/'

class TantalusApi(object):
    """Tantalus API class."""
    def __init__(self):
        """Set up authentication using basic authentication.

        Expects to find valid environment variables
        TANTALUS_API_USERNAME and TANTALUS_API_PASSWORD. Also looks for
        an optional TANTALUS_API_URL.
        """
        # Create session and give it with auth
        self.session = requests.Session()
        self.session.auth = (
            os.environ.get('TANTALUS_API_USERNAME'),
            os.environ.get('TANTALUS_API_PASSWORD'),)

        # Tell Tantalus we're sending JSON
        self.session.headers.update({'content-type': 'application/json'})

        # Record the base API URL
        self.base_api_url = os.environ.get(
            'TANTALUS_API_URL',
            TANTALUS_API_URL)

        self.tantalus_document_url = self.base_api_url + 'swagger/?format=openapi'

        auth = coreapi.auth.BasicAuthentication(
            username=os.environ.get('TANTALUS_API_USERNAME'),
            password=os.environ.get('TANTALUS_API_PASSWORD'),
        )
        decoders = [OpenAPICodec(), JSONCodec()]

        self.coreapi_client = coreapi.Client(auth=auth, decoders=decoders)
        self.coreapi_schema = self.coreapi_client.get(self.tantalus_document_url, format='openapi')

    @staticmethod
    def join_urls(*pieces):
        """Join pieces of an URL together safely."""
        return '/'.join(s.strip('/') for s in pieces) + '/'

    def sequence_dataset_add(self, model_dictionaries, tag_name=None):
        """POST to the sequence_dataset_add endpoint.

        Args:
            model_dictionaries: A list of dictionaries containing
                information about a model to create.
            tag_name: An optional string (or None) containing the name
                of the tag to associate with the model instances
                represented in the model_dictionaries.
        """
        endpoint_url = self.join_urls(
            self.base_api_url,
            '/sequence_dataset_add/',)

        payload = json.dumps(
            {"model_dictionaries": model_dictionaries,
             "tag": tag_name},
            cls=DjangoJSONEncoder)

        r = self.session.post(
            endpoint_url,
            data=payload,)

        try:
            # Ensure that the request was successful
            assert 200 <= r.status_code < 300
        except AssertionError:
            print("An HTTP request to %s failed with status %s." %
                  (endpoint_url, r.status_code),
                  file=sys.stderr,)


    def get(self, table_name, **fields):
        ''' Check if a resource exists in Tantalus and return it. '''

        get_params = {}

        for field in self.coreapi_schema[table_name]['list'].fields:
            if field.name in ('limit', 'offset'):
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        list_results = self.coreapi_client.action(self.coreapi_schema, [table_name, 'list'], params=get_params)

        if list_results['count'] > 1:
            raise ValueError('more than 1 object for {}, {}'.format(
                table_name, fields))

        if list_results['count'] == 0:
            raise ValueError('no object for {}, {}'.format(
                table_name, fields))

        else:
            result = list_results['results'][0]

            for field_name, field_value in fields.iteritems():
                if field_name not in result:
                    raise ValueError('field {} not in {}'.format(
                        field_name, name))

                if result[field_name] != field_value:
                    raise ValueError('field {} mismatches, set to {} not {}'.format(
                        field_name, result[field_name], field_value))

            return result

    # TODO: do these handle pagination
    # TODO: refactor so that for instance get uses list

    def list(self, table_name, **fields):
        ''' List resources in tantalus. '''

        get_params = {}

        for field in self.coreapi_schema[table_name]['list'].fields:
            if field.name in ('limit', 'offset'):
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        list_results = self.coreapi_client.action(self.coreapi_schema, [table_name, 'list'], params=get_params)

        for result in list_results:
            for field_name, field_value in fields.iteritems():
                if field_name not in result:
                    raise ValueError('field {} not in {}'.format(
                        field_name, name))

                if result[field_name] != field_value:
                    raise ValueError('field {} mismatches, set to {} not {}'.format(
                        field_name, result[field_name], field_value))

        return list_result


    def get_or_create(self, table_name, **fields):
        ''' Check if a resource exists in Tantalus and return it. 
        If it does not exist, create the resource and return it. '''

        get_params = {}

        for field in self.coreapi_schema[table_name]['list'].fields:
            if field.name in ('limit', 'offset'):
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        list_results = self.coreapi_client.action(self.coreapi_schema, [table_name, 'list'], params=get_params)

        if list_results['count'] > 1:
            raise ValueError('more than 1 object for {}, {}'.format(
                table_name, fields))

        elif list_results['count'] == 1:
            result = list_results['results'][0]

            for field_name, field_value in fields.iteritems():
                if field_name not in result:
                    raise ValueError('field {} not in {}'.format(
                        field_name, name))

                if result[field_name] != field_value:
                    raise ValueError('field {} already set to {} not {}'.format(
                        field_name, result[field_name], field_value))

        else:
            result = self.coreapi_client.action(self.coreapi_schema, [table_name, 'create'], params=fields)

        return result


