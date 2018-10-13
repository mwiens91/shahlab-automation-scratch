""" Contains a Basic API class to make requests to a REST API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os
import requests
import coreapi
from openapi_codec import OpenAPICodec
from coreapi.codecs import JSONCodec
from django.core.serializers.json import DjangoJSONEncoder
import json


class NotFoundError(Exception):
    pass


class BasicAPIClient(object):
    """ Basic API class. """
    # Parameters used for pagination. Change this in subclasses.
    pagination_param_names = ()

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

    def get_list_pagination_initial_params(self, params):
        """Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        # Implement specific methods here
        pass

    def get_list_pagination_next_page_params(self, params):
        """Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        # Implement specific methods here
        pass

    def list(self, table_name, **fields):
        ''' List resources in from endpoint with given filter fields. '''

        get_params = {}

        for field in self.coreapi_schema[table_name]['list'].fields:
            if field.name in self.pagination_param_names:
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        # Add in pagination params
        self.get_list_pagination_initial_params(get_params)

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

                    try:
                        result_field = result[field_name]['id']
                    except TypeError:
                        result_field = result[field_name]

                    # Fields to exclude. Note that * to many
                    # relationships are problems because filter for
                    # exactly one related row will work even if there
                    # are many related rows.
                    # TODO(mwiens91): find a more elegant way of
                    # achieving this effect
                    exclude_fields = (
                        'created',  # datetimes have different formats
                        'sequence_lanes',   # in list is nested, but not in create
                        'file_resources',   # *->many are issues
                        'tags',   # *->many are issues
                    )

                    if result_field != field_value and field_name not in exclude_fields:
                        raise Exception('field {} mismatches, set to {} not {}'.format(
                            field_name, result_field, field_value))

                yield result

            if list_results.get('next') is None:
                break

            # Set up for the next page
            self.get_list_pagination_next_page_params(get_params)

    def get_or_create(self, table_name, **fields):
        ''' Check if a resource exists in and if so return it.
        If it does not exist, create the resource and return it. '''

        try:
            return self.get(table_name, **fields)
        except NotFoundError:
            pass

        for field_name, field_value in fields.iteritems():
            fields[field_name] = eval(DjangoJSONEncoder().encode(field_value))
        print(fields)

        return self.coreapi_client.action(self.coreapi_schema, [table_name, 'create'], params=fields)

