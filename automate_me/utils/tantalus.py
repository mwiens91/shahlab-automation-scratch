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
        self.headers.update({'content-type': 'application/json'})

        # Record the base API URL
        self.base_api_url = os.environ.get(
            'TANTALUS_API_URL',
            TANTALUS_API_URL)

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
