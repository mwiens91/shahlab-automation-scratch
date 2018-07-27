"""Contains a Tantalus API class to make requests to Tantalus.

This class makes no attempt at being all-encompassing. It covers a
subset of Tantalus API features to meet the needs of the automation
scripts.
"""

import json
import os
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

        # Record the base API URL
        self.base_api_url = os.environ.get(
            'TANTALUS_API_URL',
            TANTALUS_API_URL)

    @staticmethod
    def join_urls(*pieces):
        """Join pieces of an URL together safely."""
        return '/'.join(s.strip('/') for s in pieces)

    def read_models(self, json_list):
        """POST to the read_models endpoint."""
        endpoint_url = self.join_urls(
            self.base_api_url,
            'backend/read_models/',)

        r = self.session.post(
            endpoint_url,
            data={'json_list': json.dumps(json_list)})

        assert r.status_code == 200
