from __future__ import print_function
import os
import sys
import requests
from basicclient import BasicAPIClient


COLOSSUS_API_URL = 'http://colossus.bcgsc.ca/api/'


class ColossusApi(BasicAPIClient):
    """ Colossus API class. """

    def __init__(self):
        """ Set up authentication using basic authentication.

        Expects to find valid environment variables
        COLOSSUS_API_USERNAME and COLOSSUS_API_PASSWORD. Also looks for
        an optional COLOSSUS_API_URL.
        """

        super(ColossusApi, self).__init__(
            os.environ.get('COLOSSUS_API_URL', COLOSSUS_API_URL),
            username=os.environ.get('COLOSSUS_API_USERNAME'),
            password=os.environ.get('COLOSSUS_API_PASSWORD'))

    def get_colossus_sublibraries_from_library_id(self, library_id):
        """ Gets the sublibrary information from a library id.
        """

        return list(self.list('sublibraries', library__pool_id=library_id))


    def query_libraries_by_library_id(self, library_id):
        """ Gets a library by its library_id.
        """

        return self.get('library', pool_id=library_id)


_default_client = ColossusApi()
get_colossus_sublibraries_from_library_id = _default_client.get_colossus_sublibraries_from_library_id
query_libraries_by_library_id = _default_client.query_libraries_by_library_id
