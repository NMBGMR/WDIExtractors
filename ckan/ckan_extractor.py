# ===============================================================================
# Copyright 2020 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
from os import environ

import requests
from ckanapi import RemoteCKAN
from ckanapi.errors import ValidationError
from pyclowder import files
from pyclowder.extractors import Extractor
import logging


class CKANExtractor(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def ckan_ctx(self):
        logger = logging.getLogger(__name__)

        url = environ.get('CKAN_URL')
        apikey = environ.get('CKAN_APIKEY')

        logger.debug('CKAN URL={}'.format(url))
        if apikey is None:
            logger.debug('Warning: no apikey')

        return RemoteCKAN(url, apikey=apikey)

    def process_message(self, connector, host, secret_key, resource, parameters):
        logger = logging.getLogger(__name__)

        # add tags
        tags = {'tags': ['QCNeeded']}
        file_id = resource['id']
        files.upload_tags(connector, host, secret_key, file_id, tags)

        # make dataset package
        pkg_name = 'foo'
        extras = {}
        package = {'extras': extras, 'name': pkg_name}

        # upload to ckan
        with self.ckan_ctx() as ckan:
            try:
                ckan.action.package_create(**package)
            except (ValidationError, requests.ConnectionError):
                pass

            # make resource package
            res_name = 'resourcefile'

            resource = {'package_id': pkg_name,
                        'name': res_name,
                        'url': '{}/files'.format(host),
                        'resource_type':  resource['type']}
            try:
                ckan.action.resource_create(**resource)
            except (ValidationError, requests.ConnectionError):
                pass


if __name__ == '__main__':
    e = CKANExtractor()
    e.start()
# ============= EOF =============================================
