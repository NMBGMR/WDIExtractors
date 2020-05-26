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
import json
import yaml
import logging

from pyclowder import files
from pyclowder.extractors import Extractor


class YAMLExtractor(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):

        inputfile = resource["local_paths"][0]
        file_id = resource['id']
        datasetid = resource['datasetId']

        if self._validate(inputfile):
            # set tags
            tags = {'tags': ['STNeeded', 'CKANNeeded']}
            rtags = {'tags': ['YNeeded', 'YAMLValidationFailed']}
        else:

            tags = {'tags': ['YAMLValidationFailed']}
            rtags = {'tags': ['YNeeded']}

        files.upload_tags(connector, host, secret_key, file_id, tags)

        if rtags:
            headers = {'Content-Type': 'application/json'}
            url = '{}api/files/{}/tags?key={}'.format(host, file_id, secret_key)
            connector.delete(url, headers=headers, data=json.dumps(tags),
                             verify=connector.ssl_verify if connector else True)

    def _validate(self, ip):
        logger = logging.getLogger(__name__)
        required_keys = ('location', 'thing',
                         'sensor', 'observed_property',
                         'datastream', 'observations')
        with open(ip, 'r') as rf:
            yd = yaml.load(rf)
            try:
                keys = [key for key in required_keys if key not in yd]
                if keys:
                    keys = []
                    logger.info('validation failed. missing keys={}'.format(keys))
                else:
                    return True
            except yaml.YAMLError as e:
                logger.critical('Validation of yaml file failed. Error={}'.format(e))


if __name__ == '__main__':
    e = YAMLExtractor()
    e.start()
# ============= EOF =============================================
