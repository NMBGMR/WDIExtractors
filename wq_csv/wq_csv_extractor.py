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
import os
import logging

from pyclowder import files
from pyclowder.extractors import Extractor
from util import Parser


class WQCSVExtractor(Extractor):
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

    def process_message(self, connector, host, secret_key, resource, parameters):
        logger = logging.getLogger(__name__)
        logger.debug('resource {}'.format(resource))
        logger.debug('parameters {}'.format(parameters))

        inputfile = resource["local_paths"][0]
        file_id = resource['id']
        datasetid = parameters['datasetId']

        extracted = False
        with Parser() as p:
            for fp in p.items(inputfile):
                # add yaml file
                fid = files.upload_to_dataset(connector, host, secret_key, datasetid,
                                              fp,
                                              check_duplicate=True)
                tags = {'tags': ['YNeeded']}
                files.upload_tags(connector, host, secret_key, fid, tags)
                os.remove(fp)
                extracted = True

        if extracted:
            # set tags
            tags = {'tags': ['CSVExtracted']}
            files.upload_tags(connector, host, secret_key, file_id, tags)


if __name__ == '__main__':
    e = WQCSVExtractor()
    e.start()
# ============= EOF =============================================
