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
import os
import tempfile
from itertools import groupby
from operator import attrgetter

import yaml
import logging

from pyclowder import files
from pyclowder.utils import StatusMessage
from pyclowder.extractors import Extractor
import pyclowder.files
from util import Parser


class CSVExtractor(Extractor):
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
        inputfile = resource["local_paths"][0]
        file_id = resource['id']
        datasetid = resource['datasetId']

        name = os.path.splitext(os.path.basename(inputfile))[0]

        # set tags
        tags = {'tags': ['XLSExtracted']}
        files.upload_tags(connector, host, secret_key, file_id, tags)

        with Parser() as p:
            for fp in p.items(inputfile):
                # add yaml file
                files.upload_to_dataset(connector, host, secret_key, datasetid,
                                        fp,
                                        check_duplicate=True)
                os.remove(fp)


if __name__ == '__main__':
    e = CSVExtractor()
    e.start()
# ============= EOF =============================================
