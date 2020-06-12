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
import re
import tempfile
from datetime import datetime

import yaml
from itertools import groupby
from operator import attrgetter, itemgetter

NO_DESCRIPTION = 'No Description Available'
import logging
logger = logging.getLogger('Parser')
logger.setLevel(logging.DEBUG)


def rows_to_yaml(location_name, path, items, wq_tag):
    longitude_key = 'Longitude'
    latitude_key = 'Latitude'

    items = list(items)
    item = items[0]
    logger.debug('items {}'.format(items))
    logger.debug('item {}'.format(item))

    obj = {'location': {'name': location_name, 'description': NO_DESCRIPTION},
           'sensor': {'name': 'Analytical Water Chemistry', 'description': NO_DESCRIPTION},
           'thing': {'name': 'WaterQuality', 'description': NO_DESCRIPTION},
           'datastream': {'name': '{} Water Quality Datastream'.format(wq_tag), 'description': NO_DESCRIPTION},
           'observerd_property': {'name': wq_tag, 'description': NO_DESCRIPTION}}

    loc = obj['location']
    loc['geometry'] = {'type': 'Point', 'coordinates': [float(item[longitude_key]), float(item[latitude_key])]}

    ds = obj['datastream']
    ds['unitofMeasurement'] = 'ppm'
    result = item[wq_tag]
    ds['observationType'] = get_observation_type(result)

    def obsfactory(i):
        pt = i['CollectionDate']
        pt = datetime.strptime(pt, '%Y-%m-%d %H:%M:%S.%f')
        return '{}Z, {}'.format(pt.isoformat(timespec="milliseconds"), i[wq_tag])

    obj['observations'] = [obsfactory(item) for item in items]

    with open(path, 'w') as wf:
        yaml.dump(obj, wf)


DOUBLE = re.compile(r'^-?\d+.\d+')
BOOL = re.compile(r'^true|false|t|f')
URI = re.compile(r'^http')
INT = re.compile(r'^-?\d+$')


def get_observation_type(value):
    for res, oti in ((DOUBLE, 'double'),
                     (BOOL, 'bool'),
                     (URI, 'uri'),
                     (INT, 'integer')):

        if res.match(value.strip().lower()):
            ot = oti
            break

    else:
        ot = 'any'
    return ot


class Parser:
    def __enter__(self):
        self._tempdir = tempfile.mkdtemp()
        # self._tempdir = '/Users/ross/Sandbox/wdi/csvextractor'
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
        # os.removedirs(self._tempdir)

    def items(self, inputfile):
        """
        read inputfile as csv, convert to a list of yamls

        WQ_XXX.csv example headers
        WQ_Arsenic
            POINT_ID,CollectionDate,HistoricDate,Arsenic,Latitude,Longitude,WellDepth,DataSource,
            DataSourceInfo,Arsenic_Symbol,GeoLocation


        :param inputfile:
        :return: list of paths
        """
        delimiter = ','
        with open(inputfile, 'r') as rf:
            rows = []
            header = None
            for line in rf:
                row = line.split(delimiter)
                if header is None:
                    header = [r.strip() for r in row]
                    continue

                row = dict(zip(header, [r.strip() for r in row]))
                rows.append(row)

            # determine wq_tag from header
            wq_tag = next((n for n in ('Arsenic', ) if n in header), None)
            for location_name, items in groupby(sorted(rows,
                                                       key=itemgetter('POINT_ID')),
                                                key=itemgetter('POINT_ID')):
                location_name = location_name.replace(' ', '_')
                name = '{}.yaml'.format(location_name)
                tmpfile = os.path.join(self._tempdir, name)
                rows_to_yaml(location_name, tmpfile, items, wq_tag)
                yield tmpfile


# if __name__ == '__main__':
#     with Parser() as p:
#         for i in p.items('/Users/ross/Programming/wdidata/wq_arsenic.csv'):
#             print('isda', i)
#             with open(i, 'r') as rfile:
#                 obj = yaml.load(rfile, Loader=yaml.SafeLoader)
#                 print('asd', obj)
#                 break
# ============= EOF =============================================
