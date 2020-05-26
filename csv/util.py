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
import yaml
from itertools import groupby
from operator import attrgetter, itemgetter

NO_DESCRIPTION = 'No Description Available'
import logging
logger = logging.getLogger('Parser')
logger.setLevel(logging.DEBUG)


def rows_to_yaml(path, items):
    items = list(items)
    item = items[0]
    logger.debug('items {}'.format(items))
    logger.debug('item {}'.format(item))

    obj = {attr: {'name': item[attr],
                  'description': item.get('{}_description'.format(attr), NO_DESCRIPTION)}
           for attr in ('location', 'sensor', 'thing',
                        'datastream', 'observed_property')}

    loc = obj['location']
    loc['geometry'] = {'type': 'Point', 'coordinates': [float(item['lon']), float(item['lat'])]}

    ds = obj['datastream']
    ds['unitofMeasurement'] = item.get('datastream_units', 'foot')
    ds['observationType'] = get_observation_type(item['result'])
    obj['observations'] = ['{}, {}'.format(item['phenomenonTime'], float(item['result'])) for item in items]

    with open(path, 'w') as wf:
        yaml.dump(obj, wf)


DOUBLE = re.compile(r'^-?\d+.\d+')
BOOL = re.compile(r'^true|false|t|f')
URI = re.compile(r'^http')
INT = re.compile(r'^-?\d+$')


def get_observation_type(value):
    ot = None
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

        example xls file header
        location, lat, lon, location_description,
        thing, thing_description,
        sensor, sensor_description,
        phenomenonTime, result,
        datastream, datastream_description,
        datastream_units,
        observed_property, observed_property_description


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

            for location_name, items in groupby(sorted(rows,
                                                       key=itemgetter('location')),
                                                key=itemgetter('location')):
                name = '{}.yaml'.format(location_name)
                tmpfile = os.path.join(self._tempdir, name)
                rows_to_yaml(tmpfile, items)
                yield name


# if __name__ == '__main__':
#     with Parser() as p:
#         for i in p.items('/Users/ross/sandbox/wdi/upload.csv'):
#             print('isda', i)
# ============= EOF =============================================
