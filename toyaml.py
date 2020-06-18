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
import re
import logging
from datetime import datetime
import yaml

NO_DESCRIPTION = 'No Description Available'

logger = logging.getLogger('toyaml')
logger.setLevel(logging.DEBUG)

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


def dicts_toyaml(location_name, path, items, config):
    items = list(items)
    item = items[0]
    logger.debug('items {}'.format(items))
    logger.debug('item {}'.format(item))

    obj = {'location': {'name': location_name, 'description': NO_DESCRIPTION},
           'sensor': {'name': 'Diver', 'description': NO_DESCRIPTION},

           'datastream': {'name': 'Depth Below Surface',
                          'description': 'calculated depth below ground surface. calibrated to manual measurements'},
           'observed_property': {'name': 'Depth below ground surface',
                                 'description': 'continuous pressure transducer measurement of groundwater head'}}

    thing = {'name': 'WaterLevelPressure',
             'description': 'Water Well'}
    if config.get('thing_properties'):
        thing['properties'] = config['thing_properties'](item)

    obj['thing'] = thing

    loc = obj['location']
    loc['geometry'] = {'type': 'Point', 'coordinates': [float(item[config['lat']]),
                                                        float(item[config['lon']])]}

    ds = obj['datastream']
    ds['unitofMeasurement'] = 'foot'

    ds['observationType'] = get_observation_type(str(getter(item, config['result'])))

    def obsfactory(i):
        pt = getter(i, config['time'])
        if isinstance(pt, str):
            pt = datetime.strptime(pt, '%Y-%m-%d %H:%M:%S.%f')

        return '{}.000Z, {}'.format(pt.isoformat(), getter(i, config['result']))

    obj['observations'] = [obsfactory(item) for item in items]

    with open(path, 'w') as wf:
        yaml.dump(obj, wf)


def getter(item, key):
    if isinstance(key, str):
        return item[key]
    else:
        return key(item)
# ============= EOF =============================================
