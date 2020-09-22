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
from datetime import datetime

import pyproj
import logging

NO_DESCRIPTION = 'No Description Available'

logger = logging.getLogger('toyaml')
logger.setLevel(logging.DEBUG)

DOUBLE = re.compile(r'^-?\d+.\d+')
BOOL = re.compile(r'^true|false|t|f')
URI = re.compile(r'^http')
INT = re.compile(r'^-?\d+$')
projections = {}


def row_gen(p, delimiter=','):
    with open(p, 'r') as rfile:
        header = next(rfile).strip().split(delimiter)
        header = [h.strip() for h in header]
        for line in rfile:
            row = line.strip().split(delimiter)
            row = dict(zip(header, row))
            yield row


def st_iso_format(t, fmt='%m/%d/%y'):
    t = datetime.strptime(t, fmt)
    return '{}.000Z'.format(t.isoformat())


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


def make_geometry(e ,n, zone=None, datum=None, srid=None):
    if zone:
        if zone in projections:
            p = projections[zone]
        else:
            p = pyproj.Proj(proj='utm', zone=int(zone), datum=datum)
    elif srid:
        # get zone
        if srid in projections:
            p = projections[srid]
        else:
            # p = pyproj.Proj(proj='utm', zone=int(zone), ellps='WGS84')
            p = pyproj.Proj('EPSG:{}'.format(srid))

    projections[srid] = p
    lon, lat = p(e, n, inverse=True)

    return {'type': 'Point', 'coordinates': [lon, lat]}


def assemble(location_name, thing_name, items, config):
    items = list(items)
    item = items[0]
    logger.debug('items {}'.format(items))
    logger.debug('item {}'.format(item))

    n = item[config['n']]
    e = item[config['e']]
    srid = item[config['srid']]

    obj = {'location': {'name': location_name,
                        'description': NO_DESCRIPTION,
                        'properties': config['location_properties'](item),
                        'geometry': make_geometry(e, n, srid=srid)},
           'sensor': {'name': config['sensor'](item),
                      'description': NO_DESCRIPTION},

           'datastream': {'name': 'Depth Below Surface',
                          'description': config['datastream_description']},
           'observed_property': {'name': 'Depth below ground surface',
                                 'description': config['observed_property_description']}}

    thing = {'name': thing_name,
             'description': 'Water Well'}
    if config.get('thing_properties'):
        thing['properties'] = config['thing_properties'](item)

    obj['thing'] = thing

    # loc = obj['location']
    # loc['geometry'] = {'type': 'Point', 'coordinates': [float(item[config['lat']]),
    #                                                     float(item[config['lon']])]}

    ds = obj['datastream']
    ds['unitofMeasurement'] = 'foot'

    ds['observationType'] = get_observation_type(str(getter(item, config['result'])))

    def obsfactory(i):
        pt = getter(i, config['time'])
        if isinstance(pt, str):
            pt = datetime.strptime(pt, '%Y-%m-%d %H:%M:%S.%f')

        v = getter(i, config['result'])
        if v is not None:
            return '{}.000Z, {}'.format(pt.isoformat(), v)

    obs = [obsfactory(item) for item in items]
    obj['observations'] = [o for o in obs if o is not None]

    return obj


def getter(item, key):
    if isinstance(key, str):
        return item[key]
    else:
        return key(item)


def r_mkdir(p):
    if p and not os.path.isdir(p):
        try:
            os.mkdir(p)
        except OSError:
            r_mkdir(os.path.dirname(p))
            os.mkdir(p)

# ============= EOF =============================================
