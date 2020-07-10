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
from datetime import datetime

import pyproj
import yaml

projections = {}


def row_gen(p, delimiter):
    with open(p, 'r') as rfile:
        header = next(rfile).strip().split(delimiter)
        header = [h.strip() for h in header]
        for line in rfile:
            row = line.strip().split(delimiter)
            row = dict(zip(header, row))
            yield row


def factory(row):
    zone = row['utm_zone']
    easting = float(row['easting'])
    northing = float(row['northing'])

    if zone in projections:
        p = projections[zone]
    else:
        p = pyproj.Proj(proj='utm', zone=int(zone), ellps='WGS84')

    projections[zone] = p
    lon, lat = p(easting, northing, inverse=True)

    location = {'name': 'NMWDI-$autoinc',
                'description': 'OSE POD import',
                'geometry': {'type': 'Point',
                             'coordinates': [lat, lon]}}
    podid = row['pod_rec_nbr']
    agency_id = '{}-{}'.format(row['pod_basin'], row['pod_nbr'])
    thing = {'name': agency_id,
             'description': 'OSE POD',
             'properties': {'agency_id': podid,
                            'agency_key': 'pod_rec_nbr'}}

    obj = {'location': location,
           'thing': thing}

    return podid, obj


def meter_factory(meter):
    return {'name': meter['mtr_serial_nbr'],
              'description': 'OSE POD mtr_serial_nbr'}


def datastream_factory():
    return {'name': 'Meter Reading',
            'description': 'OSE POD meter reading',
            'unitofMeasurement': 'foot',
            'observationType': 'double'}


def obs_factory(mtr_id, p):
    obs = []
    for row in row_gen(p, ','):
        if row['mtr_rec_nbr'] == mtr_id:
            t = row['reading_date']
            t = datetime.strptime(t, '%m/%d/%y')
            r = float(row['mtr_reading'])
            obs.append((t.isoformat(), r))

    obs = sorted(obs, key=lambda x: x[0])
    obs = ['{}.000Z, {}'.format(*o) for o in obs]
    return obs


def main():
    pods_path = '/Users/ross/Programming/wdidata/waters/pod.tsv'
    meter_info = '/Users/ross/Programming/wdidata/waters/meter_info.csv'
    meter_detail = '/Users/ross/Programming/wdidata/waters/meter_detail.csv'

    # load the meters_info.csv file
    meters = list(row_gen(meter_info, ','))

    for row in row_gen(pods_path, '\t'):
        # iterate each row in pods table
        podid, obj = factory(row)

        # get the meter for the list of available meters extracted from meter_info.csv
        meter = next((m for m in meters if m['pod_rec_nbr'] == podid), None)
        if meter:
            # construct sensor, datastream, observations
            obj['sensor'] = meter_factory(meter)
            obj['datastream'] = datastream_factory()
            obj['observations'] = obs_factory(meter['mtr_rec_nbr'], meter_detail)

            print('obj', obj)

            # write to file. upload to clowder is currently manual
            with open('data/waters/{}.yml'.format(podid), 'w') as wfile:
                yaml.dump(obj, wfile)


if __name__ == '__main__':
    main()

# ============= EOF =============================================
