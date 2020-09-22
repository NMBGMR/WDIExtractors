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

from db import execute_query
from util import r_mkdir, make_geometry, get_observation_type

ANALYTE = 'TDS'
ANALYTE = 'Na'
ANALYTE = 'Mg'
ANALYTE = 'Ca'

ANALYTE = 'SO4'
ANALYTE = 'Cl'
ANALYTE = 'K'

ANALYTES = ('Ca', 'Cl', 'K', 'Na', 'Mg', 'SO4', 'TDS')


def get_pointids(analyte):
    sql = '''select L.PointID from Location as L
    join MajorChemistry as MC on substring(MC.SamplePointID, 0, LEN(MC.SamplePointID))=L.PointID
    where MC.Analyte=%d and L.PublicRelease=1 and MC.AnalysisDate is not NULL and MC.AnalysesAgency='NMBGMR'
    group by L.PointID
    order by L.PointID
    '''
    return [p['PointID'] for p in execute_query(sql, analyte)]


def get_data(analyte, pid):
    sql = '''
    select * from MajorChemistry as MC
    where MC.Analyte=%d and MC.SamplePointID like %d and MC.AnalysisDate is not NULL 
    order by MC.AnalysisDate
    '''
    pid = '{}%'.format(pid)
    return execute_query(sql, (analyte, pid))


def make_location(pid):
    sql = '''select *, 
            ludr.MEANING as [DR_Meaning], 
            luam.MEANING as [AM_Meaning]
            from Location as L
                join dbo.LU_DataReliability as ludr on ludr.Code = L.DataReliability
                join dbo.LU_AltitudeMethod as luam on luam.Code = L.AltitudeMethod
        where PointID=%d
'''
    loc = execute_query(sql, pid)[0]

    geom = make_geometry(loc['Easting'], loc['Northing'], 13, loc['UTMDatum'])

    return {'name': 'NMWDI-%autoinc',
            'description': 'No Description Available',
            'properties': {'altitude': loc['Altitude'],
                           'altitude_accuracy': loc['AltitudeAccuracy'],
                           'altitude_datum': loc['AltDatum'],
                           'altitude_method': loc.get('AM_Meaning')},
            'geometry': geom}


def make_thing(pid, data):
    return {'name': pid,
            'description': 'Water Well',
            'properties': {'organization': 'NMBGMR',
                           'organization_key': 'PointID',
                           'organization_id': pid}
            }


def make_sensor(record):
    return {'name': record['AnalysisMethod'],
            'description': 'No Description'}


def make_observations(data):
    def factory(d):
        pt = d['AnalysisDate']
        v = d['SampleValue']
        return '{}.000Z, {}'.format(pt.isoformat(), v)

    obs = [factory(d) for d in data]
    return obs


def make_datastream(record):
    return {'name': 'WaterQuality',
            'description': 'water quality datastream',
            'unitOfMeasurement': record['Units'],
            'observationType': get_observation_type(str(record['SampleValue']))
            }


def make_observed_property(analyte, record):
    return {'name': analyte,
            'description': 'Analytical chemistry'}


def make_wq(analyte, pid, p, i, prev):
    data = get_data(analyte, pid)
    if data:
        print('making {} {}: {} {}'.format(i, pid, ANALYTE, p))
        record = data[0]
        with open(p, 'w') as wfile:
            obj = {'location': make_location(pid) if prev is None else prev['location'],
                   'thing': make_thing(pid, record),
                   'datastream': make_datastream(record),
                   'observed_property': make_observed_property(analyte, record),
                   'observations': make_observations(data),
                   'sensor': make_sensor(record),
                   'destination': 'https://st.newmexicowaterdata.org/FROST-Server/v1.1'
                   }
            json.dump(obj, wfile, indent=2)
    return prev


def main():

    for a in ANALYTES:
        pids = get_pointids(a)
        n = len(pids)
        print('analyte={} npointds={}'.format(a, n))

        for i, pid in enumerate(pids):
            group = pid.split('-')[0]
            prev = None
            for analyte in ANALYTES:
                root = 'data/wqs/{}/{}'.format(analyte, group)

                r_mkdir(root)
                op = '{}/{}.json'.format(root, pid)
                if not os.path.isfile(op):
                    prev = make_wq(analyte, pid, op, i, prev)


if __name__ == '__main__':
    main()
# ============= EOF =============================================
