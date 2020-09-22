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
from datetime import datetime
from itertools import groupby
from operator import itemgetter

import requests

from db import execute_query
from util import assemble, r_mkdir

ST = 'https://st.newmexicowaterdata.org/FROST-Server/v1.1'
KIND = 'manual'
KIND = 'non_NMBGMR_cgwmn'
KIND = 'non_MEASURING_AGENCY_cgwmn'

if KIND == 'acoustic':
    TABLE = 'WaterLevelsContinuous_Acoustic'
    ROOT = 'wlacoustic'
    OP_DESCRIPTION = 'continuous acoustic transducer measurement depth to groundwater'
    DS_DESCRIPTION = 'calculated depth below ground surface. calibrated to manual measurements'
    PID_SQL = '''select L.PointID from WaterLevelsContinuous_Acoustic as T
            join Location L on T.PointID = L.PointID
            where L.PublicRelease=1 and T.MeasuringAgency='NMBGMR'
            group by L.PointID'''
elif KIND == 'pressure':
    TABLE = 'WaterLevelsContinuous_Pressure'
    ROOT = 'wlpressure'
    OP_DESCRIPTION = 'continuous pressure transducer measurement of groundwater head'
    DS_DESCRIPTION = 'calculated depth below ground surface. calibrated to manual measurements'
    PID_SQL = '''select L.PointID from WaterLevelsContinuous_Pressure as T
    join Location L on T.PointID = L.PointID
    where L.PublicRelease=1 and T.QCed=1 and T.MeasuringAgency='NMBGMR'
    group by L.PointID'''
elif KIND == 'manual':
    TABLE = 'WaterLevels'
    ROOT = 'wlmanual'
    OP_DESCRIPTION = 'manual measurement depth to groundwater'
    DS_DESCRIPTION = 'manual measurements'
    PID_SQL = '''select L.PointID from WaterLevels as T
    join Location L on T.PointID=L.PointID
    where L.PublicRelease=1 and T.MeasuringAgency='NMBGMR'
    group by L.PointID
    order by L.PointID
    '''
elif KIND == 'non_NMBGMR_cgwmn':
    TABLE = 'WaterLevels'
    ROOT = 'cgwmn_wlmanual'
    OP_DESCRIPTION = 'manual measurement depth to groundwater'
    DS_DESCRIPTION = 'manual measurements'
    PID_SQL = '''select L.PointID from ProjectLocations as T
        join Location L on T.PointID=L.PointID
        join WaterLevels as WL on WL.PointID=L.PointID
        where L.PublicRelease=1 and WL.MeasuringAgency!='NMBGMR' and T.ProjectName='Water Level Network'
        group by L.PointID
        order by L.PointID
        '''
elif KIND == 'non_MEASURING_AGENCY_cgwmn':
    TABLE = 'WaterLevels'
    ROOT = 'cgwmn_wlmanual_manual_pids'
    OP_DESCRIPTION = 'manual measurement depth to groundwater'
    DS_DESCRIPTION = 'manual measurements'
    PID_SQL = '''select L.PointID from ProjectLocations as T
        join Location L on T.PointID=L.PointID
        join WaterLevels as WL on WL.PointID=L.PointID
        where L.PublicRelease=1 and WL.MeasuringAgency is NULL and T.ProjectName='Water Level Network'
        group by L.PointID
        order by L.PointID
        '''


def extract_thing_properties(p):
    sql = '''select TOP(1) *, 
                    ludr.MEANING as [DR_Meaning], 
                    lumm.MEANING as [MM_Meaning],
                    luam.MEANING as [AM_Meaning]
                    from dbo.{} as P
                join Location as L on L.PointID = P.PointID
                join dbo.LU_MeasurementMethod as lumm on lumm.Code = P.MeasurementMethod
                join dbo.LU_DataReliability as ludr on ludr.Code = L.DataReliability
                join dbo.LU_AltitudeMethod as luam on luam.Code = L.AltitudeMethod

                where P.PointID=%d '''.format(TABLE)
    return execute_query(sql, p)


def extract_data2(p):
    sql = '''select *,
       L.GEOMETRY.STY as [NorthingGG],
       L.GEOMETRY.STX as [EastingGG],
       L.GEOMETRY.STSrid as [SRID],
       ludr.MEANING as [DR_Meaning], 
       lumm.MEANING as [MM_Meaning],
       CAST(DateMeasured as DateTime) as DateTimeMeasured
       from
       NM_Aquifer.dbo.{} as P
       join Location as L on L.PointID = P.PointID
       join dbo.LU_MeasurementMethod as lumm on lumm.Code = P.MeasurementMethod
       join dbo.LU_DataReliability as ludr on ludr.Code = L.DataReliability
       where P.PointID=%d
       order by P.DateMeasured asc '''.format(TABLE)
    return execute_query(sql, p)


def extract_data(p):
    sql = '''select *,
    L.GEOMETRY.STY as [NorthingGG],
    L.GEOMETRY.STX as [EastingGG],
    L.GEOMETRY.STSrid as [SRID],
    ludr.MEANING as [DR_Meaning], 
    lumm.MEANING as [MM_Meaning],
    luam.MEANING as [AM_Meaning],
    CAST(DateMeasured as DateTime) as DateTimeMeasured
    from
    NM_Aquifer.dbo.{} as P
    join Location as L on L.PointID = P.PointID
    join dbo.LU_MeasurementMethod as lumm on lumm.Code = P.MeasurementMethod
    join dbo.LU_DataReliability as ludr on ludr.Code = L.DataReliability
    join dbo.LU_AltitudeMethod as luam on luam.Code = L.AltitudeMethod
    where P.PointID=%d
    order by P.DateMeasured asc '''.format(TABLE)
    return execute_query(sql, p)


def get_things(p):
    resp = requests.get("{}/Things?$filter=name eq '{}'".format(ST, p))
    return resp.json()['value']


def make_location_properties(record):
    return {'altitude': record['Altitude'],
            'altitude_accuracy': record['AltitudeAccuracy'],
            'altitude_datum': record['AltDatum'],
            'altitude_method': record.get('AM_Meaning')}


def make_thing_properties(record):
    agency_key = 'PointID'

    return {'data_reliability': record['DR_Meaning'],
            'project': 'CGWMN',
            'measuring_agency': record['MeasuringAgency'],
            'organization': 'NMBGMR',
            'organization_id': record[agency_key],
            'organization_key': agency_key}


def make_sensor(record):
    return record['MM_Meaning']


def generate_wl(pid, op):
    records = extract_data(pid)
    if not records:
        records = extract_data2(pid)
        if not records:
            print('not records for {}'.format(pid))
            return

    def result(record):
        v = record['DepthToWaterBGS']
        if v is not None:
            return round(v, 2)

    location_name = 'NMWDI-$autoinc'
    thingid = pid
    try:
        obj = assemble(location_name, thingid, records, {'n': 'NorthingGG',
                                                         'e': 'EastingGG',
                                                         'srid': 'SRID',
                                                         'time': 'DateTimeMeasured',
                                                         'result': result,
                                                         'sensor': make_sensor,
                                                         'location_properties': make_location_properties,
                                                         'observed_property_description': OP_DESCRIPTION,
                                                         'datastream_description': DS_DESCRIPTION,
                                                         'thing_properties': make_thing_properties})
        obj['destination'] = 'https://st.newmexicowaterdata.org/FROST-Server/v1.1'

        with open(op, 'w') as wfile:
            json.dump(obj, wfile, indent=2)

        print('wrote {}. nrecords={}'.format(op, len(records)))
        return True
    except BaseException as e:
        print('Failed making {}. {}'.format(op, e))


def get_pointids():
    return [pid['PointID'] for pid in execute_query(PID_SQL)]


def generate_wls():
    badpids = []
    for pid in ['NM-27127', 'WL-0163', 'WL-0167', 'WL-0168', 'WL-0171',
                'WL-0192', 'WL-0195', 'WL-0204', 'WL-0205', 'WL-0206', 'WL-0207']:
        # for pid in get_pointids():
        # print('pid', pid)
        group = pid.split('-')[0]
        root = 'data/{}/orig/{}'.format(ROOT, group)

        r_mkdir(root)

        op = '{}/{}.json'.format(root, pid)
        if not os.path.isfile(op):
            print('generating {}'.format(pid))
            if not generate_wl(pid, op):
                badpids.append(pid)

    print('badpids', badpids)


def reduce_wls(subdir='orig', tag=None):
    for p in get_pointids():
        pp = p
        if tag:
            pp = '{}.{}'.format(p, tag)

        path = 'data/{}/{}/{}.json'.format(ROOT, subdir, pp)
        if not os.path.isfile(path):
            continue

        with open(path, 'r') as rfile:
            yd = json.load(rfile)

        obs = yd['observations']

        def convert(o):
            t, r = o.split(',')
            t = datetime.fromisoformat(t[:-5])
            return t

        timemask = [(convert(o), i, o.split(',')[0], float(o.split(',')[1])) for i, o in enumerate(obs)]

        def key(i):
            return i[0].date()

        nobs = []
        for group, items in groupby(sorted(timemask, key=key), key=key):
            # print(group, list(items))
            items = sorted(items, key=itemgetter(3))
            item = list(items)[0]
            nobs.append('{}, {:0.2f}'.format(item[2], item[3]))

        print('reduced: {}/{}'.format(len(nobs), len(obs)))
        yd['observations'] = nobs
        with open('data/{}/reduced/{}.reduced.json'.format(ROOT, p), 'w') as wfile:
            json.dump(yd, wfile, indent=2)


# def delete_duplicates():
#     for pid in get_pointids():
#         things = get_things(pid)
#         if len(things) > 1:
#             print('thi', things)
#             iots = [i['@iot.id'] for i in things]
#             print(iots)
#
#


def main():
    """

    :return:
    """
    # patch_wls()
    generate_wls()
    # reduce_wls('patched', tag='patched')
    # fix_wls()
    # patch_wls()
    # delete_duplicates()


if __name__ == '__main__':
    main()
# ============= EOF =============================================
# def patch_wls():
#     for pid in get_pointids():
#         print('patching: {}'.format(pid))
#         record = extract_thing_properties(pid)[0]
#         path = 'data/{}/orig/{}.json'.format(ROOT, pid)
#         if not os.path.isfile(path):
#             print('not a file. {}'.format(path))
#             continue
#
#         with open(path, 'r') as rfile:
#             yd = json.load(rfile)
#
#         location = yd['location']
#         location['properties'] = make_location_properties(record)
#
#         thing = yd['thing']
#         thing['properties'] = make_thing_properties(record)
#
#         yd['location'] = location
#         yd['thing'] = thing
#         yd['sensor'] = {'name': make_sensor(record), 'description': 'No Description'}
#
#         path = 'data/{}/patched/{}.patched.json'.format(ROOT, pid)
#         with open(path, 'w') as wfile:
#             json.dump(yd, wfile, indent=2)
# def sort_wls():
#     for p in get_pointids():
#         if p == 'AR-0028':
#             continue
#
#         path = 'data/{}/{}.json'.format(ROOT, p)
#         if not os.path.isfile(path):
#             continue
#
#         with open(path, 'r') as rfile:
#             yd = json.load(rfile)
#
#         obs = yd['observations']
#
#         def skey(obs):
#             t, r = obs.split(',')
#             t = t.strip()
#             return datetime.fromisoformat(t[:-1])
#
#         yd['observations'] = sorted(obs, key=skey)
#         with open('data/{}/sorted/{}.sorted.json'.format(ROOT, p), 'w') as wfile:
#             json.dump(yd, wfile)
