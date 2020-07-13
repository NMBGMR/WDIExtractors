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
from datetime import datetime

import yaml

from public_pointids import PUBLIC_POINTIDS
from os import environ
import pymssql

from toyaml import dicts_toyaml


def nm_aquifier_connection():
    config = {'database': 'NM_Aquifer',
              'user': environ.get('DB_USER'),
              'password': environ.get('DB_PWD'),
              'server': environ.get('DB_HOST')}
    connection = pymssql.connect(**config)
    return connection


def extract_data(p):
    conn = nm_aquifier_connection()
    cursor = conn.cursor()
    sql = '''select * from NM_Aquifer.dbo.WaterLevelsContinuous_Pressure as P
    join Location as L on L.PointID = P.PointID
    where P.PointID=%d
    order by P.DateMeasured asc '''
    cursor.execute(sql, p)

    results = cursor.fetchall()

    conn.close()
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, ri)) for ri in results]


def generate_wl_yml(pid, op):
    records = extract_data(pid)

    def result(record):
        v = record['DepthToWaterBGS']
        return round(v, 2)

    def properties(record):
        agency_key = 'PointID'

        return {'data_source': record['DataSource'],
                'measurement_method': record['MeasurementMethod'],
                'data_reliability': record['DataReliability'],
                'organization': 'NMBGMR',
                'organization_id': record[agency_key],
                'organization_key': agency_key}

    location_name = 'NMWDI-$autoinc'
    thingid = pid
    try:
        dicts_toyaml(location_name, thingid, op, records, {'lat': 'LatitudeDD',
                                                           'lon': 'LongitudeDD',
                                                           'time': 'DateMeasured',
                                                           'result': result,
                                                           'thing_properties': properties})
        print('wrote {}. nrecords={}'.format(op, len(records)))
    except BaseException as e:
        print('Failed making {}. {}'.format(p, e))


def generate_wl_ymls():
    for pid in PUBLIC_POINTIDS:
        op = os.path.join('data', 'wl', '{}.yml'.format(pid))
        generate_wl_yml(pid, op)


def sort_ymls():
    for p in PUBLIC_POINTIDS:
        if p == 'AR-0028':
            continue

        with open('data/wl/{}.yml'.format(p), 'r') as rfile:
            yd = yaml.load(rfile, Loader=yaml.SafeLoader)

        obs = yd['observations']

        def skey(obs):
            t, r = obs.split(',')
            t = t.strip()
            return datetime.fromisoformat(t[:-1])

        yd['observations'] = sorted(obs, key=skey)
        yd['destination'] = 'https://st.newmexicowaterdata.org/FROST-Server/v1.1'
        with open('data/wl/{}.sorted.yml'.format(p), 'w') as wfile:
            yaml.dump(yd, wfile)


def main():
    """

    :return:
    """
    # generate_wl_ymls()
    sort_ymls()


if __name__ == '__main__':
    main()
# ============= EOF =============================================
