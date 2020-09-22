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

import requests

from db import execute_query
from util import r_mkdir

ST = 'https://st.newmexicowaterdata.org/FROST-Server/v1.1'
ROOT = 'data/cgwmn_patch'
PWD = os.getenv('WRITE_PWD')


def get_thing(p):
    resp = requests.get("{}/Things?$filter=name eq '{}'".format(ST, p))
    ts = resp.json()['value']
    if ts:
        return ts[0]


def get_pointids():
    sql = '''
    select WL.PointID, WL.MeasuringAgency from ProjectLocations as T
    join WaterLevels as WL on T.PointID = WL.PointID
    --join Location as L on L.PointID = T.PointID
    join WaterLevelsContinuous_Acoustic as WL on T.PointID = WL.PointID
    where T.ProjectName='Water Level Network' and L.PublicRelease=1
    group by WL.PointID, WL.PointID, WL.MeasuringAgency
    order by WL.PointID
    '''
    pids = [(pid['PointID'], pid['MeasuringAgency']) for pid in execute_query(sql)]

    return pids


def patch_thing(thing, ma):
    url = thing['@iot.selfLink']
    props = thing['properties']
    props['project'] = 'CGWMN'

    props['measuring_agency'] = ma
    nthing = {'properties': props, 'url': url}
    with open(os.path.join(ROOT, '{}.json'.format(thing['name'])), 'w') as wfile:
        json.dump(nthing, wfile, indent=2)


def post_patch(p):
    with open(os.path.join(ROOT, p), 'r') as rfile:
        obj = json.load(rfile)
        url = obj['url']
        properties = obj['properties']

        properties['measuring_agency'] = properties.get('MeasuringAgency', properties.get('measuring_agency'))
        del properties['MeasuringAgency']
        thing = {'properties': properties}

    resp = requests.patch(url,
                          auth=('write', PWD),
                          json=thing)
    print(p, resp)


def gen_patches():
    badthings = []

    r_mkdir(ROOT)
    for pid, ma in get_pointids():
        if os.path.isfile(os.path.join(ROOT, '{}.json'.format(pid))):
            continue

        print('pid: {}, ma={}'.format(pid, ma))
        thing = get_thing(pid)
        if thing:
            patch_thing(thing, ma)
        else:
            badthings.append(pid)

    print('badthings: {}'.format(badthings))


def post_patches():
    for f in os.listdir(ROOT):
        post_patch(f)


def main():
    post_patches()
    # gen_patches()


if __name__ == '__main__':
    main()

# ============= EOF =============================================
# ['BC-0333', 'NM-00841', 'NM-27127', 'sv-0130', 'TB-0139', 'TB-0139', 'TB-0139', 'TB-0255', 'TB-0255', 'WL-0003',
# 'WL-0009', 'WL-0010', 'WL-0012', 'WL-0019', 'WL-0020', 'WL-0021', 'WL-0021', 'WL-0029', 'WL-0066', 'WL-0066',
# 'WL-0074', 'WL-0111', 'WL-0119', 'WL-0138', 'WL-0139', 'WL-0161', 'WL-0163', 'WL-0167', 'WL-0168', 'WL-0171',
# 'WL-0175', 'WL-0192', 'WL-0195', 'WL-0204', 'WL-0205', 'WL-0206', 'WL-0207']
