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
from util import row_gen, st_iso_format
import json


def transform(row):
    location = {'name': 'NMWDI-NPDES-$autoinc',
                'description': 'NM_NPDES import',
                'geometry': {'type': 'Point',
                             'coordinates': [float(row['LONGITUDE']),
                                             float(row['LATITUDE'])]}}

    properties = {'organization': 'NMED',
                  'organization_id': row['PERMITNUMB'],
                  'organization_key': 'PERMITNUMB',
                  'major_minor': row['MAJOR_MINO'],
                  'locationty': row['LOCATIONTY'],
                  'locationna': row['LOCATIONNA'],
                  'facilityna': row['FACILITYNA']}

    thing = {'name': row['PERMITNUMB'],
             'description': 'NM_NPDES permit',
             'properties': properties}

    ds = {'name': 'Permit Duration',
          'description': 'effective and expiration dates for this permit'}

    obsp = {'name': row['PERMITTYPE'],
            'description': 'PERMITTYPE'}

    fmt = '%Y-%m-%d'
    obs = ['{}, effective'.format(st_iso_format(row['EFFECTIVE'], fmt)),
           '{}, expiration'.format(st_iso_format(row['EXPIRATION'], fmt))]

    obj = {'location': location,
           'thing': thing,
           'sensor': {'name': 'Permitting App',
                      'description': 'No Description'},
           'datastream': ds,
           'observed_property': obsp,
           'observations': obs}

    return thing['name'], obj


def main():
    p = './data/NM_NPDES/active.csv'
    for row in row_gen(p, delimiter='\t'):
        n,obj = transform(row)
        print(n, obj)
        op = './data/NM_NPDES/out/{}.json'.format(n)
        with open(op, 'w') as wfile:
            json.dump(obj, wfile, indent=4)



if __name__ == '__main__':
    main()
# ============= EOF =============================================
