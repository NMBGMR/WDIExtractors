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
from pyclowder import files
from pyclowder.client import ClowderClient
from requests_toolbelt import MultipartEncoder

UPLOAD_KEY = 'e6677b36-58ba-4adb-a20c-093b25abdbe2'

MANUALWL = '5f555bb2e4b0630bcb239184'
ROOT = 'data/wlmanual/orig/'
SKIP = ['', 'AH', 'WL', 'AR', 'AS', 'CP', 'EB']


NON_NMBGMR_CGWMN_MANUAL = '5f5f88ffe4b0630bcb24e11f'
ROOT = 'data/cgwmn_wlmanual/orig/'
SKIP = ['']

NON_AGENCY_CGWMN_MANUAL = '5f5fa2a8e4b0630bcb252207'
ROOT = 'data/cgwmn_wlmanual_non_agency/orig/'
SKIP = ['']

CGWMN_MANUAL = '5f5fadf0e4b0630bcb253efa'
ROOT = 'data/cgwmn_wlmanual_manual_pids/orig/'
SKIP = ['']

WQ_NMBGMR = '5f64da72e4b0630bcb254b9d'
ROOT = 'data/wqs'

COLLECTION = WQ_NMBGMR


class UploadClowderClient:
    def __init__(self):
        self._clt = ClowderClient(host='https://clowder.newmexicowaterdata.org',
                                  key=UPLOAD_KEY)
        # username='', password='')

    # def create_collection(self, name):
    #     content = {'name': name,
    #                'description': ''}
    #     url ='collections'
    #     resp = self._clt.post(url, content)
    #     return resp['id']

    def create_dataset(self, name, collectionid):

        content = {"name": name,
                   "description": '',
                   "collection": [collectionid]}
        print('craet', content)
        url = 'datasets/createempty'
        resp = self._clt.post(url, content)
        return resp['id']

    def add_files(self, root):
        for d, ds, fs in os.walk(root):
            dname = os.path.basename(d)
            if dname in SKIP:
                continue

            # create dataset
            datasetid = self.create_dataset(dname, COLLECTION)

            url = 'uploadToDataset/{}'.format(datasetid)
            for fi in fs:
                p = os.path.join(d, fi)
                print('upload {}'.format(p))
                self._clt.post_file(url, p)


def main():
    clt = UploadClowderClient()
    clt.add_files(ROOT)


if __name__ == '__main__':
    main()

# ============= EOF =============================================
