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
from os import environ
import re

import requests

IDREGEX = re.compile(r'(?P<id>\(\d+\))')


class STBase:
    iotid = None
    api_tag = None
    selflink = None

    def __init__(self, yd=None):
        self._yd = yd
        self.base_url = environ.get('ST_URL')

    def __getattr__(self, item):
        return self._yd.get(item, 'No {}'.format(item))

    @property
    def iotid_(self):
        return {'@iot.id': self.iotid}

    def add(self, test_unique=True):
        """
        post to configured ST instance
        :return:
        """
        if not test_unique or not self.get_existing(self.api_tag):
            print('payload', self.payload())
            resp = requests.post('{}/{}'.format(self.base_url, self.api_tag), json=self.payload())
            print('response', resp.text)
            m = IDREGEX.search(resp.headers.get('location'))
            if m:
                iotid = m.group('id')[1:-1]
            else:
                iotid = resp.json()['@iot.id']

            self.setiotid(iotid)

    def setiotid(self, iotid):
        self.iotid = int(iotid)
        self.selflink = '{}/{}({})'.format(self.base_url, self.api_tag, self.iotid)

    def get_existing(self, url):
        resp = requests.get("{}/{}?$filter=name eq '{}'".format(self.base_url, url, self.name))
        try:
            self.setiotid(resp.json()['value'][0]['@iot.id'])
            return self.iotid
        except (KeyError, IndexError, TypeError):
            pass

    def payload(self):
        raise NotImplementedError

    def _base_payload(self):
        return {'name': self.name, 'description': self.description}

    # def _get_related(self, base, expand):
    #     url = '{}/{}?$expand={}'.format(self.base_url, base, expand)
    #     resp = requests.get(url)
    #     obj = resp.json()
    #     print(obj)
    #     if expand in obj:
    #         for ti in obj[expand]:
    #             if ti['name'] == self.name:
    #                 return ti['@iot.id']

    def _get_items(self, url, callback=None):
        items = []

        def _get(u):
            resp = requests.get(u)
            j = resp.json()

            if callback:
                callback(items, j)
            else:
                try:
                    items.extend(j['value'])
                except KeyError:
                    items.append(j)

            try:
                next = j['@iot.nextLink']
            except KeyError:
                return

            _get(next)

        _get(url)
        return items


class Related(STBase):
    _related = None

    def set_related(self, *args, **kw):
        raise NotImplementedError

# ============= EOF =============================================
