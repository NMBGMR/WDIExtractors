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

from pysensorthings.base import STBase, Related
from pysensorthings.definitions import FOOT, OM_Observation, UNITS, OTYPES, CASTS


class Location(STBase):
    api_tag = 'Locations'

    def payload(self):
        p = self._base_payload()
        p['encodingType'] = "application/vnd.geo+json"

        p['location'] = self.geometry
        return p


class Thing(Related):
    api_tag = 'Things'
    _location_id = None

    def get_existing(self, url):
        url = 'Locations({})/Things'.format(self._location_id)
        return super(Thing, self).get_existing(url)

    def set_related(self, location):
        self._location_id = location.iotid
        self._related = {'Locations': [location.iotid_]}

    def payload(self):
        p = self._base_payload()
        if isinstance(self.properties, (list, dict)):
            p['properties'] = self.properties

        if self._related:
            p.update(self._related)
            return p


class Sensor(STBase):
    api_tag = 'Sensors'

    def payload(self):
        p = self._base_payload()
        p['encodingType'] = 'application/pdf'
        p['metadata'] = self.metadata
        return p


class ObservedProperty(STBase):
    api_tag = 'ObservedProperties'

    def payload(self):
        p = self._base_payload()
        p['definition'] = self.definition
        return p


class Datastream(Related):
    api_tag = 'Datastreams'
    _thing_id = None

    def get_existing(self, url):
        url = 'Things({})/Datastreams'.format(self._thing_id)
        return super(Datastream, self).get_existing(url)

    @property
    def obslink(self):
        return '{}/Observations'.format(self.selflink)

    def set_related(self, thing, observedproperty, sensor):
        self._thing_id = thing.iotid
        self._related = {'Thing': thing.iotid_,
                         'ObservedProperty': observedproperty.iotid_,
                         'Sensor': sensor.iotid_}

    def payload(self):
        if self._related:
            p = self._base_payload()
            p['unitOfMeasurement'] = UNITS.get(self.unitofMeasurement.lower(), FOOT)
            p['observationType'] = OTYPES.get(self.observationType.lower(), OM_Observation)
            p.update(self._related)
            return p

    def cast(self, result):
        return CASTS.get(self.observationType.lower(), str)(result)


class Observation(Related):
    api_tag = 'Observations'

    _cast = None

    def __init__(self, yl):
        super(Observation, self).__init__()
        t, r = yl.split(',')
        self.phenomenonTime = t.strip()
        self.resultTime = self.phenomenonTime

        self.result = r.strip()

    def set_related(self, datastream):
        self._related = {'Datastream': datastream.iotid_}
        self._cast = datastream.cast

    def payload(self):
        if self._related:
            p = {'phenomenonTime': self.phenomenonTime,
                 'resultTime': self.resultTime,
                 'result': self._cast(self.result)}
            p.update(self._related)
            return p

    def add(self):
        super(Observation, self).add(test_unique=False)
# ============= EOF =============================================
