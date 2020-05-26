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
import yaml
import logging
from pyclowder.utils import StatusMessage
from pyclowder.extractors import Extractor
import pyclowder.files

from pysensorthings.objects import Location, Thing, Sensor, ObservedProperty, Datastream, Observation


def process_meta(yd):
    location = Location(yd['location'])
    location.add()

    thing = Thing(yd['thing'])
    thing.set_related(location)
    thing.add()

    sensor = Sensor(yd['sensor'])
    sensor.add()

    obprop = ObservedProperty(yd['observed_property'])
    obprop.add()

    ds = Datastream(yd['datastream'])
    ds.set_related(thing, obprop, sensor)
    ds.add()

    for oi in yd['observations']:
        obs = Observation(oi)
        obs.set_related(ds)
        obs.add()

    metadata = {
        'sensorthings': {'location_link': location.selflink,
                         'thing_link': thing.selflink,
                         'sensor_link': sensor.selflink,
                         'observed_property_link': obprop.selflink,
                         'datastream_link': ds.selflink,
                         'observations_link': ds.obslink
                         }
    }
    return metadata


def validate_file(yd):
    return all((key in yd
                for key in ('location', 'thing',
                            'sensor', 'observed_property',
                            'datastream', 'observations')))


def st_upload(input_file):
    """
    take a upload.yml file and add to sensor things

    :param input_file:
    :return:
    """
    ret = {}
    with open(input_file, 'r') as rfile:

        try:
            yd = yml.load(rfile, Loader=yml.SafeLoader)
        except BaseException as e:
            yd = None
            print('asdf', e)

        if yd:
            if validate_file(yd):
                metadata = process_meta(yd)
                ret['metadata'] = metadata

    return ret


class STExtractor(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):
        logger = logging.getLogger(__name__)
        inputfile = resource["local_paths"][0]
        file_id = resource['id']

        ret = st_upload(inputfile)
        if ret:
            metadata = self.get_metadata(ret['metadata'], 'file', file_id, host)
            logger.debug(metadata)

            # upload metadata
            pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)

            # set tags
            tags = {'tags': ['SensorThings']}
            pyclowder.files.upload_tags(connector, host, secret_key, file_id, tags)
            connector.status_update(StatusMessage.processing, {"type": "file", "id": file_id}, "Deleting file tags.")

            # delete tags
            headers = {'Content-Type': 'application/json'}
            url = '{}api/files/{}/tags?key={}'.format(host, file_id, secret_key)
            tags = {'tags': ['QCNeeded']}
            connector.delete(url, headers=headers, data=json.dumps(tags),
                             verify=connector.ssl_verify if connector else True)


if __name__ == '__main__':
    e = STExtractor()
    e.start()
# ============= EOF =============================================
