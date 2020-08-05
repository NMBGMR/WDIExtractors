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


def row_gen(p, delimiter=','):
    with open(p, 'r') as rfile:
        header = next(rfile).strip().split(delimiter)
        header = [h.strip() for h in header]
        for line in rfile:
            row = line.strip().split(delimiter)
            row = dict(zip(header, row))
            yield row


def st_iso_format(t, fmt='%m/%d/%y'):
    t = datetime.strptime(t, fmt)
    return '{}.000Z'.format(t)



# ============= EOF =============================================
