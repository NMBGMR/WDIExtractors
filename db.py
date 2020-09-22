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
import pymssql


def nm_aquifier_connection():
    config = {'database': 'NM_Aquifer',
              'user': environ.get('DB_USER'),
              'password': environ.get('DB_PWD'),
              'server': environ.get('DB_HOST')}
    connection = pymssql.connect(**config)
    return connection


def execute_query(sql, *args):
    conn = nm_aquifier_connection()
    cursor = conn.cursor()
    cursor.execute(sql, *args)
    results = cursor.fetchall()
    conn.close()
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, ri)) for ri in results]

# ============= EOF =============================================
