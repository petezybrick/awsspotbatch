# Copyright 2015 IPC Global (http://www.ipc-global.com) and others.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Master Parm Item
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import json
from awsext.vpc.connection import InboundRuleItem


class MasterParmItem(object):
    """Attributes required to launch Master and Request dispatchers"""

    def __init__(self, pathParmFile ):
        """

        :param pathParmFile: path/name.ext of Master Parm file

        """
        with open( pathParmFile ) as json_data:
            map_parms = json.load(json_data)
            json_data.close()

        self.region_name = map_parms['region_name']
        self.profile_name = map_parms[ 'profile_name']
        if self.profile_name == 'None': self.profile_name = None
        temp_boolean = map_parms[ 'dry_run' ].lower()
        if 'false' == temp_boolean: self.dry_run = False
        else: self.dry_run = True