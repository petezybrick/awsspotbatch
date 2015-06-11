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
All values necessary to deploy and restart services
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import json

class DeployParmItem(object):
    """All values necessary to deploy and restart services"""


    def __init__(self, json_path_name_ext ):
        """

        :param json_path_name_ext: parm file path/name.ext

        """
        with open( json_path_name_ext ) as json_data:
            map_parms = json.load(json_data)
            json_data.close()
            
        self.instance_ids = map_parms[ "instance_ids" ]
        self.key_path_name_ext = map_parms[ "key_path_name_ext" ]
        self.client_bootstrap_local_path_name_ext = map_parms[ "client_bootstrap_local_path_name_ext" ]
        self.client_bootstrap_remote_path_name_ext = map_parms[ "client_bootstrap_remote_path_name_ext" ]
        self.client_cmds_local_path_name_ext = map_parms[ "client_cmds_local_path_name_ext" ]
        self.client_cmds_remote_path_name_ext = map_parms[ "client_cmds_remote_path_name_ext" ]
        self.awsext_zip_local_path_name_ext = map_parms[ "awsext_zip_local_path_name_ext" ]
        self.awsext_zip_remote_path_name_ext = map_parms[ "awsext_zip_remote_path_name_ext" ]
        self.awsspotbatch_zip_local_path_name_ext = map_parms[ "awsspotbatch_zip_local_path_name_ext" ]
        self.awsspotbatch_zip_remote_path_name_ext = map_parms[ "awsspotbatch_zip_remote_path_name_ext" ]
        self.remote_cmd = map_parms[ "remote_cmd" ]

