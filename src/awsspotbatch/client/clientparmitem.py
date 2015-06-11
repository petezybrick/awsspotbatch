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
Short Description
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import json
import awsspotbatch.common.const


class SpotClientParmItem(object):
    """Attributes required to launch and track the execution of the client program"""


    def __init__(self, pathInParmFile=None, region_name=None, 
                 spot_request_uuid=None, spot_master_uuid=None, spot_request_id=None, 
                 spot_request_queue_name=awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME,
                 script_name_args=None ):
        """

        :param pathInParmFile: client job-specific parm file  (Default value = None)
        :param region_name: region name (Default value = None)
        :param spot_request_uuid: spot request uuid (Default value = None)
        :param spot_master_uuid: spot master uuid (Default value = None)
        :param spot_request_id: spot request id (Default value = None)
        :param spot_request_queue_name: spot request queue name (Default value = awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME)
        :param script_name_args: name and arguments of script to be executed (Default value = None)

        """
        if pathInParmFile != None:
            with open( pathInParmFile ) as json_data:
                parms = json.load(json_data)
                json_data.close()
            self.region_name = parms['region_name']     
            self.spot_request_uuid = parms['spot_request_uuid']
            self.spot_master_uuid = parms['spot_master_uuid']
            self.spot_request_id = parms['spot_request_id']
            self.spot_request_queue_name = parms['spot_request_queue_name']
            self.script_name_args = parms['script_name_args']
            
        else:
            self.region_name = region_name
            self.spot_request_uuid = spot_request_uuid
            self.spot_master_uuid = spot_master_uuid
            self.spot_request_id = spot_request_id
            self.spot_request_queue_name = spot_request_queue_name
            self.script_name_args = script_name_args

        
    def to_json(self):
        """ """
        json_dict = { 
                     'region_name':self.region_name,
                     'spot_request_uuid':self.spot_request_uuid,
                     'spot_master_uuid':self.spot_master_uuid,
                     'spot_request_id':self.spot_request_id,
                     'spot_request_queue_name':self.spot_request_queue_name,
                     'script_name_args':self.script_name_args,                     
                     }
        return json.dumps(json_dict)


    def write_to_file(self, path_out_parm_file ):
        """

        :param path_out_parm_file: 

        """
        raw_json = self.to_json()
        with open(path_out_parm_file, "w") as text_file:
            text_file.write( raw_json )
        