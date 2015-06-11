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
Unit test - serialize ClientParmItem to disk
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

from awsspotbatch.client.clientparmitem import SpotClientParmItem
import awsspotbatch.common.const


def main():
    """ """
    script_name_args = ['python','/home/pete.zybrick/Development/Workspaces/Python/awsspotbatch/src/awsspotbatch/test/unit/testsleep.py', '30' ]
    spot_client_parm_item = SpotClientParmItem(region_name='us-east-1', 
                                               profile_name='ipc-training',
                                               spot_request_uuid='b6eeef8e-e2db-11e4-bd26-101f74edff46', 
                                               spot_master_uuid='b1611204-e2db-11e4-93e8-101f74edff46', 
                                               spot_request_id='sri-2-1429039402',
                                               spot_request_queue_name=awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME, 
                                               script_name_args=script_name_args)
    
    path_out_parm_file = '/home/pete.zybrick/temp/clientparmitem1.json'
    spot_client_parm_item.write_to_file(path_out_parm_file)
    
if __name__ == "__main__":
    main()
