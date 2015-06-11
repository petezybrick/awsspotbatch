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
Unit test - display spot instance status' based on Master UUID
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import awsspotbatch.common.const
import boto.dynamodb2
import awsext.ec2
from boto.dynamodb2.table import Table
from awsspotbatch.common.tabledef import TableSpotRequest
   

def find_spot_request_instance_ids_by_master_uuid( spot_master_uuid ):
    """

    :param spot_master_uuid: 

    """
    instance_ids = [] 
    dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training')                
    request_table = Table( awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, connection=dynamodb_conn ) 
    spot_request_items = request_table.query_2( spot_master_uuid__eq=spot_master_uuid, index='MasterUuid' )
    for spot_request_item in spot_request_items:
        instance_ids.append(spot_request_item[ TableSpotRequest.instance_id ])
    return instance_ids

    
def main():
    """ """
    instance_ids = find_spot_request_instance_ids_by_master_uuid( '28dd21b6-ea9c-11e4-a29b-101f74edff46' )
    print instance_ids
    ec2_conn = awsext.ec2.connect_to_region( 'us-east-1', profile_name='ipc-training' )
    num_spot_instances = len( instance_ids )
    num_terminated = 0
    for spot_request_instance_id in instance_ids:
        state_name, status = ec2_conn.get_instance_state_name_and_status( spot_request_instance_id )
        print state_name

      
if __name__ == "__main__":
    main()
