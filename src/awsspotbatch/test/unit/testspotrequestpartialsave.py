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
Unit test - partial save of DynamoDB item
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import logging
import boto.dynamodb2
from awsspotbatch.common.tabledef import TableSpotRequest
from boto.dynamodb2.table import Table



def main():     
    """ """
    
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting' )
        spot_request_table_name = 'spotbatch.spotrequest'
        spot_request_uuid = '90719024-e546-11e4-9020-101f74edff46'
        dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training')
        ts_pending_termination_detected = '2015-01-05T18:02:00Z' 
                    
        spot_request_table = Table( spot_request_table_name, connection=dynamodb_conn ) 
        spot_request_item = spot_request_table.get_item( spot_request_uuid=spot_request_uuid )
        
        spot_request_item[TableSpotRequest.ts_pending_termination_detected] = ts_pending_termination_detected
        partial_save_result = spot_request_item.partial_save()
        logger.info(partial_save_result)
        

        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()
