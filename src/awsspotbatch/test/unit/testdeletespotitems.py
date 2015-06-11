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
Unit test - delete all rows in all DynamoDB tables
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import boto.dynamodb2
import logging
import awsspotbatch.common.const
from boto.dynamodb2.table import Table


def main():     
    """ """
    
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting' )
        
        table_names = [ awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, 
                  awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME,
                  awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME,
                  awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME ]
        dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training')
        
        for table_name in table_names:
            logging.info('Clearing ' + table_name )
            table = Table( table_name, connection=dynamodb_conn )
            items = table.scan()
            for item in items:
                item.delete()
        
        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()
