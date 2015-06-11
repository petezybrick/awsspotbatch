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
Unit test - simple table put/get to verify IAM permissions
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import time
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection
from awsext.dynamodb import delete_and_wait_until_tables_deleted, wait_until_tables_active
from boto.dynamodb2.types import NUMBER


def main():     
    """ """
    
    import logging.config
    logging.config.fileConfig( '../../config/consoleonly.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting' )
        master_tablename = 'spotbatch.master'
        request_tablename = 'spotbatch.request'
        dynamodb_conn = boto.dynamodb2.connect_to_region( 'us-east-1', profile_name='ipc-training')
                    
        master_table = Table( master_tablename, connection=dynamodb_conn ) 
        request_table = Table( request_tablename, connection=dynamodb_conn ) 
        
        data_item = master_table.get_item( uuid='aaaa' )
        logger.info( str(data_item))
        
        data_item['ts_start'] = 5555
        data_item['ts_end'] = 7777
        result = data_item.partial_save()
        print result
        
        
        
#         data_item = master_table.query_2(limit, index, reverse, consistent, attributes, max_page_size, query_filter, conditional_operator)
#         get_item( uuid='aaaa' )
        logger.info( str(data_item))

        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()