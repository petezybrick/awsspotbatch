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
Unit testing of Master and Request tables - load with simple data
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import boto.dynamodb2
from boto.dynamodb2.table import Table


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
        
        master_table.put_item(data={
                              'uuid':'aaaa',
                              'is_open':True,
                              'ts_start':1111
                              })
        master_table.put_item(data={
                               'uuid':'bbbb',
                              'is_open':False,
                              'ts_start':2222
                              })
        
        master_table.put_item(data={
                              'uuid':'cccc',
                              'is_open':True,
                              'ts_start':3333
                              })
        master_table.put_item(data={
                               'uuid':'dddd',
                              'is_open':False,
                              'ts_start':4444
                              })

        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == "__main__":
    main()