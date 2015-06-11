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
Launch dispatcher on t2.micro EC2 instance, in the future this will be in AWS Lambda
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import logging
import sys
import traceback
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.microsvc.master.spotmasterdispatcher import SpotMasterDispatcher
from awsspotbatch.microsvc.request.spotrequestdispatcher import SpotRequestDispatcher
from awsspotbatch.common.masterparmitem import MasterParmItem


def launch_spot_batch_service():
    """ 
        Launch dispatcher on t2.micro EC2 instance, in the future this will be in AWS Lambda
        1. Multiple instances can (and should) be launched concurrently, i.e. in different AZ's
        2. Install as a service that starts at system boot, this is detailed in the README
    """
    if len(sys.argv) == 1:
        print 'ERROR: Missing log configuration file, first argument must be path/name.ext of the log configuration file'
        sys.exit(8)
    logging.config.fileConfig( sys.argv[1], disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( '**********************************' )
        logger.info( 'Starting SpotBatchMgr Version: ' + awsspotbatch.Version )
        logger.info( '**********************************' )
        if len(sys.argv) == 2:
            logger.error('Missing master parm item file, second argument must be path/name.ext of master parm item json file')
            sys.exit(8)
            
        master_parm_item = MasterParmItem( sys.argv[2] )
        is_purge_queues = False
        if len(sys.argv) > 3 and sys.argv[3] == 'purge': is_purge_queues = True
         
        spot_master_sqs_message_durable = SqsMessageDurable( awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, master_parm_item.region_name, profile_name=master_parm_item.profile_name )
        spot_request_sqs_message_durable = SqsMessageDurable( awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME, master_parm_item.region_name, profile_name=master_parm_item.profile_name )

        if is_purge_queues:
            spot_master_sqs_message_durable.purge_queue()
            spot_request_sqs_message_durable.purge_queue()
        
        spot_master_dispatcher = SpotMasterDispatcher( region_name=master_parm_item.region_name, 
                                                       profile_name=master_parm_item.profile_name )
        spot_request_dispatcher = SpotRequestDispatcher( region_name=master_parm_item.region_name, 
                                                       profile_name=master_parm_item.profile_name )
        
        spot_master_dispatcher.start()
        logger.info("Started: spot_master_dispatcher")
        spot_request_dispatcher.start()
        logger.info("Started: spot_request_dispatcher")
        
        spot_master_dispatcher.join()

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)


if __name__ == '__main__':
    launch_spot_batch_service()
    