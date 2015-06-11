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
Test locally - launch master and request dispatchers, run test batch job
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import uuid
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.msg import SpotMasterMsg
from awsspotbatch.microsvc.master.spotmasterdispatcher import SpotMasterDispatcher
from awsspotbatch.microsvc.request.spotrequestdispatcher import SpotRequestDispatcher
from awsspotbatch.common.masterparmitem import MasterParmItem
from awsspotbatch.common.util import create_microsvc_message_attributes


import logging
logger = logging.getLogger(__name__)            


def create_spot_master_msg_batch_submit( path_batch_job_parm_file, path_user_job_parm_file=None ):
    """

    :param path_batch_job_parm_file: 
    :param path_user_job_parm_file:  (Default value = None)

    """
    with open( path_batch_job_parm_file ) as parm_file:
        raw_batch_job_parm_item = parm_file.read()
        
    if path_user_job_parm_file != None:   
        with open( path_user_job_parm_file ) as parm_file:
            raw_user_job_parm_item = parm_file.read()
    else: raw_user_job_parm_item = None

    spot_master_uuid = str(uuid.uuid1())
    logger.info('Submitting test batch message, spot_master_uuid=' + spot_master_uuid )
    spot_master_msg = SpotMasterMsg( spot_master_uuid=spot_master_uuid, spot_master_msg_type=SpotMasterMsg.TYPE_SUBMIT_BATCH,
                                     raw_batch_job_parm_item=raw_batch_job_parm_item, raw_user_job_parm_item=raw_user_job_parm_item)
    return spot_master_msg

        
def main():
    """ """
    import logging.config
    logging.config.fileConfig( '../../../../config/consoleandfile.conf', disable_existing_loggers=False)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info( 'Starting' )
        master_parm_item = MasterParmItem( sys.argv[1] )
         
        spot_master_sqs_message_durable = SqsMessageDurable( awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, master_parm_item.region_name, profile_name=master_parm_item.profile_name )
        spot_request_sqs_message_durable = SqsMessageDurable( awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME, master_parm_item.region_name, profile_name=master_parm_item.profile_name )

        # TEST TEST TEST - only during development
        spot_master_sqs_message_durable.purge_queue()
        spot_request_sqs_message_durable.purge_queue()
        
        spot_master_dispatcher = SpotMasterDispatcher( region_name=master_parm_item.region_name, 
                                                       profile_name=master_parm_item.profile_name )
        spot_request_dispatcher = SpotRequestDispatcher( region_name=master_parm_item.region_name, 
                                                       profile_name=master_parm_item.profile_name )
        
        spot_master_dispatcher.start()
        spot_request_dispatcher.start()

        spot_master_msg_batch_submit = create_spot_master_msg_batch_submit( sys.argv[2], sys.argv[3] )
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageSubmitBatch )
        spot_master_sqs_message_durable.send_message( spot_master_msg_batch_submit.to_json(),
                                                      message_attributes=message_attributes )
         
        spot_master_dispatcher.join()
        
        logger.info( 'Completed Successfully' )

    except StandardError as e:
        logger.error( e )
        logger.error( traceback.format_exc() )
        sys.exit(8)

      
if __name__ == '__main__':
    main()