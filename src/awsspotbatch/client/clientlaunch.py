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
Launch client's program, as well a heartbeat and termination checker - all on separate threads
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import subprocess
import time
import logging
import awsspotbatch.common.const
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.client.clientparmitem import SpotClientParmItem
from awsspotbatch.client.instancestatus import SpotInstanceStatusThread
from awsspotbatch.common.msg import SpotRequestMsg
from awsspotbatch.common.util import create_microsvc_message_attributes


def main():     
    """ """
    if( len(sys.argv) < 2 ):
        print 'Invalid format, execution cancelled'
        print 'Correct format: python awsspotbatch.spotclientlaunch <parmFile.json>'
        sys.exit(8)
    
    logging.basicConfig( format='%(asctime)s [%(levelname)s] [%(module)s] [%(funcName)s] [%(message)s]', level=logging.INFO )
    logger = logging.getLogger(__name__)

    try:
        spot_client_parm_item = SpotClientParmItem( pathInParmFile=sys.argv[1] )    
        logger.info( 'Starting, region_name=' + spot_client_parm_item.region_name )
        spot_instance_status_thread = SpotInstanceStatusThread( 
                                                             spot_client_parm_item.spot_request_queue_name, 
                                                             spot_client_parm_item.region_name, 
                                                             spot_request_uuid=spot_client_parm_item.spot_request_uuid, 
                                                             spot_master_uuid=spot_client_parm_item.spot_master_uuid, 
                                                             spot_request_id=spot_client_parm_item.spot_request_id )
        spot_instance_status_thread.start()
        child_process = subprocess.Popen( spot_client_parm_item.script_name_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        std_out, std_err = child_process.communicate( )
        returncode = child_process.returncode   
        std_out, std_err = awsspotbatch.common.util.trimStdOutErrSqsPayload( std_out, std_err )
        sqs_message_send_durable = SqsMessageDurable( spot_client_parm_item.spot_request_queue_name,
                                                      spot_client_parm_item.region_name)
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceBatchProcessComplete )
        sqs_message_send_durable.send_message( SpotRequestMsg( spot_request_uuid=spot_client_parm_item.spot_request_uuid,
                                                               spot_master_uuid=spot_client_parm_item.spot_master_uuid,
                                                               spot_request_msg_type=SpotRequestMsg.TYPE_INSTANCE_BATCH_PROCESS_COMPLETE,
                                                               spot_request_id=spot_client_parm_item.spot_request_id,
                                                               name_value_pairs={
                                                                                 SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_COMPLETE_TIMESTAMP:int(time.time()),
                                                                                 SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_RETURNCODE:str(returncode),
                                                                                 SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_STD_OUT:std_out,
                                                                                 SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_STD_ERR:std_err                                                                                        
                                                                                 } ).to_json(),
                                              message_attributes=message_attributes )
        spot_instance_status_thread.shutdown()
        spot_instance_status_thread.join( 60 )
        logger.info( 'Completed Successfully, child_process returncode=' + str(returncode) )

    except StandardError as e:
        spot_instance_status_thread.is_shutdown = True;
        message = ''
        for arg in e.args:
            message = arg + '|'
        logger.error( message )
        logger.error( traceback.format_exc() )
        sqs_message_send_durable = SqsMessageDurable( spot_client_parm_item.spot_request_queue_name,
                                                      spot_client_parm_item.region_name)
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageInstanceBatchProcessStartException )
        sqs_message_send_durable.send_message( SpotRequestMsg( spot_request_uuid=spot_client_parm_item.spot_request_uuid,
                                                               spot_master_uuid=spot_client_parm_item.spot_master_uuid,
                                                               spot_request_msg_type=SpotRequestMsg.TYPE_INSTANCE_BATCH_PROCESS_START_EXCEPTION,
                                                               spot_request_id=spot_client_parm_item.spot_request_id,
                                                               name_value_pairs={
                                                                                 SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_COMPLETE_TIMESTAMP:int(time.time()),
                                                                                 SpotRequestMsg.PAIR_NAME_INSTANCE_BATCH_PROCESS_START_EXCEPTION_MESSAGE:message,
                                                                                 SpotRequestMsg.PAIR_NAME_INSTANCE_BATCH_PROCESS_START_EXCEPTION_TRACEBACK:traceback.format_exc(),                                          
                                                                                 } ).to_json(),
                                              message_attributes=message_attributes  )
        spot_instance_status_thread.join( 60 )
        
        sys.exit(8)

      
if __name__ == "__main__":
    main()