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
Spot Request (i.e. user script on spot instance) has completed
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import traceback
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg, SpotRequestMsg
from awsspotbatch.common.tabledef import SpotRequestStateCode, TableSpotRequest
from awsspotbatch.microsvc.request.spotrequestmessagebase import SpotRequestMessageBase
from awsspotbatch.microsvc.request import spot_request_row_partial_save, get_spot_request_item, fmt_request_item_msg_hdr
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.util import create_microsvc_message_attributes


import logging
logger = logging.getLogger(__name__)


class SpotRequestMessageInstanceBatchProcessComplete(SpotRequestMessageBase):
    """Spot Request (i.e. user script on spot instance) has completed"""

    def __init__( self,
                  spot_master_table_name=awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME, 
                  spot_master_queue_name=awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, 
                  spot_request_table_name=awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME, 
                  spot_request_queue_name=awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME, 
                  spot_rsa_key_table_name=awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME, 
                  spot_batch_job_parm_table_name=awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME,
                  region_name='us-east-1', profile_name=None ):
        """

        :param spot_master_table_name:  (Default value = awsspotbatch.common.const.SPOT_MASTER_TABLE_NAME)
        :param spot_master_queue_name:  (Default value = awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME)
        :param spot_request_table_name:  (Default value = awsspotbatch.common.const.SPOT_REQUEST_TABLE_NAME)
        :param spot_request_queue_name:  (Default value = awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME)
        :param spot_rsa_key_table_name:  (Default value = awsspotbatch.common.const.SPOT_RSA_KEY_TABLE_NAME)
        :param spot_batch_job_parm_table_name:  (Default value = awsspotbatch.common.const.SPOT_BATCH_JOB_PARM_TABLE_NAME)
        :param region_name:  (Default value = 'us-east-1')
        :param profile_name:  (Default value = None)

        """

        SpotRequestMessageBase.__init__(self, spot_master_table_name=spot_master_table_name, 
                  spot_master_queue_name=spot_master_queue_name, 
                  spot_request_table_name=spot_request_table_name, 
                  spot_request_queue_name=spot_request_queue_name, 
                  spot_rsa_key_table_name=spot_rsa_key_table_name, 
                  spot_batch_job_parm_table_name=spot_batch_job_parm_table_name,
                  region_name=region_name, profile_name=profile_name )

        
    def process( self, message ) :
        """ 
            Spot Request has completed, write completion info to SpotRequestItem in DynamoDB,
            let master know this request has completed so the master can determine if the job has completed

        :param message: SQS Message instance

        """
        try:
            spot_request_msg = SpotRequestMsg( raw_json=message.get_body() )
            spot_request_item = get_spot_request_item( self.spot_request_table_name, spot_request_msg.spot_request_uuid, region_name=self.region_name, profile_name=self.profile_name )
            ts_cmd_complete = spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_COMPLETE_TIMESTAMP]
            cmd_returncode = spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_RETURNCODE]
            cmd_std_out = spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_STD_OUT]
            cmd_std_err = spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_BATCH_PROCESS_STD_ERR]
            key_value_pairs = {
                                TableSpotRequest.is_open:0,
                                TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_complete,
                                TableSpotRequest.ts_cmd_complete:ts_cmd_complete,
                                TableSpotRequest.cmd_returncode:cmd_returncode
                                 }
            if cmd_std_out != None and len(cmd_std_out) > 0: key_value_pairs[ TableSpotRequest.cmd_std_out ] = cmd_std_out
            if cmd_std_err != None and len(cmd_std_err) > 0: key_value_pairs[ TableSpotRequest.cmd_std_err ] = cmd_std_err       
            spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, key_value_pairs,
                                           region_name=self.region_name, profile_name=self.profile_name  )
            # let the Master increment the completion count to determine if the job is complete
            master_msg_incr_instance_success = SpotMasterMsg( spot_master_uuid=spot_request_msg.spot_master_uuid, 
                                                              spot_master_msg_type=SpotMasterMsg.TYPE_INCR_INSTANCE_SUCCESS_CNT )
            message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageIncrSuccessCnt )
            spot_master_sqs_message_durable = SqsMessageDurable( self.spot_master_queue_name, self.region_name, profile_name=self.profile_name )
            spot_master_sqs_message_durable.send_message( master_msg_incr_instance_success.to_json(),
                                                               message_attributes=message_attributes )
            self.spot_request_sqs_message_durable.delete_message(message)

        except StandardError as e:
            logger.error( fmt_request_item_msg_hdr( spot_request_item ) + 'Exiting SpotRequestDispatcher due to exception'  )
            logger.error( fmt_request_item_msg_hdr( spot_request_item ) + str(e) )
            logger.error( fmt_request_item_msg_hdr( spot_request_item ) + traceback.format_exc() )    
