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
Exception occuring while checking instance metadata for pending termination
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import traceback
import time
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotRequestMsg
from awsspotbatch.microsvc.request.spotrequestmessagebase import SpotRequestMessageBase
from awsspotbatch.microsvc.request import fmt_request_uuid_msg_hdr
from awsspotbatch.common.tabledef import TableSpotRequest, SpotRequestStateCode
from awsspotbatch.microsvc.request import spot_request_row_partial_save, get_spot_request_item

import logging
logger = logging.getLogger(__name__)


class SpotRequestMessageInstancePendingTerminationException(SpotRequestMessageBase):
    """
        Exception occuring while checking instance metadata for pending termination
        This should never happen unless 1) metadata can't be accessed (a huge issue) or 2) AWS changes the metadata response
        """

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
        """ This should never happen, so don't terminate the instance, leave it up so it can get SSH'ed into
            to determine the cause of failure

        :param message: SQS Message instance

        """
        try:
            spot_request_msg = SpotRequestMsg( raw_json=message.get_body() )
            instance_termination_exception = spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_INSTANCE_TERMINATION_TIME_EXCEPTION ]
            spot_request_item = get_spot_request_item( self.spot_request_table_name, spot_request_msg.spot_request_uuid, region_name=self.region_name, profile_name=self.profile_name )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + 'process_pending_termination_exception for spot_request_uuid, instance_termination_exception=' + instance_termination_exception )
            ts_now = int( time.time() )
            spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, 
                                           {
                                            TableSpotRequest.is_open:0,
                                            TableSpotRequest.ts_end:ts_now,
                                            TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_force_termination_exception,
                                            TableSpotRequest.instance_termination_exception:instance_termination_exception
                                            },
                                            region_name=self.region_name, profile_name=self.profile_name  )
            self.spot_request_sqs_message_durable.delete_message(message)            
    
        except StandardError as e:
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + 'Exiting SpotRequestDispatcher due to exception'  )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + str(e) )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + traceback.format_exc() )    
