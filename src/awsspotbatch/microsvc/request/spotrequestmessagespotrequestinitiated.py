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
Initiate Spot Request
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import traceback
import time
import boto.dynamodb2
import awsspotbatch.common.exception
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotRequestMsg
from awsspotbatch.common.tabledef import SpotRequestStateCode, TableSpotRequest
from awsspotbatch.microsvc.request import fmt_request_uuid_msg_hdr
from awsspotbatch.microsvc.request.spotrequestmessagebase import SpotRequestMessageBase
from awsspotbatch.common.util import create_microsvc_message_attributes


import logging
logger = logging.getLogger(__name__)


class SpotRequestMessageSpotRequestInitiated(SpotRequestMessageBase):
    """Initiate Spot Request"""

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
        """ Start SpotRequest process
            1. Create item in SpotRequestItem table
            2. queue up potRequestMessageCheckStatus, this will start the state-based process

        :param message: SQS Message instance

        """
        try:
            spot_request_msg = SpotRequestMsg( raw_json=message.get_body() )
            logger.info( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + 'process_spot_request_initiated for spot_master_uuid: ' + spot_request_msg.spot_master_uuid )
            ts_now = int( time.time() )
            dict_create_spot_request_item = {
                                        TableSpotRequest.spot_request_uuid:spot_request_msg.spot_request_uuid,
                                        TableSpotRequest.spot_master_uuid:spot_request_msg.spot_master_uuid,
                                        TableSpotRequest.spot_request_id:spot_request_msg.spot_request_id,
                                        TableSpotRequest.ts_last_state_check:ts_now,
                                        TableSpotRequest.attempt_number:spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_ATTEMPT_NUMBER ],
                                        TableSpotRequest.spot_price:spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_SPOT_PRICE ],
                                        TableSpotRequest.instance_username:spot_request_msg.name_value_pairs[ SpotRequestMsg.PAIR_NAME_INSTANCE_USERNAME ],
                                        TableSpotRequest.is_open:1,
                                        TableSpotRequest.spot_request_state_code:SpotRequestStateCode.spot_request_in_progress,
                                        TableSpotRequest.ts_start:ts_now,
                                       }
            put_attempt_cnt = 0
            put_attempt_max = 10
            while True:
                dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
                spot_request_table = Table( self.spot_request_table_name, connection=dynamodb_conn ) 
                result_spot_request_put = spot_request_table.put_item(data=dict_create_spot_request_item)
                if result_spot_request_put: break
                put_attempt_cnt += 1
                if put_attempt_cnt == put_attempt_max: 
                    raise awsspotbatch.common.exception.DynamoDbPutItemMaxAttemptsExceeded('Failed attempt to insert item in: ' + self.spot_request_table_name + 
                                                             ' for spot_request_uuid: ' + spot_request_msg.spot_request_uuid, self.spot_request_table_name )
                time.sleep(6)
            
            next_status_msg_delay_secs = 30
            spot_request_msg_check_status = SpotRequestMsg( 
                                                           spot_request_uuid=spot_request_msg.spot_request_uuid, 
                                                           spot_master_uuid=spot_request_msg.spot_master_uuid,
                                                           spot_request_msg_type=SpotRequestMsg.TYPE_CHECK_STATUS, 
                                                           spot_request_id=spot_request_msg.spot_request_id )
            message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageCheckStatus )
            self.spot_request_sqs_message_durable.send_message( spot_request_msg_check_status.to_json(), delay_seconds=next_status_msg_delay_secs, message_attributes=message_attributes )
            self.spot_request_sqs_message_durable.delete_message(message) 
            
        except StandardError as e:
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + 'Exiting SpotRequestDispatcher due to exception'  )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + str(e) )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_msg.spot_request_uuid ) + traceback.format_exc() )    
