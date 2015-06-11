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
Process the SpotMasterMessageIncrSuccessCnt message
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import traceback
import boto.dynamodb2
from boto.dynamodb2.table import Table
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg
from awsspotbatch.common.tabledef import TableSpotMaster
from awsspotbatch.microsvc.master.spotmastermessagebase import SpotMasterMessageBase
from awsspotbatch.microsvc.master import fmt_master_item_msg_hdr


import logging
logger = logging.getLogger(__name__)


class SpotMasterMessageIncrSuccessCnt(SpotMasterMessageBase):
    """ Process the SpotMasterMessageIncrSuccessCnt message"""

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

        SpotMasterMessageBase.__init__(self, spot_master_table_name=spot_master_table_name, 
                  spot_master_queue_name=spot_master_queue_name, 
                  spot_request_table_name=spot_request_table_name, 
                  spot_request_queue_name=spot_request_queue_name, 
                  spot_rsa_key_table_name=spot_rsa_key_table_name, 
                  spot_batch_job_parm_table_name=spot_batch_job_parm_table_name,
                  region_name=region_name, profile_name=profile_name )

        
    def process( self, message ) :
        """ Increment the Success Count.  When this matches 

        :param message: SQS Message instance

        """
        try: 
            spot_master_msg = SpotMasterMsg( raw_json=message.get_body() )
            spot_master_uuid = spot_master_msg.spot_master_uuid
            dynamodb_conn = boto.dynamodb2.connect_to_region( self.region_name, profile_name=self.profile_name )
    
            max_attempts = 10
            num_attempts = 0
            while True:
                spot_master_table = Table( self.spot_master_table_name, connection=dynamodb_conn ) 
                spot_master_item = spot_master_table.get_item( spot_master_uuid=spot_master_uuid )
                spot_master_item[ TableSpotMaster.num_requests_complete_ok ] = spot_master_item[ TableSpotMaster.num_requests_complete_ok ] + 1
                try:
                    partial_save_result = spot_master_item.partial_save()
                    # partial_save_result = master_item.partial_save()
                    if partial_save_result: break
                except StandardError:
                    pass
                num_attempts += 1
                if num_attempts == max_attempts: 
                    raise awsspotbatch.common.exception.DynamoDbPartialSaveError('Exceeded partial save attempts on master table for num_requests_complete_ok, spot_master_uuid=' + spot_master_uuid, self.spot_master_table_name )
                time.sleep(6)
                    
            self.spot_master_sqs_message_durable.delete_message(message)        
        
        except StandardError as e:
            logger.error( fmt_master_item_msg_hdr( spot_master_item ) + str(e) )
            logger.error( fmt_master_item_msg_hdr( spot_master_item ) + traceback.format_exc() )
        

