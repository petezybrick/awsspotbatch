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
Create and Test Master and Request SQS Queues
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import sys
import traceback
import time
import uuid
import awsext.sqs
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg
from awsext.sqs.messagedurable import SqsMessageDurable


import logging
logger = logging.getLogger(__name__)


class QueueMgr(object):
    """classdocs"""


    def __init__(self, region_name, profile_name=None, spot_master_queue_name=awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME, 
                 spot_request_queue_name=awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME):
        """

        :param region_name: 
        :param profile_name:  (Default value = None)
        :param spot_master_queue_name:  (Default value = awsspotbatch.common.const.SPOT_MASTER_QUEUE_NAME)
        :param spot_request_queue_name:  (Default value = awsspotbatch.common.const.SPOT_REQUEST_QUEUE_NAME)

        """
        self.region_name = region_name
        self.profile_name = profile_name
        self.spot_master_queue_name = spot_master_queue_name
        self.spot_request_queue_name = spot_request_queue_name
        self.sqs_conn = awsext.sqs.connect_to_region( self.region_name, profile_name=self.profile_name )
        self.spot_master_queue = None
        self.spot_request_queue = None     


    def create_queues( self ):         
        """ """
        try:
            logger.info( 'Starting' )                           
            # if the queue exists then delete it
            is_any_queue_exists = False
            self.spot_master_queue = self.sqs_conn.get_queue( self.spot_master_queue_name )
            if self.spot_master_queue != None: 
                self.sqs_conn.delete_queue_sync( self.spot_master_queue )
                is_any_queue_exists = True
            self.spot_request_queue = self.sqs_conn.get_queue( self.spot_request_queue_name )
            if self.spot_request_queue != None: 
                self.sqs_conn.delete_queue_sync( self.spot_request_queue )
                is_any_queue_exists = True
            if is_any_queue_exists: 
                logger.info('Sleeping for 1 minute for queue deletion to clear')
                time.sleep(60)
                
            self.spot_master_queue = self.sqs_conn.create_queue_sync( self.spot_master_queue_name )
            self.spot_request_queue = self.sqs_conn.create_queue_sync( self.spot_request_queue_name )
            
            # Long Polling
            # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
            is_set_ok = self.sqs_conn.set_queue_attribute( self.spot_master_queue, 'ReceiveMessageWaitTimeSeconds', '20' )
            if not is_set_ok: logger.warn('Failed: set_queue_attribute ReceiveMessageWaitTimeSeconds on master_queue')   
            is_set_ok = self.sqs_conn.set_queue_attribute( self.spot_request_queue, 'ReceiveMessageWaitTimeSeconds', '20' )
            if not is_set_ok: logger.warn('Failed: set_queue_attribute ReceiveMessageWaitTimeSeconds on spot_request_queue')
            # All retries (ec2, dynamodb, etc.) are set for 10 attempts w/ 6 second sleep in between - minimize
            # probability of visibility timeout window expiring during retries
            is_set_ok = self.sqs_conn.set_queue_attribute( self.spot_master_queue, 'VisibilityTimeout', '90' )
            if not is_set_ok: logger.warn('Failed: set_queue_attribute VisibilityTimeout on master_queue')   
            is_set_ok = self.sqs_conn.set_queue_attribute( self.spot_request_queue, 'VisibilityTimeout', '90' )
            if not is_set_ok: logger.warn('Failed: set_queue_attribute VisibilityTimeout on spot_request_queue')
            

            logger.info( 'Completed Successfully' )
    
        except StandardError as e:
            logger.error( e )
            logger.error( traceback.format_exc() )
            sys.exit(8)
    
    
    def send_test_data( self ):         
        """ """
        try:
            spot_master_uuid = str( uuid.uuid1() )
            spot_master_msg_submit_batch = SpotMasterMsg( spot_master_uuid, SpotMasterMsg.TYPE_SUBMIT_BATCH )
            
            spot_master_msg_check_status = SpotMasterMsg( spot_master_uuid, SpotMasterMsg.TYPE_CHECK_STATUS )
            
            spot_sqs_message_durable = SqsMessageDurable( self.spot_master_queue_name, region_name=self.region_name, 
                                                     profile_name=self.profile_name)
            
            spot_sqs_message_durable.send_message( spot_master_msg_submit_batch.to_json() )
            spot_sqs_message_durable.send_message( spot_master_msg_check_status.to_json() )

        except StandardError as e:
            logger.error( e )
            logger.error( traceback.format_exc() )
            sys.exit(8)
    
    
    def receive_test_data( self ):         
        """ """
        try:
            spot_sqs_message_durable = SqsMessageDurable( self.spot_master_queue_name, region_name=self.region_name, 
                                                     profile_name=self.profile_name)
            while True:
                message = spot_sqs_message_durable.receive_message();
                if message == None: break;
                spot_master_msg = SpotMasterMsg( raw_json=message.get_body() )
                logger.info('spot_master_msg: type=' + spot_master_msg.spot_master_msg_type )
                spot_sqs_message_durable.delete_message(message)
    
        except StandardError as e:
            logger.error( e )
            logger.error( traceback.format_exc() )
            sys.exit(8)
