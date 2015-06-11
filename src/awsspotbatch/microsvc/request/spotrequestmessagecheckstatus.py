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
Process the SpotRequestMessageCheckStatus message
:author: Pete Zybrick
:contact: pete.zybrick@ipc-global.com, pzybrick@gmail.com
:version: 1.1
"""

import time
import traceback
import awsext.ec2
import boto.exception
from awsspotbatch.microsvc.request import spot_request_row_partial_save, get_spot_request_item, launch_remote_client, fmt_request_uuid_msg_hdr, fmt_request_item_msg_hdr
import awsspotbatch.common.const
from awsspotbatch.common.msg import SpotMasterMsg, SpotRequestMsg
from awsspotbatch.common.tabledef import SpotRequestStateCode, TableSpotRequest
from awsspotbatch.microsvc.request.spotrequestmessagebase import SpotRequestMessageBase
from awsext.sqs.messagedurable import SqsMessageDurable
from awsspotbatch.common.util import create_microsvc_message_attributes


import logging
logger = logging.getLogger(__name__)


class SpotRequestMessageCheckStatus(SpotRequestMessageBase):
    """ Process the SpotRequestMessageCheckStatus message"""

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
        """ Process the message

        :param message: SQS Message instance

        """
        try:
            spot_request_msg = SpotRequestMsg( raw_json=message.get_body() )
            spot_request_uuid = spot_request_msg.spot_request_uuid
            spot_master_uuid = spot_request_msg.spot_master_uuid
            spot_request_id = spot_request_msg.spot_request_id
            logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'process_check_status' )
            # Get spot request row from DynamoDB and process based on state
            spot_request_item = get_spot_request_item( self.spot_request_table_name, spot_request_uuid, region_name=self.region_name, profile_name=self.profile_name )
            logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'spot request state=' + spot_request_item[TableSpotRequest.spot_request_state_code])
             
            next_status_msg_delay_secs = 60
            is_send_request_msg_check_status = True
            spot_request_state_code = spot_request_item[TableSpotRequest.spot_request_state_code]
            # Update the LastStateCheck timestamp
            spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                            TableSpotRequest.ts_last_state_check:int( time.time() ),
                                            },
                                            region_name=self.region_name, profile_name=self.profile_name )
             
            if SpotRequestStateCode.spot_request_in_progress == spot_request_state_code:
                self.handle_state_request_spot_request_in_progress( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
            elif SpotRequestStateCode.instance_starting == spot_request_state_code:
                self.handle_state_request_instance_starting( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
            elif SpotRequestStateCode.instance_running == spot_request_state_code:
                self.handle_state_request_instance_running( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
            elif SpotRequestStateCode.instance_complete == spot_request_state_code:
                self.handle_state_request_instance_complete( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
                is_send_request_msg_check_status = False
            elif SpotRequestStateCode.instance_state_unknown == spot_request_state_code:
                self.handle_state_request_instance_state_unknown( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
            elif SpotRequestStateCode.constraint_encountered == spot_request_state_code:
                self.handle_state_request_constraint_encountered( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )       
            elif SpotRequestStateCode.instance_force_termination_pending == spot_request_state_code:
                self.handle_state_instance_force_termination_pending( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
            elif SpotRequestStateCode.instance_force_terminated == spot_request_state_code:
                self.handle_state_request_instance_force_terminated( spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid )
                is_send_request_msg_check_status = False
            
            if is_send_request_msg_check_status:
                spot_request_msg_check_status = SpotRequestMsg( spot_request_uuid=spot_request_uuid, 
                                                                spot_master_uuid=spot_master_uuid, 
                                                                spot_request_msg_type=SpotRequestMsg.TYPE_CHECK_STATUS, 
                                                                spot_request_id=spot_request_id )
                message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_REQUEST_CLASSNAME_SpotRequestMessageCheckStatus )
                self.spot_request_sqs_message_durable.send_message( spot_request_msg_check_status.to_json(), 
                                                                    delay_seconds=next_status_msg_delay_secs, 
                                                                    message_attributes=message_attributes  )
        
            self.spot_request_sqs_message_durable.delete_message(message)

        except StandardError as e:
            logger.error( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'Exiting SpotRequestDispatcher due to exception'  )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_uuid ) + str(e) )
            logger.error( fmt_request_uuid_msg_hdr( spot_request_uuid ) + traceback.format_exc() )    
  
    
    def handle_state_request_spot_request_in_progress( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ Check if spot request has been fullfilled, terminated or is still pending

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_spot_request_in_progress' )
        ec2_conn = awsext.ec2.connect_to_region( region_name=self.region_name, profile_name=self.profile_name )
        try:
            spot_instance_requests = ec2_conn.get_all_spot_instance_requests(request_ids=[spot_request_item[TableSpotRequest.spot_request_id]] )
            if spot_instance_requests != None and len(spot_instance_requests) == 1:
                spot_instance_request = spot_instance_requests[0]
                logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'spot_instance_request.status.code=' + spot_instance_request.status.code + 
                            ' for spot_request_id=' + spot_request_item[TableSpotRequest.spot_request_id ] + 
                            ', spot_request_uuid=' + spot_request_uuid )
                if spot_instance_request.status.code == 'schedule-expired' or spot_instance_request.status.code in awsext.ec2.connection.AwsExtEC2Connection.SPOT_REQUEST_CONSTRAINTS:
                    # update status so that a new spot request can occur
                    spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                  TableSpotRequest.spot_request_state_code:SpotRequestStateCode.constraint_encountered,
                                                  TableSpotRequest.constraint_code:spot_instance_request.status.code,
                                                  },
                                                  region_name=self.region_name, profile_name=self.profile_name )
                # Instance has been assigned, it's starting up now
                if spot_instance_request.instance_id != None:
                    ec2_conn.create_tags( [spot_instance_request.instance_id], 
                                          { 'Name':'spot_request_uuid_'+spot_request_uuid, 'spot_master_uuid':spot_master_uuid, 'spot_request_uuid':spot_request_uuid} )
                    spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                        TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_starting,
                                                        TableSpotRequest.instance_id:spot_instance_request.instance_id,
                                                        },
                                                        region_name=self.region_name, profile_name=self.profile_name )
                
        except boto.exception.EC2ResponseError as e:
            logger.error( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'Instance unknown state, exception code=' + e.code + ', spot_request_uuid=' + spot_request_uuid + ', spot_master_uuid=' + spot_master_uuid)
            logger.error( fmt_request_uuid_msg_hdr( spot_request_uuid ) + traceback.format_exc() )  
            spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                            TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_state_unknown,
                                                            },
                                                            region_name=self.region_name, profile_name=self.profile_name )

    
    def handle_state_request_instance_starting( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ Spot request fullfilled, the instance is starting.  
            Once the instance is running/ok, change status to instance_running

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_instance_starting' )   
        # Check if the instance is totally up (running and all status checks ok)
        ec2_conn = awsext.ec2.connect_to_region( region_name=self.region_name, profile_name=self.profile_name )
        instance_id = spot_request_item[ TableSpotRequest.instance_id ]
        state_name, status = ec2_conn.get_instance_state_name_and_status( instance_id )
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'spot instance state_name=' + state_name + ', status=' + status )
        if state_name == 'running' and status == 'ok':
            try:
                instance = ec2_conn.get_only_instances(instance_ids=[instance_id])[0]
                spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                        TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_running,
                                                        TableSpotRequest.instance_public_ip_address:instance.ip_address,
                                                        },
                                                        region_name=self.region_name, profile_name=self.profile_name )
                # Instance is running and ok - run the clientlaunch, which will start the wrapper, run some init cmds, launch user cmd on thread and
                # start sending heartbeats
                launch_remote_client( self.spot_batch_job_parm_table_name, 
                                      self.spot_rsa_key_table_name, spot_request_item, 
                                      region_name=self.region_name, 
                                      profile_name=self.profile_name)

            except boto.exception.EC2ResponseError as e:
                if e.code == 'InvalidInstanceID.NotFound':
                    logger.warning( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'Instance terminated after starting and before init complete, spot_master_uuid=' + spot_master_uuid)
                    # looks like spot instance was terminated between the time it was allocated and initialized
                    spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                            TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_force_terminated,
                                                            },
                                                            region_name=self.region_name, profile_name=self.profile_name )
                else: 
                    logger.error( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'Instance unknown state, exception code=' + e.code + ', spot_master_uuid=' + spot_master_uuid)
                    logger.error( traceback.format_exc() )  
                    spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                            TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_state_unknown,
                                                            },
                                                            region_name=self.region_name, profile_name=self.profile_name )
        elif state_name == 'terminated':
            logger.warning( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'Instance terminated after starting and before init complete, spot_master_uuid=' + spot_master_uuid)
            # looks like spot instance was terminated between the time it was allocated and initialized
            spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                    TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_force_terminated,
                                                    },
                                                    region_name=self.region_name, profile_name=self.profile_name )
   
   
    def handle_state_request_instance_running( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ At this point, :class:awsspotbatch.client.clientlaunch is running on the spot instance,
            it's sending back heartbeats, checking for spot termination and monitoring the users' batch job

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_instance_running' )
    
    
    def handle_state_request_instance_complete( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ User's batch job has completed, start instance termination process

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_instance_complete' )
        # terminate the instance, if it exists              
        if spot_request_item[TableSpotRequest.instance_id] != None:
            ec2_conn = awsext.ec2.connect_to_region( self.region_name, profile_name=self.profile_name )
            ec2_conn.terminate_instances( instance_ids=[ spot_request_item[TableSpotRequest.instance_id] ] )
        ts_now = int( time.time() )
        spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, 
                                            {TableSpotRequest.is_open:0, TableSpotRequest.ts_end:ts_now },
                                            region_name=self.region_name, profile_name=self.profile_name )
    
    
    def handle_state_request_instance_force_terminated( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ Spot instance was terminated by AWS, the termination has completed

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_instance_force_terminated' )
        ts_now = int( time.time() )
        spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                                TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_complete,
                                                                TableSpotRequest.is_open:0, 
                                                                TableSpotRequest.ts_end:ts_now
                                                                 },
                                                                 region_name=self.region_name, profile_name=self.profile_name )
        # Create a new spot request based on the spot request that just failed
        master_msg_resubmit_failed_request = SpotMasterMsg( spot_master_uuid=spot_request_msg.spot_master_uuid, 
                                                  spot_master_msg_type=SpotMasterMsg.TYPE_RESUBMIT_FAILED_REQUEST,
                                                  spot_request_uuid=spot_request_msg.spot_request_uuid
                                                   )
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageResubmitFailedRequest )
        spot_master_sqs_message_durable = SqsMessageDurable( self.spot_master_queue_name, self.region_name, profile_name=self.profile_name )
        spot_master_sqs_message_durable.send_message( master_msg_resubmit_failed_request.to_json(),
                                                           message_attributes=message_attributes )    
    
    
    def handle_state_instance_force_termination_pending( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ AWS has started the termination process for this instance, i.e. the price has increased
            This is the beginning of the two minute warning pending forced termination
            Terminate the instance and start another spot request

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_instance_force_termination_pending' )
        ts_now = int( time.time() )
        spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                                TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_complete,
                                                                TableSpotRequest.is_open:0, 
                                                                TableSpotRequest.ts_end:ts_now
                                                                 },
                                                                 region_name=self.region_name, profile_name=self.profile_name )
        # Create a new spot request based on the spot request that just failed
        master_msg_resubmit_failed_request = SpotMasterMsg( spot_master_uuid=spot_request_msg.spot_master_uuid, 
                                                  spot_master_msg_type=SpotMasterMsg.TYPE_RESUBMIT_FAILED_REQUEST,
                                                  spot_request_uuid=spot_request_msg.spot_request_uuid
                                                   )
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageResubmitFailedRequest )
        spot_master_sqs_message_durable = SqsMessageDurable( self.spot_master_queue_name, self.region_name, profile_name=self.profile_name )
        spot_master_sqs_message_durable.send_message( master_msg_resubmit_failed_request.to_json(),
                                                           message_attributes=message_attributes )
    
    
    def handle_state_request_instance_state_unknown( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ Handle any future states - 

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.warning( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_instance_state_unknown' )
        ts_now = int( time.time() )
        spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                                TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_complete,
                                                                TableSpotRequest.is_open:0, 
                                                                TableSpotRequest.ts_end:ts_now
                                                                 },
                                                                 region_name=self.region_name, profile_name=self.profile_name )
    
    
    def handle_state_request_constraint_encountered( self, spot_request_msg, spot_request_item, spot_request_uuid, spot_master_uuid ):
        """ Constraint encountered after spot request initiated but before request fullfilled, 
            i.e. time limit expired
            Submit another spot request

        :param spot_request_msg: 
        :param spot_request_item: 
        :param spot_request_uuid: 
        :param spot_master_uuid: 

        """
        logger.info( fmt_request_uuid_msg_hdr( spot_request_uuid ) + 'handle_state_request_constraint_encountered' )
        ts_now = int( time.time() )
        spot_request_row_partial_save( self.spot_request_table_name, spot_request_item, {
                                                                TableSpotRequest.spot_request_state_code:SpotRequestStateCode.instance_complete,
                                                                TableSpotRequest.is_open:0, 
                                                                TableSpotRequest.ts_end:ts_now
                                                                 },
                                                                 region_name=self.region_name, profile_name=self.profile_name )
        # Create a new spot request based on the spot request that just failed
        master_msg_resubmit_failed_request = SpotMasterMsg( spot_master_uuid=spot_request_msg.spot_master_uuid, 
                                                  spot_master_msg_type=SpotMasterMsg.TYPE_RESUBMIT_FAILED_REQUEST,
                                                  spot_request_uuid=spot_request_msg.spot_request_uuid
                                                   )
        message_attributes = create_microsvc_message_attributes( awsspotbatch.common.const.MICROSVC_MASTER_CLASSNAME_SpotMasterMessageResubmitFailedRequest )
        spot_master_sqs_message_durable = SqsMessageDurable( self.spot_master_queue_name, self.region_name, profile_name=self.profile_name )
        spot_master_sqs_message_durable.send_message( master_msg_resubmit_failed_request.to_json(),
                                                           message_attributes=message_attributes )
                    
